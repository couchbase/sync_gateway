//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"sort"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/google/uuid"
)

// Options for changes-feeds.  ChangesOptions must not contain any mutable pointer references, as
// changes processing currently assumes a deep copy when doing chanOpts := changesOptions.
type ChangesOptions struct {
	Since          SequenceID      // sequence # to start _after_
	Limit          int             // Max number of changes to return, if nonzero
	Conflicts      bool            // Show all conflicting revision IDs, not just winning one?
	IncludeDocs    bool            // Include doc body of each change?
	Wait           bool            // Wait for results, instead of immediately returning empty result?
	Continuous     bool            // Run continuously until terminated?
	RequestPlusSeq uint64          // Do not stop changes before cached sequence catches up with requestPlusSeq
	HeartbeatMs    uint64          // How often to send a heartbeat to the client
	TimeoutMs      uint64          // After this amount of time, close the longpoll connection
	ActiveOnly     bool            // If true, only return information on non-deleted, non-removed revisions
	Revocations    bool            // Specifies whether revocation messages should be sent on the changes feed
	clientType     clientType      // Can be used to determine if the replication is being started from a CBL 2.x or SGR2 client
	ChangesCtx     context.Context // Used for cancelling checking the changes feed should stop
}

// A changes entry; Database.GetChanges returns an array of these.
// Marshals into the standard CouchDB _changes format.
type ChangeEntry struct {
	Seq          SequenceID      `json:"seq"`
	ID           string          `json:"id"`
	Deleted      bool            `json:"deleted,omitempty"`
	Removed      base.Set        `json:"removed,omitempty"`
	Doc          json.RawMessage `json:"doc,omitempty"`
	Changes      []ChangeRev     `json:"changes"`
	Err          error           `json:"err,omitempty"` // Used to notify feed consumer of errors
	allRemoved   bool            // Flag to track whether an entry is a removal in all channels visible to the user.
	branched     bool
	backfill     backfillFlag // Flag used to identify non-client entries used for backfill synchronization (di only)
	principalDoc bool         // Used to indicate _user/_role docs
	Revoked      bool         `json:"revoked,omitempty"`
	collectionID uint32
}

const (
	WaiterClosed uint32 = iota
	WaiterHasChanges
	WaiterCheckTerminated
)

type backfillFlag int8

const (
	BackfillFlag_None backfillFlag = iota
	BackfillFlag_Pending
	BackfillFlag_Complete
)

type ChangeRev map[string]string // Key is always "rev", value is rev ID

type ViewDoc struct {
	Json json.RawMessage // should be type 'document', but that fails to unmarshal correctly
}

func (db *DatabaseCollectionWithUser) AddDocToChangeEntry(ctx context.Context, entry *ChangeEntry, options ChangesOptions) {
	db.addDocToChangeEntry(ctx, entry, options)
}

// Adds a document body and/or its conflicts to a ChangeEntry
func (db *DatabaseCollectionWithUser) addDocToChangeEntry(ctx context.Context, entry *ChangeEntry, options ChangesOptions) {

	includeConflicts := options.Conflicts && entry.branched
	if !options.IncludeDocs && !includeConflicts {
		return
	}

	// If this is principal doc, we don't send body and/or conflicts
	if entry.principalDoc {
		return
	}

	// Three options for retrieving document content, depending on what's required:
	//   includeConflicts only:
	//      - Retrieve document metadata from bucket (required to identify current set of conflicts)
	//   includeDocs only:
	//      - Use rev cache to retrieve document body
	//   includeConflicts and includeDocs:
	//      - Retrieve document AND metadata from bucket; single round-trip usually more efficient than
	//      metadata retrieval + rev cache retrieval (since rev cache miss will trigger KV retrieval of doc+metadata again)

	if options.IncludeDocs && includeConflicts {
		// Load doc body + metadata
		doc, err := db.GetDocument(ctx, entry.ID, DocUnmarshalAll)
		if err != nil {
			base.WarnfCtx(ctx, "Changes feed: error getting doc %q: %v", base.UD(entry.ID), err)
			return
		}
		db.AddDocInstanceToChangeEntry(ctx, entry, doc, options)

	} else if includeConflicts {
		// Load doc metadata only
		doc := &Document{}
		var err error
		doc.SyncData, err = db.GetDocSyncData(ctx, entry.ID)
		if err != nil {
			base.WarnfCtx(ctx, "Changes feed: error getting doc sync data %q: %v", base.UD(entry.ID), err)
			return
		}
		db.AddDocInstanceToChangeEntry(ctx, entry, doc, options)

	} else if options.IncludeDocs {
		// Retrieve document via rev cache
		revID := entry.Changes[0]["rev"]
		err := db.AddDocToChangeEntryUsingRevCache(ctx, entry, revID)
		if err != nil {
			base.WarnfCtx(ctx, "Changes feed: error getting revision body for %q (%s): %v", base.UD(entry.ID), revID, err)
		}
	}

}

func (db *DatabaseCollectionWithUser) AddDocToChangeEntryUsingRevCache(ctx context.Context, entry *ChangeEntry, revID string) (err error) {
	rev, err := db.getRev(ctx, entry.ID, revID, 0, nil)
	if err != nil {
		return err
	}
	entry.Doc, err = rev.As1xBytes(ctx, db, nil, nil, false)
	return err
}

// Adds a document body and/or its conflicts to a ChangeEntry
func (db *DatabaseCollectionWithUser) AddDocInstanceToChangeEntry(ctx context.Context, entry *ChangeEntry, doc *Document, options ChangesOptions) {

	includeConflicts := options.Conflicts && entry.branched

	revID := entry.Changes[0]["rev"]
	if includeConflicts {
		doc.History.forEachLeaf(func(leaf *RevInfo) {
			if leaf.ID != revID {
				if !leaf.Deleted {
					entry.Deleted = false
				}
				if !(options.ActiveOnly && leaf.Deleted) {
					entry.Changes = append(entry.Changes, ChangeRev{"rev": leaf.ID})
				}
			}
		})
	}
	if options.IncludeDocs {
		var err error
		entry.Doc, _, err = db.get1xRevFromDoc(ctx, doc, revID, false)
		db.dbStats().Database().NumDocReadsRest.Add(1)
		if err != nil {
			base.WarnfCtx(ctx, "Changes feed: error getting doc %q/%q: %v", base.UD(doc.ID), revID, err)
		}
	}
}

// Parameters
// revokedAt: This is the point at which the channel was revoked from the user. Used here for the triggeredBy.
// revocationSinceSeq: The point in time at which we need to 'diff' against in order to check what documents we need to
// revoke - only documents in the channel at the revocationSinceSeq may have been replicated, and need to be revoked.
// This is used in this function in 'wasDocInChannelAtSeq'.
// revokeFrom: This is the point at which we should run the changes feed from to find the documents we should revoke. It
// is calculated higher up based on whether we are resuming an interrupted feed or not.
func (db *DatabaseCollectionWithUser) buildRevokedFeed(ctx context.Context, ch channels.ID, options ChangesOptions, revokedAt, revocationSinceSeq, revokeFrom uint64, to string) <-chan *ChangeEntry {
	feed := make(chan *ChangeEntry, 1)
	sinceVal := options.Since.Seq

	queryLimit := db.channelQueryLimit()
	requestLimit := options.Limit

	paginationOptions := options
	paginationOptions.Since.LowSeq = 0

	// For changes feed we can initiate the revocation work from this passed in value.
	// In the event that we have a interrupted replication we can restart part way through, otherwise we have to
	// check from 0.
	paginationOptions.Since.Seq = revokeFrom

	// Use a bypass channel cache for revocations (CBG-1695)
	singleChannelCache, err := db.changeCache().getChannelCache().getBypassChannelCache(ch)
	if err != nil {
		base.WarnfCtx(ctx, "Error obtaining channel cache for channel %q: %v", base.UD(singleChannelCache.ChannelID().Name), err)
		change := ChangeEntry{
			Err: base.ErrChannelFeed,
		}
		feed <- &change
		close(feed)
		return feed
	}

	go func() {
		defer base.FatalPanicHandler()
		defer close(feed)
		var itemsSent int
		var lastSeq uint64

		// Pagination based on ChannelQueryLimit.  This loop may terminated in three ways (see return statements):
		//   1. Query returns fewer rows than ChannelQueryLimit
		//   2. A limit is specified on the incoming ChangesOptions, and that limit is reached
		//   3. An error is returned when calling singleChannelCache.GetChanges
		//   4. An error is returned when calling wasDocInChannelPriorToRevocation
		for {
			if options.ChangesCtx.Err() != nil {
				base.DebugfCtx(ctx, base.KeyChanges, "Terminating revocation channel feed %s", base.UD(to))
				return
			}

			if requestLimit == 0 {
				paginationOptions.Limit = queryLimit
			} else {
				remainingLimit := requestLimit - itemsSent
				paginationOptions.Limit = base.Min(remainingLimit, queryLimit)
			}

			// Get changes from 0 to latest seq
			base.TracefCtx(ctx, base.KeyChanges, "Querying channel %q for revocation with options: %+v", base.UD(singleChannelCache.ChannelID().Name), paginationOptions)
			changes, err := singleChannelCache.GetChanges(ctx, paginationOptions)
			if err != nil {
				base.WarnfCtx(ctx, "Error retrieving changes for channel %q: %v", base.UD(singleChannelCache.ChannelID()), err)
				change := ChangeEntry{
					Err: base.ErrChannelFeed,
				}
				feed <- &change
				return
			}
			base.DebugfCtx(ctx, base.KeyChanges, "[revocationChangesFeed] Found %d changes for channel %q", len(changes), base.UD(singleChannelCache.ChannelID().Name))

			sentChanges := 0
			for _, logEntry := range changes {
				if options.ChangesCtx.Err() != nil {
					base.DebugfCtx(ctx, base.KeyChanges, "Terminating revocation channel feed %s", base.UD(to))
					return
				}

				seqID := SequenceID{
					Seq:         logEntry.Sequence,
					TriggeredBy: revokedAt,
				}

				// We need to check whether a change / document sequence is greater than since.
				// If its less than we can send a standard revocation with Sequence ID as above.
				// Otherwise: we need to determine whether a previous revision of the document was in the channel prior
				// to the since value, and only send a revocation if that was the case

				lastSeq = logEntry.Sequence
				if logEntry.Sequence > sinceVal {
					// Get doc sync data so we can verify the docs grant history
					syncData, err := db.GetDocSyncData(ctx, logEntry.DocID)
					if err != nil {
						base.InfofCtx(ctx, base.KeyChanges, "Couldn't get history for doc %q for channel %s, skipping revocation checks: %v", base.UD(logEntry.DocID), base.UD(singleChannelCache.ChannelID().Name), err)
						continue
					}
					requiresRevocation, err := db.wasDocInChannelPriorToRevocation(ctx, syncData, logEntry.DocID, singleChannelCache.ChannelID().Name, revocationSinceSeq)
					if err != nil {
						change := ChangeEntry{
							Err: base.ErrChannelFeed,
						}
						feed <- &change
						base.WarnfCtx(ctx, "Error checking document history during revocation, seq: %v in channel %s, ending revocation feed. Error: %v", seqID, base.UD(singleChannelCache.ChannelID().Name), err)
						return
					}

					if !requiresRevocation {
						base.TracefCtx(ctx, base.KeyChanges, "Channel feed processing revocation, seq: %v in channel %s does not require revocation", seqID, base.UD(singleChannelCache.ChannelID().Name))
						continue
					}
				}

				userHasAccessToDoc, err := UserHasDocAccess(ctx, db, logEntry.DocID)
				if err != nil {
					change := ChangeEntry{
						Err: base.ErrChannelFeed,
					}
					feed <- &change
					return
				}

				if userHasAccessToDoc {
					paginationOptions.Since.Seq = lastSeq
					continue
				}

				change := makeRevocationChangeEntry(logEntry, seqID, singleChannelCache.ChannelID())

				base.DebugfCtx(ctx, base.KeyChanges, "Channel feed processing revocation seq: %v in channel %s ", seqID, base.UD(singleChannelCache.ChannelID().Name))

				select {
				case <-options.ChangesCtx.Done():
					base.DebugfCtx(ctx, base.KeyChanges, "Terminating revocation channel feed %s", base.UD(to))
					return

				case feed <- &change:
					base.Audit(ctx, base.AuditIDDocumentRevoke, base.AuditFields{
						base.AuditFieldDocID:      logEntry.DocID,
						base.AuditFieldDocVersion: logEntry.RevID,
					})
					sentChanges++
				}
			}

			if len(changes) < paginationOptions.Limit {
				return
			}

			itemsSent += sentChanges
			if requestLimit > 0 && itemsSent >= requestLimit {
				return
			}

			paginationOptions.Since.Seq = lastSeq
		}
	}()

	return feed
}

// UserHasDocAccess checks whether the user has access to the active revision of the document
func UserHasDocAccess(ctx context.Context, collection *DatabaseCollectionWithUser, docID string) (bool, error) {
	currentRev, err := collection.revisionCache.GetActive(ctx, docID)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			return false, nil
		}
		return false, err
	}

	if collection.user == nil {
		return true, nil
	}

	authErr := collection.user.AuthorizeAnyCollectionChannel(collection.ScopeName, collection.Name, currentRev.Channels)
	return authErr == nil, nil
}

// Checks if a document needs to be revoked. This is used in the case where the since < doc sequence
func (col *DatabaseCollectionWithUser) wasDocInChannelPriorToRevocation(ctx context.Context, syncData SyncData, docID, chanName string, since uint64) (bool, error) {
	// Obtain periods where the channel we're interested in was accessible by the user
	channelAccessPeriods, err := col.user.CollectionChannelGrantedPeriods(col.ScopeName, col.Name, chanName)
	if err != nil {
		return false, err
	}

	// Iterate over the channel history information on the document and find any periods where the doc was in the
	// channel and the channel was accessible by the user
	isStarChan := chanName == channels.UserStarChannel
	for _, docHistoryEntry := range append(syncData.ChannelSet, syncData.ChannelSetHistory...) {
		if !isStarChan && docHistoryEntry.Name != chanName {
			continue
		}

		for _, accessPeriod := range channelAccessPeriods {
			if accessPeriod.EndSeq <= since {
				continue
			}

			start := base.Max(docHistoryEntry.Start, accessPeriod.StartSeq)

			end := uint64(math.MaxUint64)
			if docHistoryEntry.End != 0 {
				end = docHistoryEntry.End
			}

			if accessPeriod.EndSeq != 0 {
				end = base.Min(end, accessPeriod.EndSeq)
			}

			// If we have an overlap between when the doc was in the channel and when we had access to the channel
			if start < end {
				return true, nil
			}
		}
	}

	return false, nil
}

// Creates a Go-channel of all the changes made on a channel.
// Does NOT handle the Wait option. Does NOT check authorization.
func (db *DatabaseCollectionWithUser) changesFeed(ctx context.Context, singleChannelCache SingleChannelCache, options ChangesOptions, to string) <-chan *ChangeEntry {

	feed := make(chan *ChangeEntry, 1)

	queryLimit := db.channelQueryLimit()
	requestLimit := options.Limit

	// Make a copy of the changesOptions so that query pagination can modify since and limit.  Pagination uses safe sequence
	// as starting point and can subsequently ignore LowSeq - it is added back to entries as needed when the main
	// changes loop processes the channel's feed.
	paginationOptions := options
	paginationOptions.Since.Seq = options.Since.SafeSequence()
	paginationOptions.Since.LowSeq = 0

	go func() {
		defer base.FatalPanicHandler()
		defer close(feed)
		var itemsSent int
		var lastSeq uint64
		// Pagination based on ChannelQueryLimit.  This loop may terminated in three ways (see return statements):
		//   1. Query returns fewer rows than ChannelQueryLimit
		//   2. A limit is specified on the incoming ChangesOptions, and that limit is reached
		//   3. An error is returned when calling singleChannelCache.GetChanges
		for {
			if options.ChangesCtx.Err() != nil {
				base.DebugfCtx(ctx, base.KeyChanges, "Terminating channel feed %s", base.UD(to))
				return
			}
			// Calculate limit for this iteration
			if requestLimit == 0 {
				paginationOptions.Limit = queryLimit
			} else {
				remainingLimit := requestLimit - itemsSent
				paginationOptions.Limit = base.Min(remainingLimit, queryLimit)
			}

			base.TracefCtx(ctx, base.KeyChanges, "Querying channel %q with options: %+v", base.UD(singleChannelCache.ChannelID().Name), paginationOptions)
			changes, err := singleChannelCache.GetChanges(ctx, paginationOptions)
			if err != nil {
				base.WarnfCtx(ctx, "Error retrieving changes for channel %q: %v", base.UD(singleChannelCache.ChannelID().Name), err)
				change := ChangeEntry{
					Err: base.ErrChannelFeed,
				}
				feed <- &change
				return
			}
			base.DebugfCtx(ctx, base.KeyChanges, "[changesFeed] Found %d changes for channel %q", len(changes), base.UD(singleChannelCache.ChannelID().Name))

			// Now write each log entry to the 'feed' channel in turn:
			sentChanges := 0
			for _, logEntry := range changes {
				if options.ChangesCtx.Err() != nil {
					base.DebugfCtx(ctx, base.KeyChanges, "Terminating channel feed %s", base.UD(to))
					return
				}
				if logEntry.Sequence >= options.Since.TriggeredBy {
					options.Since.TriggeredBy = 0
				}
				seqID := SequenceID{
					Seq:         logEntry.Sequence,
					TriggeredBy: options.Since.TriggeredBy,
				}

				change := makeChangeEntry(logEntry, seqID, singleChannelCache.ChannelID())
				lastSeq = logEntry.Sequence

				// Don't include deletes or removals during initial channel backfill
				if options.Since.TriggeredBy > 0 && (change.Deleted || len(change.Removed) > 0) {
					continue
				}

				base.DebugfCtx(ctx, base.KeyChanges, "Channel feed processing seq:%v in channel %s %s", seqID, base.UD(singleChannelCache.ChannelID().Name), base.UD(to))
				select {
				case <-options.ChangesCtx.Done():
					base.DebugfCtx(ctx, base.KeyChanges, "Terminating channel feed %s", base.UD(to))
					return
				case feed <- &change:
					sentChanges++
				}
			}

			// If the query returned fewer results than the pagination limit, we're done
			if len(changes) < paginationOptions.Limit {
				return
			}

			// If we've reached the request limit, we're done
			itemsSent += sentChanges
			if requestLimit > 0 && itemsSent >= requestLimit {
				return
			}

			paginationOptions.Since.Seq = lastSeq
		}
	}()
	return feed
}

func makeChangeEntry(logEntry *LogEntry, seqID SequenceID, channel channels.ID) ChangeEntry {
	change := ChangeEntry{
		Seq:          seqID,
		ID:           logEntry.DocID,
		Deleted:      (logEntry.Flags & channels.Deleted) != 0,
		Changes:      []ChangeRev{{"rev": logEntry.RevID}},
		branched:     (logEntry.Flags & channels.Branched) != 0,
		principalDoc: logEntry.IsPrincipal,
		collectionID: logEntry.CollectionID,
	}
	if logEntry.Flags&channels.Removed != 0 {
		change.Removed = base.SetOf(channel.Name)
	}

	return change
}

func makeRevocationChangeEntry(logEntry *LogEntry, seqID SequenceID, channel channels.ID) ChangeEntry {
	entry := makeChangeEntry(logEntry, seqID, channel)
	entry.Revoked = true

	return entry
}

// AuditReadEvent issues a read event for this change entry. If there is no document body, there will be no event used.
func (ce *ChangeEntry) AuditReadEvent(ctx context.Context) {
	if ce.Err != nil {
		return
	}
	if ce.Doc == nil {
		return
	}
	base.Audit(ctx, base.AuditIDDocumentRead, base.AuditFields{
		base.AuditFieldDocID:      ce.ID,
		base.AuditFieldDocVersion: ce.Changes[0]["rev"],
	})
}

func (ce *ChangeEntry) SetBranched(isBranched bool) {
	ce.branched = isBranched
}

func (ce *ChangeEntry) String() string {

	var deletedString, removedString, errString, allRemovedString, branchedString, backfillString string
	if ce.Deleted {
		deletedString = ", Deleted:true"
	}
	if len(ce.Removed) > 0 {
		removedString = fmt.Sprintf(", Removed:%v", ce.Removed)
	}
	if ce.Err != nil {
		errString = fmt.Sprintf(", Err:%v", ce.Err)
	}
	if ce.allRemoved {
		allRemovedString = ", allRemoved:true"
	}
	if ce.branched {
		branchedString = ", branched:true"
	}
	if ce.backfill != BackfillFlag_None {
		backfillString = fmt.Sprintf(", backfill:%d", ce.backfill)
	}
	return fmt.Sprintf("{Seq:%s, ID:%s, Changes:%s%s%s%s%s%s%s}", ce.Seq, ce.ID, ce.Changes, deletedString, removedString, errString, allRemovedString, branchedString, backfillString)
}

func makeErrorEntry(message string) ChangeEntry {

	change := ChangeEntry{
		Err: errors.New(message),
	}
	return change
}

func (db *DatabaseCollectionWithUser) MultiChangesFeed(ctx context.Context, chans base.Set, options ChangesOptions) (<-chan *ChangeEntry, error) {
	if len(chans) == 0 {
		return nil, nil
	}

	if options.ChangesCtx == nil {
		base.ErrorfCtx(ctx, "MultiChangesFeed: Changes Context missing")
	}

	base.DebugfCtx(ctx, base.KeyChanges, "Int sequence multi changes feed...")

	return db.SimpleMultiChangesFeed(ctx, chans, options)

}

func (db *DatabaseCollectionWithUser) startChangeWaiter(trackUnusedSequences bool) *ChangeWaiter {
	return db.mutationListener().NewWaiterWithChannels(channels.Set{}, db.user, trackUnusedSequences)
}

func (db *DatabaseCollectionWithUser) appendUserFeed(feeds []<-chan *ChangeEntry, options ChangesOptions) []<-chan *ChangeEntry {
	userSeq := SequenceID{Seq: db.user.Sequence()}
	if options.Since.Before(userSeq) {
		name := db.user.Name()
		if name == "" {
			name = base.GuestUsername
		}
		entry := ChangeEntry{
			Seq:          userSeq,
			ID:           "_user/" + name,
			Changes:      []ChangeRev{},
			principalDoc: true,
		}
		userFeed := make(chan *ChangeEntry, 1)
		userFeed <- &entry
		close(userFeed)
		feeds = append(feeds, userFeed)
	}
	return feeds
}

func (col *DatabaseCollectionWithUser) checkForUserUpdates(ctx context.Context, userChangeCount uint64, changeWaiter *ChangeWaiter, isContinuous bool) (isChanged bool, newCount uint64, changedChannels channels.ChangedKeys, err error) {

	newCount = changeWaiter.CurrentUserCount()
	// If not continuous, we force user reload as a workaround for https://github.com/couchbase/sync_gateway/issues/2068.  For continuous, #2068 is handled by changedChannels check, and
	// we can reload only when there's been a user change notification
	if newCount > userChangeCount || !isContinuous {
		var previousChannels channels.TimedSet
		base.DebugfCtx(ctx, base.KeyChanges, "MultiChangesFeed reloading user %+v", base.UD(col.user))

		if col.user != nil {
			previousChannels = col.user.InheritedCollectionChannels(col.ScopeName, col.Name)
			previousRoles := col.user.RoleNames()
			if err := col.ReloadUser(ctx); err != nil {
				base.WarnfCtx(ctx, "Error reloading user %q: %v", base.UD(col.user.Name()), err)
				return false, 0, nil, err
			}
			// check whether channel set has changed
			changedChannels = col.user.InheritedCollectionChannels(col.ScopeName, col.Name).CompareKeys(previousChannels)
			if len(changedChannels) > 0 {
				base.DebugfCtx(ctx, base.KeyChanges, "Modified channel set after user reload: %v", base.UD(changedChannels))
			}

			changedRoles := col.user.RoleNames().CompareKeys(previousRoles)
			if len(changedRoles) > 0 {
				changeWaiter.RefreshUserKeys(col.User(), col.dbCtx.MetadataKeys)
			}
		}
		return true, newCount, changedChannels, nil
	}
	return false, userChangeCount, nil, nil
}

// Returns the (ordered) union of all of the changes made to multiple channels.
func (col *DatabaseCollectionWithUser) SimpleMultiChangesFeed(ctx context.Context, chans base.Set, options ChangesOptions) (<-chan *ChangeEntry, error) {

	to := ""
	if col.user != nil && col.user.Name() != "" {
		to = fmt.Sprintf("  (to %s)", col.user.Name())
	} else {
		to = "  (to ADMIN)"
	}

	base.InfofCtx(ctx, base.KeyChanges, "MultiChangesFeed(channels: %s, options: %s) ... %s", base.UD(chans), options, base.UD(to))
	output := make(chan *ChangeEntry, 50)

	collectionID := col.GetCollectionID()
	go func() {

		defer func() {
			if panicked := recover(); panicked != nil {
				base.WarnfCtx(ctx, "[%s] Unexpected panic sending changes - terminating changes: \n %s", panicked, debug.Stack())
			} else {
				base.InfofCtx(ctx, base.KeyChanges, "MultiChangesFeed done %s", base.UD(to))
			}
			close(output)
		}()

		var changeWaiter *ChangeWaiter
		var lowSequence uint64
		var currentCachedSequence uint64 // The highest contiguous sequence buffered over the caching feed
		var lateSequenceFeeds map[channels.ID]*lateSequenceFeed
		var useLateSequenceFeeds bool            // LateSequence feeds are only used for continuous, or one-shot where options.RequestPlusSeq > currentCachedSequence
		var userCounter uint64                   // Wait counter used to identify changes to the user document
		var changedChannels channels.ChangedKeys // Tracks channels added/removed to the user during changes processing.
		var userChanged bool                     // Whether the user document has changed in a given iteration loop
		var deferredBackfill bool                // Whether there's a backfill identified in the user doc that's deferred while the SG cache catches up

		// Retrieve the current max cached sequence - ensures there isn't a race between the subsequent channel cache queries
		currentCachedSequence = col.changeCache().getChannelCache().GetHighCacheSequence()

		// If changes feed requires more than one ChangesLoop iteration, initialize changeWaiter
		if options.Wait || options.RequestPlusSeq > currentCachedSequence {
			trackUnusedSequences := options.RequestPlusSeq > 0
			changeWaiter = col.startChangeWaiter(trackUnusedSequences) // Waiter is updated with the actual channel set (post-user reload) at the start of the outer changes loop
			userCounter = changeWaiter.CurrentUserCount()
			// Reload user to pick up user changes that happened between auth and the change waiter
			// initialization.  Without this, notification for user doc changes in that window (a) won't be
			// included in the initial changes loop iteration, and (b) won't wake up the ChangeWaiter.
			if col.user != nil {
				previousRoles := col.user.RoleNames()
				if err := col.ReloadUser(ctx); err != nil {
					base.WarnfCtx(ctx, "Error reloading user during changes initialization %q: %v", base.UD(col.user.Name()), err)
					change := makeErrorEntry("User not found during reload - terminating changes feed")
					output <- &change
					return
				}
				changedRoles := col.user.RoleNames().CompareKeys(previousRoles)
				if len(changedRoles) > 0 {
					changeWaiter.RefreshUserKeys(col.User(), col.dbCtx.MetadataKeys)
				}
			}

		}

		// Restrict to available channels, expand wild-card, and find since when these channels
		// have been available to the user:
		var channelsSince channels.TimedSet
		if col.user != nil {
			var channelsRemoved []string
			channelsSince, channelsRemoved = col.user.FilterToAvailableCollectionChannels(col.ScopeName, col.Name, chans)
			if len(channelsRemoved) > 0 {
				base.InfofCtx(ctx, base.KeyChanges, "Channels %s request without access by user %s", base.UD(channelsRemoved), base.UD(col.user.Name()))
			}
		} else {
			channelsSince = channels.AtSequence(chans, 0)
		}

		// Mark channel set as active, schedule defer
		col.activeChannels().IncrChannels(collectionID, channelsSince)
		defer col.activeChannels().DecrChannels(ctx, collectionID, channelsSince)

		// For a continuous feed, initialise the lateSequenceFeeds that track late-arriving sequences
		// to the channel caches.
		if options.Continuous || options.RequestPlusSeq > currentCachedSequence {
			useLateSequenceFeeds = true
			lateSequenceFeeds = make(map[channels.ID]*lateSequenceFeed)
			defer col.closeLateFeeds(ctx, lateSequenceFeeds)
		}

		// Store incoming low sequence, for potential use by longpoll iterations
		requestLowSeq := options.Since.LowSeq
		// Last sent low sequence is needed for continuous replications that need to reset their late sequence feed (e.g.
		// due to cache compaction)
		lastSentLowSeq := options.Since.LowSeq

		// This loop is used to re-run the fetch after every database change, in Wait mode
	outer:
		for {
			// Updates the ChangeWaiter to the current set of available channels
			if changeWaiter != nil {
				changeWaiter.UpdateChannels(col.GetCollectionID(), channelsSince)
			}
			base.DebugfCtx(ctx, base.KeyChanges, "MultiChangesFeed: channels expand to %#v ... %s", base.UD(channelsSince), base.UD(to))

			// lowSequence is used to send composite keys to clients, so that they can obtain any currently
			// skipped sequences in a future iteration or request.
			oldestSkipped := col.changeCache().getOldestSkippedSequence(ctx)
			if oldestSkipped > 0 {
				lowSequence = oldestSkipped - 1
				base.InfofCtx(ctx, base.KeyChanges, "%d is the oldest skipped sequence, using stable sequence number of %d for this feed %s", oldestSkipped, lowSequence, base.UD(to))
			} else {
				lowSequence = 0
			}

			// If a request has a low sequence that matches the current lowSequence,
			// ignore the low sequence.  This avoids infinite looping of the records between
			// low::high.  It also means any additional skipped sequences between low::high won't
			// be sent until low arrives or is abandoned.
			if options.Since.LowSeq != 0 && options.Since.LowSeq == lowSequence {
				options.Since.LowSeq = 0
			}

			// Populate the parallel arrays of channels and names:
			feeds := make([]<-chan *ChangeEntry, 0, len(channelsSince))

			// Get read lock for late-arriving sequences, to avoid sending the same late arrival in
			// two different changes iterations.  e.g. without the RLock, a late-arriving sequence
			// could be written to channel X during one iteration, and channel Y during another.  Users
			// with access to both channels would see two versions on the feed.

			deferredBackfill = false
			for chanName, vbSeqAddedAt := range channelsSince {
				chanOpts := options

				chanID := channels.NewID(chanName, collectionID)
				// Obtain a SingleChannelCache instance to use for both normal and late feeds.  Required to ensure consistency
				// if cache is evicted during processing
				singleChannelCache, err := col.changeCache().getChannelCache().getSingleChannelCache(ctx, chanID)
				if err != nil {
					base.WarnfCtx(ctx, "Unable to obtain channel cache for %s, terminating feed", base.UD(chanName))
					change := makeErrorEntry("Channel cache unavailable, terminating feed")
					select {
					case output <- &change:
					case <-options.ChangesCtx.Done():
					}
					return
				}

				// Set up late sequence handling first, as we need to roll back the regular feed on error
				// Handles previously skipped sequences prior to options.Since that
				// have arrived in the channel cache since this changes request started.  Only needed for
				// continuous feeds - one-off changes requests only require the standard channel cache.
				if useLateSequenceFeeds {
					lateSequenceFeedHandler := lateSequenceFeeds[chanID]
					if lateSequenceFeedHandler != nil {
						latefeed, err := col.getLateFeed(options.ChangesCtx, lateSequenceFeedHandler, singleChannelCache)
						if err != nil {
							base.WarnfCtx(ctx, "MultiChangesFeed got error reading late sequence feed %q, rolling back channel changes feed to last sent low sequence #%d.", base.UD(chanName), lastSentLowSeq)
							chanOpts.Since.LowSeq = lastSentLowSeq
							if lateFeed := col.newLateSequenceFeed(singleChannelCache); lateFeed != nil {
								lateSequenceFeeds[chanID] = lateFeed
							}
						} else {
							// Mark feed as actively used in this iteration.  Used to remove lateSequenceFeeds
							// when the user loses channel access
							lateSequenceFeedHandler.active = true
							feeds = append(feeds, latefeed)
						}
					} else {
						// Initialize lateSequenceFeeds[name] for next iteration
						if lateFeed := col.newLateSequenceFeed(singleChannelCache); lateFeed != nil {
							lateSequenceFeeds[chanID] = lateFeed
						}
					}
				}

				seqAddedAt := vbSeqAddedAt.Sequence

				// Check whether requires backfill based on changedChannels in this _changes feed
				isNewChannel := false
				if changedChannels != nil {
					isNewChannel, _ = changedChannels[chanID.Name]
				}

				// Check whether requires backfill based on current sequence, seqAddedAt
				// Triggered by handling:
				//   1. options.Since.TriggeredBy == seqAddedAt : We're in the middle of backfill for this channel, based
				//    on the access grant in sequence options.Since.TriggeredBy.  Normally the entire backfill would be done in one
				//    changes feed iteration, but this can be split over multiple iterations when 'limit' is used.
				//   2. options.Since.TriggeredBy == 0 : Not currently doing a backfill
				//   3. options.Since.TriggeredBy != 0 and <= seqAddedAt: We're in the middle of a backfill for another channel, but the backfill for
				//     this channel is still pending.  Initiate the backfill for this channel - will be ordered below in the usual way (iterating over all channels)
				//   4. options.Since.TriggeredBy !=0 and options.Since.TriggeredBy > seqAddedAt: We're in the
				//  middle of a backfill for another channel.  This should issue normal (non-backfill) changes
				//  request with  since= options.Since.TriggeredBy for the non-backfill channel.

				// Backfill required when seqAddedAt is before current sequence
				backfillRequired := seqAddedAt > 1 && options.Since.Before(SequenceID{Seq: seqAddedAt}) && seqAddedAt <= currentCachedSequence
				if seqAddedAt > currentCachedSequence {
					base.DebugfCtx(ctx, base.KeyChanges, "Grant for channel [%s] is after the current sequence - skipped for this iteration.  Grant:[%d] Current:[%d] %s", base.UD(chanName), seqAddedAt, currentCachedSequence, base.UD(to))
					deferredBackfill = true
					continue
				}

				// Ensure backfill isn't already in progress for this seqAddedAt
				backfillPending := options.Since.TriggeredBy == 0 || options.Since.TriggeredBy < seqAddedAt

				backfillInOtherChannel := options.Since.TriggeredBy != 0 && options.Since.TriggeredBy > seqAddedAt

				if isNewChannel || (backfillRequired && backfillPending) {
					// Newly added channel so initiate backfill:
					chanOpts.Since = SequenceID{Seq: 0, TriggeredBy: seqAddedAt}
				} else if backfillInOtherChannel {
					chanOpts.Since = SequenceID{Seq: options.Since.TriggeredBy - 1}
				}

				feed := col.changesFeed(ctx, singleChannelCache, chanOpts, to)
				feeds = append(feeds, feed)

			}
			// If the user object has changed, create a special pseudo-feed for it:
			if col.user != nil {
				feeds = col.appendUserFeed(feeds, options)
			}

			if options.Revocations && col.user != nil && !options.ActiveOnly {
				channelsToRevoke := col.user.RevokedCollectionChannels(col.ScopeName, col.Name, options.Since.Seq, options.Since.LowSeq, options.Since.TriggeredBy)
				for channel, revokedSeq := range channelsToRevoke {
					revocationSinceSeq := options.Since.SafeSequence()
					revokeFrom := uint64(0)

					// If we have a triggeredBy sequence:
					// If channel access was lost at the triggeredBy sequence then replication may have been interrupted
					// so we need to roll back one sequence to re-send the values with that previous triggeredBy as we
					// cannot be sure that they were all sent. However, we can get changes from triggeredBy rather than
					// 0 when finding docs to revoke.
					// If channel access was after the triggeredBy then we can just use the triggeredBy and need to
					// check for docs to revoke since 0.
					if options.Since.TriggeredBy != 0 {
						if revokedSeq == options.Since.TriggeredBy {
							revocationSinceSeq = options.Since.TriggeredBy - 1
							revokeFrom = options.Since.Seq
						}
						if revokedSeq > options.Since.TriggeredBy {
							revocationSinceSeq = options.Since.TriggeredBy
							revokeFrom = 0
						}
					}

					feed := col.buildRevokedFeed(ctx, channels.NewID(channel, collectionID), options, revokedSeq, revocationSinceSeq, revokeFrom, to)
					feeds = append(feeds, feed)
				}
			}

			current := make([]*ChangeEntry, len(feeds))

			// This loop reads the available entries from all the feeds in parallel, merges them,
			// and writes them to the output channel:
			var sentSomething bool

			// postStableSeqsFound tracks whether we hit any sequences later than the stable sequence.  In this scenario the user
			// may not get another wait notification, so we bypass wait loop processing.
			postStableSeqsFound := false
			for {
				// Read more entries to fill up the current[] array:
				for i, cur := range current {
					if cur == nil && feeds[i] != nil {
						var ok bool
						current[i], ok = <-feeds[i]
						if !ok {
							feeds[i] = nil
						} else {
							// On feed error, send the error and exit changes processing
							if current[i].Err == base.ErrChannelFeed {
								base.WarnfCtx(ctx, "MultiChangesFeed got error reading changes feed: %v", current[i].Err)
								select {
								case <-options.ChangesCtx.Done():
								case output <- current[i]:
								}
								return
							}
						}
					}
				}

				// Find the current entry with the minimum sequence:
				minSeq := MaxSequenceID
				var minEntry *ChangeEntry
				for _, cur := range current {
					if cur != nil && cur.Seq.Before(minSeq) {
						minSeq = cur.Seq
						minEntry = cur
					}
				}

				if minEntry == nil {
					break // Exit the loop when there are no more entries
				}

				// Clear the current entries for the sequence just sent:
				if minEntry.Removed != nil {
					minEntry.allRemoved = true
				}
				for i, cur := range current {
					if cur != nil && cur.Seq == minSeq {
						current[i] = nil
						// Track whether this is a removal from all user's channels
						if cur.Removed == nil && minEntry.allRemoved == true {
							minEntry.allRemoved = false
						}
						// Also concatenate the matching entries' Removed arrays:
						if cur != minEntry && cur.Removed != nil {
							if minEntry.Removed == nil {
								minEntry.Removed = cur.Removed
							} else {
								minEntry.Removed = minEntry.Removed.Union(cur.Removed)
							}
						}
					}
				}

				if options.ActiveOnly {
					if minEntry.Deleted || minEntry.allRemoved {
						continue
					}
				}

				// Don't send any entries later than the cached sequence at the start of this iteration, unless they are part of a revocation triggered
				// at or before the cached sequence
				isValidRevocation := minEntry.Revoked == true && minEntry.Seq.TriggeredBy <= currentCachedSequence
				if currentCachedSequence < minEntry.Seq.Seq && !isValidRevocation {
					base.DebugfCtx(ctx, base.KeyChanges, "Found sequence later than stable sequence: stable:[%d] entry:[%d] (%s)", currentCachedSequence, minEntry.Seq.Seq, base.UD(minEntry.ID))
					postStableSeqsFound = true
					continue
				}

				// Update options.Since for use in the next outer loop iteration.  Only update
				// when minSeq is greater than the previous options.Since value - we don't want to
				// roll back the Since value when a late sequence is processed.
				if options.Since.Before(minSeq) {
					options.Since = minSeq
				}

				// Add the doc body or the conflicting rev IDs, if those options are set:
				if options.IncludeDocs || options.Conflicts {
					col.addDocToChangeEntry(ctx, minEntry, options)
				}

				// Update the low sequence on the entry we're going to send
				// NOTE: if 0, the low seq part of compound sequence gets removed
				minEntry.Seq.LowSeq = lowSequence
				lastSentLowSeq = lowSequence

				// Send the entry, and repeat the loop:
				base.DebugfCtx(ctx, base.KeyChanges, "MultiChangesFeed sending %+v %s", base.UD(minEntry), base.UD(to))

				if options.ChangesCtx.Err() != nil {
					return
				}
				select {
				case <-options.ChangesCtx.Done():
					return
				case output <- minEntry:
				}
				sentSomething = true

				// Stop when we hit the limit (if any):
				if options.Limit > 0 {
					options.Limit--
					if options.Limit == 0 {
						break outer
					}
				}
			}

			// Check whether non-continuous changes feeds that aren't waiting to reach requestPlus sequence can exit
			if !options.Continuous && currentCachedSequence >= options.RequestPlusSeq {
				// If non-longpoll, or longpoll has sent something, can exit
				if !options.Wait || sentSomething {
					break
				}
			}

			// For longpoll requests that didn't send any results, reset low sequence to the original since value,
			// as the system low sequence may change before the longpoll request wakes up, and longpoll feeds don't
			// use lateSequenceFeeds.
			if !useLateSequenceFeeds {
				options.Since.LowSeq = requestLowSeq
			}

			// If nothing found, and in wait mode: wait for the db to change, then run again.
			// First notify the reader that we're waiting by sending a nil.
			base.DebugfCtx(ctx, base.KeyChanges, "MultiChangesFeed waiting... %s", base.UD(to))
			select {
			case <-options.ChangesCtx.Done():
				return
			case output <- nil:
			}

			// If this is an initial replication using CBL 2.x (active only), flip activeOnly now the client has caught up.
			if options.clientType == clientTypeCBL2 && options.ActiveOnly {
				base.DebugfCtx(ctx, base.KeyChanges, "%v MultiChangesFeed initial replication caught up - setting ActiveOnly to false... %s", options.Since, base.UD(to))
				options.ActiveOnly = false
			}

		waitForChanges:
			for {
				col.dbStats().CBLReplicationPull().NumPullReplTotalCaughtUp.Add(1)
				// If we're in a deferred Backfill, the user may not get notification when the cache catches up to the backfill (e.g. when the granting doc isn't
				// visible to the user), and so ChangeWaiter.Wait() would block until the next user-visible doc arrives.  Use a hardcoded wait instead
				// Similar handling for when we see sequences later than the stable sequence.
				if deferredBackfill || postStableSeqsFound {
					cancelled := col.waitForCacheUpdate(options.ChangesCtx, currentCachedSequence)
					if cancelled {
						return
					}
					break waitForChanges
				}

				col.dbStats().CBLReplicationPull().NumPullReplCaughtUp.Add(1)
				waitResponse := changeWaiter.Wait(ctx)
				col.dbStats().CBLReplicationPull().NumPullReplCaughtUp.Add(-1)

				if options.ChangesCtx.Err() != nil {
					return
				} else if waitResponse == WaiterClosed {
					break outer
				} else if waitResponse == WaiterHasChanges {
					break waitForChanges
				}
			}
			// Update the current max cached sequence for the next changes iteration
			currentCachedSequence = col.changeCache().getChannelCache().GetHighCacheSequence()

			// Check whether user channel access has changed while waiting:
			var err error
			userChanged, userCounter, changedChannels, err = col.checkForUserUpdates(ctx, userCounter, changeWaiter, options.Continuous)
			if err != nil {
				change := makeErrorEntry("User not found during reload - terminating changes feed")
				base.DebugfCtx(ctx, base.KeyChanges, "User not found during reload - terminating changes feed with entry %+v", base.UD(change))
				select {
				case <-options.ChangesCtx.Done():
				case output <- &change:
				}
				return
			}
			if userChanged && col.user != nil {
				newChannelsSince, _ := col.user.FilterToAvailableCollectionChannels(col.ScopeName, col.Name, chans)
				changedChannels = newChannelsSince.CompareKeys(channelsSince)

				if len(changedChannels) > 0 {
					col.activeChannels().UpdateChanged(ctx, collectionID, changedChannels)
				}
				channelsSince = newChannelsSince
			}

			// Clean up inactive lateSequenceFeeds (because user has lost access to the channel)
			for channel, lateFeed := range lateSequenceFeeds {
				if !lateFeed.active {
					col.closeLateFeed(ctx, lateFeed)
					delete(lateSequenceFeeds, channel)
				} else {
					lateFeed.active = false
				}
			}

		}
	}()
	return output, nil
}

func (col *DatabaseCollectionWithUser) waitForCacheUpdate(ctx context.Context, currentCachedSequence uint64) (cancelled bool) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for retry := 0; retry <= 50; retry++ {
		select {
		// Check if feed has been terminated regardless of if any changes have happened
		case <-ctx.Done():
			return true
		case <-ticker.C:
			if col.changeCache().getChannelCache().GetHighCacheSequence() != currentCachedSequence {
				return false
			}
		}
	}
	return false
}

// Returns the set of cached log entries for a given channel
func (c *DatabaseCollection) GetChangeLog(ctx context.Context, channel channels.ID, afterSeq uint64) (entries []*LogEntry, err error) {
	return c.changeCache().getChannelCache().GetCachedChanges(ctx, channel)
}

// WaitForSequenceNotSkipped blocks until the given sequence has been received or skipped by the change cache.
func (c *DatabaseCollection) WaitForSequence(ctx context.Context, sequence uint64) (err error) {
	base.DebugfCtx(ctx, base.KeyChanges, "Waiting for sequence: %d", sequence)
	return c.changeCache().waitForSequence(ctx, sequence, base.DefaultWaitForSequence)
}

// WaitForSequenceNotSkipped blocks until the given sequence has been received by the change cache without being skipped.
func (c *DatabaseCollection) WaitForSequenceNotSkipped(ctx context.Context, sequence uint64) (err error) {
	base.DebugfCtx(ctx, base.KeyChanges, "Waiting for sequence: %d", sequence)
	return c.changeCache().waitForSequenceNotSkipped(ctx, sequence, base.DefaultWaitForSequence)
}

// WaitForPendingChanges blocks until the change-cache has caught up with the latest writes to the database.
func (c *DatabaseCollection) WaitForPendingChanges(ctx context.Context) error {
	lastSequence, err := c.LastSequence(ctx)
	if err != nil {
		return err
	}
	base.DebugfCtx(ctx, base.KeyChanges, "Waiting for sequence: %d", lastSequence)
	return c.changeCache().waitForSequence(ctx, lastSequence, base.DefaultWaitForSequence)
}

// Late Sequence Feed
// Manages the changes feed interaction with a channels cache's set of late-arriving entries
type lateSequenceFeed struct {
	active           bool        // Whether the changes feed is still serving the channel this feed is associated with
	lastSequence     uint64      // Last late sequence processed on the feed
	channel          channels.ID // Channel
	lateSequenceUUID uuid.UUID   // Ensures cache doesn't change underneath us
}

// Returns a lateSequenceFeed for the channel, used to find late-arriving (previously
// skipped) sequences that have been sent to the channel cache.  The lateSequenceFeed stores the last (late)
// sequence seen by this particular _changes feed to support continuous changes.
func (db *DatabaseCollectionWithUser) newLateSequenceFeed(singleChannelCache SingleChannelCache) *lateSequenceFeed {

	if !singleChannelCache.SupportsLateFeed() {
		return nil
	}

	lsf := &lateSequenceFeed{
		active:           true,
		lateSequenceUUID: singleChannelCache.LateSequenceUUID(),
		channel:          singleChannelCache.ChannelID(),
		lastSequence:     singleChannelCache.RegisterLateSequenceClient(),
	}
	return lsf
}

// Feed to process late sequences for the channel.  Updates lastSequence as it works the feed.  Error indicates
// previous position in late sequence feed isn't available, and caller should reset to low sequence.
func (db *DatabaseCollectionWithUser) getLateFeed(ctx context.Context, feedHandler *lateSequenceFeed, singleChannelCache SingleChannelCache) (<-chan *ChangeEntry, error) {

	if !singleChannelCache.SupportsLateFeed() {
		return nil, errors.New("Cache doesn't support late feeds")
	}
	// If the associated cache instance for this feedHandler doesn't match SingleChannelCache, it means the channel cache
	// has been evicted/recreated, and the current feedHandler is no longer valid
	if feedHandler.lateSequenceUUID != singleChannelCache.LateSequenceUUID() {
		return nil, errors.New("Cache/handler mismatch")
	}

	// Use LogPriorityQueue for late entries, to utilize the existing Len/Less/Swap methods on LogPriorityQueue for sort
	var logs LogPriorityQueue
	logs, lastSequence, err := singleChannelCache.GetLateSequencesSince(feedHandler.lastSequence)
	if err != nil {
		return nil, err
	}
	if logs == nil || len(logs) == 0 {
		// There are no late entries newer than lastSequence
		feed := make(chan *ChangeEntry)
		close(feed)
		return feed, nil
	}

	// Sort late sequences, to ensure duplicates aren't sent in a single continuous _changes iteration when multiple
	// channels have late arrivals
	sort.Sort(logs)

	feed := make(chan *ChangeEntry, 1)
	go func() {
		defer close(feed)
		// Write each log entry to the 'feed' channel in turn:
		for _, logEntry := range logs {
			// We don't need TriggeredBy handling here, because when backfilling from a
			// channel in response to a user being added to the channel, we don't need to worry about
			// late arrived sequences
			seqID := SequenceID{
				Seq: logEntry.Sequence,
			}
			change := makeChangeEntry(logEntry, seqID, singleChannelCache.ChannelID())
			select {
			case <-ctx.Done():
				return

			case feed <- &change:
			}
		}
	}()

	feedHandler.lastSequence = lastSequence
	return feed, nil
}

// Closes a single late sequence feed.
func (db *DatabaseCollectionWithUser) closeLateFeed(ctx context.Context, feedHandler *lateSequenceFeed) {
	singleChannelCache, err := db.changeCache().getChannelCache().getSingleChannelCache(ctx, feedHandler.channel)
	if err != nil || !singleChannelCache.SupportsLateFeed() {
		return
	}
	if singleChannelCache.LateSequenceUUID() == feedHandler.lateSequenceUUID {
		singleChannelCache.ReleaseLateSequenceClient(feedHandler.lastSequence)
	}
}

// Closes set of feeds.  Invoked on changes termination
func (db *DatabaseCollectionWithUser) closeLateFeeds(ctx context.Context, feeds map[channels.ID]*lateSequenceFeed) {
	for _, feed := range feeds {
		db.closeLateFeed(ctx, feed)
	}
}

// Generate the changes for a specific list of doc ID's, only documents accessible to the user will generate
// results. Only supports non-continuous changes, closes buffered channel before returning.
func (db *DatabaseCollectionWithUser) DocIDChangesFeed(ctx context.Context, userChannels base.Set, explicitDocIds []string, options ChangesOptions) (<-chan *ChangeEntry, error) {

	// Subroutine that creates a response row for a document:
	output := make(chan *ChangeEntry, len(explicitDocIds))
	rowMap := make(map[uint64]*ChangeEntry)

	// Sort results by sequence
	var sequences base.SortedUint64Slice
	for _, docID := range explicitDocIds {
		row := createChangesEntry(ctx, docID, db, options)
		if row != nil {
			rowMap[row.Seq.Seq] = row
			sequences = append(sequences, row.Seq.Seq)
		}
	}

	// Send ChangeEntries sorted by sequenceID
	sequences.Sort()
	for _, sequence := range sequences {
		output <- rowMap[sequence]
		if options.Limit > 0 {
			options.Limit--
			if options.Limit == 0 {
				break
			}
		}
	}

	close(output)

	return output, nil
}

// createChangesEntry is used when creating a doc ID filtered changes feed
func createChangesEntry(ctx context.Context, docid string, db *DatabaseCollectionWithUser, options ChangesOptions) *ChangeEntry {
	row := &ChangeEntry{ID: docid}

	populatedDoc, err := db.GetDocument(ctx, docid, DocUnmarshalSync)
	if err != nil {
		base.InfofCtx(ctx, base.KeyChanges, "Unable to get changes for docID %v, caused by %v", base.UD(docid), err)
		return nil
	}

	if populatedDoc.Sequence <= options.Since.Seq {
		return nil
	}

	changes := make([]ChangeRev, 1)
	changes[0] = ChangeRev{"rev": populatedDoc.CurrentRev}
	row.Changes = changes
	row.Deleted = populatedDoc.Deleted
	row.Seq = SequenceID{Seq: populatedDoc.Sequence}
	row.SetBranched((populatedDoc.Flags & channels.Branched) != 0)

	var removedChannels []string

	userCanSeeDocChannel := false

	// If admin, or the user has the star channel, include it in the results
	if db.user == nil || db.user.CollectionChannels(db.ScopeName, db.Name).Contains(channels.UserStarChannel) {
		userCanSeeDocChannel = true
	} else if len(populatedDoc.Channels) > 0 {
		// Iterate over the doc's channels, including in the results:
		//   - the active revision is in a channel the user can see (removal==nil)
		//   - the doc has been removed from a user's channel later the requested since value (removal.Seq > options.Since.Seq).  In this case, we need to send removal:true changes entry
		for channel, removal := range populatedDoc.Channels {
			if db.user.CanSeeCollectionChannel(db.ScopeName, db.Name, channel) && (removal == nil || removal.Seq > options.Since.Seq) {
				userCanSeeDocChannel = true
				// If removal, update removed channels and deleted flag.
				if removal != nil {
					removedChannels = append(removedChannels, channel)
					if removal.Deleted {
						row.Deleted = true
					}
				}
			}
		}
	}

	if !userCanSeeDocChannel {
		return nil
	}

	row.Removed = base.SetFromArray(removedChannels)
	if options.IncludeDocs || options.Conflicts {
		db.AddDocInstanceToChangeEntry(ctx, row, populatedDoc, options)
	}

	return row
}

func (options ChangesOptions) String() string {
	return fmt.Sprintf(
		`{Since: %s, Limit: %d, Conflicts: %t, IncludeDocs: %t, Wait: %t, Continuous: %t, HeartbeatMs: %d, TimeoutMs: %d, ActiveOnly: %t, Revocations: %t, RequestPlusSeq: %d}`,
		options.Since,
		options.Limit,
		options.Conflicts,
		options.IncludeDocs,
		options.Wait,
		options.Continuous,
		options.HeartbeatMs,
		options.TimeoutMs,
		options.ActiveOnly,
		options.Revocations,
		options.RequestPlusSeq,
	)
}

// Used by BLIP connections for changes.  Supports both one-shot and continuous changes. Returns an error in the case that the feed does not start up, or there is a fatal error in the feed. The caller is responsible for closing the connection, no more changes will be generated. forceClose will be true if connection was terminated underneath the changes feed.
func generateBlipSyncChanges(ctx context.Context, database *DatabaseCollectionWithUser, inChannels base.Set, options ChangesOptions, docIDFilter []string, send func([]*ChangeEntry) error) (forceClose bool, err error) {

	// Store one-shot here to protect
	isOneShot := !options.Continuous
	err, forceClose = GenerateChanges(ctx, database, inChannels, options, docIDFilter, send)

	if _, ok := err.(*ChangesSendErr); ok {
		// If there was already an error in a send function, do not send last one shot changes message, since it probably will not work anyway.
		return forceClose, err // error is probably because the client closed the connection
	}

	// For one-shot changes, invoke the callback w/ nil to trigger the 'caught up' changes message.  (For continuous changes, this
	// is done by MultiChangesFeed prior to going into Wait mode)
	if isOneShot {
		_ = send(nil)
	}
	return forceClose, err
}

type ChangesSendErr struct{ error }

// Shell of the continuous changes feed -- calls out to a `send` function to deliver the change.
// This is called from BLIP connections as well as HTTP handlers, which is why this is not a
// method on `handler`.
func GenerateChanges(ctx context.Context, database *DatabaseCollectionWithUser, inChannels base.Set, options ChangesOptions, docIDFilter []string, send func([]*ChangeEntry) error) (err error, forceClose bool) {
	// Set up heartbeat/timeout
	var timeoutInterval time.Duration
	var timer *time.Timer
	var heartbeat <-chan time.Time
	if options.HeartbeatMs > 0 {
		ticker := time.NewTicker(time.Duration(options.HeartbeatMs) * time.Millisecond)
		defer ticker.Stop()
		heartbeat = ticker.C
	} else if options.TimeoutMs > 0 {
		timeoutInterval = time.Duration(options.TimeoutMs) * time.Millisecond
		defer func() {
			if timer != nil {
				timer.Stop()
			}
		}()
	}

	if options.Continuous {
		options.Wait = true // we want the feed channel to wait for changes
	}

	if !options.Since.IsNonZero() {
		database.dbStats().CBLReplicationPull().NumPullReplSinceZero.Add(1)
	}

	var lastSeq SequenceID
	var feed <-chan *ChangeEntry
	var timeout <-chan time.Time
	var feedErr error

	// feedStarted identifies whether at least one MultiChangesFeed has been started.  Used to identify when a one-shot changes is done.
	feedStarted := false

loop:
	for {
		// If the feed has already been started once and closed, and this isn't a continuous
		// replication, we're done.
		if feedStarted && feed == nil && !options.Continuous {
			break loop
		}

		if feed == nil {
			// Refresh the feed of all current changes:
			if lastSeq.IsNonZero() { // start after end of last feed
				options.Since = lastSeq
			}
			if database.IsClosed() {
				forceClose = true
				break loop
			}
			if len(docIDFilter) > 0 {
				feed, feedErr = database.DocIDChangesFeed(ctx, inChannels, docIDFilter, options)
			} else {
				feed, feedErr = database.MultiChangesFeed(ctx, inChannels, options)
			}
			if feedErr != nil || feed == nil {
				return feedErr, forceClose
			}
			feedStarted = true
		}

		if timeoutInterval > 0 && timer == nil {
			// Timeout resets after every change is sent
			timer = time.NewTimer(timeoutInterval)
			timeout = timer.C
		}

		var sendErr error
		// Wait for either a new change, a heartbeat, or a timeout:
		select {
		case entry, ok := <-feed:
			if !ok {
				feed = nil
			} else if entry == nil {
				sendErr = send(nil)
			} else if entry.Err != nil {
				feedErr = entry.Err
				break loop // error returned by feed - end changes
			} else {
				entries := []*ChangeEntry{entry}
				waiting := false
				// Batch up as many entries as we can without waiting:
			collect:
				for len(entries) < 20 {
					select {
					case entry, ok = <-feed:
						if !ok {
							feed = nil
							break collect
						} else if entry == nil {
							waiting = true
							break collect
						} else if entry.Err != nil {
							feedErr = entry.Err
							break loop // error returned by feed - end changes
						}
						entries = append(entries, entry)
					default:
						break collect
					}
				}
				base.TracefCtx(ctx, base.KeyChanges, "sending %d change(s)", len(entries))
				sendErr = send(entries)

				if sendErr == nil && waiting {
					sendErr = send(nil)
				}

				lastSeq = entries[len(entries)-1].Seq
				if options.Limit > 0 {
					if len(entries) >= options.Limit {
						forceClose = true
						break loop
					}
					options.Limit -= len(entries)
				}
			}
			// Reset the timeout after sending an entry:
			if timer != nil {
				timer.Stop()
				timer = nil
			}
		case <-heartbeat:
			sendErr = send(nil)
			base.DebugfCtx(ctx, base.KeyChanges, "heartbeat written to _changes feed for request received")
		case <-timeout:
			forceClose = true
			break loop
		case <-database.exitChanges():
			forceClose = true
			break loop
		case <-options.ChangesCtx.Done():
			forceClose = true
			break loop
		}
		if sendErr != nil {
			forceClose = true
			return &ChangesSendErr{sendErr}, forceClose
		}
	}

	// if the ChangesCtx is done, the connection was force closed. This could actually happen and send a ChangeEntry.Err. Instead of checking each place in this function, set the forceClose flag here.
	if options.ChangesCtx.Err() != nil {
		forceClose = true
	}

	return feedErr, forceClose
}
