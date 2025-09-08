/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-blip"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// handlersByProfile defines the routes for each message profile (verb) of an incoming request to the function that handles it.
var handlersByProfile = map[string]blipHandlerFunc{
	MessageGetCheckpoint:   collectionBlipHandler((*blipHandler).handleGetCheckpoint),
	MessageSetCheckpoint:   collectionBlipHandler((*blipHandler).handleSetCheckpoint),
	MessageSubChanges:      userBlipHandler(collectionBlipHandler((*blipHandler).handleSubChanges)),
	MessageUnsubChanges:    userBlipHandler(collectionBlipHandler((*blipHandler).handleUnsubChanges)),
	MessageChanges:         userBlipHandler(collectionBlipHandler((*blipHandler).handleChanges)),
	MessageRev:             userBlipHandler(collectionBlipHandler((*blipHandler).handleRev)),
	MessageNoRev:           collectionBlipHandler((*blipHandler).handleNoRev),
	MessageGetAttachment:   userBlipHandler(collectionBlipHandler((*blipHandler).handleGetAttachment)),
	MessageProveAttachment: userBlipHandler(collectionBlipHandler((*blipHandler).handleProveAttachment)),
	MessageProposeChanges:  collectionBlipHandler((*blipHandler).handleProposeChanges),
	MessageGetRev:          userBlipHandler(collectionBlipHandler((*blipHandler).handleGetRev)),
	MessagePutRev:          userBlipHandler(collectionBlipHandler((*blipHandler).handlePutRev)),

	MessageGetCollections: userBlipHandler((*blipHandler).handleGetCollections),
}

var kConnectedClientHandlersByProfile = map[string]blipHandlerFunc{
	MessageFunction: userBlipHandler((*blipHandler).handleFunction),
}

// Replication throttling
const (
	// DefaultMaxConcurrentChangesBatches is the maximum number of in-flight changes batches a client is allowed to send concurrently without being throttled.
	DefaultMaxConcurrentChangesBatches = 2
	// DefaultMaxConcurrentRevs is the maximum number of in-flight revisions a client is allowed to send or receive concurrently without being throttled.
	DefaultMaxConcurrentRevs = 5
)

type blipHandler struct {
	*BlipSyncContext
	db            *Database                   // Handler-specific copy of the BlipSyncContext's blipContextDb
	collection    *DatabaseCollectionWithUser // Handler-specific copy of the BlipSyncContext's collection specific DB
	collectionCtx *blipSyncCollectionContext  // Sync-specific data for this collection
	collectionIdx *int                        // index into BlipSyncContext.collectionMapping for the collection
	loggingCtx    context.Context             // inherited from BlipSyncContext.loggingCtx with additional handler-only information (like keyspace)
	serialNumber  uint64                      // This blip handler's serial number to differentiate logs w/ other handlers
}

func newBlipHandler(ctx context.Context, bc *BlipSyncContext, db *Database, serialNumber uint64) *blipHandler {
	return &blipHandler{
		BlipSyncContext: bc,
		db:              db,
		loggingCtx:      ctx,
		serialNumber:    serialNumber,
	}
}

// BlipSyncContextClientType represents whether to replicate to another Sync Gateway or Couchbase Lite
type BLIPSyncContextClientType string

const (
	BLIPCorrelationIDResponseHeader = "X-Correlation-ID"

	BLIPSyncClientTypeQueryParam = "client"

	BLIPClientTypeCBL2 BLIPSyncContextClientType = "cbl2"
	BLIPClientTypeSGR2 BLIPSyncContextClientType = "sgr2"
)

type blipHandlerFunc func(*blipHandler, *blip.Message) error

var (
	// CBLReconnectErrorCode is the error code that CBL will use to trigger a reconnect
	CBLReconnectErrorCode = http.StatusServiceUnavailable

	ErrUseProposeChanges = base.HTTPErrorf(http.StatusConflict, "Use 'proposeChanges' instead")

	// ErrDatabaseWentAway is returned when a replication tries to use a closed database.
	// HTTP 503 tells the client to reconnect and try again.
	ErrDatabaseWentAway = base.HTTPErrorf(http.StatusServiceUnavailable, "Sync Gateway database went away - asking client to reconnect")

	// ErrAttachmentNotFound is returned when the attachment that is asked by one of the peers does
	// not exist in another to prove that it has the attachment during Inter-Sync Gateway Replication.
	ErrAttachmentNotFound = base.HTTPErrorf(http.StatusNotFound, "attachment not found")
)

// userBlipHandler wraps another blip handler with code that reloads the user object when the user
// or the user's roles have changed, to make sure that the replication has the latest channel access grants.
// Uses a userChangeWaiter to detect changes to the user or roles.  Note that in the case of a pushed document
// triggering a user access change, this happens at write time (via MarkPrincipalsChanged), and doesn't
// depend on the userChangeWaiter.
func userBlipHandler(next blipHandlerFunc) blipHandlerFunc {
	return func(bh *blipHandler, bm *blip.Message) error {

		// Reload user if it has changed
		if err := bh.refreshUser(); err != nil {
			return err
		}
		// Call down to the underlying handler and return it's value
		return next(bh, bm)
	}
}

func (bh *blipHandler) refreshUser() error {

	bc := bh.BlipSyncContext
	if bc.userName != "" {
		// Check whether user needs to be refreshed
		bc.dbUserLock.Lock()
		defer bc.dbUserLock.Unlock()
		userChanged := bc.userChangeWaiter.RefreshUserCount()

		// If changed, refresh the user and db while holding the lock
		if userChanged {
			// Refresh the BlipSyncContext database
			err := bc.blipContextDb.ReloadUser(bh.loggingCtx)
			if err != nil {
				return base.NewHTTPError(CBLReconnectErrorCode, err.Error())
			}
			newUser := bc.blipContextDb.User()
			newUser.InitializeRoles()
			bc.userChangeWaiter.RefreshUserKeys(newUser, bc.blipContextDb.MetadataKeys)
			// refresh the handler's database with the new BlipSyncContext database
			bh.db = bh._copyContextDatabase()
			if bh.collection != nil {
				bh.collection = &DatabaseCollectionWithUser{
					DatabaseCollection: bh.collection.DatabaseCollection,
					user:               bh.db.User(),
				}
			}
		}
	}
	return nil
}

// collectionBlipHandler wraps another blip handler to specify a collection
func collectionBlipHandler(next blipHandlerFunc) blipHandlerFunc {
	return func(bh *blipHandler, bm *blip.Message) error {
		collectionIndexStr, ok := bm.Properties[BlipCollection]
		if !ok {
			if !bh.db.HasDefaultCollection() {
				return base.HTTPErrorf(http.StatusBadRequest, "Collection property not specified and default collection is not configured for this database")
			}
			if bh.collections.hasNamedCollections() {
				return base.HTTPErrorf(http.StatusBadRequest, "GetCollections already occurred, subsequent messages need a Collection property")
			}
			var err error
			bh.collection, err = bh.db.GetDefaultDatabaseCollectionWithUser()
			if err != nil {
				return err
			}
			bh.collectionCtx, err = bh.collections.get(nil)
			if err != nil {
				bh.collections.setNonCollectionAware(newBlipSyncCollectionContext(bh.loggingCtx, bh.collection.DatabaseCollection))
				bh.collectionCtx, _ = bh.collections.get(nil)
			}
			bh.loggingCtx = bh.collection.AddCollectionContext(bh.BlipSyncContext.loggingCtx)
			return next(bh, bm)
		}
		if !bh.collections.hasNamedCollections() {
			return base.HTTPErrorf(http.StatusBadRequest, "Passing collection requires calling %s first", MessageGetCollections)
		}

		collectionIndex, err := strconv.Atoi(collectionIndexStr)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "collection property needs to be an int, was %q", collectionIndexStr)
		}

		bh.collectionIdx = &collectionIndex
		bh.collectionCtx, err = bh.collections.get(&collectionIndex)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "%s", err)
		}
		bh.collection = &DatabaseCollectionWithUser{
			DatabaseCollection: bh.collectionCtx.dbCollection,
			user:               bh.db.user,
		}
		bh.loggingCtx = bh.collection.AddCollectionContext(bh.BlipSyncContext.loggingCtx)
		// Call down to the underlying handler and return it's value
		return next(bh, bm)
	}
}

// ////// CHECKPOINTS

// Received a "getCheckpoint" request
func (bh *blipHandler) handleGetCheckpoint(rq *blip.Message) error {

	client := rq.Properties[BlipClient]
	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("Client:%s", client))

	response := rq.Response()
	if response == nil {
		return nil
	}

	value, err := bh.collection.GetSpecial(DocTypeLocal, CheckpointDocIDPrefix+client)
	if err != nil {
		return err
	}
	if value == nil {
		return base.NewHTTPError(http.StatusNotFound, http.StatusText(http.StatusNotFound))
	}
	response.Properties[GetCheckpointResponseRev] = value[BodyRev].(string)
	delete(value, BodyRev)
	delete(value, BodyId)
	// TODO: Marshaling here when we could use raw bytes all the way from the bucket
	_ = response.SetJSONBody(value)
	return nil
}

// Received a "setCheckpoint" request
func (bh *blipHandler) handleSetCheckpoint(rq *blip.Message) error {

	checkpointMessage := SetCheckpointMessage{rq}
	bh.logEndpointEntry(rq.Profile(), checkpointMessage.String())

	var checkpoint Body
	if err := checkpointMessage.ReadJSONBody(&checkpoint); err != nil {
		return err
	}
	if revID := checkpointMessage.rev(); revID != "" {
		checkpoint[BodyRev] = revID
	}
	revID, _, err := bh.collection.PutSpecial(DocTypeLocal, CheckpointDocIDPrefix+checkpointMessage.client(), checkpoint)
	if err != nil {
		return err
	}

	checkpointResponse := SetCheckpointResponse{checkpointMessage.Response()}
	checkpointResponse.setRev(revID)

	return nil
}

// ////// CHANGES

// Received a "subChanges" subscription request
func (bh *blipHandler) handleSubChanges(rq *blip.Message) error {
	latestSeq := func() (SequenceID, error) {
		seq, err := bh.collection.LastSequence(bh.loggingCtx)
		return SequenceID{Seq: seq}, err
	}
	subChangesParams, err := NewSubChangesParams(bh.loggingCtx, rq, latestSeq, bh.db.Options.ChangesRequestPlus)
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid subChanges parameters")
	}

	// Ensure that only _one_ subChanges subscription can be open on this blip connection at any given time.  SG #3222.
	collectionCtx := bh.collectionCtx
	collectionCtx.changesCtxLock.Lock()
	defer collectionCtx.changesCtxLock.Unlock()
	if !collectionCtx.activeSubChanges.CASRetry(false, true) {
		collectionStr := "default collection"
		if bh.collectionIdx != nil {
			collectionStr = fmt.Sprintf("collection %d", *bh.collectionIdx)
		}
		return fmt.Errorf("blipHandler for %s already has an outstanding subChanges. Cannot open another one", collectionStr)
	}

	// Create ctx if it has been cancelled
	if collectionCtx.changesCtx.Err() != nil {
		collectionCtx.changesCtx, collectionCtx.changesCtxCancel = context.WithCancel(bh.loggingCtx)
	}

	if len(subChangesParams.docIDs()) > 0 && subChangesParams.continuous() {
		return base.HTTPErrorf(http.StatusBadRequest, "DocIDs filter not supported for continuous subChanges")
	}

	bh.logEndpointEntry(rq.Profile(), subChangesParams.String())

	var channels base.Set
	if filter := subChangesParams.filter(); filter == base.ByChannelFilter {
		var err error

		channels, err = subChangesParams.channelsExpandedSet()
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "%s", err)
		} else if len(channels) == 0 {
			return base.HTTPErrorf(http.StatusBadRequest, "Empty channel list")
		}
	} else if filter != "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Unknown filter; try sync_gateway/bychannel")
	}

	collectionCtx.channels = channels

	clientType := clientTypeCBL2
	if rq.Properties["client_sgr2"] == trueProperty {
		clientType = clientTypeSGR2
	}

	continuous := subChangesParams.continuous()

	requestPlusSeq := uint64(0)
	// If non-continuous, check whether requestPlus handling is set for request or via database config
	if continuous == false {
		useRequestPlus := subChangesParams.requestPlus()
		if useRequestPlus {
			seq, requestPlusErr := bh.db.GetRequestPlusSequence()
			if requestPlusErr != nil {
				return base.HTTPErrorf(http.StatusServiceUnavailable, "Unable to retrieve current sequence for requestPlus=true: %v", requestPlusErr)
			}
			requestPlusSeq = seq
		}
	}

	bh.collectionCtx.sendReplacementRevs = subChangesParams.sendReplacementRevs()

	// Start asynchronous changes goroutine
	go func() {
		// Pull replication stats by type
		if continuous {
			bh.replicationStats.SubChangesContinuousActive.Add(1)
			defer bh.replicationStats.SubChangesContinuousActive.Add(-1)
			bh.replicationStats.SubChangesContinuousTotal.Add(1)
		} else {
			bh.replicationStats.SubChangesOneShotActive.Add(1)
			defer bh.replicationStats.SubChangesOneShotActive.Add(-1)
			bh.replicationStats.SubChangesOneShotTotal.Add(1)
		}

		defer func() {
			collectionCtx.changesCtxCancel()
			collectionCtx.activeSubChanges.Set(false)
		}()
		// sendChanges runs until blip context closes, or fails due to error
		startTime := time.Now()
		_, err = bh.sendChanges(rq.Sender, &sendChangesOptions{
			docIDs:            subChangesParams.docIDs(),
			since:             subChangesParams.Since(),
			continuous:        continuous,
			activeOnly:        subChangesParams.activeOnly(),
			batchSize:         subChangesParams.batchSize(),
			channels:          collectionCtx.channels,
			revocations:       subChangesParams.revocations(),
			clientType:        clientType,
			ignoreNoConflicts: clientType == clientTypeSGR2, // force this side to accept a "changes" message, even in no conflicts mode for SGR2.
			changesCtx:        collectionCtx.changesCtx,
			requestPlusSeq:    requestPlusSeq,
		})
		if err != nil {
			base.DebugfCtx(bh.loggingCtx, base.KeySyncMsg, "Closing blip connection due to changes feed error %+v\n", err)
			bh.ctxCancelFunc()
		}
		base.DebugfCtx(bh.loggingCtx, base.KeySyncMsg, "#%d: Type:%s   --> Time:%v", bh.serialNumber, rq.Profile(), time.Since(startTime))
	}()

	auditFields := base.AuditFields{
		base.AuditFieldSince: subChangesParams.Since().String(),
	}
	if subChangesParams.filter() != "" {
		auditFields[base.AuditFieldFilter] = subChangesParams.filter()
	}
	if len(subChangesParams.docIDs()) > 0 {
		auditFields[base.AuditFieldDocIDs] = subChangesParams.docIDs()
		auditFields[base.AuditFieldFilter] = base.DocIDsFilter
	}
	if continuous {
		auditFields[base.AuditFieldFeedType] = "continuous"
	} else {
		auditFields[base.AuditFieldFeedType] = "normal"
	}
	if len(channels) > 0 {
		auditFields[base.AuditFieldChannels] = channels
	}
	base.Audit(bh.loggingCtx, base.AuditIDChangesFeedStarted, auditFields)
	return nil
}

func (bh *blipHandler) handleUnsubChanges(rq *blip.Message) error {
	collectionCtx := bh.collectionCtx
	collectionCtx.changesCtxLock.Lock()
	defer collectionCtx.changesCtxLock.Unlock()
	collectionCtx.changesCtxCancel()
	return nil
}

type clientType uint8

const (
	clientTypeCBL2 clientType = iota
	clientTypeSGR2
)

type sendChangesOptions struct {
	docIDs            []string
	since             SequenceID
	continuous        bool
	activeOnly        bool
	batchSize         int
	channels          base.Set
	clientType        clientType
	revocations       bool
	ignoreNoConflicts bool
	changesCtx        context.Context
	requestPlusSeq    uint64
}

type changesDeletedFlag uint

const (
	// Bitfield flags used to build changes deleted property below
	changesDeletedFlagDeleted changesDeletedFlag = 0b001
	changesDeletedFlagRevoked changesDeletedFlag = 0b010
	changesDeletedFlagRemoved changesDeletedFlag = 0b100
)

func (flag changesDeletedFlag) HasFlag(deletedFlag changesDeletedFlag) bool {
	return flag&deletedFlag != 0
}

// sendChanges will start a changes feed and send changes. Returns bool to indicate whether the changes feed finished and all changes were sent. The error value is only used to indicate a fatal error, where the blip connection should be terminated. If the blip connection is disconnected by the client, the error will be nil, but the boolean parameter will be false.
func (bh *blipHandler) sendChanges(sender *blip.Sender, opts *sendChangesOptions) (bool, error) {
	defer func() {
		if panicked := recover(); panicked != nil {
			bh.replicationStats.NumHandlersPanicked.Add(1)
			base.WarnfCtx(bh.loggingCtx, "[%s] PANIC sending changes: %v\n%s", bh.blipContext.ID, panicked, debug.Stack())
		}
	}()

	base.InfofCtx(bh.loggingCtx, base.KeySync, "Sending changes since %v", opts.since)

	var changeType ChangesVersionType
	if bh.useHLV() {
		changeType = ChangesVersionTypeCV
	} else {
		changeType = ChangesVersionTypeRevTreeID
	}

	options := ChangesOptions{
		Since:          opts.since,
		Conflicts:      false, // CBL 2.0/BLIP don't support branched rev trees (LiteCore #437)
		Continuous:     opts.continuous,
		ActiveOnly:     opts.activeOnly,
		Revocations:    opts.revocations,
		clientType:     opts.clientType,
		ChangesCtx:     opts.changesCtx,
		RequestPlusSeq: opts.requestPlusSeq,
		VersionType:    changeType,
	}

	channelSet := opts.channels
	if channelSet == nil {
		channelSet = base.SetOf(channels.AllChannelWildcard)
	}

	caughtUp := false
	pendingChanges := make([][]interface{}, 0, opts.batchSize)
	sendPendingChangesAt := func(minChanges int) error {
		if len(pendingChanges) >= minChanges {
			if err := bh.sendBatchOfChanges(sender, pendingChanges, opts.ignoreNoConflicts); err != nil {
				return err
			}
			pendingChanges = make([][]interface{}, 0, opts.batchSize)
		}
		return nil
	}

	// Create a distinct database instance for changes, to avoid races between reloadUser invocation in changes.go
	// and BlipSyncContext user access.
	changesDb, err := bh.copyDatabaseCollectionWithUser(bh.collectionIdx)
	if err != nil {
		base.WarnfCtx(bh.loggingCtx, "[%s] error sending changes: %v", bh.blipContext.ID, err)
		return false, err
	}

	forceClose, err := generateBlipSyncChanges(bh.loggingCtx, changesDb, channelSet, options, opts.docIDs, func(changes []*ChangeEntry) error {
		base.DebugfCtx(bh.loggingCtx, base.KeySync, "    Sending %d changes", len(changes))
		for _, change := range changes {
			if !strings.HasPrefix(change.ID, "_") {
				// If change is a removal and we're running with protocol V3 and change change is not a tombstone
				// fall into 3.0 removal handling.
				// Changes with change.Revoked=true have already evaluated UserHasDocAccess in changes.go, don't check again.
				if change.allRemoved && bh.activeCBMobileSubprotocol >= CBMobileReplicationV3 && !change.Deleted && !change.Revoked && !bh.db.Options.UnsupportedOptions.BlipSendDocsWithChannelRemoval {
					// If client doesn't want removals / revocations, don't send change
					if !opts.revocations {
						continue
					}

					// If the user has access to the doc through another channel don't send change
					userHasAccessToDoc, err := UserHasDocAccess(bh.loggingCtx, changesDb, change.ID)
					if err == nil && userHasAccessToDoc {
						continue
					}
					// If we can't determine user access due to an error, log error and fall through to send change anyway.
					// In the event of an error we should be cautious and send a revocation anyway, even if the user
					// may actually have an alternate access method. This is the safer approach security-wise and
					// also allows for a recovery if the user notices they are missing a doc they should have access
					// to. A recovery option would be to trigger a mutation of the document for it to be sent in a
					// subsequent changes request. If we were to avoid sending a removal there is no recovery
					// option to then trigger that removal later on.
					if err != nil {
						base.WarnfCtx(bh.loggingCtx, "Unable to determine whether user has access to %s, will send removal: %v", base.UD(change.ID), err)
					}

				}
				for _, item := range change.Changes {
					changeRow := bh.buildChangesRow(change, item)
					pendingChanges = append(pendingChanges, changeRow)
				}

				if err := sendPendingChangesAt(opts.batchSize); err != nil {
					return err
				}
			}
		}
		if caughtUp || len(changes) == 0 {
			if err := sendPendingChangesAt(1); err != nil {
				return err
			}
			if !caughtUp {
				caughtUp = true
				// Signal to client that it's caught up
				if err := bh.sendBatchOfChanges(sender, nil, opts.ignoreNoConflicts); err != nil {
					return err
				}
			}
		}
		return nil
	})

	// On forceClose, send notify to trigger immediate exit from change waiter
	if forceClose {
		user := ""
		if bh.db.User() != nil {
			user = bh.db.User().Name()
		}
		bh.db.DatabaseContext.NotifyTerminatedChanges(bh.loggingCtx, user)
	}
	return (err == nil && !forceClose), err
}

func (bh *blipHandler) buildChangesRow(change *ChangeEntry, changeVersion ChangeByVersionType) []interface{} {
	var changeRow []interface{}

	// change map should only have one entry
	if len(changeVersion) > 1 {
		base.AssertfCtx(bh.loggingCtx, "more changes in list than expected on change entry: %v", change.ID)
	}
	rev, _ := changeVersion.GetChangeRowRev()

	if bh.activeCBMobileSubprotocol >= CBMobileReplicationV3 {
		deletedFlags := changesDeletedFlag(0)
		if change.Deleted {
			deletedFlags |= changesDeletedFlagDeleted
		}
		if change.Revoked {
			deletedFlags |= changesDeletedFlagRevoked
		}
		if change.allRemoved {
			deletedFlags |= changesDeletedFlagRemoved
		}

		changeRow = []interface{}{change.Seq, change.ID, rev, deletedFlags}
		if deletedFlags == 0 {
			changeRow = changeRow[0:3]
		}

	} else {
		changeRow = []interface{}{change.Seq, change.ID, rev, change.Deleted}
		if !change.Deleted {
			changeRow = changeRow[0:3]
		}
	}

	return changeRow
}

func (bh *blipHandler) sendBatchOfChanges(sender *blip.Sender, changeArray [][]interface{}, ignoreNoConflicts bool) error {
	outrq := blip.NewRequest()
	outrq.SetProfile("changes")
	if ignoreNoConflicts {
		outrq.Properties[ChangesMessageIgnoreNoConflicts] = trueProperty
	}
	if bh.collectionIdx != nil {
		outrq.Properties[BlipCollection] = strconv.Itoa(*bh.collectionIdx)
	}
	err := outrq.SetJSONBody(changeArray)
	if err != nil {
		base.InfofCtx(bh.loggingCtx, base.KeyAll, "Error setting changes: %v", err)
	}

	if len(changeArray) > 0 {
		// Check for user updates before creating the db copy for handleChangesResponse
		if err := bh.refreshUser(); err != nil {
			return err
		}

		handleChangesResponseDbCollection, err := bh.copyDatabaseCollectionWithUser(bh.collectionIdx)
		if err != nil {
			return err
		}

		sendTime := time.Now()
		if !bh.sendBLIPMessage(sender, outrq) {
			return ErrClosedBLIPSender
		}

		bh.inFlightChangesThrottle <- struct{}{}
		atomic.AddInt64(&bh.changesPendingResponseCount, 1)

		bh.replicationStats.SendChangesCount.Add(int64(len(changeArray)))
		// Spawn a goroutine to await the client's response:
		go func(bh *blipHandler, sender *blip.Sender, response *blip.Message, changeArray [][]interface{}, sendTime time.Time, dbCollection *DatabaseCollectionWithUser) {
			if err := bh.handleChangesResponse(bh.loggingCtx, sender, response, changeArray, sendTime, dbCollection, bh.collectionIdx); err != nil {
				base.WarnfCtx(bh.loggingCtx, "Error from bh.handleChangesResponse: %v", err)
				if bh.fatalErrorCallback != nil {
					bh.fatalErrorCallback(err)
				}
			}

			// Sent all of the revs for this changes batch, allow another changes batch to be sent.
			select {
			case <-bh.inFlightChangesThrottle:
			case <-bh.terminator:
			}

			atomic.AddInt64(&bh.changesPendingResponseCount, -1)
		}(bh, sender, outrq.Response(), changeArray, sendTime, handleChangesResponseDbCollection)
	} else {
		outrq.SetNoReply(true)
		if !bh.sendBLIPMessage(sender, outrq) {
			return ErrClosedBLIPSender
		}
	}

	if len(changeArray) > 0 {
		sequence := changeArray[0][0].(SequenceID)
		base.InfofCtx(bh.loggingCtx, base.KeySync, "Sent %d changes to client, from seq %s", len(changeArray), sequence.String())
	} else {
		base.InfofCtx(bh.loggingCtx, base.KeySync, "Sent all changes to client")
	}

	return nil
}

// Handles a "changes" request, i.e. a set of changes pushed by the client
func (bh *blipHandler) handleChanges(rq *blip.Message) error {
	var ignoreNoConflicts bool
	if val := rq.Properties[ChangesMessageIgnoreNoConflicts]; val != "" {
		ignoreNoConflicts = val == trueProperty
	}

	if !ignoreNoConflicts && !bh.collection.AllowConflicts() {
		return ErrUseProposeChanges
	}

	var changeList [][]interface{}
	if err := rq.ReadJSONBody(&changeList); err != nil {
		base.WarnfCtx(bh.loggingCtx, "Handle changes got error: %v", err)
		return err
	}

	collectionCtx := bh.collectionCtx
	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("#Changes:%d", len(changeList)))
	if len(changeList) == 0 {
		// An empty changeList is sent when a one-shot replication sends its final changes
		// message, or a continuous replication catches up *for the first time*.
		// Note that this doesn't mean that rev messages associated with previous changes
		// messages have been fully processed
		if collectionCtx.emptyChangesMessageCallback != nil {
			collectionCtx.emptyChangesMessageCallback()
		}
		return nil
	}
	output := bytes.NewBuffer(make([]byte, 0, 100*len(changeList)))
	output.Write([]byte("["))
	jsonOutput := base.JSONEncoder(output)
	nWritten := 0
	nRequested := 0

	// Include changes messages w/ proposeChanges stats, although CBL should only be using proposeChanges
	startTime := time.Now()
	bh.replicationStats.HandleChangesCount.Add(int64(len(changeList)))
	defer func() {
		bh.replicationStats.HandleChangesTime.Add(time.Since(startTime).Nanoseconds())
	}()

	// DocID+RevID -> SeqNo
	expectedSeqs := make(map[IDAndRev]SequenceID, 0)
	alreadyKnownSeqs := make([]SequenceID, 0)

	versionVectorProtocol := bh.useHLV()

	for _, change := range changeList {
		docID := change[1].(string)
		rev := change[2].(string)
		var missing, possible []string

		changeIsVector := false
		if versionVectorProtocol {
			changeIsVector = strings.Contains(rev, "@")
		}
		if !versionVectorProtocol || !changeIsVector {
			missing, possible = bh.collection.RevDiff(bh.loggingCtx, docID, []string{rev})
		} else {
			missing, possible = bh.collection.CheckChangeVersion(bh.loggingCtx, docID, rev)
		}

		if nWritten > 0 {
			output.Write([]byte(","))
		}

		deletedFlags := changesDeletedFlag(0)
		if len(change) > 3 {
			switch v := change[3].(type) {
			case json.Number:
				deletedIntFlag, err := v.Int64()
				if err != nil {
					base.ErrorfCtx(bh.loggingCtx, "Failed to parse deletedFlags: %v", err)
					continue
				}
				deletedFlags = changesDeletedFlag(deletedIntFlag)
			case bool:
				deletedFlags = changesDeletedFlagDeleted
			default:
				base.ErrorfCtx(bh.loggingCtx, "Unknown type for deleted field in changes message: %T", v)
				continue
			}

		}

		if bh.purgeOnRemoval && bh.activeCBMobileSubprotocol >= CBMobileReplicationV3 &&
			(deletedFlags.HasFlag(changesDeletedFlagRevoked) || deletedFlags.HasFlag(changesDeletedFlagRemoved)) {
			err := bh.collection.Purge(bh.loggingCtx, docID, true)
			if err != nil {
				base.WarnfCtx(bh.loggingCtx, "Failed to purge document: %v", err)
			}
			bh.replicationStats.HandleRevDocsPurgedCount.Add(1)

			// Fall into skip sending case
			missing = nil
		}

		if missing == nil {
			// already have this rev, tell the peer to skip sending it
			output.Write([]byte("0"))
			if collectionCtx.sgr2PullAlreadyKnownSeqsCallback != nil {
				seq, err := ParseJSONSequenceID(seqStr(bh.loggingCtx, change[0]))
				if err != nil {
					base.WarnfCtx(bh.loggingCtx, "Unable to parse known sequence %q for %q / %q: %v", change[0], base.UD(docID), rev, err)
				} else {
					// we're not able to checkpoint a sequence we can't parse and aren't expecting so just skip the callback if we errored
					alreadyKnownSeqs = append(alreadyKnownSeqs, seq)
				}
			}
		} else {
			// we want this rev, send possible ancestors to the peer
			nRequested++
			if len(possible) == 0 {
				output.Write([]byte("[]"))
			} else {
				err := jsonOutput.Encode(possible)
				if err != nil {
					base.InfofCtx(bh.loggingCtx, base.KeyAll, "Error encoding json: %v", err)
				}
			}

			// skip parsing seqno if we're not going to use it (no callback defined)
			if collectionCtx.sgr2PullAddExpectedSeqsCallback != nil {
				seq, err := ParseJSONSequenceID(seqStr(bh.loggingCtx, change[0]))
				if err != nil {
					// We've already asked for the doc/rev for the sequence so assume we're going to receive it... Just log this and carry on
					base.WarnfCtx(bh.loggingCtx, "Unable to parse expected sequence %q for %q / %q: %v", change[0], base.UD(docID), rev, err)
				} else {
					expectedSeqs[IDAndRev{DocID: docID, RevID: rev}] = seq
				}
			}
		}
		nWritten++
	}
	output.Write([]byte("]"))
	response := rq.Response()
	if bh.sgCanUseDeltas {
		base.DebugfCtx(bh.loggingCtx, base.KeyAll, "Setting deltas=true property on handleChanges response")
		response.Properties[ChangesResponseDeltas] = trueProperty
		bh.replicationStats.HandleChangesDeltaRequestedCount.Add(int64(nRequested))
	}
	response.SetCompressed(true)
	response.SetBody(output.Bytes())

	if collectionCtx.sgr2PullAddExpectedSeqsCallback != nil {
		collectionCtx.sgr2PullAddExpectedSeqsCallback(expectedSeqs)
	}
	if collectionCtx.sgr2PullAlreadyKnownSeqsCallback != nil {
		collectionCtx.sgr2PullAlreadyKnownSeqsCallback(alreadyKnownSeqs...)
	}

	return nil
}

// Handles a "proposeChanges" request, similar to "changes" but in no-conflicts mode
func (bh *blipHandler) handleProposeChanges(rq *blip.Message) error {

	// we don't know whether this batch of changes has completed because they look like unsolicited revs to us,
	// but we can stop clients swarming us with these causing CheckProposedRev work
	bh.inFlightChangesThrottle <- struct{}{}
	defer func() { <-bh.inFlightChangesThrottle }()

	includeConflictRev := false
	if val := rq.Properties[ProposeChangesConflictsIncludeRev]; val != "" {
		includeConflictRev = val == trueProperty
	}

	var changeList [][]interface{}
	if err := rq.ReadJSONBody(&changeList); err != nil {
		return err
	}
	bh.logEndpointEntry(rq.Profile(), fmt.Sprintf("#Changes: %d", len(changeList)))
	if len(changeList) == 0 {
		return nil
	}
	output := bytes.NewBuffer(make([]byte, 0, 5*len(changeList)))
	output.Write([]byte("["))
	nWritten := 0

	// proposeChanges stats
	startTime := time.Now()
	bh.replicationStats.HandleChangesCount.Add(int64(len(changeList)))
	defer func() {
		bh.replicationStats.HandleChangesTime.Add(time.Since(startTime).Nanoseconds())
	}()
	versionVectorProtocol := bh.useHLV()

	for i, change := range changeList {
		docID := change[0].(string)
		rev := change[1].(string) // rev can represent a RevTree ID or HLV current version
		parentRevID := ""
		if len(change) > 2 {
			parentRevID = change[2].(string)
		}
		var status ProposedRevStatus
		var currentRev string

		changeIsVector := false
		if versionVectorProtocol {
			// TODO: CBG-4812 Use base.IsRevTreeID
			changeIsVector = strings.Contains(rev, "@")
		}
		if versionVectorProtocol && changeIsVector {
			proposedVersionStr := ExtractCVFromProposeChangesRev(rev)
			status, currentRev = bh.collection.CheckProposedVersion(bh.loggingCtx, docID, proposedVersionStr, parentRevID, rev)
		} else {
			status, currentRev = bh.collection.CheckProposedRev(bh.loggingCtx, docID, rev, parentRevID)
		}
		if status == ProposedRev_OK_IsNew {
			// Remember that the doc doesn't exist locally, in order to optimize the upcoming Put:
			bh.collectionCtx.notePendingInsertion(docID)
		} else if status != ProposedRev_OK {
			// Reject the proposed change.
			// Skip writing trailing zeroes; but if we write a number afterwards we have to catch up
			if nWritten > 0 {
				output.Write([]byte(","))
			}
			for ; nWritten < i; nWritten++ {
				output.Write([]byte("0,"))
			}
			if includeConflictRev && status == ProposedRev_Conflict {
				revEntry := IncludeConflictRevEntry{Status: status, Rev: currentRev}
				entryBytes, marshalErr := base.JSONMarshal(revEntry)
				if marshalErr != nil {
					base.WarnfCtx(bh.loggingCtx, "Unable to marshal proposeChangesEntry as includeConflictRev - falling back to status-only entry.  Error: %v", marshalErr)
					output.Write([]byte(strconv.FormatInt(int64(status), 10)))
				}
				output.Write(entryBytes)

			} else {
				output.Write([]byte(strconv.FormatInt(int64(status), 10)))
			}
			nWritten++
		}
	}
	output.Write([]byte("]"))
	response := rq.Response()
	if bh.sgCanUseDeltas {
		base.DebugfCtx(bh.loggingCtx, base.KeyAll, "Setting deltas=true property on proposeChanges response")
		response.Properties[ChangesResponseDeltas] = trueProperty
	}
	response.SetCompressed(true)
	response.SetBody(output.Bytes())
	return nil
}

// ////// DOCUMENTS:

func (bsc *BlipSyncContext) sendRevAsDelta(ctx context.Context, sender *blip.Sender, docID, revID string, deltaSrcRevID string, seq SequenceID, knownRevs map[string]bool, maxHistory int, handleChangesResponseCollection *DatabaseCollectionWithUser, collectionIdx *int) error {
	bsc.replicationStats.SendRevDeltaRequestedCount.Add(1)

	revDelta, redactedRev, err := handleChangesResponseCollection.GetDelta(ctx, docID, deltaSrcRevID, revID)
	if err == ErrForbidden { // nolint: gocritic // can't convert if/else if to switch since base.IsFleeceDeltaError is not switchable
		return err
	} else if base.IsFleeceDeltaError(err) {
		// Something went wrong in the diffing library. We want to know about this!
		base.WarnfCtx(ctx, "Falling back to full body replication. Error generating delta from %s to %s for key %s - err: %v", deltaSrcRevID, revID, base.UD(docID), err)
		return bsc.sendRevision(ctx, sender, docID, revID, seq, knownRevs, maxHistory, handleChangesResponseCollection, collectionIdx, false)
	} else if err == base.ErrDeltaSourceIsTombstone {
		base.TracefCtx(ctx, base.KeySync, "Falling back to full body replication. Delta source %s is tombstone. Unable to generate delta to %s for key %s", deltaSrcRevID, revID, base.UD(docID))
		return bsc.sendRevision(ctx, sender, docID, revID, seq, knownRevs, maxHistory, handleChangesResponseCollection, collectionIdx, false)
	} else if err != nil {
		base.DebugfCtx(ctx, base.KeySync, "Falling back to full body replication. Couldn't get delta from %s to %s for key %s - err: %v", deltaSrcRevID, revID, base.UD(docID), err)
		return bsc.sendRevision(ctx, sender, docID, revID, seq, knownRevs, maxHistory, handleChangesResponseCollection, collectionIdx, false)
	}

	if redactedRev != nil {
		var history []string
		var revTreeProperty []string
		if !bsc.useHLV() {
			history = toHistory(redactedRev.History, knownRevs, maxHistory)
		} else {
			history = append(history, redactedRev.hlvHistory)
		}
		if bsc.sendRevTreeProperty() {
			revTreeProperty = append(revTreeProperty, redactedRev.RevID)
			revTreeProperty = append(revTreeProperty, toHistory(redactedRev.History, knownRevs, maxHistory)...)
		}

		properties := blipRevMessageProperties(history, redactedRev.Deleted, seq, "", revTreeProperty)
		return bsc.sendRevisionWithProperties(ctx, sender, docID, revID, collectionIdx, redactedRev.BodyBytes, nil, properties, seq, nil)
	}

	if revDelta == nil {
		base.DebugfCtx(ctx, base.KeySync, "Falling back to full body replication. Couldn't get delta from %s to %s for key %s", deltaSrcRevID, revID, base.UD(docID))
		return bsc.sendRevision(ctx, sender, docID, revID, seq, knownRevs, maxHistory, handleChangesResponseCollection, collectionIdx, false)
	}

	resendFullRevisionFunc := func() error {
		base.InfofCtx(ctx, base.KeySync, "Resending revision as full body. Peer couldn't process delta %s from %s to %s for key %s", base.UD(revDelta.DeltaBytes), deltaSrcRevID, revID, base.UD(docID))
		return bsc.sendRevision(ctx, sender, docID, revID, seq, knownRevs, maxHistory, handleChangesResponseCollection, collectionIdx, false)
	}

	base.TracefCtx(ctx, base.KeySync, "docID: %s - delta: %v", base.UD(docID), base.UD(string(revDelta.DeltaBytes)))
	if err := bsc.sendDelta(ctx, sender, docID, collectionIdx, deltaSrcRevID, revDelta, seq, resendFullRevisionFunc); err != nil {
		return err
	}

	// We'll consider this one doc read for collection stats purposes, since GetDelta doesn't go through the normal getRev codepath.
	handleChangesResponseCollection.collectionStats.NumDocReads.Add(1)
	handleChangesResponseCollection.collectionStats.DocReadsBytes.Add(int64(len(revDelta.DeltaBytes)))

	bsc.replicationStats.SendRevDeltaSentCount.Add(1)
	return nil
}

func (bh *blipHandler) handleNoRev(rq *blip.Message) error {
	docID, revID := rq.Properties[NorevMessageId], rq.Properties[NorevMessageRev]
	var seqStr string
	if bh.activeCBMobileSubprotocol <= CBMobileReplicationV2 && bh.clientType == BLIPClientTypeSGR2 {
		seqStr = rq.Properties[NorevMessageSeq]
	} else {
		seqStr = rq.Properties[NorevMessageSequence]
	}
	base.InfofCtx(bh.loggingCtx, base.KeySyncMsg, "%s: norev for doc %q / %q seq:%q - error: %q - reason: %q",
		rq.String(), base.UD(docID), revID, seqStr, rq.Properties[NorevMessageError], rq.Properties[NorevMessageReason])

	if bh.collectionCtx.sgr2PullProcessedSeqCallback != nil {
		seq, err := ParseJSONSequenceID(seqStr)
		if err != nil {
			base.WarnfCtx(bh.loggingCtx, "Unable to parse sequence %q from norev message: %v - not tracking for checkpointing", seqStr, err)
		} else {
			bh.collectionCtx.sgr2PullProcessedSeqCallback(&seq, IDAndRev{DocID: docID, RevID: revID})
		}
	}

	// Couchbase Lite always sends noreply=true for norev profiles
	// but for testing purposes, it's useful to know which handler processed the message
	if !rq.NoReply() && rq.Properties[SGShowHandler] == trueProperty {
		response := rq.Response()
		response.Properties[SGHandler] = "handleNoRev"
	}

	return nil
}

type processRevStats struct {
	count            *base.SgwIntStat // Increments when rev processed successfully
	errorCount       *base.SgwIntStat
	deltaRecvCount   *base.SgwIntStat
	bytes            *base.SgwIntStat
	processingTime   *base.SgwIntStat
	docsPurgedCount  *base.SgwIntStat
	throttledRevs    *base.SgwIntStat
	throttledRevTime *base.SgwIntStat
}

// Processes a "rev" request, i.e. client is pushing a revision body
// stats must always be provided, along with all the fields filled with valid pointers
func (bh *blipHandler) processRev(rq *blip.Message, stats *processRevStats) (err error) {
	startTime := time.Now()
	defer func() {
		stats.processingTime.Add(time.Since(startTime).Nanoseconds())
		if err == nil {
			stats.count.Add(1)
		} else {
			stats.errorCount.Add(1)
		}
	}()

	if bh.db.DatabaseContext.Options.UnsupportedOptions != nil && bh.db.DatabaseContext.Options.UnsupportedOptions.RejectWritesWithSkippedSequences {
		// if we are in slow broadcast mode reject write with 503 and increment rejected writes stat
		if bh.db.BroadcastSlowMode.Load() {
			bh.db.DbStats.DatabaseStats.NumDocWritesRejected.Add(1)
			return base.HTTPErrorf(http.StatusServiceUnavailable, "Database cache is behind and cannot accept writes at this time. Please try again later.")
		}
	}

	// throttle concurrent revs
	if cap(bh.inFlightRevsThrottle) > 0 {
		select {
		case bh.inFlightRevsThrottle <- struct{}{}:
		default:
			stats.throttledRevs.Add(1)
			throttleStart := time.Now()
			bh.inFlightRevsThrottle <- struct{}{}
			stats.throttledRevTime.Add(time.Since(throttleStart).Nanoseconds())
		}
		defer func() { <-bh.inFlightRevsThrottle }()
	}

	// addRevisionParams := newAddRevisionParams(rq)
	revMessage := RevMessage{Message: rq}

	// Doc metadata comes from the BLIP message metadata, not magic document properties:
	docID, found := revMessage.ID()
	rev, rfound := revMessage.Rev()
	if !found || !rfound {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing docID or rev")
	}

	if bh.readOnly {
		return base.HTTPErrorf(http.StatusForbidden, "Replication context is read-only, docID: %s, rev:%s", docID, rev)
	}

	base.DebugfCtx(bh.loggingCtx, base.KeySyncMsg, "#%d: Type:%s %s", bh.serialNumber, rq.Profile(), revMessage.String())

	bodyBytes, err := rq.Body()
	if err != nil {
		return err
	}

	base.TracefCtx(bh.loggingCtx, base.KeySyncMsg, "#%d: Properties:%v  Body:%s", bh.serialNumber, base.UD(revMessage.Properties), base.UD(string(bodyBytes)))

	stats.bytes.Add(int64(len(bodyBytes)))

	if bh.BlipSyncContext.purgeOnRemoval && bytes.Contains(bodyBytes, []byte(`"`+BodyRemoved+`":`)) {
		var body Body
		if err := body.Unmarshal(bodyBytes); err != nil {
			return err
		}
		if removed, ok := body[BodyRemoved].(bool); ok && removed {
			base.InfofCtx(bh.loggingCtx, base.KeySync, "Purging doc %v - removed at rev %v", base.UD(docID), rev)
			if err := bh.collection.Purge(bh.loggingCtx, docID, true); err != nil {
				return err
			}

			stats.docsPurgedCount.Add(1)
			if bh.collectionCtx.sgr2PullProcessedSeqCallback != nil {
				seqStr := rq.Properties[RevMessageSequence]
				seq, err := ParseJSONSequenceID(seqStr)
				if err != nil {
					base.WarnfCtx(bh.loggingCtx, "Unable to parse sequence %q from rev message: %v - not tracking for checkpointing", seqStr, err)
				} else {
					bh.collectionCtx.sgr2PullProcessedSeqCallback(&seq, IDAndRev{DocID: docID, RevID: rev})
				}
			}
			return nil
		}
	}

	newDoc := &Document{
		ID: docID,
	}

	var history []string
	historyStr := rq.Properties[RevMessageHistory]
	var incomingHLV *HybridLogicalVector
	// Build history/HLV
	var legacyRevList []string
	// TODO: CBG-4812 Use base.IsRevTreeID
	changeIsVector := strings.Contains(rev, "@")
	if !bh.useHLV() || !changeIsVector {
		newDoc.RevID = rev
		history = []string{rev}
		if historyStr != "" {
			history = append(history, strings.Split(historyStr, ",")...)
		}
	} else {
		incomingHLV, legacyRevList, err = GetHLVFromRevMessage(rq)
		if err != nil {
			base.InfofCtx(bh.loggingCtx, base.KeySync, "Error parsing hlv while processing rev for doc %v. Error: %v", base.UD(docID), err)
			return base.HTTPErrorf(http.StatusUnprocessableEntity, "error extracting hlv from blip message")
		}
		newDoc.HLV = incomingHLV
	}

	isBlipRevTreeProperty := false
	// if the client is SGW and there are no legacy revs being sent (i.e. doc is not a pre-upgraded doc) check the rev tree property
	if bh.clientType == BLIPClientTypeSGR2 && len(legacyRevList) == 0 {
		revTree, ok := rq.Properties[RevMessageTreeHistory]
		if ok {
			legacyRevList = append(legacyRevList, strings.Split(revTree, ",")...)
			isBlipRevTreeProperty = true
			if len(legacyRevList) > 0 {
				newDoc.RevID = legacyRevList[0]
			}
		}
	}

	newDoc.UpdateBodyBytes(bodyBytes)

	injectedAttachmentsForDelta := false
	if deltaSrcRevID, isDelta := revMessage.DeltaSrc(); isDelta && !revMessage.Deleted() {
		if !bh.sgCanUseDeltas {
			return base.HTTPErrorf(http.StatusBadRequest, "Deltas are disabled for this peer")
		}

		//  TODO: Doing a GetRevCopy here duplicates some rev cache retrieval effort, since deltaRevSrc is always
		//        going to be the current rev (no conflicts), and PutExistingRev will need to retrieve the
		//        current rev over again.  Should push this handling down PutExistingRev and use the version
		//        returned via callback in WriteUpdate, but blocked by moving attachment metadata to a rev property first
		//        (otherwise we don't have information needed to do downloadOrVerifyAttachments below prior to PutExistingRev)

		// Note: Using GetRevCopy here, and not direct rev cache retrieval, because it's still necessary to apply access check
		//       while retrieving deltaSrcRevID.  Couchbase Lite replication guarantees client has access to deltaSrcRevID,
		//       due to no-conflict write restriction, but we still need to enforce security here to prevent leaking data about previous
		//       revisions to malicious actors (in the scenario where that user has write but not read access).
		var deltaSrcRev DocumentRevision
		if bh.useHLV() && !base.IsRevTreeID(deltaSrcRevID) {
			deltaSrcVersion, parseErr := ParseVersion(deltaSrcRevID)
			if parseErr != nil {
				return base.HTTPErrorf(http.StatusUnprocessableEntity, "Unable to parse version for delta source for doc %s, error: %v", base.UD(docID), parseErr)
			}
			deltaSrcRev, err = bh.collection.GetCV(bh.loggingCtx, docID, &deltaSrcVersion, false)
		} else {
			deltaSrcRev, err = bh.collection.GetRev(bh.loggingCtx, docID, deltaSrcRevID, false, nil)
		}
		if err != nil {
			return base.HTTPErrorf(http.StatusUnprocessableEntity, "Can't fetch doc %s for deltaSrc=%s %v", base.UD(docID), deltaSrcRevID, err)
		}

		// Receiving a delta to be applied on top of a tombstone is not valid.
		if deltaSrcRev.Deleted {
			return base.HTTPErrorf(http.StatusUnprocessableEntity, "Can't use delta. Found tombstone for doc %s deltaSrc=%s", base.UD(docID), deltaSrcRevID)
		}

		deltaSrcBody, err := deltaSrcRev.MutableBody()
		if err != nil {
			return base.HTTPErrorf(http.StatusUnprocessableEntity, "Unable to unmarshal mutable body for doc %s deltaSrc=%s %v", base.UD(docID), deltaSrcRevID, err)
		}

		if deltaSrcBody[BodyRemoved] != nil {
			return base.HTTPErrorf(http.StatusUnprocessableEntity, "Can't use delta. Found _removed property for doc %s deltaSrc=%s", base.UD(docID), deltaSrcRevID)
		}
		// Stamp attachments so we can patch them
		if len(deltaSrcRev.Attachments) > 0 {
			deltaSrcBody[BodyAttachments] = map[string]interface{}(deltaSrcRev.Attachments)
			injectedAttachmentsForDelta = true
		}

		deltaSrcMap := map[string]interface{}(deltaSrcBody)
		err = base.Patch(&deltaSrcMap, newDoc.Body(bh.loggingCtx))
		// err should only ever be a FleeceDeltaError here - but to be defensive, handle other errors too (e.g. somehow reaching this code in a CE build)
		if err != nil {
			// Something went wrong in the diffing library. We want to know about this!
			base.WarnfCtx(bh.loggingCtx, "Error patching deltaSrc %s with %s for doc %s with delta - err: %v", deltaSrcRevID, rev, base.UD(docID), err)
			return base.HTTPErrorf(http.StatusUnprocessableEntity, "Error patching deltaSrc with delta: %s", err)
		}

		newDoc.UpdateBody(deltaSrcMap)
		base.TracefCtx(bh.loggingCtx, base.KeySync, "docID: %s - body after patching: %v", base.UD(docID), base.UD(deltaSrcMap))
		stats.deltaRecvCount.Add(1)
	}

	err = validateBlipBody(bh.loggingCtx, bodyBytes, newDoc)
	if err != nil {
		return err
	}

	// Handle and pull out expiry
	if bytes.Contains(bodyBytes, []byte(BodyExpiry)) {
		body := newDoc.Body(bh.loggingCtx)
		expiry, err := body.ExtractExpiry()
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
		}
		newDoc.DocExpiry = expiry
		newDoc.UpdateBody(body)
	}

	newDoc.Deleted = revMessage.Deleted()

	// noconflicts flag from LiteCore
	// https://github.com/couchbase/couchbase-lite-core/wiki/Replication-Protocol#rev
	revNoConflicts := false
	if val, ok := rq.Properties[RevMessageNoConflicts]; ok {
		var err error
		revNoConflicts, err = strconv.ParseBool(val)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid value for noconflicts: %s", err)
		}
	}

	var rawBucketDoc *sgbucket.BucketDocument

	// Attachment processing
	if injectedAttachmentsForDelta || bytes.Contains(bodyBytes, []byte(BodyAttachments)) {

		body := newDoc.Body(bh.loggingCtx)
		// The bytes.Contains([]byte(BodyAttachments)) check will pass even if _attachments is not a toplevel key but rather a nested key or subkey. That check is an optimization to avoid having to unmarshal the document if there are no attachments. Therefore, check again that the unmarshalled body contains BodyAttachments.
		if body[BodyAttachments] != nil {

			var currentBucketDoc *Document

			minRevpos := 0
			if historyStr != "" {
				// fetch current bucket doc.  Treats error as not found
				currentBucketDoc, rawBucketDoc, _ = bh.collection.GetDocumentWithRaw(bh.loggingCtx, docID, DocUnmarshalSync)

				// For revtree clients, can use revPos as an optimization.  HLV always compares incoming
				// attachments with current attachments on the document
				if !bh.useHLV() {
					// Look at attachments with revpos > the last common ancestor's
					// If we're able to obtain current doc data then we should use the common ancestor generation++ for min revpos
					// as we will already have any attachments on the common ancestor so don't need to ask for them.
					// Otherwise we'll have to go as far back as we can in the doc history and choose the last entry in there.
					if currentBucketDoc != nil {
						commonAncestor := currentBucketDoc.History.findAncestorFromSet(currentBucketDoc.GetRevTreeID(), history)
						minRevpos, _ = ParseRevID(bh.loggingCtx, commonAncestor)
						minRevpos++
					} else {
						minRevpos, _ = ParseRevID(bh.loggingCtx, history[len(history)-1])
					}
				}
			}

			// currentDigests is a map from attachment name to the current bucket doc digest,
			// for any attachments on the incoming document that are also on the current bucket doc
			var currentDigests map[string]string

			// Do we have a previous doc? If not don't need to do this check
			if currentBucketDoc != nil {
				bodyAtts := GetBodyAttachments(body)
				currentDigests = make(map[string]string, len(bodyAtts))
				for name, value := range bodyAtts {
					// Check if we have this attachment name already, if we do, continue check
					currentAttachment, ok := currentBucketDoc.Attachments()[name]
					if !ok {
						// If we don't have this attachment already, ensure incoming revpos is greater than minRevPos, otherwise
						// update to ensure it's fetched and uploaded
						if minRevpos > 0 {
							bodyAtts[name].(map[string]interface{})["revpos"], _ = ParseRevID(bh.loggingCtx, rev)
						}
						continue
					}

					currentAttachmentMeta, ok := currentAttachment.(map[string]interface{})
					if !ok {
						return base.HTTPErrorf(http.StatusInternalServerError, "Current attachment data is invalid, is not a json object")
					}

					currentAttachmentDigest, ok := currentAttachmentMeta["digest"].(string)
					if !ok {
						return base.HTTPErrorf(http.StatusInternalServerError, "Current attachment data is invalid, does not contain digest")
					}
					currentDigests[name] = currentAttachmentDigest

					incomingAttachmentMeta, ok := value.(map[string]interface{})
					if !ok {
						return base.HTTPErrorf(http.StatusBadRequest, "Invalid attachment, expecting json object")
					}

					// If this attachment has data then we're fine, this isn't a stub attachment and therefore doesn't
					// need the check.
					if incomingAttachmentMeta["data"] != nil {
						continue
					}

					incomingAttachmentDigest, ok := incomingAttachmentMeta["digest"].(string)
					if !ok {
						return base.HTTPErrorf(http.StatusBadRequest, "Invalid attachment, does not have digest field")
					}
					// For revtree clients, can use revPos as an optimization.  HLV always compares incoming
					// attachments with current attachments on the document
					if !bh.useHLV() {
						incomingAttachmentRevpos, ok := base.ToInt64(incomingAttachmentMeta["revpos"])
						if !ok {
							return base.HTTPErrorf(http.StatusBadRequest, "Invalid attachment, does not have revpos field")
						}

						// Compare the revpos and attachment digest. If incoming revpos is less than or equal to minRevPos and
						// digest is different we need to override the revpos and set it to the current revision to ensure
						// the attachment is requested and stored
						if int(incomingAttachmentRevpos) <= minRevpos && currentAttachmentDigest != incomingAttachmentDigest {
							bodyAtts[name].(map[string]interface{})["revpos"], _ = ParseRevID(bh.loggingCtx, rev)
						}
					}
				}

				body[BodyAttachments] = bodyAtts
			}

			if err := bh.downloadOrVerifyAttachments(rq.Sender, body, minRevpos, docID, currentDigests); err != nil {
				base.ErrorfCtx(bh.loggingCtx, "Error during downloadOrVerifyAttachments for doc %s/%s: %v", base.UD(docID), rev, err)
				return err
			}

			newDoc.SetAttachments(GetBodyAttachments(body))
			delete(body, BodyAttachments)
			newDoc.UpdateBody(body)
		}

	}

	if rawBucketDoc == nil && bh.collectionCtx.checkPendingInsertion(docID) {
		// At the time we handled the `proposeChanges` request, there was no doc with this docID
		// in the bucket. As an optimization, tell PutExistingRev to assume the doc still doesn't
		// exist and bypass getting it from the bucket during the save. If we're wrong, the save
		// will fail with a CAS mismatch and the retry will fetch the existing doc.
		rawBucketDoc = &sgbucket.BucketDocument{} // empty struct with zero CAS
	}

	// Finally, save the revision (with the new attachments inline)
	// If a conflict resolver is defined for the handler, write with conflict resolution.

	// If the doc is a tombstone we want to allow conflicts when running SGR2
	// bh.conflictResolver != nil represents an active SGR2 and BLIPClientTypeSGR2 represents a passive SGR2
	forceAllowConflictingTombstone := newDoc.Deleted && (!bh.conflictResolver.IsEmpty() || bh.clientType == BLIPClientTypeSGR2)
	if bh.useHLV() && changeIsVector {
		opts := PutDocOptions{
			NewDoc:                         newDoc,
			RevTreeHistory:                 legacyRevList,
			ForceAllowConflictingTombstone: forceAllowConflictingTombstone,
			ExistingDoc:                    rawBucketDoc,
			NewDocHLV:                      incomingHLV,
			ConflictResolver:               bh.conflictResolver.hlvConflictResolver,
			AlignRevTrees:                  isBlipRevTreeProperty,
		}
		_, _, _, err = bh.collection.PutExistingCurrentVersion(bh.loggingCtx, opts)
	} else {
		docUpdateEvent := ExistingVersionWithUpdateToHLV
		if bh.useHLV() {
			docUpdateEvent = ExistingVersionLegacyRev
		}
		opts := PutDocOptions{
			NewDoc:                         newDoc,
			RevTreeHistory:                 history,
			ForceAllowConflictingTombstone: forceAllowConflictingTombstone,
			ExistingDoc:                    rawBucketDoc,
			DocUpdateEvent:                 docUpdateEvent,
		}
		if bh.conflictResolver.revTreeConflictResolver != nil {
			opts.ConflictResolver = bh.conflictResolver.revTreeConflictResolver
			opts.NoConflicts = true
		} else {
			opts.NoConflicts = revNoConflicts
		}
		_, _, err = bh.collection.PutExistingRevWithConflictResolution(bh.loggingCtx, opts)
	}
	if err != nil {
		return err
	}

	if bh.collectionCtx.sgr2PullProcessedSeqCallback != nil {
		seqProperty := rq.Properties[RevMessageSequence]
		seq, err := ParseJSONSequenceID(seqProperty)
		if err != nil {
			base.WarnfCtx(bh.loggingCtx, "Unable to parse sequence %q from rev message: %v - not tracking for checkpointing", seqProperty, err)
		} else {
			bh.collectionCtx.sgr2PullProcessedSeqCallback(&seq, IDAndRev{DocID: docID, RevID: rev})
		}
	}

	return nil
}

// Handler for when a rev is received from the client
func (bh *blipHandler) handleRev(rq *blip.Message) (err error) {
	stats := processRevStats{
		count:            bh.replicationStats.HandleRevCount,
		errorCount:       bh.replicationStats.HandleRevErrorCount,
		deltaRecvCount:   bh.replicationStats.HandleRevDeltaRecvCount,
		bytes:            bh.replicationStats.HandleRevBytes,
		processingTime:   bh.replicationStats.HandleRevProcessingTime,
		docsPurgedCount:  bh.replicationStats.HandleRevDocsPurgedCount,
		throttledRevs:    bh.replicationStats.HandleRevThrottledCount,
		throttledRevTime: bh.replicationStats.HandleRevThrottledTime,
	}
	return bh.processRev(rq, &stats)
}

// ////// ATTACHMENTS:

func (bh *blipHandler) handleProveAttachment(rq *blip.Message) error {
	nonce, err := rq.Body()
	if err != nil {
		return err
	}

	if len(nonce) == 0 {
		return base.HTTPErrorf(http.StatusBadRequest, "no nonce sent with proveAttachment")
	}

	digest, ok := rq.Properties[ProveAttachmentDigest]
	if !ok {
		return base.HTTPErrorf(http.StatusBadRequest, "no digest sent with proveAttachment")
	}

	allowedAttachment := bh.allowedAttachment(digest)
	attachmentKey := MakeAttachmentKey(allowedAttachment.version, allowedAttachment.docID, digest)
	attData, err := bh.collection.GetAttachment(attachmentKey)
	if err != nil {
		if bh.clientType == BLIPClientTypeSGR2 {
			return ErrAttachmentNotFound
		}
		if base.IsDocNotFoundError(err) {
			return ErrAttachmentNotFound
		}
		return base.HTTPErrorf(http.StatusInternalServerError, "Error getting client attachment: %v", err)
	}

	proof := ProveAttachment(bh.loggingCtx, attData, nonce)

	resp := rq.Response()
	resp.SetBody([]byte(proof))

	bh.replicationStats.HandleProveAttachment.Add(1)

	return nil
}

// Received a "getAttachment" request
func (bh *blipHandler) handleGetAttachment(rq *blip.Message) error {

	getAttachmentParams := newGetAttachmentParams(rq)
	bh.logEndpointEntry(rq.Profile(), getAttachmentParams.String())

	digest := getAttachmentParams.digest()
	if digest == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing 'digest'")
	}

	docID := ""
	attachmentAllowedKey := digest
	if bh.activeCBMobileSubprotocol >= CBMobileReplicationV3 {
		docID = getAttachmentParams.docID()
		if docID == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "Missing 'docID'")
		}
		attachmentAllowedKey = docID + digest
	}

	// attachmentName should only be used for logging, it is one possible name of an attachment for a given document if multiple attachments share the same digest.
	allowedAttachment := bh.allowedAttachment(attachmentAllowedKey)
	if allowedAttachment.counter <= 0 {
		return base.HTTPErrorf(http.StatusForbidden, "Attachment's doc not being synced")
	}

	if bh.activeCBMobileSubprotocol <= CBMobileReplicationV2 {
		docID = allowedAttachment.docID
	}

	attachmentKey := MakeAttachmentKey(allowedAttachment.version, docID, digest)
	attachment, err := bh.collection.GetAttachment(attachmentKey)
	if err != nil {
		return err

	}
	base.DebugfCtx(bh.loggingCtx, base.KeySync, "Sending attachment with digest=%q (%.2f KB)", digest, float64(len(attachment))/float64(1024))
	response := rq.Response()
	response.SetBody(attachment)
	response.SetCompressed(rq.Properties[BlipCompress] == trueProperty)
	bh.replicationStats.HandleGetAttachment.Add(1)
	bh.replicationStats.HandleGetAttachmentBytes.Add(int64(len(attachment)))
	base.Audit(bh.loggingCtx, base.AuditIDAttachmentRead, base.AuditFields{
		base.AuditFieldDocID:        docID,
		base.AuditFieldDocVersion:   allowedAttachment.docVersion,
		base.AuditFieldAttachmentID: allowedAttachment.name,
	})

	return nil
}

var errNoBlipHandler = fmt.Errorf("404 - No handler for BLIP request")

// sendGetAttachment requests the full attachment from the peer.
func (bh *blipHandler) sendGetAttachment(sender *blip.Sender, docID string, name string, digest string, meta map[string]interface{}) ([]byte, error) {
	base.DebugfCtx(bh.loggingCtx, base.KeySync, "    Asking for attachment %q for doc %s (digest %s)", base.UD(name), base.UD(docID), digest)
	outrq := blip.NewRequest()
	outrq.SetProfile(MessageGetAttachment)
	outrq.Properties[GetAttachmentDigest] = digest
	if bh.collectionIdx != nil {
		outrq.Properties[BlipCollection] = strconv.Itoa(*bh.collectionIdx)
	}
	if isCompressible(name, meta) {
		outrq.Properties[BlipCompress] = trueProperty
	}

	if bh.activeCBMobileSubprotocol >= CBMobileReplicationV3 {
		outrq.Properties[GetAttachmentID] = docID
	}

	if !bh.sendBLIPMessage(sender, outrq) {
		return nil, ErrClosedBLIPSender
	}

	resp := outrq.Response()

	respBody, err := resp.Body()
	if err != nil {
		return nil, err
	}

	if resp.Properties[BlipErrorCode] != "" {
		return nil, fmt.Errorf("error %s from getAttachment: %s", resp.Properties[BlipErrorCode], respBody)
	}
	lNum, metaLengthOK := meta["length"]
	if !metaLengthOK {
		return nil, fmt.Errorf("no attachment length provided in meta")
	}

	metaLength, ok := base.ToInt64(lNum)
	if !ok {
		return nil, fmt.Errorf("invalid attachment length %q found in meta", lNum)
	}

	// Verify that the attachment we received matches the metadata stored in the document
	expectedLength := int(metaLength)
	actualLength := len(respBody)
	if actualLength != expectedLength {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "Incorrect data sent for attachment with digest: %s (length mismatch - expected %d got %d)", digest, expectedLength, actualLength)
	}

	actualDigest := Sha1DigestKey(respBody)
	if actualDigest != digest {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "Incorrect data sent for attachment with digest: %s (digest mismatch - got %s)", digest, actualDigest)
	}

	bh.replicationStats.GetAttachment.Add(1)
	bh.replicationStats.GetAttachmentBytes.Add(metaLength)

	return respBody, nil
}

// sendProveAttachment asks the peer to prove they have the attachment, without actually sending it.
// This is to prevent clients from creating a doc with a digest for an attachment they otherwise can't access, in order to download it.
func (bh *blipHandler) sendProveAttachment(sender *blip.Sender, docID, name, digest string, knownData []byte) error {
	base.DebugfCtx(bh.loggingCtx, base.KeySync, "    Verifying attachment %q for doc %s (digest %s)", base.UD(name), base.UD(docID), digest)
	nonce, proof, err := GenerateProofOfAttachment(bh.loggingCtx, knownData)
	if err != nil {
		return err
	}
	outrq := blip.NewRequest()
	outrq.SetProfile(MessageProveAttachment)
	outrq.Properties[ProveAttachmentDigest] = digest
	if bh.collectionIdx != nil {
		outrq.Properties[BlipCollection] = strconv.Itoa(*bh.collectionIdx)
	}
	outrq.SetBody(nonce)
	if !bh.sendBLIPMessage(sender, outrq) {
		return ErrClosedBLIPSender
	}

	resp := outrq.Response()

	body, err := resp.Body()
	if err != nil {
		base.WarnfCtx(bh.loggingCtx, "Error returned for proveAttachment message for doc %s (digest %s).  Error: %v", base.UD(docID), digest, err)
		return err
	}

	if resp.Type() == blip.ErrorType &&
		resp.Properties[BlipErrorDomain] == blip.BLIPErrorDomain &&
		resp.Properties[BlipErrorCode] == "404" {
		return errNoBlipHandler
	}

	if resp.Type() == blip.ErrorType &&
		errorDomainIsHTTP(resp) &&
		resp.Properties[BlipErrorCode] == "404" {
		return ErrAttachmentNotFound
	}

	if string(body) != proof {
		base.WarnfCtx(bh.loggingCtx, "Incorrect proof for attachment %s : I sent nonce %x, expected proof %q, got %q", digest, base.MD(nonce), base.MD(proof), base.MD(string(body)))
		return base.HTTPErrorf(http.StatusForbidden, "Incorrect proof for attachment %s", digest)
	}

	bh.replicationStats.ProveAttachment.Add(1)

	base.InfofCtx(bh.loggingCtx, base.KeySync, "proveAttachment successful for doc %s (digest %s)", base.UD(docID), digest)
	return nil
}

// For each attachment in the revision, makes sure it's in the database, asking the client to
// upload it if necessary. This method blocks until all the attachments have been processed.
func (bh *blipHandler) downloadOrVerifyAttachments(sender *blip.Sender, body Body, minRevpos int, docID string, currentDigests map[string]string) error {
	return bh.collection.ForEachStubAttachment(body, minRevpos, docID, currentDigests, func(name string, digest string, knownData []byte, meta map[string]interface{}) ([]byte, error) {
		// Request attachment if we don't have it
		if knownData == nil {
			return bh.sendGetAttachment(sender, docID, name, digest, meta)
		}

		// Ask client to prove they have the attachment without sending it
		proveAttErr := bh.sendProveAttachment(sender, docID, name, digest, knownData)
		if proveAttErr == nil {
			return nil, nil
		}

		// Peer doesn't support proveAttachment or does not have attachment. Fall back to using getAttachment as proof.
		if proveAttErr == errNoBlipHandler || proveAttErr == ErrAttachmentNotFound {
			base.InfofCtx(bh.loggingCtx, base.KeySync, "Peer sent prove attachment error %v, falling back to getAttachment for proof in doc %s (digest %s)", proveAttErr, base.UD(docID), digest)
			_, getAttErr := bh.sendGetAttachment(sender, docID, name, digest, meta)
			if getAttErr == nil {
				// Peer proved they have matching attachment. Keep existing attachment
				return nil, nil
			}
			return nil, getAttErr
		}

		return nil, proveAttErr
	})
}

func (bsc *BlipSyncContext) incrementSerialNumber() uint64 {
	return atomic.AddUint64(&bsc.handlerSerialNumber, 1)
}

func (bsc *BlipSyncContext) addAllowedAttachments(docID string, docVersion string, attMeta []AttachmentStorageMeta, activeSubprotocol CBMobileSubprotocolVersion) {
	if len(attMeta) == 0 {
		return
	}

	bsc.allowedAttachmentsLock.Lock()
	defer bsc.allowedAttachmentsLock.Unlock()

	if bsc.allowedAttachments == nil {
		bsc.allowedAttachments = make(map[string]AllowedAttachment, 100)
	}
	for _, attachment := range attMeta {
		key := allowedAttachmentKey(docID, attachment.digest, activeSubprotocol)
		att, found := bsc.allowedAttachments[key]
		if found {
			att.counter++
			bsc.allowedAttachments[key] = att
		} else {
			bsc.allowedAttachments[key] = AllowedAttachment{
				version:    attachment.version,
				counter:    1,
				docID:      docID,
				docVersion: docVersion,
				name:       attachment.name,
			}
		}
	}

	base.TracefCtx(bsc.loggingCtx, base.KeySync, "addAllowedAttachments, added: %v current set: %v", attMeta, bsc.allowedAttachments)
}

func (bsc *BlipSyncContext) removeAllowedAttachments(docID string, attMeta []AttachmentStorageMeta, activeSubprotocol CBMobileSubprotocolVersion) {
	if len(attMeta) == 0 {
		return
	}

	bsc.allowedAttachmentsLock.Lock()
	defer bsc.allowedAttachmentsLock.Unlock()

	for _, attachment := range attMeta {
		key := allowedAttachmentKey(docID, attachment.digest, activeSubprotocol)
		att, found := bsc.allowedAttachments[key]
		if found {
			if n := att.counter; n > 1 {
				att.counter = n - 1
				bsc.allowedAttachments[key] = att
			} else {
				delete(bsc.allowedAttachments, key)
			}
		}
	}

	base.TracefCtx(bsc.loggingCtx, base.KeySync, "removeAllowedAttachments, removed: %v current set: %v", attMeta, bsc.allowedAttachments)
}

func allowedAttachmentKey(docID, digest string, activeCBMobileSubprotocol CBMobileSubprotocolVersion) string {
	if activeCBMobileSubprotocol >= CBMobileReplicationV3 {
		return docID + digest
	}
	return digest
}

func (bh *blipHandler) logEndpointEntry(profile, endpoint string) {
	base.InfofCtx(bh.loggingCtx, base.KeySyncMsg, "#%d: Type:%s %s", bh.serialNumber, profile, endpoint)
}

// GetHLVFromRevMessage extracts the full HLV from a rev message. This will fail the test if the message does not contain a valid HLV.
//
// Function will return list of revIDs if legacy revtree IDs were found in the HLV history section (PV)
func GetHLVFromRevMessage(msg *blip.Message) (*HybridLogicalVector, []string, error) {
	revID := msg.Properties[RevMessageRev]
	if revID == "" {
		return nil, nil, errors.New("RevID property is empty")
	}
	versionVectorStr := revID
	historyStr := msg.Properties[RevMessageHistory]
	if historyStr != "" {
		// this means that there is a mv
		if strings.Contains(historyStr, ";") {
			versionVectorStr += "," + historyStr
		} else {
			versionVectorStr += ";" + historyStr
		}
	}
	return extractHLVFromBlipString(versionVectorStr)
}
