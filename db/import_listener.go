/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"fmt"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// ImportListener manages the import DCP feed.  ProcessFeedEvent is triggered for each feed events,
// and invokes ImportFeedEvent for any event that's eligible for import handling.
type importListener struct {
	bucketName       string                                // Used for logging
	terminator       chan bool                             // Signal to cause cbdatasource bucketdatasource.Close() to be called, which removes dcp receiver
	dbName           string                                // used for naming the DCP feed
	metaStore        base.Bucket                           // collection to store DCP metadata
	collections      map[uint32]DatabaseCollectionWithUser // Admin databases used for import, keyed by collection ID (CB-server-side)
	dbStats          *base.DatabaseStats                   // Database stats group
	importStats      *base.SharedBucketImportStats         // import stats group
	cbgtContext      *base.CbgtContext                     // Handle to cbgt manager,cfg
	checkpointPrefix string                                // DCP checkpoint key prefix
	loggingCtx       context.Context                       // ctx for logging on event callbacks
}

func NewImportListener(groupID string) *importListener {
	importListener := &importListener{
		terminator:       make(chan bool),
		checkpointPrefix: base.DCPCheckpointPrefixWithGroupID(groupID),
	}
	return importListener
}

// StartImportFeed starts an import DCP feed.  Always starts the feed based on previous checkpoints (Backfill:FeedResume).
// Writes DCP stats into the StatKeyImportDcpStats map
func (il *importListener) StartImportFeed(ctx context.Context, bucket base.Bucket, dbStats *base.DbStats, dbContext *DatabaseContext) (err error) {
	il.bucketName = bucket.GetName()
	il.dbName = dbContext.Name
	il.loggingCtx = ctx
	il.metaStore = dbContext.Bucket // FIXME(CBG-2266): use proper metadata collection
	il.collections = make(map[uint32]DatabaseCollectionWithUser)
	il.dbStats = dbStats.Database()
	il.importStats = dbStats.SharedBucketImport()

	collectionNamesByScope := make(map[string][]string)
	var scopeName string

	if len(dbContext.Scopes) > 1 {
		return fmt.Errorf("multiple scopes not supported")
	}
	for sn := range dbContext.Scopes {
		scopeName = sn
	}

	if !dbContext.onlyDefaultCollection() {
		coll, err := base.AsCollection(bucket)
		if err != nil {
			return fmt.Errorf("configured with named collections, but bucket is not collection: %w", err)
		}
		collectionManifest, err := coll.GetCollectionManifest()
		if err != nil {
			return fmt.Errorf("failed to load collection manifest: %w", err)
		}

		collectionNamesByScope[scopeName] = make([]string, 0, len(dbContext.Scopes[scopeName].Collections))
		for collName, _ := range dbContext.Scopes[scopeName].Collections {
			collectionNamesByScope[scopeName] = append(collectionNamesByScope[scopeName], collName)

			collID, ok := base.GetIDForCollection(collectionManifest, scopeName, collName)
			if !ok {
				return fmt.Errorf("failed to find ID for collection %s.%s", base.MD(scopeName).Redact(), base.MD(collName).Redact())
			}
			dbCollection := dbContext.CollectionByID[collID]
			if dbCollection == nil {
				return fmt.Errorf("failed to find collection for  %s.%s", base.MD(scopeName).Redact(), base.MD(collName).Redact())

			}
			il.collections[collID] = DatabaseCollectionWithUser{
				DatabaseCollection: dbCollection,
				user:               nil, // admin
			}
		}
	} else {
		collectionID := base.DefaultCollectionID
		il.collections[collectionID] = DatabaseCollectionWithUser{
			DatabaseCollection: dbContext.CollectionByID[collectionID],
			user:               nil, // admin
		}

	}

	feedArgs := sgbucket.FeedArguments{
		ID:               base.DCPImportFeedID,
		Backfill:         sgbucket.FeedResume,
		Terminator:       il.terminator,
		DoneChan:         make(chan struct{}),
		CheckpointPrefix: il.checkpointPrefix,
		Scopes:           collectionNamesByScope,
	}

	base.InfofCtx(ctx, base.KeyDCP, "Attempting to start import DCP feed %v...", base.MD(base.ImportDestKey(il.dbName)))

	importFeedStatsMap := dbContext.DbStats.Database().ImportFeedMapStats

	// Store the listener in global map for dbname-based retrieval by cbgt prior to index registration
	base.StoreDestFactory(ctx, base.ImportDestKey(il.dbName), il.NewImportDest)

	// Start DCP mutation feed
	base.InfofCtx(ctx, base.KeyDCP, "Starting DCP import feed for bucket: %q ", base.UD(bucket.GetName()))

	// TODO: need to clean up StartDCPFeed to push bucket dependencies down
	cbStore, ok := base.AsCouchbaseStore(bucket)
	if !ok {
		// walrus is not a couchbasestore
		return bucket.StartDCPFeed(feedArgs, il.ProcessFeedEvent, importFeedStatsMap.Map)
	}
	if !base.IsEnterpriseEdition() {
		groupID := ""
		collection, err := base.AsCollection(bucket)
		if err != nil {
			return err
		}
		return base.StartGocbDCPFeed(collection, bucket.GetName(), feedArgs, il.ProcessFeedEvent, importFeedStatsMap.Map, base.DCPMetadataStoreCS, groupID)
	}
	il.cbgtContext, err = base.StartShardedDCPFeed(ctx, dbContext.Name, dbContext.Options.GroupID, dbContext.UUID, dbContext.Heartbeater,
		bucket, cbStore.GetSpec(), scopeName, collectionNamesByScope[scopeName], dbContext.Options.ImportOptions.ImportPartitions, dbContext.CfgSG)
	return err
}

// ProcessFeedEvent is invoked for each mutate or delete event seen on the server's mutation feed.  It may be
// executed concurrently for multiple events from different vbuckets.  Filters out
// internal documents based on key, then checks sync metadata to determine whether document needs to be imported
func (il *importListener) ProcessFeedEvent(event sgbucket.FeedEvent) (shouldPersistCheckpoint bool) {

	// Ignore non-mutation/deletion events
	if event.Opcode != sgbucket.FeedOpMutation && event.Opcode != sgbucket.FeedOpDeletion {
		return true
	}
	key := string(event.Key)

	// Ignore internal documents
	if strings.HasPrefix(key, base.SyncDocPrefix) {
		// Ignore all DCP checkpoints no matter config group ID
		if strings.HasPrefix(key, base.DCPCheckpointPrefixWithoutGroupID) {
			return false
		}
		return true
	}

	// If this is a delete and there are no xattrs (no existing SG revision), we shouldn't import
	if event.Opcode == sgbucket.FeedOpDeletion && len(event.Value) == 0 {
		base.DebugfCtx(il.loggingCtx, base.KeyImport, "Ignoring delete mutation for %s - no existing Sync Gateway metadata.", base.UD(event.Key))
		return true
	}

	// If this is a binary document we can ignore, but update checkpoint to avoid reprocessing upon restart
	if event.DataType == base.MemcachedDataTypeRaw {
		return true
	}

	il.ImportFeedEvent(event)
	return true
}

func (il *importListener) ImportFeedEvent(event sgbucket.FeedEvent) {
	// Unmarshal the doc metadata (if present) to determine if this mutation requires import.
	collectionCtx, ok := il.collections[event.CollectionID]
	if !ok {
		base.WarnfCtx(il.loggingCtx, "Received import event for unrecognised collection 0x%x", event.CollectionID)
		return
	}

	syncData, rawBody, rawXattr, rawUserXattr, err := UnmarshalDocumentSyncDataFromFeed(event.Value, event.DataType, collectionCtx.userXattrKey(), false)
	if err != nil {
		base.DebugfCtx(il.loggingCtx, base.KeyImport, "Found sync metadata, but unable to unmarshal for feed document %q.  Will not be imported.  Error: %v", base.UD(event.Key), err)
		if err == base.ErrEmptyMetadata {
			base.WarnfCtx(il.loggingCtx, "Unexpected empty metadata when processing feed event.  docid: %s opcode: %v datatype:%v", base.UD(event.Key), event.Opcode, event.DataType)
		}
		return
	}

	var isSGWrite bool
	var crc32Match bool
	if syncData != nil {
		isSGWrite, crc32Match, _ = syncData.IsSGWrite(event.Cas, rawBody, rawUserXattr)
		if crc32Match {
			il.dbStats.Crc32MatchCount.Add(1)
		}
	}

	// If syncData is nil, or if this was not an SG write, attempt to import
	if syncData == nil || !isSGWrite {
		isDelete := event.Opcode == sgbucket.FeedOpDeletion
		if isDelete {
			rawBody = nil
		}
		docID := string(event.Key)

		// last attempt to exit processing if the importListener has been closed before attempting to write to the bucket
		select {
		case <-il.terminator:
			base.InfofCtx(il.loggingCtx, base.KeyImport, "Aborting import for doc %q - importListener.terminator was closed", base.UD(docID))
			return
		default:
		}

		_, err := collectionCtx.ImportDocRaw(il.loggingCtx, docID, rawBody, rawXattr, rawUserXattr, isDelete, event.Cas, &event.Expiry, ImportFromFeed)
		if err != nil {
			if err == base.ErrImportCasFailure {
				base.DebugfCtx(il.loggingCtx, base.KeyImport, "Not importing mutation - document %s has been subsequently updated and will be imported based on that mutation.", base.UD(docID))
			} else if err == base.ErrImportCancelledFilter {
				// No logging required - filter info already logged during importDoc
			} else {
				base.DebugfCtx(il.loggingCtx, base.KeyImport, "Did not import doc %q - external update will not be accessible via Sync Gateway.  Reason: %v", base.UD(docID), err)
			}
		}
	}
}

func (il *importListener) Stop() {
	if il != nil {
		if il.cbgtContext != nil {
			il.cbgtContext.StopHeartbeatListener()

			// Close open PIndexes before stopping the manager.
			_, pindexes := il.cbgtContext.Manager.CurrentMaps()
			for _, pIndex := range pindexes {
				err := il.cbgtContext.Manager.ClosePIndex(pIndex)
				if err != nil {
					base.DebugfCtx(il.loggingCtx, base.KeyImport, "Error closing pindex: %v", err)
				}
			}
			// ClosePIndex calls are synchronous, so can stop manager once they've completed
			il.cbgtContext.Manager.Stop()
			il.cbgtContext.RemoveFeedCredentials(il.dbName)

			// Remove entry from global listener directory
			base.RemoveDestFactory(base.ImportDestKey(il.dbName))

			// TODO: Shut down the cfg (when cfg supports)
		}
		close(il.terminator)
	}
}
