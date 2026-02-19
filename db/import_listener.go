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
	"errors"
	"runtime/debug"
	"strings"

	"github.com/couchbase/cbgt"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// ImportListener manages the import DCP feed.  ProcessFeedEvent is triggered for each feed events,
// and invokes ImportFeedEvent for any event that's eligible for import handling.
type importListener struct {
	terminator       chan bool                             // Signal to cause DCP Client.Close() to be called, which removes dcp receiver
	collections      map[uint32]DatabaseCollectionWithUser // Admin databases used for import, keyed by collection ID (CB-server-side)
	dbStats          *base.DatabaseStats                   // Database stats group
	importStats      *base.SharedBucketImportStats         // import stats group
	cbgtContext      *base.CbgtContext                     // Handle to cbgt manager,cfg
	checkpointPrefix string                                // DCP checkpoint key prefix
	loggingCtx       context.Context                       // ctx for logging on event callbacks
	importDestKey    string                                // cbgt index name
}

// NewImportListener constructs an object to start an import feed.
func NewImportListener(ctx context.Context, checkpointPrefix string, dbContext *DatabaseContext) *importListener {
	importListener := &importListener{
		dbStats:          dbContext.DbStats.Database(),
		checkpointPrefix: checkpointPrefix,
		collections:      make(map[uint32]DatabaseCollectionWithUser),
		importStats:      dbContext.DbStats.SharedBucketImport(),
		loggingCtx:       base.CorrelationIDLogCtx(ctx, base.DCPImportFeedID),

		terminator: make(chan bool),
	}

	return importListener
}

// StartImportFeed starts an import DCP feed.  Always starts the feed based on previous checkpoints (Backfill:FeedResume).
// Writes DCP stats into the StatKeyImportDcpStats map
func (il *importListener) StartImportFeed(dbContext *DatabaseContext) (err error) {

	for collectionID, collection := range dbContext.CollectionByID {
		il.collections[collectionID] = DatabaseCollectionWithUser{
			DatabaseCollection: collection,
			user:               nil, // admin
		}
	}
	collectionNamesByScope := dbContext.collectionNames()
	il.importDestKey = base.ImportDestKey(dbContext.Name, dbContext.scopeName, collectionNamesByScope[dbContext.scopeName])

	base.InfofCtx(il.loggingCtx, base.KeyDCP, "Attempting to start import DCP feed %v...", base.MD(il.importDestKey))

	importFeedStatsMap := dbContext.DbStats.Database().ImportFeedMapStats

	// Store the listener in global map for dbname-based retrieval by cbgt prior to index registration
	base.StoreDestFactory(il.loggingCtx, il.importDestKey, getImportDestFactory(
		il.loggingCtx,
		il.ProcessFeedEvent,
		dbContext,
		il.checkpointPrefix),
	)

	// Start DCP mutation feed
	base.InfofCtx(il.loggingCtx, base.KeyImport, "Starting DCP import feed for bucket: %q ", base.UD(dbContext.Bucket.GetName()))

	if !dbContext.useShardedDCP() {
		feedArgs := sgbucket.FeedArguments{
			ID:               base.DCPImportFeedID,
			Backfill:         sgbucket.FeedResume,
			Terminator:       il.terminator,
			DoneChan:         make(chan struct{}),
			CheckpointPrefix: il.checkpointPrefix,
			Scopes:           collectionNamesByScope,
		}

		return dbContext.Bucket.StartDCPFeed(il.loggingCtx, feedArgs, il.ProcessFeedEvent, importFeedStatsMap.Map)
	}

	il.cbgtContext, err = base.StartShardedDCPFeed(il.loggingCtx, dbContext.Name, dbContext.Options.GroupID, dbContext.UUID, dbContext.Heartbeater,
		dbContext.Bucket, dbContext.BucketSpec, dbContext.scopeName, collectionNamesByScope[dbContext.scopeName], dbContext.Options.ImportOptions.ImportPartitions, dbContext.CfgSG)
	return err
}

// ProcessFeedEvent is invoked for each mutate or delete event seen on the server's mutation feed.  It may be
// executed concurrently for multiple events from different vbuckets.  Filters out
// internal documents based on key, then checks sync metadata to determine whether document needs to be imported.
// Returns true if the checkpoints should be persisted.
func (il *importListener) ProcessFeedEvent(event sgbucket.FeedEvent) bool {
	ctx := il.loggingCtx
	docID := string(event.Key)
	defer func() {
		if r := recover(); r != nil {
			base.PanicRecoveryfCtx(ctx, "[%s] Unexpected panic importing document %s - skipping import: \n %s", r, base.UD(docID), debug.Stack())
			if il.importStats != nil {
				il.importStats.ImportErrorCount.Add(1)
			}
			// if this is in a recover block, the unnamed return value is false, and the checkpoint will not be persisted
		}
	}()
	shouldPersistCheckpoint := true
	// Ignore non-mutation/deletion events
	if event.Opcode != sgbucket.FeedOpMutation && event.Opcode != sgbucket.FeedOpDeletion {
		return shouldPersistCheckpoint
	}

	collection, ok := il.collections[event.CollectionID]
	if !ok {
		base.WarnfCtx(ctx, "Received import event for unrecognised collection 0x%x", event.CollectionID)
		return shouldPersistCheckpoint
	}
	ctx = collection.AddCollectionContext(ctx)

	// Ignore internal documents
	if strings.HasPrefix(docID, base.SyncDocPrefix) {
		// Ignore all DCP checkpoints no matter config group ID
		return !strings.HasPrefix(docID, base.DCPCheckpointRootPrefix)
	}

	// If this is a delete and there are no xattrs (no existing SG revision), we shouldn't import
	if event.Opcode == sgbucket.FeedOpDeletion && len(event.Value) == 0 {
		base.DebugfCtx(ctx, base.KeyImport, "Ignoring delete mutation for %s - no existing Sync Gateway metadata.", base.UD(docID))
		return shouldPersistCheckpoint
	}

	// If this is a binary document we can ignore, but update checkpoint to avoid reprocessing upon restart
	if event.DataType == base.MemcachedDataTypeRaw {
		base.DebugfCtx(ctx, base.KeyImport, "Ignoring binary mutation event for %s.", base.UD(docID))
		return shouldPersistCheckpoint
	}

	il.ImportFeedEvent(ctx, &collection, event)
	il.importStats.ImportFeedProcessedCount.Add(1)
	return shouldPersistCheckpoint
}

func (il *importListener) ImportFeedEvent(ctx context.Context, collection *DatabaseCollectionWithUser, event sgbucket.FeedEvent) {
	rawDoc, syncData, err := UnmarshalDocumentSyncDataFromFeed(event.Value, event.DataType, collection.UserXattrKey(), false)
	if err != nil {
		if errors.Is(err, sgbucket.ErrEmptyMetadata) {
			base.WarnfCtx(ctx, "Unexpected empty metadata when processing feed event.  docid: %s opcode: %v datatype:%v", base.UD(event.Key), event.Opcode, event.DataType)
			il.importStats.ImportErrorCount.Add(1)
			return
		}
		base.DebugfCtx(ctx, base.KeyImport, "%s will not be imported: %v", base.UD(event.Key), err)
		return
	}

	if syncData == nil && event.Opcode == sgbucket.FeedOpDeletion {
		return
	}

	var isSGWrite bool
	var crc32Match bool
	if syncData != nil {
		if !syncData.HasValidSyncData() {
			base.WarnfCtx(ctx, "Invalid sync data for doc %s - not importing.", base.UD(event.Key))
			il.importStats.ImportErrorCount.Add(1)
			return
		}
		var cv *rawHLV
		vv := rawDoc.Xattrs[base.VvXattrName]
		if len(vv) > 0 {
			cv = base.Ptr(rawHLV(vv))
		}

		isSGWrite, crc32Match, _ = syncData.IsSGWrite(ctx, event.Cas, rawDoc.Body, rawDoc.Xattrs[collection.UserXattrKey()], cv)
		if crc32Match {
			il.dbStats.Crc32MatchCount.Add(1)
		}
	}

	docID := string(event.Key)
	// If syncData is nil, or if this was not an SG write, attempt to import
	if syncData == nil || !isSGWrite {
		isDelete := event.Opcode == sgbucket.FeedOpDeletion
		if isDelete {
			rawDoc.Body = nil
		}

		// last attempt to exit processing if the importListener has been closed before attempting to write to the bucket
		select {
		case <-il.terminator:
			base.InfofCtx(ctx, base.KeyImport, "Aborting import for doc %q - importListener.terminator was closed", base.UD(docID))
			return
		default:
		}
		importOpts := importDocOptions{
			isDelete: isDelete,
			mode:     ImportFromFeed,
			expiry:   &event.Expiry,
			revSeqNo: event.RevNo,
		}

		_, err := collection.ImportDocRaw(ctx, docID, rawDoc.Body, rawDoc.Xattrs, importOpts, event.Cas)
		if err != nil {
			if err == base.ErrImportCasFailure {
				base.DebugfCtx(ctx, base.KeyImport, "Not importing mutation - document %s has been subsequently updated and will be imported based on that mutation.", base.UD(docID))
			} else if err == base.ErrImportCancelledFilter {
				// No logging required - filter info already logged during importDoc
			} else {
				base.DebugfCtx(ctx, base.KeyImport, "Did not import doc %q - external update will not be accessible via Sync Gateway.  Reason: %v", base.UD(docID), err)
			}
		}
	} else if syncData != nil && syncData.AttachmentsPre4dot0 != nil {
		base.DebugfCtx(ctx, base.KeyImport, "Attachment metadata found in sync data for doc with id %s, migrating attachment metadata", base.UD(docID))
		// we have attachments to migrate
		err := collection.MigrateAttachmentMetadata(ctx, docID, event.Cas, syncData)
		if err != nil && !base.IsCasMismatch(err) {
			base.WarnfCtx(ctx, "error migrating attachment metadata from sync data to global sync for doc %s. Error: %v", base.UD(docID), err)
		}
	}
}

func (il *importListener) Stop() {
	if il != nil {
		if il.cbgtContext != nil {
			il.cbgtContext.Stop()

			// Remove entry from global listener directory
			base.RemoveDestFactory(il.importDestKey)

			// TODO: Shut down the cfg (when cfg supports)
		}
		close(il.terminator)
	}
}

func (db *DatabaseContext) ImportPartitionCount() int {
	il := db.ImportListener
	_, pindexes := il.cbgtContext.Manager.CurrentMaps()
	return len(pindexes)
}

// getImportDestFactory returns a function to create cbgt.Dest targeting the importListener's ProcessFeedEvent
func getImportDestFactory(
	ctx context.Context,
	callback sgbucket.FeedEventCallbackFunc,
	dbContext *DatabaseContext,
	checkpointPrefix string) func(func(),
) (cbgt.Dest, error) {
	ctx = base.CorrelationIDLogCtx(ctx, base.DCPImportFeedID)
	return func(janitorRollback func()) (cbgt.Dest, error) {
		importFeedStatsMap := dbContext.DbStats.Database().ImportFeedMapStats
		importPartitionStat := dbContext.DbStats.SharedBucketImport().ImportPartitions
		persistCheckpoints := true

		return base.NewDCPDest(
			ctx,
			callback,
			dbContext.MetadataStore,
			dbContext.numVBuckets,
			persistCheckpoints,
			importFeedStatsMap.Map,
			importPartitionStat,
			checkpointPrefix)
	}
}
