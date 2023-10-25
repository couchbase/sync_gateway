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
	"sort"
	"strings"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// ImportListener manages the import DCP feed.  ProcessFeedEvent is triggered for each feed events,
// and invokes ImportFeedEvent for any event that's eligible for import handling.
type importListener struct {
	bucketName       string                                // Used for logging
	terminator       chan bool                             // Signal to cause DCP Client.Close() to be called, which removes dcp receiver
	dbName           string                                // used for naming the DCP feed
	bucket           base.Bucket                           // bucket to get vb stats for feed
	collections      map[uint32]DatabaseCollectionWithUser // Admin databases used for import, keyed by collection ID (CB-server-side)
	dbStats          *base.DatabaseStats                   // Database stats group
	importStats      *base.SharedBucketImportStats         // import stats group
	metadataKeys     *base.MetadataKeys
	cbgtContext      *base.CbgtContext // Handle to cbgt manager,cfg
	checkpointPrefix string            // DCP checkpoint key prefix
	loggingCtx       context.Context   // ctx for logging on event callbacks
	importDestKey    string            // cbgt index name
}

// NewImportListener constructs an object to start an import feed.
func NewImportListener(ctx context.Context, checkpointPrefix string, dbContext *DatabaseContext) *importListener {
	importListener := &importListener{
		bucket:           dbContext.Bucket,
		bucketName:       dbContext.Bucket.GetName(),
		dbName:           dbContext.Name,
		dbStats:          dbContext.DbStats.Database(),
		checkpointPrefix: checkpointPrefix,
		collections:      make(map[uint32]DatabaseCollectionWithUser),
		importStats:      dbContext.DbStats.SharedBucketImport(),
		loggingCtx:       ctx,
		metadataKeys:     dbContext.MetadataKeys,
		terminator:       make(chan bool),
	}

	return importListener
}

// StartImportFeed starts an import DCP feed.  Always starts the feed based on previous checkpoints (Backfill:FeedResume).
// Writes DCP stats into the StatKeyImportDcpStats map
func (il *importListener) StartImportFeed(dbContext *DatabaseContext) (err error) {

	collectionNamesByScope := make(map[string][]string)
	var scopeName string

	if len(dbContext.Scopes) > 1 {
		return fmt.Errorf("multiple scopes not supported")
	}
	for sn := range dbContext.Scopes {
		scopeName = sn
	}

	for collectionID, collection := range dbContext.CollectionByID {
		il.collections[collectionID] = DatabaseCollectionWithUser{
			DatabaseCollection: collection,
			user:               nil, // admin
		}
		if il.bucket.IsSupported(sgbucket.BucketStoreFeatureCollections) && !dbContext.OnlyDefaultCollection() {
			collectionNamesByScope[collection.ScopeName] = append(collectionNamesByScope[collection.ScopeName], collection.Name)
		}
	}
	sort.Strings(collectionNamesByScope[scopeName])
	if dbContext.OnlyDefaultCollection() {
		il.importDestKey = base.ImportDestKey(il.dbName, "", []string{})
	} else {
		il.importDestKey = base.ImportDestKey(il.dbName, scopeName, collectionNamesByScope[scopeName])
	}
	feedArgs := sgbucket.FeedArguments{
		ID:               base.DCPImportFeedID,
		Backfill:         sgbucket.FeedResume,
		Terminator:       il.terminator,
		DoneChan:         make(chan struct{}),
		CheckpointPrefix: il.checkpointPrefix,
		Scopes:           collectionNamesByScope,
	}

	base.InfofCtx(il.loggingCtx, base.KeyDCP, "Attempting to start import DCP feed %v...", base.MD(il.importDestKey))

	importFeedStatsMap := dbContext.DbStats.Database().ImportFeedMapStats

	// Store the listener in global map for dbname-based retrieval by cbgt prior to index registration
	base.StoreDestFactory(il.loggingCtx, il.importDestKey, il.NewImportDest)

	// Start DCP mutation feed
	base.InfofCtx(il.loggingCtx, base.KeyImport, "Starting DCP import feed for bucket: %q ", base.UD(il.bucket.GetName()))

	// TODO: need to clean up StartDCPFeed to push bucket dependencies down
	cbStore, ok := base.AsCouchbaseBucketStore(il.bucket)
	if !ok {
		// walrus is not a couchbasestore
		return il.bucket.StartDCPFeed(il.loggingCtx, feedArgs, il.ProcessFeedEvent, importFeedStatsMap.Map)
	}

	if !base.IsEnterpriseEdition() {
		groupID := ""
		gocbv2Bucket, err := base.AsGocbV2Bucket(il.bucket)
		if err != nil {
			return err
		}
		return base.StartGocbDCPFeed(il.loggingCtx, gocbv2Bucket, il.bucket.GetName(), feedArgs, il.ProcessFeedEvent, importFeedStatsMap.Map, base.DCPMetadataStoreCS, groupID)
	}

	il.cbgtContext, err = base.StartShardedDCPFeed(il.loggingCtx, dbContext.Name, dbContext.Options.GroupID, dbContext.UUID, dbContext.Heartbeater,
		il.bucket, cbStore.GetSpec(), scopeName, collectionNamesByScope[scopeName], dbContext.Options.ImportOptions.ImportPartitions, dbContext.CfgSG)
	return err
}

// ProcessFeedEvent is invoked for each mutate or delete event seen on the server's mutation feed.  It may be
// executed concurrently for multiple events from different vbuckets.  Filters out
// internal documents based on key, then checks sync metadata to determine whether document needs to be imported.
// Returns true if the checkpoints should be persisted.
func (il *importListener) ProcessFeedEvent(event sgbucket.FeedEvent) (shouldPersistCheckpoint bool) {

	ctx := il.loggingCtx
	// Ignore non-mutation/deletion events
	if event.Opcode != sgbucket.FeedOpMutation && event.Opcode != sgbucket.FeedOpDeletion {
		return true
	}
	docID := string(event.Key)

	collection, ok := il.collections[event.CollectionID]
	if !ok {
		base.WarnfCtx(ctx, "Received import event for unrecognised collection 0x%x", event.CollectionID)
		return true
	}
	ctx = base.CollectionLogCtx(ctx, collection.Name)

	// Ignore internal documents
	if strings.HasPrefix(docID, base.SyncDocPrefix) {
		// Ignore all DCP checkpoints no matter config group ID
		return !strings.HasPrefix(docID, base.DCPCheckpointRootPrefix)
	}

	// If this is a delete and there are no xattrs (no existing SG revision), we shouldn't import
	if event.Opcode == sgbucket.FeedOpDeletion && len(event.Value) == 0 {
		base.DebugfCtx(ctx, base.KeyImport, "Ignoring delete mutation for %s - no existing Sync Gateway metadata.", base.UD(docID))
		return true
	}

	// If this is a binary document we can ignore, but update checkpoint to avoid reprocessing upon restart
	if event.DataType == base.MemcachedDataTypeRaw {
		base.InfofCtx(ctx, base.KeyImport, "Ignoring binary mutation event for %s.", base.UD(docID))
		return true
	}

	il.ImportFeedEvent(ctx, &collection, event)
	return true
}

func (il *importListener) ImportFeedEvent(ctx context.Context, collection *DatabaseCollectionWithUser, event sgbucket.FeedEvent) {
	var importAttempt bool
	startTime := time.Now()
	defer func() {
		// if we aren't attempting to import a doc we don't want a calculation to happen
		if !importAttempt {
			return
		}
		functionTime := time.Since(startTime).Milliseconds()
		bytes := len(event.Value)
		stat := CalculateComputeStat(int64(bytes), functionTime)
		il.dbStats.ImportProcessCompute.Add(stat)
	}()

	syncData, rawBody, rawXattr, rawUserXattr, err := UnmarshalDocumentSyncDataFromFeed(event.Value, event.DataType, collection.userXattrKey(), false)
	if err != nil {
		if err == base.ErrEmptyMetadata {
			base.WarnfCtx(ctx, "Unexpected empty metadata when processing feed event.  docid: %s opcode: %v datatype:%v", base.UD(event.Key), event.Opcode, event.DataType)
		} else {
			base.WarnfCtx(ctx, "Found sync metadata, but unable to unmarshal for feed document %q.  Will not be imported.  Error: %v", base.UD(event.Key), err)
		}
		il.importStats.ImportErrorCount.Add(1)
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
		importAttempt = true
		isDelete := event.Opcode == sgbucket.FeedOpDeletion
		if isDelete {
			rawBody = nil
		}
		docID := string(event.Key)

		// last attempt to exit processing if the importListener has been closed before attempting to write to the bucket
		select {
		case <-il.terminator:
			base.InfofCtx(ctx, base.KeyImport, "Aborting import for doc %q - importListener.terminator was closed", base.UD(docID))
			return
		default:
		}

		_, err := collection.ImportDocRaw(ctx, docID, rawBody, rawXattr, rawUserXattr, isDelete, event.Cas, &event.Expiry, ImportFromFeed)
		if err != nil {
			if err == base.ErrImportCasFailure {
				base.DebugfCtx(ctx, base.KeyImport, "Not importing mutation - document %s has been subsequently updated and will be imported based on that mutation.", base.UD(docID))
			} else if err == base.ErrImportCancelledFilter {
				// No logging required - filter info already logged during importDoc
			} else {
				base.DebugfCtx(ctx, base.KeyImport, "Did not import doc %q - external update will not be accessible via Sync Gateway.  Reason: %v", base.UD(docID), err)
			}
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

func (db *DatabaseContext) PartitionCount() int {
	il := db.ImportListener
	_, pindexes := il.cbgtContext.Manager.CurrentMaps()
	return len(pindexes)
}
