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
	terminator       chan bool                             // Signal to cause cbdatasource bucketdatasource.Close() to be called, which removes dcp receiver
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
	base.InfofCtx(il.loggingCtx, base.KeyDCP, "Starting DCP import feed for bucket: %q ", base.UD(il.bucket.GetName()))

	// TODO: need to clean up StartDCPFeed to push bucket dependencies down
	cbStore, ok := base.AsCouchbaseBucketStore(il.bucket)
	if !ok {
		// walrus is not a couchbasestore
		return il.bucket.StartDCPFeed(feedArgs, il.ProcessFeedEvent, importFeedStatsMap.Map)
	}

	if !base.IsEnterpriseEdition() {
		groupID := ""
		gocbv2Bucket, err := base.AsGocbV2Bucket(il.bucket)
		if err != nil {
			return err
		}
		return base.StartGocbDCPFeed(gocbv2Bucket, il.bucket.GetName(), feedArgs, il.ProcessFeedEvent, importFeedStatsMap.Map, base.DCPMetadataStoreCS, groupID)
	}

	il.cbgtContext, err = base.StartShardedDCPFeed(il.loggingCtx, dbContext.Name, dbContext.Options.GroupID, dbContext.UUID, dbContext.Heartbeater,
		il.bucket, cbStore.GetSpec(), scopeName, collectionNamesByScope[scopeName], dbContext.Options.ImportOptions.ImportPartitions, dbContext.CfgSG)
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
		return !strings.HasPrefix(key, base.DCPCheckpointRootPrefix)
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

// calculateImportCompute calculates the import Process compute stat. NOTE: this current uses MiB as the MB unit to compute the stat
// to use MB (Si) unit we must change the 'MiB' constant used here to 1000000
func calculateImportCompute(bytes, functionTime float64) float64 {
	timeMS := functionTime * float64(1000)
	eventMb := bytes / base.MiB
	stat := timeMS * (eventMb / 32 * base.MiB)
	return stat
}

func (il *importListener) ImportFeedEvent(event sgbucket.FeedEvent) {
	var importAttempt bool
	startTime := time.Now()
	defer func() {
		// if we aren't attempting to import doc we don't want a calculation to happen
		if !importAttempt {
			return
		}
		// we must grab the time in seconds here and convert to ms as the .Milliseconds() function returns integer millisecond count
		functionTime := time.Since(startTime).Seconds()
		bytes := float64(len(event.Value))
		stat := calculateImportCompute(bytes, functionTime)
		il.importStats.ImportProcessCompute.Add(stat)
	}()
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
		importAttempt = true
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
			il.cbgtContext.Stop()

			// Remove entry from global listener directory
			base.RemoveDestFactory(il.importDestKey)

			// TODO: Shut down the cfg (when cfg supports)
		}
		close(il.terminator)
	}
}
