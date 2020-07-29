package db

import (
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// ImportListener manages the import DCP feed.  ProcessFeedEvent is triggered for each feed events,
// and invokes ImportFeedEvent for any event that's eligible for import handling.
type importListener struct {
	bucketName  string              // Used for logging
	terminator  chan bool           // Signal to cause cbdatasource bucketdatasource.Close() to be called, which removes dcp receiver
	database    Database            // Admin database instance to be used for import
	stats       *base.DatabaseStats // Database stats group
	cbgtContext *base.CbgtContext   // Handle to cbgt manager,cfg
}

func NewImportListener() *importListener {
	importListener := &importListener{
		terminator: make(chan bool),
	}
	return importListener
}

// StartImportFeed starts an import DCP feed.  Always starts the feed based on previous checkpoints (Backfill:FeedResume).
// Writes DCP stats into the StatKeyImportDcpStats map
func (il *importListener) StartImportFeed(bucket base.Bucket, dbStats *DatabaseStats, dbContext *DatabaseContext) (err error) {

	base.Infof(base.KeyDCP, "Attempting to start import DCP feed...")

	il.bucketName = bucket.GetName()
	il.database = Database{DatabaseContext: dbContext, user: nil}
	il.stats = dbStats.NewStats.DatabaseStats
	feedArgs := sgbucket.FeedArguments{
		ID:         base.DCPImportFeedID,
		Backfill:   sgbucket.FeedResume,
		Terminator: il.terminator,
	}

	importFeedStatsMap := dbContext.DbStats.NewStats.Database().ImportFeedMapStats

	// Register cbgt PIndex to support sharded import.
	il.RegisterImportPindexImpl()

	// Start DCP mutation feed
	base.Infof(base.KeyDCP, "Starting DCP import feed for bucket: %q ", base.UD(bucket.GetName()))

	// TODO: need to clean up StartDCPFeed to push bucket dependencies down
	gocbBucket, ok := base.AsGoCBBucket(bucket)
	if !ok || !base.IsEnterpriseEdition() {
		// Non-gocb bucket or CE, start a non-sharded feed
		return bucket.StartDCPFeed(feedArgs, il.ProcessFeedEvent, importFeedStatsMap.Map)
	} else {
		il.cbgtContext, err = base.StartShardedDCPFeed(dbContext.Name, dbContext.UUID, dbContext.Heartbeater, gocbBucket, dbContext.Options.ImportOptions.ImportPartitions, dbContext.CfgSG)
		return err
	}

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
	if strings.HasPrefix(key, base.SyncPrefix) {
		if strings.HasPrefix(key, base.DCPCheckpointPrefix) {
			return false
		}
		return true
	}

	// If this is a delete and there are no xattrs (no existing SG revision), we shouldn't import
	if event.Opcode == sgbucket.FeedOpDeletion && len(event.Value) == 0 {
		base.Debugf(base.KeyImport, "Ignoring delete mutation for %s - no existing Sync Gateway metadata.", base.UD(event.Key))
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
	syncData, rawBody, rawXattr, err := UnmarshalDocumentSyncDataFromFeed(event.Value, event.DataType, false)
	if err != nil {
		base.Debugf(base.KeyImport, "Found sync metadata, but unable to unmarshal for feed document %q.  Will not be imported.  Error: %v", base.UD(event.Key), err)
		if err == base.ErrEmptyMetadata {
			base.Warnf("Unexpected empty metadata when processing feed event.  docid: %s opcode: %v datatype:%v", base.UD(event.Key), event.Opcode, event.DataType)
		}
		return
	}

	var isSGWrite bool
	var crc32Match bool
	if syncData != nil {
		isSGWrite, crc32Match = syncData.IsSGWrite(event.Cas, rawBody)
		if crc32Match {
			il.stats.Crc32MatchCount.Add(1)
		}
	}

	// If syncData is nil, or if this was not an SG write, attempt to import
	if syncData == nil || !isSGWrite {
		isDelete := event.Opcode == sgbucket.FeedOpDeletion
		if isDelete {
			rawBody = nil
		}
		docID := string(event.Key)
		_, err := il.database.ImportDocRaw(docID, rawBody, rawXattr, isDelete, event.Cas, &event.Expiry, ImportFromFeed)
		if err != nil {
			if err == base.ErrImportCasFailure {
				base.Debugf(base.KeyImport, "Not importing mutation - document %s has been subsequently updated and will be imported based on that mutation.", base.UD(docID))
			} else if err == base.ErrImportCancelledFilter {
				// No logging required - filter info already logged during importDoc
			} else {
				base.Debugf(base.KeyImport, "Did not import doc %q - external update will not be accessible via Sync Gateway.  Reason: %v", base.UD(docID), err)
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
					base.Debugf(base.KeyImport, "Error closing pindex: %v", err)
				}
			}
			// ClosePIndex calls are synchronous, so can stop manager once they've completed
			il.cbgtContext.Manager.Stop()

			// TODO: Shut down the cfg (when cfg supports)
		}
		close(il.terminator)
	}
}
