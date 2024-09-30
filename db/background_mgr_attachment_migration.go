//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
)

type AttachmentMigrationManager struct {
	DocsProcessed base.AtomicInt
	DocsChanged   base.AtomicInt
	MigrationID   string
	CollectionIDs []uint32
	lock          sync.RWMutex
}

var _ BackgroundManagerProcessI = &AttachmentMigrationManager{}

func NewAttachmentMigrationManager(metadataStore base.DataStore, metaKeys *base.MetadataKeys) *BackgroundManager {
	return &BackgroundManager{
		name:    "attachment_migration",
		Process: &AttachmentMigrationManager{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "attachment_migration",
		},
		terminator: base.NewSafeTerminator(),
	}
}

func (a *AttachmentMigrationManager) Init(ctx context.Context, options map[string]interface{}, clusterStatus []byte) error {
	newRunInit := func() error {
		uniqueUUID, err := uuid.NewRandom()
		if err != nil {
			return err
		}

		a.MigrationID = uniqueUUID.String()
		base.InfofCtx(ctx, base.KeyAll, "Attachment Migration: Starting new migration run with migration ID: %q", a.MigrationID)
		return nil
	}

	if clusterStatus != nil {
		var statusDoc AttachmentMigrationManagerStatusDoc
		err := base.JSONUnmarshal(clusterStatus, &statusDoc)

		// If the previous run completed, or there was an error during unmarshalling the status we will start the
		// process from scratch with a new migration ID. Otherwise, we should resume with the migration ID, stats specified in the doc.
		if statusDoc.State == BackgroundProcessStateCompleted || err != nil {
			return newRunInit()
		}
		a.MigrationID = statusDoc.MigrationID
		a.SetStatus(statusDoc.DocsChanged, statusDoc.DocsProcessed)
		a.SetCollectionIDs(statusDoc.CollectionIDs)

		base.InfofCtx(ctx, base.KeyAll, "Attachment Migration: Attempting to resume migration with migration ID: %s", a.MigrationID)

		return nil
	}

	return newRunInit()
}

func (a *AttachmentMigrationManager) Run(ctx context.Context, options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	db := options["database"].(*Database)
	migrationLoggingID := "Migration: " + a.MigrationID

	persistClusterStatus := func() {
		err := persistClusterStatusCallback(ctx)
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to persist cluster status on-demand for attachment migration operation: %v", migrationLoggingID, err)
		}
	}
	defer persistClusterStatus()

	var processFailure error
	failProcess := func(err error, format string, args ...interface{}) bool {
		processFailure = err
		terminator.Close()
		base.WarnfCtx(ctx, format, args...)
		return false
	}

	callback := func(event sgbucket.FeedEvent) bool {
		docID := string(event.Key)
		collection := db.CollectionByID[event.CollectionID]
		base.TracefCtx(ctx, base.KeyAll, "[%s] Received DCP event %d for doc %v", migrationLoggingID, event.Opcode, base.UD(docID))

		// Ignore documents without xattrs, to avoid processing unnecessary documents
		if event.DataType&base.MemcachedDataTypeXattr == 0 {
			return true
		}

		// Don't want to process raw binary docs
		// The binary check should suffice but for additional safety also check for empty bodies
		if event.DataType == base.MemcachedDataTypeRaw || len(event.Value) == 0 {
			return true
		}

		// We only want to process full docs. Not any sync docs.
		if strings.HasPrefix(docID, base.SyncDocPrefix) {
			return true
		}

		a.DocsProcessed.Add(1)
		syncData, _, _, err := UnmarshalDocumentSyncDataFromFeed(event.Value, event.DataType, collection.userXattrKey(), false)
		if err != nil {
			failProcess(err, "error unmarshaling document %s: %v", base.UD(docID), err)
		}

		if syncData == nil || syncData.Attachments == nil {
			// no attachments to migrate
			return true
		}

		collCtx := collection.AddCollectionContext(ctx)
		collWithUser := &DatabaseCollectionWithUser{
			DatabaseCollection: collection,
		}
		// xattr migration to take place
		err = collWithUser.MigrateAttachmentMetadata(collCtx, docID, event.Cas, syncData)
		if err != nil {
			failProcess(err, "error migrating document attachment metadata for doc: %s: %v", base.UD(docID), err)
		} else {
			a.DocsChanged.Add(1)
		}
		return true
	}

	bucket, err := base.AsGocbV2Bucket(db.Bucket)
	if err != nil {
		return err
	}

	currCollectionIDs := db.GetCollectionIDs()
	checkpointPrefix := db.MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID) + "att_migration:"

	// check for mismatch in collection id's between current collections on the db and prev run
	err = a.resetDCPMetadataIfNeeded(ctx, db.MetadataStore, db, checkpointPrefix, currCollectionIDs)
	if err != nil {
		return err
	}

	a.SetCollectionIDs(currCollectionIDs)
	dcpOptions := getMigrationDCPClientOptions(currCollectionIDs, db.Options.GroupID, checkpointPrefix)
	dcpFeedKey := GenerateAttachmentMigrationDCPStreamName(a.MigrationID)
	dcpClient, err := base.NewDCPClient(ctx, dcpFeedKey, callback, *dcpOptions, bucket)
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to create attachment migration DCP client: %v", migrationLoggingID, err)
		return err
	}
	base.InfofCtx(ctx, base.KeyAll, "[%s] Starting DCP feed %q for attachment migration", migrationLoggingID, dcpFeedKey)

	doneChan, err := dcpClient.Start()
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to start attachment migration DCP feed: %v", migrationLoggingID, err)
		_ = dcpClient.Close()
		return err
	}
	base.DebugfCtx(ctx, base.KeyAll, "[%s] DCP client started for Attachment Migration.", migrationLoggingID)

	select {
	case <-doneChan:
		base.InfofCtx(ctx, base.KeyAll, "[%s] Finished migrating attachment metadata from sync data to global sync data. %d/%d docs changed", migrationLoggingID, a.DocsChanged.Value(), a.DocsProcessed.Value())
		err = dcpClient.Close()
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to close attachment migration DCP client! %v", migrationLoggingID, err)
			return err
		}
		if processFailure != nil {
			return processFailure
		}
		// set sync info here
		for _, collectionID := range currCollectionIDs {
			dbc, ok := db.CollectionByID[collectionID]
			if !ok {
				base.InfofCtx(ctx, base.KeyAll, "[%s] Completed attachment migration, but unable to update syncInfo for collection %v (collection is no longer associated with the database)", migrationLoggingID, collectionID)
				continue
			}
			if err := base.SetSyncInfoMetaVersion(dbc.dataStore, base.MetadataVersionNumber); err != nil {
				base.WarnfCtx(ctx, "[%s] Completed attachment migration, but unable to update syncInfo for collection %v: %v", migrationLoggingID, collectionID, err)
			}
		}
	case <-terminator.Done():
		base.DebugfCtx(ctx, base.KeyAll, "[%s] Terminator closed. Ending Attachment Migration process.", migrationLoggingID)
		err = dcpClient.Close()
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to close attachment migration DCP client! %v", migrationLoggingID, err)
			return err
		}
		if processFailure != nil {
			return processFailure
		}
		err = <-doneChan
		if err != nil {
			return err
		}
		base.InfofCtx(ctx, base.KeyAll, "[%s] Attachment Migration was terminated. Docs changed: %d Docs Processed: %d", migrationLoggingID, a.DocsChanged.Value(), a.DocsProcessed.Value())
	}
	return nil
}

func (a *AttachmentMigrationManager) SetStatus(docChanged, docProcessed int64) {

	a.DocsChanged.Set(docChanged)
	a.DocsProcessed.Set(docProcessed)
}

func (a *AttachmentMigrationManager) SetCollectionIDs(collectionID []uint32) {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.CollectionIDs = collectionID
}

func (a *AttachmentMigrationManager) ResetStatus() {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.DocsProcessed.Set(0)
	a.DocsChanged.Set(0)
	a.CollectionIDs = nil
}

func (a *AttachmentMigrationManager) GetProcessStatus(status BackgroundManagerStatus) ([]byte, []byte, error) {
	a.lock.RLock()
	defer a.lock.RUnlock()

	response := AttachmentMigrationManagerResponse{
		BackgroundManagerStatus: status,
		MigrationID:             a.MigrationID,
		DocsChanged:             a.DocsChanged.Value(),
		DocsProcessed:           a.DocsProcessed.Value(),
	}

	meta := AttachmentMigrationMeta{
		CollectionIDs: a.CollectionIDs,
	}

	statusJSON, err := base.JSONMarshal(response)
	if err != nil {
		return nil, nil, err
	}
	metaJSON, err := base.JSONMarshal(meta)
	if err != nil {
		return nil, nil, err
	}

	return statusJSON, metaJSON, err
}

func getMigrationDCPClientOptions(collectionIDs []uint32, groupID, prefix string) *base.DCPClientOptions {
	checkpointPrefix := prefix + "att_migration:"
	clientOptions := &base.DCPClientOptions{
		OneShot:           true,
		FailOnRollback:    false,
		MetadataStoreType: base.DCPMetadataStoreCS,
		GroupID:           groupID,
		CollectionIDs:     collectionIDs,
		CheckpointPrefix:  checkpointPrefix,
	}
	return clientOptions
}

type AttachmentMigrationManagerResponse struct {
	BackgroundManagerStatus
	MigrationID   string `json:"migration_id"`
	DocsChanged   int64  `json:"docs_changed"`
	DocsProcessed int64  `json:"docs_processed"`
}

type AttachmentMigrationMeta struct {
	CollectionIDs []uint32 `json:"collection_ids"`
}

type AttachmentMigrationManagerStatusDoc struct {
	AttachmentMigrationManagerResponse `json:"status"`
	AttachmentMigrationMeta            `json:"meta"`
}

// GenerateAttachmentMigrationDCPStreamName returns the DCP stream name for a resync.
func GenerateAttachmentMigrationDCPStreamName(migrationID string) string {
	return fmt.Sprintf(
		"sg-%v:att_migration:%v",
		base.ProductAPIVersion,
		migrationID)
}

// resetDCPMetadataIfNeeded will check for mismatch between current collectionIDs and collectionIDs on previous run
func (a *AttachmentMigrationManager) resetDCPMetadataIfNeeded(ctx context.Context, datastore base.DataStore, database *Database, metadataKeyPrefix string, collectionIDs []uint32) error {
	// if we are pon our first run, no collections will be defined on the manager yet
	if len(a.CollectionIDs) == 0 {
		return nil
	}
	if len(a.CollectionIDs) != len(collectionIDs) {
		err := a.PurgeDCPMetadata(ctx, datastore, database, metadataKeyPrefix)
		if err != nil {
			return err
		}
	}
	slices.Sort(collectionIDs)
	slices.Sort(a.CollectionIDs)

	var purgeNeeded bool
	for i, v := range collectionIDs {
		if v != a.CollectionIDs[i] {
			purgeNeeded = true
			break
		}
	}
	if purgeNeeded {
		err := a.PurgeDCPMetadata(ctx, datastore, database, metadataKeyPrefix)
		if err != nil {
			return err
		}
	}
	return nil
}

// PurgeDCPMetadata will purge all DCP metadata from previous run in the bucket, used to reset dcp client to 0
func (a *AttachmentMigrationManager) PurgeDCPMetadata(ctx context.Context, datastore base.DataStore, database *Database, metadataKeyPrefix string) error {

	bucket, err := base.AsGocbV2Bucket(database.Bucket)
	if err != nil {
		return err
	}
	numVbuckets, err := bucket.GetMaxVbno()
	if err != nil {
		return err
	}

	metadata := base.NewDCPMetadataCS(ctx, datastore, numVbuckets, base.DefaultNumWorkers, metadataKeyPrefix)
	base.InfofCtx(ctx, base.KeyDCP, "purging persisted dcp metadata for attachment migration run %s", a.MigrationID)
	metadata.Purge(ctx, base.DefaultNumWorkers)
	return nil
}
