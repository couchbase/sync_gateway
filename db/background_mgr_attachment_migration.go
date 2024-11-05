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
	databaseCtx   *DatabaseContext
	lock          sync.RWMutex
}

var _ BackgroundManagerProcessI = &AttachmentMigrationManager{}

const MetaVersionValue = "4.0.0" // Meta version to set in syncInfo document upon completion of attachment migration for collection

func NewAttachmentMigrationManager(database *DatabaseContext) *BackgroundManager {
	metadataStore := database.MetadataStore
	metaKeys := database.MetadataKeys
	return &BackgroundManager{
		name: "attachment_migration",
		Process: &AttachmentMigrationManager{
			databaseCtx: database,
		},
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
		base.InfofCtx(ctx, base.KeyAll, "Attachment Migration: Starting new migration run with migration ID: %s", a.MigrationID)
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

		base.InfofCtx(ctx, base.KeyAll, "Attachment Migration: Resuming migration with migration ID: %s, %d already processed", a.MigrationID, a.DocsProcessed.Value())

		return nil
	}

	return newRunInit()
}

func (a *AttachmentMigrationManager) Run(ctx context.Context, options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	db := a.databaseCtx
	migrationLoggingID := "Migration: " + a.MigrationID

	persistClusterStatus := func() {
		err := persistClusterStatusCallback(ctx)
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to persist latest cluster status for attachment migration: %v", migrationLoggingID, err)
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
			failProcess(err, "[%s] error unmarshaling document %s: %v, stopping attachment migration.", migrationLoggingID, base.UD(docID), err)
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
			failProcess(err, "[%s] error migrating document attachment metadata for doc: %s: %v", migrationLoggingID, base.UD(docID), err)
		}
		a.DocsChanged.Add(1)
		return true
	}

	bucket, err := base.AsGocbV2Bucket(db.Bucket)
	if err != nil {
		return err
	}

	currCollectionIDs, err := getCollectionIDsForMigration(db)
	if err != nil {
		return err
	}
	dcpFeedKey := GenerateAttachmentMigrationDCPStreamName(a.MigrationID)
	dcpPrefix := db.MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID)

	// check for mismatch in collection id's between current collections on the db and prev run
	checkpointPrefix := fmt.Sprintf("%s:%v", dcpPrefix, dcpFeedKey)
	err = a.resetDCPMetadataIfNeeded(ctx, db, checkpointPrefix, currCollectionIDs)
	if err != nil {
		return err
	}

	a.SetCollectionIDs(currCollectionIDs)
	dcpOptions := getMigrationDCPClientOptions(currCollectionIDs, db.Options.GroupID, dcpPrefix)
	dcpClient, err := base.NewDCPClient(ctx, dcpFeedKey, callback, *dcpOptions, bucket)
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to create attachment migration DCP client: %v", migrationLoggingID, err)
		return err
	}
	base.DebugfCtx(ctx, base.KeyAll, "[%s] Starting DCP feed %q for attachment migration", migrationLoggingID, dcpFeedKey)

	doneChan, err := dcpClient.Start()
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to start attachment migration DCP feed: %v", migrationLoggingID, err)
		_ = dcpClient.Close()
		return err
	}
	base.TracefCtx(ctx, base.KeyAll, "[%s] DCP client started for Attachment Migration.", migrationLoggingID)

	select {
	case <-doneChan:
		err = dcpClient.Close()
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to close attachment migration DCP client after attachment migration process was finished %v", migrationLoggingID, err)
		}
		if processFailure != nil {
			return processFailure
		}
		updatedDsNames := make(map[base.ScopeAndCollectionName]struct{}, len(db.CollectionByID))
		// set sync info metadata version
		for _, collectionID := range currCollectionIDs {
			dbc := db.CollectionByID[collectionID]
			if err := base.SetSyncInfoMetaVersion(dbc.dataStore, MetaVersionValue); err != nil {
				base.WarnfCtx(ctx, "[%s] Completed attachment migration, but unable to update syncInfo for collection %s: %v", migrationLoggingID, dbc.Name, err)
				return err
			}
			updatedDsNames[base.ScopeAndCollectionName{Scope: dbc.ScopeName, Collection: dbc.Name}] = struct{}{}
		}
		collectionsRequiringMigration := make([]base.ScopeAndCollectionName, 0)
		for _, dsName := range db.RequireAttachmentMigration {
			_, ok := updatedDsNames[dsName]
			if !ok {
				collectionsRequiringMigration = append(collectionsRequiringMigration, dsName)
			}
		}
		db.RequireAttachmentMigration = collectionsRequiringMigration

		base.InfofCtx(ctx, base.KeyAll, "[%s] Finished migrating attachment metadata from sync data to global sync data. %d/%d docs changed", migrationLoggingID, a.DocsChanged.Value(), a.DocsProcessed.Value())
	case <-terminator.Done():
		err = dcpClient.Close()
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to close attachment migration DCP client after attachment migration process was terminated %v", migrationLoggingID, err)
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
	clientOptions := &base.DCPClientOptions{
		OneShot:           true,
		FailOnRollback:    false,
		MetadataStoreType: base.DCPMetadataStoreCS,
		GroupID:           groupID,
		CollectionIDs:     collectionIDs,
		CheckpointPrefix:  prefix,
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
func (a *AttachmentMigrationManager) resetDCPMetadataIfNeeded(ctx context.Context, database *DatabaseContext, metadataKeyPrefix string, collectionIDs []uint32) error {
	// if we are on our first run, no collections will be defined on the manager yet
	if len(a.CollectionIDs) == 0 {
		return nil
	}
	if len(a.CollectionIDs) != len(collectionIDs) {
		base.InfofCtx(ctx, base.KeyDCP, "Purging invalid checkpoints for background task run %s", a.MigrationID)
		err := PurgeDCPCheckpoints(ctx, database, metadataKeyPrefix, a.MigrationID)
		if err != nil {
			return err
		}
		return nil
	}
	slices.Sort(collectionIDs)
	slices.Sort(a.CollectionIDs)
	purgeNeeded := slices.Compare(collectionIDs, a.CollectionIDs)
	if purgeNeeded != 0 {
		base.InfofCtx(ctx, base.KeyDCP, "Purging invalid checkpoints for background task run %s", a.MigrationID)
		err := PurgeDCPCheckpoints(ctx, database, metadataKeyPrefix, a.MigrationID)
		if err != nil {
			return err
		}
	}
	return nil
}

// getCollectionIDsForMigration will get all collection IDs required for DCP client on migration run
func getCollectionIDsForMigration(db *DatabaseContext) ([]uint32, error) {
	collectionIDs := make([]uint32, 0)

	// if all collections are included in RequireAttachmentMigration then we need to run against all collections,
	// if no collections are specified in RequireAttachmentMigration, run against all collections. This is to support job
	// being triggered by rest api (even after job was previously completed)
	if len(db.RequireAttachmentMigration) == 0 {
		// get all collection IDs
		collectionIDs = db.GetCollectionIDs()
	} else {
		// iterate through and grab collectionIDs we need
		for _, v := range db.RequireAttachmentMigration {
			collection, err := db.GetDatabaseCollection(v.ScopeName(), v.CollectionName())
			if err != nil {
				return nil, base.RedactErrorf("failed to find ID for collection %s.%s", base.MD(v.ScopeName()), base.MD(v.CollectionName()))
			}
			collectionIDs = append(collectionIDs, collection.GetCollectionID())
		}
	}
	return collectionIDs, nil
}
