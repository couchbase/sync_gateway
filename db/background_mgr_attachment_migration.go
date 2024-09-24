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
		syncData, rawBody, rawXattrs, err := UnmarshalDocumentSyncDataFromFeed(event.Value, event.DataType, collection.userXattrKey(), false)
		if err != nil {
			failProcess(err, "error unmarshaling document %s: %v", base.UD(docID), err)
		}

		if rawXattrs[base.GlobalXattrName] != nil {
			// global xattr already defined for this document, don't process
			return true
		}

		collCtx := collection.AddCollectionContext(ctx)
		collWithUser := &DatabaseCollectionWithUser{
			DatabaseCollection: collection,
		}
		if event.DataType&base.MemcachedDataTypeXattr == 0 {
			// no xattrs on dcp event, check for sync data
			if syncData == nil {
				// no inline sync xattr, skip event
				return true
			}
			// we have inline sync xattr to migrate
			opts := DefaultMutateInOpts()
			existingDoc := &sgbucket.BucketDocument{
				Cas:    event.Cas,
				Body:   rawBody,
				Expiry: event.Expiry,
			}
			_, _, err = collWithUser.migrateMetadata(ctx, docID, existingDoc, opts)
			if err != nil {
				failProcess(err, "error migrating document attachment metadata for doc: %s: %v", base.UD(docID), err)
			} else {
				a.DocsChanged.Add(1)
			}
		} else {
			// normal xattr migration to take place
			err = collWithUser.MigrateAttachmentMetadata(collCtx, docID, event.Cas, syncData)
			if err != nil {
				failProcess(err, "error migrating document attachment metadata for doc: %s: %v", base.UD(docID), err)
			} else {
				a.DocsChanged.Add(1)
			}
		}
		return true
	}

	bucket, err := base.AsGocbV2Bucket(db.Bucket)
	if err != nil {
		return err
	}

	collectionIDs := getCollectionIDs(db)
	dcpOptions := getMigrationDCPClientOptions(collectionIDs, db.Options.GroupID, db.MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID))
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
	base.DebugfCtx(ctx, base.KeyAll, "[%s] DCP client started.", migrationLoggingID)

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
		for _, collectionID := range collectionIDs {
			dbc, ok := db.CollectionByID[collectionID]
			if !ok {
				base.WarnfCtx(ctx, "[%s] Completed attachment migration, but unable to update syncInfo for collection %v (not found)", migrationLoggingID, collectionID)
				continue
			}
			if err := base.SetSyncInfoMetaVersion(dbc.dataStore, base.ProductMajorVersionInteger); err != nil {
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

func (a *AttachmentMigrationManager) ResetStatus() {
	a.DocsProcessed.Set(0)
	a.DocsChanged.Set(0)
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
	statusJSON, err := base.JSONMarshal(response)
	if err != nil {
		return nil, nil, err
	}
	return statusJSON, nil, err
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

func getCollectionIDs(db *Database) []uint32 {
	collIDs := make([]uint32, 0)
	for id, _ := range db.CollectionByID {
		collIDs = append(collIDs, id)
	}
	return collIDs
}

type AttachmentMigrationManagerResponse struct {
	BackgroundManagerStatus
	MigrationID   string `json:"migration_id"`
	DocsChanged   int64  `json:"docs_changed"`
	DocsProcessed int64  `json:"docs_processed"`
}

type AttachmentMigrationManagerStatusDoc struct {
	AttachmentMigrationManagerResponse `json:"status"`
}

// GenerateAttachmentMigrationDCPStreamName returns the DCP stream name for a resync.
func GenerateAttachmentMigrationDCPStreamName(migrationID string) string {
	return fmt.Sprintf(
		"sg-%v:att_migration:%v",
		base.ProductAPIVersion,
		migrationID)
}
