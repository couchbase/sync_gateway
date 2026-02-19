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
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
)

// =====================================================================
// Resync Implementation of Background Manager Process using DCP stream
// =====================================================================

type ResyncManagerDCP struct {
	DocsProcessed       base.AtomicInt
	DocsChanged         base.AtomicInt
	ResyncID            string
	VBUUIDs             []uint64
	useXattrs           bool
	ResyncedCollections base.CollectionNames
	resyncCollectionInfo
	lock sync.RWMutex
}

// resyncCollectionInfo contains information on collections included on resync run, populated in init() and used in Run()
type resyncCollectionInfo struct {
	hasAllCollections bool
	collectionIDs     []uint32
}

var _ BackgroundManagerProcessI = &ResyncManagerDCP{}

func NewResyncManagerDCP(metadataStore base.DataStore, useXattrs bool, metaKeys *base.MetadataKeys) *BackgroundManager {
	return &BackgroundManager{
		name:    "resync",
		Process: &ResyncManagerDCP{useXattrs: useXattrs},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "resync",
		},
		terminator: base.NewSafeTerminator(),
	}
}

// Init processes the options to start a resync process and sets them as struct memebers.
func (r *ResyncManagerDCP) Init(ctx context.Context, options map[string]any, clusterStatus []byte) error {
	db, ok := options["database"].(*Database)
	if !ok {
		return errors.New("database option is required and must be of type *Database")
	}
	resyncCollections, ok := options["collections"].(base.CollectionNames)
	if !ok {
		return errors.New("collections option is required and must be of type base.CollectionNames")
	}

	// Get collectionIds and store in manager for use in DCP client later
	collectionIDs, hasAllCollections, collectionNames, err := getCollectionIdsAndNames(db, resyncCollections)
	if err != nil {
		return err
	}
	r.collectionIDs = collectionIDs
	r.hasAllCollections = hasAllCollections
	// add collection list to manager for use in status call
	r.SetCollectionStatus(collectionNames)

	// If the previous run completed, or we couldn't determine, we will start the resync with a new resync ID.
	// Otherwise, we should resume with the resync ID, and the previous stats specified in the doc.
	var resetMsg string // an optional message about why we're resetting
	var statusDoc ResyncManagerStatusDocDCP
	if clusterStatus == nil {
		resetMsg = "no previous run found"
	} else if resetOpt, _ := options["reset"].(bool); resetOpt {
		resetMsg = "reset option requested"
	} else if err := base.JSONUnmarshal(clusterStatus, &statusDoc); err != nil {
		resetMsg = "failed to unmarshal cluster status"
	} else if statusDoc.State == BackgroundProcessStateCompleted {
		resetMsg = "previous run completed"
	} else if !base.SlicesEqualIgnoreOrder(r.collectionIDs, statusDoc.CollectionIDs) {
		resetMsg = "collection IDs have changed"
	} else {
		// use the resync ID from the status doc to resume
		r.ResyncID = statusDoc.ResyncID
		r.SetStatus(statusDoc.DocsChanged, statusDoc.DocsProcessed)
		base.InfofCtx(ctx, base.KeyAll, "Resync: Resuming resync with ID: %q", r.ResyncID)
		return nil
	}

	newID, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	r.ResyncID = newID.String()
	base.InfofCtx(ctx, base.KeyAll, "Resync: Running new resync process with ID: %q - %s", r.ResyncID, resetMsg)
	return nil
}

// Run starts a DCP feed to process documents for resync.
func (r *ResyncManagerDCP) Run(ctx context.Context, options map[string]any, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	db, ok := options["database"].(*Database)
	if !ok {
		return errors.New("database option is required and must be of type *Database")
	}
	regenerateSequences, ok := options["regenerateSequences"].(bool)
	if !ok {
		return errors.New("regenerateSequences option is required and must be of type bool")
	}
	resyncCollections, ok := options["collections"].(base.CollectionNames)
	if !ok {
		return errors.New("collections option is required and must be of type CollectionNames")
	}

	resyncLoggingID := "Resync: " + r.ResyncID

	persistClusterStatus := func() {
		err := persistClusterStatusCallback(ctx)
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to persist cluster status on-demand for resync operation: %v", resyncLoggingID, err)
		}
	}
	defer persistClusterStatus()

	defer atomic.CompareAndSwapUint32(&db.State, DBResyncing, DBOffline)

	callback := func(event sgbucket.FeedEvent) bool {
		docID := string(event.Key)
		base.TracefCtx(ctx, base.KeyAll, "[%s] Received DCP event %d for doc %v", resyncLoggingID, event.Opcode, base.UD(docID))

		// Ignore documents without xattrs if possible, to avoid processing unnecessary documents
		if r.useXattrs && event.DataType&base.MemcachedDataTypeXattr == 0 {
			return true
		}
		// Don't want to process raw binary docs
		// The binary check should suffice but for additional safety also check for empty bodies. This will also avoid
		// processing tombstones.
		if event.DataType == base.MemcachedDataTypeRaw || len(event.Value) == 0 {
			return true
		}

		// We only want to process full docs. Not any sync docs.
		if strings.HasPrefix(docID, base.SyncDocPrefix) {
			return true
		}

		r.DocsProcessed.Add(1)
		db.DbStats.Database().ResyncNumProcessed.Add(1)
		databaseCollection := db.CollectionByID[event.CollectionID]
		databaseCollection.collectionStats.ResyncNumProcessed.Add(1)
		collectionCtx := databaseCollection.AddCollectionContext(ctx)
		doc, err := bucketDocumentFromFeed(event)
		if err != nil {
			base.WarnfCtx(collectionCtx, "[%s] Error getting document from DCP event for doc %q: %v", resyncLoggingID, base.UD(docID), err)
			return false
		}
		err = (&DatabaseCollectionWithUser{
			DatabaseCollection: databaseCollection,
		}).ResyncDocument(collectionCtx, docID, doc, regenerateSequences)

		if err == nil {
			r.DocsChanged.Add(1)
			db.DbStats.Database().ResyncNumChanged.Add(1)
			databaseCollection.collectionStats.ResyncNumChanged.Add(1)
		} else if err != base.ErrUpdateCancel {
			base.WarnfCtx(collectionCtx, "[%s] Error updating doc %q: %v", resyncLoggingID, base.UD(docID), err)
			return false
		}
		return true
	}

	bucket, err := base.AsGocbV2Bucket(db.Bucket)
	if err != nil {
		return err
	}

	if r.hasAllCollections {
		base.InfofCtx(ctx, base.KeyAll, "[%s] running resync against all collections", resyncLoggingID)
	} else {
		base.InfofCtx(ctx, base.KeyAll, "[%s] running resync against specified collections", resyncLoggingID)
	}

	clientOptions := getResyncDCPClientOptions(r.collectionIDs, db.Options.GroupID, db.MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID))

	dcpFeedKey := GenerateResyncDCPStreamName(r.ResyncID)
	dcpClient, err := base.NewDCPClient(ctx, dcpFeedKey, callback, *clientOptions, bucket)
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to create resync DCP client! %v", resyncLoggingID, err)
		return err
	}

	base.InfofCtx(ctx, base.KeyAll, "[%s] Starting DCP feed %q for resync", resyncLoggingID, dcpFeedKey)
	doneChan, err := dcpClient.Start()
	if err != nil {
		base.WarnfCtx(ctx, "[%s] Failed to start resync DCP feed! %v", resyncLoggingID, err)
		_ = dcpClient.Close()
		return err
	}
	base.DebugfCtx(ctx, base.KeyAll, "[%s] DCP client started.", resyncLoggingID)

	r.VBUUIDs = base.GetVBUUIDs(dcpClient.GetMetadata())

	select {
	case <-doneChan:
		base.InfofCtx(ctx, base.KeyAll, "[%s] Finished running sync function. %d/%d docs changed", resyncLoggingID, r.DocsChanged.Value(), r.DocsProcessed.Value())
		err = dcpClient.Close()
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to close resync DCP client! %v", resyncLoggingID, err)
			return err
		}

		// If the principal docs sequences are regenerated, or the user doc need to be invalidated after a dynamic channel grant, db.QueryPrincipals is called to find the principal docs.
		// In the case that a database is created with "start_offline": true, it is possible the index needed to create this is not yet ready, so make sure it is ready for use.
		if !db.UseViews() && ((regenerateSequences && resyncCollections == nil) || r.DocsChanged.Value() > 0) {
			err := initializePrincipalDocsIndex(ctx, db)
			if err != nil {
				return err
			}
		}
		if regenerateSequences && resyncCollections == nil {
			var multiError *base.MultiError
			for _, databaseCollection := range db.CollectionByID {
				if updateErr := databaseCollection.updateAllPrincipalsSequences(ctx); updateErr != nil {
					multiError = multiError.Append(updateErr)
				}
			}

			if multiError.Len() > 0 {
				return fmt.Errorf("Error updating principal sequences: %s", multiError.Error())
			}
		}

		if r.DocsChanged.Value() > 0 {
			endSeq, err := db.sequences.getSequence()
			if err != nil {
				return err
			}

			collectionNames := make(base.ScopeAndCollectionNames, 0)
			for _, databaseCollection := range db.CollectionByID {
				collectionNames = append(collectionNames, databaseCollection.ScopeAndCollectionName())
			}
			err = db.invalidateAllPrincipals(ctx, collectionNames, endSeq)
			if err != nil {
				return fmt.Errorf("Could not invalidate principal documents: %w", err)
			}

		}

		// If we regenerated sequences, update syncInfo for all collections affected
		if regenerateSequences {
			updatedDsNames := make(map[base.ScopeAndCollectionName]struct{}, len(r.collectionIDs))
			for _, collectionID := range r.collectionIDs {
				dbc, ok := db.CollectionByID[collectionID]
				if !ok {
					base.WarnfCtx(ctx, "[%s] Completed resync, but unable to update syncInfo for collection %v (not found)", resyncLoggingID, collectionID)
				}
				if err := base.SetSyncInfoMetadataID(dbc.dataStore, db.DatabaseContext.Options.MetadataID); err != nil {
					base.WarnfCtx(ctx, "[%s] Completed resync, but unable to update syncInfo for collection %v: %v", resyncLoggingID, collectionID, err)
				}
				updatedDsNames[base.ScopeAndCollectionName{Scope: dbc.ScopeName, Collection: dbc.Name}] = struct{}{}
			}
			collectionsRequiringResync := make([]base.ScopeAndCollectionName, 0)
			for _, dsName := range db.RequireResync {
				_, ok := updatedDsNames[dsName]
				if !ok {
					collectionsRequiringResync = append(collectionsRequiringResync, dsName)
				}
			}
			db.RequireResync = collectionsRequiringResync
		}
	case <-terminator.Done():
		base.DebugfCtx(ctx, base.KeyAll, "[%s] Terminator closed. Ending Resync process.", resyncLoggingID)
		err = dcpClient.Close()
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to close resync DCP client! %v", resyncLoggingID, err)
			return err
		}

		err = <-doneChan
		if err != nil {
			return err
		}

		base.InfofCtx(ctx, base.KeyAll, "[%s] resync was terminated. Docs changed: %d Docs Processed: %d", resyncLoggingID, r.DocsChanged.Value(), r.DocsProcessed.Value())
	}

	return nil
}

// getCollectionIdsAndNames returns collection names. If no collections are specified, it returns all collections. The
// ids for all collections are returned.
func getCollectionIdsAndNames(db *Database, resyncCollections base.CollectionNames) (collectionIDs []uint32, hasAllCollections bool, collectionNames base.CollectionNames, err error) {
	if len(resyncCollections) == 0 {
		hasAllCollections = true
		for collectionID := range db.CollectionByID {
			collectionIDs = append(collectionIDs, collectionID)
		}
		return collectionIDs, hasAllCollections, db.collectionNames(), nil
	}
	hasAllCollections = false

	for scopeName, collectionsName := range resyncCollections {
		for _, collectionName := range collectionsName {
			collection, err := db.GetDatabaseCollection(scopeName, collectionName)
			if err != nil {
				return nil, hasAllCollections, nil, fmt.Errorf("failed to find ID for collection %s.%s", base.MD(scopeName).Redact(), base.MD(collectionName).Redact())
			}
			collectionIDs = append(collectionIDs, collection.GetCollectionID())
		}
	}
	return collectionIDs, hasAllCollections, resyncCollections, nil
}

func (r *ResyncManagerDCP) ResetStatus() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.DocsProcessed.Set(0)
	r.DocsChanged.Set(0)
	r.ResyncedCollections = nil
}

func (r *ResyncManagerDCP) SetStatus(docChanged, docProcessed int64) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.DocsChanged.Set(docChanged)
	r.DocsProcessed.Set(docProcessed)
}

// SetCollectionStatus sets the active collection names being resynced.
func (r *ResyncManagerDCP) SetCollectionStatus(collectionNames base.CollectionNames) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.ResyncedCollections = collectionNames
}

type ResyncManagerResponseDCP struct {
	BackgroundManagerStatus
	ResyncID              string              `json:"resync_id"`
	DocsChanged           int64               `json:"docs_changed"`
	DocsProcessed         int64               `json:"docs_processed"`
	CollectionsProcessing map[string][]string `json:"collections_processing,omitempty"`
}

func (r *ResyncManagerDCP) GetProcessStatus(status BackgroundManagerStatus) ([]byte, []byte, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	response := ResyncManagerResponseDCP{
		BackgroundManagerStatus: status,
		ResyncID:                r.ResyncID,
		DocsChanged:             r.DocsChanged.Value(),
		DocsProcessed:           r.DocsProcessed.Value(),
		CollectionsProcessing:   r.ResyncedCollections,
	}

	meta := ResyncManagerMeta{
		VBUUIDs:       r.VBUUIDs,
		CollectionIDs: r.collectionIDs,
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

type ResyncManagerMeta struct {
	VBUUIDs       []uint64 `json:"vbuuids"`
	CollectionIDs []uint32 `json:"collection_ids,omitempty"`
}

type ResyncManagerStatusDocDCP struct {
	ResyncManagerResponseDCP `json:"status"`
	ResyncManagerMeta        `json:"meta"`
}

// initializePrincipalDocsIndex creates the metadata indexes required for resync
func initializePrincipalDocsIndex(ctx context.Context, db *Database) error {
	n1qlStore, ok := base.AsN1QLStore(db.MetadataStore)
	if !ok {
		return errors.New("Cannot create indexes on non-Couchbase data store.")
	}
	options := InitializeIndexOptions{
		WaitForIndexesOnlineOption: base.WaitForIndexesDefault,
		NumReplicas:                db.Options.NumIndexReplicas,
		MetadataIndexes:            IndexesPrincipalOnly,
		UseXattrs:                  db.UseXattrs(),
		NumPartitions:              db.numIndexPartitions(),
	}

	return InitializeIndexes(ctx, n1qlStore, options)
}

// getResyncDCPClientOptions returns the default set of DCPClientOptions suitable for resync
func getResyncDCPClientOptions(collectionIDs []uint32, groupID string, prefix string) *base.DCPClientOptions {
	return &base.DCPClientOptions{
		OneShot:           true,
		FailOnRollback:    false,
		MetadataStoreType: base.DCPMetadataStoreCS,
		GroupID:           groupID,
		CollectionIDs:     collectionIDs,
		CheckpointPrefix:  prefix,
	}
}

// GenerateResyncDCPStreamName returns the DCP stream name for a resync.
func GenerateResyncDCPStreamName(resyncID string) string {
	return fmt.Sprintf(
		"sg-%v:resync:%v",
		base.ProductAPIVersion,
		resyncID)
}
