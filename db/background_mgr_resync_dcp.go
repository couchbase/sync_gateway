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
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/couchbase/cbgt"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
)

// =====================================================================
// Resync Implementation of Background Manager Process using DCP stream
// =====================================================================

type ResyncManagerDCP struct {
	docsProcessedLocal           atomic.Int64  // number of documents processed locally on this node since the last start or resume of resync
	docsChangedLocal             atomic.Int64  // number of documents changed locally on this node since the last start or resume of resync
	docsErroredLocal             atomic.Int64  // number of documents that failed to resync locally on this node since the last start or resume of resync
	docsProcessedLocalSerialized atomic.Int64  // number of documents processed locally on this node that have been serialized to the status document
	docsChangedLocalSerialized   atomic.Int64  // number of documents changed locally on this node that have been serialized to the status document
	docsErroredLocalSerialized   atomic.Int64  // number of documents that failed to resync locally on this node that have been serialized to the status document
	docsProcessedCrossNode       atomic.Int64  // number of documents processed across all nodes, as reported by the status document
	docsChangedCrossNode         atomic.Int64  // number of documents changed across all nodes, as reported by the status document
	docsErroredCrossNode         atomic.Int64  // number of documents that failed to resync across all nodes, as reported by the status document
	docsTargeted                 atomic.Uint64 // number of documents targeted for resync, computed once at the start of a new run
	ResyncID                     string
	VBUUIDs                      []uint64
	useXattrs                    bool
	ResyncedCollections          base.CollectionNames
	resyncCollectionInfo
	lock        sync.RWMutex
	Distributed bool
}

// resyncCollectionInfo contains information on collections included on resync run, populated in init() and used in Run()
type resyncCollectionInfo struct {
	collectionIDs     []uint32
	hasAllCollections bool
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
	collections, err := getResyncCollections(db, resyncCollections)
	if err != nil {
		return err
	}
	r.hasAllCollections = len(resyncCollections) == 0
	// add collection list to manager for use in status call
	r.setCollectionStatus(collections)

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
		r.initializeFromPreviousStatus(statusDoc)
		base.InfofCtx(ctx, base.KeyAll, "Resuming resync with ID: %q", r.ResyncID)
		return nil
	}

	if statusDoc.ResyncID != "" {
		err := r.purgeCheckpoints(ctx, db, statusDoc.ResyncID)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to delete checkpoints for previous resync ID %q: %v, these will be abandoned and unused", statusDoc.ResyncID, err)
		}
	}

	newID, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	docsTargeted, err := totalResyncDocs(ctx, collections)
	if err != nil {
		return err
	}
	r.docsTargeted.Store(docsTargeted)

	r.ResyncID = newID.String()
	base.InfofCtx(ctx, base.KeyAll, "Running new resync process with ID: %q - %s", r.ResyncID, resetMsg)
	return nil
}

// totalResyncDocs returns an estimate of the number of documents processed for resync.
func totalResyncDocs(ctx context.Context, collections DatabaseCollections) (uint64, error) {
	var total uint64
	for _, collection := range collections {
		count, err := collection.CountAllDocs(ctx)
		if err != nil {
			return 0, base.RedactErrorf("resync: failed to count docs for collection %s.%s", base.MD(collection.ScopeName), base.MD(collection.Name))
		}
		total += count
	}
	return total, nil
}

// purgeCheckpoints removes checkpoints for a given resync run.
func (r *ResyncManagerDCP) purgeCheckpoints(ctx context.Context, db *Database, resyncID string) error {
	return base.PurgeDCPCheckpoints(
		ctx,
		db.MetadataStore,
		GetResyncDCPCheckpointPrefix(db.DatabaseContext, resyncID, r.Distributed),
		db.distributedDCPFeedMode(),
	)
}

// SetVBUUIDs updates vbuuids in the manager.
func (r *ResyncManagerDCP) SetVBUUIDs(vbuuids []uint64) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.VBUUIDs = vbuuids
}

// Run starts a DCP feed to process documents for resync.
func (r *ResyncManagerDCP) Run(ctx context.Context, options map[string]any, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) (err error) {
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
	ctx = context.WithoutCancel(ctx) // drop cancellation from parent context
	ctx = base.CorrelationIDLogCtx(ctx, r.ResyncID)
	ctx, cancelResync := context.WithCancelCause(ctx)
	defer func() {
		stateErr := db.DBStateManager.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(false)})
		if stateErr != nil {
			base.WarnfCtx(ctx, "failed to update the database state: %v", stateErr)
		}
		if err != nil {
			cancelResync(err)
		} else {
			cancelResync(errors.New("resync ended normally"))
		}
	}()

	var doneChan chan error
	var dcpClient base.DCPClient

	persistClusterStatus := func() {
		err := persistClusterStatusCallback(ctx)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to persist cluster status on-demand for resync operation: %v", err)
		}
	}
	defer persistClusterStatus()

	defer atomic.CompareAndSwapUint32(&db.State, DBResyncing, DBOffline)

	callback := func(event sgbucket.FeedEvent) bool {
		docID := string(event.Key)
		base.TracefCtx(ctx, base.KeyAll, "Resync: Received DCP event %d for doc %v", event.Opcode, base.UD(docID))

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

		r.docsProcessedLocal.Add(1)
		db.DbStats.Database().ResyncNumProcessed.Add(1)
		databaseCollection := db.CollectionByID[event.CollectionID]
		databaseCollection.collectionStats.ResyncNumProcessed.Add(1)
		ctx := databaseCollection.AddCollectionContext(ctx)
		doc, err := bucketDocumentFromFeed(event)
		if err != nil {
			base.WarnfCtx(ctx, "Resync: Error getting document from DCP event for doc %q: %v", base.UD(docID), err)
			return false
		}
		err = (&DatabaseCollectionWithUser{
			DatabaseCollection: databaseCollection,
		}).ResyncDocument(ctx, docID, doc, regenerateSequences)

		if err == nil {
			r.docsChangedLocal.Add(1)
			db.DbStats.Database().ResyncNumChanged.Add(1)
			databaseCollection.collectionStats.ResyncNumChanged.Add(1)
		} else if err != base.ErrUpdateCancel {
			r.docsErroredLocal.Add(1)
			base.WarnfCtx(ctx, "Resync: Error updating doc %q: %v", base.UD(docID), err)
			return false
		}
		return true
	}

	if r.hasAllCollections {
		base.InfofCtx(ctx, base.KeyAll, "running resync against all collections")
	} else {
		base.InfofCtx(ctx, base.KeyAll, "running resync against specified collections: %s", base.MD(resyncCollections))
	}

	base.InfofCtx(ctx, base.KeyAll, "Starting DCP resync")
	if r.Distributed {
		var resyncDestKey string
		var scopeName string

		if !db.useShardedDCP() {
			return fmt.Errorf("running distributed resync is not supported")
		}

		// Dest creation
		for sn := range db.Scopes {
			scopeName = sn
		}

		collectionNamesByScope := r.ResyncedCollections

		sort.Strings(collectionNamesByScope[scopeName])
		resyncDestKey = base.DestKey(db.Name, scopeName, collectionNamesByScope[scopeName], base.ShardedDCPFeedTypeResync)

		checkPointPrefix := GetResyncDCPCheckpointPrefix(db.DatabaseContext, r.ResyncID, true)

		resyncDestFunc := func(janitorRollback func()) (cbgt.Dest, error) {
			resyncDest, err := base.NewDCPDest(ctx, callback, db.MetadataStore, db.numVBuckets, true, nil, nil, checkPointPrefix)
			if err != nil {
				return nil, fmt.Errorf("Error creating resync dest: %v", err)
			}
			return resyncDest, nil
		}

		base.StoreDestFactory(ctx, resyncDestKey, resyncDestFunc)
		defer base.RemoveDestFactory(resyncDestKey)

		// Heartbeater creation
		resyncHBPrefix := db.MetadataKeys.ResyncHeartbeaterPrefix()
		resyncHB, err := base.NewCouchbaseHeartbeater(db.MetadataStore, resyncHBPrefix, db.UUID)
		if err != nil {
			return fmt.Errorf("Error creating resync heartbeater: %v", err)
		}
		err = resyncHB.Start(ctx)
		if err != nil {
			return fmt.Errorf("Error starting resync heartbeater: %v", err)
		}
		defer resyncHB.Stop(ctx)

		resyncCfg, err := base.NewCfgSG(ctx, db.MetadataStore, db.MetadataKeys.ResyncCfgPrefix(), true)
		if err != nil {
			return fmt.Errorf("Error creating resync cfg: %v", err)
		}

		indexName, err := base.GenerateCBGTIndexName(db.Name, base.ShardedDCPFeedTypeResync)
		if err != nil {
			return fmt.Errorf("Error generating CBGT index name: %v", err)
		}
		var partitionCount uint16
		if db.Options.UnsupportedOptions != nil && db.Options.UnsupportedOptions.ResyncPartitions != nil && *db.Options.UnsupportedOptions.ResyncPartitions > 0 {
			partitionCount = *db.Options.UnsupportedOptions.ResyncPartitions
		} else {
			partitionCount = db.Options.ImportOptions.ImportPartitions
		}
		base.DebugfCtx(ctx, base.KeyAll, "Using %d partitions for resync", partitionCount)

		opts := base.ShardedDCPOptions{
			DBName:        db.Name,
			UUID:          db.UUID,
			NumPartitions: partitionCount,
			Collections:   collectionNamesByScope,
			Cfg:           resyncCfg,
			Heartbeater:   resyncHB,
			Bucket:        db.Bucket,
			IndexType:     base.CBGTIndexTypeSyncGatewayResync,
			DestKey:       resyncDestKey,
			IndexName:     indexName,
			Datastore:     db.MetadataStore,
			FeedType:      base.ShardedDCPFeedTypeResync,
		}
		resyncCbgtContext, err := base.StartShardedDCPFeed(ctx, opts)
		if err != nil {
			return fmt.Errorf("Error starting resync sharded dcp feed: %v", err)
		}
		defer resyncCbgtContext.Stop(ctx)
	} else {

		clientOptions := r.getDCPClientOptions(db.DatabaseContext, r.ResyncID, r.ResyncedCollections.ToCollectionNameSet(), callback, false)
		var err error
		dcpClient, err = base.NewDCPClient(ctx, db.DatabaseContext.Bucket, clientOptions)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to create resync DCP client! %v", err)
			return err
		}

		doneChan, err = dcpClient.Start()
		if err != nil {
			base.WarnfCtx(ctx, "Failed to start resync DCP feed! %v", err)
			_ = dcpClient.Close()
			return err
		}
		base.DebugfCtx(ctx, base.KeyAll, "Resync DCP client started.")

		r.SetVBUUIDs(base.GetVBUUIDs(dcpClient.GetMetadata()))
	}

	select {
	case <-doneChan:
		base.InfofCtx(ctx, base.KeyAll, "Finished running resync. %d/%d docs changed", r.DocsChanged(), r.DocsProcessed())
		err := dcpClient.Close()
		if err != nil {
			base.WarnfCtx(ctx, "Failed to close resync DCP client! %v", err)
			return err
		}

		// If the principal docs sequences are regenerated, or the user doc need to be invalidated after a dynamic channel grant, db.QueryPrincipals is called to find the principal docs.
		// In the case that a database is created with "start_offline": true, it is possible the index needed to create this is not yet ready, so make sure it is ready for use.
		if !db.UseViews() && ((regenerateSequences && resyncCollections == nil) || r.DocsChanged() > 0) {
			err := initializePrincipalDocsIndex(ctx, db)
			if err != nil {
				return err
			}
		}
		if regenerateSequences && resyncCollections == nil {
			err := db.updateAllPrincipalsSequences(ctx)

			if err != nil {
				return fmt.Errorf("Error updating principal sequences: %w", err)
			}
		}

		if r.DocsChanged() > 0 {
			endSeq, err := db.sequences.getSequence(ctx)
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
					base.WarnfCtx(ctx, "Completed resync, but unable to update syncInfo for collection %v (not found)", collectionID)
				}
				if err := base.SetSyncInfoMetadataID(ctx, dbc.dataStore, db.DatabaseContext.Options.MetadataID); err != nil {
					base.WarnfCtx(ctx, "Completed resync, but unable to update syncInfo for collection %v: %v", collectionID, err)
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
		if !r.Distributed {
			base.DebugfCtx(ctx, base.KeyAll, "Terminator closed. Ending Resync process.")
			err := dcpClient.Close()
			if err != nil {
				base.WarnfCtx(ctx, "Failed to close resync DCP client! %v", err)
				return err
			}

			err = <-doneChan
			if err != nil {
				return err
			}

			base.InfofCtx(ctx, base.KeyAll, "resync was terminated. Docs changed: %d Docs Processed: %d", r.DocsChanged(), r.DocsProcessed())
		}
	}

	return nil
}

// getResyncCollections returns collections requested for resync. If no collections are specified, it returns all collections.
func getResyncCollections(db *Database, resyncCollections base.CollectionNames) (collections DatabaseCollections, err error) {
	if len(resyncCollections) == 0 {
		return slices.Collect(maps.Values(db.CollectionByID)), nil
	}

	for scopeName, collectionsName := range resyncCollections {
		for _, collectionName := range collectionsName {
			collection, err := db.GetDatabaseCollection(scopeName, collectionName)
			if err != nil {
				return nil, base.RedactErrorf("failed to find ID for collection %s.%s", base.MD(scopeName).Redact(), base.MD(collectionName).Redact())
			}
			collections = append(collections, collection)
		}
	}
	return collections, nil
}

func (r *ResyncManagerDCP) ResetStatus() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.docsProcessedLocalSerialized.Store(0)
	r.docsProcessedLocal.Store(0)
	r.docsProcessedCrossNode.Store(0)
	r.docsChangedLocalSerialized.Store(0)
	r.docsChangedLocal.Store(0)
	r.docsChangedCrossNode.Store(0)
	r.docsErroredLocalSerialized.Store(0)
	r.docsErroredLocal.Store(0)
	r.docsErroredCrossNode.Store(0)
	r.docsTargeted.Store(0)
	r.ResyncedCollections = nil
}

// initializeFromPreviousStatus restores the in-memory state of the manager from a previously persisted status
// document so that a resumed run starts with the correct accumulated counts.
func (r *ResyncManagerDCP) initializeFromPreviousStatus(statusDoc ResyncManagerStatusDocDCP) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.ResyncID = statusDoc.ResyncID
	r.docsChangedLocal.Store(statusDoc.DocsChanged)
	r.docsProcessedLocal.Store(statusDoc.DocsProcessed)
	r.docsErroredLocal.Store(statusDoc.DocsErrored)
	r.docsTargeted.Store(statusDoc.DocsTargeted)
}

// setCollectionStatus sets the active collections being resynced.
func (r *ResyncManagerDCP) setCollectionStatus(collections DatabaseCollections) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.ResyncedCollections = collections.getNames()
	r.collectionIDs = collections.getIDs()
}

// ResyncManagerResponseDCP is the struct used to serialize the status of the resync process. This matches the output
// expected by GET /{db}/_resync
type ResyncManagerResponseDCP struct {
	BackgroundManagerStatus
	ResyncID              string              `json:"resync_id"`
	CollectionsProcessing map[string][]string `json:"collections_processing,omitempty"`
	resyncStats
}

// resyncStats is a part of the ResyncManagerResponseDCP struct that only contains the stats fields for efficincy.
type resyncStats struct {
	DocsChanged   int64  `json:"docs_changed"`
	DocsProcessed int64  `json:"docs_processed"`
	DocsErrored   int64  `json:"docs_errored"`
	DocsTargeted  uint64 `json:"docs_targeted"`
}

// SetProcessStatus reports the new status that was serialized to the bucket along with the last status that polled
// using GetProcessStatus.
func (r *ResyncManagerDCP) SetProcessStatus(ctx context.Context, previousStatus []byte, newStatus []byte) {
	r.lock.Lock()
	defer r.lock.Unlock()
	var previousStats resyncStats
	if len(previousStatus) > 0 {
		err := base.JSONUnmarshal(previousStatus, &previousStats)
		if err != nil {
			base.AssertfCtx(ctx, "Could not process previous status: %q, resync will lose track of its stats: %w", string(previousStatus), err)
			return
		}
	}
	var newStats resyncStats
	if len(newStatus) > 0 {
		err := base.JSONUnmarshal(newStatus, &newStats)
		if err != nil {
			base.AssertfCtx(ctx, "Could not process current status: %q, resync will lose track of its stats: %s", string(newStatus), err)
			return
		}
	}

	r.docsProcessedCrossNode.Store(newStats.DocsProcessed)
	r.docsChangedCrossNode.Store(newStats.DocsChanged)
	r.docsErroredCrossNode.Store(newStats.DocsErrored)
	r.docsProcessedLocalSerialized.Add(newStats.DocsProcessed - previousStats.DocsProcessed)
	r.docsChangedLocalSerialized.Add(newStats.DocsChanged - previousStats.DocsChanged)
	r.docsErroredLocalSerialized.Add(newStats.DocsErrored - previousStats.DocsErrored)
}

func (r *ResyncManagerDCP) GetProcessStatus(status BackgroundManagerStatus, previousStatus []byte) ([]byte, []byte, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	var previousStats resyncStats
	if len(previousStatus) > 0 {
		err := base.JSONUnmarshal(previousStatus, &previousStats)
		if err != nil {
			return nil, nil, fmt.Errorf("Could not process previous status: %q", string(previousStatus))
		}
	}

	response := ResyncManagerResponseDCP{
		BackgroundManagerStatus: status,
		ResyncID:                r.ResyncID,
		resyncStats: resyncStats{
			DocsChanged:   previousStats.DocsChanged + (r.docsChangedLocal.Load() - r.docsChangedLocalSerialized.Load()),
			DocsProcessed: previousStats.DocsProcessed + (r.docsProcessedLocal.Load() - r.docsProcessedLocalSerialized.Load()),
			DocsErrored:   previousStats.DocsErrored + (r.docsErroredLocal.Load() - r.docsErroredLocalSerialized.Load()),
			DocsTargeted:  r.docsTargeted.Load(),
		},
		CollectionsProcessing: r.ResyncedCollections,
	}

	// Fallback to internal serialized state if no previous status was provided.
	if len(previousStatus) == 0 {
		response.DocsChanged = r.DocsChanged()
		response.DocsProcessed = r.DocsProcessed()
		response.DocsErrored = r.DocsErrored()
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

// DocsChanged returns the total number of documents changed for the entire resync process. This includes docs
// changed by other nodes.
func (r *ResyncManagerDCP) DocsChanged() int64 {
	return r.docsChangedCrossNode.Load() + r.docsChangedLocal.Load() - r.docsChangedLocalSerialized.Load()
}

// DocsProcessed returns the total number of documents changed on the entire resync process. This includes docs
// processed by other nodes.
func (r *ResyncManagerDCP) DocsProcessed() int64 {
	return r.docsProcessedCrossNode.Load() + r.docsProcessedLocal.Load() - r.docsProcessedLocalSerialized.Load()
}

// DocsErrored returns the total number of documents that failed to resync across the entire resync process.
// This includes docs errored by other nodes.
func (r *ResyncManagerDCP) DocsErrored() int64 {
	return r.docsErroredCrossNode.Load() + r.docsErroredLocal.Load() - r.docsErroredLocalSerialized.Load()
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

// getResyncDCPClientOptions returns the default set of DCPClientOptions suitable for resync. collectionIDs
// represent Couchbase Server collection IDs. prefix represents the prefixed name of the checkpoint documents
// used to store DCP checkpoints.
func (r *ResyncManagerDCP) getDCPClientOptions(db *DatabaseContext, resyncID string, collectionNames base.CollectionNameSet, callback sgbucket.FeedEventCallbackFunc, distributed bool) base.DCPClientOptions {
	return base.DCPClientOptions{
		FeedID:            fmt.Sprintf("resync:%v", resyncID),
		OneShot:           true,
		FailOnRollback:    false,
		MetadataStoreType: base.DCPMetadataStoreCS,
		CollectionNames:   collectionNames,
		CheckpointPrefix:  GetResyncDCPCheckpointPrefix(db, resyncID, distributed),
		Callback:          callback,
		MetadataStore:     db.MetadataStore,
	}
}

// GetResyncDCPCheckpointPrefix returns the prefix of the DCP checkpoint documents for resync.
func GetResyncDCPCheckpointPrefix(db *DatabaseContext, resyncID string, distributed bool) string {
	var checkpointPrefix string
	if distributed {
		checkpointPrefix = fmt.Sprintf(
			"%s:sg-%v:resync-distributed:%v",
			db.MetadataKeys.DCPCheckpointPrefix(""),
			base.ProductAPIVersion,
			resyncID,
		)
	} else {
		checkpointPrefix = fmt.Sprintf(
			"%s:sg-%v:resync:%v",
			db.MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID),
			base.ProductAPIVersion,
			resyncID,
		)
	}
	return checkpointPrefix
}
