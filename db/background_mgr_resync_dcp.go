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
	db                           *DatabaseContext
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
	ResyncedCollections          base.CollectionNames
	startOptions                 map[string]any // options from the most recent Start call, persisted to meta for Resume
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

// NewResyncManagerDCP returns a new instance of ResyncManagerDCP wrapped in a BackgroundManager. If distributed is
// true, the manager will be set up to run in a distributed manner across multiple nodes, otherwise it will run on a
// single node.
func NewResyncManagerDCP(db *DatabaseContext, distributed bool) *BackgroundManager {
	b := &BackgroundManager{
		name:    "resync",
		Process: &ResyncManagerDCP{db: db, Distributed: distributed},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: db.MetadataStore,
			metaKeys:      db.MetadataKeys,
			processSuffix: "resync",
			multiNode:     distributed,
		},
		terminator: base.NewSafeTerminator(),
	}
	if distributed {
		b.updateDatabaseState = func(ctx context.Context, running bool) error {
			if db.DBStateManager == nil {
				return nil
			}
			return db.DBStateManager.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(running)})
		}
	}
	return b
}

// Init processes the options to start a resync process and sets them as struct memebers.
func (r *ResyncManagerDCP) Init(ctx context.Context, options map[string]any, clusterStatus []byte) error {
	resyncCollections, ok := options["collections"].(base.CollectionNames)
	if !ok {
		return errors.New("collections option is required and must be of type base.CollectionNames")
	}
	r.setStartOptions(options)

	var collections DatabaseCollections
	if len(resyncCollections) > 0 {
		var err error
		collections, err = r.db.collections(resyncCollections)
		if err != nil {
			return err
		}
	} else {
		collections = slices.Collect(maps.Values(r.db.CollectionByID))
		r.hasAllCollections = true
	}
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
		err := r.purgeCheckpoints(ctx, statusDoc.ResyncID)
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
		base.WarnfCtx(ctx, "Failed to count total documents for resync: %v, continuing resync", err)
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
			return 0, base.RedactErrorf("failed to count docs for collection %s.%s: %w", base.MD(collection.ScopeName), base.MD(collection.Name), err)
		}
		total += count
	}
	return total, nil
}

// purgeCheckpoints removes checkpoints for a given resync run.
func (r *ResyncManagerDCP) purgeCheckpoints(ctx context.Context, resyncID string) error {
	return base.PurgeDCPCheckpoints(
		ctx,
		r.db.MetadataStore,
		GetResyncDCPCheckpointPrefix(r.db, resyncID, r.Distributed),
		r.db.distributedDCPFeedMode(),
	)
}

// setStartOptions stores the options used to start the current run so that Resume can reconstruct them.
func (r *ResyncManagerDCP) setStartOptions(options map[string]any) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.startOptions = options
}

// SetVBUUIDs updates vbuuids in the manager.
func (r *ResyncManagerDCP) SetVBUUIDs(vbuuids []uint64) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.VBUUIDs = vbuuids
}

// Run starts a DCP feed to process documents for resync.
func (r *ResyncManagerDCP) Run(ctx context.Context, options map[string]any, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) (err error) {
	db := r.db
	regenerateSequences, ok := options["regenerateSequences"].(bool)
	if !ok {
		return errors.New("regenerateSequences option is required and must be of type bool")
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
	var dcpClientClose dcpClientCloser
	defer func() {
		// check isClosed to avoid double reporting a closed error if already closed
		if dcpClientClose.isClosed() {
			return
		}
		err := dcpClientClose.shutdown() // shutdown in case of panic
		if err != nil {
			base.WarnfCtx(ctx, "Failed to close resync DCP client on shutdown! %v", err)
		}
	}()

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

		// Ignore documents without xattrs to avoid processing unnecessary documents
		if event.DataType&base.MemcachedDataTypeXattr == 0 {
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
		base.InfofCtx(ctx, base.KeyAll, "running resync against specified collections: %s", base.MD(r.ResyncedCollections))
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

		checkPointPrefix := GetResyncDCPCheckpointPrefix(db, r.ResyncID, true)

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
		dcpClientClose.append(func() error {
			resyncHB.Stop(ctx)
			return nil
		})

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
		dcpClientClose.append(func() error {
			resyncCbgtContext.Stop(ctx)
			return nil
		})
	} else {

		clientOptions := r.getDCPClientOptions(db, r.ResyncID, r.ResyncedCollections.ToCollectionNameSet(), callback, false)
		var err error
		dcpClient, err = base.NewDCPClient(ctx, db.Bucket, clientOptions)
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
		dcpClientClose.append(func() error {
			_ = dcpClient.Close()
			err := <-doneChan
			return err
		})

		base.DebugfCtx(ctx, base.KeyAll, "Resync DCP client started.")

		r.SetVBUUIDs(base.GetVBUUIDs(dcpClient.GetMetadata()))
	}

	select {
	case <-doneChan:
		base.InfofCtx(ctx, base.KeyAll, "Finished running resync. %d/%d docs changed", r.DocsChanged(), r.DocsProcessed())
		err := dcpClientClose.shutdown()
		if err != nil {
			base.WarnfCtx(ctx, "Failed to close resync DCP client! %v", err)
			return err
		}

		if err := invalidatePrincipals(ctx, db, regenerateSequences, r.hasAllCollections, r.DocsChanged()); err != nil {
			return err
		}

		// If we regenerated sequences, update syncInfo for all collections affected
		if regenerateSequences {
			updateSyncInfo(ctx, db, r.collectionIDs)
		}
	case <-terminator.Done():

		base.DebugfCtx(ctx, base.KeyAll, "Terminator closed. Ending Resync process.")
		err := dcpClientClose.shutdown()
		if err != nil {
			base.WarnfCtx(ctx, "Failed to close resync DCP client after completion! %v", err)
		}
		base.InfofCtx(ctx, base.KeyAll, "resync was terminated. Docs changed: %d Docs Processed: %d", r.DocsChanged(), r.DocsProcessed())
		return err
	}

	return nil
}

// invalidatePrincipals invalidates principal documents after documents have been resynced.
func invalidatePrincipals(ctx context.Context, db *DatabaseContext, regenerateSequences bool, resyncAllCollections bool, docsChanged int64) error {
	// If the principal docs sequences are regenerated, or the user doc need to be invalidated after a dynamic channel grant, db.QueryPrincipals is called to find the principal docs.
	// In the case that a database is created with "start_offline": true, it is possible the index needed to create this is not yet ready, so make sure it is ready for use.
	if !db.UseViews() && ((regenerateSequences && resyncAllCollections) || docsChanged > 0) {
		err := initializePrincipalDocsIndex(ctx, db)
		if err != nil {
			return err
		}
	}
	if regenerateSequences && resyncAllCollections {
		err := db.updateAllPrincipalsSequences(ctx)
		if err != nil {
			return fmt.Errorf("Error updating principal sequences: %w", err)
		}
	}

	if docsChanged > 0 {
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
	return nil
}

// updateSyncInfo updates the syncInfo metadata for all collections that were resynced and updates the
// DatabaseContext.RequireResync attribute
func updateSyncInfo(ctx context.Context, db *DatabaseContext, collectionIDs []uint32) {
	updatedDsNames := make(map[base.ScopeAndCollectionName]struct{}, len(collectionIDs))
	for _, collectionID := range collectionIDs {
		dbc, ok := db.CollectionByID[collectionID]
		if !ok {
			base.WarnfCtx(ctx, "Completed resync, but unable to update syncInfo for collection %v (not found)", collectionID)
			continue
		}
		if err := base.SetSyncInfoMetadataID(ctx, dbc.dataStore, db.Options.MetadataID, db.ClusterCompatVersion()); err != nil {
			base.WarnfCtx(ctx, "Completed resync, but unable to update syncInfo for collection %v: %v", collectionID, err)
			continue
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
		Options:       r.startOptions,
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
	VBUUIDs       []uint64       `json:"vbuuids"`
	CollectionIDs []uint32       `json:"collection_ids,omitempty"`
	Options       map[string]any `json:"options,omitempty"` // start options, persisted for Resume
}

type ResyncManagerStatusDocDCP struct {
	ResyncManagerResponseDCP `json:"status"`
	ResyncManagerMeta        `json:"meta"`
}

// initializePrincipalDocsIndex creates the metadata indexes required for resync
func initializePrincipalDocsIndex(ctx context.Context, db *DatabaseContext) error {
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

// dcpClientCloser is a helper struct to manage closing of DCP resources. Register cleanup functions using append.
// Cleanup functions are executed in reverse order of registration.
type dcpClientCloser struct {
	lastError error
	closeLock sync.Mutex
	funcs     []func() error
	closed    bool
}

// shutdown will shut down all related resources. This function is only executed once, but it is safe to call
// multiple times and will return the same error if it has already been executed or is being simultaneously
// executed.
func (d *dcpClientCloser) shutdown() error {
	d.closeLock.Lock()
	defer d.closeLock.Unlock()
	if d.closed {
		return d.lastError
	}
	d.closed = true
	var errs []error
	for _, f := range d.funcs {
		errs = append(errs, f())
	}
	d.lastError = errors.Join(errs...)
	return d.lastError
}

// append registers a cleanup function to be executed on shutdown. Functions are executed in reverse order of
// registration.
func (d *dcpClientCloser) append(f func() error) {
	d.funcs = append([]func() error{f}, d.funcs...)
}

// isClosed returns true when shutdown has already been executed.
func (d *dcpClientCloser) isClosed() bool {
	d.closeLock.Lock()
	defer d.closeLock.Unlock()
	return d.closed
}
