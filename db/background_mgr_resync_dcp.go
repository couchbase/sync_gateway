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
	ResyncedCollections []string
	lock                sync.RWMutex
}

// ResyncCollections contains map of scope names with collection names against which resync needs to run
type ResyncCollections map[string][]string

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

func (r *ResyncManagerDCP) Init(ctx context.Context, options map[string]interface{}, clusterStatus []byte) error {
	newRunInit := func() error {
		uniqueUUID, err := uuid.NewRandom()
		if err != nil {
			return err
		}

		r.ResyncID = uniqueUUID.String()
		base.InfofCtx(ctx, base.KeyAll, "Resync: Starting new resync run with resync ID: %q", r.ResyncID)
		return nil
	}

	if clusterStatus != nil {
		var statusDoc ResyncManagerStatusDocDCP
		err := base.JSONUnmarshal(clusterStatus, &statusDoc)

		reset, ok := options["reset"].(bool)
		if reset && ok {
			base.InfofCtx(ctx, base.KeyAll, "Resync: Resetting resync process. Will not resume any partially completed process")
		}

		// If the previous run completed, or there was an error during unmarshalling the status we will start the
		// process from scratch with a new resync ID. Otherwise, we should resume with the resync ID, stats specified in the doc.
		if statusDoc.State == BackgroundProcessStateCompleted || err != nil || (reset && ok) {
			return newRunInit()
		}
		r.ResyncID = statusDoc.ResyncID
		r.SetStatus(statusDoc.DocsChanged, statusDoc.DocsProcessed)

		base.InfofCtx(ctx, base.KeyAll, "Resync: Attempting to resume resync with resync ID: %s", r.ResyncID)

		return nil
	}

	return newRunInit()
}

func (r *ResyncManagerDCP) Run(ctx context.Context, options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	db := options["database"].(*Database)
	regenerateSequences := options["regenerateSequences"].(bool)
	resyncCollections := options["collections"].(ResyncCollections)

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
		key := realDocID(docID)
		base.TracefCtx(ctx, base.KeyAll, "[%s] Received DCP event %d for doc %v", resyncLoggingID, event.Opcode, base.UD(docID))

		// Ignore documents without xattrs if possible, to avoid processing unnecessary documents
		if r.useXattrs && event.DataType&base.MemcachedDataTypeXattr == 0 {
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

		r.DocsProcessed.Add(1)
		databaseCollection := db.CollectionByID[event.CollectionID]
		_, unusedSequences, err := (&DatabaseCollectionWithUser{
			DatabaseCollection: databaseCollection,
		}).resyncDocument(ctx, docID, key, regenerateSequences, []uint64{})

		databaseCollection.releaseSequences(ctx, unusedSequences)

		if err == nil {
			r.DocsChanged.Add(1)
		} else if err != base.ErrUpdateCancel {
			base.WarnfCtx(ctx, "[%s] Error updating doc %q: %v", resyncLoggingID, base.UD(docID), err)
		}
		return true
	}

	bucket, err := base.AsGocbV2Bucket(db.Bucket)
	if err != nil {
		return err
	}

	// Get collectionIds
	collectionIDs, hasAllCollections, collectionNames, err := getCollectionIdsAndNames(db, resyncCollections)
	if err != nil {
		return err
	}
	// add collection list to manager for use in status call
	r.SetCollectionStatus(collectionNames)
	if hasAllCollections {
		base.InfofCtx(ctx, base.KeyAll, "[%s] running resync against all collections", resyncLoggingID)
	} else {
		base.InfofCtx(ctx, base.KeyAll, "[%s] running resync against specified collections", resyncLoggingID)
	}

	clientOptions := getResyncDCPClientOptions(collectionIDs, db.Options.GroupID, db.MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID))

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

		if (regenerateSequences && resyncCollections == nil) || r.DocsChanged.Value() > 0 {
			err := initializeMetadataIndexes(ctx, db)
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
			var multiError *base.MultiError
			for _, databaseCollection := range db.CollectionByID {
				err := databaseCollection.invalidateAllPrincipalsCache(ctx, endSeq)
				if err != nil {
					multiError = multiError.Append(err)
				}
			}
			if multiError.Len() > 0 {
				return fmt.Errorf("Error invalidating principals cache: %s", multiError.Error())
			}

		}

		// If we regenerated sequences, update syncInfo for all collections affected
		if regenerateSequences {
			updatedDsNames := make(map[base.ScopeAndCollectionName]struct{}, len(collectionIDs))
			for _, collectionID := range collectionIDs {
				dbc, ok := db.CollectionByID[collectionID]
				if !ok {
					base.WarnfCtx(ctx, "[%s] Completed resync, but unable to update syncInfo for collection %v (not found)", resyncLoggingID, collectionID)
				}
				if err := base.SetSyncInfo(dbc.dataStore, db.DatabaseContext.Options.MetadataID); err != nil {
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

func getCollectionIdsAndNames(db *Database, resyncCollections ResyncCollections) ([]uint32, bool, []string, error) {
	collectionIDs := make([]uint32, 0)
	var hasAllCollections bool
	var resyncCollectionNames []string

	if len(resyncCollections) == 0 {
		hasAllCollections = true
		for collectionID := range db.CollectionByID {
			collectionIDs = append(collectionIDs, collectionID)
		}
		for _, collectionNames := range db.CollectionNames {
			for collName := range collectionNames {
				resyncCollectionNames = append(resyncCollectionNames, collName)
			}
		}
	} else {
		hasAllCollections = false

		for scopeName, collectionsName := range resyncCollections {
			for _, collectionName := range collectionsName {
				collection, err := db.GetDatabaseCollection(scopeName, collectionName)
				if err != nil {
					return nil, hasAllCollections, nil, fmt.Errorf("failed to find ID for collection %s.%s", base.MD(scopeName).Redact(), base.MD(collectionName).Redact())
				}
				collectionIDs = append(collectionIDs, collection.GetCollectionID())
				resyncCollectionNames = append(resyncCollectionNames, collectionName)
			}
		}
	}
	return collectionIDs, hasAllCollections, resyncCollectionNames, nil
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

func (r *ResyncManagerDCP) SetCollectionStatus(collectionNames []string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.ResyncedCollections = collectionNames
}

type ResyncManagerResponseDCP struct {
	BackgroundManagerStatus
	ResyncID              string   `json:"resync_id"`
	DocsChanged           int64    `json:"docs_changed"`
	DocsProcessed         int64    `json:"docs_processed"`
	CollectionsProcessing []string `json:"collections_processing,omitempty"`
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

	meta := AttachmentManagerMeta{
		VBUUIDs: r.VBUUIDs,
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
	VBUUIDs []uint64 `json:"vbuuids"`
}

type ResyncManagerStatusDocDCP struct {
	ResyncManagerResponseDCP `json:"status"`
	ResyncManagerMeta        `json:"meta"`
}

// initializeMetadataIndexes creates the metadata indexes required for resync
func initializeMetadataIndexes(ctx context.Context, db *Database) error {
	n1qlStore, ok := base.AsN1QLStore(db.MetadataStore)
	if !ok {
		return errors.New("Cannot create indexes on non-Couchbase data store.")
	}
	options := InitializeIndexOptions{
		FailFast:        false,
		NumReplicas:     db.Options.NumIndexReplicas,
		MetadataIndexes: IndexesMetadataOnly,
		UseXattrs:       db.UseXattrs(),
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
