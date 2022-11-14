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
	DocsProcessed base.AtomicInt
	DocsChanged   base.AtomicInt
	ResyncID      string
	VBUUIDs       []uint64
	lock          sync.RWMutex
}

var _ BackgroundManagerProcessI = &ResyncManagerDCP{}

func NewResyncManagerDCP(metadataStore base.DataStore) *BackgroundManager {
	return &BackgroundManager{
		Process: &ResyncManagerDCP{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
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
			base.InfofCtx(ctx, base.KeyAll, "Resync: Resetting resync process. Will not  resume any "+
				"partially completed process")
		}

		// If the previous run completed, or there was an error during unmarshalling the status we will start the
		// process from scratch with a new resync ID. Otherwise, we should resume with the resync ID, stats specified in the doc.
		if statusDoc.State == BackgroundProcessStateCompleted || err != nil || (reset && ok) {
			return newRunInit()
		} else {
			r.ResyncID = statusDoc.ResyncID
			r.SetStatus(statusDoc.DocsChanged, statusDoc.DocsProcessed)

			base.InfofCtx(ctx, base.KeyAll, "Resync: Attempting to resume resync with resycn ID: %s", r.ResyncID)
		}

		return nil
	}

	return newRunInit()
}

func (r *ResyncManagerDCP) Run(ctx context.Context, options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	db := options["database"].(*Database)
	regenerateSequences := options["regenerateSequences"].(bool)

	resyncLoggingID := "Resync: " + r.ResyncID

	persistClusterStatus := func() {
		err := persistClusterStatusCallback()
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to persist cluster status on-demand for resync operation: %v", resyncLoggingID, err)
		}
	}
	defer persistClusterStatus()

	defer atomic.CompareAndSwapUint32(&db.State, DBResyncing, DBOffline)

	callback := func(event sgbucket.FeedEvent) bool {
		var err error
		docID := string(event.Key)
		key := realDocID(docID)
		base.TracefCtx(ctx, base.KeyAll, "[%s] Received DCP event %d for doc %v", resyncLoggingID, event.Opcode, base.UD(docID))
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
		}).updateDocument(ctx, docID, key, regenerateSequences, []uint64{})

		databaseCollection.releaseSequences(ctx, unusedSequences)

		if err == nil {
			r.DocsChanged.Add(1)
		} else if err != base.ErrUpdateCancel {
			base.WarnfCtx(ctx, "[%s] Error updating doc %q: %v", resyncLoggingID, base.UD(docID), err)
		}
		return true
	}

	collection, err := base.AsGocbV2Bucket(db.Bucket)
	if err != nil {
		return err
	}
	collectionID := base.DefaultCollectionID
	collectionIDs := []uint32{collectionID}

	clientOptions, err := getReSyncDCPClientOptions(collectionIDs, db.Options.GroupID)
	if err != nil {
		return err
	}

	dcpFeedKey := generateResyncDCPStreamName(r.ResyncID)
	dcpClient, err := base.NewDCPClient(dcpFeedKey, callback, *clientOptions, collection)
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

		if regenerateSequences {
			var err error
			for _, databaseCollection := range db.CollectionByID {
				if updateErr := databaseCollection.updateAllPrincipalsSequences(ctx); updateErr != nil {
					err = updateErr
				}
			}

			if err != nil {
				return err
			}
		}

		if r.DocsChanged.Value() > 0 {
			endSeq, err := db.sequences.getSequence()
			if err != nil {
				return err
			}
			for _, databaseCollection := range db.CollectionByID {
				databaseCollection.invalidateAllPrincipalsCache(ctx, endSeq)
			}

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

func (r *ResyncManagerDCP) ResetStatus() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.DocsProcessed.Set(0)
	r.DocsChanged.Set(0)
}

func (r *ResyncManagerDCP) SetStatus(docChanged, docProcessed int64) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.DocsChanged.Set(docChanged)
	r.DocsProcessed.Set(docProcessed)
}

type ResyncManagerResponseDCP struct {
	BackgroundManagerStatus
	ResyncID      string `json:"resync_id"`
	DocsChanged   int64  `json:"docs_changed"`
	DocsProcessed int64  `json:"docs_processed"`
}

func (r *ResyncManagerDCP) GetProcessStatus(status BackgroundManagerStatus) ([]byte, []byte, error) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	response := ResyncManagerResponseDCP{
		BackgroundManagerStatus: status,
		ResyncID:                r.ResyncID,
		DocsChanged:             r.DocsChanged.Value(),
		DocsProcessed:           r.DocsProcessed.Value(),
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

// getReSyncDCPClientOptions returns the default set of DCPClientOptions suitable for resync
func getReSyncDCPClientOptions(collectionIDs []uint32, groupID string) (*base.DCPClientOptions, error) {
	clientOptions := &base.DCPClientOptions{
		OneShot:           true,
		FailOnRollback:    true,
		MetadataStoreType: base.DCPMetadataStoreCS,
		GroupID:           groupID,
		CollectionIDs:     collectionIDs,
	}

	return clientOptions, nil
}

func generateResyncDCPStreamName(resyncID string) string {
	return fmt.Sprintf(
		"sg-%v:resync:%v",
		base.ProductAPIVersion,
		resyncID)
}
