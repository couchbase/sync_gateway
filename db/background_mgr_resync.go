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
	"sync"
	"sync/atomic"

	"github.com/couchbase/sync_gateway/base"
)

// ======================================================
// Resync Implementation of Background Manager Process
// ======================================================

type ResyncManager struct {
	DocsProcessed int
	DocsChanged   int
	lock          sync.Mutex
}

var _ BackgroundManagerProcessI = &ResyncManager{}

func NewResyncManager(metadataStore base.DataStore, metaKeys *base.MetadataKeys) *BackgroundManager {
	return &BackgroundManager{
		name:    "resync",
		Process: &ResyncManager{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "resync",
		},
		terminator: base.NewSafeTerminator(),
	}
}

func (r *ResyncManager) Init(ctx context.Context, options map[string]interface{}, clusterStatus []byte) error {
	return nil
}

func (r *ResyncManager) Run(ctx context.Context, options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	database := options["database"].(*Database)
	regenerateSequences := options["regenerateSequences"].(bool)
	resyncCollections := options["collections"].(ResyncCollections)

	persistClusterStatus := func() {
		err := persistClusterStatusCallback()
		if err != nil {
			base.WarnfCtx(ctx, "Failed to persist cluster status on-demand for resync operation: %v", err)
		}
	}
	defer persistClusterStatus()

	defer atomic.CompareAndSwapUint32(&database.State, DBResyncing, DBOffline)
	callback := func(docsProcessed, docsChanged *int) {
		r.SetStats(*docsProcessed, *docsChanged)
		persistClusterStatus()
	}

	collectionIDs, hasAllCollections, err := getCollectionIds(database, resyncCollections)
	if err != nil {
		return err
	}
	if hasAllCollections {
		base.InfofCtx(ctx, base.KeyAll, "running resync against all collections")
	} else {
		base.InfofCtx(ctx, base.KeyAll, "running resync against specified collections")
	}

	for _, collectionID := range collectionIDs {
		dbc := &DatabaseCollectionWithUser{
			DatabaseCollection: database.CollectionByID[collectionID],
		}
		_, err := dbc.UpdateAllDocChannels(ctx, regenerateSequences, callback, terminator)
		if err != nil {
			return err
		}
		if regenerateSequences {
			base.SetSyncInfo(dbc.dataStore, dbc.dbCtx.Options.MetadataID)
		}
	}

	return nil
}

func (r *ResyncManager) ResetStatus() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.DocsProcessed = 0
	r.DocsChanged = 0
}

func (r *ResyncManager) SetStats(docsProcessed, docsChanged int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.DocsProcessed = docsProcessed
	r.DocsChanged = docsChanged
}

type ResyncManagerResponse struct {
	BackgroundManagerStatus
	DocsChanged   int `json:"docs_changed"`
	DocsProcessed int `json:"docs_processed"`
}

func (r *ResyncManager) GetProcessStatus(backgroundManagerStatus BackgroundManagerStatus) ([]byte, []byte, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	retStatus := ResyncManagerResponse{
		BackgroundManagerStatus: backgroundManagerStatus,
		DocsChanged:             r.DocsChanged,
		DocsProcessed:           r.DocsProcessed,
	}

	statusJSON, err := base.JSONMarshal(retStatus)
	return statusJSON, nil, err
}
