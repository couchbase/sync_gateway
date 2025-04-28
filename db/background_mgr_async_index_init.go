//  Copyright 2025-Present Couchbase, Inc.
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

	"github.com/couchbase/sync_gateway/base"
)

// AsyncIndexInitManager is a background manager process that manages the cross-node job state and status of an async invocation of DatabaseInitManager (via /db/_index_init)
// This manager does not do the actual work to initialize indexes, due to go package boundaries and import-cycles, but is being fed status updates from callbacks passed into rest.DatabaseInitManager
// This status can be viewed cross-node, and similarly the start/stop actions can be used cross-node with this AsyncIndexInitManager layer.
type AsyncIndexInitManager struct {
	lock       sync.Mutex
	_statusMap *IndexStatusByCollection // _statusMap is updated by the DatabaseInitManager's callbacks. Here for BackgroundManager persistence.
	_doneChan  chan error               // _doneChan is a DatabaseInitManager worker's done channel. Here to allow Run to block until complete.
}

// Init is called synchronously to set up a run for the background manager process. See Run() for the async part.
func (a *AsyncIndexInitManager) Init(ctx context.Context, options map[string]interface{}, clusterStatus []byte) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	a._statusMap = options["statusMap"].(*IndexStatusByCollection)
	a._doneChan = options["doneChan"].(chan error)
	return nil
}

// Run is called inside a goroutine to perform the job of the job. This function should block until the job is complete.
func (a *AsyncIndexInitManager) Run(ctx context.Context, options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	a.lock.Lock()
	doneChan := a._doneChan
	a.lock.Unlock()
	select {
	case err := <-doneChan:
		return err
	case <-terminator.Done():
		return nil
	}
}

type CollectionIndexStatus string

const (
	CollectionIndexStatusQueued     CollectionIndexStatus = "queued"
	CollectionIndexStatusInProgress CollectionIndexStatus = "in progress"
	CollectionIndexStatusReady      CollectionIndexStatus = "ready"
	CollectionIndexStatusError      CollectionIndexStatus = "error"
)

type IndexStatusByCollection map[string]map[string]CollectionIndexStatus // scope->collection->status

type AsyncIndexInitManagerResponse struct {
	BackgroundManagerStatus
	IndexStatus IndexStatusByCollection `json:"index_status"`
}

func (a *AsyncIndexInitManager) GetProcessStatus(status BackgroundManagerStatus) (statusOut []byte, meta []byte, err error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	var statusMap IndexStatusByCollection
	if a._statusMap != nil {
		statusMap = *a._statusMap
	}

	retStatus := AsyncIndexInitManagerResponse{
		BackgroundManagerStatus: status,
		IndexStatus:             statusMap,
	}

	statusJSON, err := base.JSONMarshal(retStatus)
	return statusJSON, nil, err
}

func (a *AsyncIndexInitManager) ResetStatus() {
	a.lock.Lock()
	defer a.lock.Unlock()
	a._statusMap = nil
	a._doneChan = nil
	return
}

var _ BackgroundManagerProcessI = &AsyncIndexInitManager{}

func NewAsyncIndexInitManager(metadataStore base.DataStore, metaKeys *base.MetadataKeys) *BackgroundManager {
	return &BackgroundManager{
		name:    "index_init",
		Process: &AsyncIndexInitManager{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "index_init",
		},
		terminator: base.NewSafeTerminator(),
	}
}
