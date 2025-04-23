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
	lock      sync.Mutex
	statusMap *map[string]map[string]string
	doneChan  chan error
}

// Init is called synchronously to set up a run for the background manager process. See Run() for the async part.
func (a *AsyncIndexInitManager) Init(ctx context.Context, options map[string]interface{}, clusterStatus []byte) error {
	a.statusMap = options["statusMap"].(*map[string]map[string]string)
	a.doneChan = options["doneChan"].(chan error)
	return nil
}

// Run is called inside a goroutine to perform the job of the job. This function should block until the job is complete.
func (a *AsyncIndexInitManager) Run(ctx context.Context, options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	return <-a.doneChan
}

type AsyncIndexInitManagerResponse struct {
	BackgroundManagerStatus
	IndexStatus map[string]map[string]string `json:"index_status"` // scope->collection->status
}

type AsyncIndexInitManagerStatusDoc struct {
	AsyncIndexInitManagerResponse `json:"status"`
}

func (a *AsyncIndexInitManager) GetProcessStatus(status BackgroundManagerStatus) (statusOut []byte, meta []byte, err error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	var statusMap map[string]map[string]string
	if a.statusMap != nil {
		statusMap = *a.statusMap
	}

	retStatus := AsyncIndexInitManagerResponse{
		BackgroundManagerStatus: status,
		IndexStatus:             statusMap,
	}

	statusJSON, err := base.JSONMarshal(retStatus)
	return statusJSON, nil, err
}

func (a *AsyncIndexInitManager) ResetStatus() {
	a.statusMap = nil
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
