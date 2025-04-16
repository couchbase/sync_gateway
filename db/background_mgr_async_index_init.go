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

type IndexStatus string

const (
	IndexStatusInProgress IndexStatus = "in progress"
	IndexStatusReady      IndexStatus = "ready"
)

// AsyncIndexInitManager is a background manager process that is able to initialize a new set of indexes on an already running database - assuming the new indexes are named differently such that both can co-exist simultaneously.
type AsyncIndexInitManager struct {
	lock         sync.Mutex
	_indexStatus map[string]map[string]IndexStatus // scope->collection->status
}

// Init is called synchronously to set up a run for the background manager process but must not block for the full process. See Run() for more info about the async part.
func (a *AsyncIndexInitManager) Init(ctx context.Context, options map[string]interface{}, clusterStatus []byte) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	//database := options["database"].(*Database)

	scopes := options["scopes"].([]string)
	collections := options["collections"].([]string)
	a._indexStatus = make(map[string]map[string]IndexStatus)
	for _, scope := range scopes {
		a._indexStatus[scope] = make(map[string]IndexStatus)
		for _, collection := range collections {
			// Init it with InProgress just to reduce complexity and number of states... we know we're about to hit Run shortly anyway.
			a._indexStatus[scope][collection] = IndexStatusInProgress
		}
	}

	return nil
}

// Run is called to start the process and is expected to be run inside a goroutine. This function should block until the job is complete.
func (a *AsyncIndexInitManager) Run(ctx context.Context, options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {

	return nil
}

type AsyncIndexInitManagerResponse struct {
	BackgroundManagerStatus
	IndexStatus map[string]map[string]IndexStatus `json:"index_status"` // scope->collection->status
}

type AsyncIndexInitManagerStatusDoc struct {
	AttachmentManagerResponse `json:"status"`
}

func (a *AsyncIndexInitManager) GetProcessStatus(status BackgroundManagerStatus) (statusOut []byte, meta []byte, err error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	retStatus := AsyncIndexInitManagerResponse{
		BackgroundManagerStatus: status,
		IndexStatus:             a._indexStatus,
	}

	statusJSON, err := base.JSONMarshal(retStatus)
	return statusJSON, nil, err
}

func (a *AsyncIndexInitManager) ResetStatus() {
	a.lock.Lock()
	defer a.lock.Unlock()
	a._indexStatus = nil
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
