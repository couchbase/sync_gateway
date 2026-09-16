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
	"errors"
	"maps"
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

// AsyncIndexInitManager is a background manager process that manages the cross-node job state and status of an async invocation of DatabaseInitManager (via /db/_index_init)
// This manager does not do the actual work to initialize indexes, due to go package boundaries and import-cycles, but is being fed status updates from callbacks passed into rest.DatabaseInitManager
// This status can be viewed cross-node, and similarly the start/stop actions can be used cross-node with this AsyncIndexInitManager layer.
type AsyncIndexInitManager struct {
	lock           sync.Mutex
	_statusTracker *IndexStatusTracker // _statusTracker is updated by the DatabaseInitManager's callbacks. Here for BackgroundManager persistence. Never nil.
	_doneChan      chan error          // _doneChan is a DatabaseInitManager worker's done channel. Here to allow Run to block until complete.
}

// NewAsyncIndexInitProcess returns a manager process with an empty status tracker, ready to report status before any
// run has started.
func NewAsyncIndexInitProcess() *AsyncIndexInitManager {
	return &AsyncIndexInitManager{_statusTracker: NewIndexStatusTracker()}
}

// AsyncIndexInitOptions defines the options passed when starting an asynchronous index initialization process.
type AsyncIndexInitOptions struct {
	// StatusTracker provides a reference to the structure tracking index status per collection.
	StatusTracker *IndexStatusTracker
	// DoneChan receives the completion status/error from the index initialization task.
	DoneChan chan error
}

// validate returns an error if the options are not usable by Init/Run.
func (o AsyncIndexInitOptions) validate() error {
	if o.StatusTracker == nil {
		return errors.New("async index init requires a StatusTracker")
	}
	if o.DoneChan == nil {
		return errors.New("async index init requires a DoneChan")
	}
	return nil
}

// Init is called synchronously to set up a run for the background manager process. See Run() for the async part.
func (a *AsyncIndexInitManager) Init(ctx context.Context, options AsyncIndexInitOptions, clusterStatus []byte) (backgroundManagerInitMode, error) {
	if err := options.validate(); err != nil {
		return backgroundManagerInitReset, err
	}

	a.lock.Lock()
	defer a.lock.Unlock()
	a._statusTracker = options.StatusTracker
	a._doneChan = options.DoneChan
	return backgroundManagerInitReset, nil
}

// Run is called inside a goroutine to perform the job of the job. This function should block until the job is complete.
func (a *AsyncIndexInitManager) Run(ctx context.Context, options AsyncIndexInitOptions, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	if err := options.validate(); err != nil {
		return err
	}

	a.lock.Lock()
	doneChan := a._doneChan
	a.lock.Unlock()
	err := <-doneChan
	if terminator.IsClosed() {
		return nil
	}
	return err
}

type CollectionIndexStatus string

const (
	CollectionIndexStatusQueued     CollectionIndexStatus = "queued"
	CollectionIndexStatusInProgress CollectionIndexStatus = "in progress"
	CollectionIndexStatusReady      CollectionIndexStatus = "ready"
	CollectionIndexStatusError      CollectionIndexStatus = "error"
)

type IndexStatusByCollection map[string]map[string]CollectionIndexStatus // scope->collection->status

// IndexStatusTracker holds the index initialization status for each collection. The DatabaseInitManager worker
// updates it while status requests read it, so all access is guarded by lock.
type IndexStatusTracker struct {
	lock      sync.Mutex
	_statuses IndexStatusByCollection
}

// NewIndexStatusTracker returns a tracker with an empty collection set for each of the given scopes.
func NewIndexStatusTracker(scopes ...string) *IndexStatusTracker {
	statuses := make(IndexStatusByCollection, len(scopes))
	for _, scope := range scopes {
		if _, ok := statuses[scope]; !ok {
			statuses[scope] = make(map[string]CollectionIndexStatus)
		}
	}
	return &IndexStatusTracker{_statuses: statuses}
}

// Set records the status of the given collection.
func (t *IndexStatusTracker) Set(scName base.ScopeAndCollectionName, status CollectionIndexStatus) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if _, ok := t._statuses[scName.ScopeName()]; !ok {
		t._statuses[scName.ScopeName()] = make(map[string]CollectionIndexStatus)
	}
	t._statuses[scName.ScopeName()][scName.CollectionName()] = status
}

// copy returns a copy of the current statuses, safe to use after the tracker is updated again.
func (t *IndexStatusTracker) copy() IndexStatusByCollection {
	t.lock.Lock()
	defer t.lock.Unlock()
	statuses := make(IndexStatusByCollection, len(t._statuses))
	for scope, collections := range t._statuses {
		statuses[scope] = maps.Clone(collections)
	}
	return statuses
}

type AsyncIndexInitManagerResponse struct {
	BackgroundManagerStatus
	IndexStatus IndexStatusByCollection `json:"index_status"`
}

func (a *AsyncIndexInitManager) SetProcessStatus(context.Context, []byte, []byte) {}

func (a *AsyncIndexInitManager) GetProcessStatus(status BackgroundManagerStatus, _ []byte) (statusOut []byte, meta []byte, err error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	retStatus := AsyncIndexInitManagerResponse{
		BackgroundManagerStatus: status,
		IndexStatus:             a._statusTracker.copy(),
	}

	statusJSON, err := base.JSONMarshal(retStatus)
	return statusJSON, nil, err
}

func (a *AsyncIndexInitManager) ResetStatus() {
	a.lock.Lock()
	defer a.lock.Unlock()
	a._statusTracker = NewIndexStatusTracker()
	a._doneChan = nil
}

var _ BackgroundManagerProcessI[AsyncIndexInitOptions] = &AsyncIndexInitManager{}

func NewAsyncIndexInitManager(metadataStore base.DataStore, metaKeys *base.MetadataKeys) *BackgroundManager[AsyncIndexInitOptions] {
	return &BackgroundManager[AsyncIndexInitOptions]{
		name:    "index_init",
		Process: NewAsyncIndexInitProcess(),
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "index_init",
		},
		terminator: base.NewSafeTerminator(),
	}
}
