//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
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

func NewResyncManager() *BackgroundManager {
	return &BackgroundManager{
		Process:    &ResyncManager{},
		terminator: base.NewSafeTerminator(),
	}
}

func (r *ResyncManager) Init(options map[string]interface{}, clusterStatus []byte) error {
	return nil
}

func (r *ResyncManager) Run(options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	database := options["database"].(*Database)
	regenerateSequences := options["regenerateSequences"].(bool)

	defer atomic.CompareAndSwapUint32(&database.State, DBResyncing, DBOffline)
	callback := func(docsProcessed, docsChanged *int) {
		r.lock.Lock()
		defer r.lock.Unlock()
		r.DocsProcessed = *docsProcessed
		r.DocsChanged = *docsChanged
	}

	_, err := database.UpdateAllDocChannels(regenerateSequences, callback, terminator)
	if err != nil {
		return err
	}

	return nil
}

func (r *ResyncManager) ResetStatus() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.DocsProcessed = 0
	r.DocsChanged = 0
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
