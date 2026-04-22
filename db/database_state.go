//  Copyright 2026-Present Couchbase, Inc.
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
	"time"

	"github.com/couchbase/sync_gateway/base"
)

type DatabaseState struct {
	ResyncRunning bool `json:"resync,omitempty"`
}

type ResyncHandler func(resume bool)
type DatabaseStateMgr struct {
	State           DatabaseState
	CAS             uint64
	dbStateID       string
	terminator      *base.SafeTerminator
	metadataStore   base.DataStore
	pollingInterval time.Duration
	resyncHandler   ResyncHandler
	lock            sync.Mutex
}

// NewDatabaseStateMgr creates an OfflineDatabaseStateMgr for the given database.
func NewDatabaseStateMgr(metadataStore base.DataStore, dbStateID string) *DatabaseStateMgr {
	return &DatabaseStateMgr{
		dbStateID:       dbStateID,
		CAS:             0,
		terminator:      base.NewSafeTerminator(),
		metadataStore:   metadataStore,
		pollingInterval: 10 * time.Second,
	}
}

// UpdateState persists the given DatabaseState to the metadata store using a CAS write, then updates the
// in-memory State and CAS on success. Returns an error on CAS mismatch or store failure.
func (dbMgr *DatabaseStateMgr) UpdateState(state DatabaseState) (err error) {
	dbMgr.lock.Lock()
	defer dbMgr.lock.Unlock()
	cas, err := dbMgr.metadataStore.WriteCas(dbMgr.dbStateID, 0, dbMgr.CAS, state, 0)
	if err != nil {
		return err
	}
	dbMgr.State = state
	dbMgr.CAS = cas
	return
}

// GetState reads the current DatabaseState document from the metadata store. Returns the state and its CAS value.
// A doc-not-found error is treated as a zero-value state (no state document exists) and is not returned as an error.
func (dbMgr *DatabaseStateMgr) GetState() (state DatabaseState, cas uint64, err error) {
	cas, err = dbMgr.metadataStore.Get(dbMgr.dbStateID, &state)
	if err != nil && !base.IsDocNotFoundError(err) {
		return state, cas, err
	}
	return
}

// DeleteState removes the database state document from the metadata store using a CAS-protected Remove.
// Returns an error on CAS mismatch or if the document does not exist.
func (dbMgr *DatabaseStateMgr) DeleteState() (err error) {
	dbMgr.lock.Lock()
	defer dbMgr.lock.Unlock()
	_, err = dbMgr.metadataStore.Remove(dbMgr.dbStateID, dbMgr.CAS)
	if err != nil {
		return
	}
	dbMgr.CAS = 0
	dbMgr.State = DatabaseState{}
	return
}

// AddResyncFunc registers the callback that is invoked by the polling loop when a state change is detected.
// The callback receives true if a resync should resume, or false if the state document was removed (resync complete).
func (dbMgr *DatabaseStateMgr) AddResyncFunc(resyncFunc ResyncHandler) {
	dbMgr.resyncHandler = resyncFunc
}

// StartPolling launches a background goroutine that periodically calls poll() at dbMgr.pollingInterval.
// The goroutine exits when StopPolling is called (via the terminator). AddResyncFunc must be called before
// StartPolling so that state change notifications have a registered handler.
func (dbMgr *DatabaseStateMgr) StartPolling(ctx context.Context) {
	ticker := time.NewTicker(dbMgr.pollingInterval)
	go func() {
		defer base.FatalPanicHandler()
		for {
			select {
			case <-dbMgr.terminator.Done():
				return
			case <-ticker.C:
				dbMgr.poll(ctx)
			}
		}
	}()
}

// poll reads the offline state document from the metadata store and invokes the resumeFunc if the CAS has
// changed. If the document is not found it calls resumeFunc(false) to signal that resync is no longer
// running. CAS changes that match the locally held CAS are ignored to avoid redundant callbacks.
func (dbMgr *DatabaseStateMgr) poll(ctx context.Context) {
	state, cas, err := dbMgr.GetState()
	if err != nil {
		if base.IsDocNotFoundError(err) {
			dbMgr.resyncHandler(false)
			return
		}
		base.WarnfCtx(ctx, "error while polling for offline database state: %v", err)
	}
	if cas == dbMgr.CAS {
		return
	}
	dbMgr.resyncHandler(state.ResyncRunning)
	dbMgr.CAS = cas
	dbMgr.State = state
}

// StopPolling signals the background polling goroutine started by StartPolling to exit.
func (dbMgr *DatabaseStateMgr) StopPolling() {
	dbMgr.terminator.Close()
}
