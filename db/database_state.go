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
	"errors"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

var databaseStatePollingInterval = 10 * time.Second

type resyncResumeFunc func(ctx context.Context) error

type DatabaseState struct {
	ResyncRunning *bool `json:"resync,omitempty"`
}

type DatabaseStateMgr struct {
	CAS             uint64
	dbStateID       string
	metadataStore   base.DataStore
	pollingInterval time.Duration
	resumeResync    resyncResumeFunc
	lock            sync.Mutex
	terminator      chan struct{}
	done            chan struct{}
}

// NewDatabaseStateMgr creates a DatabaseStateMgr for the given database. If resumeResync is non-nil, it will be invoked when DatabaseState.ResyncRunning is detected as changed on the bucket.
func NewDatabaseStateMgr(metadataStore base.DataStore, dbStateID string, resumeResync resyncResumeFunc) *DatabaseStateMgr {
	return &DatabaseStateMgr{
		dbStateID:       dbStateID,
		CAS:             0,
		metadataStore:   metadataStore,
		pollingInterval: databaseStatePollingInterval,
		resumeResync:    resumeResync,
	}
}

// UpdateState persists the given DatabaseState to the metadata store using a CAS write, then updates the
// in-memory State and CAS on success. Returns an error store failure.
func (dbMgr *DatabaseStateMgr) UpdateState(ctx context.Context, newState DatabaseState) error {
	dbMgr.lock.Lock()
	defer dbMgr.lock.Unlock()
	cas, err := dbMgr.metadataStore.Update(ctx, dbMgr.dbStateID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
		var state DatabaseState
		if current != nil {
			err = base.JSONUnmarshal(current, &state)
			if err != nil {
				return nil, nil, false, err
			}
		}
		if newState.ResyncRunning != nil {
			if state.ResyncRunning != nil && *state.ResyncRunning == *newState.ResyncRunning {
				return nil, nil, false, base.ErrUpdateCancel
			}
			state.ResyncRunning = newState.ResyncRunning
		}
		bodyBytes, err := base.JSONMarshal(state)
		if err != nil {
			return nil, nil, false, err
		}
		return bodyBytes, nil, false, nil
	})
	if errors.Is(err, base.ErrUpdateCancel) {
		return nil
	}
	if err != nil {
		return err
	}
	dbMgr.CAS = cas
	return nil
}

// GetState reads the current DatabaseState document from the metadata store. Returns the state and its CAS value.
// All errors, including doc-not-found, are returned to the caller; callers must check base.IsDocNotFoundError
// to distinguish a missing document from a real store failure.
func (dbMgr *DatabaseStateMgr) GetState(ctx context.Context) (*DatabaseState, uint64, error) {
	var state DatabaseState
	cas, err := dbMgr.metadataStore.Get(ctx, dbMgr.dbStateID, &state)
	if err != nil {
		return nil, cas, err
	}
	return &state, cas, nil
}

// StartPolling launches a background goroutine that periodically calls poll() at dbMgr.pollingInterval.
// The goroutine exits when StopPolling is called (via the terminator).
func (dbMgr *DatabaseStateMgr) StartPolling(ctx context.Context) {
	dbMgr.terminator = make(chan struct{})
	dbMgr.done = make(chan struct{})
	ticker := time.NewTicker(dbMgr.pollingInterval)
	go func() {
		defer base.FatalPanicHandler(ctx)
		defer close(dbMgr.done)
		for {
			select {
			case <-dbMgr.terminator:
				ticker.Stop()
				return
			case <-ticker.C:
				dbMgr.poll(ctx)
			}
		}
	}()
}

// poll checks whether the state document has been updated and calls appropriate callback functions. If the callback
// functions return any errors, do not update the CAS so the next time poll runs, the callback function will rerun.
func (dbMgr *DatabaseStateMgr) poll(ctx context.Context) {
	newCAS, state, ok := dbMgr.isUpdated(ctx)
	if !ok {
		return
	}
	if dbMgr.resumeResync != nil {
		if state.ResyncRunning != nil && *state.ResyncRunning {
			err := dbMgr.resumeResync(ctx)
			if err != nil {
				base.WarnfCtx(ctx, "failed to resume resync from DatabaseStateMgr: %v, will try again.", err)
				return // leave CAS stale so next tick retries
			}
		}
	}
	dbMgr.lock.Lock()
	defer dbMgr.lock.Unlock()
	dbMgr.CAS = newCAS
}

// isUpdated reads the current state document and reports whether it has changed since the last
// successful poll. It returns (newCAS, state, true) when the store CAS differs from the locally
// held CAS, or (0, nil, false) when the CAS is unchanged, the document is not found, or a store
// error occurs. The locally held CAS is never modified here; callers are responsible for advancing
// it after they have successfully acted on the change.
func (dbMgr *DatabaseStateMgr) isUpdated(ctx context.Context) (uint64, *DatabaseState, bool) {
	state, cas, err := dbMgr.GetState(ctx)
	if err != nil {
		if !base.IsDocNotFoundError(err) {
			base.WarnfCtx(ctx, "error while polling for offline database state: %v", err)
		}
		return 0, nil, false
	}
	dbMgr.lock.Lock()
	defer dbMgr.lock.Unlock()
	if cas == dbMgr.CAS {
		return 0, nil, false
	}
	return cas, state, true
}

// StopPolling signals the background polling goroutine started by StartPolling to exit.
func (dbMgr *DatabaseStateMgr) StopPolling(ctx context.Context) {
	err := base.TerminateAndWaitForClose(dbMgr.terminator, dbMgr.done, BGTCompletionMaxWait)
	if err != nil {
		base.WarnfCtx(ctx, "couldn't stop background database state polling: %v", err)
	}
}
