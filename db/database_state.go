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

type DatabaseState struct {
	ResyncRunning *bool `json:"resync,omitempty"`
}

type DatabaseStateMgr struct {
	CAS             uint64
	dbStateID       string
	metadataStore   base.DataStore
	pollingInterval time.Duration
	resumeFunc      func(ctx context.Context) error
	lock            sync.Mutex
	terminator      chan struct{}
	done            chan struct{}
}

// NewDatabaseStateMgr creates a DatabaseStateMgr for the given database. resumeFunc is called by the polling loop
// when a state change is detected with ResyncRunning set to true; it should resume the resync process.
func NewDatabaseStateMgr(metadataStore base.DataStore, dbStateID string, resumeFunc func(ctx context.Context) error) *DatabaseStateMgr {
	return &DatabaseStateMgr{
		dbStateID:       dbStateID,
		CAS:             0,
		metadataStore:   metadataStore,
		pollingInterval: 10 * time.Second,
		resumeFunc:      resumeFunc,
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

// poll checks whether the resync handler should be invoked and, if so, calls it.
// It delegates the state-change decision to ShouldRunResyncHandler.
func (dbMgr *DatabaseStateMgr) poll(ctx context.Context) {
	if ok, state := dbMgr.ShouldRunResyncHandler(ctx); ok {
		if state.ResyncRunning != nil && *state.ResyncRunning {
			if err := dbMgr.resumeFunc(ctx); err != nil && !errors.Is(err, errBackgroundManagerStatusNotRunning) {
				base.WarnfCtx(ctx, "failed to resume resync: %v", err)
			}
		}
	}
}

// ShouldRunResyncHandler reads the current state document and determines whether the resync handler
// should be invoked. It returns (true, state) when the store CAS differs from the locally held CAS
// and ResyncRunning is non-nil. It returns (false, nil) when the CAS is unchanged, the document is
// not found, or ResyncRunning is nil. On any other store error it logs a warning and returns (false, nil).
// When a CAS change is detected the locally held CAS is updated regardless of whether the handler fires.
func (dbMgr *DatabaseStateMgr) ShouldRunResyncHandler(ctx context.Context) (bool, *DatabaseState) {
	state, cas, err := dbMgr.GetState(ctx)
	dbMgr.lock.Lock()
	defer dbMgr.lock.Unlock()
	if err != nil {
		if !base.IsDocNotFoundError(err) {
			base.WarnfCtx(ctx, "error while polling for offline database state: %v", err)
		}
		return false, nil
	}
	if cas == dbMgr.CAS {
		return false, nil
	}
	dbMgr.CAS = cas
	if state.ResyncRunning == nil {
		return false, nil
	}
	return true, state
}

// StopPolling signals the background polling goroutine started by StartPolling to exit.
func (dbMgr *DatabaseStateMgr) StopPolling(ctx context.Context) {
	err := base.TerminateAndWaitForClose(dbMgr.terminator, dbMgr.done, BGTCompletionMaxWait)
	if err != nil {
		base.WarnfCtx(ctx, "couldn't stop background database state polling: %v", err)
	}
}
