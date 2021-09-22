/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

type BackgroundProcessState string

const (
	BackgroundProcessStateRunning  BackgroundProcessState = "running"
	BackgroundProcessStateStopped  BackgroundProcessState = "stopped"
	BackgroundProcessStateStopping BackgroundProcessState = "stopping"
	BackgroundProcessStateError    BackgroundProcessState = "error"
)

type BackgroundProcessAction string

const (
	BackgroundProcessActionStart BackgroundProcessAction = "start"
	BackgroundProcessActionStop  BackgroundProcessAction = "stop"
)

type BackgroundManager struct {
	State      BackgroundProcessState
	LastError  error
	Terminator bool
	lock       sync.Mutex
}

type BackgroundManagerStatus struct {
	State BackgroundProcessState `json:"state"`
	Error string                 `json:"error"`
}

func (mgr *BackgroundManager) GetRunStatus() BackgroundProcessState {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	return mgr.State
}

func (mgr *BackgroundManager) SetRunStatus(state BackgroundProcessState) {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	mgr.State = state
}

func (mgr *BackgroundManager) GetStatus() BackgroundManagerStatus {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	return mgr._getStatus()
}

func (mgr *BackgroundManager) _getStatus() BackgroundManagerStatus {
	status := BackgroundManagerStatus{
		State: mgr.State,
	}

	if status.State == "" {
		status.State = BackgroundProcessStateStopped
	}

	if mgr.LastError != nil {
		status.Error = mgr.LastError.Error()
	}

	return status
}

func (mgr *BackgroundManager) SetError(err error) {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	mgr.LastError = err
	mgr.State = BackgroundProcessStateError
}

func (mgr *BackgroundManager) ShouldStop() bool {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	if mgr.Terminator {
		mgr.Terminator = false
		return true
	}

	return false
}

func (mgr *BackgroundManager) Stop() {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()

	mgr.State = BackgroundProcessStateStopping
	mgr.Terminator = true
}

// ==============================================
// Resync Implementation of Background Manager
// ==============================================

type ResyncStatusSpecifics struct {
	DocsProcessed int
	DocsChanged   int
}

type ResyncBackgroundManager struct {
	ResyncStatusSpecifics
	BackgroundManager
}

func (r *ResyncBackgroundManager) GetJSONStatus() ([]byte, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	type ResyncStatusResponse struct {
		BackgroundManagerStatus
		DocsProcessed int `json:"docs_processed"`
		DocsChanged   int `json:"docs_changed"`
	}

	return base.JSONMarshal(ResyncStatusResponse{
		DocsProcessed:           r.DocsProcessed,
		DocsChanged:             r.DocsChanged,
		BackgroundManagerStatus: r._getStatus(),
	})
}

func (r *ResyncBackgroundManager) ResetStatus() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.DocsProcessed = 0
	r.DocsChanged = 0
	r.BackgroundManager.LastError = nil
	r.BackgroundManager.Terminator = false
}

func (r *ResyncBackgroundManager) UpdateProcessedChanged(docsProcessed int, docsChanged int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.DocsProcessed = docsProcessed
	r.DocsChanged = docsChanged
}

// =================================================================
// Common Compactor Implementation of Background Manager
// =================================================================

type Compactor struct {
	BackgroundManager
	Process CompactorInterface
}

type CompactorInterface interface {
	GetJSONStatus() ([]byte, error)
	Run(db *Database)
}

// =================================================================
// Tombstone Compaction Implementation of Background Manager
// =================================================================

type TombstoneCompactionStatusSpecifics struct {
	Type                string `json:"type"`
	TombstonesCompacted int    `json:"tombstones_compacted"`
}

type TombstoneCompactionBackgroundManager struct {
	TombstoneCompactionStatusSpecifics
	BackgroundManager
}

func NewTombstoneCompactionBackgroundManager() *Compactor {
	return &Compactor{
		BackgroundManager: BackgroundManager{},
		Process: &TombstoneCompactionBackgroundManager{
			TombstoneCompactionStatusSpecifics: TombstoneCompactionStatusSpecifics{
				Type: "tombstone",
			},
		},
	}
}

func (t *TombstoneCompactionBackgroundManager) GetJSONStatus() ([]byte, error) {
	t.lock.Lock()
	defer t.lock.Unlock()

	type ResyncStatusResponse struct {
		TombstoneCompactionStatusSpecifics
		BackgroundManagerStatus
	}

	return base.JSONMarshal(ResyncStatusResponse{
		TombstoneCompactionStatusSpecifics: t.TombstoneCompactionStatusSpecifics,
		BackgroundManagerStatus:            t.BackgroundManager._getStatus(),
	})
}

func (t *TombstoneCompactionBackgroundManager) ResetStatus() {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.TombstonesCompacted = 0
	t.BackgroundManager.LastError = nil
	t.BackgroundManager.Terminator = false
}

func (t *TombstoneCompactionBackgroundManager) SetTombstonesCompacted(tombstonesCompacted int) {
	t.TombstonesCompacted = tombstonesCompacted
}

func (t *TombstoneCompactionBackgroundManager) Run(db *Database) {
	t.ResetStatus()
	_, err := db.Compact(true, t)
	if err != nil {
		t.SetError(err)
	}
}

// =================================================================
// Attachment Compaction Implementation of Background Manager
// =================================================================

type AttachmentCompactionStatusSpecifics struct {
	Type string `json:"type"`
}

type AttachmentCompactionBackgroundManager struct {
	AttachmentCompactionStatusSpecifics
	BackgroundManager
}

func NewAttachmentCompactionBackgroundManager() *Compactor {
	return &Compactor{
		BackgroundManager: BackgroundManager{},
		Process: &AttachmentCompactionBackgroundManager{
			AttachmentCompactionStatusSpecifics: AttachmentCompactionStatusSpecifics{
				Type: "attachment",
			},
		},
	}
}

func (a *AttachmentCompactionBackgroundManager) GetJSONStatus() ([]byte, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	type ResyncStatusResponse struct {
		AttachmentCompactionStatusSpecifics
		BackgroundManagerStatus
	}

	return base.JSONMarshal(ResyncStatusResponse{
		AttachmentCompactionStatusSpecifics: a.AttachmentCompactionStatusSpecifics,
		BackgroundManagerStatus:             a.BackgroundManager._getStatus(),
	})
}

func (a *AttachmentCompactionBackgroundManager) ResetStatus() {

}

func (a *AttachmentCompactionBackgroundManager) Run(db *Database) {
	panic("implement me")
}
