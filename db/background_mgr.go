//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
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

// BackgroundManager this is the over-arching type which is exposed in DatabaseContext
type BackgroundManager struct {
	BackgroundManagerStatus
	lastError  error
	terminator chan struct{}
	lock       sync.Mutex
	Process    BackgroundManagerProcessI
}

// BackgroundManagerStatus simply stores data used in BackgroundManager. This data can also be exposed to users over
// REST. Splitting this out into an additional embedded struct allows easy JSON marshalling
type BackgroundManagerStatus struct {
	State            BackgroundProcessState `json:"status"`
	LastErrorMessage string                 `json:"last_error"`
}

// BackgroundManagerProcessI is an interface satisfied by any of the background processes
// Examples of this: ReSync, Compaction
type BackgroundManagerProcessI interface {
	Run(options map[string]interface{}, terminator chan struct{}) error
	GetProcessStatus(status BackgroundManagerStatus) ([]byte, error)
	ResetStatus()
}

func (b *BackgroundManager) Start(options map[string]interface{}) error {
	err := b.markStart()
	if err != nil {
		return err
	}

	b.resetStatus()
	go func() {
		defer b.setRunState(BackgroundProcessStateStopped)
		err := b.Process.Run(options, b.terminator)
		if err != nil {
			base.Errorf("Error: %v", err)
			b.SetError(err)
		}
	}()
	return nil
}

func (b *BackgroundManager) markStart() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.State == BackgroundProcessStateRunning {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Process already running")
	}

	if b.State == BackgroundProcessStateStopping {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Process currently stopping. Wait until stopped to retry")
	}

	b.State = BackgroundProcessStateRunning

	return nil
}

func (b *BackgroundManager) GetStatus() ([]byte, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	backgroundStatus := b.BackgroundManagerStatus
	if string(backgroundStatus.State) == "" {
		backgroundStatus.State = BackgroundProcessStateStopped
	}

	if b.lastError != nil {
		backgroundStatus.LastErrorMessage = b.lastError.Error()
	}

	return b.Process.GetProcessStatus(backgroundStatus)
}

func (b *BackgroundManager) resetStatus() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.lastError = nil
	b.LastErrorMessage = ""
	b.terminator = make(chan struct{}, 1)
	b.Process.ResetStatus()
}

func (b *BackgroundManager) Stop() error {
	err := b.markStop()
	if err != nil {
		return err
	}

	b.terminator <- struct{}{}
	return nil
}

func (b *BackgroundManager) markStop() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.State == BackgroundProcessStateStopping {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Process already stopping")
	}

	if b.State == BackgroundProcessStateStopped {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Process already stopped")
	}

	b.State = BackgroundProcessStateStopping
	return nil
}

func (b *BackgroundManager) setRunState(state BackgroundProcessState) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.State = state
}

// Currently only test
func (b *BackgroundManager) GetRunState() BackgroundProcessState {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.State
}

func (b *BackgroundManager) SetError(err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.lastError = err
	b.State = BackgroundProcessStateError
}

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
		terminator: make(chan struct{}, 1),
	}
}

func (r *ResyncManager) Run(options map[string]interface{}, terminator chan struct{}) error {
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

func (r *ResyncManager) GetProcessStatus(backgroundManagerStatus BackgroundManagerStatus) ([]byte, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	retStatus := ResyncManagerResponse{
		BackgroundManagerStatus: backgroundManagerStatus,
		DocsChanged:             r.DocsChanged,
		DocsProcessed:           r.DocsProcessed,
	}

	return base.JSONMarshal(retStatus)
}

// =====================================================================
// Tombstone Compaction Implementation of Background Manager Process
// =====================================================================

type TombstoneCompactionManager struct {
	PurgedDocCount int64
}

var _ BackgroundManagerProcessI = &TombstoneCompactionManager{}

func NewTombstoneCompactionManager() *BackgroundManager {
	return &BackgroundManager{
		Process:    &TombstoneCompactionManager{},
		terminator: make(chan struct{}, 1),
	}
}

func (t *TombstoneCompactionManager) Run(options map[string]interface{}, terminator chan struct{}) error {
	database := options["database"].(*Database)

	defer atomic.CompareAndSwapUint32(&database.CompactState, DBCompactRunning, DBCompactNotRunning)
	callback := func(docsPurged *int) {
		atomic.StoreInt64(&t.PurgedDocCount, int64(*docsPurged))
	}

	_, err := database.Compact(true, callback, terminator)
	if err != nil {
		return err
	}

	return nil
}

type TombstoneManagerResponse struct {
	BackgroundManagerStatus
	DocsPurged int64 `json:"docs_purged"`
}

func (t *TombstoneCompactionManager) GetProcessStatus(backgroundManagerStatus BackgroundManagerStatus) ([]byte, error) {
	retStatus := TombstoneManagerResponse{
		BackgroundManagerStatus: backgroundManagerStatus,
		DocsPurged:              atomic.LoadInt64(&t.PurgedDocCount),
	}

	return base.JSONMarshal(retStatus)
}

func (t *TombstoneCompactionManager) ResetStatus() {
	atomic.StoreInt64(&t.PurgedDocCount, 0)
}

// =====================================================================
// Attachment Compaction Implementation of Background Manager Process
// =====================================================================

type AttachmentCompactionManager struct {
	MarkedAttachments int64
	PurgedAttachments int64
	lock              sync.Mutex
}

var _ BackgroundManagerProcessI = &AttachmentCompactionManager{}

func NewAttachmentCompactionManager() *BackgroundManager {
	return &BackgroundManager{
		Process:    &AttachmentCompactionManager{},
		terminator: make(chan struct{}, 1),
	}
}

func (a *AttachmentCompactionManager) Run(options map[string]interface{}, terminator chan struct{}) error {
	database := options["database"].(*Database)

	uniqueUUID, err := uuid.NewRandom()
	if err != nil {
		return err
	}

	_, err = Mark(database, uniqueUUID.String(), terminator, func(markedAttachments *int) {
		atomic.StoreInt64(&a.MarkedAttachments, int64(*markedAttachments))
	})
	if err != nil {
		return err
	}

	_, err = Sweep(database, uniqueUUID.String(), terminator, func(purgedAttachments *int) {
		atomic.StoreInt64(&a.PurgedAttachments, int64(*purgedAttachments))
	})
	if err != nil {
		return err
	}

	return nil
}

type AttachmentManagerResponse struct {
	BackgroundManagerStatus
	MarkedAttachments int64 `json:"marked_attachments"`
	PurgedAttachments int64 `json:"purged_attachments"`
}

func (a *AttachmentCompactionManager) GetProcessStatus(status BackgroundManagerStatus) ([]byte, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	retStatus := AttachmentManagerResponse{
		BackgroundManagerStatus: status,
		MarkedAttachments:       atomic.LoadInt64(&a.MarkedAttachments),
		PurgedAttachments:       atomic.LoadInt64(&a.PurgedAttachments),
	}

	return base.JSONMarshal(retStatus)
}

func (a *AttachmentCompactionManager) ResetStatus() {
	a.lock.Lock()
	defer a.lock.Unlock()

	atomic.StoreInt64(&a.MarkedAttachments, 0)
	atomic.StoreInt64(&a.PurgedAttachments, 0)
}
