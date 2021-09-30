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
	terminator chan struct{}
	lock       sync.Mutex
	Process    BackgroundManagerProcessI
}

// BackgroundManagerStatus simply stores data used in BackgroundManager. This data can also be exposed to users over
// REST. Splitting this out into an additional embedded struct allows easy JSON marshalling
type BackgroundManagerStatus struct {
	State     BackgroundProcessState `json:"status"`
	LastError error                  `json:"last_error"`
}

// BackgroundManagerProcessI is an interface satisfied by any of the background processes
// Examples of this: ReSync, Compaction
type BackgroundManagerProcessI interface {
	Run(options map[string]interface{}, terminator chan struct{}) error
	GetProcessStatus(status BackgroundManagerStatus) ([]byte, error)
	ResetStatus()
}

func (b *BackgroundManager) Start(options map[string]interface{}) {
	b.resetStatus()
	b.setRunState(BackgroundProcessStateRunning)
	go func() {
		defer b.setRunState(BackgroundProcessStateStopped)
		err := b.Process.Run(options, b.terminator)
		if err != nil {
			base.Errorf("Error")
			b.SetError(err)
		}
	}()
}

func (b *BackgroundManager) GetStatus() ([]byte, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.Process.GetProcessStatus(b.BackgroundManagerStatus)
}

func (b *BackgroundManager) resetStatus() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.LastError = nil
	b.terminator = make(chan struct{}, 1)
	b.Process.ResetStatus()
}

func (b *BackgroundManager) Stop() {
	// If we're already in the process of stopping don't do anything
	if b.State == BackgroundProcessStateStopping {
		return
	}

	b.setRunState(BackgroundProcessStateStopping)
	b.terminator <- struct{}{}
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

	b.LastError = err
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
	callback := func(docsProcessed int, docsChanged int) {
		r.lock.Lock()
		defer r.lock.Unlock()
		r.DocsProcessed = docsProcessed
		r.DocsChanged = docsChanged
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
	DocsChanged   int `json:"docs_changes"`
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
