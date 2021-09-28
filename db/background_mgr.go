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

// BackgroundManagerI this is an interface satisfied by BackgroundManager
// This is technically not needed, however, it allows easily visibility into exposed methods
type BackgroundManagerI interface {
	Start(options map[string]interface{})
	GetStatus() []byte
	ResetStatus()
	Stop()
}

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
	GetProcessStatus() []base.KVPair
	ResetStatus()
}

var _ BackgroundManagerI = &BackgroundManager{}

func (b *BackgroundManager) Start(options map[string]interface{}) {
	go func() {
		defer b.SetRunState(BackgroundProcessStateStopped)
		err := b.Process.Run(options, b.terminator)
		if err != nil {
			base.Errorf("Error")
			b.SetError(err)
		}
	}()
}

func (b *BackgroundManager) GetStatus() []byte {
	b.lock.Lock()
	status, err := base.JSONMarshal(b.BackgroundManagerStatus)
	if err != nil {
		status = []byte("{}")
	}
	b.lock.Unlock()

	processPairs := b.Process.GetProcessStatus()

	retStatus, err := base.InjectJSONProperties(status, processPairs...)
	if err != nil {
		return nil
	}

	return retStatus
}

func (b *BackgroundManager) ResetStatus() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.LastError = nil
	b.terminator = make(chan struct{}, 1)
}

func (b *BackgroundManager) Stop() {
	// If we're already in the process of stopping don't do anything
	if b.State == BackgroundProcessStateStopping {
		return
	}

	b.SetRunState(BackgroundProcessStateStopping)
	b.terminator <- struct{}{}
}

func (b *BackgroundManager) SetRunState(state BackgroundProcessState) {
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

// ==============================================
// Resync Implementation of Background Manager
// ==============================================

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

func (r *ResyncManager) GetProcessStatus() []base.KVPair {
	r.lock.Lock()
	defer r.lock.Unlock()

	return []base.KVPair{
		{Key: "docs_processed", Val: r.DocsProcessed},
		{Key: "docs_changed", Val: r.DocsChanged},
	}
}

// ResyncManagerResponse - This is not used in production. Only used in testing
type ResyncManagerResponse struct {
	Status        BackgroundProcessState `json:"status"`
	DocsChanged   int                    `json:"docs_changed"`
	DocsProcessed int                    `json:"docs_processed"`
	LastError     error                  `json:"last_error"`
}
