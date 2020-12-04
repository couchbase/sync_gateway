package db

import (
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

type ResyncManager struct {
	Status           ResyncStatus
	LastError        error
	Mutex            sync.Mutex      // Used to lock the Status and when setting / reading LastError
	ResyncTerminator base.AtomicBool // Allows resync operation to be cancelled while in progress
}

type ResyncStatus struct {
	Status        string `json:"status"`
	DocsProcessed int    `json:"docs_processed"`
	DocsChanged   int    `json:"docs_changed"`
	Error         string `json:"last_error,omitempty"`
}

const (
	ResyncStateRunning  = "running"
	ResyncStateStopped  = "stopped"
	ResyncStateStopping = "stopping"
	ResyncStateError    = "stopped on error"
)

func (rm *ResyncManager) GetStatus() *ResyncStatus {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	if rm.LastError != nil {
		rm.Status.Error = rm.LastError.Error()
	}

	return &rm.Status
}

func (rm *ResyncManager) SetRunStatus(newStatus string) {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	rm.Status.Status = newStatus
}

func (rm *ResyncManager) UpdateProcessedChanged(docsProcessed int, docsChanged int) {
	rm.Mutex.Lock()
	rm.Status.DocsProcessed = docsProcessed
	rm.Status.DocsChanged = docsChanged
	rm.Mutex.Unlock()
}

func (rm *ResyncManager) ResetStatus() {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	rm.Status.DocsProcessed = 0
	rm.Status.DocsChanged = 0
	rm.LastError = nil
}

func (rm *ResyncManager) SetError(err error) {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	rm.LastError = err
	rm.Status.Status = ResyncStateError
}
