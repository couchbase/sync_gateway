package db

import (
	"sync"
)

type ResyncManager struct {
	Status     ResyncStatus
	LastError  error
	Terminator bool       // Allows resync operation to be cancelled while in progress
	Mutex      sync.Mutex // Used to lock the Status, LastError and Terminator
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

	return rm._getStatus()
}

func (rm *ResyncManager) _getStatus() *ResyncStatus {
	retStatus := ResyncStatus{
		Status:        rm.Status.Status,
		DocsChanged:   rm.Status.DocsChanged,
		DocsProcessed: rm.Status.DocsProcessed,
	}

	if rm.LastError != nil {
		retStatus.Error = rm.LastError.Error()
	}

	return &retStatus
}

func (rm *ResyncManager) SetRunStatus(newStatus string) {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	rm.Status.Status = newStatus
}

func (rm *ResyncManager) UpdateProcessedChanged(docsProcessed int, docsChanged int) {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	rm.Status.DocsProcessed = docsProcessed
	rm.Status.DocsChanged = docsChanged
}

func (rm *ResyncManager) ResetStatus() {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	rm.Status.DocsProcessed = 0
	rm.Status.DocsChanged = 0
	rm.LastError = nil
	rm.Terminator = false
}

func (rm *ResyncManager) SetError(err error) {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	rm.LastError = err
	rm.Status.Status = ResyncStateError
}

func (rm *ResyncManager) ShouldStop() bool {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	if rm.Terminator {
		rm.Terminator = false
		return true
	}

	return false
}

func (rm *ResyncManager) Stop() *ResyncStatus {
	rm.Mutex.Lock()
	defer rm.Mutex.Unlock()

	rm.Status.Status = ResyncStateStopped
	rm.Terminator = true
	return rm._getStatus()
}
