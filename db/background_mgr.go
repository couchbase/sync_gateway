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
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
)

type BackgroundProcessState string

// These states are used for background tasks
// Running = The process is currently doing work
// Stopped = The process stopped following completion of its task
// Aborting = A user has requested that the process be stopped and therefore will stop shortly (usually after completing its current 'iteration')
// Aborted = The process has stopped either by user request or the process 'crashed' midway through --> Essentially means the process is not running and the previous run had not completed
// Error = The process errored and had to stop
const (
	BackgroundProcessStateRunning  BackgroundProcessState = "running"
	BackgroundProcessStateStopped  BackgroundProcessState = "stopped"
	BackgroundProcessStateAborting BackgroundProcessState = "aborting"
	BackgroundProcessStateAborted  BackgroundProcessState = "aborted"
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
	lastError        error
	terminator       chan struct{}
	terminatorClosed bool
	clusterAware     *ClusterAwareBackgroundManager
	lock             sync.Mutex
	Process          BackgroundManagerProcessI
}

// TODO: See if I can integrate more work into this struct currently we have a lot of work spread out accross this file
// for the cluster work
type ClusterAwareBackgroundManager struct {
	bucket        base.Bucket
	processSuffix string
}

func (b *ClusterAwareBackgroundManager) HeartbeatDocID() string {
	return base.SyncPrefix + ":background_process:heartbeat:" + b.processSuffix
}

func (b *ClusterAwareBackgroundManager) StatusDocID() string {
	return base.SyncPrefix + ":background_process:status:" + b.processSuffix
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
	Init(clusterStatus []byte) error
	Run(options map[string]interface{}, terminator chan struct{}) error
	GetProcessStatus(status BackgroundManagerStatus) ([]byte, error)
	ResetStatus()
}

func (b *BackgroundManager) Start(options map[string]interface{}) error {
	b.terminator = make(chan struct{}, 1)

	err := b.markStart()
	if err != nil {
		return err
	}

	var processClusterStatus []byte
	if b.clusterAware != nil {
		processClusterStatus, _, err = b.clusterAware.bucket.GetRaw(b.clusterAware.StatusDocID())
		if err != nil && !base.IsDocNotFoundError(err) {
			return err
		}
	}

	err = b.Process.Init(processClusterStatus)
	if err != nil {
		return err
	}

	if b.clusterAware != nil {
		go func() {
			ticker := time.NewTicker(time.Second)
			lastStatusCas := uint64(0)
			for {
				select {
				case <-ticker.C:
					status, err := b.getStatusLocal()
					if err != nil {
						// TODO: Handle error
					}

					lastStatusCas, err = b.clusterAware.bucket.WriteCas(b.clusterAware.StatusDocID(), 0, 0, lastStatusCas, status, sgbucket.Raw)
					if err != nil {
						// TODO: Handle error
					}
				case <-b.terminator:
					return
				}
			}
		}()
	}

	go func() {
		err := b.Process.Run(options, b.terminator)
		if err != nil {
			base.Errorf("Error: %v", err)
			b.SetError(err)
		}

		if b.GetRunState() == BackgroundProcessStateAborting {
			b.setRunState(BackgroundProcessStateAborted)
		} else {
			b.setRunState(BackgroundProcessStateStopped)
		}

		if b.clusterAware != nil {
			status, err := b.getStatusLocal()
			if err != nil {
				// TODO: Handle error
			}

			err = b.clusterAware.bucket.SetRaw(b.clusterAware.StatusDocID(), 0, status)
			if err != nil {
				// TODO: Handle error
			}

			// Delete the heartbeat doc to allow another process to run
			// Note: We can ignore the error, worst case is the user has toW wait until the heartbeat doc expires
			_ = b.clusterAware.bucket.Delete(b.clusterAware.HeartbeatDocID())
			b.Terminate()
		}
	}()

	if b.clusterAware != nil {
		status, err := b.getStatusLocal()
		if err != nil {
			// TODO: Handle error
		}

		err = b.clusterAware.bucket.SetRaw(b.clusterAware.StatusDocID(), 0, status)
		if err != nil {
			// TODO: Handle error
		}
	}

	return nil
}

func (b *BackgroundManager) markStart() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	processAlreadyRunningErr := base.HTTPErrorf(http.StatusServiceUnavailable, "Process already running")

	// If we're running in cluster aware 'mode' base the check off of a heartbeat doc
	if b.clusterAware != nil {
		_, err := b.clusterAware.bucket.WriteCas(b.clusterAware.HeartbeatDocID(), 0, 30, 0, []byte("{}"), sgbucket.Raw)
		if base.IsCasMismatch(err) {
			return processAlreadyRunningErr
		}

		go func() {
			ticker := time.NewTicker(1 * time.Second)
			for {
				select {
				case <-ticker.C:
					// TODO: Add a couple retries
					_, err := b.clusterAware.bucket.Touch(b.clusterAware.HeartbeatDocID(), 30)
					if err != nil {
						b.SetError(err)
					}
				case <-b.terminator:
					return
				}
			}
		}()

		b.State = BackgroundProcessStateRunning
		return nil
	}

	// If we're not in cluster aware 'mode' rely on local data
	if b.State == BackgroundProcessStateRunning {
		return processAlreadyRunningErr
	}

	if b.State == BackgroundProcessStateAborting {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Process currently stopping. Wait until stopped to retry")
	}

	b.State = BackgroundProcessStateRunning
	return nil
}

func (b *BackgroundManager) GetStatus() ([]byte, error) {
	if b.clusterAware != nil {
		status, err := b.getStatusFromCluster()
		if err != nil {
			return nil, err
		}

		// If we're running cluster mode, but we have no status it means we haven't run it yet.
		// Get local status which will construct a 'initial' status
		if status == nil {
			return b.getStatusLocal()
		}

		return status, err
	}

	return b.getStatusLocal()
}

func (b *BackgroundManager) getStatusLocal() ([]byte, error) {
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

func (b *BackgroundManager) getStatusFromCluster() ([]byte, error) {
	status, _, err := b.clusterAware.bucket.GetRaw(b.clusterAware.StatusDocID())
	if err != nil {
		if base.IsDocNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}

	var clusterStatus map[string]interface{}
	err = base.JSONUnmarshal(status, &clusterStatus)
	if err != nil {
		return nil, err
	}

	// TODO: Would be nice if we can improve this somehow
	// Work here is required because if the process crashes we'd end up in a state where a GET would return 'running'
	// when in-fact it crashed. We COULD potentially have a different status to represent rather than use aborted.
	// Worst case we should do this once if we have to do this and update the cluster status doc
	if clusterState, ok := clusterStatus["status"].(string); ok && clusterState != string(BackgroundProcessStateStopped) && clusterState != string(BackgroundProcessStateAborted) {
		_, _, err = b.clusterAware.bucket.GetRaw(b.clusterAware.HeartbeatDocID())
		if err != nil {
			if base.IsDocNotFoundError(err) {
				clusterStatus["status"] = BackgroundProcessStateAborted
				status, err = base.JSONMarshal(clusterStatus)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	return status, err
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

	b.Terminate()
	return nil
}

// Terminate stops the process via terminator channel
// Only to be used internally to this file and by tests.
func (b *BackgroundManager) Terminate() {
	if !b.terminatorClosed {
		close(b.terminator)
		b.terminatorClosed = true
	}
}

func (b *BackgroundManager) markStop() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.State == BackgroundProcessStateAborting {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Process already stopping")
	}

	if b.State == BackgroundProcessStateStopped {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Process already stopped")
	}

	b.State = BackgroundProcessStateAborting
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
	b.Terminate()
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
		Process: &ResyncManager{},
	}
}

func (r *ResyncManager) Init(clusterStatus []byte) error {
	r.ResetStatus()
	return nil
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
		Process: &TombstoneCompactionManager{},
	}
}

func (t *TombstoneCompactionManager) Init(clusterStatus []byte) error {
	t.ResetStatus()
	return nil
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
	MarkedAttachments base.AtomicInt
	PurgedAttachments base.AtomicInt
	CompactID         string
	Phase             string
	lock              sync.Mutex
}

var _ BackgroundManagerProcessI = &AttachmentCompactionManager{}

func NewAttachmentCompactionManager(bucket base.Bucket) *BackgroundManager {
	return &BackgroundManager{
		Process: &AttachmentCompactionManager{},
		clusterAware: &ClusterAwareBackgroundManager{
			bucket:        bucket,
			processSuffix: "compact",
		},
	}
}

func (a *AttachmentCompactionManager) Init(clusterStatus []byte) error {
	a.ResetStatus()

	if clusterStatus != nil {
		var attachmentResponseStatus AttachmentManagerResponse
		err := base.JSONUnmarshal(clusterStatus, &attachmentResponseStatus)
		if err != nil {
			return err
		}

		if attachmentResponseStatus.State == BackgroundProcessStateStopped {
			uniqueUUID, err := uuid.NewRandom()
			if err != nil {
				return err
			}

			a.CompactID = uniqueUUID.String()
		} else {
			a.CompactID = attachmentResponseStatus.CompactID
			a.Phase = attachmentResponseStatus.Phase
		}

	} else {
		uniqueUUID, err := uuid.NewRandom()
		if err != nil {
			return err
		}

		a.CompactID = uniqueUUID.String()
	}

	return nil
}

func (a *AttachmentCompactionManager) Run(options map[string]interface{}, terminator chan struct{}) error {
	database := options["database"].(*Database)

	// Need to check the current phase in the event we are resuming - No need to run mark again if we got as far as
	// cleanup last time...
	switch a.Phase {
	case "mark", "":
		a.Phase = "mark"
		_, err := Mark(database, a.CompactID, terminator, &a.MarkedAttachments)
		if err != nil {
			return err
		}
		fallthrough
	case "sweep":
		a.Phase = "sweep"
		_, err := Sweep(database, a.CompactID, terminator, &a.PurgedAttachments)
		if err != nil {
			return err
		}
		fallthrough
	case "cleanup":
		a.Phase = "cleanup"
		err := Cleanup(database, a.CompactID, terminator)
		if err != nil {
			return err
		}
	}

	a.Phase = ""
	return nil
}

type AttachmentManagerResponse struct {
	BackgroundManagerStatus
	MarkedAttachments int64  `json:"marked_attachments"`
	PurgedAttachments int64  `json:"purged_attachments"`
	CompactID         string `json:"compact_id"`
	Phase             string `json:"phase,omitempty"`
}

func (a *AttachmentCompactionManager) GetProcessStatus(status BackgroundManagerStatus) ([]byte, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	retStatus := AttachmentManagerResponse{
		BackgroundManagerStatus: status,
		MarkedAttachments:       a.MarkedAttachments.Value(),
		PurgedAttachments:       a.PurgedAttachments.Value(),
		CompactID:               a.CompactID,
		Phase:                   a.Phase,
	}

	return base.JSONMarshal(retStatus)
}

func (a *AttachmentCompactionManager) ResetStatus() {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.MarkedAttachments.Set(0)
	a.PurgedAttachments.Set(0)
}
