//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	pkgerrors "github.com/pkg/errors"
)

type BackgroundProcessState string

// These states are used for background tasks
// Running = The process is currently doing work
// Completed = The process stopped following completion of its task
// Stopping = A user has requested that the process be stopped and therefore will stop shortly (usually after completing its current 'iteration')
// Stopped = The process has stopped either by user request or the process 'crashed' midway through --> Essentially means the process is not running and the previous run had not completed
// Error = The process errored and had to stop
const (
	BackgroundProcessStateRunning   BackgroundProcessState = "running"
	BackgroundProcessStateCompleted BackgroundProcessState = "completed"
	BackgroundProcessStateStopping  BackgroundProcessState = "stopping"
	BackgroundProcessStateStopped   BackgroundProcessState = "stopped"
	BackgroundProcessStateError     BackgroundProcessState = "error"
)

// errBackgroundManagerAlreadyStopping is returned when a Start or Stop is called while the process is in the Stopping
// state.
var errBackgroundManagerStatusAlreadyStopping = base.HTTPErrorf(http.StatusServiceUnavailable, "Process currently stopping. Wait until stopped to retry")

// errBackgroundManagerStatusNotRunning is returned when the bucket status is not running but the local status is.
var errBackgroundManagerStatusNotRunning = errors.New("status in bucket is not running, but local status is, avoiding overwriting local status to bucket")

// errBackgroundManagerProcessAlreadyRunning is returned when an action to Start a process occurs but it is already running.
var errBackgroundManagerProcessAlreadyRunning = base.HTTPErrorf(http.StatusServiceUnavailable, "Process already running")

// errBackgroundManagerProcessAlreadyStopped is returned when an action to Stop a process occurs but it is already stopped.
var errBackgroundManagerProcessAlreadyStopped = base.HTTPErrorf(http.StatusServiceUnavailable, "Process already stopped")

// backgroundManagerUpdateClusterStatusMode controls whether updateMultiNodeClusterAwareStatus enforces
// consistency between the local and cluster states.
type backgroundManagerUpdateClusterStatusMode int

const (
	// backgroundManagerReportInconsistentStatus returns errBackgroundManagerStatusNotRunning
	// when the cluster doc shows a terminal state but the local state is running. Used by the polling loop to
	// detect that another node has stopped or completed the process.
	backgroundManagerReportInconsistentStatus backgroundManagerUpdateClusterStatusMode = iota
	// backgroundManagerOverwriteInconsistentStatus writes the current local status to the
	// cluster doc unconditionally. Used when starting or finalising a run where we own the state transition.
	backgroundManagerOverwriteInconsistentStatus
)

type BackgroundProcessAction string

const (
	BackgroundProcessActionStart BackgroundProcessAction = "start"
	BackgroundProcessActionStop  BackgroundProcessAction = "stop"
)

// BackgroundManager this is the over-arching type which is exposed in DatabaseContext
type BackgroundManager struct {
	status                                 BackgroundManagerStatus
	statusLock                             sync.RWMutex
	name                                   string
	lastError                              error
	terminator                             *base.SafeTerminator
	backgroundManagerStatusUpdateWaitGroup sync.WaitGroup
	clusterAwareOptions                    *ClusterAwareBackgroundManagerOptions
	lock                                   sync.Mutex
	Process                                BackgroundManagerProcessI
	// updateDatabaseState, when non-nil, is called from UpdateStatusClusterAware and from Resume
	// (when the cluster is not running) to mirror the local run state into the DatabaseState document.
	// running is true when the process is locally active, false otherwise.
	updateDatabaseState func(ctx context.Context, running bool) error
}

const (
	BackgroundManagerHeartbeatExpirySecs      = 30
	BackgroundManagerHeartbeatIntervalSecs    = 1
	BackgroundManagerStatusUpdateIntervalSecs = 1
)

type ClusterAwareBackgroundManagerOptions struct {
	metadataStore base.DataStore
	metaKeys      *base.MetadataKeys
	processSuffix string
	multiNode     bool // If true, the background manager is expected to run on all nodes of a Sync Gateway cluster.

	lastSuccessfulHeartbeatUnix base.AtomicInt
}

func (b *ClusterAwareBackgroundManagerOptions) HeartbeatDocID() string {
	return b.metaKeys.BackgroundProcessHeartbeatPrefix(b.processSuffix)
}

func (b *ClusterAwareBackgroundManagerOptions) StatusDocID() string {
	return b.metaKeys.BackgroundProcessStatusPrefix(b.processSuffix)
}

// BackgroundManagerStatus simply stores data used in BackgroundManager. This data can also be exposed to users over
// REST. Splitting this out into an additional embedded struct allows easy JSON marshalling
type BackgroundManagerStatus struct {
	State            BackgroundProcessState `json:"status"`
	StartTime        time.Time              `json:"start_time"`
	LastErrorMessage string                 `json:"last_error"`
}

// BackgroundManagerProcessI is an interface satisfied by any of the background processes
// Examples of this: ReSync, Compaction, Attachment Migration
type BackgroundManagerProcessI interface {
	// Init is called before Run for setup purposes. If Init errors, Run will not happen.
	Init(ctx context.Context, options map[string]any, clusterStatus []byte) error
	// Run implements all of the work of the process.
	Run(ctx context.Context, options map[string]any, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error
	// GetProcessStatus accepts the current BackgroundManagerStatus and the previousStatus that was serialized. previousStatus is
	// only populated when updating cluster status, it may be nil in some circumstances. This is only used for multi
	// node background managers.
	GetProcessStatus(status BackgroundManagerStatus, previousStatus []byte) (statusOut []byte, meta []byte, err error)
	// SetProcessStatus updates the newStatus with the latest serialized status. This includes the last known status
	// from GetProcessStatus.
	SetProcessStatus(ctx context.Context, previousStatus []byte, newStatus []byte)
	// ResetStatus is called when the process is started to reset any internal status of the process and signalling
	// that all stats should be reset to 0.
	ResetStatus()
}

type updateStatusCallbackFunc func(ctx context.Context) error

// GetName returns name of the background manager
func (b *BackgroundManager) GetName() string {
	return b.name
}

// callUpdateDatabaseState invokes updateDatabaseState if it is set, logging any error.
func (b *BackgroundManager) callUpdateDatabaseState(ctx context.Context, running bool) {
	if b.updateDatabaseState == nil {
		return
	}
	if err := b.updateDatabaseState(ctx, running); err != nil {
		base.WarnfCtx(ctx, "failed to update database state: %v", err)
	}
}

// Resume joins an already-running multi-node background process on this node using the options stored in
// the status document.  It only starts the local process when the cluster state is
// BackgroundProcessStateRunning; any other state (including no status document) returns
// errBackgroundManagerStatusNotRunning.  Only supported for multi-node background managers.
func (b *BackgroundManager) Resume(ctx context.Context) error {
	if b.mode() != backgroundManagerModeMultiNode {
		return fmt.Errorf("Resume is only supported for multi-node background managers (process %q)", b.name)
	}

	docID := b.clusterAwareOptions.StatusDocID()
	raw, _, err := b.clusterAwareOptions.metadataStore.GetRaw(ctx, docID)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			b.callUpdateDatabaseState(ctx, false)
			return errBackgroundManagerStatusNotRunning
		}
		return fmt.Errorf("failed to read status doc for background process %q: %w", b.name, err)
	}

	previousStatus, err := unmarshalBackgroundManagerStatus(raw)
	if err != nil {
		return fmt.Errorf("failed to unmarshal status doc %q for background process %q: %w", docID, b.name, err)
	}
	if previousStatus.State != BackgroundProcessStateRunning {
		b.callUpdateDatabaseState(ctx, false)
		return nil
	}

	var doc struct {
		Meta struct {
			Options map[string]any `json:"options"`
		} `json:"meta"`
	}
	if err := base.JSONUnmarshal(raw, &doc); err != nil {
		return fmt.Errorf("failed to unmarshal meta for background process %q: %w", b.name, err)
	}

	return b.start(ctx, doc.Meta.Options, raw)
}

func (b *BackgroundManager) Start(ctx context.Context, options map[string]any) error {
	var processClusterStatus []byte
	if b.mode() != backgroundManagerModeLocal {
		var err error
		processClusterStatus, _, err = b.clusterAwareOptions.metadataStore.GetRaw(ctx, b.clusterAwareOptions.StatusDocID())
		if err != nil && !base.IsDocNotFoundError(err) {
			return pkgerrors.Wrap(err, "Failed to get current process status")
		}
	}
	return b.start(ctx, options, processClusterStatus)
}

func (b *BackgroundManager) start(ctx context.Context, options map[string]any, processClusterStatus []byte) error {
	if b.mode() != backgroundManagerModeMultiNode && b.updateDatabaseState != nil {
		return fmt.Errorf("updateDatabaseState should only be set for multi-node background managers")
	}
	var previousStatus BackgroundManagerStatus
	if processClusterStatus != nil {
		var err error
		previousStatus, err = unmarshalBackgroundManagerStatus(processClusterStatus)
		if err != nil {
			base.InfofCtx(ctx, base.KeyAll, "Could not unmarshal the cluster status before calling BackgroundManager.Run %v", err)
		}
	}

	err := b.markStart(ctx, previousStatus)
	if err != nil {
		if b.mode() == backgroundManagerModeMultiNode && errors.Is(err, errBackgroundManagerProcessAlreadyRunning) {
			return nil
		}
		return err
	}

	b.resetStatus()
	b.setStartTime(time.Now().UTC())

	// If we're resuming a cluster-aware process, try to reuse the previous start time.
	if previousStatus.State == BackgroundProcessStateRunning && !previousStatus.StartTime.IsZero() {
		b.setStartTime(previousStatus.StartTime)
	}

	err = b.Process.Init(ctx, options, processClusterStatus)
	if err != nil {
		return err
	}

	if b.mode() == backgroundManagerModeSingleNode {
		b.backgroundManagerStatusUpdateWaitGroup.Add(1)
		go func(terminator *base.SafeTerminator) {
			defer b.backgroundManagerStatusUpdateWaitGroup.Done()
			ticker := time.NewTicker(BackgroundManagerStatusUpdateIntervalSecs * time.Second)
			for {
				select {
				case <-ticker.C:
					err := b.UpdateSingleNodeClusterAwareStatus(ctx)
					if err != nil {
						base.WarnfCtx(ctx, "Failed to update background manager status: %v", err)
					}
				case <-terminator.Done():
					ticker.Stop()
					return
				}
			}
		}(b.terminator)

	}
	if b.mode() == backgroundManagerModeMultiNode {
		b.backgroundManagerStatusUpdateWaitGroup.Go(func() {
			b.startPollingMultiNodeStatus(ctx, b.terminator)
		})
	}
	go func() {
		err := b.Process.Run(ctx, options, b.UpdateStatusClusterAware, b.terminator)
		if err != nil {
			base.ErrorfCtx(ctx, "Error: %v", err)
			b.SetError(err)
		}

		b.Terminate()

		b.statusLock.Lock()
		if b.status.State == BackgroundProcessStateStopping {
			b.status.State = BackgroundProcessStateStopped
		} else if b.status.State != BackgroundProcessStateError {
			b.status.State = BackgroundProcessStateCompleted
		}
		b.statusLock.Unlock()

		// Once our background process run has completed we should update the completed status and delete the heartbeat
		// doc
		if b.mode() != backgroundManagerModeLocal {
			err := b.UpdateStatusClusterAware(ctx)
			if err != nil {
				base.WarnfCtx(ctx, "Failed to update background manager status: %v", err)
			}

			// Delete the heartbeat doc to allow another process to run
			// Note: We can ignore the error, worst case is the user has to wait until the heartbeat doc expires
			_ = b.clusterAwareOptions.metadataStore.Delete(ctx, b.clusterAwareOptions.HeartbeatDocID())
		}
	}()

	if b.mode() != backgroundManagerModeLocal {
		var err error
		if b.mode() == backgroundManagerModeMultiNode {
			// when starting a background process, allow overwriting any previous state (completed, error, stopped, stopping)
			err = b.updateMultiNodeClusterAwareStatus(ctx, backgroundManagerOverwriteInconsistentStatus)
		} else {
			err = b.UpdateSingleNodeClusterAwareStatus(ctx)
		}
		if err != nil {
			base.ErrorfCtx(ctx, "Failed to update background manager status: %v", err)
			return err
		}
	}

	return nil
}

// markStart changes the local status to started.
//
// If local or single node process and the bucket status is running, return errBackgroundManagerProcessAlreadyRunning.
// If the status is stopping, return errBackgroundManagerStatusAlreadyStopping
// that should be stopping or is already stopped.
func (b *BackgroundManager) markStart(ctx context.Context, previousStatus BackgroundManagerStatus) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	// If we're running in cluster aware 'mode' base the check off of a heartbeat doc
	if b.mode() == backgroundManagerModeSingleNode {
		_, err := b.clusterAwareOptions.metadataStore.WriteCas(ctx, b.clusterAwareOptions.HeartbeatDocID(), BackgroundManagerHeartbeatExpirySecs, 0, []byte("{}"), sgbucket.Raw)
		if base.IsCasMismatch(err) {
			// Check if markStop has been called but not yet processed
			var status HeartbeatDoc
			_, err := b.clusterAwareOptions.metadataStore.Get(ctx, b.clusterAwareOptions.HeartbeatDocID(), &status)
			if err == nil && status.ShouldStop {
				return base.HTTPErrorf(http.StatusServiceUnavailable, "Process stop still in progress - please wait before restarting")
			}
			return errBackgroundManagerProcessAlreadyRunning
		}

		// Now we know that we're the only running process we should instantiate these values
		// We need to instantiate these before we setup the below goroutine as it relies upon the terminator
		b.terminator = base.NewSafeTerminator()

		go func(terminator *base.SafeTerminator) {
			ticker := time.NewTicker(BackgroundManagerHeartbeatIntervalSecs * time.Second)
			for {
				select {
				case <-ticker.C:
					err = b.UpdateHeartbeatDocClusterAware(ctx)
					if err != nil {
						base.ErrorfCtx(ctx, "Failed to update expiry on heartbeat doc: %v", err)
						b.SetError(err)
					}
				case <-terminator.Done():
					ticker.Stop()
					return
				}
			}
		}(b.terminator)

		b.setRunState(BackgroundProcessStateRunning)
		return nil
	}

	if b.mode() == backgroundManagerModeMultiNode {
		if previousStatus.State == BackgroundProcessStateStopping {
			return errBackgroundManagerStatusAlreadyStopping
		}
	}

	if b.GetRunState() == BackgroundProcessStateRunning {
		return errBackgroundManagerProcessAlreadyRunning
	}

	if b.GetRunState() == BackgroundProcessStateStopping {
		return errBackgroundManagerStatusAlreadyStopping
	}

	// Now we know that we're the only running process we should instantiate these values
	b.terminator = base.NewSafeTerminator()

	b.setRunState(BackgroundProcessStateRunning)
	return nil
}

// getClusterStatusState gets the current background process state of the cluster.
func (b *BackgroundManager) getClusterStatusState(ctx context.Context) (BackgroundProcessState, error) {
	docID := b.clusterAwareOptions.StatusDocID()
	statusRaw, _, err := b.clusterAwareOptions.metadataStore.GetSubDocRaw(ctx, docID, "status")
	if err != nil {
		return "", err
	}
	state, err := unmarshalBackgroundProcessState(statusRaw)
	if err != nil {
		return "", fmt.Errorf("could not get background manager state from cluster status doc %q: %w", docID, err)
	}
	return state, nil

}

// unmarshalBackgroundProcessState returns the BackgroundProcessState from raw bytes of the status document.
func unmarshalBackgroundProcessState(statusRaw []byte) (BackgroundProcessState, error) {
	var clusterStatus struct {
		Status BackgroundProcessState `json:"status"`
	}
	if err := base.JSONUnmarshal(statusRaw, &clusterStatus); err != nil {
		return "", err
	}
	return clusterStatus.Status, nil
}

// unmarshalBackgroundManagerStatus returns the BackgroundManagerStatus from raw bytes of the status document.
func unmarshalBackgroundManagerStatus(statusRaw []byte) (BackgroundManagerStatus, error) {
	var clusterStatus struct {
		Status BackgroundManagerStatus `json:"status"`
	}
	if err := base.JSONUnmarshal(statusRaw, &clusterStatus); err != nil {
		return BackgroundManagerStatus{}, err
	}
	return clusterStatus.Status, nil
}

func (b *BackgroundManager) GetStatus(ctx context.Context) ([]byte, error) {
	if b.mode() != backgroundManagerModeLocal {
		status, err := b.getStatusFromCluster(ctx)
		if err != nil {
			return nil, err
		}

		// If we're running cluster mode, but we have no status it means we haven't run it yet.
		// Get local status which will construct a 'initial' status
		if status == nil {
			status, _, err := b.getStatusLocalWithoutPrevious()
			return status, err
		}

		return status, err
	}

	status, _, err := b.getStatusLocalWithoutPrevious()
	return status, err
}

func (b *BackgroundManager) getStatusLocalWithoutPrevious() ([]byte, []byte, error) {
	return b.getStatusWithPrevious(nil)

}

func (b *BackgroundManager) getStatusWithPrevious(previous []byte) ([]byte, []byte, error) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()

	backgroundStatus := b.status
	if string(backgroundStatus.State) == "" {
		backgroundStatus.State = BackgroundProcessStateCompleted
	}

	if b.lastError != nil {
		backgroundStatus.LastErrorMessage = b.lastError.Error()
	}

	return b.Process.GetProcessStatus(backgroundStatus, previous)
}

func (b *BackgroundManager) getStatusFromCluster(ctx context.Context) ([]byte, error) {
	status, statusCas, err := b.clusterAwareOptions.metadataStore.GetSubDocRaw(ctx, b.clusterAwareOptions.StatusDocID(), "status")
	if err != nil {
		if base.IsDocNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}

	var clusterStatus map[string]any
	err = base.JSONUnmarshal(status, &clusterStatus)
	if err != nil {
		return nil, err
	}

	// In multi node mode, there is no heartbeat document. Each time a node comes online, it is expected to resume the
	// background process.
	if b.mode() == backgroundManagerModeMultiNode {
		return status, nil
	}

	// Work here is required because if the process crashes we'd end up in a state where a GET would return 'running'
	// when in-fact it crashed.
	// Worst case we should do this once if we have to do this and update the cluster status doc
	if clusterState, ok := clusterStatus["status"].(string); ok &&
		clusterState != string(BackgroundProcessStateCompleted) &&
		clusterState != string(BackgroundProcessStateStopped) &&
		clusterState != string(BackgroundProcessStateError) {
		_, _, err = b.clusterAwareOptions.metadataStore.GetRaw(ctx, b.clusterAwareOptions.HeartbeatDocID())
		if err != nil {
			if base.IsDocNotFoundError(err) {
				if clusterState == string(BackgroundProcessStateRunning) {
					status, _, err = b.getStatusLocalWithoutPrevious()
					if err != nil {
						return nil, err
					}
				} else {
					clusterStatus["status"] = BackgroundProcessStateStopped
					status, err = base.JSONMarshal(clusterStatus)
					if err != nil {
						return nil, err
					}
				}

				// In the event there is a crash and need to update the status we should attempt to update the doc to
				// avoid this unmarshal / marshal work from having to happen again, next time GET is called.
				// If there is an error we can just ignore it as worst case we run this unmarshal / marshal again on
				// next request
				_, err = b.clusterAwareOptions.metadataStore.WriteSubDoc(ctx, b.clusterAwareOptions.StatusDocID(), "status", statusCas, status)
				if err != nil {
					status, _, err = b.clusterAwareOptions.metadataStore.GetSubDocRaw(ctx, b.clusterAwareOptions.StatusDocID(), "status")
					if err != nil {
						return nil, err
					}
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
	b.clearLastErrorMessage()
	b.Process.ResetStatus()
}

// setLastErrorMessage sets the last error message
func (b *BackgroundManager) clearLastErrorMessage() {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	b.status.LastErrorMessage = ""
}

// Stop triggers a Stop of the background process. This will transition the state to BackgroundProcessStateStopping and
// return from this function.
//
// This will return an error if the status is not in a running state, as already stopped or stopping.
func (b *BackgroundManager) Stop(ctx context.Context) error {
	if err := b.markStop(ctx); err != nil {
		if errors.Is(err, errBackgroundManagerProcessAlreadyStopped) || errors.Is(err, errBackgroundManagerStatusAlreadyStopping) {
			return nil
		}
		return err
	}
	b.stopProcess(ctx)
	return nil
}

// Terminate stops the process via terminator channel
// Only to be used internally to this file and by tests.
func (b *BackgroundManager) Terminate() {
	b.terminator.Close()
	b.backgroundManagerStatusUpdateWaitGroup.Wait()
}

func (b *BackgroundManager) markStop(ctx context.Context) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	currentState := b.GetRunState()
	if b.mode() == backgroundManagerModeSingleNode {
		_, _, err := b.clusterAwareOptions.metadataStore.GetRaw(ctx, b.clusterAwareOptions.HeartbeatDocID())
		if err != nil {
			if base.IsDocNotFoundError(err) {
				return errBackgroundManagerProcessAlreadyStopped
			}
			return base.HTTPErrorf(http.StatusInternalServerError, "Unable to verify whether a process is running: %v", err)
		}

		err = b.clusterAwareOptions.metadataStore.Set(ctx, b.clusterAwareOptions.HeartbeatDocID(), BackgroundManagerHeartbeatExpirySecs, nil, HeartbeatDoc{ShouldStop: true})
		if err != nil {
			return base.HTTPErrorf(http.StatusInternalServerError, "Failed to mark process as stopping: %v", err)
		}

		// If this is the node running the service
		b.compareAndSwapRunState(BackgroundProcessStateRunning, BackgroundProcessStateStopping)

		return nil
	}

	if currentState == BackgroundProcessStateStopping {
		return errBackgroundManagerStatusAlreadyStopping
	}

	// Treat the initial zero state ("") the same as a terminal state: the process was never
	// started on this node, so there is nothing to stop.
	if currentState == "" || slices.Contains([]BackgroundProcessState{BackgroundProcessStateCompleted, BackgroundProcessStateStopped, BackgroundProcessStateError}, currentState) {
		return errBackgroundManagerProcessAlreadyStopped
	}
	b.setRunState(BackgroundProcessStateStopping)

	return nil
}

// GetRunState returns the in memory state of the background process. This may different from the serialized bucket state.
func (b *BackgroundManager) GetRunState() BackgroundProcessState {
	b.statusLock.RLock()
	defer b.statusLock.RUnlock()
	return b.status.State
}

// setRunState sets the in memory state of the background process. This does not updated the serialized bucket state.
func (b *BackgroundManager) setRunState(state BackgroundProcessState) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	b.status.State = state
}

// getStartTime returns the current start time of the background process from an in memory value. This may be different from the seraialized start time. If no start time is present, returns nil time.Time.
func (b *BackgroundManager) getStartTime() time.Time {
	b.statusLock.RLock()
	defer b.statusLock.RUnlock()
	return b.status.StartTime
}

// setStartTime sets the start time of the background process to an in memory value. This does not update the serialized bucket state.
func (b *BackgroundManager) setStartTime(startTime time.Time) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	b.status.StartTime = startTime
}

// SetError sets the last known error, transitions the state to BackgroundManagerStateError and terminates the process.
func (b *BackgroundManager) SetError(err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.lastError = err
	b.setRunState(BackgroundProcessStateError)
	b.Terminate()
}

// UpdateStatusClusterAware reads the local status and writes that value to the bucket. This will update the "status" and "meta" keys of the status document.
// In multi-node mode it uses FailInconsistent: if the cluster doc shows a terminal state while the local
// state is running, it returns errBackgroundManagerStatusNotRunning. This is the callback passed to Run.
func (b *BackgroundManager) UpdateStatusClusterAware(ctx context.Context) error {
	switch b.mode() {
	case backgroundManagerModeSingleNode:
		return b.UpdateSingleNodeClusterAwareStatus(ctx)
	case backgroundManagerModeMultiNode:
		return b.updateMultiNodeClusterAwareStatus(ctx, backgroundManagerReportInconsistentStatus)
	case backgroundManagerModeLocal:
		return nil
	default:
		return fmt.Errorf("unknown background manager mode: %v", b.mode())
	}
}

// UpdateSingleNodeClusterAwareStatus gets the current local status from the running process and updates the status document in
// the bucket. Implements a retry. Used for Cluster Aware operations
func (b *BackgroundManager) UpdateSingleNodeClusterAwareStatus(ctx context.Context) error {
	if b.clusterAwareOptions == nil {
		return nil
	}
	err, _ := base.RetryLoop(ctx, "UpdateStatusClusterAware", func() (shouldRetry bool, err error, value any) {
		status, metadata, err := b.getStatusLocalWithoutPrevious()
		if err != nil {
			return true, err, nil
		}

		_, err = b.clusterAwareOptions.metadataStore.WriteSubDoc(ctx, b.clusterAwareOptions.StatusDocID(), "status", 0, status)
		if err != nil {
			return true, err, nil
		}

		_, err = b.clusterAwareOptions.metadataStore.WriteSubDoc(ctx, b.clusterAwareOptions.StatusDocID(), "meta", 0, metadata)
		if err != nil {
			return true, err, nil
		}

		return false, nil, nil
	}, base.CreateSleeperFunc(5, 100))
	return err
}

// updateMultiNodeClusterAwareStatus updates the cluster status document with the current local status.
// When mode is backgroundManagerReportInconsistentStatus and the cluster doc shows a terminal
// state while the local state is running, it returns errBackgroundManagerStatusNotRunning without
// writing. When mode is backgroundManagerOverwriteInconsistentStatus the write always proceeds.
func (b *BackgroundManager) updateMultiNodeClusterAwareStatus(ctx context.Context, mode backgroundManagerUpdateClusterStatusMode) error {
	docID := b.clusterAwareOptions.StatusDocID()
	var previousStatus []byte
	var newStatus []byte
	_, err := b.clusterAwareOptions.metadataStore.Update(ctx, docID, 0, func(current []byte) ([]byte, *uint32, bool, error) {
		previousStatus = current
		status, metadata, err := b.getStatusWithPrevious(current)
		if err != nil {
			return nil, nil, false, err
		}
		output := make(map[string]json.RawMessage, 2)
		if current != nil {
			if err := base.JSONUnmarshal(current, &output); err != nil {
				return nil, nil, false, fmt.Errorf("Could not unmarshal doc(%q) within updateClusterAwareStatus: %w", docID, err)
			}
			// If the local status is running, but another node stopped or errored, do not serialize status and report
			// not running so that caller can terminate the background manager on this node
			if mode == backgroundManagerReportInconsistentStatus {
				if status, ok := output["status"]; ok {
					bucketState, err := unmarshalBackgroundProcessState(status)
					if err != nil {
						return nil, nil, false, err
					}
					if slices.Contains([]BackgroundProcessState{BackgroundProcessStateCompleted, BackgroundProcessStateStopping, BackgroundProcessStateStopped, BackgroundProcessStateError}, bucketState) && b.GetRunState() == BackgroundProcessStateRunning {
						return nil, nil, false, errBackgroundManagerStatusNotRunning
					}
				}
			}
		}
		output["status"] = json.RawMessage(status)
		output["meta"] = json.RawMessage(metadata)

		outputBytes, err := base.JSONMarshal(output)
		if err != nil {
			return nil, nil, false, fmt.Errorf("could not marshal updated status doc %q: %w", docID, err)
		}
		newStatus = status
		return outputBytes, nil, false, nil
	})
	if err != nil {
		return err
	}
	b.Process.SetProcessStatus(ctx, previousStatus, newStatus)
	b.callUpdateDatabaseState(ctx, b.GetRunState() == BackgroundProcessStateRunning)
	return nil
}

type HeartbeatDoc struct {
	ShouldStop bool `json:"should_stop"`
}

// UpdateHeartbeatDocClusterAware simply performs a touch operation on the heartbeat document to update its expiry.
// Implements a retry. Used for Cluster Aware operations
func (b *BackgroundManager) UpdateHeartbeatDocClusterAware(ctx context.Context) error {
	statusRaw, _, err := b.clusterAwareOptions.metadataStore.GetAndTouchRaw(ctx, b.clusterAwareOptions.HeartbeatDocID(), BackgroundManagerHeartbeatExpirySecs)
	if err != nil {
		// If we get an error but the error is doc not found and terminator closed it means we have terminated the
		// goroutine which intermittently runs this but this snuck in before it was stopped. This may result in the doc
		// being deleted before this runs. We can ignore that error is that is the case.
		if base.IsDocNotFoundError(err) && b.terminator.IsClosed() {
			return nil
		}

		// If we've hit an error, and we haven't had a successful heartbeat in just under its TTL then we need to quit
		// out. If we fail to write heartbeat for this time we can no longer ensure that this would be the only process
		// running and another could end up starting.
		if time.Now().Sub(time.Unix(b.clusterAwareOptions.lastSuccessfulHeartbeatUnix.Value(), 0)) > (BackgroundManagerHeartbeatExpirySecs - BackgroundManagerHeartbeatIntervalSecs) {
			return err
		}
		return nil
	}

	var status HeartbeatDoc
	err = base.JSONUnmarshal(statusRaw, &status)
	if err != nil {
		return err
	}

	if status.ShouldStop {
		err = b.Stop(ctx)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to stop process %q: %v", b.clusterAwareOptions.processSuffix, err)
		}
	}

	b.clusterAwareOptions.lastSuccessfulHeartbeatUnix.Set(time.Now().Unix())
	return nil
}

// startPollingMultiNodeStatus starts a loop which polls the status document for changes. If the status document
// indicates that the process should stop, then this will trigger a stop of the local process. This is used for
// multi-node cluster aware background managers where we want all nodes to stop if any node triggers a stop.
func (b *BackgroundManager) startPollingMultiNodeStatus(ctx context.Context, terminator *base.SafeTerminator) {
	ticker := time.NewTicker(BackgroundManagerStatusUpdateIntervalSecs * time.Second)
	for {
		select {
		case <-ticker.C:
			err := b.updateMultiNodeClusterAwareStatus(ctx, backgroundManagerReportInconsistentStatus)
			if err != nil {
				if errors.Is(err, errBackgroundManagerStatusNotRunning) {
					b.stopProcess(ctx)
					return
				} else {
					base.DebugfCtx(ctx, base.KeyAll, "Failed to update multi node cluster aware status: %v, will retry", err)
				}
			}
		case <-terminator.Done():
			ticker.Stop()
			return
		}
	}
}

// stopProcess terminates the locally running process.
func (b *BackgroundManager) stopProcess(ctx context.Context) {
	b.terminator.Close()
	b.compareAndSwapRunState(BackgroundProcessStateRunning, BackgroundProcessStateStopping)

	// Update the status to stopping for a multi node system. This was already updated in markStop for a single node
	// process.
	if b.mode() == backgroundManagerModeMultiNode {
		err := b.UpdateStatusClusterAware(ctx)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to update cluster status to stopping: %v", err)
		}
	}

}

// compareAndSwapRunState does a compare and swap on the run state. If the existing state does not match the old state then no update occurs.
func (b *BackgroundManager) compareAndSwapRunState(oldState BackgroundProcessState, newState BackgroundProcessState) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	if b.status.State == oldState {
		b.status.State = newState
	}
}

// backgroundManagerMode defines the types of BackgroundManager that can run.
type backgroundManagerMode int

const (
	// backgroundManagerModeLocal means that the BackgroundManager runs in memory only
	backgroundManagerModeLocal backgroundManagerMode = iota
	// backgroundManagerModeSingleNode means that the BackgroundManager is expected to run on a single node in a Sync Gateway cluster, and other nodes will be able to monitor the status
	backgroundManagerModeSingleNode
	// backgroundManagerModeMultiNode means that the BackgroundManager should run on all nodes of a Sync Gateway cluster
	backgroundManagerModeMultiNode
)

// mode returns the running mode a BackgroundManager.
func (b *BackgroundManager) mode() backgroundManagerMode {
	if b.clusterAwareOptions == nil {
		return backgroundManagerModeLocal
	}
	if b.clusterAwareOptions.multiNode {
		return backgroundManagerModeMultiNode
	}
	return backgroundManagerModeSingleNode
}
