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
var errBackgroundManagerAlreadyStopping = base.HTTPErrorf(http.StatusServiceUnavailable, "Process currently stopping. Wait until stopped to retry")

var errBackgroundManagerStatusNotRunning = errors.New("status in bucket is not running, but local status is, avoiding overwriting local status to bucket")

var errBackgroundManagerProcessAlreadyRunning = base.HTTPErrorf(http.StatusServiceUnavailable, "Process already running")

var errBackgroundManagerProcessAlreadyStopped = base.HTTPErrorf(http.StatusServiceUnavailable, "Process already stopped")

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
	MultiNode     bool

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
	Init(ctx context.Context, options map[string]any, clusterStatus []byte) error
	Run(ctx context.Context, options map[string]any, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error
	GetProcessStatus(status BackgroundManagerStatus) (statusOut []byte, meta []byte, err error)
	ResetStatus()
}

type updateStatusCallbackFunc func(ctx context.Context) error

// GetName returns name of the background manager
func (b *BackgroundManager) GetName() string {
	return b.name
}

func (b *BackgroundManager) Start(ctx context.Context, options map[string]any) error {
	err := b.markStart(ctx)
	if err != nil {
		if b.mode() == backgroundManagerModeMultiNode && errors.Is(err, errBackgroundManagerProcessAlreadyRunning) {
			return nil
		}
		return err
	}

	var processClusterStatus []byte
	if b.mode() != backgroundManagerModeLocal {
		processClusterStatus, _, err = b.clusterAwareOptions.metadataStore.GetRaw(b.clusterAwareOptions.StatusDocID())
		if err != nil && !base.IsDocNotFoundError(err) {
			return pkgerrors.Wrap(err, "Failed to get current process status")
		}
	}

	b.resetStatus()
	b.setStartTime(time.Now().UTC())

	// If we're resuming a cluster-aware process, try to reuse the previous start time
	if processClusterStatus != nil {
		var clusterStatus struct {
			Status struct {
				StartTime time.Time `json:"start_time"`
			} `json:"status"`
		}
		if err := base.JSONUnmarshal(processClusterStatus, &clusterStatus); err == nil {
			if !clusterStatus.Status.StartTime.IsZero() {
				b.setStartTime(clusterStatus.Status.StartTime)
			}
		}
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
			_ = b.clusterAwareOptions.metadataStore.Delete(b.clusterAwareOptions.HeartbeatDocID())
		}
	}()

	if b.mode() != backgroundManagerModeLocal {
		err := b.UpdateStatusClusterAware(ctx)
		if err != nil {
			base.ErrorfCtx(ctx, "Failed to update background manager status: %v", err)
		}
	}

	return nil
}

func (b *BackgroundManager) markStart(ctx context.Context) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	// If we're running in cluster aware 'mode' base the check off of a heartbeat doc
	if b.mode() == backgroundManagerModeSingleNode {
		_, err := b.clusterAwareOptions.metadataStore.WriteCas(b.clusterAwareOptions.HeartbeatDocID(), BackgroundManagerHeartbeatExpirySecs, 0, []byte("{}"), sgbucket.Raw)
		if base.IsCasMismatch(err) {
			// Check if markStop has been called but not yet processed
			var status HeartbeatDoc
			_, err := b.clusterAwareOptions.metadataStore.Get(b.clusterAwareOptions.HeartbeatDocID(), &status)
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
		if b.clusterStateIs(ctx, BackgroundProcessStateStopping) {
			return errBackgroundManagerAlreadyStopping
		}
	}

	if b.GetRunState() == BackgroundProcessStateRunning {
		return errBackgroundManagerProcessAlreadyRunning
	}

	if b.GetRunState() == BackgroundProcessStateStopping {
		return errBackgroundManagerAlreadyStopping
	}

	// Now we know that we're the only running process we should instantiate these values
	b.terminator = base.NewSafeTerminator()

	b.setRunState(BackgroundProcessStateRunning)
	return nil
}

// clusterStateIs returns if the state matches the serialized state in the bucket. If the document is not present, it will not match.
func (b *BackgroundManager) clusterStateIs(ctx context.Context, state BackgroundProcessState) bool {
	clusterState, err := b.getClusterStatusState(ctx)
	if err != nil {
		if !base.IsDocNotFoundError(err) {
			base.TracefCtx(ctx, base.KeyAll, "Error getting cluster status: %v, assuming no status", err)
		}
		return false
	}
	return clusterState == state
}

// getClusterStatusState gets the current background process state of the cluster and its CAS.
func (b *BackgroundManager) getClusterStatusState(ctx context.Context) (BackgroundProcessState, error) {
	docID := b.clusterAwareOptions.StatusDocID()
	statusRaw, _, err := b.clusterAwareOptions.metadataStore.GetSubDocRaw(ctx, docID, "status")
	if err != nil {
		return "", err
	}
	state, err := getBackgroundManagerState(statusRaw)
	if err != nil {
		return "", fmt.Errorf("could not get background manager state from cluster status doc %q: %w", docID, err)
	}
	return state, nil

}
func getBackgroundManagerState(statusRaw []byte) (BackgroundProcessState, error) {
	var clusterStatus struct {
		Status BackgroundProcessState `json:"status"`
	}
	if err := base.JSONUnmarshal(statusRaw, &clusterStatus); err != nil {
		return "", err
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
			status, _, err = b.getStatusLocal()
			return status, err
		}

		return status, err
	}

	status, _, err := b.getStatusLocal()
	return status, err
}

func (b *BackgroundManager) getStatusLocal() ([]byte, []byte, error) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()

	backgroundStatus := b.status
	if string(backgroundStatus.State) == "" {
		backgroundStatus.State = BackgroundProcessStateCompleted
	}

	if b.lastError != nil {
		backgroundStatus.LastErrorMessage = b.lastError.Error()
	}

	return b.Process.GetProcessStatus(backgroundStatus)
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
		_, _, err = b.clusterAwareOptions.metadataStore.GetRaw(b.clusterAwareOptions.HeartbeatDocID())
		if err != nil {
			if base.IsDocNotFoundError(err) {
				if clusterState == string(BackgroundProcessStateRunning) {
					status, _, err = b.getStatusLocal()
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
	b.setLastErrorMessage("")
	b.Process.ResetStatus()
}

func (b *BackgroundManager) setLastErrorMessage(msg string) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	b.status.LastErrorMessage = msg
}

func (b *BackgroundManager) Stop(ctx context.Context) error {
	if err := b.markStop(); err != nil {
		if errors.Is(err, errBackgroundManagerProcessAlreadyStopped) || errors.Is(err, errBackgroundManagerAlreadyStopping) {
			return nil
		}
		return err
	}

	if b.mode() == backgroundManagerModeMultiNode {
		// Use a detached context for status update as the original context might be cancelled
		err := b.UpdateStatusClusterAware(ctx)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to update cluster status to stopping: %v", err)
		}
	}

	b.Terminate()
	return nil
}

// Terminate stops the process via terminator channel
// Only to be used internally to this file and by tests.
func (b *BackgroundManager) Terminate() {
	b.terminator.Close()
	b.backgroundManagerStatusUpdateWaitGroup.Wait()
}

func (b *BackgroundManager) markStop() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.mode() == backgroundManagerModeSingleNode {
		_, _, err := b.clusterAwareOptions.metadataStore.GetRaw(b.clusterAwareOptions.HeartbeatDocID())
		if err != nil {
			if base.IsDocNotFoundError(err) {
				return errBackgroundManagerProcessAlreadyStopped
			}
			return base.HTTPErrorf(http.StatusInternalServerError, "Unable to verify whether a process is running: %v", err)
		}

		err = b.clusterAwareOptions.metadataStore.Set(b.clusterAwareOptions.HeartbeatDocID(), BackgroundManagerHeartbeatExpirySecs, nil, HeartbeatDoc{ShouldStop: true})
		if err != nil {
			return base.HTTPErrorf(http.StatusInternalServerError, "Failed to mark process as stopping: %v", err)
		}

		// If this is the node running the service
		if b.GetRunState() == BackgroundProcessStateRunning {
			b.setRunState(BackgroundProcessStateStopping)
		}

		return nil
	}

	if b.GetRunState() == BackgroundProcessStateStopping {
		return errBackgroundManagerAlreadyStopping
	}

	if b.GetRunState() == BackgroundProcessStateCompleted || b.GetRunState() == BackgroundProcessStateStopped || b.GetRunState() == BackgroundProcessStateError {
		return errBackgroundManagerProcessAlreadyStopped
	}

	b.setRunState(BackgroundProcessStateStopping)
	return nil
}

func (b *BackgroundManager) GetRunState() BackgroundProcessState {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	return b.status.State
}

func (b *BackgroundManager) setRunState(state BackgroundProcessState) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	b.status.State = state
}

func (b *BackgroundManager) getStartTime() time.Time {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	return b.status.StartTime
}

func (b *BackgroundManager) setStartTime(startTime time.Time) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	b.status.StartTime = startTime
}

func (b *BackgroundManager) SetError(err error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.lastError = err
	b.setRunState(BackgroundProcessStateError)
	b.Terminate()
}

func (b *BackgroundManager) UpdateStatusClusterAware(ctx context.Context) error {
	switch b.mode() {
	case backgroundManagerModeSingleNode:
		return b.UpdateSingleNodeClusterAwareStatus(ctx)
	case backgroundManagerModeMultiNode:
		return b.updateMultiNodeClusterAwareStatus(ctx)
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
		status, metadata, err := b.getStatusLocal()
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

// updateMultiNodeClusterAwareStatus updates the cluster status document with the current local status. If the bucket status is in a stopping / stopped / completed / error state but the local status is running, then this method will not update the bucket status and instead return. The caller is responsible for taking appropriate action.
func (b *BackgroundManager) updateMultiNodeClusterAwareStatus(ctx context.Context) error {
	docID := b.clusterAwareOptions.StatusDocID()
	_, err := b.clusterAwareOptions.metadataStore.Update(docID, 0, func(current []byte) ([]byte, *uint32, bool, error) {
		status, metadata, err := b.getStatusLocal()
		if err != nil {
			return nil, nil, false, err
		}
		output := make(map[string]json.RawMessage, 2)
		if current != nil {
			if err := base.JSONUnmarshal(current, &output); err != nil {
				return nil, nil, false, fmt.Errorf("Could not unmarshal doc(%q) within updateClusterAwareStatus: %w", docID, err)
			}
			if status, ok := output["status"]; ok {
				bucketState, err := getBackgroundManagerState(status)
				if err != nil {
					return nil, nil, false, err
				}
				if slices.Contains([]BackgroundProcessState{BackgroundProcessStateCompleted, BackgroundProcessStateStopping, BackgroundProcessStateStopped, BackgroundProcessStateError}, bucketState) && b.GetRunState() == BackgroundProcessStateRunning {
					return nil, nil, false, errBackgroundManagerStatusNotRunning
				}
			}
		}
		output["status"] = json.RawMessage(status)
		output["meta"] = json.RawMessage(metadata)

		outputBytes, err := base.JSONMarshal(output)
		if err != nil {
			return nil, nil, false, fmt.Errorf("could not marshal updated status doc %q: %w", docID, err)
		}
		return outputBytes, nil, false, nil
	})
	return err
}

type HeartbeatDoc struct {
	ShouldStop bool `json:"should_stop"`
}

// UpdateHeartbeatDocClusterAware simply performs a touch operation on the heartbeat document to update its expiry.
// Implements a retry. Used for Cluster Aware operations
func (b *BackgroundManager) UpdateHeartbeatDocClusterAware(ctx context.Context) error {
	statusRaw, _, err := b.clusterAwareOptions.metadataStore.GetAndTouchRaw(b.clusterAwareOptions.HeartbeatDocID(), BackgroundManagerHeartbeatExpirySecs)
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

func (b *BackgroundManager) startPollingMultiNodeStatus(ctx context.Context, terminator *base.SafeTerminator) {
	ticker := time.NewTicker(BackgroundManagerStatusUpdateIntervalSecs * time.Second)
	for {
		select {
		case <-ticker.C:
			triggerStop, err := b.watchStatusDocMultiNode(ctx)
			if err != nil {
				base.WarnfCtx(ctx, "Failed to watch poll status doc: %v, will retry", err)
			}
			if triggerStop {
				continue
			}
			err = b.updateMultiNodeClusterAwareStatus(ctx)
			if err != nil && !errors.Is(err, errBackgroundManagerStatusNotRunning) {
				base.DebugfCtx(ctx, base.KeyAll, "Failed to update multi node cluster aware status: %v, will retry", err)
			}
		case <-terminator.Done():
			ticker.Stop()
			return
		}
	}
}

// watchMultiNodeStatus polls the status document in the bucket. If another instance has stopped the process,
// or it encounter a fatal error, then this node should stop.
func (b *BackgroundManager) watchStatusDocMultiNode(ctx context.Context) (bool, error) {
	state, err := b.getClusterStatusState(ctx)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			return false, nil
		}
		return false, err
	}

	if slices.Contains([]BackgroundProcessState{BackgroundProcessStateStopping, BackgroundProcessStateStopped, BackgroundProcessStateCompleted, BackgroundProcessStateError}, state) && b.GetRunState() == BackgroundProcessStateRunning {
		_ = b.markStop()
		b.terminator.Close()
		return true, nil
	}

	return false, nil
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

func (b *BackgroundManager) mode() backgroundManagerMode {
	if b.clusterAwareOptions == nil {
		return backgroundManagerModeLocal
	}
	if b.clusterAwareOptions.MultiNode {
		return backgroundManagerModeMultiNode
	}
	return backgroundManagerModeSingleNode
}
