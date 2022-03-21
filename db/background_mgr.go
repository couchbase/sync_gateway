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
	"net/http"
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

type BackgroundProcessAction string

const (
	BackgroundProcessActionStart BackgroundProcessAction = "start"
	BackgroundProcessActionStop  BackgroundProcessAction = "stop"
)

// BackgroundManager this is the over-arching type which is exposed in DatabaseContext
type BackgroundManager struct {
	BackgroundManagerStatus
	lastError           error
	terminator          *base.SafeTerminator
	clusterAwareOptions *ClusterAwareBackgroundManagerOptions
	lock                sync.Mutex
	Process             BackgroundManagerProcessI
}

const (
	BackgroundManagerHeartbeatExpirySecs      = 30
	BackgroundManagerHeartbeatIntervalSecs    = 1
	BackgroundManagerStatusUpdateIntervalSecs = 1
)

type ClusterAwareBackgroundManagerOptions struct {
	bucket        base.Bucket
	processSuffix string

	lastSuccessfulHeartbeatUnix base.AtomicInt
}

func (b *ClusterAwareBackgroundManagerOptions) HeartbeatDocID() string {
	return base.SyncPrefix + ":background_process:heartbeat:" + b.processSuffix
}

func (b *ClusterAwareBackgroundManagerOptions) StatusDocID() string {
	return base.SyncPrefix + ":background_process:status:" + b.processSuffix
}

// BackgroundManagerStatus simply stores data used in BackgroundManager. This data can also be exposed to users over
// REST. Splitting this out into an additional embedded struct allows easy JSON marshalling
type BackgroundManagerStatus struct {
	State            BackgroundProcessState `json:"status"`
	StartTime        time.Time              `json:"start_time"`
	LastErrorMessage string                 `json:"last_error"`
}

// BackgroundManagerProcessI is an interface satisfied by any of the background processes
// Examples of this: ReSync, Compaction
type BackgroundManagerProcessI interface {
	Init(options map[string]interface{}, clusterStatus []byte) error
	Run(options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error
	GetProcessStatus(status BackgroundManagerStatus) (statusOut []byte, meta []byte, err error)
	ResetStatus()
}

type updateStatusCallbackFunc func() error

func (b *BackgroundManager) Start(options map[string]interface{}) error {
	err := b.markStart()
	if err != nil {
		return err
	}

	logCtx := context.TODO()
	var processClusterStatus []byte
	if b.isClusterAware() {
		processClusterStatus, _, err = b.clusterAwareOptions.bucket.GetRaw(b.clusterAwareOptions.StatusDocID())
		if err != nil && !base.IsDocNotFoundError(err) {
			return pkgerrors.Wrap(err, "Failed to get current process status")
		}
	}

	b.resetStatus()
	b.StartTime = time.Now().UTC()

	err = b.Process.Init(options, processClusterStatus)
	if err != nil {
		return err
	}

	if b.isClusterAware() {
		go func() {
			ticker := time.NewTicker(BackgroundManagerStatusUpdateIntervalSecs * time.Second)
			for {
				select {
				case <-ticker.C:
					err = b.UpdateStatusClusterAware()
					if err != nil {
						base.WarnfCtx(logCtx, "Failed to update background manager status: %v", err)
					}

				case <-b.terminator.Done():
					ticker.Stop()
					return
				}
			}
		}()
	}

	go func() {
		updateStatusClusterAwareCallback := func() error {
			return b.UpdateStatusClusterAware()
		}
		err := b.Process.Run(options, updateStatusClusterAwareCallback, b.terminator)
		if err != nil {
			base.ErrorfCtx(logCtx, "Error: %v", err)
			b.SetError(err)
		}

		b.Terminate()

		b.lock.Lock()
		if b.State == BackgroundProcessStateStopping {
			b.State = BackgroundProcessStateStopped
		} else if b.State != BackgroundProcessStateError {
			b.State = BackgroundProcessStateCompleted
		}
		b.lock.Unlock()

		// Once our background process run has completed we should update the completed status and delete the heartbeat
		// doc
		if b.isClusterAware() {
			err = b.UpdateStatusClusterAware()
			if err != nil {
				base.WarnfCtx(logCtx, "Failed to update background manager status: %v", err)
			}

			// Delete the heartbeat doc to allow another process to run
			// Note: We can ignore the error, worst case is the user has to wait until the heartbeat doc expires
			_ = b.clusterAwareOptions.bucket.Delete(b.clusterAwareOptions.HeartbeatDocID())
		}
	}()

	if b.isClusterAware() {
		err = b.UpdateStatusClusterAware()
		if err != nil {
			base.ErrorfCtx(logCtx, "Failed to update background manager status: %v", err)
		}
	}

	return nil
}

func (b *BackgroundManager) markStart() error {
	b.lock.Lock()
	defer b.lock.Unlock()

	processAlreadyRunningErr := base.HTTPErrorf(http.StatusServiceUnavailable, "Process already running")

	// If we're running in cluster aware 'mode' base the check off of a heartbeat doc
	if b.isClusterAware() {
		_, err := b.clusterAwareOptions.bucket.WriteCas(b.clusterAwareOptions.HeartbeatDocID(), 0, BackgroundManagerHeartbeatExpirySecs, 0, []byte("{}"), sgbucket.Raw)
		if base.IsCasMismatch(err) {
			// Check if markStop has been called but not yet processed
			var status HeartbeatDoc
			_, err := b.clusterAwareOptions.bucket.Get(b.clusterAwareOptions.HeartbeatDocID(), &status)
			if err == nil && status.ShouldStop {
				return base.HTTPErrorf(http.StatusServiceUnavailable, "Process stop still in progress - please wait before restarting")
			}
			return processAlreadyRunningErr
		}

		// Now we know that we're the only running process we should instantiate these values
		// We need to instantiate these before we setup the below goroutine as it relies upon the terminator
		b.terminator = base.NewSafeTerminator()

		go func() {
			ticker := time.NewTicker(BackgroundManagerHeartbeatIntervalSecs * time.Second)
			for {
				select {
				case <-ticker.C:
					err = b.UpdateHeartbeatDocClusterAware()
					if err != nil {
						base.ErrorfCtx(context.TODO(), "Failed to update expiry on heartbeat doc: %v", err)
						b.SetError(err)
					}
				case <-b.terminator.Done():
					ticker.Stop()
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

	if b.State == BackgroundProcessStateStopping {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Process currently stopping. Wait until stopped to retry")
	}

	// Now we know that we're the only running process we should instantiate these values
	b.terminator = base.NewSafeTerminator()

	b.State = BackgroundProcessStateRunning
	return nil
}

func (b *BackgroundManager) GetStatus() ([]byte, error) {
	if b.isClusterAware() {
		status, err := b.getStatusFromCluster()
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
	b.lock.Lock()
	defer b.lock.Unlock()

	backgroundStatus := b.BackgroundManagerStatus
	if string(backgroundStatus.State) == "" {
		backgroundStatus.State = BackgroundProcessStateCompleted
	}

	if b.lastError != nil {
		backgroundStatus.LastErrorMessage = b.lastError.Error()
	}

	return b.Process.GetProcessStatus(backgroundStatus)
}

func (b *BackgroundManager) getStatusFromCluster() ([]byte, error) {
	status, statusCas, err := b.clusterAwareOptions.bucket.GetSubDocRaw(b.clusterAwareOptions.StatusDocID(), "status")
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

	// Work here is required because if the process crashes we'd end up in a state where a GET would return 'running'
	// when in-fact it crashed.
	// Worst case we should do this once if we have to do this and update the cluster status doc
	if clusterState, ok := clusterStatus["status"].(string); ok &&
		clusterState != string(BackgroundProcessStateCompleted) &&
		clusterState != string(BackgroundProcessStateStopped) &&
		clusterState != string(BackgroundProcessStateError) {
		_, _, err = b.clusterAwareOptions.bucket.GetRaw(b.clusterAwareOptions.HeartbeatDocID())
		if err != nil {
			if base.IsDocNotFoundError(err) {
				clusterStatus["status"] = BackgroundProcessStateStopped
				status, err = base.JSONMarshal(clusterStatus)
				if err != nil {
					return nil, err
				}

				// In the event there is a crash and need to update the status we should attempt to update the doc to
				// avoid this unmarshal / marshal work from having to happen again, next time GET is called.
				// If there is an error we can just ignore it as worst case we run this unmarshal / marshal again on
				// next request
				_, _ = b.clusterAwareOptions.bucket.WriteSubDoc(b.clusterAwareOptions.StatusDocID(), "status", statusCas, status)
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
	b.terminator.Close()
}

func (b *BackgroundManager) markStop() error {
	processAlreadyStoppedErr := base.HTTPErrorf(http.StatusServiceUnavailable, "Process already stopped")
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.isClusterAware() {
		_, _, err := b.clusterAwareOptions.bucket.GetRaw(b.clusterAwareOptions.HeartbeatDocID())
		if err != nil {
			if base.IsDocNotFoundError(err) {
				return processAlreadyStoppedErr
			}
			return base.HTTPErrorf(http.StatusInternalServerError, "Unable to verify whether a process is running: %v", err)
		}

		err = b.clusterAwareOptions.bucket.Set(b.clusterAwareOptions.HeartbeatDocID(), BackgroundManagerHeartbeatExpirySecs, nil, HeartbeatDoc{ShouldStop: true})
		if err != nil {
			return base.HTTPErrorf(http.StatusInternalServerError, "Failed to mark process as stopping: %v", err)
		}

		// If this is the node running the service
		if b.State == BackgroundProcessStateRunning {
			b.State = BackgroundProcessStateStopping
		}

		return nil
	}

	if b.State == BackgroundProcessStateStopping {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Process already stopping")
	}

	if b.State == BackgroundProcessStateCompleted || b.State == BackgroundProcessStateStopped {
		return processAlreadyStoppedErr
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
	b.Terminate()
}

// UpdateStatusClusterAware gets the current local status from the running process and updates the status document in
// the bucket. Implements a retry. Used for Cluster Aware operations
func (b *BackgroundManager) UpdateStatusClusterAware() error {
	if b.clusterAwareOptions == nil {
		return nil
	}
	err, _ := base.RetryLoop("UpdateStatusClusterAware", func() (shouldRetry bool, err error, value interface{}) {
		status, metadata, err := b.getStatusLocal()
		if err != nil {
			return true, err, nil
		}

		_, err = b.clusterAwareOptions.bucket.WriteSubDoc(b.clusterAwareOptions.StatusDocID(), "status", 0, status)
		if err != nil {
			return true, err, nil
		}

		_, err = b.clusterAwareOptions.bucket.WriteSubDoc(b.clusterAwareOptions.StatusDocID(), "meta", 0, metadata)
		if err != nil {
			return true, err, nil
		}

		return false, nil, nil
	}, base.CreateSleeperFunc(5, 100))
	return err
}

type HeartbeatDoc struct {
	ShouldStop bool `json:"should_stop"`
}

// UpdateHeartbeatDocClusterAware simply performs a touch operation on the heartbeat document to update its expiry.
// Implements a retry. Used for Cluster Aware operations
func (b *BackgroundManager) UpdateHeartbeatDocClusterAware() error {
	statusRaw, _, err := b.clusterAwareOptions.bucket.GetAndTouchRaw(b.clusterAwareOptions.HeartbeatDocID(), BackgroundManagerHeartbeatExpirySecs)
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
		err = b.Stop()
		if err != nil {
			base.WarnfCtx(context.TODO(), "Failed to stop process %q: %v", b.clusterAwareOptions.processSuffix, err)
		}
	}

	b.clusterAwareOptions.lastSuccessfulHeartbeatUnix.Set(time.Now().Unix())
	return nil
}

func (b *BackgroundManager) isClusterAware() bool {
	return b.clusterAwareOptions != nil
}
