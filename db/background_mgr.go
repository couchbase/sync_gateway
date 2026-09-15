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
	"sync/atomic"
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

// isTerminal reports whether a run in this state has finished.
func (s BackgroundProcessState) isTerminal() bool {
	return slices.Contains([]BackgroundProcessState{BackgroundProcessStateCompleted, BackgroundProcessStateStopped, BackgroundProcessStateError}, s)
}

// errBackgroundManagerAlreadyStopping is returned when a Start or Stop is called while the process is in the Stopping
// state.
var errBackgroundManagerStatusAlreadyStopping = base.HTTPErrorf(http.StatusServiceUnavailable, "Process currently stopping. Wait until stopped to retry")

// errBackgroundManagerStatusNotRunning is returned when the bucket status is not running but the local status is.
type errBackgroundManagerStatusNotRunning struct {
	state   BackgroundProcessState
	message string
}

func (e errBackgroundManagerStatusNotRunning) Error() string {
	return e.message
}

func newErrBackgroundManagerStatusNotRunning(state BackgroundProcessState, message string) errBackgroundManagerStatusNotRunning {
	return errBackgroundManagerStatusNotRunning{
		state:   state,
		message: message,
	}
}

// errBackgroundManagerProcessAlreadyRunning is returned when an action to Start a process occurs but it is already running.
var errBackgroundManagerProcessAlreadyRunning = base.HTTPErrorf(http.StatusServiceUnavailable, "Process already running")

// errBackgroundManagerProcessAlreadyStopped is returned when an action to Stop a process occurs but it is already stopped.
var errBackgroundManagerProcessAlreadyStopped = base.HTTPErrorf(http.StatusServiceUnavailable, "Process already stopped")

// backgroundManagerInitMode is the return type of BackgroundManagerProcessI.Init, indicating whether
// the process is starting fresh or resuming a prior run.
type backgroundManagerInitMode int

const (
	// backgroundManagerInitReset means Init started a new run, discarding any previous state.
	backgroundManagerInitReset backgroundManagerInitMode = iota
	// backgroundManagerInitResume means Init found a previous run and will continue from where it left off.
	backgroundManagerInitResume
)

// backgroundManagerUpdateClusterStatusMode controls whether updateMultiNodeClusterAwareStatus enforces
// consistency between the local and cluster states.
type backgroundManagerUpdateClusterStatusMode int

const (
	// backgroundManagerStatusUpdate returns errBackgroundManagerStatusNotRunning
	// when the cluster doc shows the run is over but the status being written is running. Used by the polling loop
	// and periodic status updates to detect that another node has stopped or completed the process.
	backgroundManagerStatusUpdate backgroundManagerUpdateClusterStatusMode = iota
	// backgroundManagerStatusStart writes the current local status to the cluster doc unconditionally
	// and does not pass the previous status when updating local status. Used when starting a new run.
	backgroundManagerStatusStart
	// backgroundManagerStatusResume writes the current local status to the cluster doc unconditionally
	// and passes the previous status when updating local status. Used when resuming an existing run.
	backgroundManagerStatusResume
	// backgroundManagerStatusJoin passes the previous status like a resume, but is refused when the cluster has
	// already ended the run. Used when joining a run another node started.
	backgroundManagerStatusJoin
)

// isClaim reports whether the mode claims a run for this node rather than reporting on one it already holds.
func (m backgroundManagerUpdateClusterStatusMode) isClaim() bool {
	return m == backgroundManagerStatusStart || m == backgroundManagerStatusResume
}

type BackgroundProcessAction string

const (
	BackgroundProcessActionStart BackgroundProcessAction = "start"
	BackgroundProcessActionStop  BackgroundProcessAction = "stop"
)

// BackgroundManager this is the over-arching type which is exposed in DatabaseContext.
// O is the type of the options struct passed to BackgroundManagerProcessI.Init and BackgroundManagerProcessI.Run - this allows BackgroundManager to unmarshal the options as
// the (process-specific) options struct during Join.
type BackgroundManager[O any] struct {
	status BackgroundManagerStatus
	// pendingTerminalState is the terminal state a run adopts once its goroutine finishes. Guarded by statusLock.
	pendingTerminalState BackgroundProcessState
	statusLock           sync.RWMutex
	name                 string
	terminator           atomic.Pointer[base.SafeTerminator]
	// runWaitGroup tracks every goroutine of a run, so start can wait out the previous run.
	runWaitGroup        sync.WaitGroup
	clusterAwareOptions *ClusterAwareBackgroundManagerOptions
	lock                sync.Mutex
	Process             BackgroundManagerProcessI[O]
	// updateDatabaseState, when non-nil, is called from UpdateStatusClusterAware and from Join
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

// HeartbeatDocID returns the document name for the heartbeat document associated with this BackgroundManager.
func (b *ClusterAwareBackgroundManagerOptions) HeartbeatDocID() string {
	return b.metaKeys.BackgroundProcessHeartbeatPrefix(b.processSuffix)
}

// StatusDocID returns the document name for the status document associated with this BackgroundManager.
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

// BackgroundManagerProcessI is an interface satisfied by any of the background processes.
// O is the type of the options struct passed to Init and Run.
// Examples of this: ReSync, Compaction, Attachment Migration
type BackgroundManagerProcessI[O any] interface {
	// Init is called before Run for setup purposes. If Init errors, Run will not happen.
	// Returns backgroundManagerInitResume if resuming an existing (stopped) run, backgroundManagerInitReset if starting a new one.
	Init(ctx context.Context, options O, clusterStatus []byte) (backgroundManagerInitMode, error)
	// Run implements all of the work of the process.
	Run(ctx context.Context, options O, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error
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

// StoppableBackgroundManager allows stopping of the generic background managers.
type StoppableBackgroundManager interface {
	GetName() string
	GetRunState() BackgroundProcessState
	Stop(ctx context.Context) error
}

// updateStatusCallbackFunc is used inside a background process to signal that stats should be serialized to the
// bucket separately from the standard BackgroundManagerStatusUpdateIntervalSecs interval.
type updateStatusCallbackFunc func(ctx context.Context) error

// installTerminator gives the new run its terminator, and reports false if a Stop ended the run first. b.lock
// orders this against markStop, so a Stop either arrives before this decision or closes the terminator stored here.
func (b *BackgroundManager[O]) installTerminator() (*base.SafeTerminator, bool) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if b.GetRunState() == BackgroundProcessStateStopping {
		return nil, false
	}

	terminator := base.NewSafeTerminator()
	b.terminator.Store(terminator)
	return terminator, true
}

// GetName returns name of the background manager
func (b *BackgroundManager[O]) GetName() string {
	return b.name
}

// callUpdateDatabaseState invokes updateDatabaseState if it is set, logging any error.
func (b *BackgroundManager[O]) callUpdateDatabaseState(ctx context.Context, running bool) {
	if b.updateDatabaseState == nil {
		return
	}
	if err := b.updateDatabaseState(ctx, running); err != nil {
		base.WarnfCtx(ctx, "failed to update database state: %v", err)
	}
}

// Join an already-running multi-node background process using the options stored in
// the status document.  It only starts the local process when the cluster state is
// BackgroundProcessStateRunning; if there is no status document it returns
// errBackgroundManagerStatusNotRunning, and for any other terminal state it returns nil
// without starting the local process.  It also returns nil when the cluster ends the process while this node is
// joining it.  Only supported for multi-node background managers.
func (b *BackgroundManager[O]) Join(ctx context.Context) error {
	if b.mode() != backgroundManagerModeMultiNode {
		err := fmt.Errorf("Join is only supported for multi-node background managers (process %q)", b.name)
		base.WarnfCtx(ctx, "%v", err)
		return err
	}

	docID := b.clusterAwareOptions.StatusDocID()
	raw, _, err := b.clusterAwareOptions.metadataStore.GetRaw(ctx, docID)
	if err != nil {
		if base.IsDocNotFoundError(err) {
			b.callUpdateDatabaseState(ctx, false)
			// The status document does not exist, meaning the background process is not running yet on the cluster.
			// We report BackgroundProcessStateCompleted as the state since it is currently not running.
			return newErrBackgroundManagerStatusNotRunning(BackgroundProcessStateCompleted, fmt.Sprintf("failed to join background process %q: no cluster status document found in bucket", b.name))
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
			Options O `json:"options"`
		} `json:"meta"`
	}
	if err := base.JSONUnmarshal(raw, &doc); err != nil {
		return fmt.Errorf("failed to unmarshal meta for background process %q: %w", b.name, err)
	}

	return b.start(ctx, doc.Meta.Options, raw, true)
}

// Start starts a background manager with the given process-specific options and returns once the background
// process is started, or once a Stop landing while it waited for the previous run has ended it instead.
//
// Returns:
//   - errBackgroundManagerProcessAlreadyRunning if already running in local or single node cluster aware mode
//   - errBackgroundManagerStatusAlreadyStopping if in the process of stopping
//   - an error from Process.Init
func (b *BackgroundManager[O]) Start(ctx context.Context, options O) error {
	var processClusterStatus []byte
	if b.mode() != backgroundManagerModeLocal {
		var err error
		processClusterStatus, err = b.readClusterStatus(ctx)
		if err != nil {
			return err
		}
	}
	return b.start(ctx, options, processClusterStatus, false)
}

// readClusterStatus returns the cluster status document, or nil when the process has not run on this cluster yet.
func (b *BackgroundManager[O]) readClusterStatus(ctx context.Context) ([]byte, error) {
	raw, _, err := b.clusterAwareOptions.metadataStore.GetRaw(ctx, b.clusterAwareOptions.StatusDocID())
	if err != nil {
		if base.IsDocNotFoundError(err) {
			return nil, nil
		}
		return nil, pkgerrors.Wrap(err, "Failed to get current process status")
	}
	return raw, nil
}

// statusFromClusterDoc returns the status recorded in a cluster status document, or the zero status when there is
// none to read.
func statusFromClusterDoc(ctx context.Context, raw []byte) BackgroundManagerStatus {
	if raw == nil {
		return BackgroundManagerStatus{}
	}
	status, err := unmarshalBackgroundManagerStatus(raw)
	if err != nil {
		base.InfofCtx(ctx, base.KeyAll, "Could not unmarshal the cluster status before calling BackgroundManager.Run %v", err)
		return BackgroundManagerStatus{}
	}
	return status
}

// start marks the process as running, calls Process.Init, and launches Process.Run in a goroutine. options are
// process-specific and passed through to Init/Run, processClusterStatus is the last known status document (nil if
// none), and isJoin is true when this node is joining an already-running multi-node process rather than starting a
// new one.
//
// Returns:
//   - errBackgroundManagerProcessAlreadyRunning if already running in local or single node cluster aware mode
//   - errBackgroundManagerStatusAlreadyStopping if in the process of stopping
//   - an error from Process.Init
func (b *BackgroundManager[O]) start(ctx context.Context, options O, processClusterStatus []byte, isJoin bool) error {
	mode := b.mode()
	if mode != backgroundManagerModeMultiNode && b.updateDatabaseState != nil {
		return fmt.Errorf("updateDatabaseState should only be set for multi-node background managers")
	}
	previousStatus := statusFromClusterDoc(ctx, processClusterStatus)

	err := b.markStart(ctx, previousStatus)
	if err != nil {
		if mode == backgroundManagerModeMultiNode && errors.Is(err, errBackgroundManagerProcessAlreadyRunning) {
			return nil
		}
		return err
	}

	// The previous run reports its terminal state before its goroutines exit, so wait them out first.
	b.runWaitGroup.Wait()

	// A Stop landed while we waited. End the run before resetStatus and Init, which discard the previous run's
	// stats and checkpoints.
	terminator, started := b.installTerminator()
	if !started {
		b.finishRun(ctx, nil)
		return nil
	}

	// Init decides from these bytes whether to resume, so read them again now the previous run has published its
	// terminal status.
	if mode != backgroundManagerModeLocal {
		processClusterStatus, err = b.readClusterStatus(ctx)
		if err != nil {
			b.finishRun(ctx, err)
			return err
		}
		previousStatus = statusFromClusterDoc(ctx, processClusterStatus)
		// A stop landed while we waited. Leave the previous run's stats and checkpoints alone: resetStatus and
		// Init below would discard them for a run the cluster will not admit.
		if mode == backgroundManagerModeMultiNode && previousStatus.State == BackgroundProcessStateStopping {
			b.endRunWithoutPublishing(BackgroundProcessStateStopped)
			return errBackgroundManagerStatusAlreadyStopping
		}
	}

	b.resetStatus()
	b.setStartTime(time.Now().UTC())

	if mode == backgroundManagerModeSingleNode {
		b.runWaitGroup.Go(func() {
			b.updateHeartbeatDocPeriodically(ctx, terminator)
		})
	}

	// The start time identifies the run, so a node joining or resuming a run in progress adopts the one the cluster
	// is already reporting rather than naming a run of its own.
	if previousStatus.State == BackgroundProcessStateRunning && !previousStatus.StartTime.IsZero() {
		b.setStartTime(previousStatus.StartTime)
	}

	initMode, err := b.Process.Init(ctx, options, processClusterStatus)
	if err != nil {
		// No Process.Run goroutine was launched, so this goroutine ends the run.
		b.finishRun(ctx, err)
		return err
	}

	// Claim the run in the cluster before launching locally, so a node whose claim is refused never reaches
	// Process.Run.
	if mode != backgroundManagerModeLocal {
		var err error
		if mode == backgroundManagerModeMultiNode {
			statusMode := backgroundManagerStatusStart
			switch {
			case isJoin:
				statusMode = backgroundManagerStatusJoin
			case initMode == backgroundManagerInitResume:
				statusMode = backgroundManagerStatusResume
			}
			err = b.updateMultiNodeClusterAwareStatus(ctx, statusMode, nil)
			if stateErr, ok := errors.AsType[errBackgroundManagerStatusNotRunning](err); ok {
				// The cluster ended the process while this node was claiming it. Settle the local state
				// only: the cluster already holds the terminal status.
				b.endRunWithoutPublishing(stateErr.state)
				// A join is driven by the cluster, which has answered it. A Start was asked for by a
				// caller, who is told why the process did not start.
				if !isJoin && stateErr.state == BackgroundProcessStateStopping {
					return errBackgroundManagerStatusAlreadyStopping
				}
				return nil
			}
		} else {
			err = b.UpdateSingleNodeClusterAwareStatus(ctx)
		}
		if err != nil {
			base.ErrorfCtx(ctx, "Failed to update background manager status on start: %v", err)
			// No Process.Run goroutine was launched, so this goroutine ends the run.
			b.finishRun(ctx, err)
			return err
		}
	}

	switch mode {
	case backgroundManagerModeSingleNode:
		b.runWaitGroup.Go(func() {
			b.startPollingSingleNodeStatus(ctx, terminator)
		})
	case backgroundManagerModeMultiNode:
		b.runWaitGroup.Go(func() {
			b.startPollingMultiNodeStatus(ctx, terminator)
		})
	case backgroundManagerModeLocal:
		// Nothing to poll: a local process has no cluster status document.
	}
	b.runWaitGroup.Go(func() {
		err := b.Process.Run(ctx, options, b.UpdateStatusClusterAware, terminator)
		if err != nil {
			base.ErrorfCtx(ctx, "Error: %v", err)
		}
		b.finishRun(ctx, err)
	})

	return nil
}

// markStart changes the local status to started. previousStatus is the last known cluster status, used to check
// whether a multi-node process is currently stopping.
//
// A successful return admits this node as the only runner, but the previous run's goroutines may still be exiting.
//
// Returns:
//   - errBackgroundManagerProcessAlreadyRunning if already running in local or single node cluster aware mode
//   - errBackgroundManagerStatusAlreadyStopping if in the process of stopping
func (b *BackgroundManager[O]) markStart(ctx context.Context, previousStatus BackgroundManagerStatus) error {
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

		// The heartbeat write above seeds the grace period check in UpdateHeartbeatDocClusterAware.
		b.clusterAwareOptions.lastSuccessfulHeartbeatUnix.Set(time.Now().Unix())

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

	b.setRunState(BackgroundProcessStateRunning)
	return nil
}

// finishRun records err, terminates the process, then records and publishes the terminal state. Only the goroutine
// that owns the run may call it. Anything else records a reason with SetError instead.
func (b *BackgroundManager[O]) finishRun(ctx context.Context, err error) {
	// start calls this on its own goroutine for a run that never launched one, so join the group here.
	b.runWaitGroup.Add(1)
	defer b.runWaitGroup.Done()

	if err != nil {
		b.setLastErrorMessage(err.Error())
	}
	b.Terminate()
	b.updateTerminalStatus(ctx, b.setTerminalRunState(""))
}

// endRunWithoutPublishing ends a run that never launched Process.Run, adopting state as its terminal state. Used
// when another node has already published the terminal status, which this node's copy would overwrite.
func (b *BackgroundManager[O]) endRunWithoutPublishing(state BackgroundProcessState) {
	b.runWaitGroup.Add(1)
	defer b.runWaitGroup.Done()

	// Terminate first: settling the state admits the next run, whose terminator must not be the one closed here.
	b.Terminate()
	b.setTerminalRunState(state)
}

// updateTerminalStatus persists terminal, the status the run settled on rather than the local status a later run
// may already have replaced, and removes the heartbeat doc to allow a subsequent run.
func (b *BackgroundManager[O]) updateTerminalStatus(ctx context.Context, terminal BackgroundManagerStatus) {
	mode := b.mode()
	if mode == backgroundManagerModeLocal {
		return
	}

	var err error
	if mode == backgroundManagerModeMultiNode {
		err = b.updateMultiNodeClusterAwareStatus(ctx, backgroundManagerStatusUpdate, &terminal)
	} else {
		// A single node run holds the heartbeat doc until the delete below, so no other run can be writing here.
		err = b.UpdateSingleNodeClusterAwareStatus(ctx)
	}
	if err != nil {
		if _, ok := errors.AsType[errBackgroundManagerStatusNotRunning](err); !ok {
			base.WarnfCtx(ctx, "Failed to update terminal background manager status after finishing process: %v", err)
		}
	}

	// Delete the heartbeat doc to allow another process to run
	// Note: We can ignore the error, worst case is the user has to wait until the heartbeat doc expires
	_ = b.clusterAwareOptions.metadataStore.Delete(ctx, b.clusterAwareOptions.HeartbeatDocID())
}

// getClusterStatusState gets the current background process state of the cluster.
func (b *BackgroundManager[O]) getClusterStatusState(ctx context.Context) (BackgroundProcessState, error) {
	docID := b.clusterAwareOptions.StatusDocID()
	statusRaw, _, err := b.clusterAwareOptions.metadataStore.GetSubDocRaw(ctx, docID, "status")
	if err != nil {
		return "", err
	}
	status, err := unmarshalStatusSubDoc(statusRaw)
	if err != nil {
		return "", fmt.Errorf("could not get background manager state from cluster status doc %q: %w", docID, err)
	}
	return status.State, nil

}

// unmarshalStatusSubDoc returns the status held in the "status" subdocument of a cluster status document.
func unmarshalStatusSubDoc(statusRaw []byte) (BackgroundManagerStatus, error) {
	var status BackgroundManagerStatus
	if err := base.JSONUnmarshal(statusRaw, &status); err != nil {
		return BackgroundManagerStatus{}, err
	}
	return status, nil
}

// sameRun reports whether two statuses describe the same run. A run is identified by its start time: a start claims
// one, and a resume or a join adopts the one the cluster is already running, so every node of a run reports the same
// value. A zero time names no run and is treated as a match.
func sameRun(a, b BackgroundManagerStatus) bool {
	return a.StartTime.IsZero() || b.StartTime.IsZero() || a.StartTime.Equal(b.StartTime)
}

// unmarshalBackgroundManagerStatus returns the BackgroundManagerStatus from raw bytes of a whole status document.
func unmarshalBackgroundManagerStatus(statusRaw []byte) (BackgroundManagerStatus, error) {
	var clusterStatus struct {
		Status BackgroundManagerStatus `json:"status"`
	}
	if err := base.JSONUnmarshal(statusRaw, &clusterStatus); err != nil {
		return BackgroundManagerStatus{}, err
	}
	return clusterStatus.Status, nil
}

// GetStatus returns the bytes of the status document, preferring the cluster status if cluster aware and populated,
// otherwise falling back to the local status.
func (b *BackgroundManager[O]) GetStatus(ctx context.Context) ([]byte, error) {
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

// getStatusLocalWithoutPrevious returns the byte arrays of the status and meta fields of the status document by
// delegating to underlying background process. This should be called only when the existing status is not populated.
func (b *BackgroundManager[O]) getStatusLocalWithoutPrevious() (status []byte, meta []byte, err error) {
	return b.getStatusWithPrevious(nil)

}

// getStatusWithPrevious returns the byte arrays of the status and meta fields of the status document by
// delegating to the underlying background process. previous is the last serialized status document, used to
// merge/preserve existing stats; pass nil when there is no previous status to merge.
func (b *BackgroundManager[O]) getStatusWithPrevious(previous []byte) (status []byte, meta []byte, err error) {
	return b.serializeStatus(nil, previous)
}

// serializeStatus serializes the status to write. override, when non-nil, is used in place of the local status.
func (b *BackgroundManager[O]) serializeStatus(override *BackgroundManagerStatus, previous []byte) (status []byte, meta []byte, err error) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()

	if override != nil {
		return b.Process.GetProcessStatus(*override, previous)
	}

	backgroundStatus := b.status
	if backgroundStatus.State == "" {
		backgroundStatus.State = BackgroundProcessStateCompleted
	}
	// A run with a pending terminal state is already over, and publishing it as running would revive it.
	if b.pendingTerminalState != "" {
		backgroundStatus.State = b.pendingTerminalState
	}

	return b.Process.GetProcessStatus(backgroundStatus, previous)
}

// getStatusFromCluster returns the status subdocument of the status document as bytes by reading it from the cluster.
func (b *BackgroundManager[O]) getStatusFromCluster(ctx context.Context) ([]byte, error) {
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

// resetStatus clears the in memory status of the BackgroundManager. Used to reset the state from a
// previous status.
func (b *BackgroundManager[O]) resetStatus() {
	b.lock.Lock()
	defer b.lock.Unlock()

	b.setLastErrorMessage("")
	b.setPendingTerminalState("")
	b.Process.ResetStatus()
}

// setLastErrorMessage sets the last error message. The run state is left to setTerminalRunState, which settles it
// once the run stops.
func (b *BackgroundManager[O]) setLastErrorMessage(msg string) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	b.status.LastErrorMessage = msg
}

// setPendingTerminalState records the state a run adopts when its goroutine finishes. Ending the run any earlier
// would let markStart admit the next one while Process.Run is still working.
func (b *BackgroundManager[O]) setPendingTerminalState(state BackgroundProcessState) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	b.pendingTerminalState = state
}

// setTerminalRunState moves the run state to its terminal value and returns the status it settled on, so the
// caller can publish that rather than a later run's. pending is a terminal state learned from the cluster, or ""
// to use the one recorded by setPendingTerminalState.
func (b *BackgroundManager[O]) setTerminalRunState(pending BackgroundProcessState) BackgroundManagerStatus {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	if pending == "" {
		pending = b.pendingTerminalState
	}
	b.pendingTerminalState = ""
	switch {
	case b.status.LastErrorMessage != "":
		b.status.State = BackgroundProcessStateError
	// A stop, local or reported by another node, outranks a state the cluster reached on its own.
	case b.status.State == BackgroundProcessStateStopping, pending == BackgroundProcessStateStopping:
		b.status.State = BackgroundProcessStateStopped
	case pending != "":
		b.status.State = pending
	case b.status.State == BackgroundProcessStateRunning:
		b.status.State = BackgroundProcessStateCompleted
	}
	return b.status
}

// Stop triggers a Stop of the background process. This will transition the state to BackgroundProcessStateStopping and
// return from this function.
//
// This will return an error if the status is not in a running state, as already stopped or stopping.
func (b *BackgroundManager[O]) Stop(ctx context.Context) error {
	if err := b.markStop(ctx); err != nil {
		if errors.Is(err, errBackgroundManagerProcessAlreadyStopped) || errors.Is(err, errBackgroundManagerStatusAlreadyStopping) {
			return nil
		}
		return err
	}
	b.stopProcess(ctx)
	return nil
}

// Terminate stops the process via the terminator channel of the currently installed run, without waiting for that
// run's goroutines, which start does.
// Only to be used internally to this file and by tests.
func (b *BackgroundManager[O]) Terminate() {
	// The terminator is nil until the first run installs one.
	if terminator := b.terminator.Load(); terminator != nil {
		terminator.Close()
	}
}

// markStop will change the local state of the background manager and signal to background managers on other Sync
// Gateway nodes to stop.
func (b *BackgroundManager[O]) markStop(ctx context.Context) error {
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
	if currentState == "" || currentState.isTerminal() {
		return errBackgroundManagerProcessAlreadyStopped
	}
	b.setRunState(BackgroundProcessStateStopping)

	return nil
}

// GetRunState returns the in memory state of the background process. This may different from the serialized bucket state.
func (b *BackgroundManager[O]) GetRunState() BackgroundProcessState {
	b.statusLock.RLock()
	defer b.statusLock.RUnlock()
	return b.status.State
}

// setRunState sets the in memory state of the background process. This does not updated the serialized bucket state.
func (b *BackgroundManager[O]) setRunState(state BackgroundProcessState) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	b.status.State = state
}

// getStartTime returns the current start time of the background process from an in memory value. This may be different from the seraialized start time. If no start time is present, returns nil time.Time.
func (b *BackgroundManager[O]) getStartTime() time.Time {
	b.statusLock.RLock()
	defer b.statusLock.RUnlock()
	return b.status.StartTime
}

// setStartTime sets the start time of the background process to an in memory value. This does not update the serialized bucket state.
func (b *BackgroundManager[O]) setStartTime(startTime time.Time) {
	b.statusLock.Lock()
	defer b.statusLock.Unlock()
	b.status.StartTime = startTime
}

// SetError sets the last known error and terminates the process. The run state is settled by the goroutine running
// the process when it stops.
func (b *BackgroundManager[O]) SetError(err error) {
	b.setLastErrorMessage(err.Error())
	b.Terminate()
}

// UpdateStatusClusterAware reads the local status and writes that value to the bucket. This will update the "status" and "meta" keys of the status document.
// In multi-node mode, if the cluster doc shows a terminal state while the local state is running, it returns errBackgroundManagerStatusNotRunning without writing.
// This is the callback passed to Run.
func (b *BackgroundManager[O]) UpdateStatusClusterAware(ctx context.Context) error {
	switch b.mode() {
	case backgroundManagerModeSingleNode:
		return b.UpdateSingleNodeClusterAwareStatus(ctx)
	case backgroundManagerModeMultiNode:
		return b.updateMultiNodeClusterAwareStatus(ctx, backgroundManagerStatusUpdate, nil)
	case backgroundManagerModeLocal:
		return nil
	default:
		return fmt.Errorf("unknown background manager mode: %v", b.mode())
	}
}

// UpdateSingleNodeClusterAwareStatus gets the current local status from the running process and writes the status
// document in the bucket. Used for Cluster Aware operations
func (b *BackgroundManager[O]) UpdateSingleNodeClusterAwareStatus(ctx context.Context) error {
	if b.clusterAwareOptions == nil {
		return nil
	}
	status, metadata, err := b.getStatusLocalWithoutPrevious()
	if err != nil {
		return err
	}

	doc := map[string]json.RawMessage{
		"status": status,
		"meta":   metadata,
	}

	return b.clusterAwareOptions.metadataStore.Set(ctx, b.clusterAwareOptions.StatusDocID(), 0, nil, doc)
}

// updateMultiNodeClusterAwareStatus updates the cluster status document with the current local status.
// statusOverride, when non-nil, is written in place of that local status, so a finished run publishes the state it
// settled on.
// The write is refused with errBackgroundManagerStatusNotRunning when it does not belong on the document: a claim
// against a cluster that is stopping, a join against a run the cluster has ended, or an update from a run other than
// the one the document holds.
func (b *BackgroundManager[O]) updateMultiNodeClusterAwareStatus(ctx context.Context, mode backgroundManagerUpdateClusterStatusMode, statusOverride *BackgroundManagerStatus) error {
	docID := b.clusterAwareOptions.StatusDocID()
	var previousStatus []byte
	var newStatus []byte
	var publishedState BackgroundProcessState
	_, err := b.clusterAwareOptions.metadataStore.Update(ctx, docID, 0, func(current []byte) ([]byte, *uint32, bool, error) {
		// In the case of starting a run (backgroundManagerStatusStart), don't send the previous status to
		// BackgroundManagerProcessI so as to not process previous stats.
		// For status updates and resuming runs, we do pass the previous status so we can merge/preserve existing stats.
		if mode != backgroundManagerStatusStart {
			previousStatus = current
		}
		status, metadata, err := b.serializeStatus(statusOverride, previousStatus)
		if err != nil {
			return nil, nil, false, err
		}
		// Check the status about to be written rather than the local status: the process serializes a status of
		// its own, and the local one can move on in between.
		proposedStatus, err := unmarshalStatusSubDoc(status)
		if err != nil {
			return nil, nil, false, err
		}
		output := make(map[string]json.RawMessage, 2)
		if current != nil {
			if err := base.JSONUnmarshal(current, &output); err != nil {
				return nil, nil, false, fmt.Errorf("Could not unmarshal doc(%q) within updateClusterAwareStatus: %w", docID, err)
			}
			if currentStatus, ok := output["status"]; ok {
				documentStatus, err := unmarshalStatusSubDoc(currentStatus)
				if err != nil {
					return nil, nil, false, err
				}
				bucketState := documentStatus.State
				// Stopping counts as over: this node must not report itself running to it.
				runIsOver := bucketState == BackgroundProcessStateStopping || bucketState.isTerminal()
				switch {
				// A claim must not restart a process the cluster is still stopping. Terminal states are left
				// alone: a new run is meant to replace them.
				case mode.isClaim() && bucketState == BackgroundProcessStateStopping:
					return nil, nil, false, newErrBackgroundManagerStatusNotRunning(bucketState, fmt.Sprintf("canceling start: the cluster is stopping background process %q", b.name))
				// A node joining a run must not resurrect a process the cluster has already ended.
				case mode == backgroundManagerStatusJoin && runIsOver:
					return nil, nil, false, newErrBackgroundManagerStatusNotRunning(bucketState, fmt.Sprintf("canceling join: the cluster already transitioned background process %q to state %q", b.name, bucketState))
				// If the local status is running, but another node stopped or errored, adopt that state locally so
				// that we transition properly when our process terminates, and report not running so that caller can
				// terminate the background manager on this node.
				case mode == backgroundManagerStatusUpdate && runIsOver && proposedStatus.State == BackgroundProcessStateRunning:
					return nil, nil, false, newErrBackgroundManagerStatusNotRunning(bucketState, fmt.Sprintf("canceling update: another node already transitioned background process %q to terminal state %q", b.name, bucketState))
				// This node is reporting on a run the document no longer holds, so its status, terminal or not,
				// belongs to a run that is over. State alone cannot tell the two apart.
				case mode == backgroundManagerStatusUpdate && !sameRun(proposedStatus, documentStatus):
					return nil, nil, false, newErrBackgroundManagerStatusNotRunning(bucketState, fmt.Sprintf("canceling update: background process %q has moved on to another run", b.name))
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
		publishedState = proposedStatus.State
		return outputBytes, nil, false, nil
	})
	if err != nil {
		// A refused write means this node does not hold the run the document describes. Mirror that, or the
		// database state document keeps this node at running and another node joins a run that is over.
		if _, ok := errors.AsType[errBackgroundManagerStatusNotRunning](err); ok {
			b.callUpdateDatabaseState(ctx, false)
		}
		return err
	}
	b.Process.SetProcessStatus(ctx, previousStatus, newStatus)
	// Mirror the published state, not the run state: a node that knows the cluster ended the run keeps working
	// until Process.Run returns.
	b.callUpdateDatabaseState(ctx, publishedState == BackgroundProcessStateRunning)
	return nil
}

type HeartbeatDoc struct {
	ShouldStop bool `json:"should_stop"`
}

// UpdateHeartbeatDocClusterAware performs a touch operation on the heartbeat document to update its expiry, and
// stops the process if the doc indicates a stop was requested. Used for Cluster Aware operations.
func (b *BackgroundManager[O]) UpdateHeartbeatDocClusterAware(ctx context.Context) error {
	statusRaw, _, err := b.clusterAwareOptions.metadataStore.GetAndTouchRaw(ctx, b.clusterAwareOptions.HeartbeatDocID(), BackgroundManagerHeartbeatExpirySecs)
	if err != nil {
		// If we get an error but the error is doc not found and terminator closed it means we have terminated the
		// goroutine which intermittently runs this but this snuck in before it was stopped. This may result in the doc
		// being deleted before this runs. We can ignore that error is that is the case.
		if terminator := b.terminator.Load(); base.IsDocNotFoundError(err) && terminator != nil && terminator.IsClosed() {
			return nil
		}

		// If we've hit an error, and we haven't had a successful heartbeat in just under its TTL then we need to quit
		// out. If we fail to write heartbeat for this time we can no longer ensure that this would be the only process
		// running and another could end up starting.
		if time.Since(time.Unix(b.clusterAwareOptions.lastSuccessfulHeartbeatUnix.Value(), 0)) > (BackgroundManagerHeartbeatExpirySecs-BackgroundManagerHeartbeatIntervalSecs)*time.Second {
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

// updateHeartbeatDocPeriodically refreshes the heartbeat doc until the terminator closes, keeping other nodes from
// starting the same process.
func (b *BackgroundManager[O]) updateHeartbeatDocPeriodically(ctx context.Context, terminator *base.SafeTerminator) {
	ticker := time.NewTicker(BackgroundManagerHeartbeatIntervalSecs * time.Second)
	for {
		select {
		case <-ticker.C:
			if err := b.UpdateHeartbeatDocClusterAware(ctx); err != nil {
				base.ErrorfCtx(ctx, "Failed to update expiry on heartbeat doc: %v", err)
				b.SetError(err)
			}
		case <-terminator.Done():
			return
		}
	}
}

// startPollingSingleNodeStatus starts a loop which writes the local status to the bucket until the terminator closes.
func (b *BackgroundManager[O]) startPollingSingleNodeStatus(ctx context.Context, terminator *base.SafeTerminator) {
	ticker := time.NewTicker(BackgroundManagerStatusUpdateIntervalSecs * time.Second)
	for {
		select {
		case <-ticker.C:
			if err := b.UpdateSingleNodeClusterAwareStatus(ctx); err != nil {
				base.WarnfCtx(ctx, "Failed to update background manager status in periodic polling: %v, will retry", err)
			}
		case <-terminator.Done():
			return
		}
	}
}

// startPollingMultiNodeStatus starts a loop which polls the status document for changes. If the status document
// indicates that the process should stop, then this will trigger a stop of the local process. This is used for
// multi-node cluster aware background managers where we want all nodes to stop if any node triggers a stop.
func (b *BackgroundManager[O]) startPollingMultiNodeStatus(ctx context.Context, terminator *base.SafeTerminator) {
	ticker := time.NewTicker(BackgroundManagerStatusUpdateIntervalSecs * time.Second)
	for {
		select {
		case <-ticker.C:
			if err := b.updateMultiNodeClusterAwareStatus(ctx, backgroundManagerStatusUpdate, nil); err != nil {
				if stateErr, ok := errors.AsType[errBackgroundManagerStatusNotRunning](err); ok {
					// Another node ended the process. Record the state it reached, for the Process.Run
					// goroutine to adopt when it finishes.
					b.setPendingTerminalState(stateErr.state)
					terminator.Close()
					return
				}
				base.DebugfCtx(ctx, base.KeyAll, "Failed to update multi node cluster aware status: %v, will retry", err)
			}
		case <-terminator.Done():
			return
		}
	}
}

// stopProcess terminates the locally running process.
func (b *BackgroundManager[O]) stopProcess(ctx context.Context) {
	b.Terminate()
	b.compareAndSwapRunState(BackgroundProcessStateRunning, BackgroundProcessStateStopping)

	// Update the status to stopping for a multi node system. This was already updated in markStop for a single node
	// process.
	if b.mode() == backgroundManagerModeMultiNode {
		err := b.UpdateStatusClusterAware(ctx)
		if err != nil {
			if _, ok := errors.AsType[errBackgroundManagerStatusNotRunning](err); !ok {
				base.WarnfCtx(ctx, "Failed to update cluster status to stopping: %v", err)
			}
		}
	}

}

// compareAndSwapRunState does a compare and swap on the run state. If the existing state does not match the old state then no update occurs.
func (b *BackgroundManager[O]) compareAndSwapRunState(oldState BackgroundProcessState, newState BackgroundProcessState) {
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

// mode returns the running mode of a BackgroundManager.
func (b *BackgroundManager[O]) mode() backgroundManagerMode {
	if b.clusterAwareOptions == nil {
		return backgroundManagerModeLocal
	}
	if b.clusterAwareOptions.multiNode {
		return backgroundManagerModeMultiNode
	}
	return backgroundManagerModeSingleNode
}
