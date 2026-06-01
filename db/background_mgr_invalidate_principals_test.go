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
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockableProcess is a BackgroundManagerProcessI whose Run blocks until released or the terminator fires.
// After the first run, subsequent runs complete immediately to simulate a successful restart.
type blockableProcess struct {
	runCount atomic.Int32
	blockCh  chan struct{} // close to unblock the first run
}

func newBlockableProcess() *blockableProcess {
	return &blockableProcess{blockCh: make(chan struct{})}
}

func (b *blockableProcess) Init(_ context.Context, _ map[string]any, _ []byte) error { return nil }

func (b *blockableProcess) Run(_ context.Context, _ map[string]any, _ updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	count := b.runCount.Add(1)
	if count == 1 {
		select {
		case <-b.blockCh:
		case <-terminator.Done():
		}
	}
	return nil
}

func (b *blockableProcess) GetProcessStatus(status BackgroundManagerStatus, _ []byte) ([]byte, []byte, error) {
	out, err := base.JSONMarshal(status)
	return out, nil, err
}

func (b *blockableProcess) SetProcessStatus(_ context.Context, _, _ []byte) {}
func (b *blockableProcess) ResetStatus()                                    {}

// newBlockableManager returns a local BackgroundManager backed by a blockableProcess.
func newBlockableManager(p *blockableProcess) *BackgroundManager {
	return &BackgroundManager{
		name:       "test_invalidate_principals",
		Process:    p,
		terminator: base.NewSafeTerminator(),
	}
}

// TestInvalidatePrincipalsManagerCompletes verifies that a manager whose process completes quickly transitions
// to BackgroundProcessStateCompleted.
func TestInvalidatePrincipalsManagerCompletes(t *testing.T) {
	ctx := base.TestCtx(t)

	proc := newBlockableProcess()
	close(proc.blockCh) // unblock immediately so Run returns right away
	mgr := newBlockableManager(proc)

	require.NoError(t, mgr.Start(ctx, nil))

	RequireBackgroundManagerState(t, mgr, BackgroundProcessStateCompleted)
}

// TestRunInvalidatePrincipalsLoopCompletesNormally verifies that invalidatePrincipals returns nil
// when the manager completes successfully.
func TestRunInvalidatePrincipalsLoopCompletesNormally(t *testing.T) {
	ctx := base.TestCtx(t)

	proc := newBlockableProcess()
	close(proc.blockCh) // unblock immediately so the first Run returns right away
	resync := &ResyncManagerDCP{
		invalidatePrincipalsManager:  newBlockableManager(proc),
		invalidatePrincipalsPollWait: time.Millisecond,
	}

	terminator := base.NewSafeTerminator()
	doneCh := make(chan error, 1)
	go func() {
		doneCh <- resync.invalidatePrincipals(ctx, nil, false, false, terminator)
	}()

	require.NoError(t, base.RequireChanRecv(t, doneCh))
}

// TestRunInvalidatePrincipalsLoopNoOpIfAlreadyStarted verifies that the loop does not fail when Start
// returns errBackgroundManagerProcessAlreadyRunning; it waits and then proceeds once the manager completes.
func TestRunInvalidatePrincipalsLoopNoOpIfAlreadyStarted(t *testing.T) {
	ctx := base.TestCtx(t)

	proc := newBlockableProcess()
	mgr := newBlockableManager(proc)

	resync := &ResyncManagerDCP{
		invalidatePrincipalsManager:  mgr,
		invalidatePrincipalsPollWait: time.Millisecond,
	}
	terminator := base.NewSafeTerminator()

	// Start the manager externally so it is already running when the loop begins.
	require.NoError(t, mgr.Start(ctx, nil))
	RequireBackgroundManagerState(t, mgr, BackgroundProcessStateRunning)

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- resync.invalidatePrincipals(ctx, nil, false, false, terminator)
	}()

	// Loop should be polling (not failed) while the manager is running.
	select {
	case err := <-doneCh:
		t.Fatalf("loop returned prematurely: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Unblock the process — manager transitions to Completed and the loop returns.
	close(proc.blockCh)

	require.NoError(t, base.RequireChanRecv(t, doneCh))
}

// TestRunInvalidatePrincipalsLoopRestartsAfterStop verifies that when the manager is stopped before
// completing, the loop restarts it and waits for the subsequent run to complete.
func TestRunInvalidatePrincipalsLoopRestartsAfterStop(t *testing.T) {
	ctx := base.TestCtx(t)

	proc := newBlockableProcess()
	mgr := newBlockableManager(proc)

	resync := &ResyncManagerDCP{
		invalidatePrincipalsManager:  mgr,
		invalidatePrincipalsPollWait: time.Millisecond,
	}
	terminator := base.NewSafeTerminator()

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- resync.invalidatePrincipals(ctx, nil, false, false, terminator)
	}()

	// Wait for the manager to start.
	RequireBackgroundManagerState(t, mgr, BackgroundProcessStateRunning)

	// Loop should still be polling.
	select {
	case err := <-doneCh:
		t.Fatalf("loop returned prematurely: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Stop the manager while it is blocked — simulates a crash or external stop.
	require.NoError(t, mgr.Stop(ctx))

	// The loop must detect the Stopped state and restart the manager. The second run (count >= 2)
	// completes immediately, so the loop should finish successfully. We do not assert Stopped state
	// here because the fast poll interval means the loop may have already restarted the manager by
	// the time we check.
	require.NoError(t, base.RequireChanRecv(t, doneCh))
	assert.GreaterOrEqual(t, int(proc.runCount.Load()), 2, "process should have been run at least twice")
}

// slowBlockableProcess is a BlockableProcess whose first Run signals readyCh when it starts and then
// waits for blockCh to be closed before completing. This lets a test interleave an invalidatePrincipals
// call between "process is known to be running" and "process completes".
type slowBlockableProcess struct {
	blockableProcess
	readyCh chan struct{} // closed by Run once the first goroutine is inside the select
}

func newSlowBlockableProcess() *slowBlockableProcess {
	return &slowBlockableProcess{
		blockableProcess: blockableProcess{blockCh: make(chan struct{})},
		readyCh:          make(chan struct{}),
	}
}

func (s *slowBlockableProcess) Run(ctx context.Context, opts map[string]any, cb updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	count := s.runCount.Add(1)
	if count == 1 {
		close(s.readyCh) // signal that we are inside Run and blocking
		select {
		case <-s.blockCh:
		case <-terminator.Done():
		}
	}
	return nil
}

// TestRunInvalidatePrincipalsLoopAlreadyRunningCompletesBeforeWait reproduces the bug where Start()
// returns errBackgroundManagerProcessAlreadyRunning but the manager's process finishes before
// waitInvalidatePrincipals begins polling.
//
// The old implementation blocked on the manager's internal terminator channel. When the process
// completes between the Start() call and the select statement, the terminator fires immediately and
// the subsequent RetryLoop may see GetRunState() == Running (the state is updated after Terminate()
// closes the terminator), causing it to spin for up to 500 ms.
//
// The new implementation polls GetRunState on a ticker, so the terminal state is detected on the
// next tick regardless of how GetRunState looked at the moment the terminator fired.
func TestRunInvalidatePrincipalsLoopAlreadyRunningCompletesBeforeWait(t *testing.T) {
	ctx := base.TestCtx(t)

	proc := newSlowBlockableProcess()
	mgr := newBlockableManager(&proc.blockableProcess)
	// Swap the Run implementation to the slow version that signals readyCh.
	mgr.Process = proc

	resync := &ResyncManagerDCP{
		invalidatePrincipalsManager:  mgr,
		invalidatePrincipalsPollWait: time.Millisecond,
	}
	terminator := base.NewSafeTerminator()

	// Start the manager externally; the process blocks until we close blockCh.
	require.NoError(t, mgr.Start(ctx, nil))

	// Wait until the goroutine is inside Run so we know state == Running.
	select {
	case <-proc.readyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("process goroutine did not start")
	}

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- resync.invalidatePrincipals(ctx, nil, false, false, terminator)
	}()

	// invalidatePrincipals calls Start(), receives errBackgroundManagerProcessAlreadyRunning, and is
	// about to call waitInvalidatePrincipals. Unblock the process now so that the manager transitions
	// to Completed before (or just as) waitInvalidatePrincipals begins. The loop must correctly detect
	// the completed state and return nil rather than hanging.
	close(proc.blockCh)

	require.NoError(t, base.RequireChanRecv(t, doneCh))
}

// TestRunInvalidatePrincipalsLoopExitsOnTerminator verifies that the loop exits cleanly when the resync
// terminator fires while waiting for the manager to complete.
func TestRunInvalidatePrincipalsLoopExitsOnTerminator(t *testing.T) {
	ctx := base.TestCtx(t)

	proc := newBlockableProcess()
	mgr := newBlockableManager(proc)

	resync := &ResyncManagerDCP{
		invalidatePrincipalsManager:  mgr,
		invalidatePrincipalsPollWait: time.Millisecond,
	}
	terminator := base.NewSafeTerminator()

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- resync.invalidatePrincipals(ctx, nil, false, false, terminator)
	}()

	// Wait for the manager to start.
	RequireBackgroundManagerState(t, mgr, BackgroundProcessStateRunning)

	// Close the resync terminator — the loop should exit without error.
	terminator.Close()

	require.NoError(t, base.RequireChanRecv(t, doneCh))
}
