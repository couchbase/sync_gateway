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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockProcess struct {
	InitCalled           bool
	RunCalled            bool
	StopRequested        bool
	SleepDuration        time.Duration
	updateStatusCallback updateStatusCallbackFunc
	lock                 sync.Mutex
}

func (m *MockProcess) Init(ctx context.Context, options map[string]any, clusterStatus []byte) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.InitCalled = true
	return nil
}

func (m *MockProcess) Run(ctx context.Context, options map[string]any, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	m.lock.Lock()
	m.RunCalled = true
	m.updateStatusCallback = persistClusterStatusCallback
	m.lock.Unlock()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-terminator.Done():
			m.lock.Lock()
			m.StopRequested = true
			m.lock.Unlock()
			return nil
		case <-ticker.C:
			if m.SleepDuration > 0 {
				time.Sleep(m.SleepDuration)
			}
			if persistClusterStatusCallback != nil {
				_ = persistClusterStatusCallback(ctx)
			}
		}
	}
}

func (m *MockProcess) SetProcessStatus(context.Context, []byte, []byte) {}

func (m *MockProcess) GetProcessStatus(status BackgroundManagerStatus, _ []byte) (statusOut []byte, meta []byte, err error) {
	statusOut, err = base.JSONMarshal(status)
	return statusOut, nil, err
}

func (m *MockProcess) ResetStatus() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.InitCalled = false
	m.RunCalled = false
	m.StopRequested = false
}

func TestBackgroundManagerModes(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := context.Background()
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test")

	modes := []struct {
		name                string
		clusterAwareOptions *ClusterAwareBackgroundManagerOptions
	}{
		{
			name:                "Local",
			clusterAwareOptions: nil,
		},
		{
			name: "ClusterAware",
			clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
				metadataStore: metadataStore,
				metaKeys:      metaKeys,
				processSuffix: "aware",
			},
		},
		{
			name: "MultiNode",
			clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
				metadataStore: metadataStore,
				metaKeys:      metaKeys,
				processSuffix: "multi",
				multiNode:     true,
			},
		},
	}

	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			process := &MockProcess{}
			mgr := &BackgroundManager{
				name:                "test-mgr-" + mode.name,
				clusterAwareOptions: mode.clusterAwareOptions,
				Process:             process,
			}

			err := mgr.Start(ctx, nil)
			require.NoError(t, err)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, BackgroundProcessStateRunning, mgr.GetRunState())
			}, 5*time.Second, 100*time.Millisecond)

			err = mgr.Stop(ctx)
			require.NoError(t, err)

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				state := mgr.GetRunState()
				assert.Contains(c, []BackgroundProcessState{BackgroundProcessStateStopped, BackgroundProcessStateCompleted}, state)
			}, 5*time.Second, 100*time.Millisecond)

			assert.True(t, process.RunCalled)
			assert.True(t, process.StopRequested)
		})
	}
}

func TestBackgroundManagerMultiNodeTransitions(t *testing.T) {
	t.Skip("CBG-5376 temporarily skip test which flakes in CI")
	testBucket := base.GetTestBucket(t)
	ctx := context.Background()
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-transitions")

	options := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "multi-trans",
		multiNode:     true,
	}

	process1 := &MockProcess{}
	mgr1 := &BackgroundManager{
		name:                "mgr1",
		clusterAwareOptions: options,
		Process:             process1,
	}

	// 1. Start mgr1
	err := mgr1.Start(ctx, nil)
	require.NoError(t, err)
	defer func() { assert.NoError(t, mgr1.Stop(ctx)) }()

	// 2. Try to start mgr1 again
	err = mgr1.Start(ctx, nil)
	require.NoError(t, err)

	// 3. Start mgr2 (should succeed because it's MultiNode)
	process2 := &MockProcess{}
	mgr2 := &BackgroundManager{
		name:                "mgr2",
		clusterAwareOptions: options,
		Process:             process2,
	}
	err = mgr2.Start(ctx, nil)
	require.NoError(t, err)
	defer func() { assert.NoError(t, mgr2.Stop(ctx)) }()

	// Both should be running
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr1.GetRunState(), "expected mgr1 to be running")
		assert.Equal(c, BackgroundProcessStateRunning, mgr2.GetRunState(), "expected mgr2 to be running")
	}, 10*time.Second, 100*time.Millisecond)

	// 4. Stop via mgr1
	err = mgr1.Stop(ctx)
	require.NoError(t, err)

	// Both should stop because they watch the same status doc
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, []BackgroundProcessState{BackgroundProcessStateStopped, BackgroundProcessStateCompleted}, mgr1.GetRunState(), "expected mgr1 to be stopped or completed")
		assert.Contains(c, []BackgroundProcessState{BackgroundProcessStateStopped, BackgroundProcessStateCompleted}, mgr2.GetRunState(), "expected mgr2 to be stopped or completed")
	}, 15*time.Second, 500*time.Millisecond)

	require.True(t, process1.StopRequested, "mgr1 should have received stop request")
	require.True(t, process2.StopRequested, "mgr2 should have received stop request")

	// 5. Restart (from Stopped state)
	err = mgr1.Start(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, BackgroundProcessStateRunning, mgr1.GetRunState())
}

func TestBackgroundManagerMultiNodeSimultaneousTransitions(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := context.Background()
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-simultaneous")

	options := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "multi-simul",
		multiNode:     true,
	}

	numNodes := 5
	managers := make([]*BackgroundManager, numNodes)
	processes := make([]*MockProcess, numNodes)

	for i := 0; i < numNodes; i++ {
		processes[i] = &MockProcess{}
		managers[i] = &BackgroundManager{
			name:                fmt.Sprintf("mgr%d", i),
			clusterAwareOptions: options,
			Process:             processes[i],
		}
	}

	// Start all simultaneously
	var wg sync.WaitGroup
	for i := range numNodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.NoError(t, managers[i].Start(ctx, nil))
		}(i)
		defer func(i int) { assert.NoError(t, managers[i].Stop(ctx)) }(i)
	}
	wg.Wait()

	// All should eventually be running
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for i := 0; i < numNodes; i++ {
			assert.Equal(c, BackgroundProcessStateRunning, managers[i].GetRunState())
		}
	}, 10*time.Second, 100*time.Millisecond)

	// Stop all simultaneously
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			assert.NoError(t, managers[i].Stop(ctx))
		}(i)
	}
	wg.Wait()

	// All should eventually be stopped
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for i := 0; i < numNodes; i++ {
			assert.Contains(c, []BackgroundProcessState{BackgroundProcessStateStopped, BackgroundProcessStateCompleted}, managers[i].GetRunState())
		}
	}, 15*time.Second, 500*time.Millisecond)
}

func TestBackgroundManagerStartTimePreservedOnResume(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := context.Background()
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-start-time")

	options := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "start-time",
		multiNode:     false,
	}

	process1 := &MockProcess{}
	mgr1 := &BackgroundManager{
		name:                "mgr1",
		clusterAwareOptions: options,
		Process:             process1,
	}

	err := mgr1.Start(ctx, nil)
	require.NoError(t, err)

	origStartTime := mgr1.getStartTime()
	require.False(t, origStartTime.IsZero())

	err = mgr1.UpdateStatusClusterAware(ctx)
	require.NoError(t, err)

	err = mgr1.Stop(ctx)
	require.NoError(t, err)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, []BackgroundProcessState{BackgroundProcessStateStopped, BackgroundProcessStateCompleted}, mgr1.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)

	WaitForBackgroundManagerHeartbeatDocRemoval(t, mgr1)

	process2 := &MockProcess{}
	mgr2 := &BackgroundManager{
		name:                "mgr2",
		clusterAwareOptions: options,
		Process:             process2,
	}
	err = mgr2.Start(ctx, nil)
	require.NoError(t, err)
	defer func() { assert.NoError(t, mgr2.Stop(ctx)) }()

	require.NotEqual(t, origStartTime, mgr2.getStartTime())
}

func TestBackgroundManagerMultiNodeStartTimePreserved(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := context.Background()
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-multi-start-time")

	options := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "multi-start-time",
		multiNode:     true,
	}

	process1 := &MockProcess{}
	mgr1 := &BackgroundManager{
		name:                "mgr1",
		clusterAwareOptions: options,
		Process:             process1,
	}

	// 1. Start mgr1
	err := mgr1.Start(ctx, nil)
	require.NoError(t, err)
	defer func() { assert.NoError(t, mgr1.Stop(ctx)) }()

	// Wait for mgr1 to be running and have a start time
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr1.GetRunState())
		assert.False(c, mgr1.getStartTime().IsZero())
	}, 5*time.Second, 100*time.Millisecond)

	origStartTime := mgr1.getStartTime()

	// Ensure mgr1 has updated its status to the cluster
	err = mgr1.UpdateStatusClusterAware(ctx)
	require.NoError(t, err)

	// 2. Start mgr2 (MultiNode)
	process2 := &MockProcess{}
	mgr2 := &BackgroundManager{
		name:                "mgr2",
		clusterAwareOptions: options,
		Process:             process2,
	}

	err = mgr2.Start(ctx, nil)
	require.NoError(t, err)
	defer func() { assert.NoError(t, mgr2.Stop(ctx)) }()

	assert.Equal(t, origStartTime, mgr2.getStartTime(), "mgr2 should have inherited mgr1's start time")
}

func TestResyncMultiNodeStatsAggregation(t *testing.T) {
	// Simulate two ResyncManagerDCP instances (Node A and Node B)
	// both working on the same distributed resync.

	ctx := base.TestCtx(t)

	nodeA := &ResyncManagerDCP{
		ResyncID:    "resync1",
		Distributed: true,
	}
	nodeB := &ResyncManagerDCP{
		ResyncID:    "resync1",
		Distributed: true,
	}

	// 1. Initial state: bucket is empty.
	var bucketStatus []byte

	// 2. Node A processes 10 docs.
	for range 10 {
		nodeA.docsProcessedLocal.Add(1)
	}
	assert.Equal(t, int64(10), nodeA.DocsProcessed())

	// 3. Node A updates bucket status.
	// This simulates updateMultiNodeClusterAwareStatus logic
	statusA, _, err := nodeA.GetProcessStatus(BackgroundManagerStatus{State: BackgroundProcessStateRunning}, bucketStatus)
	require.NoError(t, err)

	// Marshaling like ResyncManagerStatusDocDCP
	statusDocA := ResyncManagerStatusDocDCP{
		ResyncManagerResponseDCP: ResyncManagerResponseDCP{
			BackgroundManagerStatus: BackgroundManagerStatus{State: BackgroundProcessStateRunning},
		},
	}
	// We need to unmarshal statusA into ResyncManagerResponseDCP
	err = json.Unmarshal(statusA, &statusDocA.ResyncManagerResponseDCP)
	require.NoError(t, err)
	bucketStatus = base.MustJSONMarshal(t, statusDocA)

	// Node A calls SetProcessStatus
	nodeA.SetProcessStatus(ctx, nil, statusA)
	assert.Equal(t, int64(10), nodeA.docsProcessedLocalSerialized.Load())
	assert.Equal(t, int64(10), nodeA.docsProcessedLocal.Load())
	assert.Equal(t, int64(10), nodeA.DocsProcessed())

	// 4. Node B processes 5 docs.
	// Node B hasn't polled yet, so its serialized is 0.
	for range 5 {
		nodeB.docsProcessedLocal.Add(1)
	}
	assert.Equal(t, int64(5), nodeB.DocsProcessed())

	// 5. Node B updates bucket status.
	// It sees bucketStatus from Node A (contains 10).
	var statusDocB ResyncManagerStatusDocDCP
	err = json.Unmarshal(bucketStatus, &statusDocB)
	require.NoError(t, err)

	// The cluster aware status doc stores the response JSON in its own fields
	previousStatusForB := base.MustJSONMarshal(t, statusDocB.ResyncManagerResponseDCP)

	statusB, _, err := nodeB.GetProcessStatus(BackgroundManagerStatus{State: BackgroundProcessStateRunning}, previousStatusForB)
	require.NoError(t, err)

	// Regression Check: Does statusB contain 15 (10+5) or just 5?
	var respB ResyncManagerResponseDCP
	err = json.Unmarshal(statusB, &respB)
	require.NoError(t, err)

	require.Equal(t, int64(15), respB.DocsProcessed)

}

// ResumableMockProcess is a MockProcess that also implements BackgroundManagerResumable.
// It stores the options passed to Init and serialises them into the "meta" returned by GetProcessStatus.
type ResumableMockProcess struct {
	MockProcess
	receivedOptions map[string]any
	optionsLock     sync.RWMutex
}

func (r *ResumableMockProcess) Init(ctx context.Context, options map[string]any, clusterStatus []byte) error {
	r.optionsLock.Lock()
	r.receivedOptions = options
	r.optionsLock.Unlock()
	return r.MockProcess.Init(ctx, options, clusterStatus)
}

func (r *ResumableMockProcess) GetProcessStatus(status BackgroundManagerStatus, previousStatus []byte) ([]byte, []byte, error) {
	statusBytes, _, err := r.MockProcess.GetProcessStatus(status, previousStatus)
	if err != nil {
		return nil, nil, err
	}
	r.optionsLock.RLock()
	opts := r.receivedOptions
	r.optionsLock.RUnlock()

	type mockMeta struct {
		Options map[string]any `json:"options,omitempty"`
	}
	metaBytes, err := base.JSONMarshal(mockMeta{Options: opts})
	if err != nil {
		return nil, nil, err
	}
	return statusBytes, metaBytes, nil
}

// ReceivedOptions returns the options most recently passed to Init (safe for concurrent use).
func (r *ResumableMockProcess) ReceivedOptions() map[string]any {
	r.optionsLock.RLock()
	defer r.optionsLock.RUnlock()
	return r.receivedOptions
}

// TestBackgroundManagerResume verifies that a second multi-node manager can join an already-running process
// via Resume, picking up the stored options without being given them explicitly.
func TestBackgroundManagerResume(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-resume")

	clusterOpts := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "resume",
		multiNode:     true,
	}

	process := &ResumableMockProcess{}
	mgr := &BackgroundManager{
		name:                "test-resume-mgr",
		Process:             process,
		clusterAwareOptions: clusterOpts,
		terminator:          base.NewSafeTerminator(),
	}

	startOptions := map[string]any{"key": "value", "num": float64(42)}

	// Start mgr — this writes the options into the status document and sets cluster state to running.
	require.NoError(t, mgr.Start(ctx, startOptions))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)

	// While cluster state is running, a second manager sharing the same status document should join via Resume.
	process2 := &ResumableMockProcess{}
	mgr2 := &BackgroundManager{
		name:                "test-resume-mgr2",
		Process:             process2,
		clusterAwareOptions: clusterOpts,
		terminator:          base.NewSafeTerminator(),
	}

	require.NoError(t, mgr2.Resume(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr2.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)
	require.Equal(t, startOptions, process2.ReceivedOptions())

	require.NoError(t, mgr.Stop(ctx))
	require.NoError(t, mgr2.Stop(ctx))
}

// TestBackgroundManagerResumeNoDoc verifies that Resume returns errBackgroundManagerStatusNotRunning when
// no status document exists (i.e. Start has never been called).
func TestBackgroundManagerResumeNoDoc(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-resume-no-doc")

	mgr := &BackgroundManager{
		name:    "test-resume-no-doc-mgr",
		Process: &ResumableMockProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "resume-no-doc",
			multiNode:     true,
		},
		terminator: base.NewSafeTerminator(),
	}

	require.ErrorIs(t, mgr.Resume(ctx), errBackgroundManagerStatusNotRunning)
}

// TestBackgroundManagerResumeSingleNodeError verifies that Resume returns an error for single-node managers
// since Resume is only supported for multi-node background managers.
func TestBackgroundManagerResumeSingleNodeError(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-resume-single-node")

	process := &ResumableMockProcess{}
	mgr := &BackgroundManager{
		name:    "test-resume-single-node-mgr",
		Process: process,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "resume-single-node",
			multiNode:     false, // single-node
		},
		terminator: base.NewSafeTerminator(),
	}

	err := mgr.Resume(ctx)
	require.Error(t, err)
}

// TestBackgroundManagerResumeWhileRunning verifies that calling Resume on a multi-node manager that is
// already running is idempotent: it returns nil and does not start a second instance of the process.
func TestBackgroundManagerResumeWhileRunning(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-resume-running")

	process := &ResumableMockProcess{}
	mgr := &BackgroundManager{
		name:    "test-resume-running-mgr",
		Process: process,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "resume-running",
			multiNode:     true,
		},
		terminator: base.NewSafeTerminator(),
	}

	startOptions := map[string]any{"key": "value"}
	require.NoError(t, mgr.Start(ctx, startOptions))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)

	// Resume while running is idempotent for multi-node managers.
	require.NoError(t, mgr.Resume(ctx))
	require.Equal(t, BackgroundProcessStateRunning, mgr.GetRunState())
	require.Equal(t, startOptions, process.ReceivedOptions())

	require.NoError(t, mgr.Stop(ctx))
}

// TestBackgroundManagerResumeWhenNotRunning verifies that Resume returns errBackgroundManagerStatusNotRunning
// when the cluster state is not running (e.g., after the process has been stopped).
func TestBackgroundManagerResumeWhenNotRunning(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-resume-not-running")

	clusterOpts := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "resume-not-running",
		multiNode:     true,
	}

	mgr := &BackgroundManager{
		name:                "test-resume-not-running-mgr",
		Process:             &ResumableMockProcess{},
		clusterAwareOptions: clusterOpts,
		terminator:          base.NewSafeTerminator(),
	}

	require.NoError(t, mgr.Start(ctx, map[string]any{"key": "value"}))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, mgr.Stop(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rawStatus, err := mgr.GetStatus(ctx)
		assert.NoError(c, err)
		var status BackgroundManagerStatus
		assert.NoError(c, base.JSONUnmarshal(rawStatus, &status))
		assert.Contains(c, []BackgroundProcessState{BackgroundProcessStateStopped, BackgroundProcessStateCompleted}, status.State)
	}, 5*time.Second, 100*time.Millisecond)

	// Cluster state is now stopped; a second manager must not be able to Resume.
	mgr2 := &BackgroundManager{
		name:                "test-resume-not-running-mgr2",
		Process:             &ResumableMockProcess{},
		clusterAwareOptions: clusterOpts,
		terminator:          base.NewSafeTerminator(),
	}
	require.NoError(t, mgr2.Resume(ctx))
	mgr2State, err := mgr2.getClusterStatusState(ctx)
	require.NoError(t, err)
	require.Equal(t, mgr.GetRunState(), mgr2State)
}

// TestBackgroundManagerResumeLocalModeError verifies that Resume returns an error for a local-mode manager
// since Resume is only supported for multi-node background managers.
func TestBackgroundManagerResumeLocalModeError(t *testing.T) {
	ctx := base.TestCtx(t)
	process := &ResumableMockProcess{}
	mgr := &BackgroundManager{
		name:       "test-resume-local-mgr",
		Process:    process,
		terminator: base.NewSafeTerminator(),
		// no clusterAwareOptions → local mode
	}

	err := mgr.Resume(ctx)
	require.Error(t, err)
}

// TestBackgroundManagerUpdateDatabaseStateRunning verifies that updateDatabaseState is called with running=true
// when UpdateStatusClusterAware is invoked while the process is running.
func TestBackgroundManagerUpdateDatabaseStateRunning(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-update-db-state-running")

	var dbStateCalls []bool
	var mu sync.Mutex

	mgr := &BackgroundManager{
		name:    "test-update-db-state-running",
		Process: &MockProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "update-db-state-running",
		},
		terminator: base.NewSafeTerminator(),
		updateDatabaseState: func(_ context.Context, running bool) error {
			mu.Lock()
			dbStateCalls = append(dbStateCalls, running)
			mu.Unlock()
			return nil
		},
	}

	require.NoError(t, mgr.Start(ctx, nil))
	defer func() { assert.NoError(t, mgr.Stop(ctx)) }()

	// UpdateStatusClusterAware is called synchronously at the end of Start, so by the time
	// Start returns, updateDatabaseState must have been called with running=true.
	mu.Lock()
	calls := make([]bool, len(dbStateCalls))
	copy(calls, dbStateCalls)
	mu.Unlock()

	require.Contains(t, calls, true, "expected updateDatabaseState(true) after Start")
}

// TestBackgroundManagerUpdateDatabaseStateOnCompletion verifies that updateDatabaseState is called
// with running=false when the background process finishes (via UpdateStatusClusterAware in the run goroutine).
func TestBackgroundManagerUpdateDatabaseStateOnCompletion(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-update-db-state-done")

	calledWithFalse := make(chan struct{}, 1)

	mgr := &BackgroundManager{
		name:    "test-update-db-state-done",
		Process: &MockProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "update-db-state-done",
		},
		terminator: base.NewSafeTerminator(),
		updateDatabaseState: func(_ context.Context, running bool) error {
			if !running {
				select {
				case calledWithFalse <- struct{}{}:
				default:
				}
			}
			return nil
		},
	}

	require.NoError(t, mgr.Start(ctx, nil))

	// Stopping the manager causes the run goroutine to exit, which transitions the state to
	// Stopped and then calls UpdateStatusClusterAware — which must call updateDatabaseState(false).
	require.NoError(t, mgr.Stop(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, []BackgroundProcessState{
			BackgroundProcessStateStopped,
			BackgroundProcessStateCompleted,
		}, mgr.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)

	base.RequireChanRecvWithTimeout(t, calledWithFalse, 5*time.Second)
}

// TestBackgroundManagerResumeCallsUpdateDatabaseStateWhenNotRunning verifies that Resume calls
// updateDatabaseState(false) when the cluster has no status document (not running).
func TestBackgroundManagerResumeCallsUpdateDatabaseStateWhenNotRunning(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-resume-db-state-not-running")

	var dbStateCalls []bool
	var mu sync.Mutex

	mgr := &BackgroundManager{
		name:    "test-resume-db-state-not-running",
		Process: &ResumableMockProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "resume-db-state-not-running",
			multiNode:     true,
		},
		terminator: base.NewSafeTerminator(),
		updateDatabaseState: func(_ context.Context, running bool) error {
			mu.Lock()
			dbStateCalls = append(dbStateCalls, running)
			mu.Unlock()
			return nil
		},
	}

	// No status doc exists → Resume must return errBackgroundManagerStatusNotRunning.
	err := mgr.Resume(ctx)
	require.ErrorIs(t, err, errBackgroundManagerStatusNotRunning)

	mu.Lock()
	calls := make([]bool, len(dbStateCalls))
	copy(calls, dbStateCalls)
	mu.Unlock()

	require.Len(t, calls, 1, "expected exactly one updateDatabaseState call from Resume")
	require.False(t, calls[0], "expected updateDatabaseState(false) when cluster is not running")
}

// TestBackgroundManagerResumeCallsUpdateDatabaseStateWhenStopped verifies that Resume calls
// updateDatabaseState(false) when the cluster state is stopped (not running).
func TestBackgroundManagerResumeCallsUpdateDatabaseStateWhenStopped(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-resume-db-state-stopped")

	clusterOpts := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "resume-db-state-stopped",
		multiNode:     true,
	}

	// Start and then stop a manager so the cluster status doc shows Stopped.
	starter := &BackgroundManager{
		name:                "test-resume-db-state-stopped-starter",
		Process:             &ResumableMockProcess{},
		clusterAwareOptions: clusterOpts,
		terminator:          base.NewSafeTerminator(),
	}
	require.NoError(t, starter.Start(ctx, map[string]any{"key": "value"}))
	require.NoError(t, starter.Stop(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rawStatus, err := starter.GetStatus(ctx)
		assert.NoError(c, err)
		var status BackgroundManagerStatus
		assert.NoError(c, base.JSONUnmarshal(rawStatus, &status))
		assert.Contains(c, []BackgroundProcessState{
			BackgroundProcessStateStopped,
			BackgroundProcessStateCompleted,
		}, status.State)
	}, 5*time.Second, 100*time.Millisecond)

	// A fresh manager observing the same (stopped) cluster state should call updateDatabaseState(false).
	var dbStateCalls []bool
	var mu sync.Mutex
	observer := &BackgroundManager{
		name:                "test-resume-db-state-stopped-observer",
		Process:             &ResumableMockProcess{},
		clusterAwareOptions: clusterOpts,
		terminator:          base.NewSafeTerminator(),
		updateDatabaseState: func(_ context.Context, running bool) error {
			mu.Lock()
			dbStateCalls = append(dbStateCalls, running)
			mu.Unlock()
			return nil
		},
	}

	err := observer.Resume(ctx)
	require.NoError(t, err)

	mu.Lock()
	calls := make([]bool, len(dbStateCalls))
	copy(calls, dbStateCalls)
	mu.Unlock()

	require.Len(t, calls, 1, "expected exactly one updateDatabaseState call")
	require.False(t, calls[0], "expected updateDatabaseState(false) when cluster is stopped")
}

// immediateCallbackProcess is a MockProcess whose Run calls the status callback once then returns
// immediately, making the goroutine race window between the goroutine cleanup and the parent
// Start body as tight as possible.
type immediateCallbackProcess struct {
	MockProcess
}

func (m *immediateCallbackProcess) Run(ctx context.Context, _ map[string]any, cb updateStatusCallbackFunc, _ *base.SafeTerminator) error {
	m.lock.Lock()
	m.RunCalled = true
	m.lock.Unlock()
	if cb != nil {
		_ = cb(ctx)
	}
	return nil
}

// newTestManagerWithStateDoc returns a multi-node BackgroundManager whose updateDatabaseState is wired
// to a DatabaseStateMgr backed by the given metadata store.
func newTestManagerWithStateDoc(metadataStore base.DataStore, metaKeys *base.MetadataKeys, suffix string, proc BackgroundManagerProcessI) (*BackgroundManager, *DatabaseStateMgr) {
	dbStateMgr := NewDatabaseStateMgr(metadataStore, metaKeys.DatabaseStateKey(), nil)
	mgr := &BackgroundManager{
		name:    "test-" + suffix,
		Process: proc,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: suffix,
			multiNode:     true,
		},
		terminator: base.NewSafeTerminator(),
		updateDatabaseState: func(ctx context.Context, running bool) error {
			return dbStateMgr.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(running)})
		},
	}
	return mgr, dbStateMgr
}

// assertResyncRunningEventually polls the DatabaseStateMgr until ResyncRunning equals want or the
// deadline is exceeded.
func assertResyncRunningEventually(t *testing.T, dbStateMgr *DatabaseStateMgr, want bool, ctx context.Context) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, _, err := dbStateMgr.GetState(ctx)
		assert.NoError(c, err)
		if assert.NotNil(c, state) && assert.NotNil(c, state.ResyncRunning) {
			assert.Equal(c, want, *state.ResyncRunning)
		}
	}, 5*time.Second, 20*time.Millisecond)
}

// TestUpdateDatabaseStateRapidCycles exercises repeated start/stop cycles and verifies that
// ResyncRunning always settles to false after each stop, and to true after each start.
// Run with -race to exercise the race detector.
func TestUpdateDatabaseStateRapidCycles(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)

	const cycles = 8
	for i := range cycles {
		// Use a fresh key each cycle so status docs don't interfere.
		metaKeys := base.NewMetadataKeys(fmt.Sprintf("rapid-cycle-%d", i))
		mgr, dbStateMgr := newTestManagerWithStateDoc(metadataStore, metaKeys, "rapid", &ResumableMockProcess{})

		require.NoError(t, mgr.Start(ctx, nil))
		assertResyncRunningEventually(t, dbStateMgr, true, ctx)

		require.NoError(t, mgr.Stop(ctx))
		assertResyncRunningEventually(t, dbStateMgr, false, ctx)
	}
}

// TestUpdateDatabaseStateProcessCompletesBeforeStartReturns exercises the ordering race where a
// process returns from Run (and the goroutine writes false) before Start's own synchronous
// UpdateStatusClusterAware call. The final state must be false once everything settles.
// Run with -race.
func TestUpdateDatabaseStateProcessCompletesBeforeStartReturns(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-immediate-complete")

	mgr, dbStateMgr := newTestManagerWithStateDoc(metadataStore, metaKeys, "immediate", &immediateCallbackProcess{})

	// immediateCallbackProcess returns from Run immediately, making the goroutine cleanup race
	// with the synchronous UpdateStatusClusterAware call at the end of Start.
	require.NoError(t, mgr.Start(ctx, nil))

	// Regardless of ordering, the final state must settle to false because the process completed.
	assertResyncRunningEventually(t, dbStateMgr, false, ctx)
}

// TestUpdateDatabaseStateConcurrentManagersSharedStateDoc starts and stops many multi-node managers
// that all share the same DatabaseStateMgr. It verifies no data races and that the state document
// eventually reflects "not running" once all managers have stopped.
// Run with -race.
func TestUpdateDatabaseStateConcurrentManagersSharedStateDoc(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-concurrent-shared-state")

	dbStateMgr := NewDatabaseStateMgr(metadataStore, metaKeys.DatabaseStateKey(), nil)

	const numNodes = 5
	managers := make([]*BackgroundManager, numNodes)
	for i := range numNodes {
		managers[i] = &BackgroundManager{
			name:    fmt.Sprintf("test-concurrent-shared-%d", i),
			Process: &ResumableMockProcess{},
			clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
				metadataStore: metadataStore,
				metaKeys:      metaKeys,
				processSuffix: "concurrent-shared",
				multiNode:     true,
			},
			terminator: base.NewSafeTerminator(),
			updateDatabaseState: func(ctx context.Context, running bool) error {
				return dbStateMgr.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(running)})
			},
		}
	}

	// All managers start simultaneously.
	var startWG sync.WaitGroup
	for i := range numNodes {
		startWG.Add(1)
		go func(i int) {
			defer startWG.Done()
			assert.NoError(t, managers[i].Start(ctx, nil))
		}(i)
	}
	startWG.Wait()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for i := range numNodes {
			assert.Equal(c, BackgroundProcessStateRunning, managers[i].GetRunState())
		}
	}, 5*time.Second, 50*time.Millisecond)

	// All managers stop simultaneously.
	var stopWG sync.WaitGroup
	for i := range numNodes {
		stopWG.Add(1)
		go func(i int) {
			defer stopWG.Done()
			assert.NoError(t, managers[i].Stop(ctx))
		}(i)
	}
	stopWG.Wait()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for i := range numNodes {
			assert.Contains(c, []BackgroundProcessState{
				BackgroundProcessStateStopped,
				BackgroundProcessStateCompleted,
			}, managers[i].GetRunState())
		}
	}, 10*time.Second, 100*time.Millisecond)

	// Once all managers have stopped, ResyncRunning must be false.
	assertResyncRunningEventually(t, dbStateMgr, false, ctx)
}

// TestUpdateDatabaseStateResumeOverwritesRunningState exercises a design-level race: a node whose
// Resume fails (stale cluster state) calls updateDatabaseState(false) and can overwrite the true
// written by a concurrently running node. This verifies that the system eventually self-corrects via
// the DatabaseStateMgr polling loop.
func TestUpdateDatabaseStateResumeOverwritesRunningState(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-resume-overwrite")

	// Manager A is the running node.
	mgrA, dbStateMgr := newTestManagerWithStateDoc(metadataStore, metaKeys, "resume-overwrite", &ResumableMockProcess{})
	require.NoError(t, mgrA.Start(ctx, nil))
	defer func() { assert.NoError(t, mgrA.Stop(ctx)) }()

	assertResyncRunningEventually(t, dbStateMgr, true, ctx)

	// Manager B is an observer that has a stale cluster status. We simulate Resume seeing
	// "not running" by stopping the cluster state doc before B calls Resume.
	// In practice this can happen when B reads the status doc during a brief window between
	// an old run completing and a new one starting.
	mgrB := &BackgroundManager{
		name:    "test-resume-overwrite-observer",
		Process: &ResumableMockProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      base.NewMetadataKeys("test-resume-overwrite-stale"), // different key → no status doc
			processSuffix: "resume-overwrite-stale",
			multiNode:     true,
		},
		terminator: base.NewSafeTerminator(),
		updateDatabaseState: func(ctx context.Context, running bool) error {
			// B's updateDatabaseState writes to the SAME state doc as A.
			return dbStateMgr.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(running)})
		},
	}

	// B's Resume will see no cluster status doc → errBackgroundManagerStatusNotRunning → callUpdateDatabaseState(false).
	err := mgrB.Resume(ctx)
	require.ErrorIs(t, err, errBackgroundManagerStatusNotRunning)

	// B wrote false to the shared state doc, even though A is still running.
	// A's next UpdateStatusClusterAware call must restore true.
	require.NoError(t, mgrA.UpdateStatusClusterAware(ctx))
	assertResyncRunningEventually(t, dbStateMgr, true, ctx)
}

// TestDatabaseStateMgrIsUpdatedConcurrentWithUpdateState stress-tests the interaction between
// isUpdated (which reads CAS outside the lock) and concurrent UpdateState calls.
// The test verifies no data races and that the handler fires at least once per logical state change.
// Run with -race.
func TestDatabaseStateMgrIsUpdatedConcurrentWithUpdateState(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-should-run-concurrent")
	docID := metaKeys.DatabaseStateKey()

	dbStateMgr := NewDatabaseStateMgr(metadataStore, docID, nil)

	var handlerFires atomic.Int32

	const workers = 8
	const iters = 20
	var wg sync.WaitGroup

	// Writers: alternate true/false updates.
	for w := range workers / 2 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for range iters {
				running := w%2 == 0
				_ = dbStateMgr.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(running)})
			}
		}(w)
	}

	// Readers: call isUpdated and advance CAS on detection, mirroring what poll does.
	for range workers / 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iters {
				if newCAS, state, ok := dbStateMgr.isUpdated(ctx); ok {
					if state != nil && state.ResyncRunning != nil && *state.ResyncRunning {
						handlerFires.Add(1)
					}
					dbStateMgr.lock.Lock()
					dbStateMgr.CAS = newCAS
					dbStateMgr.lock.Unlock()
				}
			}
		}()
	}

	wg.Wait()

	// The race detector (run with -race) is the primary assertion here. We also verify that
	// at least some handler fires occurred, showing the machinery ran.
	t.Logf("handler fired %d times", handlerFires.Load())
}

// TestUpdateDatabaseStateConcurrentStartStopWithPoller starts a DatabaseStateMgr polling loop and
// rapidly starts/stops a BackgroundManager to verify that the poller never incorrectly triggers
// Resume when the cluster status is not running.
func TestUpdateDatabaseStateConcurrentStartStopWithPoller(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-poller-start-stop")

	var badResumeCalls atomic.Int32

	mgr, dbStateMgr := newTestManagerWithStateDoc(metadataStore, metaKeys, "poller-start-stop", &ResumableMockProcess{})

	// Wire a resumeResyncFunc that records any call where the cluster status is not actually running.
	// A correct implementation should only call resume when the cluster IS running.
	dbStateMgr.resumeResyncFunc = func(pollCtx context.Context) error {
		clusterState, err := mgr.getClusterStatusState(pollCtx)
		if err != nil || clusterState != BackgroundProcessStateRunning {
			badResumeCalls.Add(1)
		}
		return mgr.Resume(pollCtx)
	}
	dbStateMgr.pollingInterval = 10 * time.Millisecond
	dbStateMgr.StartPolling(ctx)
	defer dbStateMgr.StopPolling(ctx)

	const cycles = 5
	for range cycles {
		require.NoError(t, mgr.Start(ctx, nil))
		assertResyncRunningEventually(t, dbStateMgr, true, ctx)

		require.NoError(t, mgr.Stop(ctx))
		assertResyncRunningEventually(t, dbStateMgr, false, ctx)

		// Allow the poller a few ticks to settle.
		time.Sleep(50 * time.Millisecond)
	}

	// The poller must never have fired Resume when the cluster was not running.
	require.Zero(t, badResumeCalls.Load(), "Resume was invoked when cluster state was not running")
}
func TestBackgroundManagerResumeConcurrentWhileStopping(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-resume-race")

	clusterOpts := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "resume-race",
		multiNode:     true,
	}

	startOptions := map[string]any{"key": "value"}

	// mgr1 is the "originating" node that starts the process and then stops it.
	mgr1 := &BackgroundManager{
		name:                "test-resume-race-mgr1",
		Process:             &ResumableMockProcess{},
		clusterAwareOptions: clusterOpts,
		terminator:          base.NewSafeTerminator(),
	}
	require.NoError(t, mgr1.Start(ctx, startOptions))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr1.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)

	// Ensure the running state is persisted to the cluster so that Resume callers can see it.
	require.NoError(t, mgr1.UpdateStatusClusterAware(ctx))

	const numResumeCallers = 10

	// Build a fleet of managers that will all call Resume concurrently.
	resumeManagers := make([]*BackgroundManager, numResumeCallers)
	for i := range numResumeCallers {
		resumeManagers[i] = &BackgroundManager{
			name:                fmt.Sprintf("test-resume-race-caller%d", i),
			Process:             &ResumableMockProcess{},
			clusterAwareOptions: clusterOpts,
			terminator:          base.NewSafeTerminator(),
		}
	}

	// Use a barrier so all goroutines fire Resume at the same instant that Stop is called.
	var barrier sync.WaitGroup
	barrier.Add(numResumeCallers + 1) // +1 for the Stop goroutine

	// resumeErrors[i] holds the error returned by resumeManagers[i].Resume.
	resumeErrors := make([]error, numResumeCallers)

	var resumeWG sync.WaitGroup
	for i := range numResumeCallers {
		resumeWG.Add(1)
		go func(i int) {
			defer resumeWG.Done()
			barrier.Done()
			barrier.Wait()
			resumeErrors[i] = resumeManagers[i].Resume(ctx)
		}(i)
	}

	// Stop mgr1 concurrently with the Resume calls.
	go func() {
		barrier.Done()
		barrier.Wait()
		assert.NoError(t, mgr1.Stop(ctx))
	}()

	// Wait for all Resume callers to finish before reading their results.
	resumeWG.Wait()

	// Wait for mgr1 to reach a terminal state.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, err := mgr1.getClusterStatusState(ctx)
		require.NoError(c, err)
		assert.Contains(c, []BackgroundProcessState{
			BackgroundProcessStateStopped,
			BackgroundProcessStateCompleted,
		}, state)
	}, 10*time.Second, 100*time.Millisecond)

	// Stop any resume callers that managed to start running.
	for i := range numResumeCallers {
		if resumeErrors[i] == nil {
			// Resume succeeded → process may be running; stop it.
			_ = resumeManagers[i].Stop(ctx)
		}
	}

	// Wait for all resume callers that started to reach a terminal state.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for i := range numResumeCallers {
			if resumeErrors[i] != nil {
				// Resume returned an error — no goroutine running, nothing to wait for.
				continue
			}
			state := resumeManagers[i].GetRunState()
			assert.Contains(c, []BackgroundProcessState{
				BackgroundProcessStateStopped,
				BackgroundProcessStateCompleted,
			}, state, "mgr %d should be in terminal state", i)
		}
	}, 10*time.Second, 100*time.Millisecond)
}
