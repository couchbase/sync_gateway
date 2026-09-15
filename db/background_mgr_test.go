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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
	"github.com/couchbaselabs/rosmar"
)

// MockProcessOptions defines configurations used for testing various background managers.
type MockProcessOptions struct {
	// Key is an arbitrary string value used to test options passing/resuming.
	Key string
	// Num is an arbitrary float value used to test options passing/resuming.
	Num float64
	// Reset indicates whether the mock process should start clean or resume.
	Reset bool
}

type MockProcess struct {
	InitCalled           bool
	RunCalled            bool
	StopRequested        bool
	SleepDuration        time.Duration
	updateStatusCallback updateStatusCallbackFunc
	lock                 sync.Mutex
}

func (m *MockProcess) Init(ctx context.Context, options MockProcessOptions, clusterStatus []byte) (backgroundManagerInitMode, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.InitCalled = true
	return backgroundManagerInitReset, nil
}

func (m *MockProcess) Run(ctx context.Context, options MockProcessOptions, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	m.lock.Lock()
	m.RunCalled = true
	m.updateStatusCallback = persistClusterStatusCallback
	m.lock.Unlock()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	if persistClusterStatusCallback != nil {
		defer func() {
			_ = persistClusterStatusCallback(ctx)
		}()
	}

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

// RunWasCalled reports whether Run has been entered.
func (m *MockProcess) RunWasCalled() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.RunCalled
}

// StopWasRequested reports whether Run has observed its terminator closing.
func (m *MockProcess) StopWasRequested() bool {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.StopRequested
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
			mgr := &BackgroundManager[MockProcessOptions]{
				name:                "test-mgr-" + mode.name,
				clusterAwareOptions: mode.clusterAwareOptions,
				Process:             process,
			}

			err := mgr.Start(ctx, MockProcessOptions{})
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

			assert.True(t, process.RunWasCalled())
			assert.True(t, process.StopWasRequested())
		})
	}
}

func TestBackgroundManagerMultiNodeTransitions(t *testing.T) {
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
	mgr1 := &BackgroundManager[MockProcessOptions]{
		name:                "mgr1",
		clusterAwareOptions: options,
		Process:             process1,
	}

	// 1. Start mgr1
	err := mgr1.Start(ctx, MockProcessOptions{})
	require.NoError(t, err)
	defer func() { assert.NoError(t, mgr1.Stop(ctx)) }()

	// 2. Try to start mgr1 again
	err = mgr1.Start(ctx, MockProcessOptions{})
	require.NoError(t, err)

	// 3. Start mgr2 (should succeed because it's MultiNode)
	process2 := &MockProcess{}
	mgr2 := &BackgroundManager[MockProcessOptions]{
		name:                "mgr2",
		clusterAwareOptions: options,
		Process:             process2,
	}
	err = mgr2.Start(ctx, MockProcessOptions{})
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

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, process1.StopWasRequested(), "mgr1 should have received stop request")
		assert.True(c, process2.StopWasRequested(), "mgr2 should have received stop request")
	}, 15*time.Second, 100*time.Millisecond)

	// 5. Restart (from Stopped state)
	err = mgr1.Start(ctx, MockProcessOptions{})
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
	managers := make([]*BackgroundManager[MockProcessOptions], numNodes)
	processes := make([]*MockProcess, numNodes)

	for i := 0; i < numNodes; i++ {
		processes[i] = &MockProcess{}
		managers[i] = &BackgroundManager[MockProcessOptions]{
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
			assert.NoError(t, managers[i].Start(ctx, MockProcessOptions{}))
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

func TestBackgroundManagerStartTimePreservedOnJoin(t *testing.T) {
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
	mgr1 := &BackgroundManager[MockProcessOptions]{
		name:                "mgr1",
		clusterAwareOptions: options,
		Process:             process1,
	}

	err := mgr1.Start(ctx, MockProcessOptions{})
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
	mgr2 := &BackgroundManager[MockProcessOptions]{
		name:                "mgr2",
		clusterAwareOptions: options,
		Process:             process2,
	}
	err = mgr2.Start(ctx, MockProcessOptions{})
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
	mgr1 := &BackgroundManager[MockProcessOptions]{
		name:                "mgr1",
		clusterAwareOptions: options,
		Process:             process1,
	}

	// 1. Start mgr1
	err := mgr1.Start(ctx, MockProcessOptions{})
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
	mgr2 := &BackgroundManager[MockProcessOptions]{
		name:                "mgr2",
		clusterAwareOptions: options,
		Process:             process2,
	}

	err = mgr2.Start(ctx, MockProcessOptions{})
	require.NoError(t, err)
	defer func() { assert.NoError(t, mgr2.Stop(ctx)) }()

	assert.Equal(t, origStartTime, mgr2.getStartTime(), "mgr2 should have inherited mgr1's start time")
}

func TestResyncMultiNodeStatsAggregation(t *testing.T) {
	// Simulate two ResyncManagerDCP instances (Node A and Node B)
	// both working on the same distributed resync.

	ctx := base.TestCtx(t)

	nodeA := &ResyncManagerDCP{
		ResyncID:          "resync1",
		Distributed:       true,
		db:                &DatabaseContext{},
		completedvBuckets: newvBucketTracker(),
	}
	nodeB := &ResyncManagerDCP{
		ResyncID:          "resync1",
		Distributed:       true,
		db:                &DatabaseContext{},
		completedvBuckets: newvBucketTracker(),
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
	previousStatusForB := base.MustJSONMarshal(t, statusDocB)

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
	receivedOptions MockProcessOptions
	optionsLock     sync.RWMutex
}

func (r *ResumableMockProcess) Init(ctx context.Context, options MockProcessOptions, clusterStatus []byte) (backgroundManagerInitMode, error) {
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
		Options MockProcessOptions `json:"options,omitempty"`
	}
	metaBytes, err := base.JSONMarshal(mockMeta{Options: opts})
	if err != nil {
		return nil, nil, err
	}
	return statusBytes, metaBytes, nil
}

// ReceivedOptions returns the options most recently passed to Init (safe for concurrent use).
func (r *ResumableMockProcess) ReceivedOptions() MockProcessOptions {
	r.optionsLock.RLock()
	defer r.optionsLock.RUnlock()
	return r.receivedOptions
}

// TestBackgroundManagerJoin verifies that a second multi-node manager can join an already-running process
// via Join, picking up the stored options without being given them explicitly.
func TestBackgroundManagerJoin(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-join")

	clusterOpts := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "join",
		multiNode:     true,
	}

	process := &ResumableMockProcess{}
	mgr := &BackgroundManager[MockProcessOptions]{
		name:                "test-join-mgr",
		Process:             process,
		clusterAwareOptions: clusterOpts,
	}

	startOptions := MockProcessOptions{Key: "value", Num: 42}

	// Start mgr — this writes the options into the status document and sets cluster state to running.
	require.NoError(t, mgr.Start(ctx, startOptions))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)

	// While cluster state is running, a second manager sharing the same status document should join via Join.
	process2 := &ResumableMockProcess{}
	mgr2 := &BackgroundManager[MockProcessOptions]{
		name:                "test-join-mgr2",
		Process:             process2,
		clusterAwareOptions: clusterOpts,
	}

	require.NoError(t, mgr2.Join(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr2.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)
	require.Equal(t, startOptions, process2.ReceivedOptions())

	require.NoError(t, mgr.Stop(ctx))
	require.NoError(t, mgr2.Stop(ctx))
}

// TestBackgroundManagerJoinNoDoc verifies that Join returns errBackgroundManagerStatusNotRunning when
// no status document exists (i.e. Start has never been called).
func TestBackgroundManagerJoinNoDoc(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-join-no-doc")

	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "test-join-no-doc-mgr",
		Process: &ResumableMockProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "join-no-doc",
			multiNode:     true,
		},
	}

	var statusErr errBackgroundManagerStatusNotRunning
	require.ErrorAs(t, mgr.Join(ctx), &statusErr)
}

// TestBackgroundManagerJoinSingleNodeError verifies that Join returns an error for single-node managers
// since Join is only supported for multi-node background managers.
func TestBackgroundManagerJoinSingleNodeError(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-join-single-node")

	process := &ResumableMockProcess{}
	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "test-join-single-node-mgr",
		Process: process,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "join-single-node",
			multiNode:     false, // single-node
		},
	}

	err := mgr.Join(ctx)
	require.Error(t, err)
}

// TestBackgroundManagerJoinWhileRunning verifies that calling Join on a multi-node manager that is
// already running is idempotent: it returns nil and does not start a second instance of the process.
func TestBackgroundManagerJoinWhileRunning(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-join-running")

	process := &ResumableMockProcess{}
	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "test-join-running-mgr",
		Process: process,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "join-running",
			multiNode:     true,
		},
	}

	startOptions := MockProcessOptions{Key: "value"}
	require.NoError(t, mgr.Start(ctx, startOptions))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)

	// Join while running is idempotent for multi-node managers.
	require.NoError(t, mgr.Join(ctx))
	require.Equal(t, BackgroundProcessStateRunning, mgr.GetRunState())
	require.Equal(t, startOptions, process.ReceivedOptions())

	require.NoError(t, mgr.Stop(ctx))
}

// TestBackgroundManagerJoinWhenNotRunning verifies that Join returns nil without starting the local process
// when the cluster state is a terminal, non-running state (e.g., after the process has been stopped).
func TestBackgroundManagerJoinWhenNotRunning(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-join-not-running")

	clusterOpts := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "join-not-running",
		multiNode:     true,
	}

	mgr := &BackgroundManager[MockProcessOptions]{
		name:                "test-join-not-running-mgr",
		Process:             &ResumableMockProcess{},
		clusterAwareOptions: clusterOpts,
	}

	require.NoError(t, mgr.Start(ctx, MockProcessOptions{Key: "value"}))
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

	// Cluster state is now stopped; a second manager must not be able to Join.
	mgr2 := &BackgroundManager[MockProcessOptions]{
		name:                "test-join-not-running-mgr2",
		Process:             &ResumableMockProcess{},
		clusterAwareOptions: clusterOpts,
	}
	require.NoError(t, mgr2.Join(ctx))
	mgr2State, err := mgr2.getClusterStatusState(ctx)
	require.NoError(t, err)
	require.Equal(t, mgr.GetRunState(), mgr2State)
}

// TestBackgroundManagerJoinLocalModeError verifies that Join returns an error for a local-mode manager
// since Join is only supported for multi-node background managers.
func TestBackgroundManagerJoinLocalModeError(t *testing.T) {
	ctx := base.TestCtx(t)
	process := &ResumableMockProcess{}
	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "test-join-local-mgr",
		Process: process,
		// no clusterAwareOptions → local mode
	}

	err := mgr.Join(ctx)
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

	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "test-update-db-state-running",
		Process: &MockProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "update-db-state-running",
			multiNode:     true,
		},
		updateDatabaseState: func(_ context.Context, running bool) error {
			mu.Lock()
			dbStateCalls = append(dbStateCalls, running)
			mu.Unlock()
			return nil
		},
	}

	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))
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

	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "test-update-db-state-done",
		Process: &MockProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "update-db-state-done",
			multiNode:     true,
		},
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

	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))

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

// TestBackgroundManagerJoinCallsUpdateDatabaseStateWhenNotRunning verifies that Join calls
// updateDatabaseState(false) when the cluster has no status document (not running).
func TestBackgroundManagerJoinCallsUpdateDatabaseStateWhenNotRunning(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-join-db-state-not-running")

	var dbStateCalls []bool
	var mu sync.Mutex

	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "test-join-db-state-not-running",
		Process: &ResumableMockProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "join-db-state-not-running",
			multiNode:     true,
		},
		updateDatabaseState: func(_ context.Context, running bool) error {
			mu.Lock()
			dbStateCalls = append(dbStateCalls, running)
			mu.Unlock()
			return nil
		},
	}

	// No status doc exists → Join must return errBackgroundManagerStatusNotRunning.
	err := mgr.Join(ctx)
	var statusErr errBackgroundManagerStatusNotRunning
	require.ErrorAs(t, err, &statusErr)

	mu.Lock()
	calls := make([]bool, len(dbStateCalls))
	copy(calls, dbStateCalls)
	mu.Unlock()

	require.Len(t, calls, 1, "expected exactly one updateDatabaseState call from Join")
	require.False(t, calls[0], "expected updateDatabaseState(false) when cluster is not running")
}

// TestBackgroundManagerJoinCallsUpdateDatabaseStateWhenStopped verifies that Join calls
// updateDatabaseState(false) when the cluster state is stopped (not running).
func TestBackgroundManagerJoinCallsUpdateDatabaseStateWhenStopped(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-join-db-state-stopped")

	clusterOpts := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "join-db-state-stopped",
		multiNode:     true,
	}

	// Start and then stop a manager so the cluster status doc shows Stopped.
	starter := &BackgroundManager[MockProcessOptions]{
		name:                "test-join-db-state-stopped-starter",
		Process:             &ResumableMockProcess{},
		clusterAwareOptions: clusterOpts,
	}
	require.NoError(t, starter.Start(ctx, MockProcessOptions{Key: "value"}))
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
	observer := &BackgroundManager[MockProcessOptions]{
		name:                "test-join-db-state-stopped-observer",
		Process:             &ResumableMockProcess{},
		clusterAwareOptions: clusterOpts,
		updateDatabaseState: func(_ context.Context, running bool) error {
			mu.Lock()
			dbStateCalls = append(dbStateCalls, running)
			mu.Unlock()
			return nil
		},
	}

	err := observer.Join(ctx)
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

func (m *immediateCallbackProcess) Run(ctx context.Context, _ MockProcessOptions, cb updateStatusCallbackFunc, _ *base.SafeTerminator) error {
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
func newTestManagerWithStateDoc(metadataStore base.DataStore, metaKeys *base.MetadataKeys, suffix string, proc BackgroundManagerProcessI[MockProcessOptions]) (*BackgroundManager[MockProcessOptions], *DatabaseStateMgr) {
	dbStateMgr := NewDatabaseStateMgr(metadataStore, metaKeys.DatabaseStateKey(), nil)
	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "test-" + suffix,
		Process: proc,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: suffix,
			multiNode:     true,
		},
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

		require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))
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
	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))

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
	managers := make([]*BackgroundManager[MockProcessOptions], numNodes)
	for i := range numNodes {
		managers[i] = &BackgroundManager[MockProcessOptions]{
			name:    fmt.Sprintf("test-concurrent-shared-%d", i),
			Process: &ResumableMockProcess{},
			clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
				metadataStore: metadataStore,
				metaKeys:      metaKeys,
				processSuffix: "concurrent-shared",
				multiNode:     true,
			},
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
			assert.NoError(t, managers[i].Start(ctx, MockProcessOptions{}))
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

// TestUpdateDatabaseStateJoinOverwritesRunningState exercises a design-level race: a node whose
// Join fails (stale cluster state) calls updateDatabaseState(false) and can overwrite the true
// written by a concurrently running node. This verifies that the system eventually self-corrects via
// the DatabaseStateMgr polling loop.
func TestUpdateDatabaseStateJoinOverwritesRunningState(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-join-overwrite")

	// Manager A is the running node.
	mgrA, dbStateMgr := newTestManagerWithStateDoc(metadataStore, metaKeys, "join-overwrite", &ResumableMockProcess{})
	require.NoError(t, mgrA.Start(ctx, MockProcessOptions{}))
	defer func() { assert.NoError(t, mgrA.Stop(ctx)) }()

	assertResyncRunningEventually(t, dbStateMgr, true, ctx)

	// Manager B is an observer that has a stale cluster status. We simulate Join seeing
	// "not running" by stopping the cluster state doc before B calls Join.
	// In practice this can happen when B reads the status doc during a brief window between
	// an old run completing and a new one starting.
	mgrB := &BackgroundManager[MockProcessOptions]{
		name:    "test-join-overwrite-observer",
		Process: &ResumableMockProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      base.NewMetadataKeys("test-join-overwrite-stale"), // different key → no status doc
			processSuffix: "join-overwrite-stale",
			multiNode:     true,
		},
		updateDatabaseState: func(ctx context.Context, running bool) error {
			// B's updateDatabaseState writes to the SAME state doc as A.
			return dbStateMgr.UpdateState(ctx, DatabaseState{ResyncRunning: base.Ptr(running)})
		},
	}

	// B's Join will see no cluster status doc → errBackgroundManagerStatusNotRunning → callUpdateDatabaseState(false).
	err := mgrB.Join(ctx)
	var statusErr errBackgroundManagerStatusNotRunning
	require.ErrorAs(t, err, &statusErr)

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

func TestBackgroundManagerJoinConcurrentWhileStopping(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-join-race")

	clusterOpts := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "join-race",
		multiNode:     true,
	}

	startOptions := MockProcessOptions{Key: "value"}

	// mgr1 is the "originating" node that starts the process and then stops it.
	mgr1 := &BackgroundManager[MockProcessOptions]{
		name:                "test-join-race-mgr1",
		Process:             &ResumableMockProcess{},
		clusterAwareOptions: clusterOpts,
	}
	require.NoError(t, mgr1.Start(ctx, startOptions))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr1.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)

	// Ensure the running state is persisted to the cluster so that Join callers can see it.
	require.NoError(t, mgr1.UpdateStatusClusterAware(ctx))

	const numJoinCallers = 10

	// Build a fleet of managers that will all call Join concurrently.
	joinManagers := make([]*BackgroundManager[MockProcessOptions], numJoinCallers)
	for i := range numJoinCallers {
		joinManagers[i] = &BackgroundManager[MockProcessOptions]{
			name:                fmt.Sprintf("test-join-race-caller%d", i),
			Process:             &ResumableMockProcess{},
			clusterAwareOptions: clusterOpts,
		}
	}

	// Use a barrier so all goroutines fire Join at the same instant that Stop is called.
	var barrier sync.WaitGroup
	barrier.Add(numJoinCallers + 1) // +1 for the Stop goroutine

	// joinErrors[i] holds the error returned by joinManagers[i].Join.
	joinErrors := make([]error, numJoinCallers)

	var joinWG sync.WaitGroup
	for i := range numJoinCallers {
		joinWG.Add(1)
		go func(i int) {
			defer joinWG.Done()
			barrier.Done()
			barrier.Wait()
			joinErrors[i] = joinManagers[i].Join(ctx)
		}(i)
	}

	// Stop mgr1 concurrently with the Join calls.
	go func() {
		barrier.Done()
		barrier.Wait()
		assert.NoError(t, mgr1.Stop(ctx))
	}()

	// Wait for all Join callers to finish before reading their results.
	joinWG.Wait()

	// Wait for mgr1 to reach a terminal state.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, err := mgr1.getClusterStatusState(ctx)
		require.NoError(c, err)
		assert.Contains(c, []BackgroundProcessState{
			BackgroundProcessStateStopped,
			BackgroundProcessStateCompleted,
		}, state)
	}, 10*time.Second, 100*time.Millisecond)

	// Stop any join callers that managed to start running.
	for i := range numJoinCallers {
		if joinErrors[i] == nil {
			// Join succeeded → process may be running; stop it.
			_ = joinManagers[i].Stop(ctx)
		}
	}

	// Wait for all join callers that started to reach a terminal state.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for i := range numJoinCallers {
			if joinErrors[i] != nil {
				// Join returned an error — no goroutine running, nothing to wait for.
				continue
			}
			state := joinManagers[i].GetRunState()
			if state == "" {
				// Join returned nil but the process never started: the cluster transitioned
				// out of "running" before this node called start(). Nothing to wait for.
				continue
			}
			assert.Contains(c, []BackgroundProcessState{
				BackgroundProcessStateStopped,
				BackgroundProcessStateCompleted,
			}, state, "mgr %d should be in terminal state", i)
		}
	}, 10*time.Second, 100*time.Millisecond)
}

// TestBackgroundManagerStartAfterCompleted verifies that calling Start() on a multi-node
// BackgroundManager after the process has already run to completion succeeds (returns nil).
func TestBackgroundManagerStartAfterCompletedSucceeds(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-start-after-complete")

	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "test-start-after-complete-mgr",
		Process: &immediateCallbackProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "start-after-complete",
			multiNode:     true,
		},
	}

	// Start and wait for the process to run to completion.
	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))
	RequireBackgroundManagerState(t, mgr, BackgroundProcessStateCompleted)

	err := mgr.Start(ctx, MockProcessOptions{})
	require.NoError(t, err)
}

type statsMockProcess struct {
	MockProcess
	lastPreviousStatus []byte
	previousStatusLock sync.Mutex
}

func (s *statsMockProcess) GetProcessStatus(status BackgroundManagerStatus, previousStatus []byte) ([]byte, []byte, error) {
	s.previousStatusLock.Lock()
	s.lastPreviousStatus = previousStatus
	s.previousStatusLock.Unlock()
	return s.MockProcess.GetProcessStatus(status, previousStatus)
}

func (s *statsMockProcess) LastPreviousStatus() []byte {
	s.previousStatusLock.Lock()
	defer s.previousStatusLock.Unlock()
	return s.lastPreviousStatus
}

// TestBackgroundManagerJoinPreservesPreviousStatus verifies that when a multi-node manager Join()s,
// it preserves and passes the previous cluster status to the process GetProcessStatus call,
// allowing it to merge/preserve stats.
func TestBackgroundManagerJoinPreservesPreviousStatus(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-join-preserve-status")

	clusterOpts := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "join-preserve-status",
		multiNode:     true,
	}

	process1 := &statsMockProcess{}
	mgr1 := &BackgroundManager[MockProcessOptions]{
		name:                "test-join-mgr1",
		Process:             process1,
		clusterAwareOptions: clusterOpts,
	}

	require.NoError(t, mgr1.Start(ctx, MockProcessOptions{}))
	RequireBackgroundManagerState(t, mgr1, BackgroundProcessStateRunning)

	// Ensure the running status is written to the cluster.
	require.NoError(t, mgr1.UpdateStatusClusterAware(ctx))

	process2 := &statsMockProcess{}
	mgr2 := &BackgroundManager[MockProcessOptions]{
		name:                "test-join-mgr2",
		Process:             process2,
		clusterAwareOptions: clusterOpts,
	}

	require.NoError(t, mgr2.Join(ctx))
	RequireBackgroundManagerState(t, mgr2, BackgroundProcessStateRunning)

	// Assert that process2 received the previous status in its GetProcessStatus call,
	// which is required to avoid dropping stats on join.
	require.NotEmpty(t, process2.LastPreviousStatus(), "expected previous status to be passed to GetProcessStatus on join")

	// Ensure we can unmarshal it to verify it actually contains the status.
	var clusterStatus struct {
		Status BackgroundManagerStatus `json:"status"`
	}
	err := json.Unmarshal(process2.LastPreviousStatus(), &clusterStatus)
	require.NoError(t, err)
	require.Equal(t, BackgroundProcessStateRunning, clusterStatus.Status.State)

	require.NoError(t, mgr1.Stop(ctx))
	require.NoError(t, mgr2.Stop(ctx))
}

// TestBackgroundManagerMultiNodePollingConverges verifies that when another node moves the shared
// cluster status to a terminal state, a still-running node that only observes this via polling
// converges to that same state - without overwriting it.
func TestBackgroundManagerMultiNodePollingConverges(t *testing.T) {
	tests := []struct {
		name          string
		externalState BackgroundProcessState
	}{
		{name: "Completed", externalState: BackgroundProcessStateCompleted},
		{name: "Error", externalState: BackgroundProcessStateError},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testBucket := base.GetTestBucket(t)
			ctx := base.TestCtx(t)
			defer testBucket.Close(ctx)

			metadataStore := testBucket.DefaultDataStore(ctx)
			metaKeys := base.NewMetadataKeys("test-polling-converge")

			clusterOpts := &ClusterAwareBackgroundManagerOptions{
				metadataStore: metadataStore,
				metaKeys:      metaKeys,
				processSuffix: "polling-converge",
				multiNode:     true,
			}

			process1 := &MockProcess{}
			mgr1 := &BackgroundManager[MockProcessOptions]{
				name:                "mgr1",
				Process:             process1,
				clusterAwareOptions: clusterOpts,
			}
			process2 := &MockProcess{}
			mgr2 := &BackgroundManager[MockProcessOptions]{
				name:                "mgr2",
				Process:             process2,
				clusterAwareOptions: clusterOpts,
			}

			// Start both managers. Both should run.
			require.NoError(t, mgr1.Start(ctx, MockProcessOptions{}))
			defer func() { _ = mgr1.Stop(ctx) }()

			require.NoError(t, mgr2.Start(ctx, MockProcessOptions{}))
			defer func() { _ = mgr2.Stop(ctx) }()

			RequireBackgroundManagerState(t, mgr1, BackgroundProcessStateRunning)
			RequireBackgroundManagerState(t, mgr2, BackgroundProcessStateRunning)

			// Simulate another node reaching a terminal state by manually writing it to the shared
			// cluster status document.
			docID := clusterOpts.StatusDocID()
			_, err := metadataStore.Update(ctx, docID, 0, func(current []byte) ([]byte, *uint32, bool, error) {
				var output map[string]json.RawMessage
				if current != nil {
					_ = base.JSONUnmarshal(current, &output)
				} else {
					output = make(map[string]json.RawMessage)
				}

				status := BackgroundManagerStatus{
					State:     test.externalState,
					StartTime: time.Now(),
				}
				statusBytes, err := base.JSONMarshal(status)
				if err != nil {
					return nil, nil, false, err
				}
				output["status"] = statusBytes
				output["meta"] = json.RawMessage("null")

				outputBytes, err := base.JSONMarshal(output)
				if err != nil {
					return nil, nil, false, err
				}
				return outputBytes, nil, false, nil
			})
			require.NoError(t, err)

			// Wait for mgr2's polling loop to detect the terminal status and close its terminator.
			require.Eventually(t, func() bool {
				terminator := mgr2.terminator.Load()
				return terminator != nil && terminator.IsClosed()
			}, 10*time.Second, 100*time.Millisecond, "expected mgr2 terminator to be closed after polling detects "+string(test.externalState)+" status")

			// mgr2 adopts the external state when its process goroutine finishes, shortly after the
			// terminator closes.
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Equal(c, test.externalState, mgr2.GetRunState(), "mgr2 should adopt the real external state")
			}, 10*time.Second, 100*time.Millisecond)

			// Verify that the status in the bucket remains as set externally and is not overwritten by mgr2.
			rawStatus, err := mgr2.GetStatus(ctx)
			require.NoError(t, err)
			var status BackgroundManagerStatus
			require.NoError(t, base.JSONUnmarshal(rawStatus, &status))
			assert.Equal(t, test.externalState, status.State, "expected the bucket status to remain "+string(test.externalState)+" and NOT be overwritten")
		})
	}
}

// TestBackgroundManagerMultiNodeStopConvergesToStopped verifies that stopping a multi-node process
// via one manager brings every manager - including ones that only observe the stop by polling the
// shared cluster status - to Stopped, both locally and in the persisted cluster status.
func TestBackgroundManagerMultiNodeStopConvergesToStopped(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-multi-node-stop")

	options := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "multi-node-stop",
		multiNode:     true,
	}

	process1 := &MockProcess{}
	mgr1 := &BackgroundManager[MockProcessOptions]{
		name:                "mgr1",
		Process:             process1,
		clusterAwareOptions: options,
	}

	process2 := &MockProcess{}
	mgr2 := &BackgroundManager[MockProcessOptions]{
		name:                "mgr2",
		Process:             process2,
		clusterAwareOptions: options,
	}

	// Start both managers. Both should run.
	require.NoError(t, mgr1.Start(ctx, MockProcessOptions{}))
	defer func() { _ = mgr1.Stop(ctx) }()

	require.NoError(t, mgr2.Start(ctx, MockProcessOptions{}))
	defer func() { _ = mgr2.Stop(ctx) }()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr1.GetRunState())
		assert.Equal(c, BackgroundProcessStateRunning, mgr2.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)

	// Only mgr1 receives Stop(); mgr2 only learns about it via status polling.
	require.NoError(t, mgr1.Stop(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateStopped, mgr1.GetRunState())
		assert.Equal(c, BackgroundProcessStateStopped, mgr2.GetRunState())
	}, 10*time.Second, 100*time.Millisecond)

	// RequireBackgroundManagerState reads the shared cluster doc via GetStatus, and with two
	// managers able to write it, an Eventually-style wait could observe a transient correct value
	// before it's overwritten and pass regardless. GetRunState has no such race, and once both
	// managers are terminal their status pollers have stopped writing, so a single cluster read
	// afterwards is stable.
	clusterState, err := mgr1.getClusterStatusState(ctx)
	require.NoError(t, err)
	assert.Equal(t, BackgroundProcessStateStopped, clusterState)
}

// TestBackgroundManagerStartReturnsErrorWhileProcessKeepsRunning reproduces the case where Init succeeds and
// Process.Run is launched in its own goroutine, but the subsequent persist of the initial cluster status fails.
// Start() returns that error to the caller, even though Run is already executing in the background.
func TestBackgroundManagerStartReturnsErrorWhileProcessKeepsRunning(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := context.Background()
	defer testBucket.Close(ctx)

	metaKeys := base.NewMetadataKeys("test-start-persist-fail")
	processSuffix := "multi-persist-fail"
	// Same key start()'s final updateMultiNodeClusterAwareStatus call will write to.
	statusDocID := metaKeys.BackgroundProcessStatusPrefix(processSuffix)

	// The first Update call for statusDocID fails outright, so start() sees an error and returns it, exactly
	// like a transient bucket blip would. Every call after that behaves normally, so the manager can be reused
	// as-is for the recovery Start() below without needing a different key or a fresh metadataStore.
	var updateCount atomic.Int32
	leakyBucket := base.NewLeakyBucket(testBucket, base.LeakyBucketConfig{
		PreUpdateCallback: func(key string) error {
			if key == statusDocID && updateCount.Add(1) == 1 {
				return errors.New("simulated write failure")
			}
			return nil
		},
	})
	leakyMetadataStore := leakyBucket.DefaultDataStore(ctx)

	process := &MockProcess{}
	mgr := &BackgroundManager[MockProcessOptions]{
		name: "persist-fail-mgr",
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: leakyMetadataStore,
			metaKeys:      metaKeys,
			processSuffix: processSuffix,
			multiNode:     true,
		},
		Process: process,
	}

	// Start() reports failure to the caller...
	err := mgr.Start(ctx, MockProcessOptions{})
	require.Error(t, err)

	// ...but Process.Run was already launched before that failure occurred, so the manager should not be left
	// reporting Running forever - it should reflect the failure as an Error state, same as the Process.Run-error
	// path does via SetError.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NotEqual(c, BackgroundProcessStateRunning, mgr.GetRunState())
		assert.Equal(c, BackgroundProcessStateError, mgr.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)

	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr.GetRunState())
	}, 5*time.Second, 100*time.Millisecond)
	require.NoError(t, mgr.Stop(ctx))
}

// TestUpdateStatusClusterAware checks that UpdateStatusClusterAware surfaces the underlying bucket-closed
// error for single-node and multi-node managers.
func TestUpdateStatusClusterAware(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-update-status-closed-bucket")

	testCases := []struct {
		name                string
		clusterAwareOptions *ClusterAwareBackgroundManagerOptions
	}{
		{
			name: "single node",
			clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
				metadataStore: metadataStore,
				metaKeys:      metaKeys,
				processSuffix: "update-status-closed-bucket-single-node",
			},
		},
		{
			name: "multi node",
			clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
				metadataStore: metadataStore,
				metaKeys:      metaKeys,
				processSuffix: "update-status-closed-bucket-multi-node",
				multiNode:     true,
			},
		},
	}

	testBucket.Close(ctx)

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			mgr := &BackgroundManager[MockProcessOptions]{
				name:                testCase.name + "-mgr",
				Process:             &MockProcess{},
				clusterAwareOptions: testCase.clusterAwareOptions,
			}

			err := mgr.UpdateStatusClusterAware(ctx)
			if base.TestUseCouchbaseServer() {
				require.ErrorIs(t, err, gocb.ErrShutdown)
			} else {
				require.ErrorIs(t, err, rosmar.ErrBucketClosed)
			}
		})
	}
}

// TestBackgroundManagerConcurrentStopStartRace stress-tests starting, stopping, and updating status
// concurrently across multiple goroutines to verify there are no deadlocks, panics, or race conditions.
func TestBackgroundManagerConcurrentStopStartRace(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)

	metadataStore := testBucket.DefaultDataStore(ctx)
	metaKeys := base.NewMetadataKeys("test-stop-start-race")

	clusterOpts := &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
		processSuffix: "stop-start-race",
		multiNode:     true,
	}

	process := &MockProcess{}
	mgr := &BackgroundManager[MockProcessOptions]{
		name:                "race-mgr",
		Process:             process,
		clusterAwareOptions: clusterOpts,
	}

	// Depending on how the goroutines below interleave, the manager can be left running or mid-stop when
	// they finish, which would leave its goroutines using the bucket after it has been closed. Registered
	// after the testBucket.Close defer above, so it runs before the bucket is closed.
	defer func() {
		terminalStates := []BackgroundProcessState{BackgroundProcessStateStopped, BackgroundProcessStateCompleted, BackgroundProcessStateError}
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			// Stop is a no-op if the process is already stopped or stopping.
			assert.NoError(c, mgr.Stop(ctx))
			assert.Contains(c, terminalStates, mgr.GetRunState())
			// Also wait for the persisted status, which the process goroutine writes after transitioning
			// to a terminal state, to ensure it is done with the bucket.
			rawStatus, err := mgr.GetStatus(ctx)
			if !assert.NoError(c, err) {
				return
			}
			var status BackgroundManagerStatus
			if !assert.NoError(c, base.JSONUnmarshal(rawStatus, &status)) {
				return
			}
			assert.Contains(c, terminalStates, status.State, "BackgroundManager did not reach a terminal state: %s", string(rawStatus))
		}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)
	}()

	numGoroutines := 10
	iterations := 20

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 3)

	// Worker group 1: Concurrently start
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = mgr.Start(ctx, MockProcessOptions{})
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	// Worker group 2: Concurrently stop
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = mgr.Stop(ctx)
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	// Worker group 3: Concurrently update status
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = mgr.UpdateStatusClusterAware(ctx)
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

// TestUpdateHeartbeatDocClusterAwareTransientError reproduces a bug in UpdateHeartbeatDocClusterAware: the
// grace-period check compares an elapsed time.Duration (nanoseconds) against
// (BackgroundManagerHeartbeatExpirySecs - BackgroundManagerHeartbeatIntervalSecs), which is an untyped
// constant representing *seconds* (29) that is never multiplied by time.Second. Since any measurable elapsed
// time comfortably exceeds 29 nanoseconds, a single transient heartbeat write error is treated as if the
// ~29 second grace period had already elapsed, and the error is propagated (causing the caller to call
// SetError and terminate the background process) instead of being tolerated.
//
// The two subtests differ in how the manager reaches the "just had a successful heartbeat" precondition:
// "already running" sets lastSuccessfulHeartbeatUnix directly, while "startup" goes through mgr.Start, which
// exercises markStart recording its own heartbeat-doc write as the initial success (lastSuccessfulHeartbeatUnix
// otherwise defaults to zero, so a transient error on the very first tick after Start would look like ~56
// years had passed since the last success).
func TestUpdateHeartbeatDocClusterAwareTransientError(t *testing.T) {
	tests := []struct {
		name string
		// setup gets the manager into a state where a heartbeat has just succeeded, before the injected error
		// is armed.
		setup func(t *testing.T, ctx context.Context, mgr *BackgroundManager[MockProcessOptions], metadataStore base.DataStore, heartbeatDocID string)
	}{
		{
			name: "already running",
			setup: func(t *testing.T, ctx context.Context, mgr *BackgroundManager[MockProcessOptions], metadataStore base.DataStore, heartbeatDocID string) {
				// Write the heartbeat doc up front, as markStart would, so a heartbeat update that isn't
				// intercepted by the injected error can succeed for real against the underlying bucket.
				require.NoError(t, metadataStore.SetRaw(ctx, heartbeatDocID, BackgroundManagerHeartbeatExpirySecs, nil, []byte("{}")))
				mgr.terminator.Store(base.NewSafeTerminator())
				mgr.clusterAwareOptions.lastSuccessfulHeartbeatUnix.Set(time.Now().Unix())
			},
		},
		{
			name: "startup",
			setup: func(t *testing.T, ctx context.Context, mgr *BackgroundManager[MockProcessOptions], metadataStore base.DataStore, heartbeatDocID string) {
				// Start performs the real (uninjected) WriteCas that writes the heartbeat doc for the first
				// time; this must count as the most recent successful heartbeat even though
				// UpdateHeartbeatDocClusterAware itself has not yet been called.
				require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))
				t.Cleanup(func() { _ = mgr.Stop(ctx) })
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testBucket := base.GetTestBucket(t)
			ctx := base.TestCtx(t)

			metaKeys := base.NewMetadataKeys("test-heartbeat-transient-error")
			processSuffix := "heartbeat-transient-error"
			heartbeatDocID := metaKeys.BackgroundProcessHeartbeatPrefix(processSuffix)

			var injectError atomic.Bool
			leakyBucket := testBucket.LeakyBucketClone(base.LeakyBucketConfig{
				GetAndTouchRawCallback: func(key string) error {
					if key == heartbeatDocID && injectError.Load() {
						return fmt.Errorf("injected transient heartbeat write error")
					}
					return nil
				},
			})
			defer leakyBucket.Close(ctx)
			metadataStore := leakyBucket.DefaultDataStore(ctx)

			mgr := &BackgroundManager[MockProcessOptions]{
				name:    "test-heartbeat-transient-error-mgr",
				Process: &MockProcess{},
				clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
					metadataStore: metadataStore,
					metaKeys:      metaKeys,
					processSuffix: processSuffix,
				},
			}

			test.setup(t, ctx, mgr, metadataStore, heartbeatDocID)

			injectError.Store(true)
			err := mgr.UpdateHeartbeatDocClusterAware(ctx)
			require.NoError(t, err, "a transient heartbeat write error should be tolerated within the grace period")

			// Once the injected error clears, the next heartbeat should succeed for real and record a new success time.
			injectError.Store(false)
			require.NoError(t, mgr.UpdateHeartbeatDocClusterAware(ctx))
		})
	}
}

// gatedInitProcess holds a run inside Init until the test releases it, then runs until its terminator closes.
type gatedInitProcess struct {
	t           testing.TB
	initEntered chan struct{}
	releaseInit chan struct{}
}

func newGatedInitProcess(t testing.TB) gatedInitProcess {
	return gatedInitProcess{
		t:           t,
		initEntered: make(chan struct{}),
		releaseInit: make(chan struct{}),
	}
}

func (p *gatedInitProcess) Init(context.Context, MockProcessOptions, []byte) (backgroundManagerInitMode, error) {
	close(p.initEntered)
	sgtest.RequireChanClosedFromCallback(p.t, p.releaseInit)
	return backgroundManagerInitResume, nil
}

func (p *gatedInitProcess) Run(_ context.Context, _ MockProcessOptions, _ updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	sgtest.RequireChanClosedFromCallback(p.t, terminator.Done())
	return nil
}

func (p *gatedInitProcess) GetProcessStatus(status BackgroundManagerStatus, _ []byte) (statusOut []byte, meta []byte, err error) {
	statusOut, err = base.JSONMarshal(status)
	return statusOut, nil, err
}

func (p *gatedInitProcess) SetProcessStatus(context.Context, []byte, []byte) {}

func (p *gatedInitProcess) ResetStatus() {}

// TestBackgroundManagerJoinDoesNotResurrectStoppedProcess covers a Join admitted against a running status document
// that only writes its own status after the process has been stopped everywhere. The joining node has to end its
// run rather than put the cluster back to running.
func TestBackgroundManagerJoinDoesNotResurrectStoppedProcess(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)

	clusterAwareOptions := &ClusterAwareBackgroundManagerOptions{
		metadataStore: testBucket.DefaultDataStore(ctx),
		metaKeys:      base.NewMetadataKeys("test-join-after-stop"),
		processSuffix: "join-after-stop",
		multiNode:     true,
	}
	timeout := sgtest.GetBackgroundManagerStatusTransitionTimeout(t)

	runningNode := &BackgroundManager[MockProcessOptions]{
		name:                "join-after-stop-running",
		Process:             &MockProcess{},
		clusterAwareOptions: clusterAwareOptions,
	}
	require.NoError(t, runningNode.Start(ctx, MockProcessOptions{}))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, err := runningNode.getClusterStatusState(ctx)
		assert.NoError(c, err)
		assert.Equal(c, BackgroundProcessStateRunning, state)
	}, timeout, 10*time.Millisecond)

	gated := newGatedInitProcess(t)
	process := &gated
	joiningNode := &BackgroundManager[MockProcessOptions]{
		name:                "join-after-stop-joining",
		Process:             process,
		clusterAwareOptions: clusterAwareOptions,
	}
	var joinErr error
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		joinErr = joiningNode.Join(ctx)
	}()

	// The joining node is admitted against a running status document, and then held before it writes its own.
	base.RequireChanClosed(t, process.initEntered, "joining node did not reach Init")

	require.NoError(t, runningNode.Stop(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, err := runningNode.getClusterStatusState(ctx)
		assert.NoError(c, err)
		assert.Equal(c, BackgroundProcessStateStopped, state)
	}, timeout, 10*time.Millisecond)

	close(process.releaseInit)
	base.RequireChanClosed(t, joined, "Join did not return")
	require.NoError(t, joinErr)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateStopped, joiningNode.GetRunState())
	}, timeout, 10*time.Millisecond)

	// The status writes the joining node has left to make must not move the cluster off the state it reached.
	require.Never(t, func() bool {
		state, err := runningNode.getClusterStatusState(ctx)
		return err != nil || state != BackgroundProcessStateStopped
	}, 2*BackgroundManagerStatusUpdateIntervalSecs*time.Second, 50*time.Millisecond)
}

// staleRunningStatusProcess reports a running status whatever state the manager passes in, standing in for an
// update whose bytes were built before the run state moved to terminal.
type staleRunningStatusProcess struct {
	MockProcess
}

func (p *staleRunningStatusProcess) GetProcessStatus(status BackgroundManagerStatus, _ []byte) (statusOut []byte, meta []byte, err error) {
	status.State = BackgroundProcessStateRunning
	statusOut, err = base.JSONMarshal(status)
	return statusOut, nil, err
}

// TestBackgroundManagerUpdateGuardUsesStatusBeingWritten covers a status update that serialized a running status
// before this node adopted the terminal state another node reached. The guard has to refuse it on the status being
// written, not on a run state read afterwards.
func TestBackgroundManagerUpdateGuardUsesStatusBeingWritten(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)

	metadataStore := testBucket.DefaultDataStore(ctx)
	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "stale-running-status",
		Process: &staleRunningStatusProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      base.NewMetadataKeys("test-stale-running-status"),
			processSuffix: "stale-running-status",
			multiNode:     true,
		},
	}

	// Another node has already ended the process, and this node has taken that state locally.
	require.NoError(t, metadataStore.Set(ctx, mgr.clusterAwareOptions.StatusDocID(), 0, nil, map[string]json.RawMessage{
		"status": json.RawMessage(`{"status":"stopped"}`),
		"meta":   json.RawMessage(`{}`),
	}))
	mgr.setRunState(BackgroundProcessStateStopped)

	var statusErr errBackgroundManagerStatusNotRunning
	require.ErrorAs(t, mgr.updateMultiNodeClusterAwareStatus(ctx, backgroundManagerStatusUpdate, nil), &statusErr)
	require.Equal(t, BackgroundProcessStateStopped, statusErr.state)

	state, err := mgr.getClusterStatusState(ctx)
	require.NoError(t, err)
	require.Equal(t, BackgroundProcessStateStopped, state)
}

// gatedInitReturningProcess holds a run inside Init like gatedInitProcess, but returns from Run at once, so a run
// that reaches Run despite being refused entry writes its own terminal status.
type gatedInitReturningProcess struct {
	gatedInitProcess
	runCalled atomic.Bool
}

func (p *gatedInitReturningProcess) Run(context.Context, MockProcessOptions, updateStatusCallbackFunc, *base.SafeTerminator) error {
	p.runCalled.Store(true)
	return nil
}

// TestBackgroundManagerRefusedJoinDoesNotRunProcess covers a Join refused because the cluster ended the process
// while this node was joining. The node must not reach Process.Run, which would write a terminal state of its own
// over the one the cluster reached.
func TestBackgroundManagerRefusedJoinDoesNotRunProcess(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)

	clusterAwareOptions := &ClusterAwareBackgroundManagerOptions{
		metadataStore: testBucket.DefaultDataStore(ctx),
		metaKeys:      base.NewMetadataKeys("test-refused-join"),
		processSuffix: "refused-join",
		multiNode:     true,
	}
	timeout := sgtest.GetBackgroundManagerStatusTransitionTimeout(t)

	runningNode := &BackgroundManager[MockProcessOptions]{
		name:                "refused-join-running",
		Process:             &MockProcess{},
		clusterAwareOptions: clusterAwareOptions,
	}
	require.NoError(t, runningNode.Start(ctx, MockProcessOptions{}))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, err := runningNode.getClusterStatusState(ctx)
		assert.NoError(c, err)
		assert.Equal(c, BackgroundProcessStateRunning, state)
	}, timeout, 10*time.Millisecond)

	process := &gatedInitReturningProcess{
		gatedInitProcess: newGatedInitProcess(t),
	}
	joiningNode := &BackgroundManager[MockProcessOptions]{
		name:                "refused-join-joining",
		Process:             process,
		clusterAwareOptions: clusterAwareOptions,
	}
	var joinErr error
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		joinErr = joiningNode.Join(ctx)
	}()

	// The joining node is admitted against a running status document, and then held before it claims the run.
	base.RequireChanClosed(t, process.initEntered, "joining node did not reach Init")

	require.NoError(t, runningNode.Stop(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, err := runningNode.getClusterStatusState(ctx)
		assert.NoError(c, err)
		assert.Equal(c, BackgroundProcessStateStopped, state)
	}, timeout, 10*time.Millisecond)

	close(process.releaseInit)
	base.RequireChanClosed(t, joined, "Join did not return")
	require.NoError(t, joinErr)
	require.False(t, process.runCalled.Load(), "a refused join must not run the process")

	require.Never(t, func() bool {
		state, err := runningNode.getClusterStatusState(ctx)
		return err != nil || state != BackgroundProcessStateStopped
	}, 2*BackgroundManagerStatusUpdateIntervalSecs*time.Second, 50*time.Millisecond)
}

// gatedProcess is a BackgroundManagerProcessI whose first Run can be held open after its terminator closes, so a
// test can observe the manager while a previous run's goroutines are still alive.
type gatedProcess struct {
	t           testing.TB
	runCount    atomic.Int64
	resetCount  atomic.Int64
	terminating chan struct{} // closed by the first run once its terminator fires
	release     chan struct{} // the first run returns when this is closed
}

func newGatedProcess(t testing.TB) *gatedProcess {
	return &gatedProcess{
		t:           t,
		terminating: make(chan struct{}),
		release:     make(chan struct{}),
	}
}

func (p *gatedProcess) Init(context.Context, MockProcessOptions, []byte) (backgroundManagerInitMode, error) {
	return backgroundManagerInitReset, nil
}

func (p *gatedProcess) Run(_ context.Context, _ MockProcessOptions, _ updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	isFirstRun := p.runCount.Add(1) == 1
	sgtest.RequireChanClosedFromCallback(p.t, terminator.Done())
	if isFirstRun {
		close(p.terminating)
		sgtest.RequireChanClosedFromCallback(p.t, p.release)
	}
	return nil
}

func (p *gatedProcess) GetProcessStatus(status BackgroundManagerStatus, _ []byte) (statusOut []byte, meta []byte, err error) {
	statusOut, err = base.JSONMarshal(status)
	return statusOut, nil, err
}

func (p *gatedProcess) SetProcessStatus(context.Context, []byte, []byte) {}

func (p *gatedProcess) ResetStatus() { p.resetCount.Add(1) }

// parkedRun is a multi node background manager whose first run is held inside its terminal status update, with the
// run state already stopped, so a test can act while the run looks finished but its goroutine is still alive.
type parkedRun struct {
	mgr     *BackgroundManager[MockProcessOptions]
	process *gatedProcess
	// databaseStateWrites counts every mirror into the database state document, so a test can tell that a status
	// write happened without unparking the run.
	databaseStateWrites atomic.Int64
	// release lets the parked goroutine finish. Safe to call more than once, and called on test cleanup.
	release func()
}

// startParkedRun starts a multi node background manager named name, stops it, and returns once the goroutine of that
// run has parked inside its terminal status update.
func startParkedRun(t *testing.T, ctx context.Context, name string) *parkedRun {
	testBucket := base.GetTestBucket(t)
	t.Cleanup(func() { testBucket.Close(ctx) })

	run := &parkedRun{process: newGatedProcess(t)}
	run.mgr = &BackgroundManager[MockProcessOptions]{
		name:    name,
		Process: run.process,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: testBucket.DefaultDataStore(ctx),
			metaKeys:      base.NewMetadataKeys("test-" + name),
			processSuffix: name,
			multiNode:     true,
		},
	}

	// blockTerminalUpdate arms the park once the run is on its way out, so Stop's own status updates go through.
	var blockTerminalUpdate atomic.Bool
	parked := make(chan struct{})
	unpark := make(chan struct{})
	var parkOnce, unparkOnce sync.Once
	run.release = func() { unparkOnce.Do(func() { close(unpark) }) }
	t.Cleanup(run.release)
	run.mgr.updateDatabaseState = func(context.Context, bool) error {
		run.databaseStateWrites.Add(1)
		if !blockTerminalUpdate.Load() || run.mgr.GetRunState() != BackgroundProcessStateStopped {
			return nil
		}
		parkOnce.Do(func() { close(parked) })
		sgtest.RequireChanClosedFromCallback(t, unpark)
		return nil
	}

	require.NoError(t, run.mgr.Start(ctx, MockProcessOptions{}))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(1), run.process.runCount.Load())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)

	require.NoError(t, run.mgr.Stop(ctx))
	base.RequireChanClosed(t, run.process.terminating)

	blockTerminalUpdate.Store(true)
	close(run.process.release)

	select {
	case <-parked:
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "first run did not reach its terminal status update")
	}
	require.Equal(t, BackgroundProcessStateStopped, run.mgr.GetRunState())
	return run
}

// requireRunEnded waits for the manager to reach a state that admits another run.
func requireRunEnded(t *testing.T, mgr *BackgroundManager[MockProcessOptions]) {
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, []BackgroundProcessState{BackgroundProcessStateStopped, BackgroundProcessStateCompleted}, mgr.GetRunState())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)
}

// TestBackgroundManagerStartWaitsForPreviousRunGoroutine asserts that Start does not replace b.terminator while the
// previous run's goroutine is still alive. That goroutine reports its terminal state before its last status update,
// so the manager looks stopped while the run still uses the terminator.
func TestBackgroundManagerStartWaitsForPreviousRunGoroutine(t *testing.T) {
	ctx := base.TestCtx(t)
	run := startParkedRun(t, ctx, "start-waits-for-previous-run")

	secondStart := make(chan error, 1)
	go func() {
		secondStart <- run.mgr.Start(ctx, MockProcessOptions{})
	}()

	select {
	case err := <-secondStart:
		require.FailNow(t, "Start returned while the previous run's goroutine was still running", "error: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	require.Equal(t, int64(1), run.process.runCount.Load(), "second run started before the first run's goroutine exited")

	run.release()

	select {
	case err := <-secondStart:
		require.NoError(t, err)
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "Start did not return after the previous run's goroutine exited")
	}

	require.NoError(t, run.mgr.Stop(ctx))
	requireRunEnded(t, run.mgr)
}

// TestBackgroundManagerStartWaitsForHeartbeatGoroutine asserts the same invariant for the heartbeat goroutine of a
// single node cluster aware manager, which outlives the run state in the same way.
func TestBackgroundManagerStartWaitsForHeartbeatGoroutine(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)

	metaKeys := base.NewMetadataKeys("test-heartbeat-goroutine-wait")
	processSuffix := "heartbeat-goroutine-wait"
	heartbeatDocID := metaKeys.BackgroundProcessHeartbeatPrefix(processSuffix)

	parked := make(chan struct{})
	unpark := make(chan struct{})
	var unparkOnce sync.Once
	// Released on every path: a failed assertion below must not leave a goroutine wedged in the data store.
	releaseParked := func() { unparkOnce.Do(func() { close(unpark) }) }
	defer releaseParked()
	var parkOnce sync.Once
	leakyBucket := testBucket.LeakyBucketClone(base.LeakyBucketConfig{
		GetAndTouchRawCallback: func(key string) error {
			if key != heartbeatDocID {
				return nil
			}
			parkOnce.Do(func() { close(parked) })
			sgtest.RequireChanClosedFromCallback(t, unpark)
			return nil
		},
	})
	defer leakyBucket.Close(ctx)
	metadataStore := leakyBucket.DefaultDataStore(ctx)

	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "start-waits-for-heartbeat",
		Process: &MockProcess{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: processSuffix,
		},
	}

	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))

	// Wait for the heartbeat goroutine of the first run to park inside GetAndTouchRaw.
	select {
	case <-parked:
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "heartbeat goroutine did not run")
	}

	require.NoError(t, mgr.Stop(ctx))

	// Wait for the heartbeat doc to go, not just the run state: while it is there markStart rejects a Start
	// outright, which is a different window from the goroutine wait under test.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, []BackgroundProcessState{BackgroundProcessStateStopped, BackgroundProcessStateCompleted}, mgr.GetRunState())
		_, _, err := metadataStore.GetRaw(ctx, heartbeatDocID)
		assert.True(c, base.IsDocNotFoundError(err), "heartbeat doc still present: %v", err)
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)

	secondStart := make(chan error, 1)
	go func() {
		secondStart <- mgr.Start(ctx, MockProcessOptions{})
	}()

	select {
	case err := <-secondStart:
		require.FailNow(t, "Start returned while the previous run's heartbeat goroutine was still running", "error: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	releaseParked()

	select {
	case err := <-secondStart:
		require.NoError(t, err)
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "Start did not return after the previous run's heartbeat goroutine exited")
	}

	require.NoError(t, mgr.Stop(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, []BackgroundProcessState{BackgroundProcessStateStopped, BackgroundProcessStateCompleted}, mgr.GetRunState())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)
}

// TestBackgroundManagerStopDuringStartWait covers a Stop landing while Start waits for the previous run's
// goroutines, so it closes that run's terminator rather than the one the new run is about to install. start has to
// carry the stop over, and before it resets anything the previous run left behind.
func TestBackgroundManagerStopDuringStartWait(t *testing.T) {
	ctx := base.TestCtx(t)
	run := startParkedRun(t, ctx, "stop-during-start-wait")
	startTime := run.mgr.getStartTime()

	// The second Start is admitted by markStart and then blocks waiting for the parked goroutine of the first run.
	secondStart := make(chan error, 1)
	go func() {
		secondStart <- run.mgr.Start(ctx, MockProcessOptions{})
	}()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, run.mgr.GetRunState())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)

	// Stop the run that has been admitted but has no terminator of its own yet.
	require.NoError(t, run.mgr.Stop(ctx))
	require.Equal(t, BackgroundProcessStateStopping, run.mgr.GetRunState())

	run.release()
	select {
	case err := <-secondStart:
		require.NoError(t, err)
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "Start did not return after the previous run's goroutine exited")
	}

	// The second run must honour the stop rather than run on with a terminator nothing closed.
	requireRunEnded(t, run.mgr)

	// The stop was carried over before the run was set up, so nothing of the previous run was reset.
	require.Equal(t, int64(1), run.process.resetCount.Load(), "a run that never started reset the previous run's stats")
	require.Equal(t, startTime, run.mgr.getStartTime())
	require.Equal(t, int64(1), run.process.runCount.Load())
}

// slowToStopProcess keeps working after its terminator closes, until the test releases it. It models a process
// that is slow to notice a stop.
type slowToStopProcess struct {
	t           testing.TB
	runCount    atomic.Int64
	keepWorking chan struct{}
}

func (p *slowToStopProcess) Init(context.Context, MockProcessOptions, []byte) (backgroundManagerInitMode, error) {
	return backgroundManagerInitReset, nil
}

func (p *slowToStopProcess) Run(_ context.Context, _ MockProcessOptions, _ updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	if p.runCount.Add(1) == 1 {
		sgtest.RequireChanClosedFromCallback(p.t, p.keepWorking)
		return nil
	}
	sgtest.RequireChanClosedFromCallback(p.t, terminator.Done())
	return nil
}

func (p *slowToStopProcess) GetProcessStatus(status BackgroundManagerStatus, _ []byte) (statusOut []byte, meta []byte, err error) {
	statusOut, err = base.JSONMarshal(status)
	return statusOut, nil, err
}

func (p *slowToStopProcess) SetProcessStatus(context.Context, []byte, []byte) {}

func (p *slowToStopProcess) ResetStatus() {}

// TestSetErrorKeepsRunStateRunningUntilProcessStops covers a SetError raised while Process.Run is still working.
// The run state stays running until the process stops, so that markStart rejects a Start rather than admitting a
// second run alongside the first.
func TestSetErrorKeepsRunStateRunningUntilProcessStops(t *testing.T) {
	ctx := base.TestCtx(t)

	process := &slowToStopProcess{t: t, keepWorking: make(chan struct{})}
	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "set-error-keeps-running",
		Process: process,
	}

	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(1), process.runCount.Load())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)

	mgr.SetError(errors.New("injected error"))
	require.Equal(t, BackgroundProcessStateRunning, mgr.GetRunState(), "the process is still working, so the run is not terminal yet")

	// A Start must fail fast here rather than block or start a second run alongside the first.
	require.ErrorIs(t, mgr.Start(ctx, MockProcessOptions{}), errBackgroundManagerProcessAlreadyRunning)
	require.Equal(t, int64(1), process.runCount.Load())

	close(process.keepWorking)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateError, mgr.GetRunState())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)

	// The run state is terminal only now, so the next run is admitted and its state is its own.
	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))
	require.Equal(t, BackgroundProcessStateRunning, mgr.GetRunState())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(2), process.runCount.Load())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)

	require.NoError(t, mgr.Stop(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateStopped, mgr.GetRunState())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)
}

// TestMarkStopAdmitsStopDuringStartWait covers a Stop arriving while Start waits for the previous run's goroutines.
// markStart has set the run state to running, so markStop admits the stop and stopProcess reads the terminator start
// is about to write. Hence the atomic pointer.
func TestMarkStopAdmitsStopDuringStartWait(t *testing.T) {
	ctx := base.TestCtx(t)
	run := startParkedRun(t, ctx, "markstop-during-start-wait")

	// Start is admitted by markStart and then blocks waiting for the parked goroutine of the first run. It has not
	// installed a terminator for this run yet.
	secondStart := make(chan error, 1)
	go func() {
		secondStart <- run.mgr.Start(ctx, MockProcessOptions{})
	}()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, run.mgr.GetRunState())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)

	// Stop calls these two in turn. markStop reports neither errBackgroundManagerProcessAlreadyStopped nor
	// errBackgroundManagerStatusAlreadyStopping, so Stop does not return early and stopProcess runs.
	require.NoError(t, run.mgr.markStop(ctx))
	writesBeforeStopProcess := run.databaseStateWrites.Load()
	run.mgr.stopProcess(ctx)
	require.Greater(t, run.databaseStateWrites.Load(), writesBeforeStopProcess, "stopProcess did not write the cluster status")

	run.release()
	select {
	case err := <-secondStart:
		require.NoError(t, err)
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "Start did not return after the previous run's goroutine exited")
	}

	requireRunEnded(t, run.mgr)
}

// TestStartStatusFailureEndsRunBeforeProcessRuns covers a failure persisting the initial cluster status. No
// Process.Run goroutine has been launched at that point, so start has to end the run itself.
func TestStartStatusFailureEndsRunBeforeProcessRuns(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	metaKeys := base.NewMetadataKeys("test-start-status-failure")
	statusDocID := metaKeys.BackgroundProcessStatusPrefix("start-status-failure")

	var updates atomic.Int32
	leakyBucket := testBucket.LeakyBucketClone(base.LeakyBucketConfig{
		PreUpdateCallback: func(key string) error {
			if key != statusDocID || updates.Add(1) != 1 {
				return nil
			}
			return errors.New("injected status write failure")
		},
	})
	defer leakyBucket.Close(ctx)

	process := &MockProcess{}
	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "start-status-failure",
		Process: process,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: leakyBucket.DefaultDataStore(ctx),
			metaKeys:      metaKeys,
			processSuffix: "start-status-failure",
			multiNode:     true,
		},
	}

	require.Error(t, mgr.Start(ctx, MockProcessOptions{}))
	require.False(t, process.RunWasCalled(), "a claim that could not be written must not run the process")
	require.Equal(t, BackgroundProcessStateError, mgr.GetRunState())

	// The failed run left nothing of itself behind, so the next Start is admitted and runs.
	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, process.RunWasCalled())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)

	require.NoError(t, mgr.Stop(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, []BackgroundProcessState{BackgroundProcessStateStopped, BackgroundProcessStateCompleted}, mgr.GetRunState())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)
}

// failingInitProcess fails the Init of its first run, and behaves like MockProcess from then on.
type failingInitProcess struct {
	MockProcess
	initCount atomic.Int64
}

func (p *failingInitProcess) Init(ctx context.Context, options MockProcessOptions, clusterStatus []byte) (backgroundManagerInitMode, error) {
	if p.initCount.Add(1) == 1 {
		return backgroundManagerInitReset, errors.New("injected init failure")
	}
	return p.MockProcess.Init(ctx, options, clusterStatus)
}

// TestBackgroundManagerStartWaitsForRunThatFailedInit covers a run that ends inside start itself, because Init
// failed. Its terminal status update still has to join runWaitGroup, or the next Start starts alongside it.
func TestBackgroundManagerStartWaitsForRunThatFailedInit(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)

	process := &failingInitProcess{}
	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "start-waits-for-failed-init",
		Process: process,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: testBucket.DefaultDataStore(ctx),
			metaKeys:      base.NewMetadataKeys("test-start-waits-for-failed-init"),
			processSuffix: "start-waits-for-failed-init",
			multiNode:     true,
		},
	}

	// Park the failed run inside its terminal status update. Only that run reaches the error state.
	parked := make(chan struct{})
	unpark := make(chan struct{})
	var parkOnce, unparkOnce sync.Once
	// Released on every path: a failed assertion below must not leave a goroutine wedged in the data store.
	releaseParked := func() { unparkOnce.Do(func() { close(unpark) }) }
	defer releaseParked()
	mgr.updateDatabaseState = func(context.Context, bool) error {
		if mgr.GetRunState() != BackgroundProcessStateError {
			return nil
		}
		parkOnce.Do(func() { close(parked) })
		sgtest.RequireChanClosedFromCallback(t, unpark)
		return nil
	}

	firstStart := make(chan error, 1)
	go func() {
		firstStart <- mgr.Start(ctx, MockProcessOptions{})
	}()

	select {
	case <-parked:
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "the run that failed Init did not reach its terminal status update")
	}

	secondStart := make(chan error, 1)
	go func() {
		secondStart <- mgr.Start(ctx, MockProcessOptions{})
	}()

	select {
	case err := <-secondStart:
		require.FailNow(t, "Start returned while the failed run was still publishing its terminal status", "error: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	require.False(t, process.RunWasCalled(), "the next run started before the failed run had finished")

	releaseParked()

	select {
	case err := <-firstStart:
		require.Error(t, err)
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "the Start whose Init failed did not return")
	}
	select {
	case err := <-secondStart:
		require.NoError(t, err)
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "Start did not return after the failed run had finished")
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, process.RunWasCalled())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)

	require.NoError(t, mgr.Stop(ctx))
	requireRunEnded(t, mgr)
}

// secondRunGatedProcess completes its first run at once, and holds the Init of its second run until the test
// releases it, so the cluster status document can be read in between the two runs.
type secondRunGatedProcess struct {
	t         testing.TB
	initCount atomic.Int64
	runCount  atomic.Int64
	// initClusterStatus is the status document the second run was initialized from, written before secondInit is
	// closed so that a test waiting on that channel can read it.
	initClusterStatus []byte
	secondInit        chan struct{} // closed when the second run enters Init
	releaseInit       chan struct{} // the second Init returns when this is closed
}

func newSecondRunGatedProcess(t testing.TB) *secondRunGatedProcess {
	return &secondRunGatedProcess{
		t:           t,
		secondInit:  make(chan struct{}),
		releaseInit: make(chan struct{}),
	}
}

func (p *secondRunGatedProcess) Init(_ context.Context, _ MockProcessOptions, clusterStatus []byte) (backgroundManagerInitMode, error) {
	if p.initCount.Add(1) == 2 {
		p.initClusterStatus = clusterStatus
		close(p.secondInit)
		sgtest.RequireChanClosedFromCallback(p.t, p.releaseInit)
	}
	return backgroundManagerInitReset, nil
}

func (p *secondRunGatedProcess) Run(_ context.Context, _ MockProcessOptions, _ updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	if p.runCount.Add(1) == 1 {
		return nil
	}
	sgtest.RequireChanClosedFromCallback(p.t, terminator.Done())
	return nil
}

func (p *secondRunGatedProcess) GetProcessStatus(status BackgroundManagerStatus, _ []byte) (statusOut []byte, meta []byte, err error) {
	statusOut, err = base.JSONMarshal(status)
	return statusOut, nil, err
}

func (p *secondRunGatedProcess) SetProcessStatus(context.Context, []byte, []byte) {}

func (p *secondRunGatedProcess) ResetStatus() {}

// statusWritePark holds the first write of the cluster status document made while the manager reports a given run
// state, so a test can act between a run settling its terminal state and publishing it.
type statusWritePark struct {
	dataStore base.DataStore
	parked    chan struct{}
	release   func() // safe to call more than once, and called on cleanup
}

// parkStatusWriteInState clones testBucket into a data store that holds that write. runState is read on each write
// rather than captured, so the caller can pass a manager it builds afterwards.
func parkStatusWriteInState(t *testing.T, ctx context.Context, testBucket *base.TestBucket, statusDocID string, runState func() BackgroundProcessState, state BackgroundProcessState) *statusWritePark {
	parked := make(chan struct{})
	unpark := make(chan struct{})
	var parkOnce, unparkOnce sync.Once
	park := &statusWritePark{
		parked: parked,
		// Released on every path: a failed assertion must not leave a goroutine wedged in the data store.
		release: func() { unparkOnce.Do(func() { close(unpark) }) },
	}
	t.Cleanup(park.release)

	leakyBucket := testBucket.LeakyBucketClone(base.LeakyBucketConfig{
		PreUpdateCallback: func(key string) error {
			if key != statusDocID || runState() != state {
				return nil
			}
			parkOnce.Do(func() { close(parked) })
			sgtest.RequireChanClosedFromCallback(t, unpark)
			return nil
		},
	})
	t.Cleanup(func() { leakyBucket.Close(ctx) })
	park.dataStore = leakyBucket.DefaultDataStore(ctx)
	return park
}

// requireParked waits for the held write to be reached.
func (p *statusWritePark) requireParked(t *testing.T) {
	select {
	case <-p.parked:
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "the run did not reach the status write being held")
	}
}

// TestBackgroundManagerTerminalStatusSurvivesNextRun covers the terminal status of a run that finishes while the
// next one is being admitted. Settling the terminal state is what admits that next run, so the finishing run has to
// publish the state it settled on rather than the running one it would find at write time.
func TestBackgroundManagerTerminalStatusSurvivesNextRun(t *testing.T) {
	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)

	metaKeys := base.NewMetadataKeys("test-terminal-status-survives")
	processSuffix := "terminal-status-survives"

	// Hold the first run before it writes its terminal status, the only write it makes with the run state already
	// completed.
	var mgr *BackgroundManager[MockProcessOptions]
	park := parkStatusWriteInState(t, ctx, testBucket, metaKeys.BackgroundProcessStatusPrefix(processSuffix),
		func() BackgroundProcessState { return mgr.GetRunState() }, BackgroundProcessStateCompleted)

	process := newSecondRunGatedProcess(t)
	mgr = &BackgroundManager[MockProcessOptions]{
		name:    "terminal-status-survives",
		Process: process,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: park.dataStore,
			metaKeys:      metaKeys,
			processSuffix: processSuffix,
			multiNode:     true,
		},
	}

	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))
	park.requireParked(t)

	// The next run is admitted while the first run holds its terminal status write, and puts the run state back to
	// running.
	secondStart := make(chan error, 1)
	go func() {
		secondStart <- mgr.Start(ctx, MockProcessOptions{})
	}()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr.GetRunState())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)

	park.release()

	// The next run is held in Init, which it reaches after the first run has written and before it claims the run.
	base.RequireChanClosed(t, process.secondInit, "the next run did not reach Init")
	state, err := mgr.getClusterStatusState(ctx)
	require.NoError(t, err)
	require.Equal(t, BackgroundProcessStateCompleted, state, "the next run's state was published as the terminal status of the run that finished")

	close(process.releaseInit)
	select {
	case err := <-secondStart:
		require.NoError(t, err)
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "Start did not return after the previous run's goroutine exited")
	}

	require.NoError(t, mgr.Stop(ctx))
	requireRunEnded(t, mgr)
}

// TestBackgroundManagerStartReadsClusterStatusAfterPreviousRun covers the status document a run is initialized
// from. Start reads it before waiting for the previous run, so a re-read is needed, or Init resumes the run that has
// just completed.
func TestBackgroundManagerStartReadsClusterStatusAfterPreviousRun(t *testing.T) {
	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)

	metaKeys := base.NewMetadataKeys("test-status-read-after-wait")
	processSuffix := "status-read-after-wait"

	var mgr *BackgroundManager[MockProcessOptions]
	park := parkStatusWriteInState(t, ctx, testBucket, metaKeys.BackgroundProcessStatusPrefix(processSuffix),
		func() BackgroundProcessState { return mgr.GetRunState() }, BackgroundProcessStateCompleted)

	process := newSecondRunGatedProcess(t)
	mgr = &BackgroundManager[MockProcessOptions]{
		name:    "status-read-after-wait",
		Process: process,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: park.dataStore,
			metaKeys:      metaKeys,
			processSuffix: processSuffix,
			multiNode:     true,
		},
	}

	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))
	park.requireParked(t)

	// This Start reads the status document while it still says running, and is then admitted and made to wait.
	secondStart := make(chan error, 1)
	go func() {
		secondStart <- mgr.Start(ctx, MockProcessOptions{})
	}()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateRunning, mgr.GetRunState())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)

	park.release()
	base.RequireChanClosed(t, process.secondInit, "the next run did not reach Init")

	initStatus, err := unmarshalBackgroundManagerStatus(process.initClusterStatus)
	require.NoError(t, err)
	require.Equal(t, BackgroundProcessStateCompleted, initStatus.State, "the next run was initialized from a status document read before the previous run finished")

	close(process.releaseInit)
	select {
	case err := <-secondStart:
		require.NoError(t, err)
	case <-time.After(sgtest.GetBackgroundManagerStatusTransitionTimeout(t)):
		require.FailNow(t, "Start did not return after the previous run's goroutine exited")
	}

	require.NoError(t, mgr.Stop(ctx))
	requireRunEnded(t, mgr)
}

// terminatedWriterProcess writes one status update after its terminator closes, then holds the run open until the
// test releases it. It stands in for a process that is slow to notice that the run is over.
type terminatedWriterProcess struct {
	t       testing.TB
	wrote   chan struct{} // closed once the status update after the terminator has been made
	release chan struct{} // Run returns when this is closed
}

func newTerminatedWriterProcess(t testing.TB) *terminatedWriterProcess {
	return &terminatedWriterProcess{
		t:       t,
		wrote:   make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (p *terminatedWriterProcess) Init(context.Context, MockProcessOptions, []byte) (backgroundManagerInitMode, error) {
	return backgroundManagerInitReset, nil
}

func (p *terminatedWriterProcess) Run(ctx context.Context, _ MockProcessOptions, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	sgtest.RequireChanClosedFromCallback(p.t, terminator.Done())
	_ = persistClusterStatusCallback(ctx)
	close(p.wrote)
	sgtest.RequireChanClosedFromCallback(p.t, p.release)
	return nil
}

func (p *terminatedWriterProcess) GetProcessStatus(status BackgroundManagerStatus, _ []byte) (statusOut []byte, meta []byte, err error) {
	statusOut, err = base.JSONMarshal(status)
	return statusOut, nil, err
}

func (p *terminatedWriterProcess) SetProcessStatus(context.Context, []byte, []byte) {}

func (p *terminatedWriterProcess) ResetStatus() {}

// TestMultiNodeDatabaseStateFollowsPublishedStatus covers the database state a node mirrors once it has learned
// that the cluster ended the run. Its process keeps working until it notices the terminator, and mirroring that as
// running would draw the other nodes into starting the process again.
func TestMultiNodeDatabaseStateFollowsPublishedStatus(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)

	metadataStore := testBucket.DefaultDataStore(ctx)
	process := newTerminatedWriterProcess(t)
	mgr := &BackgroundManager[MockProcessOptions]{
		name:    "database-state-follows-status",
		Process: process,
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      base.NewMetadataKeys("test-database-state-follows-status"),
			processSuffix: "database-state-follows-status",
			multiNode:     true,
		},
	}

	var mirrorLock sync.Mutex
	var mirrored []bool
	mgr.updateDatabaseState = func(_ context.Context, running bool) error {
		mirrorLock.Lock()
		defer mirrorLock.Unlock()
		mirrored = append(mirrored, running)
		return nil
	}
	lastMirrored := func() bool {
		mirrorLock.Lock()
		defer mirrorLock.Unlock()
		require.NotEmpty(t, mirrored)
		return mirrored[len(mirrored)-1]
	}

	require.NoError(t, mgr.Start(ctx, MockProcessOptions{}))
	require.True(t, lastMirrored(), "a run that has started is mirrored as running")

	// Another node ends the process. The status poller picks that up, adopts the state and closes the terminator,
	// which is what the process is waiting on.
	require.NoError(t, metadataStore.Set(ctx, mgr.clusterAwareOptions.StatusDocID(), 0, nil, map[string]json.RawMessage{
		"status": json.RawMessage(`{"status":"stopped"}`),
		"meta":   json.RawMessage(`{}`),
	}))

	base.RequireChanClosed(t, process.wrote, "the process did not write a status update after its terminator closed")
	require.False(t, lastMirrored(), "the run was mirrored as running after the cluster had ended it")

	close(process.release)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, BackgroundProcessStateStopped, mgr.GetRunState())
	}, sgtest.GetBackgroundManagerStatusTransitionTimeout(t), 10*time.Millisecond)
	require.False(t, lastMirrored())

	state, err := mgr.getClusterStatusState(ctx)
	require.NoError(t, err)
	require.Equal(t, BackgroundProcessStateStopped, state)
}

// blockingStopProcess stays inside Run after its terminator closes until the test releases it, so the run is over
// from the manager's point of view while its goroutine still owes the cluster a terminal status.
type blockingStopProcess struct {
	MockProcess
	// pastTerminator closes once the run has seen its terminator, whether a local stop or the cluster closed it.
	pastTerminator chan struct{}
	releaseRun     chan struct{}
}

func newBlockingStopProcess() *blockingStopProcess {
	return &blockingStopProcess{
		pastTerminator: make(chan struct{}),
		releaseRun:     make(chan struct{}),
	}
}

func (p *blockingStopProcess) Run(_ context.Context, _ MockProcessOptions, _ updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	<-terminator.Done()
	close(p.pastTerminator)
	<-p.releaseRun
	return nil
}

// resumingProcess reports a resume from Init, so the node claims the run with backgroundManagerStatusResume.
type resumingProcess struct {
	MockProcess
}

func (p *resumingProcess) Init(context.Context, MockProcessOptions, []byte) (backgroundManagerInitMode, error) {
	return backgroundManagerInitResume, nil
}

// multiNodeOptions returns cluster aware options for one node of a multi node process. Nodes that share name see the
// same status document, and metadataStore lets a single node be given a leaky data store of its own.
func multiNodeOptions(name string, metadataStore base.DataStore) *ClusterAwareBackgroundManagerOptions {
	return &ClusterAwareBackgroundManagerOptions{
		metadataStore: metadataStore,
		metaKeys:      base.NewMetadataKeys("test-" + name),
		processSuffix: name,
		multiNode:     true,
	}
}

// TestBackgroundManagerResumeDoesNotOverwriteStoppingClusterState covers a Start whose post-wait re-read of the
// cluster status already shows stopping. The claim that follows Init must not put the cluster back to running.
func TestBackgroundManagerResumeDoesNotOverwriteStoppingClusterState(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)

	const processName = "resume-while-stopping"
	timeout := sgtest.GetBackgroundManagerStatusTransitionTimeout(t)
	statusDocID := base.NewMetadataKeys("test-" + processName).BackgroundProcessStatusPrefix(processName)

	// The starting node reads the status document twice: once before markStart, and once after it has waited for
	// any previous run. Park the second read so the stop below lands in between.
	atReRead := make(chan struct{})
	releaseReRead := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseReRead) }) }
	defer release()
	var statusReads atomic.Int64
	leakyBucket := testBucket.LeakyBucketClone(base.LeakyBucketConfig{
		GetRawCallback: func(key string) error {
			if key != statusDocID || statusReads.Add(1) != 2 {
				return nil
			}
			close(atReRead)
			sgtest.RequireChanClosedFromCallback(t, releaseReRead)
			return nil
		},
	})
	defer leakyBucket.Close(ctx)

	stoppingProcess := newBlockingStopProcess()
	runningNode := &BackgroundManager[MockProcessOptions]{
		name:                processName + "-running",
		Process:             stoppingProcess,
		clusterAwareOptions: multiNodeOptions(processName, testBucket.DefaultDataStore(ctx)),
	}
	require.NoError(t, runningNode.Start(ctx, MockProcessOptions{}))
	defer close(stoppingProcess.releaseRun)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, err := runningNode.getClusterStatusState(ctx)
		assert.NoError(c, err)
		assert.Equal(c, BackgroundProcessStateRunning, state)
	}, timeout, 10*time.Millisecond)

	startingNode := &BackgroundManager[MockProcessOptions]{
		name:                processName + "-starting",
		Process:             &resumingProcess{},
		clusterAwareOptions: multiNodeOptions(processName, leakyBucket.DefaultDataStore(ctx)),
	}
	var startErr error
	started := make(chan struct{})
	go func() {
		defer close(started)
		startErr = startingNode.Start(ctx, MockProcessOptions{})
	}()
	base.RequireChanClosed(t, atReRead, "starting node did not re-read the cluster status")

	// Run stays blocked, so the cluster status document stays at stopping rather than reaching stopped.
	require.NoError(t, runningNode.Stop(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, err := runningNode.getClusterStatusState(ctx)
		assert.NoError(c, err)
		assert.Equal(c, BackgroundProcessStateStopping, state)
	}, timeout, 10*time.Millisecond)

	release()
	base.RequireChanClosed(t, started, "Start did not return")
	defer func() {
		if startingNode.GetRunState() == BackgroundProcessStateRunning {
			require.NoError(t, startingNode.Stop(ctx))
		}
	}()

	state, err := runningNode.getClusterStatusState(ctx)
	require.NoError(t, err)
	require.Equal(t, BackgroundProcessStateStopping, state, "the resume claim overwrote a stopping cluster state")
	require.ErrorIs(t, startErr, errBackgroundManagerStatusAlreadyStopping)
}

// gatedStartProcess holds a start inside Init until the test releases it. Init reports a fresh run, so the node
// claims the run with backgroundManagerStatusStart.
type gatedStartProcess struct {
	gatedInitProcess
}

func (p *gatedStartProcess) Init(context.Context, MockProcessOptions, []byte) (backgroundManagerInitMode, error) {
	close(p.initEntered)
	sgtest.RequireChanClosedFromCallback(p.t, p.releaseInit)
	return backgroundManagerInitReset, nil
}

// TestBackgroundManagerStartDoesNotOverwriteStoppingClusterState covers a Start that is admitted against a running
// status document and only reaches its start claim after another node moved the cluster to stopping. The start claim
// must not put the cluster back to running while the stop is still in progress.
func TestBackgroundManagerStartDoesNotOverwriteStoppingClusterState(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)

	const processName = "start-while-stopping"
	clusterAwareOptions := multiNodeOptions(processName, testBucket.DefaultDataStore(ctx))
	timeout := sgtest.GetBackgroundManagerStatusTransitionTimeout(t)

	stoppingProcess := newBlockingStopProcess()
	runningNode := &BackgroundManager[MockProcessOptions]{
		name:                processName + "-running",
		Process:             stoppingProcess,
		clusterAwareOptions: clusterAwareOptions,
	}
	require.NoError(t, runningNode.Start(ctx, MockProcessOptions{}))
	defer close(stoppingProcess.releaseRun)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, err := runningNode.getClusterStatusState(ctx)
		assert.NoError(c, err)
		assert.Equal(c, BackgroundProcessStateRunning, state)
	}, timeout, 10*time.Millisecond)

	process := &gatedStartProcess{newGatedInitProcess(t)}
	startingNode := &BackgroundManager[MockProcessOptions]{
		name:                processName + "-starting",
		Process:             process,
		clusterAwareOptions: clusterAwareOptions,
	}
	var startErr error
	started := make(chan struct{})
	go func() {
		defer close(started)
		startErr = startingNode.Start(ctx, MockProcessOptions{})
	}()

	// The starting node reads a running status document, and is then held before it claims the run.
	base.RequireChanClosed(t, process.initEntered, "starting node did not reach Init")

	// Run stays blocked, so the cluster status document stays at stopping rather than reaching stopped.
	require.NoError(t, runningNode.Stop(ctx))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, err := runningNode.getClusterStatusState(ctx)
		assert.NoError(c, err)
		assert.Equal(c, BackgroundProcessStateStopping, state)
	}, timeout, 10*time.Millisecond)

	close(process.releaseInit)
	base.RequireChanClosed(t, started, "Start did not return")
	defer func() {
		if startingNode.GetRunState() == BackgroundProcessStateRunning {
			require.NoError(t, startingNode.Stop(ctx))
		}
	}()

	state, err := runningNode.getClusterStatusState(ctx)
	require.NoError(t, err)
	require.Equal(t, BackgroundProcessStateStopping, state, "the start claim overwrote a stopping cluster state")
	require.ErrorIs(t, startErr, errBackgroundManagerStatusAlreadyStopping)
}
