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
	metadataStore := testBucket.DefaultDataStore()
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
	testBucket := base.GetTestBucket(t)
	ctx := context.Background()
	defer testBucket.Close(ctx)
	metadataStore := testBucket.DefaultDataStore()
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
	metadataStore := testBucket.DefaultDataStore()
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
	metadataStore := testBucket.DefaultDataStore()
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
	metadataStore := testBucket.DefaultDataStore()
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
		nodeA.docsProcessedSinceLastUpdate.Add(1)
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
	assert.Equal(t, int64(10), nodeA.docsProcessedSerialized.Load())
	assert.Equal(t, int64(0), nodeA.docsProcessedSinceLastUpdate.Load())
	assert.Equal(t, int64(10), nodeA.DocsProcessed())

	// 4. Node B processes 5 docs.
	// Node B hasn't polled yet, so its serialized is 0.
	for i := 0; i < 5; i++ {
		nodeB.docsProcessedSinceLastUpdate.Add(1)
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

	require.Equal(t, uint64(15), respB.DocsProcessed)

}
