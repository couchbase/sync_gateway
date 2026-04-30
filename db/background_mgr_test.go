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

func (m *MockProcess) GetProcessStatus(status BackgroundManagerStatus) (statusOut []byte, meta []byte, err error) {
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
				MultiNode:     true,
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
		MultiNode:     true,
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
		MultiNode:     true,
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
		MultiNode:     false,
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

	require.Equal(t, origStartTime, mgr2.getStartTime(), "mgr2 should have reused the start time from the cluster status doc")
}
