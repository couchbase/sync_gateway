// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/google/uuid"
)

// RestTesterCluster can be used to simulate a multi-node Sync Gateway cluster.
type RestTesterCluster struct {
	t               *testing.T
	testBucket      *base.TestBucket
	restTestersLock sync.RWMutex // guards _restTesters, since AddNode can append after construction, concurrently with round-robin readers
	_restTesters    []*RestTester
	roundRobinCount int64
	config          *RestTesterClusterConfig
	groupID         string
}

// nodes returns the current set of RestTester nodes in the cluster.
func (rtc *RestTesterCluster) nodes() []*RestTester {
	rtc.restTestersLock.RLock()
	defer rtc.restTestersLock.RUnlock()
	return rtc._restTesters
}

// RefreshClusterDbConfigs will synchronously fetch the latest db configs from each bucket for each RestTester.
func (rtc *RestTesterCluster) RefreshClusterDbConfigs() (count int, err error) {
	for _, rt := range rtc.nodes() {
		c, err := rt.ServerContext().fetchAndLoadConfigs(rt.Context(), false)
		if err != nil {
			return 0, err
		}
		count += c
	}
	return count, nil
}

func (rtc *RestTesterCluster) NumNodes() int {
	return len(rtc.nodes())
}

// ForEachNode runs the given function on each RestTester node.
func (rtc *RestTesterCluster) ForEachNode(fn func(rt *RestTester)) {
	for _, rt := range rtc.nodes() {
		fn(rt)
	}
}

// RoundRobin returns the next RestTester instance, cycling through all of them sequentially.
func (rtc *RestTesterCluster) RoundRobin() *RestTester {
	nodes := rtc.nodes()
	requestNum := atomic.AddInt64(&rtc.roundRobinCount, 1) % int64(len(nodes))
	node := requestNum % int64(len(nodes))
	return nodes[node]
}

// Node returns a specific RestTester instance.
func (rtc *RestTesterCluster) Node(i int) *RestTester {
	return rtc.nodes()[i]
}

// AddNode starts a new RestTester node sharing the cluster's bucket and group ID, and waits for
// it to discover every database already running on the rest of the cluster before returning.
func (rtc *RestTesterCluster) AddNode() *RestTester {
	nodes := rtc.nodes()
	expectedDbNames := nodes[0].ServerContext().AllDatabaseNames()

	rtConfig := &RestTesterConfig{
		GroupID:             &rtc.groupID,
		PersistentConfig:    true,
		CustomTestBucket:    rtc.testBucket.NoCloseClone(),
		MutateStartupConfig: rtc.config.MutateStartupConfig,
	}
	rt := NewRestTester(rtc.t, rtConfig)
	sc := rt.ServerContext()

	_, err := sc.fetchAndLoadConfigs(rt.Context(), false)
	require.NoError(rtc.t, err)
	require.ElementsMatch(rtc.t, expectedDbNames, sc.AllDatabaseNames(), "new node did not discover the same databases as the rest of the cluster")

	rtc.restTestersLock.Lock()
	rtc._restTesters = append(rtc._restTesters, rt)
	rtc.restTestersLock.Unlock()
	return rt
}

// Close closes all of RestTester nodes and the shared TestBucket.
func (rtc *RestTesterCluster) Close(ctx context.Context) {
	for _, rt := range rtc.nodes() {
		rt.Close()
	}
	rtc.testBucket.Close(ctx)
}

// RestTesterClusterConfig are options to create multiple RestTester objects backed by the same bucket.
type RestTesterClusterConfig struct {
	NumNodes            uint8                // Number of RestTester objects to create
	MutateStartupConfig func(*StartupConfig) // Passes this option to the RestTesterConfig for each RestTester
}

func defaultRestTesterClusterConfig() *RestTesterClusterConfig {
	return &RestTesterClusterConfig{
		NumNodes: 3,
	}
}

func NewRestTesterCluster(t *testing.T, config *RestTesterClusterConfig) *RestTesterCluster {
	if config == nil {
		config = defaultRestTesterClusterConfig()
	}

	require.NotZero(t, config.NumNodes)

	groupID := uuid.NewString()

	tb := base.GetTestBucket(t)

	// Start up all rest testers in parallel
	wg := sync.WaitGroup{}
	restTesters := make([]*RestTester, config.NumNodes)
	for i := range config.NumNodes {
		wg.Go(func() {
			// RestTesterConfig is mutated by NewRestTester, make a new instance in each loop
			rtConfig := &RestTesterConfig{
				GroupID:             &groupID,
				PersistentConfig:    true,
				CustomTestBucket:    tb.NoCloseClone(),
				MutateStartupConfig: config.MutateStartupConfig,
			}
			rt := NewRestTester(t, rtConfig)
			// initialize the RestTester before we attempt to use it
			_ = rt.ServerContext()
			restTesters[i] = rt
		})
	}
	wg.Wait()

	return &RestTesterCluster{
		t:            t,
		testBucket:   tb,
		_restTesters: restTesters,
		config:       config,
		groupID:      groupID,
	}
}

// dbConfigForTestBucket returns a barebones DbConfig for the given TestBucket.
func dbConfigForTestBucket(tb *base.TestBucket) DbConfig {
	return DbConfig{
		BucketConfig: BucketConfig{
			Bucket: base.Ptr(tb.GetName()),
		},
		Index: &IndexConfig{
			NumReplicas: base.Ptr(uint(0)),
		},
		UseViews: base.Ptr(base.TestsDisableGSI()),
	}
}
