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
	testBucket      *base.TestBucket
	restTesters     []*RestTester
	roundRobinCount int64
	config          *RestTesterClusterConfig
	groupID         string
}

// RefreshClusterDbConfigs will synchronously fetch the latest db configs from each bucket for each RestTester.
func (rtc *RestTesterCluster) RefreshClusterDbConfigs() (count int, err error) {
	for _, rt := range rtc.restTesters {
		c, err := rt.ServerContext().fetchAndLoadConfigs(rt.Context(), false)
		if err != nil {
			return 0, err
		}
		count += c
	}
	return count, nil
}

func (rtc *RestTesterCluster) NumNodes() int {
	return len(rtc.restTesters)
}

// ForEachNode runs the given function on each RestTester node.
func (rtc *RestTesterCluster) ForEachNode(fn func(rt *RestTester)) {
	for _, rt := range rtc.restTesters {
		fn(rt)
	}
}

// RoundRobin returns the next RestTester instance, cycling through all of them sequentially.
func (rtc *RestTesterCluster) RoundRobin() *RestTester {
	requestNum := atomic.AddInt64(&rtc.roundRobinCount, 1) % int64(len(rtc.restTesters))
	node := requestNum % int64(len(rtc.restTesters))
	return rtc.restTesters[node]
}

// Node returns a specific RestTester instance.
func (rtc *RestTesterCluster) Node(i int) *RestTester {
	return rtc.restTesters[i]
}

// Close closes all of RestTester nodes and the shared TestBucket.
func (rtc *RestTesterCluster) Close(ctx context.Context) {
	for _, rt := range rtc.restTesters {
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
		testBucket:  tb,
		restTesters: restTesters,
		config:      config,
		groupID:     groupID,
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
		UseViews:     base.Ptr(base.TestsDisableGSI()),
		EnableXattrs: base.Ptr(base.TestUseXattrs()),
	}
}
