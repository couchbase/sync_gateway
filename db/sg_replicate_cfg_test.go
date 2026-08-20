/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// Test node operations on SGReplicateManager
func TestReplicateManagerReplications(t *testing.T) {

	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	testCfg, err := base.NewCfgSG(ctx, testBucket.GetSingleDataStore(), "", false)
	require.NoError(t, err)

	manager, err := NewSGReplicateManager(ctx, &DatabaseContext{Name: "test"}, testCfg)
	require.NoError(t, err)
	defer manager.Stop()

	replication1_id := "replication1"
	err = manager.AddReplication(testReplicationCfg(replication1_id, ""))
	require.NoError(t, err)

	r, err := manager.GetReplication(replication1_id)
	require.NoError(t, err)
	assert.Equal(t, replication1_id, r.ID)

	// Request non-existent replication
	_, err = manager.GetReplication("dne")
	require.Error(t, err, base.ErrNotFound)

	// Attempt to add existing replication
	err = manager.AddReplication(testReplicationCfg(replication1_id, ""))
	require.Error(t, err, base.ErrAlreadyExists)

	// Add a second replication
	replication2_id := "replication2"
	err = manager.AddReplication(testReplicationCfg(replication2_id, ""))
	require.NoError(t, err)

	r, err = manager.GetReplication(replication1_id)
	require.NoError(t, err)
	assert.Equal(t, replication1_id, r.ID)

	replications, err := manager.GetReplications()
	require.NoError(t, err)
	assert.Len(t, replications, 2)

	// Remove replication
	err = manager.DeleteReplication(replication1_id)
	require.NoError(t, err)
	replications, err = manager.GetReplications()
	require.NoError(t, err)
	assert.Len(t, replications, 1)

	// Remove non-existent replication
	err = manager.DeleteReplication(replication1_id)
	require.Error(t, base.ErrNotFound, err)
	replications, err = manager.GetReplications()
	require.NoError(t, err)
	assert.Len(t, replications, 1)

	// Remove last replication
	err = manager.DeleteReplication(replication2_id)
	require.NoError(t, err)
	replications, err = manager.GetReplications()
	require.NoError(t, err)
	assert.Len(t, replications, 0)
}

// Test node operations on SGReplicateManager
func TestReplicateManagerNodes(t *testing.T) {

	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	testCfg, err := base.NewCfgSG(ctx, testBucket.GetSingleDataStore(), "", false)
	require.NoError(t, err)

	manager, err := NewSGReplicateManager(ctx, &DatabaseContext{Name: "test"}, testCfg)
	require.NoError(t, err)
	defer manager.Stop()

	err = manager.registerNodeForHost("node1", "host1")
	require.NoError(t, err)

	nodes, err := manager.getNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 1)

	err = manager.registerNodeForHost("node2", "host2")
	require.NoError(t, err)

	nodes, err = manager.getNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 2)

	// re-adding an existing node is a no-op
	err = manager.registerNodeForHost("node1", "host1")
	require.NoError(t, err)

	nodes, err = manager.getNodes()
	require.NoError(t, err)
	assert.Len(t, nodes, 2)

	// Remove node
	err = manager.RemoveNode("node1")
	require.NoError(t, err)

	nodes, err = manager.getNodes()
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	node2, ok := nodes["node2"]
	require.True(t, ok)
	require.Equal(t, node2.UUID, "node2")

	// Removing an already removed node is a no-op
	err = manager.RemoveNode("node1")
	require.NoError(t, err)

	replications, err := manager.GetReplications()
	require.NoError(t, err)
	assert.Len(t, replications, 0)
}

// TestReplicateManagerRegisterNodeRefreshesVersion verifies registerNodeForHost's in-place
// Version refresh path: if an SGNode already exists in the cluster config without a Version
// (the state a pre-4.1 peer would have left behind), a CCV-aware re-registration must overwrite
// it with the local build's ProductVersion so subsequent observers correctly classify this
// node as CCV-aware. Re-registering with a matching Version remains a no-op.
func TestReplicateManagerRegisterNodeRefreshesVersion(t *testing.T) {

	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	testCfg, err := base.NewCfgSG(ctx, testBucket.GetSingleDataStore(), "", false)
	require.NoError(t, err)

	manager, err := NewSGReplicateManager(ctx, &DatabaseContext{Name: "test"}, testCfg)
	require.NoError(t, err)
	defer manager.Stop()

	const nodeUUID = "legacy-style-node"
	// Seed the cluster with a Version-less entry (simulates a pre-4.1 peer that wrote itself
	// before SGNode.Version existed).
	err = manager.updateCluster(func(cluster *SGRCluster) (cancel bool, err error) {
		cluster.Nodes[nodeUUID] = &SGNode{UUID: nodeUUID, Host: "legacy-host"}
		return false, nil
	})
	require.NoError(t, err)

	nodes, err := manager.getNodes()
	require.NoError(t, err)
	require.Contains(t, maps.Keys(nodes), nodeUUID)
	require.Nil(t, nodes[nodeUUID].Version, "seeded entry should have no Version")

	// First refresh: Version was nil → must be set to ProductVersion.
	require.NoError(t, manager.registerNodeForHost(nodeUUID, "legacy-host"))
	nodes, err = manager.getNodes()
	require.NoError(t, err)
	require.NotNil(t, nodes[nodeUUID].Version, "Version must be set after refresh")
	assert.True(t, nodes[nodeUUID].Version.Equal(base.ProductVersion), "Version must match local build")

	// Second refresh: Version is already current → no-op, must not error.
	require.NoError(t, manager.registerNodeForHost(nodeUUID, "legacy-host"))
	nodes, err = manager.getNodes()
	require.NoError(t, err)
	assert.True(t, nodes[nodeUUID].Version.Equal(base.ProductVersion), "idempotent re-registration must not perturb Version")
}

// Test concurrent node operations on SGReplicateManager
func TestReplicateManagerConcurrentNodeOperations(t *testing.T) {

	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	testCfg, err := base.NewCfgSG(ctx, testBucket.GetSingleDataStore(), "", false)
	require.NoError(t, err)

	manager, err := NewSGReplicateManager(ctx, &DatabaseContext{Name: "test"}, testCfg)
	require.NoError(t, err)
	defer manager.Stop()

	var nodeWg sync.WaitGroup

	for i := range 20 {
		nodeWg.Add(1)
		go func(i int) {
			defer nodeWg.Done()
			err := manager.registerNodeForHost(fmt.Sprintf("node_%d", i), fmt.Sprintf("host_%d", i))
			assert.NoError(t, err)
		}(i)
	}

	nodeWg.Wait()
	nodes, err := manager.getNodes()
	require.NoError(t, err)
	require.Len(t, nodes, 20)

	for i := range 20 {
		nodeWg.Add(1)
		go func(i int) {
			defer nodeWg.Done()
			err := manager.RemoveNode(fmt.Sprintf("node_%d", i))
			assert.NoError(t, err)
		}(i)
	}

	nodeWg.Wait()
	nodes, err = manager.getNodes()
	require.NoError(t, err)
	require.Len(t, nodes, 0)
}

// Test concurrent replication operations on SGReplicateManager
func TestReplicateManagerConcurrentReplicationOperations(t *testing.T) {

	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	testCfg, err := base.NewCfgSG(ctx, testBucket.GetSingleDataStore(), "", false)
	require.NoError(t, err)

	manager, err := NewSGReplicateManager(ctx, &DatabaseContext{Name: "test"}, testCfg)
	require.NoError(t, err)
	defer manager.Stop()

	var replicationWg sync.WaitGroup

	for i := range 20 {
		replicationWg.Add(1)
		go func(i int) {
			defer replicationWg.Done()
			err := manager.AddReplication(testReplicationCfg(fmt.Sprintf("r_%d", i), ""))
			assert.NoError(t, err)
		}(i)
	}

	replicationWg.Wait()
	replications, err := manager.GetReplications()
	require.NoError(t, err)
	require.Len(t, replications, 20)

	for i := range 20 {
		replicationWg.Add(1)
		go func(i int) {
			defer replicationWg.Done()
			err := manager.DeleteReplication(fmt.Sprintf("r_%d", i))
			assert.NoError(t, err)
		}(i)
	}

	replicationWg.Wait()
	replications, err = manager.GetReplications()
	require.NoError(t, err)
	require.Len(t, replications, 0)
}

func testReplicationCfg(id, assignedNode string) *ReplicationCfg {
	return &ReplicationCfg{
		ReplicationConfig: ReplicationConfig{ID: id},
		AssignedNode:      assignedNode,
	}
}

func TestRebalanceReplications(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate)

	type rebalanceTest struct {
		name                  string                     // Test name
		nodes                 map[string]*SGNode         // Initial node set
		replications          map[string]*ReplicationCfg // Initial replication assignment
		expectedMinPerNode    int                        // Minimum replications per node after rebalance
		expectedMaxPerNode    int                        // Maximum replications per node after rebalance
		expectedTotalAssigned int                        // Expected total number of assigned replications post-rebalance
	}
	testCases := []rebalanceTest{
		{
			name: "new nodes",
			nodes: map[string]*SGNode{
				"n1": {UUID: "n1"},
				"n2": {UUID: "n2"},
				"n3": {UUID: "n3"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": testReplicationCfg("r1", "n1"),
				"r2": testReplicationCfg("r2", "n1"),
				"r3": testReplicationCfg("r3", "n1"),
			},
			expectedMinPerNode:    1,
			expectedMaxPerNode:    1,
			expectedTotalAssigned: 3,
		},
		{
			name: "new replications",
			nodes: map[string]*SGNode{
				"n1": {UUID: "n1"},
				"n2": {UUID: "n2"},
				"n3": {UUID: "n3"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": testReplicationCfg("r1", ""),
				"r2": testReplicationCfg("r2", ""),
				"r3": testReplicationCfg("r3", ""),
			},
			expectedMinPerNode:    1,
			expectedMaxPerNode:    1,
			expectedTotalAssigned: 3,
		},
		{
			name: "remove nodes",
			nodes: map[string]*SGNode{
				"n1": {UUID: "n1"},
				"n2": {UUID: "n2"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": testReplicationCfg("r1", "n1"),
				"r2": testReplicationCfg("r2", "n2"),
				"r3": testReplicationCfg("r3", "n3"),
				"r4": testReplicationCfg("r4", "n4"),
			},
			expectedMinPerNode:    2,
			expectedMaxPerNode:    2,
			expectedTotalAssigned: 4,
		},
		{
			name:  "no nodes",
			nodes: map[string]*SGNode{},
			replications: map[string]*ReplicationCfg{
				"r1": testReplicationCfg("r1", "n1"),
				"r2": testReplicationCfg("r2", "n1"),
				"r3": testReplicationCfg("r3", "n2"),
			},
			expectedMinPerNode:    0,
			expectedMaxPerNode:    0,
			expectedTotalAssigned: 0,
		},
		{
			name: "single node",
			nodes: map[string]*SGNode{
				"n1": {UUID: "n1"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": testReplicationCfg("r1", "n1"),
				"r2": testReplicationCfg("r2", "n2"),
				"r3": testReplicationCfg("r3", ""),
			},
			expectedMinPerNode:    3,
			expectedMaxPerNode:    3,
			expectedTotalAssigned: 3,
		},
		{
			name: "unbalanced distribution",
			nodes: map[string]*SGNode{
				"n1": {UUID: "n1"},
				"n2": {UUID: "n2"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": testReplicationCfg("r1", "n1"),
				"r2": testReplicationCfg("r2", "n1"),
				"r3": testReplicationCfg("r3", "n1"),
			},
			expectedMinPerNode:    1,
			expectedMaxPerNode:    2,
			expectedTotalAssigned: 3,
		},
		{
			name: "multiple reassignments new nodes",
			nodes: map[string]*SGNode{
				"n1": {UUID: "n1"},
				"n2": {UUID: "n2"},
				"n3": {UUID: "n3"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": testReplicationCfg("r1", "n1"),
				"r2": testReplicationCfg("r2", "n1"),
				"r3": testReplicationCfg("r3", "n1"),
				"r4": testReplicationCfg("r4", "n1"),
				"r5": testReplicationCfg("r5", "n1"),
				"r6": testReplicationCfg("r6", "n1"),
			},
			expectedMinPerNode:    2,
			expectedMaxPerNode:    2,
			expectedTotalAssigned: 6,
		},
		{
			name: "multiple reassignments new replications",
			nodes: map[string]*SGNode{
				"n1": {UUID: "n1"},
				"n2": {UUID: "n2"},
				"n3": {UUID: "n3"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": testReplicationCfg("r1", ""),
				"r2": testReplicationCfg("r2", ""),
				"r3": testReplicationCfg("r3", ""),
				"r4": testReplicationCfg("r4", ""),
				"r5": testReplicationCfg("r5", "n1"),
				"r6": testReplicationCfg("r6", "n1"),
			},
			expectedMinPerNode:    2,
			expectedMaxPerNode:    2,
			expectedTotalAssigned: 6,
		},
		{
			name: "reassignment from unknown host",
			nodes: map[string]*SGNode{
				"n1": {UUID: "n1"},
				"n2": {UUID: "n2"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": testReplicationCfg("r1", "n3"),
				"r2": testReplicationCfg("r2", "n3"),
				"r3": testReplicationCfg("r3", "n3"),
				"r4": testReplicationCfg("r4", "n3"),
			},
			expectedMinPerNode:    2,
			expectedMaxPerNode:    2,
			expectedTotalAssigned: 4,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {

			cluster := NewSGRCluster()
			cluster.loggingCtx = base.CorrelationIDLogCtx(base.TestCtx(t), sgrClusterMgrContextID+"test")
			cluster.Nodes = testCase.nodes
			cluster.Replications = testCase.replications
			cluster.RebalanceReplications()

			// Verify post-rebalance distribution
			for host, _ := range cluster.Nodes {
				nodeReplications := cluster.GetReplicationIDsForNode(host)
				assert.True(t, len(nodeReplications) >= testCase.expectedMinPerNode)
				assert.True(t, len(nodeReplications) <= testCase.expectedMaxPerNode)
			}

			// Verify replications are all assigned
			assignedCount := 0
			for _, replication := range cluster.Replications {
				if replication.AssignedNode != "" {
					assignedCount++
				}
			}
			assert.Equal(t, testCase.expectedTotalAssigned, assignedCount)
		})
	}
}

func TestUpsertReplicationConfig(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate)

	type rebalanceTest struct {
		name           string                   // Test name
		existingConfig *ReplicationConfig       // Initial replication definition
		updatedConfig  *ReplicationUpsertConfig // Initial replication assignment
		expectedConfig *ReplicationConfig       // Minimum replications per node after rebalance
	}
	testCases := []rebalanceTest{
		{
			name: "modify string parameter",
			existingConfig: &ReplicationConfig{
				ID:        "foo",
				Remote:    "remote",
				Direction: "pull",
			},
			updatedConfig: &ReplicationUpsertConfig{
				Direction: base.Ptr("push"),
			},
			expectedConfig: &ReplicationConfig{
				ID:        "foo",
				Remote:    "remote",
				Direction: "push",
			},
		},
		{
			name: "remove string parameter",
			existingConfig: &ReplicationConfig{
				ID:                   "foo",
				Remote:               "remote",
				Direction:            "pull",
				ConflictResolutionFn: "func(){}",
			},
			updatedConfig: &ReplicationUpsertConfig{
				ConflictResolutionFn: base.Ptr(""),
			},
			expectedConfig: &ReplicationConfig{
				ID:                   "foo",
				Remote:               "remote",
				Direction:            "pull",
				ConflictResolutionFn: "",
			},
		},
		{
			name: "switch QueryParams type",
			existingConfig: &ReplicationConfig{
				ID:          "foo",
				Remote:      "remote",
				Direction:   "pull",
				QueryParams: []string{"ABC"},
			},
			updatedConfig: &ReplicationUpsertConfig{
				QueryParams: map[string]any{"ABC": true},
			},
			expectedConfig: &ReplicationConfig{
				ID:          "foo",
				Remote:      "remote",
				Direction:   "pull",
				QueryParams: map[string]any{"ABC": true},
			},
		},
		{
			name: "modify all",
			existingConfig: &ReplicationConfig{
				ID:                     "foo",
				Remote:                 "a",
				Direction:              "a",
				ConflictResolutionType: "a",
				ConflictResolutionFn:   "a",
				PurgeOnRemoval:         true,
				DeltaSyncEnabled:       true,
				MaxBackoff:             5,
				InitialState:           "a",
				Continuous:             true,
				Filter:                 "a",
				QueryParams:            []any{"ABC"},
			},
			updatedConfig: &ReplicationUpsertConfig{
				ID:                     "foo",
				Remote:                 base.Ptr("b"),
				Direction:              base.Ptr("b"),
				ConflictResolutionType: base.Ptr("b"),
				ConflictResolutionFn:   base.Ptr("b"),
				PurgeOnRemoval:         base.Ptr(false),
				DeltaSyncEnabled:       base.Ptr(false),
				MaxBackoff:             base.Ptr(10),
				InitialState:           base.Ptr("b"),
				Continuous:             base.Ptr(false),
				Filter:                 base.Ptr("b"),
				QueryParams:            []any{"DEF"},
			},
			expectedConfig: &ReplicationConfig{
				ID:                     "foo",
				Remote:                 "b",
				Direction:              "b",
				ConflictResolutionType: "b",
				ConflictResolutionFn:   "b",
				PurgeOnRemoval:         false,
				DeltaSyncEnabled:       false,
				MaxBackoff:             10,
				InitialState:           "b",
				Continuous:             false,
				Filter:                 "b",
				QueryParams:            []any{"DEF"},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.existingConfig.Upsert(base.TestCtx(t), testCase.updatedConfig)
			testCase.existingConfig.UpdatedAt = nil // remove updated at field for comparison below
			equal, err := testCase.existingConfig.Equals(testCase.expectedConfig)
			assert.NoError(t, err)
			assert.True(t, equal)
		})
	}
}

func TestIsCfgChanged(t *testing.T) {

	getInitialCfg := func() *ReplicationCfg {
		return &ReplicationCfg{
			ReplicationConfig: ReplicationConfig{
				ID:                     "foo",
				Remote:                 "a",
				Direction:              ActiveReplicatorTypePull,
				ConflictResolutionType: ConflictResolverCustom,
				ConflictResolutionFn:   "a",
				PurgeOnRemoval:         true,
				DeltaSyncEnabled:       true,
				MaxBackoff:             5,
				InitialState:           "a",
				Continuous:             true,
				Filter:                 "a",
				QueryParams:            []any{"ABC"},
				Username:               "alice",
				Password:               "password",
				CollectionsLocal:       []string{"foo.bar"},
			},
		}
	}

	type cfgChangedTest struct {
		name            string                   // Test name
		updatedConfig   *ReplicationUpsertConfig // Updated replication config
		expectedChanged bool
	}
	testCases := []cfgChangedTest{
		{
			name: "remoteChanged",
			updatedConfig: &ReplicationUpsertConfig{
				Remote: base.Ptr("b"),
			},
			expectedChanged: true,
		},
		{
			name: "directionChanged",
			updatedConfig: &ReplicationUpsertConfig{
				Direction: base.Ptr(string(ActiveReplicatorTypePushAndPull)),
			},
			expectedChanged: true,
		},
		{
			name: "conflictResolverChanged",
			updatedConfig: &ReplicationUpsertConfig{
				ConflictResolutionType: base.Ptr(string(ConflictResolverDefault)),
			},
			expectedChanged: true,
		},
		{
			name: "conflictResolverFnChange",
			updatedConfig: &ReplicationUpsertConfig{
				ConflictResolutionFn: base.Ptr("b"),
			},
			expectedChanged: true,
		},
		{
			name: "passwordChanged", // Verify fix CBG-1858
			updatedConfig: &ReplicationUpsertConfig{
				Password: base.Ptr("changed"),
			},
			expectedChanged: true,
		},
		{
			name: "collections enabled",
			updatedConfig: &ReplicationUpsertConfig{
				CollectionsEnabled: base.Ptr(true),
			},
			expectedChanged: true,
		},
		{
			name: "collections local",
			updatedConfig: &ReplicationUpsertConfig{
				CollectionsLocal: []string{"foo.bar", "bar.buzz"},
			},
			expectedChanged: true,
		},
		{
			name: "collections local",
			updatedConfig: &ReplicationUpsertConfig{
				CollectionsLocal: []string{"foo.bar", "bar.buzz"},
			},
			expectedChanged: true,
		},
		{
			name: "unchanged",
			updatedConfig: &ReplicationUpsertConfig{
				Remote:               base.Ptr("a"),
				ConflictResolutionFn: base.Ptr("a"),
				CollectionsLocal:     []string{"foo.bar"},
			},
			expectedChanged: false,
		},
	}

	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	testCfg, err := base.NewCfgSG(ctx, testBucket.GetSingleDataStore(), "", false)
	require.NoError(t, err)

	mgr, err := NewSGReplicateManager(ctx, &DatabaseContext{Name: "test"}, testCfg)
	require.NoError(t, err)
	defer mgr.Stop()

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			replicationCfg := getInitialCfg()
			replicatorConfig, err := mgr.NewActiveReplicatorConfig(replicationCfg)
			require.NoError(t, err)

			replicationCfg.Upsert(ctx, testCase.updatedConfig)

			isChanged, err := mgr.isCfgChanged(replicationCfg, replicatorConfig)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedChanged, isChanged)
		})
	}

}

// Test replicators assigned nodes with different group IDs
func TestReplicateGroupIDAssignedNodes(t *testing.T) {
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	// scopes config will set up from test environment whether backed by default or non default collection
	scopesConfig := GetScopesOptions(t, tb, 1)
	// Set up databases
	dbDefault, err := NewDatabaseContext(ctx, "default", tb.NoCloseClone(), false, DatabaseContextOptions{GroupID: "", Scopes: scopesConfig})
	require.NoError(t, err)
	defer dbDefault.Close(ctx)
	ctx = dbDefault.AddDatabaseLogContext(ctx)
	err = dbDefault.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	dbGroupA, err := NewDatabaseContext(ctx, "groupa", tb.NoCloseClone(), false, DatabaseContextOptions{GroupID: "GroupA", Scopes: scopesConfig})
	require.NoError(t, err)
	defer dbGroupA.Close(ctx)
	ctx = dbGroupA.AddDatabaseLogContext(ctx)
	err = dbGroupA.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	dbGroupB, err := NewDatabaseContext(ctx, "groupb", tb.NoCloseClone(), false, DatabaseContextOptions{GroupID: "GroupB", Scopes: scopesConfig})
	require.NoError(t, err)
	defer dbGroupB.Close(ctx)
	ctx = dbGroupB.AddDatabaseLogContext(ctx)
	err = dbGroupB.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	// Set up replicators
	err = dbDefault.SGReplicateMgr.RegisterNode("nodeDefault")
	require.NoError(t, err)
	err = dbDefault.SGReplicateMgr.AddReplication(&ReplicationCfg{
		ReplicationConfig: ReplicationConfig{
			ID:           "repl",
			InitialState: ReplicationStateStopped,
		},
	})
	require.NoError(t, err)

	err = dbGroupA.SGReplicateMgr.RegisterNode("nodeGroupA")
	require.NoError(t, err)
	err = dbGroupA.SGReplicateMgr.AddReplication(&ReplicationCfg{
		ReplicationConfig: ReplicationConfig{
			ID:           "repl",
			InitialState: ReplicationStateStopped,
		},
	})
	require.NoError(t, err)

	err = dbGroupB.SGReplicateMgr.RegisterNode("nodeGroupB")
	require.NoError(t, err)
	err = dbGroupB.SGReplicateMgr.AddReplication(&ReplicationCfg{
		ReplicationConfig: ReplicationConfig{
			ID:           "repl",
			InitialState: ReplicationStateStopped,
		},
	})
	require.NoError(t, err)

	// Check replications are assigned to correct nodes
	replications, err := dbDefault.SGReplicateMgr.GetReplications()
	require.NoError(t, err)
	assert.Len(t, replications, 1)
	cfg, exists := replications["repl"]
	require.True(t, exists, "Replicator not found")
	assert.Equal(t, "nodeDefault", cfg.AssignedNode)

	replications, err = dbGroupA.SGReplicateMgr.GetReplications()
	require.NoError(t, err)
	assert.Len(t, replications, 1)
	cfg, exists = replications["repl"]
	require.True(t, exists, "Replicator not found")
	assert.Equal(t, "nodeGroupA", cfg.AssignedNode)

	replications, err = dbGroupB.SGReplicateMgr.GetReplications()
	require.NoError(t, err)
	assert.Len(t, replications, 1)
	cfg, exists = replications["repl"]
	require.True(t, exists, "Replicator not found")
	assert.Equal(t, "nodeGroupB", cfg.AssignedNode)
}

// pausingCfg parks the first read of cfgKeySGRCluster made once armed, holding a RefreshReplicationCfg
// between its cluster read and its acquisition of activeReplicatorsLock - the window Stop() runs in.
type pausingCfg struct {
	cbgt.Cfg
	armed    atomic.Bool
	readDone chan struct{}
	release  chan struct{}
}

func (c *pausingCfg) Get(key string, cas uint64) ([]byte, uint64, error) {
	b, gotCas, err := c.Cfg.Get(key, cas)
	if key == cfgKeySGRCluster && c.armed.CompareAndSwap(true, false) {
		c.readDone <- struct{}{}
		<-c.release
	}
	return b, gotCas, err
}

// TestRefreshReplicationCfgRacesStopTeardown asserts that a RefreshReplicationCfg already in flight when
// Stop() runs cannot restart a replication:
//
//  1. The subscriber services a cfg event; RefreshReplicationCfg reads the cluster cfg, seeing the local
//     node registered and the replication assigned to it with target state running.
//  2. An admin config PUT closes the DatabaseContext, so Stop() runs, holding activeReplicatorsLock
//     across its teardown loop.
//  3. The refresh blocks on that lock, then proceeds on its now-stale snapshot and restarts the
//     replication - which nobody owns, since Stop() has been and gone.
//
// Only step 1 landing inside step 2 is chance; the lock makes the rest deterministic, and pausingCfg parks
// the refresh there so the test is too.  Nothing re-registers the node - Stop() does not RemoveNode until
// after the teardown loop, so the step 1 read is simply still valid.
//
// This is the only coverage for the isStopping() check under activeReplicatorsLock.
func TestRefreshReplicationCfgRacesStopTeardown(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyCluster)

	testDB, ctx := setupTestDB(t)
	defer testDB.Close(ctx)

	wrapped := &pausingCfg{Cfg: testDB.CfgSG, readDone: make(chan struct{}, 1), release: make(chan struct{})}

	const (
		localNodeUUID = "localNode"
		replicationID = "rep1"
	)

	mgr, err := NewSGReplicateManager(ctx, testDB.DatabaseContext, wrapped)
	require.NoError(t, err)
	require.NoError(t, mgr.StartLocalNode(localNodeUUID, nil))
	require.NoError(t, mgr.AddReplication(&ReplicationCfg{
		ReplicationConfig: ReplicationConfig{
			ID:                 replicationID,
			Direction:          ActiveReplicatorTypePush,
			Remote:             "http://localhost:4984/remotedb",
			Continuous:         true,
			CollectionsEnabled: base.TestsUseNamedCollections(),
		},
		AssignedNode: localNodeUUID,
		TargetState:  ReplicationStateStopped,
	}))

	// Register the replicator, target state stopped so nothing reaches the remote.
	require.NoError(t, mgr.RefreshReplicationCfg(ctx))
	repl := mgr.GetActiveReplicator(replicationID)
	require.NotNil(t, repl, "replicator was not registered")
	state, _ := repl.State(ctx)
	require.Equal(t, ReplicationStateStopped, state, "replicator should not be running yet")
	defer func() { _ = repl.Stop() }()

	// Target state running is what the racing refresh reads, and what makes it restart the replicator.
	require.NoError(t, mgr.UpdateReplicationState(replicationID, ReplicationStateRunning))

	// Step 1: the refresh reads the cfg, then parks.
	wrapped.armed.Store(true)
	var (
		wg         sync.WaitGroup
		refreshErr error
	)
	wg.Go(func() {
		refreshErr = mgr.RefreshReplicationCfg(ctx)
	})
	base.RequireChanRecv(t, wrapped.readDone)

	// Steps 2 and 3: Stop() runs to completion while the refresh holds its stale snapshot.
	mgr.Stop()

	// Step 4: the refresh proceeds.
	close(wrapped.release)
	base.WaitWithTimeout(t, &wg, time.Minute)
	require.NoError(t, refreshErr)

	stateAfter, _ := repl.State(ctx)
	assert.Equal(t, ReplicationStateStopped, stateAfter,
		"RefreshReplicationCfg restarted a replication on a stopped manager - nothing will ever stop it")
}

// TestStartReplicationsRacesStopTeardown asserts that a startReplications already in flight when Stop() runs
// cannot start a replication:
//
//  1. The database context spawns startReplications, which reads the cluster cfg, seeing the replication
//     assigned to the local node with target state running.
//  2. The database is closed, so Stop() runs its teardown loop over an activeReplicators the startup has not
//     registered into yet, then blocks in closeWg.Wait().
//  3. startReplications proceeds on its now-stale snapshot and starts the replication - which nobody owns,
//     since Stop()'s teardown loop has been and gone, and closeWg.Wait() only waits for it to finish
//     starting the replication, not for anything to stop it.
//
// This is the startup-path counterpart to TestRefreshReplicationCfgRacesStopTeardown, and the only coverage
// for the isStopping() check in startAssignedReplications.
func TestStartReplicationsRacesStopTeardown(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyCluster)

	testDB, ctx := setupTestDB(t)
	defer testDB.Close(ctx)

	wrapped := &pausingCfg{Cfg: testDB.CfgSG, readDone: make(chan struct{}, 1), release: make(chan struct{})}

	const (
		localNodeUUID = "localNode"
		replicationID = "rep1"
	)

	mgr, err := NewSGReplicateManager(ctx, testDB.DatabaseContext, wrapped)
	require.NoError(t, err)
	require.NoError(t, mgr.StartLocalNode(localNodeUUID, nil))
	require.NoError(t, mgr.AddReplication(&ReplicationCfg{
		ReplicationConfig: ReplicationConfig{
			ID:                 replicationID,
			Direction:          ActiveReplicatorTypePush,
			Remote:             "http://localhost:4984/remotedb",
			Continuous:         true,
			CollectionsEnabled: base.TestsUseNamedCollections(),
		},
		AssignedNode: localNodeUUID,
		TargetState:  ReplicationStateRunning,
	}))

	// Step 1: startReplications reads the cfg, then parks, as production does inside a closeWg-tracked
	// goroutine.
	wrapped.armed.Store(true)
	var startWg sync.WaitGroup
	mgr.closeWg.Add(1)
	startWg.Go(func() {
		defer mgr.closeWg.Done()
		assert.NoError(t, mgr.startReplications(ctx))
	})
	base.RequireChanRecv(t, wrapped.readDone)

	// Step 2: Stop() runs its teardown loop over an empty map, then blocks in closeWg.Wait().
	var stopWg sync.WaitGroup
	stopWg.Go(mgr.Stop)
	// Stop() closes clusterSubscribeTerminator before taking activeReplicatorsLock, so waiting on it here
	// guarantees the parked startup sees a stopping manager once released.
	<-mgr.clusterSubscribeTerminator

	// Step 3: the startup proceeds.
	close(wrapped.release)
	base.WaitWithTimeout(t, &startWg, time.Minute)
	base.WaitWithTimeout(t, &stopWg, time.Minute)

	repl := mgr.GetActiveReplicator(replicationID)
	if repl != nil {
		defer func() { _ = repl.Stop() }()
		state, _ := repl.State(ctx)
		assert.Equal(t, ReplicationStateStopped, state,
			"startReplications started a replication on a stopped manager - nothing will ever stop it")
	}
	assert.Equal(t, 0, mgr.GetNumberActiveReplicators(),
		"stopped manager still holds active replicators registered by startReplications")
}

// TestStopInterruptsReplicationStartupWait asserts that Stop() does not have to wait out the startup timer in
// DatabaseContext.startReplications.  That goroutine is tracked by closeWg, so with no signal from the
// manager it blocks Stop() in closeWg.Wait() for the full timer duration whenever Stop is called without the
// database context being closed - Close closes dbc.terminator first, so it does not pay this cost.
func TestStopInterruptsReplicationStartupWait(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyCluster)

	testDB, ctx := setupTestDB(t)
	defer testDB.Close(ctx)

	// ServerContextHasStarted is never signalled here, so the startup goroutine has only the timer to wait on.
	require.Nil(t, testDB.ServerContextHasStarted)
	testDB.Options.SGReplicateOptions.Enabled = true
	testDB.startReplications(ctx)

	mgr := testDB.SGReplicateMgr
	start := time.Now()
	mgr.Stop()
	// Comfortably under the timer - interrupting it takes well under a millisecond.
	require.Less(t, time.Since(start), sgReplicateStartupWait/2,
		"Stop() waited out the ISGR startup timer instead of interrupting it")

	// Clear the manager only after Stop, whose closeWg.Wait orders this write after the startup goroutine's
	// last read of the field.  Close would otherwise stop the manager a second time, panicking on the
	// already-closed terminators.
	testDB.SGReplicateMgr = nil
}

// TestStartReplicationsRacesStopTeardownDuringStart asserts that a replication whose Start is still in
// flight when Stop() runs its teardown loop is left stopped:
//
//  1. startReplications registers the replication, then blocks inside Start - the remote accepts the
//     connection and never answers, which has no timeout.
//  2. Stop()'s teardown loop calls Stop on that replication, which blocks on the replicator's own lock,
//     held by Start.
//  3. Start completes and falls into its reconnect loop, then releases the lock and the pending Stop runs,
//     cancelling it.
//
// So this interleaving needs no handling in startAssignedReplication beyond registering under
// activeReplicatorsLock - which is why it does not recheck after Start.  This test pins that reasoning: it
// fails if Stop's teardown stops being ordered behind an in-flight Start.
//
// Note that Stop leaves entries in activeReplicators, so this asserts replication state rather than map
// membership.
func TestStartReplicationsRacesStopTeardownDuringStart(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyCluster)

	testDB, ctx := setupTestDB(t)
	defer testDB.Close(ctx)

	// The remote accepts the request and holds it, parking Start inside its reachability check.
	connectStarted := make(chan struct{}, 1)
	release := make(chan struct{})
	var remoteHit atomic.Bool
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if remoteHit.CompareAndSwap(false, true) {
			connectStarted <- struct{}{}
			<-release
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer remote.Close()

	const (
		localNodeUUID = "localNode"
		replicationID = "rep1"
	)

	// The database's own manager and cfg, so the test exercises whichever cbgt.Cfg this edition uses.
	mgr := testDB.SGReplicateMgr
	require.NoError(t, mgr.StartLocalNode(localNodeUUID, nil))
	require.NoError(t, mgr.AddReplication(&ReplicationCfg{
		ReplicationConfig: ReplicationConfig{
			ID:                 replicationID,
			Direction:          ActiveReplicatorTypePush,
			Remote:             remote.URL + "/remotedb",
			Continuous:         true,
			CollectionsEnabled: base.TestsUseNamedCollections(),
		},
		AssignedNode: localNodeUUID,
		TargetState:  ReplicationStateRunning,
	}))

	// Step 1: the startup registers the replication, then parks inside Start.
	var startWg sync.WaitGroup
	mgr.closeWg.Add(1)
	startWg.Go(func() {
		defer mgr.closeWg.Done()
		assert.NoError(t, mgr.startReplications(ctx))
	})
	base.RequireChanRecv(t, connectStarted)
	repl := mgr.GetActiveReplicator(replicationID)
	require.NotNil(t, repl, "replication should be registered before Start")

	// Step 2: Stop()'s teardown loop stops a replication whose Start has not returned yet.
	var stopWg sync.WaitGroup
	stopWg.Go(mgr.Stop)
	<-mgr.clusterSubscribeTerminator

	// Step 3: Start completes, releasing the replicator lock the pending Stop is waiting on.
	close(release)
	base.WaitWithTimeout(t, &startWg, time.Minute)
	base.WaitWithTimeout(t, &stopWg, time.Minute)

	state, _ := repl.State(ctx)
	assert.Equal(t, ReplicationStateStopped, state,
		"replication started during manager stop was left running - nothing will ever stop it")
	assert.False(t, repl.Push.reconnectActive.IsTrue(),
		"replication started during manager stop was left reconnecting")

	// Clear the manager only after Stop, whose closeWg.Wait orders this write after the startup goroutine.
	// Close would otherwise stop it a second time, panicking on the already-closed terminators.
	testDB.SGReplicateMgr = nil
}

// TestSGReplicateManagerStopDoesNotPanicOnConcurrentClusterUpdate asserts that a cluster config write racing
// Stop() does not panic.  updateCluster used to register itself on closeWg, the same waitgroup Stop waits on -
// but it runs on the caller's goroutine, so a call arriving once Stop was already inside closeWg.Wait with the
// counter at zero was a WaitGroup misuse panic, not a wait.  In production that is any request that writes the
// cluster config during a database close or config reload: PUT /{db}/_replication/{id},
// PUT /{db}/_replicationStatus/{id}, DELETE /{db}/_replication/{id}.
//
// The window is small, so this loops.  Before the fix it panicked on the first attempt.
func TestSGReplicateManagerStopDoesNotPanicOnConcurrentClusterUpdate(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelWarn, base.KeyCluster)

	testDB, ctx := setupTestDB(t)
	defer testDB.Close(ctx)

	const attempts = 200
	for i := range attempts {
		// A distinct cfg prefix and node per attempt, so attempts do not interfere.
		cfgSG, err := base.NewCfgSG(ctx, testDB.MetadataStore, fmt.Sprintf("p%d", i), false)
		require.NoError(t, err)
		mgr, err := NewSGReplicateManager(ctx, testDB.DatabaseContext, cfgSG)
		require.NoError(t, err)
		require.NoError(t, mgr.StartLocalNode(fmt.Sprintf("n%d", i), nil))
		// Registers a goroutine on closeWg, so Stop's Wait blocks and there is a waiter to misuse.
		require.NoError(t, mgr.SubscribeCfgChanges(ctx))

		var recovered atomic.Value
		catch := func(fn func()) {
			defer func() {
				if r := recover(); r != nil {
					recovered.CompareAndSwap(nil, fmt.Sprint(r))
				}
			}()
			fn()
		}

		var hammer sync.WaitGroup
		stopHammer := make(chan struct{})
		for range 4 {
			hammer.Go(func() {
				catch(func() {
					for {
						select {
						case <-stopHammer:
							return
						default:
						}
						_ = mgr.updateCluster(func(*SGRCluster) (bool, error) { return true, nil })
					}
				})
			})
		}
		catch(mgr.Stop)
		close(stopHammer)
		base.WaitWithTimeout(t, &hammer, time.Minute)

		if r := recovered.Load(); r != nil {
			t.Fatalf("attempt %d: cluster config write concurrent with Stop() panicked: %v", i, r)
		}
	}
}

// TestSGReplicateManagerStopDrainsClusterUpdates asserts the guarantee that makes the fix safe: no cluster
// update runs past Stop.  Stop blocks until an in-flight update finishes, and an update starting afterwards is
// refused rather than run against a torn-down manager.
func TestSGReplicateManagerStopDrainsClusterUpdates(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCluster)

	testDB, ctx := setupTestDB(t)
	defer testDB.Close(ctx)

	cfgSG, err := base.NewCfgSG(ctx, testDB.MetadataStore, "", false)
	require.NoError(t, err)
	mgr, err := NewSGReplicateManager(ctx, testDB.DatabaseContext, cfgSG)
	require.NoError(t, err)
	require.NoError(t, mgr.StartLocalNode("localNode", nil))

	// An update parks inside its callback, so it is in flight for as long as the test wants.
	inUpdate := make(chan struct{})
	release := make(chan struct{})
	var updateWg sync.WaitGroup
	updateWg.Go(func() {
		assert.NoError(t, mgr.updateCluster(func(*SGRCluster) (bool, error) {
			inUpdate <- struct{}{}
			<-release
			return true, nil // cancel, so there is no CAS write to retry
		}))
	})
	base.RequireChanRecv(t, inUpdate)

	stopDone := make(chan struct{})
	var stopWg sync.WaitGroup
	stopWg.Go(func() {
		mgr.Stop()
		close(stopDone)
	})

	// Stop must not complete while the update is still in flight.
	select {
	case <-stopDone:
		require.Fail(t, "Stop returned while a cluster update was still in flight")
	case <-time.After(500 * time.Millisecond):
	}

	close(release)
	base.WaitWithTimeout(t, &updateWg, time.Minute)
	base.WaitWithTimeout(t, &stopWg, time.Minute)

	// An update starting after Stop is refused outright, so it cannot touch a torn-down manager.
	ran := false
	require.NoError(t, mgr.updateCluster(func(*SGRCluster) (bool, error) {
		ran = true
		return true, nil
	}))
	require.False(t, ran, "cluster update ran after Stop returned")
}
