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
	"sync"
	"testing"

	"github.com/couchbase/sg-bucket/js"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test node operations on SGReplicateManager
func TestReplicateManagerReplications(t *testing.T) {

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testCfg, err := base.NewCfgSG(testBucket.GetSingleDataStore(), "")
	require.NoError(t, err)

	ctx := base.TestCtx(t)
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
	r, err = manager.GetReplication("dne")
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
	assert.Equal(t, 2, len(replications))

	// Remove replication
	err = manager.DeleteReplication(replication1_id)
	require.NoError(t, err)
	replications, err = manager.GetReplications()
	require.NoError(t, err)
	assert.Equal(t, 1, len(replications))

	// Remove non-existent replication
	err = manager.DeleteReplication(replication1_id)
	require.Error(t, base.ErrNotFound, err)
	replications, err = manager.GetReplications()
	require.NoError(t, err)
	assert.Equal(t, 1, len(replications))

	// Remove last replication
	err = manager.DeleteReplication(replication2_id)
	require.NoError(t, err)
	replications, err = manager.GetReplications()
	require.NoError(t, err)
	assert.Equal(t, 0, len(replications))
}

// Test node operations on SGReplicateManager
func TestReplicateManagerNodes(t *testing.T) {

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testCfg, err := base.NewCfgSG(testBucket.GetSingleDataStore(), "")
	require.NoError(t, err)

	ctx := base.TestCtx(t)
	manager, err := NewSGReplicateManager(ctx, &DatabaseContext{Name: "test"}, testCfg)
	require.NoError(t, err)
	defer manager.Stop()

	err = manager.registerNodeForHost("node1", "host1")
	require.NoError(t, err)

	nodes, err := manager.getNodes()
	require.NoError(t, err)
	assert.Equal(t, 1, len(nodes))

	err = manager.registerNodeForHost("node2", "host2")
	require.NoError(t, err)

	nodes, err = manager.getNodes()
	require.NoError(t, err)
	assert.Equal(t, 2, len(nodes))

	// re-adding an existing node is a no-op
	err = manager.registerNodeForHost("node1", "host1")
	require.NoError(t, err)

	nodes, err = manager.getNodes()
	require.NoError(t, err)
	assert.Equal(t, 2, len(nodes))

	// Remove node
	err = manager.RemoveNode("node1")
	require.NoError(t, err)

	nodes, err = manager.getNodes()
	require.NoError(t, err)
	require.Equal(t, 1, len(nodes))
	node2, ok := nodes["node2"]
	require.True(t, ok)
	require.Equal(t, node2.UUID, "node2")

	// Removing an already removed node is a no-op
	err = manager.RemoveNode("node1")
	require.NoError(t, err)

	replications, err := manager.GetReplications()
	require.NoError(t, err)
	assert.Equal(t, 0, len(replications))
}

// Test concurrent node operations on SGReplicateManager
func TestReplicateManagerConcurrentNodeOperations(t *testing.T) {

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	ctx := base.TestCtx(t)
	testCfg, err := base.NewCfgSG(testBucket.GetSingleDataStore(), "")
	require.NoError(t, err)

	manager, err := NewSGReplicateManager(ctx, &DatabaseContext{Name: "test"}, testCfg)
	require.NoError(t, err)
	defer manager.Stop()

	var nodeWg sync.WaitGroup

	for i := 0; i < 20; i++ {
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
	require.Equal(t, 20, len(nodes))

	for i := 0; i < 20; i++ {
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
	require.Equal(t, 0, len(nodes))
}

// Test concurrent replication operations on SGReplicateManager
func TestReplicateManagerConcurrentReplicationOperations(t *testing.T) {

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	ctx := base.TestCtx(t)
	testCfg, err := base.NewCfgSG(testBucket.GetSingleDataStore(), "")
	require.NoError(t, err)

	manager, err := NewSGReplicateManager(ctx, &DatabaseContext{Name: "test"}, testCfg)
	require.NoError(t, err)
	defer manager.Stop()

	var replicationWg sync.WaitGroup

	for i := 0; i < 20; i++ {
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
	require.Equal(t, 20, len(replications))

	for i := 0; i < 20; i++ {
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
	require.Equal(t, 0, len(replications))
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
		t.Run(fmt.Sprintf("%s", testCase.name), func(t *testing.T) {

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
				Direction: base.StringPtr("push"),
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
				ConflictResolutionFn: base.StringPtr(""),
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
				QueryParams: map[string]interface{}{"ABC": true},
			},
			expectedConfig: &ReplicationConfig{
				ID:          "foo",
				Remote:      "remote",
				Direction:   "pull",
				QueryParams: map[string]interface{}{"ABC": true},
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
				QueryParams:            []interface{}{"ABC"},
			},
			updatedConfig: &ReplicationUpsertConfig{
				ID:                     "foo",
				Remote:                 base.StringPtr("b"),
				Direction:              base.StringPtr("b"),
				ConflictResolutionType: base.StringPtr("b"),
				ConflictResolutionFn:   base.StringPtr("b"),
				PurgeOnRemoval:         base.BoolPtr(false),
				DeltaSyncEnabled:       base.BoolPtr(false),
				MaxBackoff:             base.IntPtr(10),
				InitialState:           base.StringPtr("b"),
				Continuous:             base.BoolPtr(false),
				Filter:                 base.StringPtr("b"),
				QueryParams:            []interface{}{"DEF"},
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
				QueryParams:            []interface{}{"DEF"},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s", testCase.name), func(t *testing.T) {
			testCase.existingConfig.Upsert(testCase.updatedConfig)
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
				QueryParams:            []interface{}{"ABC"},
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
				Remote: base.StringPtr("b"),
			},
			expectedChanged: true,
		},
		{
			name: "directionChanged",
			updatedConfig: &ReplicationUpsertConfig{
				Direction: base.StringPtr(string(ActiveReplicatorTypePushAndPull)),
			},
			expectedChanged: true,
		},
		{
			name: "conflictResolverChanged",
			updatedConfig: &ReplicationUpsertConfig{
				ConflictResolutionType: base.StringPtr(string(ConflictResolverDefault)),
			},
			expectedChanged: true,
		},
		{
			name: "conflictResolverFnChange",
			updatedConfig: &ReplicationUpsertConfig{
				ConflictResolutionFn: base.StringPtr("b"),
			},
			expectedChanged: true,
		},
		{
			name: "passwordChanged", // Verify fix CBG-1858
			updatedConfig: &ReplicationUpsertConfig{
				Password: base.StringPtr("changed"),
			},
			expectedChanged: true,
		},
		{
			name: "collections enabled",
			updatedConfig: &ReplicationUpsertConfig{
				CollectionsEnabled: base.BoolPtr(true),
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
				Remote:               base.StringPtr("a"),
				ConflictResolutionFn: base.StringPtr("a"),
				CollectionsLocal:     []string{"foo.bar"},
			},
			expectedChanged: false,
		},
	}

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()

	testCfg, err := base.NewCfgSG(testBucket.GetSingleDataStore(), "")
	require.NoError(t, err)

	dbctx := DatabaseContext{Name: "test"}
	dbctx.JS.Init(js.EngineNamed(DefaultJavaScriptEngine), 4)
	mgr, err := NewSGReplicateManager(base.TestCtx(t), &dbctx, testCfg)
	require.NoError(t, err)
	defer mgr.Stop()

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			replicationCfg := getInitialCfg()
			replicatorConfig, err := mgr.NewActiveReplicatorConfig(replicationCfg)
			require.NoError(t, err)

			replicationCfg.Upsert(testCase.updatedConfig)

			isChanged, err := mgr.isCfgChanged(replicationCfg, replicatorConfig)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedChanged, isChanged)
		})
	}

}

// Test replicators assigned nodes with different group IDs
func TestReplicateGroupIDAssignedNodes(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	tb := base.GetTestBucket(t)
	defer tb.Close()
	ctx := base.TestCtx(t)

	// scopes config will set up from test environment whether backed by default or non default collection
	scopesConfig := GetScopesOptions(t, tb, 1)
	// Set up databases
	dbDefault, err := NewDatabaseContext(ctx, "default", tb.NoCloseClone(), false, DatabaseContextOptions{GroupID: "", Scopes: scopesConfig})
	require.NoError(t, err)
	defer dbDefault.Close(ctx)

	dbGroupA, err := NewDatabaseContext(ctx, "groupa", tb.NoCloseClone(), false, DatabaseContextOptions{GroupID: "GroupA", Scopes: scopesConfig})
	require.NoError(t, err)
	defer dbGroupA.Close(ctx)

	dbGroupB, err := NewDatabaseContext(ctx, "groupb", tb.NoCloseClone(), false, DatabaseContextOptions{GroupID: "GroupB", Scopes: scopesConfig})
	require.NoError(t, err)
	defer dbGroupB.Close(ctx)

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
