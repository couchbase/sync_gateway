// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package importtest

import (
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestImportPartitionsOnConcurrentStart(t *testing.T) {
	base.TestRequiresCbgt(t)

	const (
		dbName              = "db"
		numNodes            = 4
		numImportPartitions = uint16(16)
		expectedPartitions  = 4
	)
	rtc := rest.NewRestTesterCluster(t, &rest.RestTesterClusterConfig{
		NumNodes: numNodes,
		MutateStartupConfig: func(config *rest.StartupConfig) {
			// fast polling lets the other nodes start the database at about the same time
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(50 * time.Millisecond)
		},
	})
	defer rtc.Close(base.TestCtx(t))

	node0 := rtc.Node(0)
	dbConfig := node0.NewDbConfig()
	dbConfig.AutoImport = true
	dbConfig.ImportPartitions = new(numImportPartitions)
	rest.RequireStatus(t, node0.CreateDatabase(dbName, dbConfig), http.StatusCreated)

	rtc.ForEachNode(func(rt *rest.RestTester) { rt.WaitForDatabase(dbName) })

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var currentPartitions []int
		rtc.ForEachNode(func(rt *rest.RestTester) {
			currentPartitions = append(currentPartitions, rt.GetDatabase().ImportPartitionCount())
		})
		for _, rtPartitions := range currentPartitions {
			assert.Equal(c, expectedPartitions, rtPartitions, "unbalanced partitions, distribution: %v", currentPartitions)
		}
	}, 20*time.Second, 250*time.Millisecond)
}
