// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package importtest

import (
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestImportPartitionsOnConcurrentStart(t *testing.T) {
	base.TestRequiresCbgt(t)

	// Start multiple rest testers concurrently
	numNodes := 4
	numImportPartitions := uint16(16)
	expectedPartitions := 4
	restTesters := make([]*rest.RestTester, numNodes)
	tb := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer tb.Close(ctx)
	var wg sync.WaitGroup
	for i := range numNodes {
		wg.Add(1)
		go func(i int) {
			noCloseTB := tb.NoCloseClone()
			rt := rest.NewRestTester(t, &rest.RestTesterConfig{
				CustomTestBucket: noCloseTB,
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					AutoImport:       true,
					ImportPartitions: new(numImportPartitions),
				}},
			})
			restTesters[i] = rt
			wg.Done()
		}(i)
	}
	wg.Wait()

	defer func() {
		for _, rt := range restTesters {
			if rt != nil {
				rt.Close()
			}
		}
	}()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		currentPartitions := make([]int, len(restTesters))
		for i, rt := range restTesters {
			currentPartitions[i] = rt.GetDatabase().ImportPartitionCount()
		}
		for _, rtPartitions := range currentPartitions {
			assert.Equal(c, expectedPartitions, rtPartitions, "unbalanced partitions, distribution: %v", currentPartitions)
		}
	}, time.Second*5, time.Millisecond*250)
}
