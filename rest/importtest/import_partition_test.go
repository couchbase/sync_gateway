// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package importtest

import (
	"log"
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
)

func TestImportPartitionsOnConcurrentStart(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	if !base.IsEnterpriseEdition() {
		t.Skip("This test only works against EE")
	}
	// Start multiple rest testers concurrently
	numNodes := 4
	numImportPartitions := uint16(16)
	expectedPartitions := 4
	restTesters := make([]*rest.RestTester, numNodes)
	tb := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer tb.Close(ctx)
	var wg sync.WaitGroup
	for i := 0; i < numNodes; i++ {
		wg.Add(1)
		go func(i int) {
			noCloseTB := tb.NoCloseClone()
			rt := rest.NewRestTester(t, &rest.RestTesterConfig{
				CustomTestBucket: noCloseTB,
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					AutoImport:       true,
					ImportPartitions: base.Uint16Ptr(numImportPartitions),
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

	rest.WaitAndAssertCondition(t, func() bool {
		totalPartitions := uint16(0)
		balancedPartitions := true
		currentPartitions := make([]int, len(restTesters))
		for i, rt := range restTesters {
			rtPartitions := rt.GetDatabase().PartitionCount()
			currentPartitions[i] = rtPartitions
			totalPartitions = totalPartitions + uint16(rtPartitions)
			if rtPartitions != expectedPartitions {
				balancedPartitions = false
			}
		}
		if totalPartitions == numImportPartitions && balancedPartitions == true {
			log.Printf("Partitions are balanced.  Current total: %d, distribution: %v", totalPartitions, currentPartitions)
			return true
		} else {
			log.Printf("Waiting for balanced partitions.  Current total: %d, distribution: %v", totalPartitions, currentPartitions)
			return false
		}
	})
}
