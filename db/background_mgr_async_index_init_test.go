//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"fmt"
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestAsyncIndexInitConcurrentStatus runs the index init worker's status updates against the status readers. Serving
// the status directly from the worker's map made this panic with "concurrent map iteration and map write".
func TestAsyncIndexInitConcurrentStatus(t *testing.T) {
	tracker := NewIndexStatusTracker(base.DefaultScope)
	mgr := NewAsyncIndexInitProcess()
	_, err := mgr.Init(base.TestCtx(t), AsyncIndexInitOptions{StatusTracker: tracker, DoneChan: make(chan error)}, nil)
	require.NoError(t, err)

	// a handful of collections, as a real database would have, so that each status read stays cheap
	collections := make([]base.ScopeAndCollectionName, 0, 4)
	for i := range 4 {
		collections = append(collections, base.NewScopeAndCollectionName(base.DefaultScope, fmt.Sprintf("collection_%d", i)))
	}

	const iterations = 10_000
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range iterations {
			tracker.Set(collections[i%len(collections)], CollectionIndexStatusReady)
		}
	}()
	go func() {
		defer wg.Done()
		for range iterations {
			_, _, err := mgr.GetProcessStatus(BackgroundManagerStatus{}, nil)
			assert.NoError(t, err)
		}
	}()
	wg.Wait()
}
