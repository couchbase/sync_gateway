// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package sgtest

import (
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbaselabs/rosmar"
)

func TestSetRosmarClockForTestConsecutiveOverrides(t *testing.T) {
	bucket, err := rosmar.OpenBucket(rosmar.InMemoryURL, "rosmarclock", rosmar.CreateOrOpen)
	require.NoError(t, err)
	defer bucket.Close(t.Context())
	ds := bucket.DefaultDataStore(t.Context())

	var cas uint64
	write := func(t *testing.T) uint64 {
		newCAS, err := ds.WriteCas(t.Context(), "doc", 0, cas, map[string]any{}, 0)
		require.NoError(t, err)
		require.Greater(t, newCAS, cas)
		return newCAS
	}

	t.Run("wall clock", func(t *testing.T) {
		SetRosmarClockForTest(t, sgbucket.HLCWallClock)
		cas = write(t)
	})
	// a clock a minute behind the last CAS, the same as a backward wall clock step between tests
	t.Run("clock behind", func(t *testing.T) {
		SetRosmarClockForTest(t, func() uint64 { return sgbucket.HLCWallClock() - uint64(time.Minute) })
		cas = write(t)
	})
	write(t)
}
