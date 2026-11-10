// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"testing"

	"github.com/couchbase/sync_gateway/testing/require"
)

// TestThroughputSeriesSteadyWindow checks the steady-window figures exclude the ramp and anything older
// than the window: during the ramp below each second caches 10 sequences at 100ms each, and afterwards
// 1000 sequences at 2ms each.
func TestThroughputSeriesSteadyWindow(t *testing.T) {
	const rampEnd = 1010
	var s throughputSeries
	var count, cachingTimeNs int64
	for sec := int64(1000); sec <= 1100; sec++ {
		if sec <= rampEnd {
			count += 10
			cachingTimeNs += 10 * 100e6
		} else {
			count += 1000
			cachingTimeNs += 1000 * 2e6
		}
		s.record(sec, count, cachingTimeNs)
		if sec == rampEnd {
			s.readyUnixSec = sec
		}
	}

	t.Run("window reaches back to the ramp end", func(t *testing.T) {
		overall, steady, window := s.rates(300)
		require.Equal(t, int64(1100-rampEnd), window)
		require.InDelta(t, 1000.0, steady, 1e-9)
		require.Less(t, overall, steady, "the whole-run rate includes the ramp")
		require.InDelta(t, 2.0, s.steadyAvgCachingTimeMs(300), 1e-9)
		require.Greater(t, avgCachingTimeMs(cachingTimeNs, count), 2.0, "the whole-run mean includes the ramp")
	})

	t.Run("window shorter than the post-ramp run", func(t *testing.T) {
		_, steady, window := s.rates(30)
		require.Equal(t, int64(30), window)
		require.InDelta(t, 1000.0, steady, 1e-9)
		require.InDelta(t, 2.0, s.steadyAvgCachingTimeMs(30), 1e-9)
	})

	t.Run("no steady window", func(t *testing.T) {
		var short throughputSeries
		short.record(1, 10, 1e6)
		_, steady, window := short.rates(300)
		require.Zero(t, steady)
		require.Zero(t, window)
		require.Zero(t, short.steadyAvgCachingTimeMs(300))
	})
}
