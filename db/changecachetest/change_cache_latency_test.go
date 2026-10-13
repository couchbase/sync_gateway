//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package changecachetest

import (
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
)

func TestFeedLatency(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	minuteAgo := now.Add(-time.Minute)
	hourAgo := now.Add(-time.Hour)

	testCases := []struct {
		name      string
		timeSaved time.Time
		initTime  time.Time
		expected  time.Duration
	}{
		{
			name:      "write after cache start is charged from its write time",
			timeSaved: minuteAgo,
			initTime:  hourAgo,
			expected:  time.Minute,
		},
		{
			name:      "write before cache start is charged from the cache start",
			timeSaved: hourAgo,
			initTime:  minuteAgo,
			expected:  time.Minute,
		},
		{
			name:      "write at cache start is charged from the cache start",
			timeSaved: minuteAgo,
			initTime:  minuteAgo,
			expected:  time.Minute,
		},
		{
			name:     "unset write time records no latency",
			initTime: hourAgo,
			expected: 0,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, db.FeedLatency(tc.timeSaved, tc.initTime, now))
		})
	}
}
