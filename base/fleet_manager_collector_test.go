/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"math"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/testing/assert"
)

// TestFleetManagerCollectorSettingsInterval verifies that the server-supplied hourly interval is
// converted to a Duration, and that non-positive values fall back to the default interval.
func TestFleetManagerCollectorSettingsInterval(t *testing.T) {
	testCases := []struct {
		name     string
		settings FleetManagerCollectorSettings
		expected time.Duration
	}{
		{
			name:     "positive interval converts hours to duration",
			settings: FleetManagerCollectorSettings{ReportingInterval: 6},
			expected: 6 * time.Hour,
		},
		{
			name:     "single hour",
			settings: FleetManagerCollectorSettings{ReportingInterval: 1},
			expected: time.Hour,
		},
		{
			name:     "zero interval falls back to default",
			settings: FleetManagerCollectorSettings{ReportingInterval: 0},
			expected: defaultFleetManagerReportingInterval,
		},
		{
			name:     "negative interval falls back to default",
			settings: FleetManagerCollectorSettings{ReportingInterval: -3},
			expected: defaultFleetManagerReportingInterval,
		},
		{
			name:     "enabled flag does not affect interval",
			settings: FleetManagerCollectorSettings{ReportingInterval: 2, Enabled: true},
			expected: 2 * time.Hour,
		},
		{
			name:     "largest non-overflowing interval converts",
			settings: FleetManagerCollectorSettings{ReportingInterval: maxReportingIntervalHours},
			expected: time.Duration(maxReportingIntervalHours) * time.Hour,
		},
		{
			name:     "interval one hour past the overflow boundary falls back to default",
			settings: FleetManagerCollectorSettings{ReportingInterval: maxReportingIntervalHours + 1},
			expected: defaultFleetManagerReportingInterval,
		},
		{
			name:     "max int interval falls back to default",
			settings: FleetManagerCollectorSettings{ReportingInterval: math.MaxInt},
			expected: defaultFleetManagerReportingInterval,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.settings.Interval())
		})
	}
}
