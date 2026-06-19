//go:build windows

//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"golang.org/x/sys/windows"
)

// hlcWallClock returns the current wall-clock time in nanoseconds since the Unix epoch. On Windows,
// time.Now() is backed by the coarse system timer (~0.5-15ms resolution), which is too low for the HLC:
// successive writes land in the same physical slot, forcing logical increments and diverging from the CAS.
// GetSystemTimePreciseAsFileTime reads the high-resolution system clock (~100ns) directly, so it is read
// live on every call rather than anchored once. Reading live (rather than anchoring an offset against
// QueryPerformanceCounter) keeps this in lockstep with any other clock in the same process that reads the
// same system time, avoiding a fixed startup skew between Sync Gateway's HLC and the bucket's clock.
func hlcWallClock() uint64 {
	var ft windows.Filetime
	windows.GetSystemTimePreciseAsFileTime(&ft)
	return uint64(ft.Nanoseconds())
}
