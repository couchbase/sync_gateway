// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build !race
// +build !race

package sgtest

import "testing"

// IsRaceDetectorEnabled returns false when compiled without -race.
func IsRaceDetectorEnabled(_ testing.TB) bool {
	return false
}
