// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build race
// +build race

package base

import "testing"

// IsRaceDetectorEnabled returns true if compiled with -race. Intended to be used for testing only
func IsRaceDetectorEnabled(t *testing.T) bool {
	return true
}
