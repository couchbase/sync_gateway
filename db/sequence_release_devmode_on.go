// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build cb_sg_devmode

package db

import (
	"fmt"
	"os"
	"time"
)

// GetTestReleaseSequenceWait returns an override for the time the sequence allocator waits after a reserve before
// releasing unused sequences, used for testing purposes to simulate a slow node that leaves gaps in the sequence
// numbering.  Returns 0 when unset, in which case defaultReleaseSequenceWait is used.
//
// Setting this to a very small value (e.g. SG_TEST_RELEASE_SEQUENCE_WAIT=1ns) makes any test that depends on
// densely allocated sequences fail deterministically, instead of intermittently on a loaded CI machine.  Tests
// asserting on exact sequence values should call SuspendSequenceBatching to pin the batch size to 1.
func GetTestReleaseSequenceWait() (time.Duration, error) {
	waitEnvVar := "SG_TEST_RELEASE_SEQUENCE_WAIT"
	w := os.Getenv(waitEnvVar)
	if w == "" {
		return 0, nil
	}
	wait, err := time.ParseDuration(w)
	if err != nil {
		return 0, fmt.Errorf("setting %s=%s is not a valid time: %w", waitEnvVar, w, err)
	}
	if wait < 0 {
		return 0, fmt.Errorf("setting %s=%s must not be negative", waitEnvVar, w)
	}
	return wait, nil
}
