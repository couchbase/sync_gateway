// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// Package sgtest contains test environment utilities shared across all SG packages.
// It intentionally has no dependency on any SG package to avoid import cycles.
package sgtest

import (
	"os"
	"strings"
	"testing"
	"time"

	rosmar "github.com/couchbaselabs/rosmar"
)

const (
	testEnvBackingStore          = "SG_TEST_BACKING_STORE"
	testEnvBackingStoreCouchbase = "Couchbase"
	testEnvCouchbaseServerURL    = "SG_TEST_COUCHBASE_SERVER_URL"
	testEnvRosmarURL             = "SG_TEST_ROSMAR_URL"
	defaultCouchbaseServerURL    = "couchbase://127.0.0.1"
)

// TestUseCouchbaseServer reports whether tests are configured to run against a real Couchbase Server cluster.
func TestUseCouchbaseServer() bool {
	return strings.EqualFold(os.Getenv(testEnvBackingStore), testEnvBackingStoreCouchbase)
}

// UnitTestUrl returns the configured test server URL.
func UnitTestUrl() string {
	if TestUseCouchbaseServer() {
		if u := os.Getenv(testEnvCouchbaseServerURL); u != "" {
			return u
		}
		return defaultCouchbaseServerURL
	}
	if u := os.Getenv(testEnvRosmarURL); u != "" {
		return u
	}
	return rosmar.InMemoryURL
}

// UnitTestUrlIsWalrus reports whether tests are running against an in-memory Walrus/Rosmar store.
func UnitTestUrlIsWalrus() bool {
	return !TestUseCouchbaseServer()
}

// GetBackgroundManagerStatusTransitionTimeout returns the appropriate wait timeout for background
// manager state transitions. CBS and CI environments get a longer budget to account for slower
// execution; the REST-layer timeout is intentionally larger than the direct-access variant
// (db.RequireBackgroundManagerState uses 30 s for CBS) to cover HTTP round-trip overhead.
func GetBackgroundManagerStatusTransitionTimeout(t testing.TB) time.Duration {
	if !UnitTestUrlIsWalrus() || IsRaceDetectorEnabled(t) || os.Getenv("CI") != "" {
		return 60 * time.Second
	}
	return 10 * time.Second
}
