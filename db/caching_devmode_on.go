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

// GetCachingFeedDelay returns the delay to apply to processing of each DCP event in the caching feed, used for testing
// purposes to simulate a slow feed.
func GetCachingFeedDelay() (time.Duration, error) {
	return cachingFeedDelayFromEnv("SG_TEST_CACHING_FEED_DELAY")
}

// GetCachingFeedPrincipalDocDelay returns an additional delay to apply to principal (user and role) documents on the
// caching feed, on top of GetCachingFeedDelay. Waiting on the change cache does not cover the principal doc
// notifications that tell a live replication to reload its user, so this delay lets tests widen that window.
func GetCachingFeedPrincipalDocDelay() (time.Duration, error) {
	return cachingFeedDelayFromEnv("SG_TEST_CACHING_FEED_PRINCIPAL_DOC_DELAY")
}

// cachingFeedDelayFromEnv parses a caching feed delay from the named environment variable, returning zero if unset.
func cachingFeedDelayFromEnv(delayEnvVar string) (time.Duration, error) {
	d := os.Getenv(delayEnvVar)
	if d == "" {
		return 0, nil
	}
	delay, err := time.ParseDuration(d)
	if err != nil {
		return 0, fmt.Errorf("setting %s=%s is not a valid time: %w", delayEnvVar, d, err)
	}
	return delay, nil
}
