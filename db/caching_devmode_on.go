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
	delayEnvVar := "SG_TEST_CACHING_FEED_DELAY"
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
