// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build !cb_sg_devmode

package db

import (
	"time"
)

// GetCachingFeedDelay returns the delay to apply to processing of each DCP event in the caching feed.
// If cb_sg_devmode build flag is not set, do not use.
func GetCachingFeedDelay() (time.Duration, error) {
	return 0, nil
}
