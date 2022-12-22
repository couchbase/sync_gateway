// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
)

// Fatalf logs and exits.
func (tbp *TestBucketPool) Fatalf(ctx context.Context, format string, args ...interface{}) {
	format = addPrefixes(format, ctx, LevelNone, KeySGTest)
	FatalfCtx(ctx, format, args...)
}

// Logf formats the given test bucket logging and logs to stderr.
func (tbp *TestBucketPool) Logf(ctx context.Context, format string, args ...interface{}) {
	if tbp != nil && !tbp.verbose.IsTrue() {
		return
	}

	format = addPrefixes(format, ctx, LevelNone, KeySGTest)
	if colorEnabled() {
		// Green
		format = "\033[0;32m" + format + "\033[0m"
	}

	_, _ = fmt.Fprintf(consoleFOutput, format+"\n", args...)
}

// getTestBucketSpec returns a new BucketSpec for the given test bucket name.
func getTestBucketSpec(testBucketName tbpBucketName) BucketSpec {
	bucketSpec := tbpDefaultBucketSpec
	bucketSpec.BucketName = string(testBucketName)
	bucketSpec.TLSSkipVerify = TestTLSSkipVerify()
	return bucketSpec
}

// RequireNumTestBuckets skips the given test if there are not enough test buckets available to use.
func RequireNumTestBuckets(t *testing.T, numRequired int) {
	usable := GTestBucketPool.numUsableBuckets()
	if usable < numRequired {
		t.Skipf("Only had %d usable test buckets available (test requires %d)", usable, numRequired)
	}
}

// RequireNumTestDataStores skips the given test if there are not enough test buckets available to use.
func RequireNumTestDataStores(t *testing.T, numRequired int) {
	TestRequiresCollections(t)
	available := tbpNumCollectionsPerBucket()
	if available < numRequired {
		t.Skipf("Only had %d usable test buckets available (test requires %d)", available, numRequired)
	}
}

// numUsableBuckets returns the total number of buckets in the pool that can be used by a test.
func (tbp *TestBucketPool) numUsableBuckets() int {
	if !tbp.integrationMode {
		// we can create virtually endless walrus buckets,
		// so report back 10 to match a fully available CBS bucket pool.
		return 10
	}
	return tbpNumBuckets() - int(atomic.LoadUint32(&tbp.preservedBucketCount))
}
