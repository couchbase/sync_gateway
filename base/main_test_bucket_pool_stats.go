/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// printStats outputs test bucket stats for the current package's test run.
func (tbp *TestBucketPool) printStats() {

	numBucketsOpened := time.Duration(atomic.LoadInt32(&tbp.stats.NumBucketsOpened))
	if numBucketsOpened == 0 {
		// we may have been running benchmarks if we've opened zero test buckets
		// in any case; if we have no stats, don't bother printing anything.
		return
	}

	totalBucketInitTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalBucketInitDurationNano))
	totalBucketInitCount := time.Duration(atomic.LoadInt32(&tbp.stats.TotalBucketInitCount))

	totalBucketReadierTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalBucketReadierDurationNano))
	totalBucketReadierCount := time.Duration(atomic.LoadInt32(&tbp.stats.TotalBucketReadierCount))

	totalBucketWaitTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalWaitingForReadyBucketNano))

	totalBucketUseTime := time.Duration(atomic.LoadInt64(&tbp.stats.TotalInuseBucketNano))

	origVerbose := tbp.verbose.IsTrue()
	tbp.verbose.Set(true)
	ctx := context.Background()

	tbp.Logf(ctx, "==========================")
	tbp.Logf(ctx, "= Test Bucket Pool Stats =")
	tbp.Logf(ctx, "==========================")
	if totalBucketInitCount > 0 {
		tbp.Logf(ctx, "Total bucket init time: %s for %d buckets (avg: %s)", totalBucketInitTime, totalBucketInitCount, totalBucketInitTime/totalBucketInitCount)
	} else {
		tbp.Logf(ctx, "Total bucket init time: %s for %d buckets", totalBucketInitTime, totalBucketInitCount)
	}
	if totalBucketReadierCount > 0 {
		tbp.Logf(ctx, "Total bucket readier time: %s for %d buckets (avg: %s)", totalBucketReadierTime, totalBucketReadierCount, totalBucketReadierTime/totalBucketReadierCount)
	} else {
		tbp.Logf(ctx, "Total bucket readier time: %s for %d buckets", totalBucketReadierTime, totalBucketReadierCount)
	}
	tbp.Logf(ctx, "Total buckets opened/closed: %d/%d", numBucketsOpened, atomic.LoadInt32(&tbp.stats.NumBucketsClosed))
	if numBucketsOpened > 0 {
		tbp.Logf(ctx, "Total time waiting for ready bucket: %s over %d buckets (avg: %s)", totalBucketWaitTime, numBucketsOpened, totalBucketWaitTime/numBucketsOpened)
		tbp.Logf(ctx, "Total time tests using buckets: %s (avg: %s)", totalBucketUseTime, totalBucketUseTime/numBucketsOpened)
	} else {
		tbp.Logf(ctx, "Total time waiting for ready bucket: %s over %d buckets", totalBucketWaitTime, numBucketsOpened)
		tbp.Logf(ctx, "Total time tests using buckets: %s", totalBucketUseTime)
	}
	tbp.Logf(ctx, "==========================")

	tbp.unclosedBucketsLock.Lock()
	unclosedBucketWarnings := ""
	for testName, buckets := range tbp.unclosedBuckets {
		for bucketName := range buckets {
			tbp.Logf(ctx, "WARNING: %s left %s bucket unclosed!", testName, bucketName)
			unclosedBucketWarnings += fmt.Sprintf("%s left %s bucket unclosed!\n", testName, bucketName)
		}
	}
	if unclosedBucketWarnings != "" {
		panic(unclosedBucketWarnings)
	}
	tbp.unclosedBucketsLock.Unlock()

	tbp.verbose.Set(origVerbose)
}

// bucketPoolStats is the struct used to track runtime/counts of various test bucket operations.
// printStats() is called once a package's tests have finished to output these stats.
type bucketPoolStats struct {
	TotalBucketInitDurationNano    int64
	TotalBucketInitCount           int32
	TotalBucketReadierDurationNano int64
	TotalBucketReadierCount        int32
	NumBucketsOpened               int32
	NumBucketsClosed               int32
	TotalWaitingForReadyBucketNano int64
	TotalInuseBucketNano           int64
}
