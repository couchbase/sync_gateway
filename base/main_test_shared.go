package base

import "testing"

// SetupTest is common setup/teardown for all SG packages to invoke.
// bucketReadierFunc and bucketInitFunc can be set to nil if there's no need for a test bucket pool.
func SetupTest(m *testing.M, bucketReadierFunc TBPBucketReadierFunc, bucketInitFunc TBPBucketInitFunc) (teardownFn func()) {
	teardownLoggingFn := SetUpGlobalTestLogging(m)
	teardownProfilingFn := SetUpGlobalTestProfiling(m)

	isTest = true
	SkipPrometheusStatsRegistration = true

	setupTestBucketPool := bucketReadierFunc != nil && bucketInitFunc != nil
	if setupTestBucketPool {
		GTestBucketPool = NewTestBucketPool(bucketReadierFunc, bucketInitFunc)
	}

	return func() {
		if setupTestBucketPool {
			GTestBucketPool.Close()
		}
		teardownLoggingFn()
		teardownProfilingFn()
	}
}
