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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Bucket names start with a fixed prefix and end with a sequential bucket number and a creation timestamp for uniqueness
const (
	tbpBucketNamePrefix = "sg_int_"
	tbpBucketNameFormat = "%s%d_%d"
	tbpScopePrefix      = "sg_test_"
	tbpCollectionPrefix = "sg_test_"
)

const (
	envTestClusterUsername     = "SG_TEST_USERNAME"
	defaultTestClusterUsername = "Administrator"

	envTestClusterPassword     = "SG_TEST_PASSWORD"
	defaultTestClusterPassword = "password"

	// Creates and prepares this many buckets in the backing store to be pooled for testing.
	tbpDefaultBucketPoolSize = 4
	tbpEnvBucketPoolSize     = "SG_TEST_BUCKET_POOL_SIZE"

	// Creates and prepares this many collections in each bucket in the backing store.
	tbpDefaultCollectionPoolSize = 2 // (per bucket)
	tbpEnvCollectionPoolSize     = "SG_TEST_COLLECTION_POOL_SIZE"

	// Allocate this much memory to each bucket.
	defaultBucketQuotaMB = 200
	tbpEnvBucketQuotaMB  = "SG_TEST_BUCKET_QUOTA_MB"

	// Prevents reuse and cleanup of buckets used in failed tests for later inspection.
	// When all pooled buckets are in a preserved state, any remaining tests are skipped instead of blocking waiting for a bucket.
	tbpEnvPreserve = "SG_TEST_BUCKET_POOL_PRESERVE"

	// Prints detailed debug logs from the test pooling framework.
	tbpEnvVerbose = "SG_TEST_BUCKET_POOL_DEBUG"

	tbpEnvUseDefaultCollection = "SG_TEST_USE_DEFAULT_COLLECTION"

	// tbpEnvAllowIncompatibleServerVersion allows tests to run against a server version that is not presumed compatible with version of Couchbase Server running.
	tbpEnvAllowIncompatibleServerVersion = "SG_TEST_SKIP_SERVER_VERSION_CHECK"

	// wait this long when requesting a test bucket from the pool before giving up and failing the test.
	waitForReadyBucketTimeout = time.Minute

	// Creates buckets with a specific number of number of replicas
	tbpEnvBucketNumReplicas = "SG_TEST_BUCKET_NUM_REPLICAS"

	// Environment variable to specify the topology tests to run
	TbpEnvTopologyTests = "SG_TEST_TOPOLOGY_TESTS"

	// Environment variable used by bucket pool to specify the conflict resolution strategy to use for the test buckets.
	tbpEnvXDCRConflictResolutionStrategy = "SG_TEST_XDCR_CONFLICT_RESOLUTION_STRATEGY"
)

// TestsUseNamedCollections returns true if the tests use named collections.
func TestsUseNamedCollections() bool {
	return GTestBucketPool.canUseNamedCollections()
}

// TestsUseServerCE returns true if the tests are targeting a CE server.
func TestsUseServerCE() bool {
	return !GTestBucketPool.cluster.isServerEnterprise()
}

// TestsRequireMobileRBAC skips the test if the server does not support Sync Gateway RBAC roles. These are supported by Couchbase Server Enterprise Edition only.
func TestsRequireMobileRBAC(t *testing.T) {
	if !TestCanUseMobileRBAC(t) {
		t.Skip("Mobile RBAC roles for Sync Gateway are only fully supported by Couchbase Server Enterprise")
	}
}

// TestCanUseMobileRBAC returns true if the server has Sync Gateway RBAC roles.
func TestCanUseMobileRBAC(_ *testing.T) bool {
	// rosmar does not have a cluster
	if GTestBucketPool.cluster == nil {
		return false
	}
	return GTestBucketPool.cluster.supportsMobileRBAC()
}

// canUseNamedCollections returns true if the cluster supports named collections, and they are also requested
func (tbp *TestBucketPool) canUseNamedCollections() bool {
	// Walrus views work with collections - Server views do not - we need GSI when running with CB Server...
	if !UnitTestUrlIsWalrus() && TestsDisableGSI() {
		return false
	}

	// if we've not explicitly set a use default collection flag - determine support based on other flags
	useDefaultCollection, isSet := os.LookupEnv(tbpEnvUseDefaultCollection)
	if !isSet {
		return true
	}

	requestDefaultCollection, _ := strconv.ParseBool(useDefaultCollection)
	requestNamedCollection := !requestDefaultCollection
	return requestNamedCollection
}

// tbpNumReplicas returns the number of replicas to use in each bucket.
func tbpNumReplicas() (int, error) {
	numReplicas := os.Getenv(tbpEnvBucketNumReplicas)
	if numReplicas == "" {
		return 0, nil
	}
	replicas, err := strconv.Atoi(numReplicas)
	if err != nil {
		return 0, fmt.Errorf("couldn't parse %s: %w", tbpEnvBucketPoolSize, err)
	}
	return replicas, nil
}

// tbpNumCollectionsPerBucket returns the configured number of collections prepared in a bucket.
func tbpNumCollectionsPerBucket(ctx context.Context) int {
	numCollectionsPerBucket := tbpDefaultCollectionPoolSize
	if envCollectionPoolSize := os.Getenv(tbpEnvCollectionPoolSize); envCollectionPoolSize != "" {
		var err error
		numCollectionsPerBucket, err = strconv.Atoi(envCollectionPoolSize)
		if err != nil {
			FatalfCtx(ctx, "Couldn't parse %s: %v", tbpEnvCollectionPoolSize, err)
		}
	}
	return numCollectionsPerBucket
}

// tbpBucketQuotaMB returns the configured bucket RAM quota.
func tbpBucketQuotaMB(ctx context.Context) int {
	bucketQuota := defaultBucketQuotaMB
	if envBucketQuotaMB := os.Getenv(tbpEnvBucketQuotaMB); envBucketQuotaMB != "" {
		var err error
		bucketQuota, err = strconv.Atoi(envBucketQuotaMB)
		if err != nil {
			FatalfCtx(ctx, "Couldn't parse %s: %v", tbpEnvBucketQuotaMB, err)
		}
	}
	return bucketQuota
}

// tbpVerbose returns the configured test bucket pool verbose flag.
func tbpVerbose() bool {
	verbose, _ := strconv.ParseBool(os.Getenv(tbpEnvVerbose))
	return verbose
}

// TestClusterUsername returns the configured cluster username.
func TestClusterUsername() string {
	username := defaultTestClusterUsername
	if envClusterUsername := os.Getenv(envTestClusterUsername); envClusterUsername != "" {
		username = envClusterUsername
	}
	return username
}

// TestClusterPassword returns the configured cluster password.
func TestClusterPassword() string {
	password := defaultTestClusterPassword
	if envClusterPassword := os.Getenv(envTestClusterPassword); envClusterPassword != "" {
		password = envClusterPassword
	}
	return password
}

// TestRunSGCollectIntegrationTests runs the tests only if a specific environment variable is set. These should always run under jenkins/github actions.
func TestRunSGCollectIntegrationTests(t *testing.T) {
	env := "SG_TEST_SGCOLLECT_INTEGRATION"
	val, ok := os.LookupEnv(env)
	if !ok {
		ciEnvVars := []string{
			"CI",          // convention by github actions
			"JENKINS_URL", // from jenkins
		}
		for _, ciEnv := range ciEnvVars {
			if os.Getenv(ciEnv) != "" {
				return
			}
		}
		t.Skip("Skipping sgcollect integration tests - set " + env + "=true to run")
	}

	runTests, err := strconv.ParseBool(val)
	require.NoError(t, err, "Couldn't parse %s=%s as bool", env, val)
	if !runTests {
		t.Skip("Skipping sgcollect integration tests - set " + env + "=true to run")
	}
}
