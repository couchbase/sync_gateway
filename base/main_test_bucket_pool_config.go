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
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
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
	DefaultTestClusterUsername = DefaultCouchbaseAdministrator

	envTestClusterPassword     = "SG_TEST_PASSWORD"
	DefaultTestClusterPassword = DefaultCouchbasePassword

	// Creates and prepares this many buckets in the backing store to be pooled for testing.
	tbpDefaultBucketPoolSize = 3
	tbpEnvBucketPoolSize     = "SG_TEST_BUCKET_POOL_SIZE"

	// Creates and prepares this many collections in each bucket in the backing store.
	tbpDefaultCollectionPoolSize = 3 // (per bucket)
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

	// wait this long when requesting a test bucket from the pool before giving up and failing the test.
	waitForReadyBucketTimeout = time.Minute

	// Creates buckets with a specific number of number of replicas
	tbpEnvBucketNumReplicas = "SG_TEST_BUCKET_NUM_REPLICAS"
)

var tbpDefaultBucketSpec = BucketSpec{
	Server: UnitTestUrl(),
	Auth: TestAuthenticator{
		Username: TestClusterUsername(),
		Password: TestClusterPassword(),
	},
	UseXattrs: TestUseXattrs(),
}

// TestsUseNamedCollections returns true if the tests use named collections.
func TestsUseNamedCollections() bool {
	ok, err := GTestBucketPool.canUseNamedCollections()
	return err == nil && ok
}

// TestsUseNamedCollections returns true if the tests use named collections.
func TestsUseServerCE() bool {
	ok, err := GTestBucketPool.cluster.isServerEnterprise()
	return err == nil && ok
}

// TestsRequireMobileRBAC returns true if the server has Sync Gateway RBAC roles.
func TestsRequireMobileRBAC(t *testing.T) {
	ok, err := GTestBucketPool.cluster.supportsMobileRBAC()
	if err != nil || !ok {
		t.Skip("Mobile RBAC roles for Sync Gateway are only fully supported in CBS 7.1 or greater")
	}
}

// canUseNamedCollections returns true if the cluster supports named collections, and they are also requested
func (tbp *TestBucketPool) canUseNamedCollections() (bool, error) {
	// walrus supports collections, but we need to query the server's version for capability check
	clusterSupport := true
	if tbp.cluster != nil {
		var err error
		clusterSupport, err = tbp.cluster.supportsCollections()
		if err != nil {
			return false, err
		}
	}

	// Walrus views work with collections - Server views do not - we need GSI when running with CB Server...
	queryStoreSupportsCollections := true
	if !UnitTestUrlIsWalrus() && TestsDisableGSI() {
		queryStoreSupportsCollections = false
	}

	// if we've not explicitly set a use default collection flag - determine support based on other flags
	useDefaultCollection, isSet := os.LookupEnv(tbpEnvUseDefaultCollection)
	if !isSet {
		if !queryStoreSupportsCollections {
			tbp.Logf(context.TODO(), "GSI disabled - not using named collections")
			return false, nil
		}
		tbp.Logf(context.TODO(), "Will use named collections if cluster supports them: %v", clusterSupport)
		// use collections if running GSI and server >= 7
		return clusterSupport, nil
	}

	requestDefaultCollection, _ := strconv.ParseBool(useDefaultCollection)
	requestNamedCollection := !requestDefaultCollection
	if requestNamedCollection {
		if !clusterSupport {
			return false, errors.New("Unable to use named collections - Cluster does not support collections")
		}
		if !queryStoreSupportsCollections {
			return false, errors.New("Unable to use named collections - GSI disabled")
		}

	}

	return requestNamedCollection, nil

}

// tbpNumBuckets returns the configured number of buckets to use in the pool.
func tbpNumBuckets() int {
	numBuckets := tbpDefaultBucketPoolSize
	if envPoolSize := os.Getenv(tbpEnvBucketPoolSize); envPoolSize != "" {
		var err error
		numBuckets, err = strconv.Atoi(envPoolSize)
		if err != nil {
			FatalfCtx(context.TODO(), "Couldn't parse %s: %v", tbpEnvBucketPoolSize, err)
		}
	}
	return numBuckets
}

// tbpNumReplicasreturns the number of replicas to use in each bucket.
func tbpNumReplicas() uint32 {
	numReplicas := os.Getenv(tbpEnvBucketNumReplicas)
	if numReplicas == "" {
		return 0
	}
	replicas, err := strconv.Atoi(numReplicas)
	if err != nil {
		FatalfCtx(context.TODO(), "Couldn't parse %s: %v", tbpEnvBucketPoolSize, err)
	}
	return uint32(replicas)
}

// tbpNumCollectionsPerBucket returns the configured number of collections prepared in a bucket.
func tbpNumCollectionsPerBucket() int {
	numCollectionsPerBucket := tbpDefaultCollectionPoolSize
	if envCollectionPoolSize := os.Getenv(tbpEnvCollectionPoolSize); envCollectionPoolSize != "" {
		var err error
		numCollectionsPerBucket, err = strconv.Atoi(envCollectionPoolSize)
		if err != nil {
			FatalfCtx(context.TODO(), "Couldn't parse %s: %v", tbpEnvCollectionPoolSize, err)
		}
	}
	return numCollectionsPerBucket
}

// tbpBucketQuotaMB returns the configured bucket RAM quota.
func tbpBucketQuotaMB() int {
	bucketQuota := defaultBucketQuotaMB
	if envBucketQuotaMB := os.Getenv(tbpEnvBucketQuotaMB); envBucketQuotaMB != "" {
		var err error
		bucketQuota, err = strconv.Atoi(envBucketQuotaMB)
		if err != nil {
			FatalfCtx(context.TODO(), "Couldn't parse %s: %v", tbpEnvBucketQuotaMB, err)
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
	username := DefaultTestClusterUsername
	if envClusterUsername := os.Getenv(envTestClusterUsername); envClusterUsername != "" {
		username = envClusterUsername
	}
	return username
}

// TestClusterPassword returns the configured cluster password.
func TestClusterPassword() string {
	password := DefaultTestClusterPassword
	if envClusterPassword := os.Getenv(envTestClusterPassword); envClusterPassword != "" {
		password = envClusterPassword
	}
	return password
}
