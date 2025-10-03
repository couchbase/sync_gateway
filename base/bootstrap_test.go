// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"dario.cat/mergo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeStructPointer(t *testing.T) {
	type structPtr struct {
		I *int
		S string
	}
	type wrap struct {
		Ptr *structPtr
	}
	override := wrap{Ptr: &structPtr{nil, "changed"}}

	source := wrap{Ptr: &structPtr{Ptr(5), "test"}}
	err := mergo.Merge(&source, &override, mergo.WithTransformers(&mergoNilTransformer{}), mergo.WithOverride)

	require.Nil(t, err)
	assert.Equal(t, "changed", source.Ptr.S)
	assert.Equal(t, Ptr(5), source.Ptr.I)
}

func TestBootstrapRefCounting(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires making a connection to CBS")
	}

	ctx := TestCtx(t)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int32(GTestBucketPool.numBuckets), GTestBucketPool.stats.TotalBucketInitCount.Load())
	}, 2*time.Minute, 5*time.Millisecond) // Wait for bucket pool to be initialized, since GetConfigBuckets requires equal buckets to TestBucketPool.numBuckets

	var perBucketCredentialsConfig map[string]*CredentialsConfig
	forcePerBucketAuth := false
	cluster, err := NewCouchbaseCluster(ctx, TestClusterSpec(t), forcePerBucketAuth, perBucketCredentialsConfig, TestUseXattrs(), CachedClusterConnections)
	require.NoError(t, err)
	defer cluster.Close()
	require.NotNil(t, cluster)

	clusterConnection, err := cluster.getClusterConnection()
	require.NoError(t, err)
	require.NotNil(t, clusterConnection)

	buckets, err := cluster.GetConfigBuckets()
	require.NoError(t, err)
	// ensure these are sorted for determinstic bootstraping
	sortedBuckets := make([]string, len(buckets))
	copy(sortedBuckets, buckets)
	sort.Strings(sortedBuckets)
	require.Equal(t, sortedBuckets, buckets)

	var testBuckets []string
	for _, bucket := range buckets {
		if strings.HasPrefix(bucket, tbpBucketNamePrefix) {
			testBuckets = append(testBuckets, bucket)
		}

	}
	require.Len(t, testBuckets, GTestBucketPool.numBuckets)
	// GetConfigBuckets doesn't cache connections, it uses cluster connection to determine number of buckets
	require.Len(t, cluster.cachedBucketConnections.buckets, 0)

	primeBucketConnectionCache := func(bucketNames []string) {
		// Bucket CRUD ops do cache connections
		for _, bucketName := range bucketNames {
			exists, err := cluster.KeyExists(ctx, bucketName, "keyThatDoesNotExist")
			require.NoError(t, err)
			require.False(t, exists)
		}
	}

	primeBucketConnectionCache(buckets)
	require.Len(t, cluster.cachedBucketConnections.buckets, len(buckets))

	// call removeOutdatedBuckets as no-op
	cluster.cachedBucketConnections.removeOutdatedBuckets(SetOf(buckets...))
	require.Len(t, cluster.cachedBucketConnections.buckets, len(buckets))

	// call removeOutdatedBuckets to remove all cached buckets, call multiple times to make sure idempotent
	for range 3 {
		cluster.cachedBucketConnections.removeOutdatedBuckets(Set{})
		require.Len(t, cluster.cachedBucketConnections.buckets, 0)
	}

	primeBucketConnectionCache(buckets)
	require.Len(t, cluster.cachedBucketConnections.buckets, len(buckets))

	// make sure that you can still use an active connection while the bucket has been removed
	wg := sync.WaitGroup{}
	wg.Add(1)
	makeConnection := make(chan struct{})
	go func() {
		defer wg.Done()
		b, teardown, err := cluster.getBucket(ctx, buckets[0])
		defer teardown()
		require.NoError(t, err)
		require.NotNil(t, b)
		<-makeConnection
		// make sure that we can still use bucket after it is no longer cached
		exists, err := cluster.configPersistence.keyExists(b.DefaultCollection(), "keyThatDoesNotExist")
		require.NoError(t, err)
		require.False(t, exists)
	}()

	cluster.cachedBucketConnections.removeOutdatedBuckets(Set{})
	require.Len(t, cluster.cachedBucketConnections.buckets, 0)
	makeConnection <- struct{}{}

	wg.Wait()

	// make sure you can "remove" a non existent bucket in the case that bucket removal is called multiple times
	cluster.cachedBucketConnections.removeOutdatedBuckets(SetOf("not-a-bucket"))

}
