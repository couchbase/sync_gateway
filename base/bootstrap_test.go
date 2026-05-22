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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"dario.cat/mergo"
	"github.com/couchbaselabs/rosmar"
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

	buckets, err := cluster.GetConfigBuckets(ctx)
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

// newTestBootstrapConnection returns a BootstrapConnection bound to the current test backing store
// (Rosmar in-memory or Couchbase Server) so a single test can exercise both implementations.
func newTestBootstrapConnection(t *testing.T) BootstrapConnection {
	t.Helper()
	ctx := TestCtx(t)
	if UnitTestUrlIsWalrus() {
		cluster, err := NewRosmarCluster(rosmar.InMemoryURL)
		require.NoError(t, err)
		t.Cleanup(cluster.Close)
		return cluster
	}
	cluster, err := NewCouchbaseCluster(ctx, TestClusterSpec(t), false, nil, TestUseXattrs(), CachedClusterConnections)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)
	return cluster
}

func TestBootstrapConnectionGetRawDocument(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	ds := bucket.DefaultDataStore(ctx)
	bucketName := bucket.GetName()

	cluster := newTestBootstrapConnection(t)
	const docID = "test_get_raw_doc"

	t.Run("doc absent", func(t *testing.T) {
		value, exists, err := cluster.GetRawDocument(ctx, bucketName, docID)
		require.NoError(t, err)
		require.False(t, exists)
		require.Nil(t, value)
	})

	t.Run("legacy JSON", func(t *testing.T) {
		defer func() { _ = ds.Delete(ctx, docID) }()
		legacyJSON := []byte(`{"metadataID":"x"}`)
		require.NoError(t, ds.SetRaw(ctx, docID, 0, nil, legacyJSON))

		value, exists, err := cluster.GetRawDocument(ctx, bucketName, docID)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, legacyJSON, value)
		require.Equal(t, byte('{'), value[0])
	})

	t.Run("V1 binary", func(t *testing.T) {
		defer func() { _ = ds.Delete(ctx, docID) }()
		payload := append([]byte{byte(SyncInfoTypeV1)}, []byte(`{"metadataID":"x"}`)...)
		require.NoError(t, ds.SetRaw(ctx, docID, 0, nil, payload))

		value, exists, err := cluster.GetRawDocument(ctx, bucketName, docID)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, payload, value)
		require.Equal(t, byte(SyncInfoTypeV1), value[0])
	})
}

// TestBootstrapConnectionGetDocumentExists GetDocument must report exists=true for a doc that exists.
func TestBootstrapConnectionGetDocumentExists(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	ds := bucket.DefaultDataStore(ctx)
	bucketName := bucket.GetName()

	cluster := newTestBootstrapConnection(t)

	const docID = "test_get_doc_exists"
	require.NoError(t, ds.Set(ctx, docID, 0, nil, []byte(`{"x":1}`)))

	var got map[string]any
	exists, err := cluster.GetDocument(ctx, bucketName, docID, &got)
	require.NoError(t, err)
	require.True(t, exists, "expected GetDocument to report exists=true for an existing doc")
}

// TestTouchMetadataDocument verifies that BootstrapConnection.TouchMetadataDocument honours the supplied CAS
// (rejecting stale-CAS callers with a CasMismatch error) and bumps the document's CAS on success.
// This is the mechanism rest/config_manager.go relies on to block racing writers during registry rollback.
func TestTouchMetadataDocument(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	cluster := newTestBootstrapConnection(t, ctx)
	defer cluster.Close()

	bucketName := bucket.BucketSpec.BucketName
	docID := t.Name()
	body := map[string]any{"name": "original"}

	originalCAS, err := cluster.InsertMetadataDocument(ctx, bucketName, docID, body)
	require.NoError(t, err)
	defer func() { _ = cluster.DeleteMetadataDocument(ctx, bucketName, docID, 0) }()

	// Stale CAS must be rejected with CasMismatch (mirrors CBS subdoc Touch with Cas option).
	_, err = cluster.TouchMetadataDocument(ctx, bucketName, docID, "name", "db1", originalCAS+1)
	require.Error(t, err)
	require.True(t, IsCasMismatch(err), "expected CasMismatch, got %T: %v", err, err)

	// Correct CAS succeeds and returns a new (different) CAS.
	newCAS, err := cluster.TouchMetadataDocument(ctx, bucketName, docID, "name", "db1", originalCAS)
	require.NoError(t, err)
	require.NotEqual(t, originalCAS, newCAS, "TouchMetadataDocument should bump CAS")

	// The previously-valid CAS is now stale.
	_, err = cluster.TouchMetadataDocument(ctx, bucketName, docID, "name", "db2", originalCAS)
	require.Error(t, err)
	require.True(t, IsCasMismatch(err), "expected CasMismatch on stale CAS retry, got %T: %v", err, err)
}

// newTestBootstrapConnection constructs a BootstrapConnection matching the current test backing
// store (Rosmar or Couchbase Server).
func newTestBootstrapConnection(t *testing.T, ctx context.Context) BootstrapConnection {
	t.Helper()
	if UnitTestUrlIsWalrus() {
		cluster, err := NewRosmarCluster(UnitTestUrl())
		require.NoError(t, err)
		return cluster
	}
	cluster, err := NewCouchbaseCluster(ctx, TestClusterSpec(t), false, nil, TestUseXattrs(), CachedClusterConnections)
	require.NoError(t, err)
	return cluster
}
