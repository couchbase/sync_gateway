// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"dario.cat/mergo"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbaselabs/rosmar"
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
	cluster, err := NewCouchbaseCluster(ctx, TestClusterSpec(t), perBucketCredentialsConfig, TestUseXattrs(), false, CachedClusterConnections)
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
		cluster, err := NewRosmarCluster(rosmar.InMemoryURL, false)
		require.NoError(t, err)
		t.Cleanup(cluster.Close)
		return cluster
	}
	cluster, err := NewCouchbaseCluster(ctx, TestClusterSpec(t), nil, TestUseXattrs(), false, CachedClusterConnections)
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

	cluster := newTestBootstrapConnection(t)
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

// bootstrapTestCfg is the value shape used by the dual-collection bootstrap tests.
type bootstrapTestCfg struct {
	Foo string `json:"foo"`
}

// seedLegacyBootstrapDoc writes a bootstrap-config-shaped document directly into the bucket's
// _default._default collection, mimicking pre-migration state. The on-disk shape must match what
// configPersistence.loadConfig expects, which is controlled by NewCouchbaseCluster's
// useXattrConfig — a distinct setting from SG's general use_xattrs for application metadata.
// Callers must pass the same useXattrs value used to construct the cluster under test.
func seedLegacyBootstrapDoc(t *testing.T, bucket *gocb.Bucket, docID string, value bootstrapTestCfg, useXattrs bool) {
	t.Helper()
	if useXattrs {
		_, err := bucket.DefaultCollection().MutateIn(docID, []gocb.MutateInSpec{
			gocb.UpsertSpec(cfgXattrConfigPath, value, UpsertSpecXattr),
			gocb.ReplaceSpec("", json.RawMessage(cfgXattrBody), nil),
		}, &gocb.MutateInOptions{StoreSemantic: gocb.StoreSemanticsInsert})
		require.NoError(t, err)
		return
	}
	_, err := bucket.DefaultCollection().Insert(docID, value, nil)
	require.NoError(t, err)
}

// bootstrapDualTestFixture pairs a BootstrapConnection in dual-collection mode with helpers that
// reach past the cluster API to inspect or seed each underlying collection. Implementations exist
// for both Rosmar and CouchbaseCluster; newBootstrapDualTestFixture selects based on the test's
// backing store.
type bootstrapDualTestFixture struct {
	Cluster    BootstrapConnection
	BucketName string
	// SeedLegacy writes a config-shaped doc directly into the fallback (_default._default)
	// collection, bypassing the cluster API.
	SeedLegacy func(t *testing.T, docID string, value bootstrapTestCfg)
	// PrimaryExists reports whether docID currently exists in the primary (_system._mobile).
	PrimaryExists func(t *testing.T, docID string) bool
	// FallbackExists reports whether docID currently exists in _default._default.
	FallbackExists func(t *testing.T, docID string) bool
	// CleanupDoc removes docID from both collections. Idempotent.
	CleanupDoc func(t *testing.T, docID string)
}

// newBootstrapDualTestFixture constructs a fixture for dual-collection bootstrap tests. useXattrs
// selects the bootstrap-config persistence mode on Couchbase Server (NewCouchbaseCluster's
// useXattrConfig); on Rosmar it's ignored since Rosmar's bootstrap path only supports
// document-mode persistence — callers that want to exercise useXattrs=true should skip on
// UnitTestUrlIsWalrus().
func newBootstrapDualTestFixture(t *testing.T, useXattrs bool) bootstrapDualTestFixture {
	t.Helper()
	if UnitTestUrlIsWalrus() {
		return newRosmarBootstrapDualFixture(t)
	}
	return newCouchbaseBootstrapDualFixture(t, useXattrs)
}

// newRosmarBootstrapDualFixture builds a fixture over a test-pool Rosmar bucket. The cluster and
// the seed/inspect helpers all point at the same in-memory bucket via rosmar's process-global
// bucket registry.
func newRosmarBootstrapDualFixture(t *testing.T) bootstrapDualTestFixture {
	t.Helper()
	ctx := TestCtx(t)
	tb := GetTestBucket(t)
	t.Cleanup(func() { tb.Close(ctx) })
	bucketName := tb.GetName()

	cluster, err := NewRosmarCluster(UnitTestUrl(), true)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	defaultDS, err := tb.Bucket.NamedDataStore(ctx, DefaultScopeAndCollectionName())
	require.NoError(t, err)
	systemDS, err := tb.Bucket.NamedDataStore(ctx, MobileSystemScopeAndCollectionName())
	require.NoError(t, err)

	return bootstrapDualTestFixture{
		Cluster:    cluster,
		BucketName: bucketName,
		SeedLegacy: func(t *testing.T, docID string, value bootstrapTestCfg) {
			t.Helper()
			_, err := defaultDS.WriteCas(TestCtx(t), docID, 0, 0, value, 0)
			require.NoError(t, err)
		},
		PrimaryExists: func(t *testing.T, docID string) bool {
			t.Helper()
			ok, err := systemDS.Exists(TestCtx(t), docID)
			require.NoError(t, err)
			return ok
		},
		FallbackExists: func(t *testing.T, docID string) bool {
			t.Helper()
			ok, err := defaultDS.Exists(TestCtx(t), docID)
			require.NoError(t, err)
			return ok
		},
		CleanupDoc: func(t *testing.T, docID string) {
			t.Helper()
			_, _ = systemDS.Remove(TestCtx(t), docID, 0)
			_, _ = defaultDS.Remove(TestCtx(t), docID, 0)
		},
	}
}

// newCouchbaseBootstrapDualFixture builds a fixture over the first available test-pool bucket on a
// real Couchbase Server cluster running in dual-collection mode. useXattrs selects the
// bootstrap-config persistence mode and must match the seed format used by SeedLegacy.
func newCouchbaseBootstrapDualFixture(t *testing.T, useXattrs bool) bootstrapDualTestFixture {
	t.Helper()
	ctx := TestCtx(t)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int32(GTestBucketPool.numBuckets), GTestBucketPool.stats.TotalBucketInitCount.Load())
	}, 2*time.Minute, 5*time.Millisecond)

	cluster, err := NewCouchbaseCluster(ctx, TestClusterSpec(t), nil, useXattrs, true, CachedClusterConnections)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	buckets, err := cluster.GetConfigBuckets(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, buckets)
	bucketName := buckets[0]

	bucket, teardown, err := cluster.getBucket(ctx, bucketName)
	require.NoError(t, err)
	t.Cleanup(teardown)

	primaryCol := bucket.Scope(SystemScope).Collection(SystemCollectionMobile)
	fallbackCol := bucket.DefaultCollection()
	colExists := func(c *gocb.Collection, docID string) bool {
		t.Helper()
		_, err := c.Get(docID, nil)
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return false
		}
		require.NoError(t, err)
		return true
	}

	return bootstrapDualTestFixture{
		Cluster:    cluster,
		BucketName: bucketName,
		SeedLegacy: func(t *testing.T, docID string, value bootstrapTestCfg) {
			t.Helper()
			seedLegacyBootstrapDoc(t, bucket, docID, value, useXattrs)
		},
		PrimaryExists:  func(t *testing.T, docID string) bool { return colExists(primaryCol, docID) },
		FallbackExists: func(t *testing.T, docID string) bool { return colExists(fallbackCol, docID) },
		CleanupDoc: func(t *testing.T, docID string) {
			t.Helper()
			_, _ = primaryCol.Remove(docID, nil)
			_, _ = fallbackCol.Remove(docID, nil)
		},
	}
}

// bootstrapTwoBucketFixture pairs one dual-collection BootstrapConnection with two buckets, plus
// helpers to seed a legacy (fallback-only) bootstrap doc into either. Used to prove that completing
// bootstrap migration on one bucket does not disable the _default._default read-fallback for the
// other.
type bootstrapTwoBucketFixture struct {
	Cluster BootstrapConnection
	BucketA string
	BucketB string
	// SeedLegacy writes a config-shaped doc into the named bucket's fallback (_default._default).
	SeedLegacy func(t *testing.T, bucketName, docID string, value bootstrapTestCfg)
	// CleanupDoc removes docID from the named bucket's fallback. Idempotent.
	CleanupDoc func(t *testing.T, bucketName, docID string)
}

// newBootstrapTwoBucketDualFixture builds a two-bucket dual-collection fixture for the active
// backing store. useXattrs selects the bootstrap-config persistence mode on Couchbase Server (see
// newBootstrapDualTestFixture); it is ignored on Rosmar.
func newBootstrapTwoBucketDualFixture(t *testing.T, useXattrs bool) bootstrapTwoBucketFixture {
	t.Helper()
	if UnitTestUrlIsWalrus() {
		return newRosmarBootstrapTwoBucketDualFixture(t)
	}
	return newCouchbaseBootstrapTwoBucketDualFixture(t, useXattrs)
}

func newRosmarBootstrapTwoBucketDualFixture(t *testing.T) bootstrapTwoBucketFixture {
	t.Helper()
	ctx := TestCtx(t)
	tbA := GetTestBucket(t)
	t.Cleanup(func() { tbA.Close(ctx) })
	tbB := GetTestBucket(t)
	t.Cleanup(func() { tbB.Close(ctx) })

	cluster, err := NewRosmarCluster(UnitTestUrl(), true)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	defaultDS := make(map[string]DataStore, 2)
	for _, tb := range []*TestBucket{tbA, tbB} {
		ds, err := tb.Bucket.NamedDataStore(ctx, DefaultScopeAndCollectionName())
		require.NoError(t, err)
		defaultDS[tb.GetName()] = ds
	}

	return bootstrapTwoBucketFixture{
		Cluster: cluster,
		BucketA: tbA.GetName(),
		BucketB: tbB.GetName(),
		SeedLegacy: func(t *testing.T, bucketName, docID string, value bootstrapTestCfg) {
			t.Helper()
			_, err := defaultDS[bucketName].WriteCas(TestCtx(t), docID, 0, 0, value, 0)
			require.NoError(t, err)
		},
		CleanupDoc: func(t *testing.T, bucketName, docID string) {
			t.Helper()
			_, _ = defaultDS[bucketName].Remove(TestCtx(t), docID, 0)
		},
	}
}

func newCouchbaseBootstrapTwoBucketDualFixture(t *testing.T, useXattrs bool) bootstrapTwoBucketFixture {
	t.Helper()
	ctx := TestCtx(t)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int32(GTestBucketPool.numBuckets), GTestBucketPool.stats.TotalBucketInitCount.Load())
	}, 2*time.Minute, 5*time.Millisecond)

	cluster, err := NewCouchbaseCluster(ctx, TestClusterSpec(t), nil, useXattrs, true, CachedClusterConnections)
	require.NoError(t, err)
	t.Cleanup(cluster.Close)

	// Claim two clean buckets from the test pool rather than enumerating arbitrary cluster buckets
	// via GetConfigBuckets. A pooled bucket is flushed between tests, so it carries no leftover
	// _sync:registry or migration-status docs. An arbitrary cluster bucket might have been left
	// bootstrap-migration-complete by a prior tenant; getBucket below (via
	// ensureBucketBootstrapTargetCached) would then latch it as migration-complete, disabling the
	// _default fallback before this test seeds anything and breaking its first sanity assertion.
	tbA := GetTestBucket(t)
	t.Cleanup(func() { tbA.Close(ctx) })
	tbB := GetTestBucket(t)
	t.Cleanup(func() { tbB.Close(ctx) })

	gocbBuckets := make(map[string]*gocb.Bucket, 2)
	for _, tb := range []*TestBucket{tbA, tbB} {
		name := tb.GetName()
		b, teardown, err := cluster.getBucket(ctx, name)
		require.NoError(t, err)
		t.Cleanup(teardown)
		gocbBuckets[name] = b
	}

	return bootstrapTwoBucketFixture{
		Cluster: cluster,
		BucketA: tbA.GetName(),
		BucketB: tbB.GetName(),
		SeedLegacy: func(t *testing.T, bucketName, docID string, value bootstrapTestCfg) {
			t.Helper()
			seedLegacyBootstrapDoc(t, gocbBuckets[bucketName], docID, value, useXattrs)
		},
		CleanupDoc: func(t *testing.T, bucketName, docID string) {
			t.Helper()
			_, _ = gocbBuckets[bucketName].DefaultCollection().Remove(docID, nil)
		},
	}
}

// TestBootstrapMigrationCompleteIsPerBucket is a regression test for the connection-wide
// SetMigrationComplete: completing bootstrap migration on one bucket must not disable the
// _default._default read-fallback for a different bucket that has not migrated.
func TestBootstrapMigrationCompleteIsPerBucket(t *testing.T) {
	RequireNumTestBuckets(t, 2)
	forEachBootstrapXattrMode(t, func(t *testing.T, useXattrs bool) {
		f := newBootstrapTwoBucketDualFixture(t, useXattrs)
		ctx := TestCtx(t)
		docID := SyncDocPrefix + "per-bucket-migration-complete"

		// Both buckets start un-migrated: their config doc lives only in the _default fallback.
		t.Cleanup(func() {
			f.CleanupDoc(t, f.BucketA, docID)
			f.CleanupDoc(t, f.BucketB, docID)
		})
		f.SeedLegacy(t, f.BucketA, docID, bootstrapTestCfg{Foo: "bucketA"})
		f.SeedLegacy(t, f.BucketB, docID, bootstrapTestCfg{Foo: "bucketB"})

		// Sanity: both buckets resolve their doc via the fallback before any completion.
		var loaded bootstrapTestCfg
		_, err := f.Cluster.GetMetadataDocument(ctx, f.BucketA, docID, &loaded)
		require.NoError(t, err)
		require.Equal(t, "bucketA", loaded.Foo)
		_, err = f.Cluster.GetMetadataDocument(ctx, f.BucketB, docID, &loaded)
		require.NoError(t, err)
		require.Equal(t, "bucketB", loaded.Foo)

		// Complete bootstrap migration on bucket A only.
		f.Cluster.SetMigrationComplete(f.BucketA)

		// Bucket A's fallback is now disabled: its fallback-only doc is no longer visible. This
		// confirms the completion flag actually took effect for A.
		_, err = f.Cluster.GetMetadataDocument(ctx, f.BucketA, docID, &loaded)
		require.Error(t, err)
		require.True(t, IsDocNotFoundError(err), "bucket A fallback must be disabled after its migration completes, got: %v", err)

		// THE REGRESSION CHECK: bucket B has NOT migrated, so its fallback must still be active and
		// its doc still readable. With the old connection-wide flag this read returned not-found.
		loaded = bootstrapTestCfg{}
		_, err = f.Cluster.GetMetadataDocument(ctx, f.BucketB, docID, &loaded)
		require.NoError(t, err, "completing bucket A's migration must not disable bucket B's fallback")
		require.Equal(t, "bucketB", loaded.Foo)

		// Completing bucket B disables its fallback too — confirming the flag is genuinely per-bucket
		// in both directions, not a one-off.
		f.Cluster.SetMigrationComplete(f.BucketB)
		_, err = f.Cluster.GetMetadataDocument(ctx, f.BucketB, docID, &loaded)
		require.Error(t, err)
		require.True(t, IsDocNotFoundError(err), "bucket B fallback must be disabled after its own migration completes, got: %v", err)
	})
}

// forEachBootstrapXattrMode runs body once with useXattrs=false and once with useXattrs=true,
// each as a t.Run subtest. The useXattrs=true subtest is skipped on Rosmar — its bootstrap
// path doesn't support xattr-mode persistence, so the variant has no meaningful coverage
// there.
func forEachBootstrapXattrMode(t *testing.T, body func(t *testing.T, useXattrs bool)) {
	t.Helper()
	for _, useXattrs := range []bool{false, true} {
		name := "bootstrap_xattr=false"
		if useXattrs {
			name = "bootstrap_xattr=true"
		}
		t.Run(name, func(t *testing.T) {
			if useXattrs && UnitTestUrlIsWalrus() {
				t.Skip("Rosmar bootstrap does not support xattr-mode persistence")
			}
			body(t, useXattrs)
		})
	}
}

// TestBootstrapInsertMetadataDocumentWritesPrimary verifies that with no pre-existing legacy copy,
// InsertMetadataDocument lands in the primary (_system._mobile) collection and not the fallback.
func TestBootstrapInsertMetadataDocumentWritesPrimary(t *testing.T) {
	forEachBootstrapXattrMode(t, func(t *testing.T, useXattrs bool) {
		f := newBootstrapDualTestFixture(t, useXattrs)
		ctx := TestCtx(t)
		docID := SyncDocPrefix + "metadata-insert-primary"
		t.Cleanup(func() { f.CleanupDoc(t, docID) })

		_, err := f.Cluster.InsertMetadataDocument(ctx, f.BucketName, docID, bootstrapTestCfg{Foo: "primary"})
		require.NoError(t, err)

		require.True(t, f.PrimaryExists(t, docID))
		require.False(t, f.FallbackExists(t, docID))

		var loaded bootstrapTestCfg
		_, err = f.Cluster.GetMetadataDocument(ctx, f.BucketName, docID, &loaded)
		require.NoError(t, err)
		require.Equal(t, "primary", loaded.Foo)
	})
}

// TestBootstrapWriteMetadataDocumentFallbackCAS verifies WriteMetadataDocument replays against the
// fallback collection when the supplied CAS came from a fallback read, so callers don't see a
// spurious not-found just because the doc hasn't migrated to _system._mobile yet.
func TestBootstrapWriteMetadataDocumentFallbackCAS(t *testing.T) {
	forEachBootstrapXattrMode(t, func(t *testing.T, useXattrs bool) {
		f := newBootstrapDualTestFixture(t, useXattrs)
		ctx := TestCtx(t)
		docID := SyncDocPrefix + "metadata-write-fallback-cas"
		t.Cleanup(func() { f.CleanupDoc(t, docID) })
		f.SeedLegacy(t, docID, bootstrapTestCfg{Foo: "legacy"})

		var initial bootstrapTestCfg
		cas, err := f.Cluster.GetMetadataDocument(ctx, f.BucketName, docID, &initial)
		require.NoError(t, err)
		require.Equal(t, "legacy", initial.Foo)
		require.NotZero(t, cas)

		newCAS, err := f.Cluster.WriteMetadataDocument(ctx, f.BucketName, docID, cas, bootstrapTestCfg{Foo: "updated"})
		require.NoError(t, err)
		require.NotEqual(t, cas, newCAS)

		var reloaded bootstrapTestCfg
		_, err = f.Cluster.GetMetadataDocument(ctx, f.BucketName, docID, &reloaded)
		require.NoError(t, err)
		require.Equal(t, "updated", reloaded.Foo)

		require.False(t, f.PrimaryExists(t, docID), "write must not have migrated the doc to primary")
		require.True(t, f.FallbackExists(t, docID))
	})
}

// TestBootstrapInsertMetadataDocumentFallbackDuplicate verifies InsertMetadataDocument returns
// ErrAlreadyExists when the doc already lives in the fallback collection - never silently creating
// a divergent primary copy.
func TestBootstrapInsertMetadataDocumentFallbackDuplicate(t *testing.T) {
	forEachBootstrapXattrMode(t, func(t *testing.T, useXattrs bool) {
		f := newBootstrapDualTestFixture(t, useXattrs)
		ctx := TestCtx(t)
		docID := SyncDocPrefix + "metadata-insert-fallback-dup"
		t.Cleanup(func() { f.CleanupDoc(t, docID) })
		f.SeedLegacy(t, docID, bootstrapTestCfg{Foo: "legacy"})

		_, err := f.Cluster.InsertMetadataDocument(ctx, f.BucketName, docID, bootstrapTestCfg{Foo: "primary"})
		require.ErrorIs(t, err, ErrAlreadyExists)
		require.False(t, f.PrimaryExists(t, docID), "InsertMetadataDocument must not have created a primary copy")
	})
}

// TestBootstrapTouchMetadataDocumentFallback verifies TouchMetadataDocument retries against the
// fallback collection when the primary returns ErrNotFound, leaving the doc in place rather than
// migrating it.
func TestBootstrapTouchMetadataDocumentFallback(t *testing.T) {
	forEachBootstrapXattrMode(t, func(t *testing.T, useXattrs bool) {
		f := newBootstrapDualTestFixture(t, useXattrs)
		ctx := TestCtx(t)
		docID := SyncDocPrefix + "metadata-touch-fallback"
		t.Cleanup(func() { f.CleanupDoc(t, docID) })
		f.SeedLegacy(t, docID, bootstrapTestCfg{Foo: "legacy"})

		var initial bootstrapTestCfg
		cas, err := f.Cluster.GetMetadataDocument(ctx, f.BucketName, docID, &initial)
		require.NoError(t, err)
		require.NotZero(t, cas)

		newCAS, err := f.Cluster.TouchMetadataDocument(ctx, f.BucketName, docID, "version", "v2", cas)
		require.NoError(t, err)
		require.NotEqual(t, cas, newCAS, "Touch must bump CAS")
		require.False(t, f.PrimaryExists(t, docID))
		require.True(t, f.FallbackExists(t, docID))
	})
}

// TestBootstrapDeleteMetadataDocumentFallback verifies DeleteMetadataDocument retries against the
// fallback collection when the primary returns ErrNotFound, removing the legacy doc.
func TestBootstrapDeleteMetadataDocumentFallback(t *testing.T) {
	forEachBootstrapXattrMode(t, func(t *testing.T, useXattrs bool) {
		f := newBootstrapDualTestFixture(t, useXattrs)
		ctx := TestCtx(t)
		docID := SyncDocPrefix + "metadata-delete-fallback"
		t.Cleanup(func() { f.CleanupDoc(t, docID) })
		f.SeedLegacy(t, docID, bootstrapTestCfg{Foo: "legacy"})

		var initial bootstrapTestCfg
		cas, err := f.Cluster.GetMetadataDocument(ctx, f.BucketName, docID, &initial)
		require.NoError(t, err)
		require.NotZero(t, cas)

		require.NoError(t, f.Cluster.DeleteMetadataDocument(ctx, f.BucketName, docID, cas))
		require.False(t, f.FallbackExists(t, docID))

		var afterDelete bootstrapTestCfg
		_, err = f.Cluster.GetMetadataDocument(ctx, f.BucketName, docID, &afterDelete)
		require.True(t, IsDocNotFoundError(err), "expected not-found after delete, got %T: %v", err, err)
	})
}
