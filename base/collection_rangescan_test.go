/*
Copyright 2025-Present Couchbase, Inc.

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
	"sort"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// collectScanIDs runs a scan and returns the sorted list of document IDs.
func collectScanIDs(t testing.TB, ctx context.Context, rss sgbucket.RangeScanStore, scanType sgbucket.ScanType, opts sgbucket.ScanOptions) []string {
	t.Helper()
	iter, err := rss.Scan(ctx, scanType, opts)
	require.NoError(t, err)
	defer func() { assert.NoError(t, iter.Close(ctx)) }()

	var ids []string
	for item := iter.Next(ctx); item != nil; item = iter.Next(ctx) {
		ids = append(ids, item.ID)
	}
	// A clean drain must leave no error, inspectable without closing the iterator.
	assert.NoError(t, iter.Err())
	sort.Strings(ids)
	return ids
}

// rangeScanFixture seeds a fixed set of documents and waits for them to be visible to scan.
// KV range scan reads from a per-vBucket snapshot; we do not pass SnapshotRequirements on
// CreateRangeScan, so a scan issued immediately after a write can miss it until the vBucket's
// scan view catches up. The wait is independent of the on-disk storage engine (couchstore vs
// magma) and is not about persistence to disk.
func rangeScanFixture(t *testing.T, ctx context.Context, writeDataStore sgbucket.DataStore, scanStore sgbucket.RangeScanStore) []string {
	t.Helper()
	docs := map[string][]byte{
		"doc_a": []byte(`{"name":"alpha"}`),
		"doc_b": []byte(`{"name":"bravo"}`),
		"doc_c": []byte(`{"name":"charlie"}`),
		"doc_d": []byte(`{"name":"delta"}`),
		"doc_e": []byte(`{"name":"echo"}`),
	}
	for k, v := range docs {
		require.NoError(t, writeDataStore.SetRaw(ctx, k, 0, nil, v))
	}
	allDocIDs := []string{"doc_a", "doc_b", "doc_c", "doc_d", "doc_e"}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ids := collectScanIDs(t, ctx, scanStore, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{IDsOnly: true})
		assert.Equal(c, allDocIDs, ids)
	}, 30*time.Second, 100*time.Millisecond)

	return allDocIDs
}

// runRangeScanSubtests runs the common range-scan assertions against the given (writeDS, scanStore) pair.
// writeDS is used to seed documents and delete one for the tombstone test; scanStore drives Scan() calls.
// In the admin case these are the same datastore; in the RBAC case writeDS is the admin connection
// (so we don't need management permissions on the RBAC user) while scanStore is the RBAC connection.
func runRangeScanSubtests(t *testing.T, ctx context.Context, writeDS sgbucket.DataStore, scanStore sgbucket.RangeScanStore) {
	allDocIDs := rangeScanFixture(t, ctx, writeDS, scanStore)

	t.Run("FullRange", func(t *testing.T) {
		iter, err := scanStore.Scan(ctx, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close(ctx)) }()

		var ids []string
		for item := iter.Next(ctx); item != nil; item = iter.Next(ctx) {
			ids = append(ids, item.ID)
			assert.NotNil(t, item.Body, "Expected body for key %s", item.ID)
			assert.NotZero(t, item.Cas, "Expected non-zero CAS for key %s", item.ID)
		}
		assert.NoError(t, iter.Err())
		sort.Strings(ids)
		require.Equal(t, allDocIDs, ids)
	})

	t.Run("PartialRange", func(t *testing.T) {
		scan := sgbucket.RangeScan{
			From: &sgbucket.ScanTerm{Term: "doc_b"},
			To:   &sgbucket.ScanTerm{Term: "doc_d", Exclusive: true},
		}
		ids := collectScanIDs(t, ctx, scanStore, scan, sgbucket.ScanOptions{})
		require.Equal(t, []string{"doc_b", "doc_c"}, ids)
	})

	t.Run("IDsOnly", func(t *testing.T) {
		iter, err := scanStore.Scan(ctx, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{IDsOnly: true})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close(ctx)) }()

		var ids []string
		for item := iter.Next(ctx); item != nil; item = iter.Next(ctx) {
			ids = append(ids, item.ID)
			assert.Nil(t, item.Body, "Expected nil body for IDsOnly scan, key %s", item.ID)
		}
		assert.NoError(t, iter.Err())
		sort.Strings(ids)
		require.Equal(t, allDocIDs, ids)
	})

	t.Run("EmptyRange", func(t *testing.T) {
		ids := collectScanIDs(t, ctx, scanStore, sgbucket.NewRangeScanForPrefix("zzz_nonexistent_"), sgbucket.ScanOptions{})
		assert.Empty(t, ids)
	})

	t.Run("PrefixScan", func(t *testing.T) {
		ids := collectScanIDs(t, ctx, scanStore, sgbucket.NewRangeScanForPrefix("doc_c"), sgbucket.ScanOptions{})
		require.Equal(t, []string{"doc_c"}, ids)
	})

	t.Run("TombstonesExcluded", func(t *testing.T) {
		require.NoError(t, writeDS.Delete(ctx, "doc_b"))

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			ids := collectScanIDs(t, ctx, scanStore, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{IDsOnly: true})
			assert.NotContains(c, ids, "doc_b", "Tombstoned doc should not appear in scan")
		}, 30*time.Second, 100*time.Millisecond)

		ids := collectScanIDs(t, ctx, scanStore, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{})
		assert.NotContains(t, ids, "doc_b")
		assert.Contains(t, ids, "doc_a")
		assert.Contains(t, ids, "doc_c")
	})

	t.Run("LargeFixture", func(t *testing.T) {
		// 8192 docs is enough to spread several keys per vBucket: 8 per vb on
		// a 1024-vBucket CBS bucket, more on smaller setups (e.g. 256 per vb
		// on rosmar's 32-vBucket simulation). Verifies the full set comes back
		// across multiple vBucket scan streams. Range scan returns no global
		// ordering guarantee (gocb interleaves per-vBucket streams), so the
		// assertion is set equality, not ordered equality.
		const numDocs = 8192
		const prefix = "many_"

		expected := make([]string, numDocs)
		for i := 0; i < numDocs; i++ {
			key := fmt.Sprintf("%s%04d", prefix, i)
			expected[i] = key
			require.NoError(t, writeDS.SetRaw(ctx, key, 0, nil, []byte(`{}`)))
		}

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			ids := collectScanIDs(t, ctx, scanStore, sgbucket.NewRangeScanForPrefix(prefix), sgbucket.ScanOptions{IDsOnly: true})
			assert.Equal(c, expected, ids)
		}, 60*time.Second, 500*time.Millisecond)
	})
}

// TestRangeScan exercises KV range scan via the sgbucket abstraction against the
// admin-credentialed test bucket, then (when RBAC is supported) repeats the same
// subtests through a second bucket connection authenticated as a non-admin RBAC
// user. The RBAC path verifies that range scan operations succeed for users with
// only the mobile_sync_gateway / bucket_full_access roles.
func TestRangeScan(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	adminDataStore := bucket.GetSingleDataStore()
	adminScanStore, ok := AsRangeScanStore(adminDataStore)
	require.True(t, ok, "DataStore does not support range scan")

	t.Run("Admin", func(t *testing.T) {
		runRangeScanSubtests(t, ctx, adminDataStore, adminScanStore)
	})

	t.Run("RBAC", func(t *testing.T) {
		TestsRequireMobileRBAC(t)

		gocbBucket, err := AsGocbV2Bucket(bucket.Bucket)
		require.NoError(t, err)

		mgmtEp, err := GoCBBucketMgmtEndpoint(gocbBucket)
		require.NoError(t, err)

		httpClient := gocbBucket.HttpClient(ctx)
		require.NotNil(t, httpClient)

		// Sync Gateway requires bucket_full_access (KV data) and bucket_admin
		// (management). EE additionally exposes mobile_sync_gateway.
		const rbacUsername = "sg_rangescan_test"
		const rbacPassword = "password"
		bucketName := bucket.GetName()

		roles := []string{
			fmt.Sprintf("bucket_full_access[%s]", bucketName),
			fmt.Sprintf("bucket_admin[%s]", bucketName),
			fmt.Sprintf("mobile_sync_gateway[%s]", bucketName),
		}

		MakeUser(t, httpClient, mgmtEp, rbacUsername, rbacPassword, roles)
		defer DeleteUser(t, httpClient, mgmtEp, rbacUsername)

		rbacSpec := getTestBucketSpec(TestClusterSpec(t), tbpBucketName(bucketName))
		rbacSpec.Auth = TestAuthenticator{
			Username:   rbacUsername,
			Password:   rbacPassword,
			BucketName: bucketName,
		}

		// RBAC user creation propagates to KV asynchronously, so retry the bucket
		// open until the new user is accepted by memcached.
		var rbacBucket *GocbV2Bucket
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			var connectErr error
			rbacBucket, connectErr = GetGoCBv2Bucket(ctx, rbacSpec)
			assert.NoError(c, connectErr, "Failed to open bucket with RBAC user %q (roles %v)", rbacUsername, roles)
		}, 10*time.Second, 500*time.Millisecond)
		defer rbacBucket.Close(ctx)

		rbacDataStore, err := rbacBucket.GetMatchingDataStore(ctx, adminDataStore)
		require.NoError(t, err, "Failed to open matching data store on RBAC bucket")
		rbacScanStore, ok := AsRangeScanStore(rbacDataStore)
		require.True(t, ok, "RBAC DataStore does not support range scan")

		runRangeScanSubtests(t, ctx, adminDataStore, rbacScanStore)
	})
}

// Tests that a prefix-based range scan actually captures all valid doc IDs in the range.
// Reproduces GOCBC-1821 when using gocb.MaximumTerm() or gocb.NewRangeScanForPrefix()
func TestRangeScanPrefixBoundaries(t *testing.T) {
	ctx := TestCtx(t)

	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	dataStore := bucket.GetSingleDataStore()
	scanStore, ok := AsRangeScanStore(dataStore)
	require.True(t, ok, "DataStore does not support range scan")

	const prefix = "_sync"
	type test struct {
		key      string
		included bool
		why      string
	}
	cases := []test{
		// boundaries
		{"_synb", false, "different prefix, sorts before the requested prefix"},
		{prefix, true, "the prefix itself sorts before any extension"},
		{"_synd", false, "different prefix, sorts after the bound"},

		// within range
		{prefix + "\xf3", true, "0xF3 < 0xF4 at byte after prefix"},
		{prefix + "\xf4", true, "shorter than bound and is a byte-prefix of it"},
		{prefix + "\xf4\x37", true, "byte 2: 0x37 < 0x38"},
		{prefix + "\xf48fbfb", true, "byte-prefix of bound (6 bytes vs 7), shorter so less"},
		{prefix + "\xf48fbfbe", true, "byte 7: 0x65 < 0x66 (final byte just below bound)"},
		{prefix + "\xf48fbfbf", true, "exactly equals the inclusive upper bound"},
		{prefix + "\xf48fbfbf\x00", true, "longer than bound and bytewise > it (length-extension)"},
		{prefix + "\xf48fbfbg", true, "byte 7: 0x67 > 0x66 (final byte just above bound)"},
		{prefix + "\xf48fbfc0", true, "byte 6: 0x63 > 0x62"},
		{prefix + "\xf4\x39", true, "byte 2: 0x39 > 0x38"},

		// out of range with prefix
		{prefix + "\xf5", false, "0xF5 is start of invalid UTF-8"},

		// Real-world UTF-8 cases
		{prefix + "é", true, "U+00E9 (Latin-1, 0xC3 0xA9): leading byte 0xC3 < 0xF4"},
		{prefix + "中", true, "U+4E2D 中 (CJK BMP, 0xE4 0xB8 0xAD): leading byte 0xE4 < 0xF4"},
		{prefix + "\U0001F600", true, "U+1F600 😀 (emoji, 0xF0 0x9F 0x98 0x80): leading byte 0xF0 < 0xF4"},
		{prefix + "\U000FFFFF", true, "U+FFFFF (PUA-A end, 0xF3 0xBF 0xBF 0xBF): leading byte 0xF3 < 0xF4"},
		//
		// Where the gocb.MaximumTerm() value breaks:
		// U+100000–U+10FFFF (Supplementary Private Use Area-B)
		//   leading byte 0xF4 followed by continuation bytes 0x80–0x8F, all > 0x38 (8 ASCII)
		{prefix + "\U00100000", true, "U+100000 (PUA-B start, 0xF4 0x80 0x80 0x80): byte after 0xF4 is 0x80 > 0x38"},
		{prefix + "\U0010FFFF", true, "U+10FFFF (highest valid Unicode, 0xF4 0x8F 0xBF 0xBF): byte after 0xF4 is 0x8F > 0x38"},
	}

	var expected []string
	for _, c := range cases {
		require.NoError(t, dataStore.SetRaw(ctx, c.key, 0, nil, []byte(`{}`)), "insert %q", c.key)
		if c.included {
			expected = append(expected, c.key)
		}
	}
	sort.Strings(expected)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ids := collectScanIDs(t, ctx, scanStore, sgbucket.NewRangeScanForPrefix(prefix), sgbucket.ScanOptions{IDsOnly: true})
		assert.Equal(c, expected, ids)
	}, 30*time.Second, 100*time.Millisecond)
}
