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
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectScanIDs runs a scan and returns the sorted list of document IDs.
func collectScanIDs(t testing.TB, rss sgbucket.RangeScanStore, scanType sgbucket.ScanType, opts sgbucket.ScanOptions) []string {
	t.Helper()
	iter, err := rss.Scan(scanType, opts)
	require.NoError(t, err)
	defer func() { assert.NoError(t, iter.Close()) }()

	var ids []string
	for {
		item := iter.Next()
		if item == nil {
			break
		}
		fmt.Printf("range scan item: %v\n", item)
		ids = append(ids, item.ID)
	}
	sort.Strings(ids)
	return ids
}

func TestRangeScan(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	dataStore := bucket.GetSingleDataStore()

	rss, ok := AsRangeScanStore(dataStore)
	require.True(t, ok, "DataStore does not support range scan")

	// Seed test documents
	docs := map[string][]byte{
		"doc_a": []byte(`{"name":"alpha"}`),
		"doc_b": []byte(`{"name":"bravo"}`),
		"doc_c": []byte(`{"name":"charlie"}`),
		"doc_d": []byte(`{"name":"delta"}`),
		"doc_e": []byte(`{"name":"echo"}`),
	}
	for k, v := range docs {
		require.NoError(t, dataStore.SetRaw(k, 0, nil, v))
	}

	allDocIDs := []string{"doc_a", "doc_b", "doc_c", "doc_d", "doc_e"}

	// CBS range scan may not immediately reflect recent writes (requires persistence).
	// Wait for all docs to be visible before running subtests.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ids := collectScanIDs(t, rss, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{IDsOnly: true})
		assert.Equal(c, allDocIDs, ids)
	}, 30*time.Second, 100*time.Millisecond)

	t.Run("FullRange", func(t *testing.T) {
		scan := sgbucket.NewRangeScanForPrefix("doc_")
		iter, err := rss.Scan(scan, sgbucket.ScanOptions{})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		var ids []string
		for {
			item := iter.Next()
			if item == nil {
				break
			}
			ids = append(ids, item.ID)
			assert.NotNil(t, item.Body, "Expected body for key %s", item.ID)
			assert.NotZero(t, item.Cas, "Expected non-zero CAS for key %s", item.ID)
			assert.False(t, item.IDOnly)
		}
		sort.Strings(ids)
		require.Equal(t, allDocIDs, ids)
	})

	t.Run("PartialRange", func(t *testing.T) {
		scan := sgbucket.RangeScan{
			From: &sgbucket.ScanTerm{Term: "doc_b"},
			To:   &sgbucket.ScanTerm{Term: "doc_d", Exclusive: true},
		}
		ids := collectScanIDs(t, rss, scan, sgbucket.ScanOptions{})
		require.Equal(t, []string{"doc_b", "doc_c"}, ids)
	})

	t.Run("IDsOnly", func(t *testing.T) {
		scan := sgbucket.NewRangeScanForPrefix("doc_")
		iter, err := rss.Scan(scan, sgbucket.ScanOptions{IDsOnly: true})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		var ids []string
		for {
			item := iter.Next()
			if item == nil {
				break
			}
			ids = append(ids, item.ID)
			assert.True(t, item.IDOnly)
			assert.Nil(t, item.Body, "Expected nil body for IDsOnly scan, key %s", item.ID)
		}
		sort.Strings(ids)
		require.Equal(t, allDocIDs, ids)
	})

	t.Run("EmptyRange", func(t *testing.T) {
		ids := collectScanIDs(t, rss, sgbucket.NewRangeScanForPrefix("zzz_nonexistent_"), sgbucket.ScanOptions{})
		assert.Empty(t, ids)
	})

	t.Run("PrefixScan", func(t *testing.T) {
		ids := collectScanIDs(t, rss, sgbucket.NewRangeScanForPrefix("doc_c"), sgbucket.ScanOptions{})
		require.Equal(t, []string{"doc_c"}, ids)
	})

	t.Run("TombstonesExcluded", func(t *testing.T) {
		require.NoError(t, dataStore.Delete("doc_b"))

		// Wait for the tombstone to be reflected in scan results.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			ids := collectScanIDs(t, rss, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{IDsOnly: true})
			assert.NotContains(c, ids, "doc_b", "Tombstoned doc should not appear in scan")
		}, 30*time.Second, 100*time.Millisecond)

		ids := collectScanIDs(t, rss, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{})
		assert.NotContains(t, ids, "doc_b")
		assert.Contains(t, ids, "doc_a")
		assert.Contains(t, ids, "doc_c")
	})
}

// makeRBACUser creates a Couchbase Server RBAC user via the management REST API and returns
// a cleanup function that deletes the user. The caller must have admin credentials.
func makeRBACUser(t *testing.T, httpClient *http.Client, mgmtEp, username, password string, roles []string) {
	t.Helper()
	form := url.Values{}
	form.Add("password", password)
	form.Add("roles", strings.Join(roles, ","))

	respBytes, status, err := MgmtRequest(
		httpClient, mgmtEp, http.MethodPut,
		fmt.Sprintf("/settings/rbac/users/local/%s", username),
		"application/x-www-form-urlencoded",
		TestClusterUsername(), TestClusterPassword(),
		strings.NewReader(form.Encode()),
	)
	require.NoError(t, err)
	require.Equalf(t, http.StatusOK, status, "Failed to create RBAC user %q: %s", username, respBytes)
}

// deleteRBACUser removes a Couchbase Server RBAC user via the management REST API.
func deleteRBACUser(t *testing.T, httpClient *http.Client, mgmtEp, username string) {
	t.Helper()
	_, _, err := MgmtRequest(
		httpClient, mgmtEp, http.MethodDelete,
		fmt.Sprintf("/settings/rbac/users/local/%s", username),
		"", TestClusterUsername(), TestClusterPassword(), nil,
	)
	require.NoError(t, err)
}

// TestRangeScanRBACUser verifies that KV range scan operations succeed when the bucket
// connection uses an RBAC user with the mobile_sync_gateway role (EE) or
// bucket_full_access role (CE), rather than the cluster administrator.
func TestRangeScanRBACUser(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("Requires Couchbase Server")
	}

	SetUpTestLogging(t, LevelDebug, KeyAll)

	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	// --- Obtain management endpoint and HTTP client from the admin bucket ---
	gocbBucket, err := AsGocbV2Bucket(bucket.Bucket)
	require.NoError(t, err)

	mgmtEp, err := GoCBBucketMgmtEndpoint(gocbBucket)
	require.NoError(t, err)

	httpClient := gocbBucket.HttpClient(ctx)
	require.NotNil(t, httpClient)

	// --- Create RBAC user with the appropriate roles for the edition ---
	// Sync Gateway requires bucket_full_access (KV data) and bucket_admin
	// (management operations) at minimum.  On EE, mobile_sync_gateway is
	// the canonical role for Admin API authentication.
	rbacUsername := "sg_rangescan_test"
	rbacPassword := "password"
	bucketName := bucket.GetName()

	roles := []string{
		fmt.Sprintf("bucket_full_access[%s]", bucketName),
		fmt.Sprintf("bucket_admin[%s]", bucketName),
	}
	if !TestsUseServerCE() {
		roles = append(roles, fmt.Sprintf("mobile_sync_gateway[%s]", bucketName))
	}

	makeRBACUser(t, httpClient, mgmtEp, rbacUsername, rbacPassword, roles)
	defer deleteRBACUser(t, httpClient, mgmtEp, rbacUsername)

	// --- Open a second connection to the same bucket using RBAC credentials ---
	// RBAC user creation may take a moment to propagate to the KV service,
	// so retry the connection a few times before giving up.
	rbacSpec := BucketSpec{
		Server:     bucket.BucketSpec.Server,
		BucketName: bucketName,
		Auth: TestAuthenticator{
			Username:   rbacUsername,
			Password:   rbacPassword,
			BucketName: bucketName,
		},
		UseXattrs:       bucket.BucketSpec.UseXattrs,
		TLSSkipVerify:   bucket.BucketSpec.TLSSkipVerify,
		BucketOpTimeout: bucket.BucketSpec.BucketOpTimeout,
	}
	var rbacBucket *GocbV2Bucket
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var connectErr error
		rbacBucket, connectErr = GetGoCBv2Bucket(ctx, rbacSpec)
		assert.NoError(c, connectErr, "Failed to open bucket with RBAC user %q (roles %v)", rbacUsername, roles)
	}, 10*time.Second, 500*time.Millisecond)
	defer rbacBucket.Close(ctx)

	// --- Seed test documents via the admin connection ---
	// GetSingleDataStore may return a named collection (when TestsUseNamedCollections is true)
	// or the default collection. We must use the same collection on both connections.
	adminDataStore := bucket.GetSingleDataStore()

	// Open the same collection on the RBAC-connected bucket.
	var rbacDataStore sgbucket.DataStore
	if adminDataStore.ScopeName() == DefaultScope && adminDataStore.CollectionName() == DefaultCollection {
		rbacDataStore = rbacBucket.DefaultDataStore()
	} else {
		var dsErr error
		rbacDataStore, dsErr = rbacBucket.NamedDataStore(ScopeAndCollectionName{
			Scope:      adminDataStore.ScopeName(),
			Collection: adminDataStore.CollectionName(),
		})
		require.NoError(t, dsErr, "Failed to open named collection %s.%s on RBAC bucket",
			adminDataStore.ScopeName(), adminDataStore.CollectionName())
	}
	rss, ok := AsRangeScanStore(rbacDataStore)
	require.True(t, ok, "DataStore does not support range scan")
	docs := map[string][]byte{
		"doc_a": []byte(`{"name":"alpha"}`),
		"doc_b": []byte(`{"name":"bravo"}`),
		"doc_c": []byte(`{"name":"charlie"}`),
		"doc_d": []byte(`{"name":"delta"}`),
		"doc_e": []byte(`{"name":"echo"}`),
	}
	for k, v := range docs {
		require.NoError(t, adminDataStore.SetRaw(k, 0, nil, v))
	}

	allDocIDs := []string{"doc_a", "doc_b", "doc_c", "doc_d", "doc_e"}

	// Wait for all docs to be visible through the RBAC connection's range scan.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ids := collectScanIDs(t, rss, sgbucket.NewRangeScanForPrefix("doc_"), sgbucket.ScanOptions{IDsOnly: true})
		assert.Equal(c, allDocIDs, ids)
	}, 30*time.Second, 500*time.Millisecond)

	t.Run("FullRange", func(t *testing.T) {
		scan := sgbucket.NewRangeScanForPrefix("doc_")
		iter, err := rss.Scan(scan, sgbucket.ScanOptions{})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		var ids []string
		for {
			item := iter.Next()
			if item == nil {
				break
			}
			ids = append(ids, item.ID)
			assert.NotNil(t, item.Body, "Expected body for key %s", item.ID)
			assert.NotZero(t, item.Cas, "Expected non-zero CAS for key %s", item.ID)
			assert.False(t, item.IDOnly)
		}
		sort.Strings(ids)
		require.Equal(t, allDocIDs, ids)
	})

	t.Run("PartialRange", func(t *testing.T) {
		scan := sgbucket.RangeScan{
			From: &sgbucket.ScanTerm{Term: "doc_b"},
			To:   &sgbucket.ScanTerm{Term: "doc_d", Exclusive: true},
		}
		ids := collectScanIDs(t, rss, scan, sgbucket.ScanOptions{})
		require.Equal(t, []string{"doc_b", "doc_c"}, ids)
	})

	t.Run("IDsOnly", func(t *testing.T) {
		scan := sgbucket.NewRangeScanForPrefix("doc_")
		iter, err := rss.Scan(scan, sgbucket.ScanOptions{IDsOnly: true})
		require.NoError(t, err)
		defer func() { assert.NoError(t, iter.Close()) }()

		var ids []string
		for {
			item := iter.Next()
			if item == nil {
				break
			}
			ids = append(ids, item.ID)
			assert.True(t, item.IDOnly)
			assert.Nil(t, item.Body, "Expected nil body for IDsOnly scan, key %s", item.ID)
		}
		sort.Strings(ids)
		require.Equal(t, allDocIDs, ids)
	})
}
