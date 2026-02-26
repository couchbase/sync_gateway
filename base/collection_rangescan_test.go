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
	"sort"
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
