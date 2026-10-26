// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"testing"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestKVOpCountingDataStoreUpdate checks that each attempt of an Update is counted as a read, plus a write when the
// callback asks for one, so CAS retries are counted.
func TestKVOpCountingDataStoreUpdate(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	underlying := bucket.GetSingleDataStore()

	t.Run("insert", func(t *testing.T) {
		datastore := NewKVOpCountingDataStore(underlying)
		_, err := datastore.Update(ctx, t.Name(), 0, func([]byte) ([]byte, *uint32, bool, error) {
			return []byte(`{"v":1}`), nil, false, nil
		})
		require.NoError(t, err)
		assert.Equal(t, 2, datastore.KVOps(), "read and insert")
	})

	t.Run("cancel", func(t *testing.T) {
		datastore := NewKVOpCountingDataStore(underlying)
		_, err := datastore.Update(ctx, t.Name(), 0, func([]byte) ([]byte, *uint32, bool, error) {
			return nil, nil, false, ErrUpdateCancel
		})
		require.ErrorIs(t, err, ErrUpdateCancel)
		assert.Equal(t, 1, datastore.KVOps(), "read only")
	})

	t.Run("CAS retry", func(t *testing.T) {
		key := t.Name()
		require.NoError(t, underlying.Set(ctx, key, 0, nil, map[string]int{"v": 1}))
		datastore := NewKVOpCountingDataStore(underlying)
		attempts := 0
		_, err := datastore.Update(ctx, key, 0, func([]byte) ([]byte, *uint32, bool, error) {
			attempts++
			if attempts == 1 {
				// A concurrent writer changes the CAS, so the first write fails and the update is retried.
				require.NoError(t, underlying.Set(ctx, key, 0, nil, map[string]int{"v": 2}))
			}
			return []byte(`{"v":3}`), nil, false, nil
		})
		require.NoError(t, err)
		require.Equal(t, 2, attempts)
		assert.Equal(t, 4, datastore.KVOps(), "a read and a write for each attempt")
	})
}

// TestKVOpCountingDataStoreMetadataStore checks that the reads a MetadataStore makes from its fallback, and the extra
// work of an Update on a doc that is only in the fallback, are counted.
func TestKVOpCountingDataStoreMetadataStore(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	primary := bucket.GetSingleDataStore()
	fallback := bucket.DefaultDataStore(ctx)
	if primary.GetName() == fallback.GetName() {
		t.Skip("needs a named collection to use as the primary")
	}
	metadataStore := NewMetadataStore(primary, fallback)

	primaryKey := t.Name() + "_primary"
	fallbackKey := t.Name() + "_fallback"
	require.NoError(t, primary.Set(ctx, primaryKey, 0, nil, map[string]int{"v": 1}))
	require.NoError(t, fallback.Set(ctx, fallbackKey, 0, nil, map[string]int{"v": 1}))

	testCases := []struct {
		name          string
		op            func(DataStore) error
		expectedKVOps int
	}{
		{
			name:          "get from primary",
			op:            func(ds DataStore) error { _, err := ds.Get(ctx, primaryKey, nil); return err },
			expectedKVOps: 1,
		},
		{
			name:          "get from fallback",
			op:            func(ds DataStore) error { _, _, err := ds.GetRaw(ctx, fallbackKey); return err },
			expectedKVOps: 2,
		},
		{
			name: "get missing doc",
			op: func(ds DataStore) error {
				_, _, err := ds.GetRaw(ctx, t.Name()+"_missing")
				if IsDocNotFoundError(err) {
					return nil
				}
				return err
			},
			expectedKVOps: 2,
		},
		{
			// Exists on the primary, read from the fallback, then insert into the primary.
			name: "update doc only in fallback",
			op: func(ds DataStore) error {
				_, err := ds.Update(ctx, fallbackKey, 0, func([]byte) ([]byte, *uint32, bool, error) {
					return []byte(`{"v":2}`), nil, false, nil
				})
				return err
			},
			expectedKVOps: 3,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			datastore := NewKVOpCountingDataStore(metadataStore)
			require.NoError(t, testCase.op(datastore))
			assert.Equal(t, testCase.expectedKVOps, datastore.KVOps())
		})
	}

	t.Run("shares fallback state", func(t *testing.T) {
		datastore := NewKVOpCountingDataStore(metadataStore)
		datastore.DataStore.(*MetadataStore).DisableFallbackReads()
		require.False(t, metadataStore.FallbackReadsEnabled(), "disabling fallback reads through the wrapper must disable them on the MetadataStore")
	})
}
