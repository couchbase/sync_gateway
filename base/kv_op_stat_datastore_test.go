// Copyright 2026-Present Couchbase, Inc.
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
	"strconv"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestKVOpStatDataStoreUpdate checks that each attempt of an Update is counted as a read, plus a write when the
// callback asks for one, so CAS retries are counted.
func TestKVOpStatDataStoreUpdate(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	underlying := bucket.GetSingleDataStore()

	t.Run("insert", func(t *testing.T) {
		stat := &SgwIntStat{}
		datastore := NewKVOpStatDataStore(underlying, stat)
		_, err := datastore.Update(ctx, t.Name(), 0, func([]byte) ([]byte, *uint32, bool, error) {
			return []byte(`{"v":1}`), nil, false, nil
		})
		require.NoError(t, err)
		assert.Equal(t, int64(2), stat.Value(), "read and insert")
	})

	t.Run("cancel", func(t *testing.T) {
		stat := &SgwIntStat{}
		datastore := NewKVOpStatDataStore(underlying, stat)
		_, err := datastore.Update(ctx, t.Name(), 0, func([]byte) ([]byte, *uint32, bool, error) {
			return nil, nil, false, ErrUpdateCancel
		})
		require.ErrorIs(t, err, ErrUpdateCancel)
		assert.Equal(t, int64(1), stat.Value(), "read only")
	})

	t.Run("CAS retry", func(t *testing.T) {
		key := t.Name()
		require.NoError(t, underlying.Set(ctx, key, 0, nil, map[string]int{"v": 1}))
		stat := &SgwIntStat{}
		datastore := NewKVOpStatDataStore(underlying, stat)
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
		assert.Equal(t, int64(4), stat.Value(), "a read and a write for each attempt")
	})
}

// TestKVOpStatDataStoreUpdateFailedRetryRead checks that the read of a CAS retry is counted when it fails, which
// returns before the callback runs. Couchbase Server reads the doc as JSON, so a doc rewritten as binary makes the retry
// read fail after it reaches the server. Rosmar does not care about the doc's type, so the retry read succeeds there.
func TestKVOpStatDataStoreUpdateFailedRetryRead(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("needs Couchbase Server to make the retry read fail")
	}
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	underlying := bucket.GetSingleDataStore()

	key := t.Name()
	require.NoError(t, underlying.Set(ctx, key, 0, nil, map[string]int{"v": 1}))
	stat := &SgwIntStat{}
	datastore := NewKVOpStatDataStore(underlying, stat)
	attempts := 0
	_, err := datastore.Update(ctx, key, 0, func([]byte) ([]byte, *uint32, bool, error) {
		attempts++
		// A concurrent writer changes the CAS, so the write fails and the retry reads a binary doc.
		require.NoError(t, underlying.SetRaw(ctx, key, 0, nil, []byte("binary")))
		return []byte(`{"v":2}`), nil, false, nil
	})
	require.Error(t, err)
	require.Equal(t, 1, attempts)
	assert.Equal(t, int64(3), stat.Value(), "read, failed write, and failed retry read")
}

// TestKVOpStatDataStoreWriteUpdateWithXattrs checks that each attempt of a WriteUpdateWithXattrs is counted as a read
// and a write, except a first attempt that uses the doc the caller passed. On Couchbase Server the write that fails on
// the CAS is not counted, since kv_ops leaves it out.
func TestKVOpStatDataStoreWriteUpdateWithXattrs(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	underlying := bucket.GetSingleDataStore()
	xattrKeys := []string{SyncXattrName}

	for _, withPrevious := range []bool{false, true} {
		t.Run("CAS retry, previous="+strconv.FormatBool(withPrevious), func(t *testing.T) {
			key := t.Name()
			cas, err := underlying.WriteWithXattrs(ctx, key, 0, 0, []byte(`{"v":1}`), map[string][]byte{SyncXattrName: []byte(`{"x":1}`)}, nil, nil)
			require.NoError(t, err)
			var previous *sgbucket.BucketDocument
			if withPrevious {
				previous = &sgbucket.BucketDocument{Body: []byte(`{"v":1}`), Xattrs: map[string][]byte{SyncXattrName: []byte(`{"x":1}`)}, Cas: cas}
			}
			stat := &SgwIntStat{}
			datastore := NewKVOpStatDataStore(underlying, stat)
			attempts := 0
			_, err = datastore.WriteUpdateWithXattrs(ctx, key, xattrKeys, 0, previous, &sgbucket.MutateInOptions{}, func([]byte, map[string][]byte, uint64) (sgbucket.UpdatedDoc, error) {
				attempts++
				if attempts == 1 {
					// A concurrent writer changes the CAS, so the first write fails and the update is retried.
					_, err := underlying.SetXattrs(ctx, key, map[string][]byte{SyncXattrName: []byte(`{"x":2}`)})
					require.NoError(t, err)
				}
				return sgbucket.UpdatedDoc{Doc: []byte(`{"v":3}`), Xattrs: map[string][]byte{SyncXattrName: []byte(`{"x":3}`)}}, nil
			})
			require.NoError(t, err)
			require.Equal(t, 2, attempts)
			expectedKVOps := 4 // a read and a write for each attempt
			if withPrevious {
				expectedKVOps-- // the first attempt uses previous instead of reading
			}
			if !UnitTestUrlIsWalrus() {
				expectedKVOps-- // the failed write
			}
			assert.Equal(t, int64(expectedKVOps), stat.Value())
		})
	}
}

// TestKVOpStatDataStoreDeleteWithXattrs checks that every KV operation of a DeleteWithXattrs is counted. On Couchbase
// Server a doc with no body or no xattrs takes more than one, which rosmar does in one.
func TestKVOpStatDataStoreDeleteWithXattrs(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	underlying := bucket.GetSingleDataStore()
	xattrKeys := []string{SyncXattrName}
	xattrs := map[string][]byte{SyncXattrName: []byte(`{"x":1}`)}

	testCases := []struct {
		name string
		// setup writes the doc to delete
		setup func(t *testing.T, key string)
		// expectedServerKVOps is the count on Couchbase Server
		expectedServerKVOps int
	}{
		{
			name: "body and xattrs",
			setup: func(t *testing.T, key string) {
				_, err := underlying.WriteWithXattrs(ctx, key, 0, 0, []byte(`{"v":1}`), xattrs, nil, nil)
				require.NoError(t, err)
			},
			expectedServerKVOps: 1,
		},
		{
			// The body and xattrs delete fails on the missing body, which kv_ops leaves out, so the CAS is read and the
			// xattrs are deleted on their own.
			name: "tombstone",
			setup: func(t *testing.T, key string) {
				cas, err := underlying.WriteWithXattrs(ctx, key, 0, 0, []byte(`{"v":1}`), xattrs, nil, nil)
				require.NoError(t, err)
				_, err = underlying.WriteTombstoneWithXattrs(ctx, key, 0, cas, xattrs, nil, true, nil)
				require.NoError(t, err)
			},
			expectedServerKVOps: 2,
		},
		{
			// The body and xattrs delete fails on the missing xattrs, which kv_ops counts, so the body is deleted on its
			// own.
			name: "body only",
			setup: func(t *testing.T, key string) {
				require.NoError(t, underlying.Set(ctx, key, 0, nil, map[string]int{"v": 1}))
			},
			expectedServerKVOps: 2,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			key := t.Name()
			testCase.setup(t, key)
			stat := &SgwIntStat{}
			datastore := NewKVOpStatDataStore(underlying, stat)
			require.NoError(t, datastore.DeleteWithXattrs(ctx, key, xattrKeys))
			expectedKVOps := testCase.expectedServerKVOps
			if UnitTestUrlIsWalrus() {
				expectedKVOps = 1
			}
			assert.Equal(t, int64(expectedKVOps), stat.Value())
		})
	}
}

// TestKVOpStatDataStoreFailedCalls checks that a call that fails is counted as Couchbase Server counts it in kv_ops. A
// subdoc command that fails on the whole document is left out, but one that fails on a path is not, and a remove that
// fails on the CAS is left out, but one of a missing document is not. Rosmar counts each call.
func TestKVOpStatDataStoreFailedCalls(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	underlying := bucket.GetSingleDataStore()
	xattrKeys := []string{SyncXattrName}
	xattrs := map[string][]byte{SyncXattrName: []byte(`{"x":1}`)}
	const staleCas = 1

	writeDoc := func(t *testing.T, key string) {
		_, err := underlying.WriteWithXattrs(ctx, key, 0, 0, []byte(`{"v":1}`), xattrs, nil, nil)
		require.NoError(t, err)
	}
	writeBody := func(t *testing.T, key string) {
		require.NoError(t, underlying.Set(ctx, key, 0, nil, map[string]int{"v": 1}))
	}
	noDoc := func(*testing.T, string) {}

	testCases := []struct {
		name string
		// setup writes the doc the call fails on
		setup func(t *testing.T, key string)
		call  func(ds DataStore, key string) error
		// expectedServerKVOps is the count on Couchbase Server
		expectedServerKVOps int
	}{
		{
			name:  "GetXattrs of a missing doc",
			setup: noDoc,
			call: func(ds DataStore, key string) error {
				_, _, err := ds.GetXattrs(ctx, key, xattrKeys)
				return err
			},
			expectedServerKVOps: 0,
		},
		{
			name:  "GetXattrs of a missing xattr",
			setup: writeBody,
			call: func(ds DataStore, key string) error {
				_, _, err := ds.GetXattrs(ctx, key, xattrKeys)
				return err
			},
			expectedServerKVOps: 1,
		},
		{
			// Returns the same error as a missing doc, but the lookup reaches the tombstone.
			name: "GetWithXattrs of a tombstone with no xattrs",
			setup: func(t *testing.T, key string) {
				writeBody(t, key)
				require.NoError(t, underlying.Delete(ctx, key))
			},
			call: func(ds DataStore, key string) error {
				_, _, _, err := ds.GetWithXattrs(ctx, key, xattrKeys)
				return err
			},
			expectedServerKVOps: 1,
		},
		{
			name:  "GetSubDocRaw of a missing doc",
			setup: noDoc,
			call: func(ds DataStore, key string) error {
				_, _, err := ds.GetSubDocRaw(ctx, key, "v")
				return err
			},
			expectedServerKVOps: 0,
		},
		{
			name:  "WriteWithXattrs with a stale CAS",
			setup: writeDoc,
			call: func(ds DataStore, key string) error {
				_, err := ds.WriteWithXattrs(ctx, key, 0, staleCas, []byte(`{"v":2}`), xattrs, nil, nil)
				return err
			},
			expectedServerKVOps: 0,
		},
		{
			name:  "WriteResurrectionWithXattrs of a live doc",
			setup: writeDoc,
			call: func(ds DataStore, key string) error {
				_, err := ds.WriteResurrectionWithXattrs(ctx, key, 0, []byte(`{"v":2}`), xattrs, nil)
				return err
			},
			expectedServerKVOps: 0,
		},
		{
			name:  "UpdateXattrs of a missing doc",
			setup: noDoc,
			call: func(ds DataStore, key string) error {
				_, err := ds.UpdateXattrs(ctx, key, 0, staleCas, xattrs, nil)
				return err
			},
			expectedServerKVOps: 0,
		},
		{
			name:                "SubdocInsert of an existing path",
			setup:               writeBody,
			call:                func(ds DataStore, key string) error { return ds.SubdocInsert(ctx, key, "v", 0, 2) },
			expectedServerKVOps: 1,
		},
		{
			name:                "DeleteSubDocPaths of a missing path",
			setup:               writeBody,
			call:                func(ds DataStore, key string) error { return ds.DeleteSubDocPaths(ctx, key, "missing") },
			expectedServerKVOps: 1,
		},
		{
			name:                "DeleteWithXattrs of a missing doc",
			setup:               noDoc,
			call:                func(ds DataStore, key string) error { return ds.DeleteWithXattrs(ctx, key, xattrKeys) },
			expectedServerKVOps: 0,
		},
		{
			name:  "Remove with a stale CAS",
			setup: writeBody,
			call: func(ds DataStore, key string) error {
				_, err := ds.Remove(ctx, key, staleCas)
				return err
			},
			expectedServerKVOps: 0,
		},
		{
			name:  "Remove of a missing doc",
			setup: noDoc,
			call: func(ds DataStore, key string) error {
				_, err := ds.Remove(ctx, key, 0)
				return err
			},
			expectedServerKVOps: 1,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			key := t.Name()
			testCase.setup(t, key)
			stat := &SgwIntStat{}
			err := testCase.call(NewKVOpStatDataStore(underlying, stat), key)
			expectedKVOps := testCase.expectedServerKVOps
			if UnitTestUrlIsWalrus() {
				// Rosmar lets some of these calls succeed, such as a delete of a missing path.
				expectedKVOps = 1
			} else {
				require.Error(t, err)
			}
			assert.Equal(t, int64(expectedKVOps), stat.Value())
		})
	}
}

// kvOpReportingDataStore reports a fixed number of KV operations for each call, like Couchbase Server's Collection
// does for the calls that make several, and then fails without running the callback.
type kvOpReportingDataStore struct {
	DataStore
	reported int
}

var errKVOpReportingDataStore = errors.New("kvOpReportingDataStore")

func (ds *kvOpReportingDataStore) Update(ctx context.Context, _ string, _ uint32, _ sgbucket.UpdateFunc) (uint64, error) {
	addKVOps(ctx, ds.reported)
	return 0, errKVOpReportingDataStore
}

func (ds *kvOpReportingDataStore) WriteUpdateWithXattrs(ctx context.Context, _ string, _ []string, _ uint32, _ *sgbucket.BucketDocument, _ *sgbucket.MutateInOptions, _ sgbucket.WriteUpdateWithXattrsFunc) (uint64, error) {
	addKVOps(ctx, ds.reported)
	return 0, errKVOpReportingDataStore
}

func (ds *kvOpReportingDataStore) DeleteWithXattrs(ctx context.Context, _ string, _ []string) error {
	addKVOps(ctx, ds.reported)
	return errKVOpReportingDataStore
}

// TestKVOpStatDataStoreReportedKVOps checks that the KV operations the underlying DataStore reports are counted instead
// of the estimate, which would count one read for a call whose callback never ran.
func TestKVOpStatDataStoreReportedKVOps(t *testing.T) {
	ctx := TestCtx(t)
	testCases := []struct {
		name string
		op   func(DataStore) error
	}{
		{
			name: "Update",
			op: func(ds DataStore) error {
				_, err := ds.Update(ctx, "doc", 0, nil)
				return err
			},
		},
		{
			name: "WriteUpdateWithXattrs",
			op: func(ds DataStore) error {
				_, err := ds.WriteUpdateWithXattrs(ctx, "doc", nil, 0, nil, nil, nil)
				return err
			},
		},
		{
			name: "DeleteWithXattrs",
			op:   func(ds DataStore) error { return ds.DeleteWithXattrs(ctx, "doc", nil) },
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			stat := &SgwIntStat{}
			datastore := NewKVOpStatDataStore(&kvOpReportingDataStore{reported: 3}, stat)
			require.ErrorIs(t, testCase.op(datastore), errKVOpReportingDataStore)
			assert.Equal(t, int64(3), stat.Value())
		})
	}
}

// TestKVOpStatDataStoreMetadataStore checks that the reads a MetadataStore makes from its fallback, and the extra
// work of an Update on a doc that is only in the fallback, are counted.
func TestKVOpStatDataStoreMetadataStore(t *testing.T) {
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
			stat := &SgwIntStat{}
			datastore := NewKVOpStatDataStore(metadataStore, stat)
			require.NoError(t, testCase.op(datastore))
			assert.Equal(t, int64(testCase.expectedKVOps), stat.Value())
		})
	}

	t.Run("shares fallback state", func(t *testing.T) {
		datastore := NewKVOpStatDataStore(metadataStore, &SgwIntStat{})
		datastore.(*MetadataStore).DisableFallbackReads()
		require.False(t, metadataStore.FallbackReadsEnabled(), "disabling fallback reads through the wrapper must disable them on the MetadataStore")
	})
}
