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

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestSGFeedSourceParamsEqual(t *testing.T) {
	base := SGFeedSourceParams{
		DbName: "db",
		DCPFeedParams: cbgt.DCPFeedParams{
			AutoReconnectAfterRollback: true,
			IncludeXAttrs:              true,
			Scope:                      "_default",
			Collections:                []string{"_default"},
		},
	}

	withDbName := func(p SGFeedSourceParams, dbName string) SGFeedSourceParams {
		p.DbName = dbName
		return p
	}
	withCollections := func(p SGFeedSourceParams, collections []string) SGFeedSourceParams {
		p.Collections = collections
		return p
	}
	withIncludeXAttrs := func(p SGFeedSourceParams, includeXAttrs bool) SGFeedSourceParams {
		p.IncludeXAttrs = includeXAttrs
		return p
	}
	withStopAfterSeq := func(p SGFeedSourceParams, seq uint64) SGFeedSourceParams {
		p.StopAfterSourceParams = cbgt.StopAfterSourceParams{
			StopAfter:         "markReached",
			MarkPartitionSeqs: map[string]cbgt.UUIDSeq{"0": {Seq: seq}},
		}
		return p
	}

	testCases := []struct {
		name        string
		a           SGFeedSourceParams
		b           SGFeedSourceParams
		expectEqual bool
	}{
		{
			name:        "identical",
			a:           base,
			b:           base,
			expectEqual: true,
		},
		{
			name:        "different DbName",
			a:           base,
			b:           withDbName(base, "otherdb"),
			expectEqual: false,
		},
		{
			name:        "different Collections content",
			a:           base,
			b:           withCollections(base, []string{"collection1"}),
			expectEqual: false,
		},
		{
			name:        "different Collections order",
			a:           withCollections(base, []string{"a", "b"}),
			b:           withCollections(base, []string{"b", "a"}),
			expectEqual: true, // order-insensitive: cbgtFeedParams input order isn't stable
		},
		{
			name:        "different Collections content, same length",
			a:           withCollections(base, []string{"a", "b"}),
			b:           withCollections(base, []string{"a", "c"}),
			expectEqual: false,
		},
		{
			name:        "different IncludeXAttrs",
			a:           base,
			b:           withIncludeXAttrs(base, false),
			expectEqual: false,
		},
		{
			name:        "different StopAfterSourceParams",
			a:           withStopAfterSeq(base, 100),
			b:           withStopAfterSeq(base, 200),
			expectEqual: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expectEqual, testCase.a.Equal(testCase.b))
			assert.Equal(t, testCase.expectEqual, testCase.b.Equal(testCase.a), "Equal should be symmetric")
		})
	}
}

// TestSGFeedSourceParamsEqualString covers the string-based wrapper: unmarshal errors return false,
// and differently-ordered JSON encodings of the same value still compare equal.
func TestSGFeedSourceParamsEqualString(t *testing.T) {
	const orderA = `{"includeXAttrs":true,"scope":"_default","collections":["_default"],"autoReconnectAfterRollback":true,"stopAfter":"","markPartitionSeqs":null,"sg_dbname":"db"}`
	const orderB = `{"autoReconnectAfterRollback":true,"collections":["_default"],"includeXAttrs":true,"markPartitionSeqs":null,"scope":"_default","sg_dbname":"db","stopAfter":""}`
	const differentDb = `{"sg_dbname":"otherdb"}`
	const invalidJSON = `{`
	const unmodeledFieldA = `{"sg_dbname":"db","futureField":"valueA"}`
	const unmodeledFieldB = `{"sg_dbname":"db","futureField":"valueB"}`

	testCases := []struct {
		name        string
		a           string
		b           string
		expectEqual bool
	}{
		{name: "equal despite different key order", a: orderA, b: orderB, expectEqual: true},
		{name: "different value", a: orderA, b: differentDb, expectEqual: false},
		{name: "invalid JSON in a", a: invalidJSON, b: orderB, expectEqual: false},
		{name: "invalid JSON in b", a: orderA, b: invalidJSON, expectEqual: false},
		// Current behavior: a field not modeled by SGFeedSourceParams (e.g. from a newer SG
		// node mid rolling-upgrade) is dropped on unmarshal, so a real difference there goes undetected.
		{name: "different value in unmodeled field is not detected", a: unmodeledFieldA, b: unmodeledFieldB, expectEqual: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expectEqual, SGFeedSourceParamsEqual(testCase.a, testCase.b))
		})
	}
}

func TestSGFeedIndexParamsEqual(t *testing.T) {
	testCases := []struct {
		name        string
		a           SGFeedIndexParams
		b           SGFeedIndexParams
		expectEqual bool
	}{
		{
			name:        "identical",
			a:           SGFeedIndexParams{DestKey: "db_import"},
			b:           SGFeedIndexParams{DestKey: "db_import"},
			expectEqual: true,
		},
		{
			name:        "different DestKey",
			a:           SGFeedIndexParams{DestKey: "db_import"},
			b:           SGFeedIndexParams{DestKey: "otherdb_import"},
			expectEqual: false,
		},
		{
			name:        "empty vs non-empty DestKey",
			a:           SGFeedIndexParams{},
			b:           SGFeedIndexParams{DestKey: "db_import"},
			expectEqual: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expectEqual, testCase.a.Equal(testCase.b))
			assert.Equal(t, testCase.expectEqual, testCase.b.Equal(testCase.a), "Equal should be symmetric")
		})
	}
}

// TestSGFeedIndexParamsEqualString covers the string-based wrapper, including unmarshal errors.
func TestSGFeedIndexParamsEqualString(t *testing.T) {
	testCases := []struct {
		name        string
		a           string
		b           string
		expectEqual bool
	}{
		{name: "equal", a: `{"destKey":"db_import"}`, b: `{"destKey":"db_import"}`, expectEqual: true},
		{name: "different value", a: `{"destKey":"db_import"}`, b: `{"destKey":"otherdb_import"}`, expectEqual: false},
		{name: "invalid JSON in a", a: `{`, b: `{"destKey":"db_import"}`, expectEqual: false},
		{name: "invalid JSON in b", a: `{"destKey":"db_import"}`, b: `{`, expectEqual: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expectEqual, SGFeedIndexParamsEqual(testCase.a, testCase.b))
		})
	}
}

// TestCbgtFeedParamsCollectionOrderInsensitiveEqual verifies cbgtFeedParams output compares as Equal
// regardless of input collection order, since that order isn't guaranteed stable (built from a map).
func TestCbgtFeedParamsCollectionOrderInsensitiveEqual(t *testing.T) {
	ctx := TestCtx(t)

	baselineJSON, err := cbgtFeedParams(ctx, ShardedDCPOptions{
		DBName:      "db",
		Collections: CollectionNames{"_default": {"collectionA", "collectionB", "collectionC"}},
	})
	require.NoError(t, err)
	var baseline SGFeedSourceParams
	require.NoError(t, JSONUnmarshal([]byte(baselineJSON), &baseline))

	testCases := []struct {
		name        string
		collections []string
	}{
		{name: "forward order", collections: []string{"collectionA", "collectionB", "collectionC"}},
		{name: "reversed order", collections: []string{"collectionC", "collectionB", "collectionA"}},
		{name: "arbitrary order", collections: []string{"collectionB", "collectionC", "collectionA"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			paramsJSON, err := cbgtFeedParams(ctx, ShardedDCPOptions{
				DBName:      "db",
				Collections: CollectionNames{"_default": testCase.collections},
			})
			require.NoError(t, err)

			var params SGFeedSourceParams
			require.NoError(t, JSONUnmarshal([]byte(paramsJSON), &params))
			assert.True(t, baseline.Equal(params))
		})
	}
}
