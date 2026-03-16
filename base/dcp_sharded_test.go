// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexName(t *testing.T) {
	tests := []struct {
		dbName    string
		indexName string
	}{
		{
			dbName:    "",
			indexName: "db0x0_index",
		},
		{
			dbName:    "foo",
			indexName: "db0xcfc4ae1d_index",
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("dbName %s -> indexName %s", test.indexName, test.dbName), func(t *testing.T) {
			require.Equal(t, test.indexName, GenerateIndexName(test.dbName, DCPImportFeedID))
		})
	}
}

func TestDestKey(t *testing.T) {
	tests := []struct {
		name        string
		dbName      string
		scopeName   string
		collections []string
		key         string
		feedType    ShardedDCPFeedType
	}{
		{
			name:     "import no scope or collections",
			dbName:   "foo",
			key:      "foo_import",
			feedType: ImportShardedDCPFeedType,
		},
		{
			name:     "resync no scope or collections",
			dbName:   "foo",
			key:      "foo_resync_e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			feedType: ResyncShardedDCPFeedType,
		},
		{
			name:      "import default scope without collections",
			dbName:    "foo",
			scopeName: DefaultScope,
			key:       "foo_import",
			feedType:  ImportShardedDCPFeedType,
		},
		{
			name:      "resync default scope without collections",
			dbName:    "foo",
			scopeName: DefaultScope,
			key:       "foo_resync_e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			feedType:  ResyncShardedDCPFeedType,
		},
		{
			name:        "import custom collection in default scope",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{"bar"},
			key:         "foo_import_02e3c10f452b5d9d5051ae25270ae5714471774097ca7e00424b52bf63de1f6d",
			feedType:    ImportShardedDCPFeedType,
		},
		{
			name:        "resync custom collection in default scope",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{"bar"},
			key:         "foo_resync_02e3c10f452b5d9d5051ae25270ae5714471774097ca7e00424b52bf63de1f6d",
			feedType:    ResyncShardedDCPFeedType,
		},
		{
			name:        "import custom collection in custom scope",
			dbName:      "foo",
			scopeName:   "baz",
			collections: []string{"bar"},
			key:         "foo_import_3a4b66f3c8aa40608000c82c417f201de305a1994f3048b7734a33205be5e410",
			feedType:    ImportShardedDCPFeedType,
		},
		{
			name:        "resync custom collection in custom scope",
			dbName:      "foo",
			scopeName:   "baz",
			collections: []string{"bar"},
			key:         "foo_resync_3a4b66f3c8aa40608000c82c417f201de305a1994f3048b7734a33205be5e410",
			feedType:    ResyncShardedDCPFeedType,
		},
		{
			name:        "import multiple collections in custom scope",
			dbName:      "foo",
			scopeName:   "bar",
			collections: []string{"baz", "bat"},
			key:         "foo_import_cc2777dc506c83ef70c0630be2f21cbe9380d83d2d50c8aeb428e67691503cfb",
			feedType:    ImportShardedDCPFeedType,
		},
		{
			name:        "resync multiple collections in custom scope",
			dbName:      "foo",
			scopeName:   "bar",
			collections: []string{"baz", "bat"},
			key:         "foo_resync_cc2777dc506c83ef70c0630be2f21cbe9380d83d2d50c8aeb428e67691503cfb",
			feedType:    ResyncShardedDCPFeedType,
		},
		{
			name:        "import multiple collections across scope",
			dbName:      "foo",
			scopeName:   "bar",
			collections: []string{"baz", "bat"},
			key:         "foo_import_cc2777dc506c83ef70c0630be2f21cbe9380d83d2d50c8aeb428e67691503cfb",
			feedType:    ImportShardedDCPFeedType,
		},
		{
			name:        "resync multiple collections across scope",
			dbName:      "foo",
			scopeName:   "bar",
			collections: []string{"baz", "bat"},
			key:         "foo_resync_cc2777dc506c83ef70c0630be2f21cbe9380d83d2d50c8aeb428e67691503cfb",
			feedType:    ResyncShardedDCPFeedType,
		},
		{
			name:        "import default scope with multiple collections",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{"baz", "bat"},
			key:         "foo_import_98ea225323328e1d6ae54575908419f85dcad91b2ee3acb56b3a6491145d87cf",
			feedType:    ImportShardedDCPFeedType,
		},
		{
			name:        "resync default scope with multiple collections",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{"baz", "bat"},
			key:         "foo_resync_98ea225323328e1d6ae54575908419f85dcad91b2ee3acb56b3a6491145d87cf",
			feedType:    ResyncShardedDCPFeedType,
		},
		{
			name:        "import default scope with default collection",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{DefaultCollection},
			key:         "foo_import",
			feedType:    ImportShardedDCPFeedType,
		},
		{
			name:        "resync default scope with default collection",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{DefaultCollection},
			key:         "foo_resync_03d1187922d96d534d985a0a386ecdf062d369673981c217d07610a7b8ca4a52",
			feedType:    ResyncShardedDCPFeedType,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.key, DestKey(test.dbName, test.scopeName, test.collections, test.feedType))
		})
	}
}
