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
			require.Equal(t, test.indexName, GenerateIndexName(test.dbName))
		})
	}
}

func TestImportDestKey(t *testing.T) {
	tests := []struct {
		name        string
		dbName      string
		scopeName   string
		collections []string
		key         string
	}{
		{
			name:   "no scope or collections",
			dbName: "foo",
			key:    "foo_import",
		},
		{
			name:      "scope but only not collection",
			dbName:    "foo",
			scopeName: DefaultScope,
			key:       "foo_import",
		},
		{
			name:        "custom collection, default scope",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{"bar"},
			key:         "foo_import_02e3c10f452b5d9d5051ae25270ae5714471774097ca7e00424b52bf63de1f6d",
		},
		{
			name:        "custom collection, custom scope",
			dbName:      "foo",
			scopeName:   "baz",
			collections: []string{"bar"},
			key:         "foo_import_3a4b66f3c8aa40608000c82c417f201de305a1994f3048b7734a33205be5e410",
		},
		{
			name:        "custom collections, custom scope",
			dbName:      "foo",
			scopeName:   "bar",
			collections: []string{"baz", "bat"},
			key:         "foo_import_cc2777dc506c83ef70c0630be2f21cbe9380d83d2d50c8aeb428e67691503cfb",
		},
		{
			name:        "custom collection, multiple scopes",
			dbName:      "foo",
			scopeName:   "bar",
			collections: []string{"baz", "bat"},
			key:         "foo_import_cc2777dc506c83ef70c0630be2f21cbe9380d83d2d50c8aeb428e67691503cfb",
		},
		{
			name:        "default collection, multiple scopes",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{"baz", "bat"},
			key:         "foo_import_98ea225323328e1d6ae54575908419f85dcad91b2ee3acb56b3a6491145d87cf",
		},

		{
			name:        "scope but only not collection",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{DefaultCollection},
			key:         "foo_import",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.key, ImportDestKey(test.dbName, test.scopeName, test.collections))
		})
	}
}
