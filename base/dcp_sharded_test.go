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
