/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"
	"math/rand"
	"regexp"
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDCPNameLength(t *testing.T) {
	// https://issues.couchbase.com/browse/MB-34280
	const maxDCPNameLength = 200

	feedIDs := []string{
		DCPCachingFeedID,
		DCPImportFeedID,
	}

	for _, feedID := range feedIDs {
		t.Run(feedID, func(t *testing.T) {
			dcpStreamName, err := GenerateDcpStreamName(feedID)
			require.NoError(t, err)
			t.Logf("generated name of length %d: %s", len(dcpStreamName), dcpStreamName)

			assert.Truef(t, len(dcpStreamName) <= maxDCPNameLength,
				"len(dcpStreamName)=%d is exceeds max allowed %d chars: %s",
				len(dcpStreamName), maxDCPNameLength, dcpStreamName)
		})

		t.Run("cbgt"+feedID, func(t *testing.T) {
			dcpStreamName, err := GenerateDcpStreamName(feedID)
			require.NoError(t, err)

			// Format string copied from cbgt's 'NewDCPFeed'
			cbgtDCPName := fmt.Sprintf("%s%s-%x", cbgt.DCPFeedPrefix, dcpStreamName, rand.Int31())
			t.Logf("generated name of length %d: %s", len(cbgtDCPName), cbgtDCPName)

			assert.Truef(t, len(cbgtDCPName) <= maxDCPNameLength,
				"len(cbgtDCPName)=%d is exceeds max allowed %d chars: %s",
				len(cbgtDCPName), maxDCPNameLength, cbgtDCPName)
		})
	}

	dbNames := []string{
		"db1",
		"db1$a", // dollars aren't allowed in cbgt index names, but are valid in SG/CouchDB database names
		"123",   // indexes and db names can't start with a digit
		"db1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}

	for _, dbName := range dbNames {
		t.Run("cbgt-index-"+dbName, func(t *testing.T) {
			indexName := GenerateIndexName(dbName)

			// Verify we pass CBGT's index name validation
			matched, err := regexp.Match(cbgt.INDEX_NAME_REGEXP, []byte(indexName))
			require.NoError(t, err)
			assert.True(t, matched, "index name is not a valid cbgt index name")

			// The format of a cbgt index name is of the following format:
			// sg:db0xbdcaa3f7_index_167dfc5a122bde31_f47365c5-2e5220ce
			// where 'sg:db0x..._index' are SG's generated name, and the rest is from CBGT.
			cbgtIndexDCPName := fmt.Sprintf("%s_167dfc5a122bde31_f47365c5-2e5220ce", indexName)
			t.Logf("generated name of length %d: %s", len(cbgtIndexDCPName), cbgtIndexDCPName)

			assert.Truef(t, len(cbgtIndexDCPName) <= maxDCPNameLength,
				"len(cbgtIndexDCPName)=%d is exceeds max allowed %d chars: %s",
				len(cbgtIndexDCPName), maxDCPNameLength, cbgtIndexDCPName)
		})
	}
}

func TestGetExternalAlternateAddress(t *testing.T) {
	tests := []struct {
		name         string
		dest         string
		altAddrMap   map[string]string
		expectedDest string
	}{
		{
			name:         "no alts",
			dest:         "node1.cbs.example.org:1234",
			expectedDest: "node1.cbs.example.org:1234",
		},
		{
			name: "non-matching alt",
			dest: "node1.cbs.example.org:1234",
			altAddrMap: map[string]string{
				"node9.cbs.example.org": "10.10.10.9:5678",
			},
			expectedDest: "node1.cbs.example.org:1234",
		},
		{
			name: "matching alt, alt connect URL",
			dest: "node1.cbs.example.org:1234",
			altAddrMap: map[string]string{
				"node1.cbs.example.org": "10.10.10.1:5678",
			},
			expectedDest: "10.10.10.1:5678",
		},
		{
			name: "matching alt multiple",
			dest: "node2.cbs.example.org:1234",
			altAddrMap: map[string]string{
				"node1.cbs.example.org": "10.10.10.1:5678",
				"node2.cbs.example.org": "10.10.10.2:5678",
				"node3.cbs.example.org": "10.10.10.3:5678",
			},
			expectedDest: "10.10.10.2:5678",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			newDest, err := getExternalAlternateAddress(nil, test.altAddrMap, test.dest)
			require.NoError(t, err)
			assert.Equal(t, test.expectedDest, newDest)
		})
	}
}
