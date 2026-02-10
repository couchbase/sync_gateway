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
	sgbucket "github.com/couchbase/sg-bucket"
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
			dcpStreamName, err := generateDcpStreamName(feedID)
			require.NoError(t, err)
			t.Logf("generated name of length %d: %s", len(dcpStreamName), dcpStreamName)

			assert.Truef(t, len(dcpStreamName) <= maxDCPNameLength,
				"len(dcpStreamName)=%d is exceeds max allowed %d chars: %s",
				len(dcpStreamName), maxDCPNameLength, dcpStreamName)
		})

		t.Run("cbgt"+feedID, func(t *testing.T) {
			dcpStreamName, err := generateDcpStreamName(feedID)
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

// TestFeedEventByteSliceCopy( ensures that the byte slices in the FeedEvent are copies and not the original ones - CBG-4540
func TestFeedEventByteSliceCopy(t *testing.T) {
	const (
		keyData   = "key"
		valueData = "value"
	)
	keySlice := []byte(keyData)
	valueSlice := []byte(valueData)
	e := makeFeedEvent(keySlice, valueSlice, 0, 0, 0, 0, 0, 0, sgbucket.FeedOpMutation)
	require.Equal(t, keyData, string(e.Key))
	require.Equal(t, valueData, string(e.Value))
	require.Equal(t, keyData, string(keySlice))
	require.Equal(t, valueData, string(valueSlice))

	// mutate the originals and ensure the FeedEvent byte slices didn't change with it
	keySlice[0] = 'x'
	valueSlice[0] = 'x'
	assert.Equal(t, keyData, string(e.Key))
	assert.Equal(t, valueData, string(e.Value))
	assert.NotEqual(t, keyData, string(keySlice))
	assert.NotEqual(t, valueData, string(valueSlice))
}
