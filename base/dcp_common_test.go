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
