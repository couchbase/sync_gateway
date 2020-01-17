package base

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateDcpStreamName(t *testing.T) {
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

			cbgtDCPName := fmt.Sprintf("%s%s-%x", cbgt.DCPFeedPrefix, dcpStreamName, rand.Int31())
			t.Logf("generated name of length %d: %s", len(cbgtDCPName), cbgtDCPName)

			assert.Truef(t, len(cbgtDCPName) <= maxDCPNameLength,
				"len(cbgtDCPName)=%d is exceeds max allowed %d chars: %s",
				len(cbgtDCPName), maxDCPNameLength, cbgtDCPName)
		})
	}
}
