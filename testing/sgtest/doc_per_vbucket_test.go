// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package sgtest

import (
	"context"
	"fmt"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/testing/require"
)

// VbnoGetter is a fake vbnoGetter for tests that don't need a real bucket.
type VbnoGetter struct {
	numVBuckets uint16
}

func (f VbnoGetter) GetMaxVbno(_ context.Context) (uint16, error) {
	return f.numVBuckets, nil
}

// TestVBucketDocIDs exhaustively verifies VBucketDocIDs for every reachable vBucket and supported count.
func TestVBucketDocIDs(t *testing.T) {
	for _, numVBuckets := range []uint16{32, 64, 100, 128, 1024} {
		t.Run(fmt.Sprintf("numVBuckets=%d", numVBuckets), func(t *testing.T) {
			bucket := VbnoGetter{numVBuckets: numVBuckets}
			seen := make(map[string]bool)
			for vBucket := range docIDsForCount(t, numVBuckets) {
				ids := VBucketDocIDs(t, bucket, vBucket, docIDsPerVBucket)
				require.Len(t, ids, docIDsPerVBucket)
				for _, id := range ids {
					require.False(t, seen[id], "doc ID %q returned for more than one vBucket", id)
					seen[id] = true
					vbNo := sgbucket.VBHash(id, numVBuckets)
					require.Equal(t, uint32(vBucket), vbNo, "doc ID %q should map to vBucket %d (got %d)", id, vBucket, vbNo)
				}
			}
		})
	}
}

// TestDocPerVBucket verifies DocPerVBucket returns exactly one doc ID per reachable vBucket.
func TestDocPerVBucket(t *testing.T) {
	for _, numVBuckets := range []uint16{32, 64, 100, 128, 1024} {
		t.Run(fmt.Sprintf("numVBuckets=%d", numVBuckets), func(t *testing.T) {
			bucket := VbnoGetter{numVBuckets: numVBuckets}
			docIDs := DocPerVBucket(t, bucket)
			reachable := docIDsForCount(t, numVBuckets)
			require.Len(t, docIDs, len(reachable))
			for vBucket := range reachable {
				id, ok := docIDs[vBucket]
				require.True(t, ok, "expected a doc ID for vBucket %d", vBucket)
				vbNo := sgbucket.VBHash(id, numVBuckets)
				require.Equal(t, uint32(vBucket), vbNo, "doc ID %q should map to vBucket %d (got %d)", id, vBucket, vbNo)
			}
		})
	}
}
