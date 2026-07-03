// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

import (
	"testing"

	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/require"
)

// makeConflictWrite builds a conflictingWrite from the given peer, attributed to peer name and with a CV of
// sourceID@value. name and sourceID differ for Sync Gateway peers (peer "sg1" writes under source "rosmar1").
func makeConflictWrite(t *testing.T, peer Peer, name, sourceID string, value uint64) conflictingWrite {
	hlv := db.NewHybridLogicalVector()
	require.NoError(t, hlv.AddVersion(db.Version{SourceID: sourceID, Value: value}))
	return conflictingWrite{
		peer:    peer,
		version: BodyAndVersion{docMeta: DocMetadata{DocID: "doc1", HLV: hlv}, updatePeer: name},
	}
}

// TestExpectedConflictWinner covers which write is expected to win when peers generate colliding HLC values.
// The last writer normally has the strictly-greatest value and wins, but a CBL (v4) peer that ties the winning
// value wins the resulting conflict, since only CBL peers perform conflict resolution in these topologies.
func TestExpectedConflictWinner(t *testing.T) {
	cbl := &CouchbaseLiteMockPeer{name: "cbl1", peerType: PeerTypeCouchbaseLite}
	cblV3 := &CouchbaseLiteMockPeer{name: "cbl1", peerType: PeerTypeCouchbaseLiteV3}
	sg := &SyncGatewayPeer{name: "sg1"}
	cbs := &CouchbaseServerPeer{name: "cbs1"}
	cbs2 := &CouchbaseServerPeer{name: "cbs2"}

	testCases := []struct {
		name       string
		writes     []conflictingWrite
		expectPeer string // updatePeer of the expected winner
	}{
		{
			name: "distinct values, last writer wins",
			writes: []conflictingWrite{
				makeConflictWrite(t, cbl, "cbl1", "cbl1", 100),
				makeConflictWrite(t, sg, "sg1", "rosmar1", 200),
			},
			expectPeer: "sg1",
		},
		{
			name: "value collision between CBL and last (SG) writer, CBL wins",
			writes: []conflictingWrite{
				makeConflictWrite(t, cbl, "cbl1", "cbl1", 200),
				makeConflictWrite(t, sg, "sg1", "rosmar1", 200),
			},
			expectPeer: "cbl1",
		},
		{
			name: "CBL is the last writer, wins outright",
			writes: []conflictingWrite{
				makeConflictWrite(t, sg, "sg1", "rosmar1", 200),
				makeConflictWrite(t, cbl, "cbl1", "cbl1", 200),
			},
			expectPeer: "cbl1",
		},
		{
			// With no CBL peer the function must not flip the winner - any genuine CBS/SG conflict is left for
			// XDCR to resolve. Note this exact collision cannot arise on rosmar (its process-global HLC makes CAS
			// unique across buckets), so it is purely asserting the function's scoping.
			name: "no CBL peer, collision leaves the last writer as winner",
			writes: []conflictingWrite{
				makeConflictWrite(t, cbs, "cbs1", "cbs1", 200),
				makeConflictWrite(t, cbs2, "cbs2", "cbs2", 200),
			},
			expectPeer: "cbs2",
		},
		{
			name: "v3 CBL peer does not steal a value collision",
			writes: []conflictingWrite{
				makeConflictWrite(t, cblV3, "cbl1", "cbl1", 200),
				makeConflictWrite(t, sg, "sg1", "rosmar1", 200),
			},
			expectPeer: "sg1",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			winner := expectedConflictWinner(t, tc.writes)
			require.Equal(t, tc.expectPeer, winner.updatePeer)
		})
	}
}

// TestExpectedConflictWinnerReconstructsHLV verifies that when a CBL peer wins an HLC value collision, the
// returned version carries the converged HLV - the winning CV plus the losing versions in previous versions -
// rather than the CBL peer's creation-time snapshot (which has empty previous versions). Non-CBL peers assert
// the full HLV, so the previous versions must be present or those assertions never converge.
func TestExpectedConflictWinnerReconstructsHLV(t *testing.T) {
	const value = uint64(200)
	cbl := &CouchbaseLiteMockPeer{name: "cbl1", peerType: PeerTypeCouchbaseLite}
	sg := &SyncGatewayPeer{name: "sg1"}
	// cbl1 and sg1 (source rosmar1) collide on the same CV value; the CBL peer wins the resulting conflict.
	writes := []conflictingWrite{
		makeConflictWrite(t, cbl, "cbl1", "cbl1", value),
		makeConflictWrite(t, sg, "sg1", "rosmar1", value),
	}
	winner := expectedConflictWinner(t, writes)
	require.Equal(t, "cbl1", winner.updatePeer)

	expectedHLV := db.NewHybridLogicalVector()
	require.NoError(t, expectedHLV.AddVersion(db.Version{SourceID: "cbl1", Value: value}))
	expectedHLV.SetPreviousVersion("rosmar1", value)
	require.True(t, winner.docMeta.HLV.Equal(expectedHLV), "expected converged HLV %#v, got %#v", expectedHLV, winner.docMeta.HLV)
}
