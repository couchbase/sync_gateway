// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"net/url"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestHLVEqualAllowingEncodedRevs(t *testing.T) {
	const chainRev, otherRev = "2-abcdef1234", "1-9876543210"
	// "1-initial" is a fake revTreeID with no hex digest, which has no encoded version and must not stop the comparison
	revChain := []string{chainRev, "1-initial"}

	encoded := func(revID string) db.Version {
		v, err := db.LegacyRevToRevTreeEncodedVersion(revID)
		require.NoError(t, err)
		return v
	}
	newHLV := func(pv ...db.Version) *db.HybridLogicalVector {
		hlv := db.NewHybridLogicalVector()
		hlv.SourceID, hlv.Version = "cv", 100
		for _, v := range pv {
			hlv.PreviousVersions[v.SourceID] = v.Value
		}
		return hlv
	}
	withCV := func(hlv *db.HybridLogicalVector, sourceID string, value uint64) *db.HybridLogicalVector {
		hlv.SourceID, hlv.Version = sourceID, value
		return hlv
	}
	withMV := func(hlv *db.HybridLogicalVector, mv ...db.Version) *db.HybridLogicalVector {
		for _, v := range mv {
			hlv.MergeVersions[v.SourceID] = v.Value
		}
		return hlv
	}
	withCVCAS := func(hlv *db.HybridLogicalVector, cvCAS uint64) *db.HybridLogicalVector {
		hlv.CurrentVersionCAS = cvCAS
		return hlv
	}

	testCases := []struct {
		name     string
		expected *db.HybridLogicalVector
		actual   *db.HybridLogicalVector
		equal    bool
	}{
		{
			name:     "identical",
			expected: newHLV(db.Version{SourceID: "a", Value: 1}),
			actual:   newHLV(db.Version{SourceID: "a", Value: 1}),
			equal:    true,
		},
		{
			name:     "identical merge versions",
			expected: withMV(newHLV(), db.Version{SourceID: "m", Value: 5}),
			actual:   withMV(newHLV(), db.Version{SourceID: "m", Value: 5}),
			equal:    true,
		},
		{
			// cvCAS is set by each peer from its own write, so it is never expected to match
			name:     "different cvCAS",
			expected: withCVCAS(newHLV(), 1),
			actual:   withCVCAS(newHLV(), 2),
			equal:    true,
		},
		{
			name:     "different current version source",
			expected: newHLV(),
			actual:   withCV(newHLV(), "other", 100),
			equal:    false,
		},
		{
			name:     "different current version value",
			expected: newHLV(),
			actual:   withCV(newHLV(), "cv", 101),
			equal:    false,
		},
		{
			name:     "different merge version value",
			expected: withMV(newHLV(), db.Version{SourceID: "m", Value: 5}),
			actual:   withMV(newHLV(), db.Version{SourceID: "m", Value: 6}),
			equal:    false,
		},
		{
			name:     "missing merge versions",
			expected: withMV(newHLV(), db.Version{SourceID: "m", Value: 5}),
			actual:   newHLV(),
			equal:    false,
		},
		{
			// the encoded-revision allowance only applies to pv, never to mv
			name:     "extra encoded version of a revision in the chain in merge versions",
			expected: newHLV(),
			actual:   withMV(newHLV(), encoded(chainRev)),
			equal:    false,
		},
		{
			name:     "extra encoded version of a revision in the chain",
			expected: newHLV(),
			actual:   newHLV(encoded(chainRev)),
			equal:    true,
		},
		{
			name:     "extra encoded version of a revision outside the chain",
			expected: newHLV(),
			actual:   newHLV(encoded(otherRev)),
			equal:    false,
		},
		{
			name:     "extra version that isn't revTreeID encoded",
			expected: newHLV(),
			actual:   newHLV(db.Version{SourceID: "a", Value: 1}),
			equal:    false,
		},
		{
			name:     "missing expected pv entry",
			expected: newHLV(encoded(chainRev)),
			actual:   newHLV(),
			equal:    false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.equal, hlvEqualAllowingEncodedRevs(tc.expected, tc.actual, revChain))
		})
	}
}

// TestExpectedISGRDocFromPeerLegacyDoc checks that a snapshot of a document with no HLV describes it as it is, so
// RequireDoc can assert a peer holding a legacy document was left unchanged, while WaitForDocReplicated still expects
// the receiving peer to have the revTreeID-encoded CV.
func TestExpectedISGRDocFromPeerLegacyDoc(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	runner := NewSGRTestRunner(t)
	runner.Run(func(t *testing.T) {
		peers := runner.SetupSGRPeers(t)
		const docID = "legacyDoc"
		legacyDoc := peers.PassiveRT.CreateDocNoHLV(docID, db.Body{"channels": []string{"alice"}})

		snapshot := ExpectedISGRDocFromPeer(t, peers.PassiveRT, docID)
		assert.True(t, snapshot.NoHLV)
		assert.Nil(t, snapshot.HLV)
		assert.Equal(t, DocVersion{RevTreeID: legacyDoc.GetRevTreeID()}, snapshot.Version)
		runner.RequireDoc(docID, peers.PassiveRT, snapshot)

		ar, err := db.NewActiveReplicator(peers.ActiveRT.Context(), &db.ActiveReplicatorConfig{
			ID:                     t.Name(),
			Direction:              db.ActiveReplicatorTypePull,
			RemoteDBURL:            passiveDBURL(t, peers),
			ActiveDB:               &db.Database{DatabaseContext: peers.ActiveRT.GetDatabase()},
			ChangesBatchSize:       200,
			ReplicationStatsMap:    DbReplicatorStats(t, peers.ActiveRT.GetDatabase(), t.Name()),
			CollectionsEnabled:     !peers.ActiveRT.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: runner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		require.NoError(t, ar.Start(peers.ActiveRT.Context()))
		defer func() { require.NoError(t, ar.Stop()) }()

		doc := runner.WaitForDocReplicated(docID, peers.PassiveRT, peers.ActiveRT, snapshot.Version)
		require.NotNil(t, doc.HLV)
		if runner.IsV4Protocol() {
			encodedCV, err := db.LegacyRevToRevTreeEncodedVersion(legacyDoc.GetRevTreeID())
			require.NoError(t, err)
			assert.Equal(t, encodedCV, *doc.HLV.ExtractCurrentVersionFromHLV())
		}
		// pulling must not have given the passive peer's document an HLV
		runner.RequireDoc(docID, peers.PassiveRT, snapshot)
	})
}

func passiveDBURL(t *testing.T, peers *TestISGRPeers) *url.URL {
	u, err := url.Parse(peers.PassiveDBURL)
	require.NoError(t, err)
	return u
}
