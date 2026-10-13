/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestBlipSyncContextSetUseDeltas verifies all permutations of setUseDeltas()
func TestBlipSyncContextSetUseDeltas(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeySync)

	tests := []struct {
		name string
		startingCtxDeltas,
		sgCanUseDeltas,
		clientCanUseDeltas,
		expectedCtxDeltas bool
	}{
		// fast-path (no change to current state)
		{"start off, both sides off", false, false, false, false},
		{"start off, server off", false, false, true, false},
		{"start off, client off", false, true, false, false},
		{"start on, both sides on", true, true, true, true},

		// turn on
		{"start off, both sides on", false, true, true, true},

		// Scenarios that aren't currently possible (starting on, and then turning off)
		{"both sides off", true, false, false, false},
		{"server off", true, false, true, false},
		{"client off", true, true, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &BlipSyncContext{
				blipContextDb:    &Database{},
				useDeltas:        tt.startingCtxDeltas,
				sgCanUseDeltas:   tt.sgCanUseDeltas,
				replicationStats: NewBlipSyncStats(),
				loggingCtx:       base.TestCtx(t),
			}

			ctx.setUseDeltas(tt.clientCanUseDeltas)
			assert.Equal(t, tt.expectedCtxDeltas, ctx.useDeltas)
		})
	}
}

// BenchmarkBlipSyncContextSetUseDeltas verifies all permutations of setUseDeltas()
func BenchmarkBlipSyncContextSetUseDeltas(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelInfo, base.KeyHTTP)

	tests := []struct {
		name string
		startingCtxDeltas,
		sgCanUseDeltas,
		clientCanUseDeltas,
		expectedCtxDeltas bool
	}{
		// fast-path (no change to current state)
		{"start off, both sides off", false, false, false, false},
		{"start off, server off", false, false, true, false},
		{"start off, client off", false, true, false, false},
		{"start on, both sides on", true, true, true, true},

		// turn on
		{"start off, both sides on", false, true, true, true},

		// Scenarios that aren't currently possible (starting on, and then turning off)
		{"start on, both sides off", true, false, false, false},
		{"start on, server off", true, false, true, false},
		{"start on, client off", true, true, false, false},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			ctx := &BlipSyncContext{
				blipContextDb:    &Database{},
				replicationStats: NewBlipSyncStats(),
				loggingCtx:       base.TestCtx(b),
			}
			b.ResetTimer()
			for b.Loop() {
				ctx.useDeltas = tt.startingCtxDeltas
				ctx.sgCanUseDeltas = tt.sgCanUseDeltas
				ctx.setUseDeltas(tt.clientCanUseDeltas)
			}
		})
	}
}

func TestKnownRevs(t *testing.T) {
	testCases := []struct {
		name            string
		knownRevs       []any
		expectedKnown   []string
		isLegacyRev     bool
		legacyProtocol  bool
		clientType      BLIPSyncContextClientType
		expectAssertion bool
		errorCase       bool
	}{
		{
			name:          "no known revs",
			knownRevs:     []any{},
			expectedKnown: []string{},
		},
		{
			name:           "< 4 subprotocol no known revs",
			knownRevs:      []any{},
			legacyProtocol: true,
			expectedKnown:  []string{},
		},
		{
			name:           "< 4 subprotocol: client has known rev",
			knownRevs:      []any{"1-abc"},
			expectedKnown:  []string{"1-abc"},
			legacyProtocol: true,
		},
		{
			name:          ">= 4 subprotocol: client has known rev",
			knownRevs:     []any{"123@src", "1-abc"},
			expectedKnown: []string{"123@src", "1-abc"},
		},
		{
			name:          ">= 4 subprotocol: client has known rev and rev is legacy rev",
			knownRevs:     []any{"1-abc"},
			expectedKnown: []string{"1-abc"},
			isLegacyRev:   true,
		},
		{
			name:      "non string known rev",
			knownRevs: []any{"123@src", 1},
			errorCase: true,
		},
		{
			// CBL clients with multiple remotes can send more than one cv, this is tolerated
			name:          "cbl: more than two entries with cv",
			clientType:    BLIPClientTypeCBL2,
			knownRevs:     []any{"123@src1", "123@src2", "123@src3"},
			expectedKnown: []string{"123@src1", "123@src2", "123@src3"},
		},
		{
			// ISGR never sends more than cv + current revtree ID in version vector mode, so this would indicate a bug
			name:            "isgr: more than two entries with cv",
			clientType:      BLIPClientTypeSGR2,
			knownRevs:       []any{"123@src1", "1-abc", "123@src2"},
			expectedKnown:   []string{"123@src1", "1-abc", "123@src2"},
			expectAssertion: true,
		},
		{
			// ISGR in revtree mode can return a known rev for each conflicting leaf
			name:           "isgr < 4 subprotocol: more than two revtree entries",
			clientType:     BLIPClientTypeSGR2,
			legacyProtocol: true,
			knownRevs:      []any{"3-abc", "3-def", "2-ghi"},
			expectedKnown:  []string{"3-abc", "3-def", "2-ghi"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testCtx := t.Context()
			bsc := &BlipSyncContext{
				activeCBMobileSubprotocol: CBMobileReplicationV4,
				clientType:                tc.clientType,
			}
			if tc.legacyProtocol {
				bsc.activeCBMobileSubprotocol = CBMobileReplicationV3
			}

			if tc.errorCase {
				_, _, _, err := bsc.getKnownRevs(testCtx, "someID", tc.knownRevs)
				require.Error(t, err)
				return
			}

			assertionFailCount := base.SyncGatewayStats.GlobalStats.ResourceUtilization.AssertionFailCount
			startAssertionFailCount := assertionFailCount.Value()
			if tc.expectAssertion {
				// this subtest triggers an assertion on purpose, so it needs to start clean and not leave the failure for the test harness
				if len(base.AssertionFailures()) > 0 {
					t.Skip("Skipping test - we have other assertion failures present and we can't reliably run this test this without losing those")
				}
				defer base.ClearAssertionFailures()
			}

			if tc.expectAssertion && base.IsDevMode() {
				// AssertfCtx panics in dev mode, so the return values can't be checked
				require.Panics(t, func() {
					_, _, _, _ = bsc.getKnownRevs(testCtx, "someID", tc.knownRevs)
				})
			} else {
				_, changeLegacy, knownRevisions, err := bsc.getKnownRevs(testCtx, "someID", tc.knownRevs)
				require.NoError(t, err)
				assert.Equal(t, tc.isLegacyRev, changeLegacy)
				base.RequireKeysEqual(t, tc.expectedKnown, knownRevisions)
			}

			expectedAssertionFailCount := startAssertionFailCount
			if tc.expectAssertion {
				expectedAssertionFailCount++
			}
			assert.Equal(t, expectedAssertionFailCount, assertionFailCount.Value())
		})
	}
}
