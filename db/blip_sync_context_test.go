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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		name           string
		knownRevs      []any
		expectedKnown  []string
		isLegacyRev    bool
		legacyProtocol bool
		errorCase      bool
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
			name:      "invalid known revs",
			knownRevs: []any{"123@src1", "1-abc", "123@src2"},
			errorCase: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testCtx := t.Context()
			var bsc *BlipSyncContext
			if tc.legacyProtocol {
				bsc = &BlipSyncContext{
					activeCBMobileSubprotocol: CBMobileReplicationV3,
				}
			} else {
				bsc = &BlipSyncContext{
					activeCBMobileSubprotocol: CBMobileReplicationV4,
				}
			}

			_, changeLegacy, knownRevisions, err := bsc.getKnownRevs(testCtx, "someID", tc.knownRevs)
			if !tc.errorCase {
				require.NoError(t, err)
				assert.Equal(t, tc.isLegacyRev, changeLegacy)
				base.RequireKeysEqual(t, tc.expectedKnown, knownRevisions)
			} else {
				require.Error(t, err)
			}
		})
	}
}
