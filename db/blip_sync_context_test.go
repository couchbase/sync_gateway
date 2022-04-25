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
				blipContextDb:    &Database{Ctx: base.TestCtx(t)},
				useDeltas:        tt.startingCtxDeltas,
				sgCanUseDeltas:   tt.sgCanUseDeltas,
				replicationStats: NewBlipSyncStats(),
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
				blipContextDb:    &Database{Ctx: base.TestCtx(b)},
				replicationStats: NewBlipSyncStats(),
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ctx.useDeltas = tt.startingCtxDeltas
				ctx.sgCanUseDeltas = tt.sgCanUseDeltas
				ctx.setUseDeltas(tt.clientCanUseDeltas)
			}
		})
	}
}
