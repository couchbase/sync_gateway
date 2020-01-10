package rest

import (
	"context"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
)

// TestBlipSyncContextSetUseDeltas verifies all permutations of setUseDeltas()
func TestBlipSyncContextSetUseDeltas(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeySync)()

	tests := []struct {
		startingCtxDeltas,
		sgCanUseDeltas,
		clientCanUseDeltas,
		expectedCtxDeltas bool
	}{
		// deltas starting enabled
		{true, true, true, true},    // both sides on
		{true, false, false, false}, // both sides off
		{true, false, true, false},  // server off
		{true, true, false, false},  // client off

		// deltas starting disabled
		{false, true, true, true},    // both sides on
		{false, false, false, false}, // both sides off
		{false, false, true, false},  // server off
		{false, true, false, false},  // client off
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			ctx := &blipSyncContext{
				dbStats:        db.NewDatabaseStats(),
				blipContextDb:  &db.Database{Ctx: context.TODO()},
				useDeltas:      tt.startingCtxDeltas,
				sgCanUseDeltas: tt.sgCanUseDeltas,
			}

			ctx.setUseDeltas(tt.clientCanUseDeltas)
			assert.Equal(t, tt.expectedCtxDeltas, ctx.useDeltas)
		})
	}
}
