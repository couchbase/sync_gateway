// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"context"
	"fmt"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

// TestSubprotocolString roundtrips the parse/format Subprotocol methods on the subprotocol constants.
func TestSubprotocolString(t *testing.T) {
	for i := minCBMobileSubprotocolVersion; i <= maxCBMobileSubprotocolVersion; i++ {
		str := i.SubprotocolString()
		parsed, err := ParseSubprotocolString(str)
		require.NoError(t, err)
		require.Equal(t, i, parsed)
	}
}

func TestBlipCorrelationID(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyWebSocket)
	for _, explicitTestID := range []bool{true, false} {
		t.Run(fmt.Sprintf("explicitTestID=%t", explicitTestID), func(t *testing.T) {
			testID := "explicit-test-id"
			if !explicitTestID {
				testID = ""
			}
			bc, err := NewSGBlipContext(ctx, testID, nil, nil)
			require.NoError(t, err)
			if !explicitTestID {
				require.NotEqual(t, testID, bc.ID)
				require.NotEmpty(t, bc.ID)
				testID = bc.ID
			}
			base.AssertLogContains(t, "c:"+testID, func() {
				bc.Logger(blip.LogGeneral, "Sample log message")
			})
			bsc, err := NewBlipSyncContext(ctx, bc, db, nil, cancelFunc)
			require.NoError(t, err)
			defer bsc.Close()

			base.AssertLogContains(t, "c:"+testID, func() {
				base.InfofCtx(bsc.loggingCtx, base.KeyWebSocket, "Sample log message")
			})
		})
	}
}
