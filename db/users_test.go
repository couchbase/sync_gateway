/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"testing"

	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/google/uuid"
)

// TestDeleteRoleSequenceAllocation asserts that a soft delete allocates the sequence it writes to the
// role tombstone, and that a purge allocates nothing - an unwritten sequence would leave a permanent
// gap in the sequence range.
func TestDeleteRoleSequenceAllocation(t *testing.T) {
	testCases := []struct {
		name          string
		purge         bool
		allocatedSeqs uint64
	}{
		{name: "soft delete", purge: false, allocatedSeqs: 1},
		{name: "purge", purge: true, allocatedSeqs: 0},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db, ctx := setupTestDB(t)
			defer db.Close(ctx)

			authenticator := db.Authenticator(ctx)
			roleName := uuid.NewString()
			role, err := authenticator.NewRole(roleName, channels.BaseSetOf(t, "chan1"))
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(role))

			assignedBefore := db.DbStats.Database().SequenceAssignedCount.Value()
			require.NoError(t, db.DeleteRole(ctx, roleName, testCase.purge))
			assigned := db.DbStats.Database().SequenceAssignedCount.Value() - assignedBefore
			require.Equal(t, testCase.allocatedSeqs, assigned)
		})
	}
}
