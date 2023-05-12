/*
Copyright 2017-Present Couchbase, Inc.

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

// TestBackupOldRevision ensures that old revisions are kept around temporarily for in-flight requests and delta sync purposes.
func TestBackupOldRevision(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	deltasEnabled := base.IsEnterpriseEdition()
	xattrsEnabled := base.TestUseXattrs()

	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{DeltaSyncOptions: DeltaSyncOptions{
		Enabled:          deltasEnabled,
		RevMaxAgeSeconds: DefaultDeltaSyncRevMaxAge,
	}})
	defer db.Close(ctx)
	collection := GetSingleDatabaseCollectionWithUser(t, db)

	docID := t.Name()

	rev1ID, _, err := collection.Put(ctx, docID, Body{"test": true})
	require.NoError(t, err)

	// make sure we didn't accidentally store an empty old revision
	_, err = collection.getOldRevisionJSON(base.TestCtx(t), docID, "")
	assert.Error(t, err)
	assert.Equal(t, "404 missing", err.Error())

	// check for current rev backup in xattr+delta case (to support deltas by sdk imports)
	_, err = collection.getOldRevisionJSON(base.TestCtx(t), docID, rev1ID)
	if deltasEnabled && xattrsEnabled {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		assert.Equal(t, "404 missing", err.Error())
	}

	// create rev 2 and check backups for both revs
	rev2ID := "2-abc"
	_, _, err = collection.PutExistingRevWithBody(ctx, docID, Body{"test": true, "updated": true}, []string{rev2ID, rev1ID}, true)
	require.NoError(t, err)

	// now in all cases we'll have rev 1 backed up (for at least 5 minutes)
	_, err = collection.getOldRevisionJSON(base.TestCtx(t), docID, rev1ID)
	require.NoError(t, err)

	// check for current rev backup in xattr+delta case (to support deltas by sdk imports)
	_, err = collection.getOldRevisionJSON(base.TestCtx(t), docID, rev2ID)
	if deltasEnabled && xattrsEnabled {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
		assert.Equal(t, "404 missing", err.Error())
	}
}
