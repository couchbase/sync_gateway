//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestTimestampsDocCreation(t *testing.T) {
	testBucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer testBucket.Close(ctx)
	metadataStore := testBucket.GetMetadataStore()
	metaKeys := base.NewMetadataKeys(t.Name())

	mgrOpts := TimestampsMgrOpts{
		ctx:           ctx,
		metadataStore: metadataStore,
		metaKeys:      metaKeys,
	}

	tm := NewTimestampsManager(mgrOpts)
	require.NotNil(t, tm, "NewTimestampsManager must return a TimestampsManager")

	err := tm.Start()
	require.NoError(t, err)

	tsDoc, _, err := metadataStore.GetRaw(ctx, metaKeys.TimestampsSeqKey())
	assert.NoError(t, err)
	assert.NotNil(t, tsDoc)
}
