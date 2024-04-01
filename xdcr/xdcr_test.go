// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package xdcr

import (
	"fmt"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMobileXDCRNoSyncDataCopied:
//   - Setup XDCR with mobile flag set to active if test bucket supports it, otherwise setup XDCR filter to filter out sync docs
//   - Put doc, attachment doc and sync doc
//   - Assert that doc and attachment doc are replicated, sync doc is not
//   - Assert that the version vector is written on destination bucket for each replicated doc
func TestMobileXDCRNoSyncDataCopied(t *testing.T) {
	ctx := base.TestCtx(t)
	base.RequireNumTestBuckets(t, 2)
	fromBucket := base.GetTestBucket(t)
	defer fromBucket.Close(ctx)
	toBucket := base.GetTestBucket(t)
	defer toBucket.Close(ctx)

	opts := sgbucket.XDCROptions{}
	if base.TestSupportsMobileXDCR() {
		opts.Mobile = sgbucket.XDCRMobileOn
	} else {
		opts.Mobile = sgbucket.XDCRMobileOff
		opts.FilterExpression = fmt.Sprintf("NOT REGEXP_CONTAINS(META().id, \"^%s\") OR REGEXP_CONTAINS(META().id, \"^%s\")", base.SyncDocPrefix, base.Att2Prefix)
	}
	xdcr, err := NewXDCR(ctx, fromBucket, toBucket, opts)
	require.NoError(t, err)
	err = xdcr.Start(ctx)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, xdcr.Stop(ctx))
	}()
	const (
		syncDoc       = "_sync:doc1doc2"
		attachmentDoc = "_sync:att2:foo"
		normalDoc     = "doc2"
		exp           = 0
		body          = `{"key":"value"}`
		version       = "ver"
		source        = "src"
		curCAS        = "cvCas"
	)
	dataStores := map[base.DataStore]base.DataStore{
		fromBucket.DefaultDataStore(): toBucket.DefaultDataStore(),
	}
	if base.TestsUseNamedCollections() {
		fromDs, err := fromBucket.GetNamedDataStore(0)
		require.NoError(t, err)
		toDs, err := toBucket.GetNamedDataStore(0)
		require.NoError(t, err)
		dataStores[fromDs] = toDs
	}
	for fromDs, toDs := range dataStores {
		for _, doc := range []string{syncDoc, attachmentDoc, normalDoc} {
			_, err = fromDs.Add(doc, exp, body)
			require.NoError(t, err)
		}

		// make sure attachments are copied
		for _, doc := range []string{normalDoc, attachmentDoc} {
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				var value string
				_, err = toDs.Get(doc, &value)
				assert.NoError(c, err, "Could not get doc %s", doc)
				assert.Equal(c, body, value)
			}, time.Second*5, time.Millisecond*100)
		}

		var value any
		_, err = toDs.Get(syncDoc, &value)
		require.True(t, base.IsKeyNotFoundError(toDs, err))

		// stats are not updated in real time, so we need to wait a bit
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			stats, err := xdcr.Stats(ctx)
			assert.NoError(t, err)
			assert.Equal(c, uint64(1), stats.DocsFiltered)
			assert.Equal(c, uint64(2), stats.DocsWritten)

		}, time.Second*5, time.Millisecond*100)

		if base.UnitTestUrlIsWalrus() {
			// TODO: CBG-3861 implement _vv support in rosmar
			return
		}
		// in mobile xdcr mode a version vector will be written
		if base.TestSupportsMobileXDCR() {
			// verify VV is written to docs that are replicated
			for _, doc := range []string{normalDoc, attachmentDoc} {
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					xattrs, _, err := toDs.GetXattrs(ctx, doc, []string{"_vv"})
					assert.NoError(c, err, "Could not get doc %s", doc)
					vvXattrBytes, ok := xattrs["_vv"]
					require.True(t, ok)
					var vvXattrVal map[string]any
					require.NoError(t, base.JSONUnmarshal(vvXattrBytes, &vvXattrVal))
					assert.NotNil(c, vvXattrVal[version])
					assert.NotNil(c, vvXattrVal[source])
					assert.NotNil(c, vvXattrVal[curCAS])

				}, time.Second*5, time.Millisecond*100)
			}
		}
	}
}
