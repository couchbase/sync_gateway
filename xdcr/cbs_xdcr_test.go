// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package xdcr

import (
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCBSXDCR(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is testing Couchbase Server XDCR")
	}
	ctx := base.TestCtx(t)
	bucket1 := base.GetTestBucket(t)
	bucket2 := base.GetTestBucket(t)
	defer bucket1.Close(ctx)
	defer bucket2.Close(ctx)

	fromBucket, err := base.AsGocbV2Bucket(bucket1)
	require.NoError(t, err)
	toBucket, err := base.AsGocbV2Bucket(bucket2)
	require.NoError(t, err)

	xdcr, err := NewCouchbaseServerXDCR(ctx, fromBucket, toBucket, MobileOff)
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
	)
	for _, doc := range []string{syncDoc, attachmentDoc, normalDoc} {
		_, err = bucket1.DefaultDataStore().Add(doc, exp, body)
		require.NoError(t, err)
	}
	// make sure attachments are copied
	for _, doc := range []string{normalDoc, attachmentDoc} {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			var value string
			_, err = bucket2.DefaultDataStore().Get(doc, &value)
			assert.NoError(c, err, "Could not get doc %s", doc)
			assert.Equal(c, body, value)
		}, time.Second*5, time.Millisecond*100)
	}

	var value any
	_, err = bucket2.DefaultDataStore().Get(syncDoc, &value)
	require.True(t, base.IsKeyNotFoundError(bucket2.DefaultDataStore(), err))

	// stats are not updated in real time, so we need to wait a bit
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stats, err := xdcr.Stats(ctx)
		assert.NoError(t, err)
		assert.Equal(c, uint64(1), stats.DocsFiltered)
		assert.Equal(c, uint64(2), stats.DocsWritten)

	}, time.Second*5, time.Millisecond*100)

}

// TestMobileXDCRNoSyncDataCopied:
//   - Setup XDCR with mobile flag set to active
//   - Put doc, attachment doc and sync doc
//   - Assert that doc and attachment doc are replicated, sync doc is not
//   - Assert that the version vector is written on destination bucket for each replicated doc
func TestMobileXDCRNoSyncDataCopied(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is testing Couchbase Server XDCR")
	}
	ctx := base.TestCtx(t)
	bucket1 := base.GetTestBucket(t)
	bucket2 := base.GetTestBucket(t)
	defer bucket1.Close(ctx)
	defer bucket2.Close(ctx)

	fromBucket, err := base.AsGocbV2Bucket(bucket1)
	require.NoError(t, err)
	toBucket, err := base.AsGocbV2Bucket(bucket2)
	require.NoError(t, err)

	// skip test if buckets do not support mobile XDCR
	if !BucketSupportsMobileXDCR(fromBucket) || !BucketSupportsMobileXDCR(toBucket) {
		t.Skip("test requires mobile XDCR")
	}

	xdcr, err := NewCouchbaseServerXDCR(ctx, fromBucket, toBucket, MobileActive)
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
	for _, doc := range []string{syncDoc, attachmentDoc, normalDoc} {
		_, err = bucket1.DefaultDataStore().Add(doc, exp, body)
		require.NoError(t, err)
	}

	// make sure attachments are copied
	for _, doc := range []string{normalDoc, attachmentDoc} {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			var value string
			_, err = bucket2.DefaultDataStore().Get(doc, &value)
			assert.NoError(c, err, "Could not get doc %s", doc)
			assert.Equal(c, body, value)
		}, time.Second*5, time.Millisecond*100)
	}

	var value any
	_, err = bucket2.DefaultDataStore().Get(syncDoc, &value)
	require.True(t, base.IsKeyNotFoundError(bucket2.DefaultDataStore(), err))

	// stats are not updated in real time, so we need to wait a bit
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stats, err := xdcr.Stats(ctx)
		assert.NoError(t, err)
		assert.Equal(c, uint64(1), stats.DocsFiltered)
		assert.Equal(c, uint64(2), stats.DocsWritten)

	}, time.Second*5, time.Millisecond*100)

	// verify VV is written to docs that are replicated
	for _, doc := range []string{normalDoc, attachmentDoc} {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			var response map[string]interface{}
			_, err := bucket2.DefaultDataStore().GetXattr(ctx, doc, "_vv", &response)
			assert.NoError(c, err, "Could not get doc %s", doc)
			for range response {
				assert.NotNil(c, response[version])
				assert.NotNil(c, response[source])
				assert.NotNil(c, response[curCAS])
			}
		}, time.Second*5, time.Millisecond*100)
	}
}
