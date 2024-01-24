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

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestXDCR(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	ctx := base.TestCtx(t)
	fromBucket := base.GetTestBucket(t)
	toBucket := base.GetTestBucket(t)
	defer fromBucket.Close(ctx)
	defer toBucket.Close(ctx)

	xdcr, err := NewReplication(ctx, fromBucket, toBucket)
	require.NoError(t, err)
	err = xdcr.Start(ctx)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, xdcr.Stop(ctx))
	}()
	const (
		syncDoc           = "_sync:doc1doc2"
		attachmentDoc     = "_sync:att2:foo"
		attachmentDocBody = `1ABINARYBLOB`
		normalDoc         = "doc2"
		normalDocBody     = `{"key":"value"}`
		exp               = 0
	)
	_, err = fromBucket.DefaultDataStore().AddRaw(syncDoc, exp, []byte(`{"foo", "bar"}`))
	require.NoError(t, err)

	attachmentDocCas, err := fromBucket.DefaultDataStore().WriteCas(attachmentDoc, 0, exp, 0, []byte(attachmentDocBody), sgbucket.Raw)
	require.NoError(t, err)

	normalDocCas, err := fromBucket.DefaultDataStore().WriteCas(normalDoc, 0, exp, 0, []byte(normalDocBody), 0)
	require.NoError(t, err)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		val, cas, err := toBucket.DefaultDataStore().GetRaw(normalDoc)
		assert.NoError(c, err)
		assert.Equal(c, normalDocCas, cas)
		assert.JSONEq(c, normalDocBody, string(val))
	}, time.Second*5, time.Millisecond*100)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		val, cas, err := toBucket.DefaultDataStore().GetRaw(attachmentDoc)
		assert.NoError(c, err)
		assert.Equal(c, attachmentDocCas, cas)
		assert.Equal(c, []byte(attachmentDocBody), val)
	}, time.Second*5, time.Millisecond*100)

	_, err = toBucket.DefaultDataStore().Get(syncDoc, nil)
	require.True(t, base.IsKeyNotFoundError(toBucket.DefaultDataStore(), err))

	require.NoError(t, fromBucket.DefaultDataStore().Delete(normalDoc))
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var value string
		_, err = toBucket.DefaultDataStore().Get(normalDoc, &value)
		assert.Error(t, err)
		assert.True(t, base.IsDocNotFoundError(err))
	}, time.Second*5, time.Millisecond*100)

	// stats are not updated in real time, so we need to wait a bit
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stats, err := xdcr.Stats(ctx)
		assert.NoError(t, err)
		assert.Equal(c, uint64(1), stats.DocsFiltered)
		assert.Equal(c, uint64(3), stats.DocsWritten)

	}, time.Second*5, time.Millisecond*100)

}

func TestXattrMigration(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	ctx := base.TestCtx(t)
	fromBucket := base.GetTestBucket(t)
	toBucket := base.GetTestBucket(t)
	defer fromBucket.Close(ctx)
	defer toBucket.Close(ctx)

	xdcr, err := NewReplication(ctx, fromBucket, toBucket)
	require.NoError(t, err)
	err = xdcr.Start(ctx)
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, xdcr.Stop(ctx))
	}()

	const (
		docID           = "doc1"
		systemXattrName = "_system"
		userXattrName   = "user"
		body            = `{"foo": "bar"}`
		systemXattrVal  = `{"bar": "baz"}`
		userXattrVal    = `{"baz": "baz"}`
		isDelete        = false
		deleteBody      = false
	)

	startingCas, err := fromBucket.DefaultDataStore().WriteWithXattr(ctx, docID, systemXattrName, 0, 0, []byte(body), []byte(systemXattrVal), isDelete, deleteBody, nil)
	require.NoError(t, err)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		toVal := map[string]string{}
		toSystemXattrVal := map[string]string{}
		toUserXattrVal := map[string]string{}
		cas, err := toBucket.DefaultDataStore().GetWithXattr(ctx, docID, systemXattrName, userXattrName, &toVal, &toSystemXattrVal, &toUserXattrVal)
		assert.NoError(c, err)
		assert.Equal(c, startingCas, cas)
		assert.Equal(c, mustMarshalJSON(t, body), val)
	}, time.Second*5, time.Millisecond*100)

}

func mustMarshalJSON(t *testing.T, val interface{}) []byte {
	bytes, err := base.JSONMarshal(val)
	require.NoError(t, err)
	return bytes
}
