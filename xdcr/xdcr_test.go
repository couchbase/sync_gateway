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
	"github.com/couchbase/sync_gateway/db"
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

	opts := XDCROptions{}
	if base.TestSupportsMobileXDCR() {
		opts.Mobile = MobileOn
	} else {
		opts.Mobile = MobileOff
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
		startingBody  = `{"key":"value"}`
	)
	dataStores := map[base.DataStore]base.DataStore{
		fromBucket.DefaultDataStore(): toBucket.DefaultDataStore(),
	}
	var fromDs base.DataStore
	var toDs base.DataStore
	if base.TestsUseNamedCollections() {
		fromDs, err = fromBucket.GetNamedDataStore(0)
		require.NoError(t, err)
		toDs, err = toBucket.GetNamedDataStore(0)
		require.NoError(t, err)
		dataStores[fromDs] = toDs
	} else {
		fromDs = fromBucket.DefaultDataStore()
		toDs = toBucket.DefaultDataStore()
	}
	fromBucketSourceID, err := getSourceID(ctx, fromBucket)
	require.NoError(t, err)
	docCas := make(map[string]uint64)
	for _, doc := range []string{syncDoc, attachmentDoc, normalDoc} {
		var inputCas uint64
		var err error
		docCas[doc], err = fromDs.WriteCas(doc, exp, inputCas, []byte(startingBody), 0)
		require.NoError(t, err)
		_, _, err = fromDs.GetXattrs(ctx, doc, []string{base.VvXattrName})
		// make sure that the doc does not have a version vector
		base.RequireXattrNotFoundError(t, err)
	}

	// make sure attachments are copied
	for _, doc := range []string{normalDoc, attachmentDoc} {
		var body []byte
		var xattrs map[string][]byte
		var cas uint64
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			var err error
			body, xattrs, cas, err = toDs.GetWithXattrs(ctx, doc, []string{base.VvXattrName, base.MouXattrName})
			assert.NoError(c, err, "Could not get doc %s", doc)
		}, time.Second*5, time.Millisecond*100)
		require.Equal(t, docCas[doc], cas)
		require.JSONEq(t, startingBody, string(body))
		require.NotContains(t, xattrs, base.MouXattrName)
		if !base.TestSupportsMobileXDCR() {
			require.Len(t, xattrs, 0)
			continue
		}
		require.Contains(t, xattrs, base.VvXattrName)
		casString := string(base.Uint64CASToLittleEndianHex(cas))
		var vv *db.HybridLogicalVector
		require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &vv))
		require.Equal(t, &db.HybridLogicalVector{
			CurrentVersionCAS: casString,
			SourceID:          fromBucketSourceID,
			Version:           casString,
		}, vv)
	}

	_, err = toDs.Get(syncDoc, nil)
	base.RequireDocNotFoundError(t, err)

	var totalDocsWritten uint64
	var totalDocsFiltered uint64

	// stats are not updated in real time, so we need to wait a bit
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stats, err := xdcr.Stats(ctx)
		assert.NoError(t, err)
		assert.Equal(c, totalDocsFiltered+1, stats.DocsFiltered)
		assert.Equal(c, totalDocsWritten+2, stats.DocsWritten)

	}, time.Second*5, time.Millisecond*100)
}

// getTwoBucketDataStores creates two data stores in separate buckets to run xdcr within. Returns a named collection or a default collection based on the global test configuration.
func getTwoBucketDataStores(t *testing.T) (base.Bucket, sgbucket.DataStore, base.Bucket, sgbucket.DataStore) {
	ctx := base.TestCtx(t)
	base.RequireNumTestBuckets(t, 2)
	fromBucket := base.GetTestBucket(t)
	t.Cleanup(func() {
		fromBucket.Close(ctx)
	})
	toBucket := base.GetTestBucket(t)
	t.Cleanup(func() {
		toBucket.Close(ctx)
	})
	var fromDs base.DataStore
	var toDs base.DataStore
	if base.TestsUseNamedCollections() {
		var err error
		fromDs, err = fromBucket.GetNamedDataStore(0)
		require.NoError(t, err)
		toDs, err = toBucket.GetNamedDataStore(0)
		require.NoError(t, err)
	} else {
		fromDs = fromBucket.DefaultDataStore()
		toDs = toBucket.DefaultDataStore()
	}
	return fromBucket, fromDs, toBucket, toDs
}

func TestReplicateVV(t *testing.T) {
	fromBucket, fromDs, toBucket, toDs := getTwoBucketDataStores(t)
	ctx := base.TestCtx(t)
	fromBucketSourceID, err := getSourceID(ctx, fromBucket)
	require.NoError(t, err)

	testCases := []struct {
		name        string
		docID       string
		body        string
		preXDCRFunc func(t *testing.T, docID string) uint64
	}{
		{
			name:  "normal doc",
			docID: "doc1",
			body:  `{"key":"value"}`,
			preXDCRFunc: func(t *testing.T, docID string) uint64 {
				cas, err := fromDs.WriteCas(docID, 0, 0, []byte(`{"key":"value"}`), 0)
				require.NoError(t, err)
				return cas
			},
		},
		{
			name:  "dest doc older, expect overwrite",
			docID: "doc2",
			body:  `{"datastore":"fromDs"}`,
			preXDCRFunc: func(t *testing.T, docID string) uint64 {
				_, err := toDs.WriteCas(docID, 0, 0, []byte(`{"datastore":"toDs"}`), 0)
				require.NoError(t, err)
				cas, err := fromDs.WriteCas(docID, 0, 0, []byte(`{"datastore":"fromDs"}`), 0)
				require.NoError(t, err)
				return cas
			},
		},
		{
			name:  "dest doc newer, expect keep old version",
			docID: "doc3",
			body:  `{"datastore":"fromDs"}`,
			preXDCRFunc: func(t *testing.T, docID string) uint64 {
				_, err := toDs.WriteCas(docID, 0, 0, []byte(`{"datastore":"toDs"}`), 0)
				require.NoError(t, err)
				cas, err := fromDs.WriteCas(docID, 0, 0, []byte(`{"datastore":"fromDs"}`), 0)
				require.NoError(t, err)
				return cas
			},
		},
	}
	// tests write a document
	// start xdcr
	// verify result
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fromCAS := testCase.preXDCRFunc(t, testCase.docID)

			xdcr := startXDCR(t, fromBucket, toBucket, XDCROptions{Mobile: MobileOn})
			defer func() {
				assert.NoError(t, xdcr.Stop(ctx))
			}()
			requireWaitForXDCRDocsWritten(t, xdcr, 1)

			body, xattrs, destCas, err := toDs.GetWithXattrs(ctx, testCase.docID, []string{base.VvXattrName, base.MouXattrName})
			require.NoError(t, err, "Could not get doc %s", testCase.docID)
			require.Equal(t, fromCAS, destCas)
			require.JSONEq(t, testCase.body, string(body))
			require.NotContains(t, xattrs, base.MouXattrName)
			require.Contains(t, xattrs, base.VvXattrName)
			casString := string(base.Uint64CASToLittleEndianHex(fromCAS))
			var vv *db.HybridLogicalVector
			require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &vv))
			require.Equal(t, &db.HybridLogicalVector{
				CurrentVersionCAS: casString,
				SourceID:          fromBucketSourceID,
				Version:           casString,
			}, vv)

		})
	}
}

func TestVVWriteTwice(t *testing.T) {
	fromBucket, fromDs, toBucket, toDs := getTwoBucketDataStores(t)
	ctx := base.TestCtx(t)
	fromBucketSourceID, err := getSourceID(ctx, fromBucket)
	require.NoError(t, err)

	docID := "doc1"
	ver1Body := `{"ver":1}`
	fromCAS, err := fromDs.WriteCas(docID, 0, 0, []byte(ver1Body), 0)
	require.NoError(t, err)
	xdcr := startXDCR(t, fromBucket, toBucket, XDCROptions{Mobile: MobileOn})
	defer func() {
		assert.NoError(t, xdcr.Stop(ctx))
	}()
	requireWaitForXDCRDocsWritten(t, xdcr, 1)

	body, xattrs, destCas, err := toDs.GetWithXattrs(ctx, docID, []string{base.VvXattrName, base.MouXattrName})
	require.NoError(t, err)
	require.Equal(t, fromCAS, destCas)
	require.JSONEq(t, ver1Body, string(body))
	require.Contains(t, xattrs, base.VvXattrName)
	casString := string(base.Uint64CASToLittleEndianHex(fromCAS))
	var vv *db.HybridLogicalVector
	require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &vv))
	require.Equal(t, &db.HybridLogicalVector{
		CurrentVersionCAS: casString,
		SourceID:          fromBucketSourceID,
		Version:           casString,
	}, vv)

	fromCAS2, err := fromDs.WriteCas(docID, 0, fromCAS, []byte(`{"ver":2}`), 0)
	require.NoError(t, err)
	requireWaitForXDCRDocsWritten(t, xdcr, 2)

	body, xattrs, destCas, err = toDs.GetWithXattrs(ctx, docID, []string{base.VvXattrName, base.MouXattrName})
	require.NoError(t, err)
	require.Equal(t, fromCAS2, destCas)
	require.JSONEq(t, `{"ver":2}`, string(body))
	require.Contains(t, xattrs, base.VvXattrName)
	casString = string(base.Uint64CASToLittleEndianHex(fromCAS2))
	require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &vv))
	require.Equal(t, &db.HybridLogicalVector{
		CurrentVersionCAS: casString,
		SourceID:          fromBucketSourceID,
		Version:           casString,
	}, vv)
}

func TestVVWriteOnDestAfterInitialReplication(t *testing.T) {
	fromBucket, fromDs, toBucket, toDs := getTwoBucketDataStores(t)
	ctx := base.TestCtx(t)
	fromBucketSourceID, err := getSourceID(ctx, fromBucket)
	require.NoError(t, err)

	docID := "doc1"
	ver1Body := `{"ver":1}`
	fromCAS, err := fromDs.WriteCas(docID, 0, 0, []byte(ver1Body), 0)
	require.NoError(t, err)
	xdcr := startXDCR(t, fromBucket, toBucket, XDCROptions{Mobile: MobileOn})
	defer func() {
		assert.NoError(t, xdcr.Stop(ctx))
	}()
	requireWaitForXDCRDocsWritten(t, xdcr, 1)

	body, xattrs, destCas, err := toDs.GetWithXattrs(ctx, docID, []string{base.VvXattrName, base.MouXattrName})
	require.NoError(t, err)
	require.Equal(t, fromCAS, destCas)
	require.JSONEq(t, ver1Body, string(body))
	require.Contains(t, xattrs, base.VvXattrName)
	casString := string(base.Uint64CASToLittleEndianHex(fromCAS))
	fmt.Printf("vv after write1: %+v\n", string(xattrs[base.VvXattrName]))
	var vv *db.HybridLogicalVector
	require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &vv))
	require.Equal(t, &db.HybridLogicalVector{
		CurrentVersionCAS: casString,
		SourceID:          fromBucketSourceID,
		Version:           casString,
	}, vv)

	// write to dest bucket again
	toCas2, err := toDs.WriteCas(docID, 0, fromCAS, []byte(`{"ver":3}`), 0)
	require.NoError(t, err)
	fmt.Printf("toCas2: %d\n", toCas2)

	body, xattrs, destCas, err = toDs.GetWithXattrs(ctx, docID, []string{base.VvXattrName, base.MouXattrName})
	require.NoError(t, err)
	require.Equal(t, toCas2, destCas)
	require.JSONEq(t, `{"ver":3}`, string(body))
	require.Contains(t, xattrs, base.VvXattrName)
	casString = string(base.Uint64CASToLittleEndianHex(fromCAS))
	fmt.Printf("vv after write2: %+v\n", string(xattrs[base.VvXattrName]))
	require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &vv))
	require.Equal(t, &db.HybridLogicalVector{
		CurrentVersionCAS: casString,
		SourceID:          fromBucketSourceID,
		Version:           casString,
	}, vv)
}

func startXDCR(t *testing.T, fromBucket base.Bucket, toBucket base.Bucket, opts XDCROptions) Manager {
	ctx := base.TestCtx(t)
	xdcr, err := NewXDCR(ctx, fromBucket, toBucket, opts)
	require.NoError(t, err)
	err = xdcr.Start(ctx)
	require.NoError(t, err)
	return xdcr
}

func requireWaitForXDCRDocsWritten(t *testing.T, xdcr Manager, expectedDocsWritten uint64) {
	ctx := base.TestCtx(t)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stats, err := xdcr.Stats(ctx)
		assert.NoError(t, err)
		assert.Equal(c, expectedDocsWritten, stats.DocsWritten)
	}, time.Second*5, time.Millisecond*100)
}
