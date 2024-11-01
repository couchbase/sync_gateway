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
	"slices"
	"testing"
	"time"

	"golang.org/x/exp/maps"

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
	fromBucketSourceID, err := GetSourceID(ctx, fromBucket)
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
		requireCV(t, xattrs[base.VvXattrName], fromBucketSourceID, cas)
	}

	_, err = toDs.Get(syncDoc, nil)
	base.RequireDocNotFoundError(t, err)

	var totalDocsWritten uint64
	var totalDocsFiltered uint64

	// stats are not updated in real time, so we need to wait a bit
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stats, err := xdcr.Stats(ctx)
		assert.NoError(t, err)
		assert.Equal(c, totalDocsFiltered+1, stats.MobileDocsFiltered)
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
	fromBucketSourceID, err := GetSourceID(ctx, fromBucket)
	require.NoError(t, err)

	hlvAgent := db.NewHLVAgent(t, fromDs, "fakeHLVSourceID", base.VvXattrName)

	testCases := []struct {
		name        string
		docID       string
		body        string
		HLV         func(fromCas uint64) *db.HybridLogicalVector
		hasHLV      bool
		preXDCRFunc func(t *testing.T, docID string) uint64
	}{
		{
			name:  "normal doc",
			docID: "doc1",
			body:  `{"key":"value"}`,
			HLV: func(fromCas uint64) *db.HybridLogicalVector {
				return &db.HybridLogicalVector{
					CurrentVersionCAS: fromCas,
					SourceID:          fromBucketSourceID,
					Version:           fromCas,
				}
			},
			hasHLV: true,
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
			HLV: func(fromCas uint64) *db.HybridLogicalVector {
				return &db.HybridLogicalVector{
					CurrentVersionCAS: fromCas,
					SourceID:          fromBucketSourceID,
					Version:           fromCas,
				}
			},
			hasHLV: true,
			preXDCRFunc: func(t *testing.T, docID string) uint64 {
				_, err := toDs.WriteCas(docID, 0, 0, []byte(`{"datastore":"toDs"}`), 0)
				require.NoError(t, err)
				cas, err := fromDs.WriteCas(docID, 0, 0, []byte(`{"datastore":"fromDs"}`), 0)
				require.NoError(t, err)
				return cas
			},
		},
		{
			name:   "dest doc newer, expect keep same dest doc",
			docID:  "doc3",
			body:   `{"datastore":"toDs"}`,
			hasHLV: false,
			preXDCRFunc: func(t *testing.T, docID string) uint64 {
				_, err := fromDs.WriteCas(docID, 0, 0, []byte(`{"datastore":"fromDs"}`), 0)
				require.NoError(t, err)
				cas, err := toDs.WriteCas(docID, 0, 0, []byte(`{"datastore":"toDs"}`), 0)
				require.NoError(t, err)
				return cas
			},
		},
		{
			name:  "src doc has hlv",
			docID: "doc4",
			body:  hlvAgent.GetHelperBody(),
			HLV: func(fromCas uint64) *db.HybridLogicalVector {
				return &db.HybridLogicalVector{
					CurrentVersionCAS: fromCas,
					SourceID:          hlvAgent.SourceID(),
					Version:           fromCas,
				}
			},
			hasHLV: true,
			preXDCRFunc: func(t *testing.T, docID string) uint64 {
				ctx := base.TestCtx(t)
				return hlvAgent.InsertWithHLV(ctx, docID)
			},
		},
	}
	// tests write a document
	// start xdcr
	// verify result

	var totalDocsProcessed uint64 // totalDocsProcessed will be incremented in each subtest
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fromCAS := testCase.preXDCRFunc(t, testCase.docID)

			xdcr := startXDCR(t, fromBucket, toBucket, XDCROptions{Mobile: MobileOn})
			defer func() {
				stats, err := xdcr.Stats(ctx)
				assert.NoError(t, err)
				totalDocsProcessed = stats.DocsProcessed
				assert.NoError(t, xdcr.Stop(ctx))
			}()
			requireWaitForXDCRDocsProcessed(t, xdcr, 1+totalDocsProcessed)

			body, xattrs, destCas, err := toDs.GetWithXattrs(ctx, testCase.docID, []string{base.VvXattrName, base.MouXattrName})
			require.NoError(t, err, "Could not get doc %s", testCase.docID)
			require.Equal(t, fromCAS, destCas)
			require.JSONEq(t, testCase.body, string(body))
			require.NotContains(t, xattrs, base.MouXattrName)
			if !testCase.hasHLV {
				require.NotContains(t, xattrs, base.VvXattrName)
				return
			}
			require.Contains(t, xattrs, base.VvXattrName)

			var hlv db.HybridLogicalVector
			require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &hlv))
			require.Equal(t, *testCase.HLV(fromCAS), hlv)
		})
	}
}

func TestVVWriteTwice(t *testing.T) {
	fromBucket, fromDs, toBucket, toDs := getTwoBucketDataStores(t)
	ctx := base.TestCtx(t)
	fromBucketSourceID, err := GetSourceID(ctx, fromBucket)
	require.NoError(t, err)

	docID := "doc1"
	ver1Body := `{"ver":1}`
	fromCAS, err := fromDs.WriteCas(docID, 0, 0, []byte(ver1Body), 0)
	require.NoError(t, err)
	xdcr := startXDCR(t, fromBucket, toBucket, XDCROptions{Mobile: MobileOn})
	defer func() {
		assert.NoError(t, xdcr.Stop(ctx))
	}()
	requireWaitForXDCRDocsProcessed(t, xdcr, 1)

	body, xattrs, destCas, err := toDs.GetWithXattrs(ctx, docID, []string{base.VvXattrName, base.MouXattrName})
	require.NoError(t, err)
	require.Equal(t, fromCAS, destCas)
	require.JSONEq(t, ver1Body, string(body))
	requireCV(t, xattrs[base.VvXattrName], fromBucketSourceID, fromCAS)

	fromCAS2, err := fromDs.WriteCas(docID, 0, fromCAS, []byte(`{"ver":2}`), 0)
	require.NoError(t, err)
	requireWaitForXDCRDocsProcessed(t, xdcr, 2)

	body, xattrs, destCas, err = toDs.GetWithXattrs(ctx, docID, []string{base.VvXattrName, base.MouXattrName})
	require.NoError(t, err)
	require.Equal(t, fromCAS2, destCas)
	require.JSONEq(t, `{"ver":2}`, string(body))
	require.Contains(t, xattrs, base.VvXattrName)
	requireCV(t, xattrs[base.VvXattrName], fromBucketSourceID, fromCAS2)
}

func TestVVObeyMou(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeySGTest)
	fromBucket, fromDs, toBucket, toDs := getTwoBucketDataStores(t)
	ctx := base.TestCtx(t)
	fromBucketSourceID, err := GetSourceID(ctx, fromBucket)
	require.NoError(t, err)

	docID := "doc1"
	hlvAgent := db.NewHLVAgent(t, fromDs, fromBucketSourceID, base.VvXattrName)
	fromCas1 := hlvAgent.InsertWithHLV(ctx, "doc1")

	xdcr := startXDCR(t, fromBucket, toBucket, XDCROptions{Mobile: MobileOn})
	defer func() {
		assert.NoError(t, xdcr.Stop(ctx))
	}()
	requireWaitForXDCRDocsProcessed(t, xdcr, 1)

	body, xattrs, destCas, err := toDs.GetWithXattrs(ctx, docID, []string{base.VvXattrName, base.MouXattrName, base.VirtualXattrRevSeqNo})
	require.NoError(t, err)
	require.Equal(t, fromCas1, destCas)
	require.JSONEq(t, hlvAgent.GetHelperBody(), string(body))
	require.NotContains(t, xattrs, base.MouXattrName)
	require.Contains(t, xattrs, base.VvXattrName)
	var vv db.HybridLogicalVector
	require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &vv))
	expectedVV := db.HybridLogicalVector{
		CurrentVersionCAS: fromCas1,
		SourceID:          hlvAgent.SourceID(),
		Version:           fromCas1,
	}

	require.Equal(t, expectedVV, vv)

	stats, err := xdcr.Stats(ctx)
	assert.NoError(t, err)
	require.Equal(t, Stats{
		DocsWritten:   1,
		DocsProcessed: 1,
	}, *stats)

	fmt.Printf("HONK HONK HONK\n")
	mou := &db.MetadataOnlyUpdate{
		PreviousCAS:      base.CasToString(fromCas1),
		PreviousRevSeqNo: db.RetrieveDocRevSeqNo(t, xattrs[base.VirtualXattrRevSeqNo]),
	}

	opts := &sgbucket.MutateInOptions{
		MacroExpansion: []sgbucket.MacroExpansionSpec{
			sgbucket.NewMacroExpansionSpec(db.XattrMouCasPath(), sgbucket.MacroCas),
		},
	}
	const userXattrKey = "extra_xattr"
	fromCas2, err := fromDs.UpdateXattrs(ctx, docID, 0, fromCas1, map[string][]byte{
		base.MouXattrName: base.MustJSONMarshal(t, mou),
		userXattrKey:      []byte(`{"key":"value"}`),
	}, opts)
	require.NoError(t, err)
	require.NotEqual(t, fromCas1, fromCas2)

	requireWaitForXDCRDocsProcessed(t, xdcr, 2)
	stats, err = xdcr.Stats(ctx)
	assert.NoError(t, err)
	require.Equal(t, Stats{
		TargetNewerDocs: 1,
		DocsWritten:     1,
		DocsProcessed:   2,
	}, *stats)

	body, xattrs, destCas, err = toDs.GetWithXattrs(ctx, docID, []string{base.VvXattrName, base.MouXattrName, userXattrKey})
	require.NoError(t, err)
	require.Equal(t, fromCas1, destCas)
	require.JSONEq(t, hlvAgent.GetHelperBody(), string(body))
	require.NotContains(t, xattrs, base.MouXattrName)
	require.Contains(t, xattrs, base.VvXattrName)
	require.NotContains(t, xattrs, userXattrKey)
	vv = db.HybridLogicalVector{}
	require.NoError(t, base.JSONUnmarshal(xattrs[base.VvXattrName], &vv))
	require.Equal(t, expectedVV, vv)
}

func TestLWWAfterInitialReplication(t *testing.T) {
	fromBucket, fromDs, toBucket, toDs := getTwoBucketDataStores(t)
	ctx := base.TestCtx(t)
	fromBucketSourceID, err := GetSourceID(ctx, fromBucket)
	require.NoError(t, err)

	docID := "doc1"
	ver1Body := `{"ver":1}`
	fromCAS, err := fromDs.WriteCas(docID, 0, 0, []byte(ver1Body), 0)
	require.NoError(t, err)
	xdcr := startXDCR(t, fromBucket, toBucket, XDCROptions{Mobile: MobileOn})
	defer func() {
		assert.NoError(t, xdcr.Stop(ctx))
	}()
	requireWaitForXDCRDocsProcessed(t, xdcr, 1)

	body, xattrs, destCas, err := toDs.GetWithXattrs(ctx, docID, []string{base.VvXattrName, base.MouXattrName})
	require.NoError(t, err)
	require.Equal(t, fromCAS, destCas)
	require.JSONEq(t, ver1Body, string(body))
	require.Contains(t, xattrs, base.VvXattrName)
	requireCV(t, xattrs[base.VvXattrName], fromBucketSourceID, fromCAS)

	// write to dest bucket again
	toCas2, err := toDs.WriteCas(docID, 0, fromCAS, []byte(`{"ver":3}`), 0)
	require.NoError(t, err)

	body, xattrs, destCas, err = toDs.GetWithXattrs(ctx, docID, []string{base.VvXattrName, base.MouXattrName})
	require.NoError(t, err)
	require.Equal(t, toCas2, destCas)
	require.JSONEq(t, `{"ver":3}`, string(body))
	require.Contains(t, xattrs, base.VvXattrName)
	requireCV(t, xattrs[base.VvXattrName], fromBucketSourceID, fromCAS)
}

func TestReplicateXattrs(t *testing.T) {
	fromBucket, fromDs, toBucket, toDs := getTwoBucketDataStores(t)

	testCases := []struct {
		name                 string
		startingSourceXattrs map[string][]byte
		startingDestXattrs   map[string][]byte
		finalXattrs          map[string][]byte
	}{
		{
			name: "_sync on source only",
			startingSourceXattrs: map[string][]byte{
				base.SyncXattrName: []byte(`{"source":"fromDs"}`),
			},
			finalXattrs: map[string][]byte{},
		},
		{
			name: "_sync on dest only",
			startingDestXattrs: map[string][]byte{
				base.SyncXattrName: []byte(`{"source":"toDs"}`),
			},
			finalXattrs: map[string][]byte{
				base.SyncXattrName: []byte(`{"source":"toDs"}`),
			},
		},
		{
			name: "_sync on both",
			startingSourceXattrs: map[string][]byte{
				base.SyncXattrName: []byte(`{"source":"fromDs"}`),
			},
			startingDestXattrs: map[string][]byte{
				base.SyncXattrName: []byte(`{"source":"toDs"}`),
			},
			finalXattrs: map[string][]byte{
				base.SyncXattrName: []byte(`{"source":"toDs"}`),
			},
		},
		{
			name: "_globalSync on source only",
			startingSourceXattrs: map[string][]byte{
				base.GlobalXattrName: []byte(`{"source":"fromDs"}`),
			},
			finalXattrs: map[string][]byte{
				base.GlobalXattrName: []byte(`{"source":"fromDs"}`),
			},
		},
		{
			name: "_globalSync on overwrite dest",
			startingSourceXattrs: map[string][]byte{
				base.GlobalXattrName: []byte(`{"source":"fromDs"}`),
			},
			startingDestXattrs: map[string][]byte{
				base.GlobalXattrName: []byte(`{"source":"toDs"}`),
			},
			finalXattrs: map[string][]byte{
				base.GlobalXattrName: []byte(`{"source":"fromDs"}`),
			},
		},
	}

	var totalDocsProcessed uint64 // totalDocsProcessed will be incremented in each subtest
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			docID := testCase.name

			ctx := base.TestCtx(t)
			body := []byte(`{"key":"value"}`)
			if testCase.startingDestXattrs != nil {
				_, err := toDs.WriteWithXattrs(ctx, docID, 0, 0, body, testCase.startingDestXattrs, nil, nil)
				require.NoError(t, err)
			}
			fromCas, err := fromDs.WriteWithXattrs(ctx, docID, 0, 0, body, testCase.startingSourceXattrs, nil, nil)
			require.NoError(t, err)
			xdcr := startXDCR(t, fromBucket, toBucket, XDCROptions{Mobile: MobileOn})
			defer func() {
				stats, err := xdcr.Stats(ctx)
				assert.NoError(t, err)
				totalDocsProcessed = stats.DocsProcessed
				assert.NoError(t, xdcr.Stop(ctx))
			}()
			requireWaitForXDCRDocsProcessed(t, xdcr, 1+totalDocsProcessed)

			allXattrKeys := slices.Concat(maps.Keys(testCase.startingSourceXattrs), maps.Keys(testCase.finalXattrs))
			_, xattrs, destCas, err := toDs.GetWithXattrs(ctx, docID, allXattrKeys)
			require.NoError(t, err)
			require.Equal(t, fromCas, destCas)
			require.Equal(t, testCase.finalXattrs, xattrs)
		})
	}
}

// startXDCR will create a new XDCR manager and start it. This must be closed by the caller.
func startXDCR(t *testing.T, fromBucket base.Bucket, toBucket base.Bucket, opts XDCROptions) Manager {
	ctx := base.TestCtx(t)
	xdcr, err := NewXDCR(ctx, fromBucket, toBucket, opts)
	require.NoError(t, err)
	err = xdcr.Start(ctx)
	require.NoError(t, err)
	return xdcr
}

// requireWaitForXDCRDocsProcessed waits for the replication to process the exact number of documents. If more than the expected number of documents are processed, this will fail.
func requireWaitForXDCRDocsProcessed(t *testing.T, xdcr Manager, expectedDocsProcessed uint64) {
	ctx := base.TestCtx(t)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stats, err := xdcr.Stats(ctx)
		assert.NoError(t, err)
		assert.Equal(c, expectedDocsProcessed, stats.DocsProcessed)
	}, time.Second*5, time.Millisecond*100)
}

// requireCV requires tests that a given hlv from server has a sourceID and cas matching the version. This is strict and will fail if _pv is populated (TODO: CBG-4250).
func requireCV(t *testing.T, vvBytes []byte, sourceID string, cas uint64) {
	var vv *db.HybridLogicalVector
	require.NoError(t, base.JSONUnmarshal(vvBytes, &vv))
	require.Equal(t, &db.HybridLogicalVector{
		CurrentVersionCAS: cas,
		SourceID:          sourceID,
		Version:           cas,
	}, vv)
}
