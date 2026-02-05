// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package xdcr

import (
	"encoding/base64"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiActorLosingConflictUpdateRemovingAttachments
// Removes attachments on a losing conflicting document and ensures that attachments remain present in the bucket and accessible on both documents on either side.
//
// 1. Create a document with an attachment on Actor A
// 2. Replicate to Actor B
// 3. Stop replications
// 4. Update the document on Actor A, removing the attachment
// 5. Update the document on Actor B, changing the body (twice to ensure MWW resolves this as the winner as well as LWW)
// 6. Start replications
// 7. Observe resolved conflict on both Actor A and Actor B, with the attachment still present
func TestMultiActorLosingConflictUpdateRemovingAttachments(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	// turn off auto import - since we want reliable XDCR stats and don't want MOU/import echos to interfere
	rtA := rest.NewRestTester(t, &rest.RestTesterConfig{AutoImport: base.Ptr(false)})
	defer rtA.Close()
	rtB := rest.NewRestTester(t, &rest.RestTesterConfig{AutoImport: base.Ptr(false)})
	defer rtB.Close()

	ctx := base.TestCtx(t)
	opts := XDCROptions{Mobile: MobileOn}

	// Set up bi-directional XDCR
	xdcrAtoB, err := NewXDCR(ctx, rtA.Bucket(), rtB.Bucket(), opts)
	require.NoError(t, err)
	require.NoError(t, xdcrAtoB.Start(ctx))
	xdcrBtoA, err := NewXDCR(ctx, rtB.Bucket(), rtA.Bucket(), opts)
	require.NoError(t, err)
	require.NoError(t, xdcrBtoA.Start(ctx))

	defer func() {
		// stop XDCR, will already be stopped if test doesn't fail early
		if err := xdcrAtoB.Stop(ctx); err != nil {
			assert.Equal(t, ErrReplicationNotRunning, err)
		}
		if err := xdcrBtoA.Stop(ctx); err != nil {
			assert.Equal(t, ErrReplicationNotRunning, err)
		}
	}()

	const (
		docID        = "doc1"
		attachmentID = "hello.txt"
	)
	attachment := base64.StdEncoding.EncodeToString([]byte("Hello World!"))

	rtAVersion := rtA.PutDocWithAttachment(docID, `{"key":"value"}`, attachmentID, attachment)

	// fetch attachment via REST API
	attAResp := rtA.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+docID+"/"+attachmentID, "")
	rest.RequireStatus(t, attAResp, http.StatusOK)

	// wait for doc to replicate to rtB
	rtB.WaitForVersion(docID, rtAVersion)
	rtBVersion, _ := rtB.GetDoc(docID)

	// wait for XDCR stats to ensure attachment data also made it over
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		stats, err := xdcrAtoB.Stats(ctx)
		require.NoError(c, err)
		// doc and attachment doc
		assert.Equalf(c, uint64(2), stats.DocsWritten, "expected doc and attachment to be replicated")
	}, time.Second*5, time.Millisecond*100)

	// stop replication
	require.NoError(t, xdcrAtoB.Stop(ctx))
	require.NoError(t, xdcrBtoA.Stop(ctx))

	// fetch attachment via REST API
	attBResp := rtB.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+docID+"/"+attachmentID, "")
	rest.RequireStatus(t, attBResp, http.StatusOK)

	// update doc on A, removing attachment
	_ = rtA.UpdateDoc(docID, rtAVersion, `{"key":"value2"}`)

	// update doc on B, changing body but keeping attachment stub (twice to ensure MWW resolves this as the winner as well as LWW)
	rtBVersion = rtB.UpdateDoc(docID, rtBVersion, `{"key":"value3","_attachments":{"`+attachmentID+`":{"stub":true}}}`)
	rtBVersion = rtB.UpdateDoc(docID, rtBVersion, `{"key":"value4","_attachments":{"`+attachmentID+`":{"stub":true}}}`)

	// start replication
	require.NoError(t, xdcrAtoB.Start(ctx))
	require.NoError(t, xdcrBtoA.Start(ctx))

	// wait for XDCR stats to ensure attachment data also made it over
	var expectedDocsWritten uint64 = 0 // attachment deletion (or lack of)
	// Rosmar's XDCR implementation differs in two ways:
	// 1. Stats don't get reset on restart
	// 2. No DCP checkpointing - so there's always more TargetNewerDocs than expected even if we reset stats
	if !base.TestUseCouchbaseServer() {
		expectedDocsWritten = expectedDocsWritten + 2 // (old replication stats: 1 doc + 1 attachment)
	}
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		stats, err := xdcrAtoB.Stats(ctx)
		require.NoError(c, err)
		assert.Equalf(c, expectedDocsWritten, stats.DocsWritten, "unexpected additional mutation replicated (an attachment delete?)")
	}, time.Second*5, time.Millisecond*100)

	// wait for XDCR stats to ensure attachment data also made it over
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		stats, err := xdcrBtoA.Stats(ctx)
		require.NoError(c, err)
		assert.Equalf(c, uint64(1), stats.DocsWritten, "expected resolved conflict to be replicated back to A")
	}, time.Second*5, time.Millisecond*100)

	// wait for doc (resolved conflict) to replicate back to rtA
	rtA.WaitForVersionHLVOnly(docID, rtBVersion)

	// check attachment metadata exists
	docA := rtA.GetDocument(docID)
	docB := rtB.GetDocument(docID)
	assert.Equal(t, docA.Attachments(), docB.Attachments())
	assert.Contains(t, docA.Attachments(), attachmentID)
	assert.Contains(t, docB.Attachments(), attachmentID)

	// check attachment contents are retrievable
	attAResp = rtA.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+docID+"/"+attachmentID, "")
	rest.AssertStatus(t, attAResp, http.StatusOK)
	attBResp = rtB.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+docID+"/"+attachmentID, "")
	rest.AssertStatus(t, attBResp, http.StatusOK)

}
