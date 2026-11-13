/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
)

// newTestPullMessage builds an incoming message in the shape Sync Gateway sends it, so a test can drive
// the blip tester client's pull handlers without a replication producing the message.
func newTestPullMessage(btcc *BlipTesterCollectionClient, profile string, properties blip.Properties, body []byte) *blip.Message {
	msg := blip.NewParsedIncomingMessage(nil, blip.RequestType, properties, body)
	msg.SetProfile(profile)
	btcc.addCollectionProperty(msg)
	return msg
}

// lastSeq returns the last client sequence the collection allocated.
func (btcc *BlipTesterCollectionClient) lastSeq() clientSeq {
	btcc.seqLock.RLock()
	defer btcc.seqLock.RUnlock()
	return btcc._seqLast
}

// TestBlipTesterClientPullHandling covers how the blip tester client stores what a pull delivers, driving
// the pull handlers directly rather than through a replication. Each case is a revision Sync Gateway can
// send that the client has to record under a version other than the one on the wire, or not record at all.
func TestBlipTesterClientPullHandling(t *testing.T) {
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
		defer rt.Close()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer client.Close()
		ctx, cancel := context.WithCancelCause(base.TestCtx(t))
		defer cancel(errors.New("test finished"))
		btcc := btcRunner.SingleCollection(client.id)

		if client.UseHLV() {
			// both cases turn on a revision being identified by its cv, which pre-4.0 clients do not do
			requireDuplicateRevIsNoop(t, ctx, client, btcc)
			requireStaleRevIsNoop(t, ctx, client, btcc)
		}
		requireReplacedRevIsStored(t, ctx, client, btcc)
	})
}

// requireDuplicateRevIsNoop covers the same cv being sent twice, which Sync Gateway does when a document
// is rewritten without its cv changing. The second copy is answered as a no-op, and stores nothing -
// including no client sequence, which the push changes feed would otherwise iterate over forever because
// no document sits behind it.
func requireDuplicateRevIsNoop(t *testing.T, ctx context.Context, client *BlipTesterClient, btcc *BlipTesterCollectionClient) {
	docID := sgtest.SafeDocumentName(t, t.Name()) + "_duplicateRev"
	hlv := db.NewHybridLogicalVector()
	require.NoError(t, hlv.AddVersion(db.Version{SourceID: "sourceB", Value: 200}))
	version := DocVersion{CV: *hlv.ExtractCurrentVersionFromHLV()}
	body := []byte(`{"foo":"bar"}`)

	handleRev := client.pullReplication.handleRev(ctx, client)
	newRevMsg := func() *blip.Message {
		return newTestPullMessage(btcc, db.MessageRev, blip.Properties{
			db.RevMessageID:  docID,
			db.RevMessageRev: version.CV.String(),
		}, body)
	}

	handleRev(newRevMsg())
	seqAfterFirstRev := btcc.lastSeq()

	duplicateMsg := newRevMsg()
	handleRev(duplicateMsg)
	assert.Equal(t, "true", duplicateMsg.Response().Properties[noopProperty], "a revision the client already holds is acked as a no-op")
	assert.Equal(t, seqAfterFirstRev, btcc.lastSeq(), "a no-op rev must not allocate a client sequence")

	// a sequence with no document behind it never makes the changes feed wait: it walks past the empty
	// sequence and starts again, yielding the document at the sequence before it every time
	changesCtx, cancelChanges := context.WithCancelCause(ctx)
	defer cancelChanges(errors.New("changes feed check finished"))
	changes := btcc.changesSince(changesCtx, seqAfterFirstRev, true)
	select {
	case change := <-changes:
		assert.Fail(t, "changes feed yielded a document after a no-op rev", "docID %q seq %d", change.docID, change.seq)
	case <-time.After(time.Second):
	}
}

// requireStaleRevIsNoop covers a rev for a version older than the client's latest. Revs for separate
// changes batches are sent concurrently, so this happens, and couchbase-lite-core answers the rev as a no-op.
func requireStaleRevIsNoop(t *testing.T, ctx context.Context, client *BlipTesterClient, btcc *BlipTesterCollectionClient) {
	docID := sgtest.SafeDocumentName(t, t.Name()) + "_staleRev"
	staleCV := db.Version{SourceID: "sourceD", Value: 100}
	latestCV := db.Version{SourceID: "sourceE", Value: 200}
	staleBody := []byte(`{"version":1}`)
	latestBody := []byte(`{"version":2}`)

	handleRev := client.pullReplication.handleRev(ctx, client)
	handleRev(newTestPullMessage(btcc, db.MessageRev, blip.Properties{
		db.RevMessageID:  docID,
		db.RevMessageRev: staleCV.String(),
	}, staleBody))
	handleRev(newTestPullMessage(btcc, db.MessageRev, blip.Properties{
		db.RevMessageID:      docID,
		db.RevMessageRev:     latestCV.String(),
		db.RevMessageHistory: staleCV.String(),
	}, latestBody))
	seqAfterLatestRev := btcc.lastSeq()

	staleRevMsg := newTestPullMessage(btcc, db.MessageRev, blip.Properties{
		db.RevMessageID:  docID,
		db.RevMessageRev: staleCV.String(),
	}, staleBody)
	handleRev(staleRevMsg)
	assert.Equal(t, "true", staleRevMsg.Response().Properties[noopProperty], "a rev older than the client's latest is acked as a no-op")

	assert.Equal(t, seqAfterLatestRev, btcc.lastSeq(), "a stale rev must not allocate a client sequence")
	body, hlv, version := btcc.GetDoc(docID)
	require.NotNil(t, version)
	assert.JSONEq(t, string(latestBody), string(body))
	assert.Equal(t, latestCV, *hlv.ExtractCurrentVersionFromHLV())
}

// requireReplacedRevIsStored covers the replacedRev property on a pull. The client records the revision it
// was sent under the version the server replaced, so a test waiting for the version named in the changes
// message finds it. Sync Gateway names the replaced revision with a cv or with a rev tree ID, depending on
// what the client negotiated, so both forms are covered.
func requireReplacedRevIsStored(t *testing.T, ctx context.Context, client *BlipTesterClient, btcc *BlipTesterCollectionClient) {
	replacedVersion := DocVersion{RevTreeID: "2-def"}
	sentVersion := DocVersion{RevTreeID: "3-def"}
	replacedRev, sentRev := replacedVersion.RevTreeID, sentVersion.RevTreeID
	if client.UseHLV() {
		replacedVersion = DocVersion{CV: db.Version{SourceID: "sourceC", Value: 100}}
		sentVersion = DocVersion{CV: db.Version{SourceID: "sourceC", Value: 200}}
		replacedRev, sentRev = replacedVersion.CV.String(), sentVersion.CV.String()
	}

	handleRev := client.pullReplication.handleRev(ctx, client)
	for _, deleted := range []bool{false, true} {
		docID := fmt.Sprintf("%s_replacedRev_deleted_%t", sgtest.SafeDocumentName(t, t.Name()), deleted)
		properties := blip.Properties{
			db.RevMessageID:          docID,
			db.RevMessageRev:         sentRev,
			db.RevMessageReplacedRev: replacedRev,
		}
		if deleted {
			properties[db.RevMessageDeleted] = "1"
		}
		handleRev(newTestPullMessage(btcc, db.MessageRev, properties, []byte(`{"foo":"bar"}`)))

		_, _, found := btcc.GetVersion(docID, replacedVersion)
		assert.True(t, found, "deleted=%t: replaced version %v is not stored on the client", deleted, replacedVersion)
		_, _, found = btcc.GetVersion(docID, sentVersion)
		assert.True(t, found, "deleted=%t: sent version %v is not stored on the client", deleted, sentVersion)
	}
}
