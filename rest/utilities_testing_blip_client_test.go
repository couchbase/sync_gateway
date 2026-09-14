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
)

// newTestPullMessage builds an incoming message in the shape Sync Gateway sends it, so a test can drive
// the blip tester client's pull handlers without a replication producing the message.
func newTestPullMessage(btcc *BlipTesterCollectionClient, profile string, properties blip.Properties, body []byte) *blip.Message {
	msg := blip.NewParsedIncomingMessage(nil, blip.RequestType, properties, body)
	msg.SetProfile(profile)
	btcc.addCollectionProperty(msg)
	return msg
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
			requireRevAfterNoRevIsStored(t, ctx, client, btcc)
			requireDuplicateRevIsNoop(t, ctx, client, btcc)
		} else {
			// replacedRev is a rev tree ID, so only pre-4.0 clients are sent one
			requireReplacedRevIsStored(t, ctx, client, btcc)
		}
	})
}

// requireRevAfterNoRevIsStored covers a norev placeholder followed by a rev for the same cv. The
// placeholder carries the cv but no body, so the rev that follows is the first copy of the document the
// client holds, and must be stored rather than answered as a revision the client already has.
func requireRevAfterNoRevIsStored(t *testing.T, ctx context.Context, client *BlipTesterClient, btcc *BlipTesterCollectionClient) {
	docID := SafeDocumentName(t, t.Name()) + "_revAfterNoRev"
	hlv := db.NewHybridLogicalVector()
	require.NoError(t, hlv.AddVersion(db.Version{SourceID: "sourceA", Value: 100}))
	version := DocVersion{CV: *hlv.ExtractCurrentVersionFromHLV()}

	client.pullReplication.handleNoRev(ctx, client)(newTestPullMessage(btcc, db.MessageNoRev, blip.Properties{
		db.NorevMessageId:  docID,
		db.NorevMessageRev: version.CV.String(),
	}, nil))

	body := []byte(`{"foo":"bar"}`)
	revMsg := newTestPullMessage(btcc, db.MessageRev, blip.Properties{
		db.RevMessageID:  docID,
		db.RevMessageRev: version.CV.String(),
	}, body)
	client.pullReplication.handleRev(ctx, client)(revMsg)

	assert.NotEqual(t, "true", revMsg.Response().Properties[noopProperty], "the client held only a bodyless norev placeholder, so this rev is not a no-op")

	data, _, found := btcc.GetVersion(docID, version)
	require.True(t, found, "version %v is not stored on the client", version)
	assert.JSONEq(t, string(body), string(data), "the rev body must replace the norev placeholder")
}

// requireDuplicateRevIsNoop covers the same cv being sent twice, which Sync Gateway does when a document
// is rewritten without its cv changing. The second copy is answered as a no-op, and stores nothing -
// including no client sequence, which the push changes feed would otherwise iterate over forever because
// no document sits behind it.
func requireDuplicateRevIsNoop(t *testing.T, ctx context.Context, client *BlipTesterClient, btcc *BlipTesterCollectionClient) {
	seqLast := func() clientSeq {
		btcc.seqLock.RLock()
		defer btcc.seqLock.RUnlock()
		return btcc._seqLast
	}

	docID := SafeDocumentName(t, t.Name()) + "_duplicateRev"
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
	seqAfterFirstRev := seqLast()

	duplicateMsg := newRevMsg()
	handleRev(duplicateMsg)
	assert.Equal(t, "true", duplicateMsg.Response().Properties[noopProperty], "a revision the client already holds is acked as a no-op")
	assert.Equal(t, seqAfterFirstRev, seqLast(), "a no-op rev must not allocate a client sequence")

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

// requireReplacedRevIsStored covers the replacedRev property on a pull. The client records the revision it
// was sent under the rev ID the server replaced, so a test waiting for the revision named in the changes
// message finds it.
func requireReplacedRevIsStored(t *testing.T, ctx context.Context, client *BlipTesterClient, btcc *BlipTesterCollectionClient) {
	const replacedRevID = "2-def"
	const sentRevID = "3-def"

	handleRev := client.pullReplication.handleRev(ctx, client)
	for _, deleted := range []bool{false, true} {
		docID := fmt.Sprintf("%s_replacedRev_deleted_%t", SafeDocumentName(t, t.Name()), deleted)
		properties := blip.Properties{
			db.RevMessageID:          docID,
			db.RevMessageRev:         sentRevID,
			db.RevMessageReplacedRev: replacedRevID,
		}
		if deleted {
			properties[db.RevMessageDeleted] = "1"
		}
		handleRev(newTestPullMessage(btcc, db.MessageRev, properties, []byte(`{"foo":"bar"}`)))

		_, _, found := btcc.GetVersion(docID, DocVersion{RevTreeID: replacedRevID})
		assert.True(t, found, "deleted=%t: replaced revision %q is not stored on the client", deleted, replacedRevID)
	}
}
