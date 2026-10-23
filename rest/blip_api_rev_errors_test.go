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
	"encoding/base64"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProcessRevErrorStatus reproduces the failures a rev message can hit on the receiving peer, and the status each
// one returns to the sender. The sender sees only this status, so it decides whether the failure is permanent.
func TestProcessRevErrorStatus(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeySync, base.KeySyncMsg)

	const syncFn = `function(doc) {
		switch (doc.fail) {
		case "forbidden": throw({forbidden: "rejected"});
		case "unauthorized": throw({unauthorized: "rejected"});
		case "exception": throw({other: "not a forbidden or unauthorized rejection"});
		case "timeout": while (true) {}
		case "invalidAccess": access("invalid:name", "chan"); break;
		case "invalidChannel": channel("a,b"); break;
		case "invalidRole": role("alice", "noprefix"); break;
		}
	}`
	testCases := []struct {
		name       string
		rev        string // defaults to 1-abc
		body       string
		properties blip.Properties
		protocols  []string // defaults to the BlipTester default
		// beforeConnect runs before the BlipTester connects, for options read when the replication starts
		beforeConnect func(rt *RestTester)
		setup         func(rt *RestTester, bt *BlipTester)
		eeOnly        bool
		status        string
		reason        string // substring of the error message
	}{
		{
			name:   "syncFnException",
			body:   `{"fail":"exception"}`,
			status: "500",
			reason: "Exception in JS sync function",
		},
		{
			name:   "syncFnTimeout",
			body:   `{"fail":"timeout"}`,
			status: "500",
			reason: "JS sync function timed out",
		},
		{
			name:   "syncFnInvalidAccess",
			body:   `{"fail":"invalidAccess"}`,
			status: "500",
			reason: "Error in JS sync function",
		},
		{
			// the sender has no getAttachment handler, so the attachment can't be fetched
			// TODO: CBG-6046 return the peer's status rather than 500
			name:   "attachmentFetchFailed",
			body:   `{"_attachments":{"att":{"stub":true,"digest":"sha1-wzp8ZyykdEuZ9GuqmxQ7XDrY7Co=","length":11,"revpos":1}}}`,
			status: "500",
			reason: "from getAttachment",
		},
		{
			// the parse error is wrapped, so it maps to 500 rather than the 400 an unwrapped JSON syntax error gets
			name:   "invalidJSON",
			body:   `{`,
			status: "500",
			reason: "Unable to unmarshal document body",
		},
		{
			name:   "syncFnForbidden",
			body:   `{"fail":"forbidden"}`,
			status: "403",
			reason: "rejected",
		},
		{
			name:   "syncFnUnauthorized",
			body:   `{"fail":"unauthorized"}`,
			status: "401",
			reason: "rejected",
		},
		{
			name:       "conflict",
			rev:        "1-def",
			body:       `{}`,
			properties: blip.Properties{db.RevMessageNoConflicts: "true"},
			setup: func(_ *RestTester, bt *BlipTester) {
				_, _, _, err := bt.SendRev("doc", "1-abc", []byte(`{}`), blip.Properties{})
				require.NoError(bt.TB(), err)
			},
			status: "409",
			reason: "conflict",
		},
		{
			name: "rejectWritesSlowMode",
			body: `{}`,
			setup: func(rt *RestTester, _ *BlipTester) {
				database := rt.GetDatabase()
				database.Options.UnsupportedOptions.RejectWritesWithSkippedSequences = true
				database.BroadcastSlowMode.Store(true)
			},
			status: "503",
			reason: "Database cache is behind",
		},
		// TODO: CBG-6046 add temporary KV errors, such as gocb.ErrOverload and gocb.ErrCircuitBreakerOpen, which should return 503
		{
			name:       "deltaSrcMissing",
			body:       `{}`,
			properties: blip.Properties{db.RevMessageDeltaSrc: "1-missing"},
			eeOnly:     true,
			status:     "422",
			reason:     "Can't fetch doc",
		},
		{
			name:   "invalidExpiry",
			body:   `{"_exp":"not a time"}`,
			status: "400",
			reason: "Invalid expiry",
		},
		{
			name:   "reservedProperty",
			body:   `{"_sync":"not allowed"}`,
			status: "404",
			reason: "is a reserved internal property",
		},
		{
			name:   "attachmentTooLarge",
			body:   `{"_attachments":{"att":{"data":"` + base64.StdEncoding.EncodeToString(make([]byte, 20*1024*1024+1)) + `"}}}`,
			status: "413",
			reason: "Attachment too large",
		},
		{
			name: "guestReadOnly",
			body: `{}`,
			beforeConnect: func(rt *RestTester) {
				rt.GetDatabase().Options.UnsupportedOptions.GuestReadOnly = true
			},
			status: "403",
			reason: "Replication context is read-only",
		},
		{
			// the 400 for an illegal channel name is replaced by the sync function exception
			name:   "syncFnInvalidChannel",
			body:   `{"fail":"invalidChannel"}`,
			status: "500",
			reason: "Exception in JS sync function",
		},
		{
			name:   "syncFnInvalidRole",
			body:   `{"fail":"invalidRole"}`,
			status: "500",
			reason: "Exception in JS sync function",
		},
		{
			name:       "historyDuplicateRev",
			rev:        "2-abc",
			body:       `{}`,
			properties: blip.Properties{db.RevMessageHistory: "1-abc,1-abc"},
			status:     "500",
			reason:     "already contains rev",
		},
		{
			name:       "historyGenerationNotIncreasing",
			rev:        "2-abc",
			body:       `{}`,
			properties: blip.Properties{db.RevMessageHistory: "3-abc"},
			status:     "500",
			reason:     "is not a higher generation than its parent",
		},
		{
			// a v4 peer converts a legacy rev ID to a version, which needs a hex digest
			name:      "nonHexRevID",
			rev:       "1-xyz",
			body:      `{}`,
			protocols: []string{db.CBMobileReplicationV4.SubprotocolString()},
			status:    "500",
			reason:    `parsing "xyz`,
		},
		{
			name:   "attachmentInvalidBase64",
			body:   `{"_attachments":{"att":{"data":"!!!"}}}`,
			status: "500",
			reason: "illegal base64 data",
		},
		{
			name: "attachmentMissingLength",
			body: `{"_attachments":{"att":{"stub":true,"digest":"sha1-wzp8ZyykdEuZ9GuqmxQ7XDrY7Co=","revpos":1}}}`,
			setup: func(_ *RestTester, bt *BlipTester) {
				serveAttachment(bt, []byte("hello world"))
			},
			status: "500",
			reason: "no attachment length provided in meta",
		},
		{
			name: "attachmentInvalidLength",
			body: `{"_attachments":{"att":{"stub":true,"digest":"sha1-wzp8ZyykdEuZ9GuqmxQ7XDrY7Co=","length":"eleven","revpos":1}}}`,
			setup: func(_ *RestTester, bt *BlipTester) {
				serveAttachment(bt, []byte("hello world"))
			},
			status: "500",
			reason: "invalid attachment length",
		},
		{
			// the incoming attachment is not an object, which processRev type asserts without a check
			name:       "attachmentNotObjectPanic",
			rev:        "2-abc",
			body:       `{"_attachments":{"att":"not an object"}}`,
			properties: blip.Properties{db.RevMessageHistory: "1-abc"},
			setup: func(_ *RestTester, bt *BlipTester) {
				_, _, _, err := bt.SendRev("doc", "1-abc", []byte(`{}`), blip.Properties{})
				require.NoError(bt.TB(), err)
			},
			status: "500",
			reason: "Panic: interface conversion",
		},
		{
			name: "corruptSyncXattr",
			body: `{}`,
			setup: func(rt *RestTester, _ *BlipTester) {
				_, err := rt.GetSingleDataStore().WriteWithXattrs(rt.Context(), "doc", 0, 0, []byte(`{}`),
					map[string][]byte{base.SyncXattrName: []byte(`"invalid sync data"`)}, nil, nil)
				require.NoError(rt.TB(), err)
			},
			status: "500",
			reason: "Failed to UnmarshalWithXattrs",
		},
		{
			// metadata copied from another database can carry a sequence far ahead of this database's _sync:seq
			name:       "sequenceTooFarAhead",
			rev:        "2-abc",
			body:       `{}`,
			properties: blip.Properties{db.RevMessageHistory: "1-abc"},
			setup: func(rt *RestTester, bt *BlipTester) {
				_, _, _, err := bt.SendRev("doc", "1-abc", []byte(`{}`), blip.Properties{})
				require.NoError(bt.TB(), err)
				rt.SetDocumentSequence("doc", 2*db.MaxSequencesToRelease)
			},
			status: "500",
			reason: base.ErrMaxSequenceReleasedExceeded.Error(),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if testCase.eeOnly && !base.IsEnterpriseEdition() {
				t.Skip("delta sync is EE only")
			}
			rt := NewRestTester(t, &RestTesterConfig{
				GuestEnabled: true,
				SyncFn:       syncFn,
				DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
					JavascriptTimeoutSecs: new(uint32(1)),
					DeltaSync:             &DeltaSyncConfig{Enabled: new(true)},
				}},
			})
			defer rt.Close()
			if testCase.beforeConnect != nil {
				testCase.beforeConnect(rt)
			}
			bt := NewBlipTesterFromSpecWithRT(rt, &BlipTesterSpec{blipProtocols: testCase.protocols})
			defer bt.Close()
			if testCase.setup != nil {
				testCase.setup(rt, bt)
			}

			rev := testCase.rev
			if rev == "" {
				rev = "1-abc"
			}
			revRequest := bt.newRevMessage("doc", rev, []byte(testCase.body), testCase.properties)
			bt.Send(revRequest)
			resp := revRequest.Response()
			respBody, err := resp.Body()
			require.NoError(t, err)
			assert.Equal(t, testCase.status, resp.Properties[db.BlipErrorCode], "body: %s", respBody)
			assert.Contains(t, string(respBody), testCase.reason)
		})
	}
}

// serveAttachment answers every getAttachment request from the peer with data.
func serveAttachment(bt *BlipTester, data []byte) {
	bt.blipContext.HandlerForProfile[db.MessageGetAttachment] = func(msg *blip.Message) {
		msg.Response().SetBody(data)
	}
}
