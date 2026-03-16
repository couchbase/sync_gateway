// Copyright 2020-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlipMessagePingProfiles verifies that BLIP profiles used for heartbeats are handled correctly.
func TestBlipMessagePingProfiles(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeySync)

	rtConfig := &RestTesterConfig{
		GuestEnabled: true,
	}
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, rtConfig)
		defer rt.Close()

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer btc.Close()

		// CBL-js 1.0.1 heartbeat message type
		t.Run("PING noreply", func(t *testing.T) {
			pingRequest := blip.NewRequest()
			pingRequest.SetProfile(db.MessagePing)
			pingRequest.SetNoReply(true)

			// can't AssertLogNotContains since Response() returns immediately for noreply
			btc.pullReplication.sendMsg(pingRequest)
			resp := pingRequest.Response()
			assert.Nil(t, resp)
		})

		// with reply variant for testing
		t.Run("PING", func(t *testing.T) {
			pingRequest := blip.NewRequest()
			pingRequest.SetProfile(db.MessagePing)

			var resp *blip.Message
			base.AssertLogNotContains(t, "404 Unknown profile", func() {
				btc.pullReplication.sendMsg(pingRequest)
				resp = pingRequest.Response()
			})

			require.NotNil(t, resp)
			assert.Equal(t, blip.ResponseType, resp.Type())

			body, err := resp.Body()
			require.NoError(t, err)
			assert.Empty(t, body)
		})

		// test behavior of an unknown profile with noreply (i.e. CBL-js 1.0.1 on an older SG pre-PING support)
		t.Run("unknown profile noreply", func(t *testing.T) {
			unknownRequest := blip.NewRequest()
			unknownRequest.SetProfile("not_a_ping")
			unknownRequest.SetNoReply(true)

			// can't AssertLogContains since Response() returns immediately for noreply
			btc.pullReplication.sendMsg(unknownRequest)
			resp := unknownRequest.Response()
			assert.Nil(t, resp)
		})

		t.Run("unknown profile", func(t *testing.T) {
			unknownRequest := blip.NewRequest()
			unknownRequest.SetProfile("not_a_ping")

			var resp *blip.Message
			base.AssertLogContains(t, "404 Unknown profile", func() {
				btc.pullReplication.sendMsg(unknownRequest)
				resp = unknownRequest.Response()
			})

			require.NotNil(t, resp)
			assert.Equal(t, blip.ErrorType, resp.Type())
			assert.Equal(t, "404", resp.Properties[db.BlipErrorCode])
		})
	})
}
