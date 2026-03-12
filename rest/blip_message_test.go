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

// TestBlipMessageProfiles verifies that BLIP profiles are handled correctly.
func TestBlipMessageProfiles(t *testing.T) {
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

		t.Run("PING reply", func(t *testing.T) {
			pingRequest := blip.NewRequest()
			pingRequest.SetProfile(db.MessagePing)

			base.AssertLogNotContains(t, "404 Unknown profile", func() {
				btc.pullReplication.sendMsg(pingRequest)
			})

			resp := pingRequest.Response()
			require.NotNil(t, resp)
			assert.Equal(t, blip.ResponseType, resp.Type())

			body, err := resp.Body()
			require.NoError(t, err)
			assert.Empty(t, body)
		})

		t.Run("PING noreply", func(t *testing.T) {
			pingRequest := blip.NewRequest()
			pingRequest.SetProfile(db.MessagePing)
			pingRequest.SetNoReply(true)

			base.AssertLogNotContains(t, "404 Unknown profile", func() {
				btc.pullReplication.sendMsg(pingRequest)
			})

			// With noreply=true, Response() should return nil
			resp := pingRequest.Response()
			assert.Nil(t, resp)
		})

		t.Run("unknown profile", func(t *testing.T) {
			unknownRequest := blip.NewRequest()
			unknownRequest.SetProfile("foo")

			btc.pullReplication.sendMsg(unknownRequest)
			resp := unknownRequest.Response()
			require.NotNil(t, resp)
			assert.Equal(t, blip.ErrorType, resp.Type())
			assert.Equal(t, "404", resp.Properties[db.BlipErrorCode])
		})

		t.Run("unknown profile noreply", func(t *testing.T) {
			unknownRequest := blip.NewRequest()
			unknownRequest.SetProfile("foo")
			unknownRequest.SetNoReply(true)

			// Even with noreply, the server should handle the message
			// but won't send a response back
			btc.pullReplication.sendMsg(unknownRequest)
			resp := unknownRequest.Response()
			assert.Nil(t, resp)
		})
	})
}
