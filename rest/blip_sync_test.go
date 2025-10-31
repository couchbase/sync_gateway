// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/coder/websocket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHostOnlyCORS(t *testing.T) {
	const unparseableURL = "1http:///example.com"
	testsCases := []struct {
		input    []string
		output   []string
		hasError bool
	}{
		{
			input:  []string{"http://example.com"},
			output: []string{"example.com"},
		},
		{
			input:  []string{"https://example.com", "http://example.com"},
			output: []string{"example.com", "example.com"},
		},
		{
			input:  []string{"*", "http://example.com"},
			output: []string{"*", "example.com"},
		},
		{
			input:  []string{"wss://example.com"},
			output: []string{"example.com"},
		},
		{
			input:  []string{"http://example.com:12345"},
			output: []string{"example.com:12345"},
		},
		{
			input:    []string{unparseableURL},
			output:   nil,
			hasError: true,
		},
		{
			input:    []string{"*", unparseableURL},
			output:   []string{"*"},
			hasError: true,
		},
		{
			input:    []string{"*", unparseableURL, "http://example.com"},
			output:   []string{"*", "example.com"},
			hasError: true,
		},
	}
	for _, test := range testsCases {
		t.Run(fmt.Sprintf("%v->%v", test.input, test.output), func(t *testing.T) {
			output, err := hostOnlyCORS(test.input)
			if test.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, test.output, output)
		})
	}
}

func TestOneTimeSessionBlipSyncAuthentication(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	const username = "alice"
	rt.CreateUser(username, []string{"*"})

	resp := rt.SendUserRequest(http.MethodPost, "/{{.db}}/_session", `{"one_time": true}`, username)
	RequireStatus(t, resp, http.StatusOK)

	var sessionResp struct {
		SessionID string `json:"one_time_session_id"`
	}

	require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &sessionResp))

	require.NotEmpty(t, sessionResp.SessionID, "Expected non-empty session ID for %s", resp.BodyString())

	resp = rt.SendRequestWithHeaders(http.MethodGet, "/{{.db}}/_blipsync", "", nil)
	RequireStatus(t, resp, http.StatusUnauthorized)

	resp = rt.SendUserRequest(http.MethodGet, "/{{.db}}/_blipsync", "", username)
	RequireStatus(t, resp, http.StatusUpgradeRequired)

	// no header should show database not found
	resp = rt.SendRequestWithHeaders(http.MethodGet, "/{{.db}}/_blipsync", "", nil)
	RequireStatus(t, resp, http.StatusUnauthorized)

	// invalid token is header not known
	resp = rt.SendRequestWithHeaders(http.MethodGet, "/{{.db}}/_blipsync", "", map[string]string{
		secWebSocketProtocolHeader: blipSessionIDPrefix + "badtoken",
	})
	RequireStatus(t, resp, http.StatusUnauthorized)

	// first request will succeed
	resp = rt.SendRequestWithHeaders(http.MethodGet, "/{{.db}}/_blipsync", "", map[string]string{
		secWebSocketProtocolHeader: blipSessionIDPrefix + sessionResp.SessionID,
	})
	RequireStatus(t, resp, http.StatusUpgradeRequired)

	// one time token is expired
	resp = rt.SendRequestWithHeaders(http.MethodGet, "/{{.db}}/_blipsync", "", map[string]string{
		secWebSocketProtocolHeader: blipSessionIDPrefix + sessionResp.SessionID,
	})
	RequireStatus(t, resp, http.StatusUnauthorized)

	srv := httptest.NewServer(rt.TestPublicHandler())
	defer srv.Close()

	// Construct URL to connect to blipsync target endpoint
	destURL := fmt.Sprintf("%s/%s/_blipsync", srv.URL, rt.GetDatabase().Name)
	u, err := url.Parse(destURL)
	require.NoError(t, err)
	u.Scheme = "ws"

	blipProtocolPrefix := "BLIP_3+"
	testCases := []struct {
		name      string
		protocols []string
		error     string
	}{
		{
			name:  "No Protocols",
			error: "expected handshake response status code 101 but got 500",
		},
		{
			name: "V4 Protocol",
			protocols: []string{
				blipProtocolPrefix + db.CBMobileReplicationV4.SubprotocolString(),
			},
		},
		{
			name: "V3 Protocol",
			protocols: []string{
				blipProtocolPrefix + db.CBMobileReplicationV3.SubprotocolString(),
			},
		},
		{
			name: "Multiple Protocols",
			protocols: []string{
				"some-other-protocol",
				blipProtocolPrefix + db.CBMobileReplicationV4.SubprotocolString(),
				blipProtocolPrefix + db.CBMobileReplicationV3.SubprotocolString(),
			},
		},
	}
	for _, tc := range testCases {
		rt.Run(tc.name, func(t *testing.T) {
			// Create new one-time session for each sub-test
			resp := rt.SendUserRequest(http.MethodPost, "/{{.db}}/_session", `{"one_time": true}`, username)
			RequireStatus(t, resp, http.StatusOK)

			var sessionResp struct {
				SessionID string `json:"one_time_session_id"`
			}

			require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &sessionResp))
			require.NotEmpty(t, sessionResp.SessionID, "Expected non-empty session ID for %s", resp.BodyString())

			protocols := []string{blipSessionIDPrefix + sessionResp.SessionID}
			protocols = append(protocols, tc.protocols...)
			ctx := rt.Context()
			ws, _, err := websocket.Dial(ctx, destURL, &websocket.DialOptions{Subprotocols: protocols})
			if tc.error != "" {
				require.ErrorContains(t, err, tc.error)
				return
			}
			require.NoError(t, err)
			require.NoError(t, ws.Close(websocket.StatusNormalClosure, "test complete"))
		})
	}

}
