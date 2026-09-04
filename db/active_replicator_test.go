/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
)

// TestBlipSyncErrorUserinfo ensures the websocket errors returned by blipSync contain no basic auth component.
func TestBlipSyncErrorUserinfo(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
	}{
		{
			name:     "no creds",
			username: "",
			password: "",
		},
		{
			name:     "username",
			username: "foo",
		},
		{
			name:     "user and password",
			username: "foo",
			password: "bar",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create a HTTP server to get past the initial HTTP request inside blipSync.
			// HTTP errors have basic auth components redacted by the Go stdlib anyway.
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			srvURL, err := url.Parse(srv.URL)
			require.NoError(t, err)

			if test.username != "" && test.password != "" {
				srvURL.User = url.UserPassword(test.username, test.password)
			} else if test.username != "" {
				srvURL.User = url.User(test.username)
			}

			srvURL.Path = "/db1"
			t.Logf("srvURL: %v", srvURL.String())

			_, blipContext, err := NewSGBlipContext(base.TestCtx(t), t.Name(), nil, nil)
			require.NoError(t, err)

			_, err = blipSync(base.GetHttpClientForWebSocket(false), *srvURL, blipContext)
			require.Error(t, err)
			t.Logf("error: %v", err)
			if targetPassword, hasPassword := srvURL.User.Password(); hasPassword {
				assert.NotContains(t, err.Error(), targetPassword)
			}
		})
	}
}

// blipSyncTimeout is the response header timeout the stalled-remote tests run blipSync with, short enough
// to keep them in the fast suite.
const blipSyncTimeout = 500 * time.Millisecond

// TestBlipSyncStalledRemote is the regression test for CBG-5747: a remote that accepts the connection and
// then never responds used to block ISGR forever.  Both stages of the connect are covered - the pre-flight
// GET and the websocket upgrade - because they fail differently.  websocket.Dial takes no context and
// derives its handshake deadline from HTTPClient.Timeout, which is 0 for us; the upgrade is an ordinary HTTP
// request until the 101, so the transport's ResponseHeaderTimeout is what bounds it.
func TestBlipSyncStalledRemote(t *testing.T) {
	tests := []struct {
		name string
		// target returns the URL to point blipSync at, for a remote that stalls at the named stage
		target func(t *testing.T) url.URL
	}{
		{
			name: "pre-flight GET",
			target: func(t *testing.T) url.URL {
				listener := sgtest.NewStallingListener(t)
				t.Cleanup(func() {
					listener.RequireAcceptedConnections(t, 1)
					// giving up has to hang up too, or every failed connect leaks a connection
					listener.RequireClosedConnections(t, 1)
				})
				return url.URL{Scheme: "http", Host: listener.Addr(), Path: "/db1"}
			},
		},
		{
			name: "websocket upgrade",
			target: func(t *testing.T) url.URL {
				// answers the pre-flight GET so blipSync gets as far as the upgrade, then stalls there
				stall := make(chan struct{})
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if !strings.Contains(r.URL.Path, "_blipsync") {
						w.WriteHeader(http.StatusOK)
						return
					}
					select {
					case <-stall:
					case <-r.Context().Done(): // the client gave up and hung up
					}
				}))
				// cleanups run last registered first, so the handler is released before Close waits on it
				t.Cleanup(srv.Close)
				t.Cleanup(func() { close(stall) })

				srvURL, err := url.Parse(srv.URL)
				require.NoError(t, err)
				srvURL.Path = "/db1"
				return *srvURL
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := test.target(t)

			_, blipContext, err := NewSGBlipContext(base.TestCtx(t), t.Name(), nil, nil)
			require.NoError(t, err)

			httpClient := base.GetHttpClientForWebSocket(false)
			require.Equal(t, base.DefaultHttpResponseHeaderTimeout, httpClient.Transport.(*http.Transport).ResponseHeaderTimeout)
			// each call returns its own client, so shorten this one rather than waiting out the default
			httpClient.Transport.(*http.Transport).ResponseHeaderTimeout = blipSyncTimeout

			// run in a goroutine so a regression reports as this test failing, rather than hanging the package
			errs := make(chan error, 1)
			go func() {
				_, err := blipSync(httpClient, target, blipContext)
				errs <- err
			}()
			select {
			case err := <-errs:
				require.Error(t, err, "expected the stalled remote to be given up on")
				assert.ErrorContains(t, err, "timeout awaiting response headers")
			case <-time.After(30 * time.Second):
				t.Errorf("blipSync did not give up on a stalled remote - expected it to time out after %s", blipSyncTimeout)
			}
		})
	}
}
