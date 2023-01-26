package db

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

			blipCtx, err := NewSGBlipContext(context.Background(), t.Name())
			require.NoError(t, err)
			_, err = blipSync(*srvURL, blipCtx, false)
			require.Error(t, err)
			t.Logf("error: %v", err)
			if targetPassword, hasPassword := srvURL.User.Password(); hasPassword {
				assert.NotContains(t, err.Error(), targetPassword)
			}
		})
	}
}
