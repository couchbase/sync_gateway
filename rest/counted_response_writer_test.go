package rest

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCountedResponseWriter(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	const alice = "alice"
	rt.CreateUser(alice, nil)

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/", "")
	RequireStatus(t, resp, http.StatusOK)

	stats := rt.GetDatabase().DbStats.Database()
	require.Equal(t, int64(0), stats.HTTPBytesWritten.Value())

	resp = rt.SendUserRequest(http.MethodGet, "/{{.db}}/", "", alice)
	RequireStatus(t, resp, http.StatusOK)
	require.Greater(t, stats.HTTPBytesWritten.Value(), int64(0))
	stats.HTTPBytesWritten.Set(0)

	resp = rt.SendUserRequest(http.MethodGet, "/{{.db}}/", "", "")
	RequireStatus(t, resp, http.StatusOK)
	require.Greater(t, stats.HTTPBytesWritten.Value(), int64(0))

}
