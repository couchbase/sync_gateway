// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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
