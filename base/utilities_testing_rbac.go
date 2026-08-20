// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// MakeUser creates a Couchbase Server RBAC user via the management REST API,
// retrying on transient errors. The caller must have cluster admin credentials.
func MakeUser(t *testing.T, httpClient *http.Client, serverURL, username, password string, roles []string) {
	form := url.Values{}
	form.Add("password", password)
	form.Add("roles", strings.Join(roles, ","))

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/settings/rbac/users/local/%s", serverURL, username), strings.NewReader(form.Encode()))
		if !assert.NoError(c, err) {
			return
		}

		req.SetBasicAuth(TestClusterUsername(), TestClusterPassword())
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		resp, err := httpClient.Do(req)
		if !assert.NoError(c, err) {
			return
		}
		defer func() { assert.NoError(c, resp.Body.Close()) }()
		var bodyResp []byte
		if resp.StatusCode != http.StatusOK {
			bodyResp, err = io.ReadAll(resp.Body)
			assert.NoError(c, err)
		}
		assert.Equalf(c, http.StatusOK, resp.StatusCode, "Failed to create user: %s", bodyResp)
	}, time.Second, 100*time.Millisecond)
}

// DeleteUser removes a Couchbase Server RBAC user via the management REST API.
func DeleteUser(t *testing.T, httpClient *http.Client, serverURL, username string) {
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/settings/rbac/users/local/%s", serverURL, username), nil)
		if !assert.NoError(c, err) {
			return
		}

		req.SetBasicAuth(TestClusterUsername(), TestClusterPassword())

		resp, err := httpClient.Do(req)
		if !assert.NoError(c, err) {
			return
		}
		assert.NoError(c, resp.Body.Close())
		assert.Equal(c, http.StatusOK, resp.StatusCode)
	}, time.Second, 100*time.Millisecond)
}
