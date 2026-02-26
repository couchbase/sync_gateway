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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MakeUser creates a Couchbase Server RBAC user via the management REST API,
// retrying on transient errors. The caller must have cluster admin credentials.
func MakeUser(t *testing.T, httpClient *http.Client, serverURL, username, password string, roles []string) {
	form := url.Values{}
	form.Add("password", password)
	form.Add("roles", strings.Join(roles, ","))

	retryWorker := func() (shouldRetry bool, err error, value any) {
		req, err := http.NewRequest(http.MethodPut, fmt.Sprintf("%s/settings/rbac/users/local/%s", serverURL, username), strings.NewReader(form.Encode()))
		require.NoError(t, err)

		req.SetBasicAuth(TestClusterUsername(), TestClusterPassword())
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		resp, err := httpClient.Do(req)
		if err != nil {
			return true, err, nil
		}
		defer func() { assert.NoError(t, resp.Body.Close()) }()
		var bodyResp []byte
		if resp.StatusCode != http.StatusOK {
			bodyResp, err = io.ReadAll(resp.Body)
			require.NoError(t, err, "Failed to create user: %s", bodyResp)
		}
		require.Equalf(t, http.StatusOK, resp.StatusCode, "Failed to create user: %s", bodyResp)
		return false, err, nil
	}

	err, _ := RetryLoop(TestCtx(t), "MakeUser", retryWorker, CreateSleeperFunc(10, 100))
	require.NoError(t, err)
}

// DeleteUser removes a Couchbase Server RBAC user via the management REST API.
func DeleteUser(t *testing.T, httpClient *http.Client, serverURL, username string) {
	retryWorker := func() (shouldRetry bool, err error, value *http.Response) {
		req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/settings/rbac/users/local/%s", serverURL, username), nil)
		require.NoError(t, err)

		req.SetBasicAuth(TestClusterUsername(), TestClusterPassword())

		resp, err := httpClient.Do(req)
		if err != nil {
			return true, err, resp
		}
		assert.NoError(t, resp.Body.Close())
		return false, err, resp
	}

	err, resp := RetryLoop(TestCtx(t), "DeleteUser", retryWorker, CreateSleeperFunc(10, 100))
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
}
