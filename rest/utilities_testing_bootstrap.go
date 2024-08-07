// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// BootstrapStartupConfigForTest returns a default config for use to start a Sync Gateway server. It will run APIs on randomly chosen ports.
func BootstrapStartupConfigForTest(t *testing.T) StartupConfig {
	config := DefaultStartupConfig("")

	config.Logging.Console = &base.ConsoleLoggerConfig{
		LogLevel: base.ConsoleLogLevel(),
		LogKeys:  base.ConsoleLogKey().EnabledLogKeys(),
	}

	config.API.AdminInterfaceAuthentication = base.BoolPtr(false)

	randomPort := "127.0.0.1:0"

	config.API.PublicInterface = randomPort
	config.API.AdminInterface = randomPort
	config.API.MetricsInterface = randomPort
	config.Unsupported.DiagnosticInterface = randomPort

	config.Bootstrap.Server = base.UnitTestUrl()
	config.Bootstrap.Username = base.TestClusterUsername()
	config.Bootstrap.Password = base.TestClusterPassword()
	config.Bootstrap.ServerTLSSkipVerify = base.BoolPtr(base.TestTLSSkipVerify())
	config.Bootstrap.UseTLSServer = base.BoolPtr(base.ServerIsTLS(base.UnitTestUrl()))

	uniqueUUID, err := uuid.NewRandom()
	require.NoError(t, err)

	if base.IsEnterpriseEdition() {
		config.Bootstrap.ConfigGroupID = uniqueUUID.String()
	} else if !base.UnitTestUrlIsWalrus() {
		t.Skip("BootstrapStartupConfigForTest requires EE support, since the config files can be read by future tests in the bucket pool")
	}

	return config
}

type boostrapResponse struct {
	Body       string
	Header     http.Header
	t          *testing.T
	StatusCode int
}

func (r *boostrapResponse) AssertStatus(status int) {
	assert.Equal(r.t, status, r.StatusCode, "unexpected status code - body: %s", r.Body)
}

func (r *boostrapResponse) RequireStatus(status int) {
	require.Equal(r.t, status, r.StatusCode, "unexpected status code - body: %s", r.Body)
}

func (r *boostrapResponse) AssertResponse(status int, body string) {
	assert.Equal(r.t, status, r.StatusCode, "unexpected status code - body: %s", r.Body)
	assert.Equal(r.t, body, r.Body, "unexpected body")
}

func (r *boostrapResponse) RequireResponse(status int, body string) {
	require.Equal(r.t, status, r.StatusCode, "unexpected status code - body: %s", r.Body)
	require.Equal(r.t, body, r.Body, "unexpected body")
}

func (r *boostrapResponse) Unmarshal(v interface{}) {
	err := base.JSONUnmarshal([]byte(r.Body), &v)
	require.NoError(r.t, err, "Error unmarshalling bootstrap response body")
}

func BootstrapAdminRequest(t *testing.T, sc *ServerContext, method, path, body string) boostrapResponse {
	return doBootstrapAdminRequest(t, sc, method, path, body, nil)
}

func BootstrapAdminRequestWithHeaders(t *testing.T, sc *ServerContext, method, path, body string, headers map[string]string) boostrapResponse {
	return doBootstrapAdminRequest(t, sc, method, path, body, headers)
}

func mustGetServerAddr(t *testing.T, sc *ServerContext, s serverType) string {
	addr, err := sc.getServerAddr(s)
	require.NoError(t, err, "Server %s not found in server context", s)
	return addr
}

func doBootstrapAdminRequest(t *testing.T, sc *ServerContext, method, path, body string, headers map[string]string) boostrapResponse {
	host := "http://" + mustGetServerAddr(t, sc, adminServer)
	url := host + path

	buf := bytes.NewBufferString(body)
	req, err := http.NewRequest(method, url, buf)
	require.NoError(t, err)

	for headerName, headerVal := range headers {
		req.Header.Set(headerName, headerVal)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, resp.Body.Close())
	}()

	rBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return boostrapResponse{
		t:          t,
		StatusCode: resp.StatusCode,
		Body:       string(rBody),
		Header:     resp.Header,
	}
}

func doBootstrapRequest(t *testing.T, sc *ServerContext, method, path, body string, headers map[string]string, server serverType) boostrapResponse {
	host := "http://" + mustGetServerAddr(t, sc, server)
	url := host + path

	buf := bytes.NewBufferString(body)
	req, err := http.NewRequest(method, url, buf)
	require.NoError(t, err)

	for headerName, headerVal := range headers {
		req.Header.Set(headerName, headerVal)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer func() {
		assert.NoError(t, resp.Body.Close())
	}()

	rBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return boostrapResponse{
		t:          t,
		StatusCode: resp.StatusCode,
		Body:       string(rBody),
		Header:     resp.Header,
	}
}

// StartBootstrapServer starts a server with a default bootstrap config, and returns a function to close the server. This differs from RestTester in that this is running a real server listening on random port. Prefer use of RestTester for more ergnomic APIs.
func StartBootstrapServer(t *testing.T) (*ServerContext, func()) {
	return StartBootstrapServerWithGroupID(t, nil)
}

// StartBootstrapServerWithGroupID starts a server with a bootstrap config, and returns a function to close the server. This differs from RestTester in that this is running a real server listening on random port. Prefer use of random groupID if appropriate. Prefer use of RestTester for more ergonomic APIs.
func StartBootstrapServerWithGroupID(t *testing.T, groupID *string) (*ServerContext, func()) {
	config := BootstrapStartupConfigForTest(t)
	if groupID != nil {
		config.Bootstrap.ConfigGroupID = *groupID
	}
	return StartServerWithConfig(t, &config)

}

// StartServerWithConfig starts a server from given config, and returns a function to close the server. Prefer use of RestTester for more ergonomic APIs.
func StartServerWithConfig(t *testing.T, config *StartupConfig) (*ServerContext, func()) {
	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, config, true)
	require.NoError(t, err)

	serverErr := make(chan error)

	closeFn := func() {
		sc.Close(ctx)
		assert.NoError(t, <-serverErr)
	}

	started := false
	defer func() {
		if !started {
			closeFn()
		}

	}()
	go func() {
		serverErr <- StartServer(ctx, config, sc)
	}()

	require.NoError(t, sc.WaitForRESTAPIs(ctx))
	started = true
	return sc, closeFn
}
