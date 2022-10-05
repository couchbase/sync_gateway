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
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

const (
	// offset for standard port numbers to avoid conflicts 4984 -> 14984
	BootstrapTestPortOffset = 10000
)

func BootstrapStartupConfigForTest(t *testing.T) StartupConfig {
	config := DefaultStartupConfig("")

	config.Logging.Console = &base.ConsoleLoggerConfig{
		LogLevel: base.ConsoleLogLevel(),
		LogKeys:  base.ConsoleLogKey().EnabledLogKeys(),
	}

	config.API.AdminInterfaceAuthentication = base.BoolPtr(false)

	config.API.PublicInterface = "127.0.0.1:" + strconv.FormatInt(4984+BootstrapTestPortOffset, 10)
	config.API.AdminInterface = "127.0.0.1:" + strconv.FormatInt(4985+BootstrapTestPortOffset, 10)
	config.API.MetricsInterface = "127.0.0.1:" + strconv.FormatInt(4986+BootstrapTestPortOffset, 10)

	config.Bootstrap.Server = base.UnitTestUrl()
	config.Bootstrap.Username = base.TestClusterUsername()
	config.Bootstrap.Password = base.TestClusterPassword()
	config.Bootstrap.ServerTLSSkipVerify = base.BoolPtr(base.TestTLSSkipVerify())
	config.Bootstrap.UseTLSServer = base.BoolPtr(base.ServerIsTLS(base.UnitTestUrl()))

	// avoid loading existing configs by choosing a non-default config group
	if !base.IsEnterpriseEdition() {
		t.Skipf("EE-ONLY: Skipping test %s due to requiring non-default Config Group ID", t.Name())
	}
	config.Bootstrap.ConfigGroupID = t.Name()

	return config
}

const (
	publicPort  = 4984
	adminPort   = 4985
	metricsPort = 4986
)

func bootstrapURL(basePort int) string {
	return "http://localhost:" + strconv.Itoa(basePort+BootstrapTestPortOffset)
}

type bootstrapAdminResponse struct {
	StatusCode int
	Body       string
	Header     http.Header
	t          *testing.T
}

func (r *bootstrapAdminResponse) RequireStatus(status int) {
	require.Equal(r.t, status, r.StatusCode, "unexpected status code - body: %s", r.Body)
}

func (r *bootstrapAdminResponse) RequireResponse(status int, body string) {
	require.Equal(r.t, status, r.StatusCode, "unexpected status code - body: %s", r.Body)
	require.Equal(r.t, body, r.Body, "unexpected body")
}

func BootstrapAdminRequest(t *testing.T, method, path, body string) bootstrapAdminResponse {
	return doBootstrapAdminRequest(t, method, "", path, body, nil)
}

func BootstrapAdminRequestCustomHost(t *testing.T, method, host, path, body string) bootstrapAdminResponse {
	return doBootstrapAdminRequest(t, method, host, path, body, nil)
}

func BootstrapAdminRequestWithHeaders(t *testing.T, method, path, body string, headers map[string]string) bootstrapAdminResponse {
	return doBootstrapAdminRequest(t, method, "", path, body, headers)
}

func doBootstrapAdminRequest(t *testing.T, method, host, path, body string, headers map[string]string) bootstrapAdminResponse {
	if host == "" {
		host = "http://localhost:" + strconv.FormatInt(4985+BootstrapTestPortOffset, 10)
	}
	url := host + path

	buf := bytes.NewBufferString(body)
	req, err := http.NewRequest(method, url, buf)
	require.NoError(t, err)

	for headerName, headerVal := range headers {
		req.Header.Set(headerName, headerVal)
	}

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	rBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.NoError(t, resp.Body.Close())

	return bootstrapAdminResponse{
		t:          t,
		StatusCode: resp.StatusCode,
		Body:       string(rBody),
		Header:     resp.Header,
	}
}
