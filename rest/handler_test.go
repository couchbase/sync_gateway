/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseHTTPRangeHeader(t *testing.T) {
	// http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35
	type testcase struct {
		header        string
		contentLength uint64
		status        int
		start         uint64
		end           uint64
	}
	testcases := []testcase{
		// No Range: header at all:
		{"", 100, http.StatusOK, 0, 0},

		// Syntactically invalid Range headers are ignored:
		{"lolwut", 100, http.StatusOK, 0, 0},
		{"inches=-", 100, http.StatusOK, 0, 0},
		{"bytes=-", 100, http.StatusOK, 0, 0},
		{"bytes=50-bar", 100, http.StatusOK, 0, 0},
		{"bytes=50-49", 100, http.StatusOK, 0, 0},                  // invalid, not unsatisfiable
		{"bytes=99999999999999999999-1", 100, http.StatusOK, 0, 0}, // again, invalid

		// These requests return the entire document:
		{"bytes=0-", 100, http.StatusOK, 0, 0},
		{"bytes=0-99", 100, http.StatusOK, 0, 0},
		{"bytes=-100", 100, http.StatusOK, 0, 0},
		{"bytes=-99999999999999999999", 100, http.StatusOK, 0, 0},

		// Not satisfiable:
		{"bytes=100-", 100, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=100-200", 100, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=100-99999999999999999999", 100, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=-0", 100, http.StatusRequestedRangeNotSatisfiable, 0, 0},

		{"bytes=10-", 100, http.StatusPartialContent, 10, 99},
		{"bytes=-10", 100, http.StatusPartialContent, 90, 99},
		{"bytes=0-0", 100, http.StatusPartialContent, 0, 0},
		{"bytes=50-60", 100, http.StatusPartialContent, 50, 60},
		{"bytes=99-", 100, http.StatusPartialContent, 99, 99},
		{"bytes=99-200", 100, http.StatusPartialContent, 99, 99},
		{"bytes=90-200", 100, http.StatusPartialContent, 90, 99},
		{"bytes=90-99999999999999999999", 100, http.StatusPartialContent, 90, 99},
		{"bytes=2-98", 100, http.StatusPartialContent, 2, 98},

		// Test with empty content:
		{"bytes=-1", 0, http.StatusOK, 0, 0},
		{"bytes=-10", 0, http.StatusOK, 0, 0},
		{"bytes=0-0", 0, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=0-49", 0, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=1-1", 0, http.StatusRequestedRangeNotSatisfiable, 0, 0},
		{"bytes=-0", 0, http.StatusRequestedRangeNotSatisfiable, 0, 0},
	}

	for _, expected := range testcases {
		status, start, end := parseHTTPRangeHeader(expected.header, expected.contentLength)
		t.Logf("*** Range: %s  --> %d %d-%d", expected.header, status, start, end)
		assert.Equal(t, expected.status, status)
		if status == http.StatusPartialContent {
			assert.Equal(t, expected.start, start)
			assert.Equal(t, expected.end, end)
		}
	}
}

func Test_parseKeyspace(t *testing.T) {
	tests := []struct {
		ks             string
		wantDb         string
		wantScope      *string
		wantCollection *string
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			ks:             "db",
			wantDb:         "db",
			wantScope:      nil,
			wantCollection: nil,
			wantErr:        assert.NoError,
		},
		{
			ks:             "d.c",
			wantDb:         "d",
			wantScope:      nil,
			wantCollection: base.Ptr("c"),
			wantErr:        assert.NoError,
		},
		{
			ks:             "d.s.c",
			wantDb:         "d",
			wantScope:      base.Ptr("s"),
			wantCollection: base.Ptr("c"),
			wantErr:        assert.NoError,
		},
		{
			ks:      "",
			wantErr: assert.Error,
		},
		{
			ks:      "d.s.c.z",
			wantErr: assert.Error,
		},
		{
			ks:      ".s.",
			wantErr: assert.Error,
		},
		{
			ks:      "d..c",
			wantErr: assert.Error,
		},
		{
			ks:      "d.",
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.ks, func(t *testing.T) {
			gotDb, gotScope, gotCollection, err := ParseKeyspace(tt.ks)
			if !tt.wantErr(t, err, fmt.Sprintf("ParseKeyspace(%v)", tt.ks)) {
				return
			}
			assert.Equalf(t, tt.wantDb, gotDb, "ParseKeyspace(%v)", tt.ks)
			assert.Equalf(t, tt.wantScope, gotScope, "ParseKeyspace(%v)", tt.ks)
			assert.Equalf(t, tt.wantCollection, gotCollection, "ParseKeyspace(%v)", tt.ks)
		})
	}
}

func Benchmark_parseKeyspace(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, _, _, _ = ParseKeyspace("d.s.c")
	}
}

func TestShouldCheckAdminRBAC(t *testing.T) {
	for _, requireInterfaceAuth := range []bool{false, true} {
		t.Run(fmt.Sprintf("requireInterfaceAuth=%t", requireInterfaceAuth), func(t *testing.T) {
			config := BootstrapStartupConfigForTest(t)
			config.API.AdminInterfaceAuthentication = base.Ptr(requireInterfaceAuth)
			config.API.MetricsInterfaceAuthentication = base.Ptr(requireInterfaceAuth)
			sc, closeFn := StartServerWithConfig(t, &config)
			defer closeFn()

			for _, sgcollectable := range []bool{true, false} {
				t.Run(fmt.Sprintf("sgcollectable=%t", sgcollectable), func(t *testing.T) {
					// make sure assertion counts are correct
					require.Equal(t, int64(0), base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().AssertionFailCount.Value())
					defer func() {
						base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().AssertionFailCount.Set(0)
					}()
					adminHandler := newHandler(sc, adminPrivs, adminServer, httptest.NewRecorder(), &http.Request{}, handlerOptions{sgcollect: sgcollectable})
					metricsHandler := newHandler(sc, metricsPrivs, metricsServer, httptest.NewRecorder(), &http.Request{}, handlerOptions{sgcollect: sgcollectable})
					if requireInterfaceAuth {
						require.True(t, adminHandler.shouldCheckAdminRBAC())
						require.True(t, metricsHandler.shouldCheckAdminRBAC())
					} else {
						require.False(t, adminHandler.shouldCheckAdminRBAC())

						require.False(t, metricsHandler.shouldCheckAdminRBAC())
					}
					invalidAuthHeader := http.Header{}
					invalidAuthHeader.Add("Authorization", "SGCollect invalid")
					// with invalid sgcollect token
					adminHandler = newHandler(sc, adminPrivs, adminServer, httptest.NewRecorder(), &http.Request{Header: invalidAuthHeader}, handlerOptions{sgcollect: sgcollectable})
					metricsHandler = newHandler(sc, metricsPrivs, metricsServer, httptest.NewRecorder(), &http.Request{Header: invalidAuthHeader}, handlerOptions{sgcollect: sgcollectable})
					if requireInterfaceAuth {
						if base.IsDevMode() && !sgcollectable {
							require.PanicsWithValue(t, base.AssertionFailedPrefix+sgcollectTokenInvalidRequest, func() { adminHandler.shouldCheckAdminRBAC() })
						} else {
							require.True(t, adminHandler.shouldCheckAdminRBAC(), "expected invalid token to still require auth")
						}
						require.True(t, metricsHandler.shouldCheckAdminRBAC(), "expected invalid token to still require auth")
					} else {
						require.False(t, adminHandler.shouldCheckAdminRBAC())
						require.False(t, metricsHandler.shouldCheckAdminRBAC())

					}
					// with valid sgcollect token, but sgcollect on the handler is disabled
					require.NoError(t, sc.SGCollect.createNewToken())

					validAuthHeader := http.Header{}
					validAuthHeader.Add("Authorization", fmt.Sprintf("SGCollect %s", sc.SGCollect.Token))
					adminHandler = newHandler(sc, adminPrivs, adminServer, httptest.NewRecorder(), &http.Request{Header: validAuthHeader}, handlerOptions{sgcollect: false})
					metricsHandler = newHandler(sc, metricsPrivs, metricsServer, httptest.NewRecorder(), &http.Request{Header: validAuthHeader}, handlerOptions{sgcollect: false})
					if requireInterfaceAuth {
						if base.IsDevMode() {
							require.PanicsWithValue(t, base.AssertionFailedPrefix+sgcollectTokenInvalidRequest, func() { adminHandler.shouldCheckAdminRBAC() })
						} else {
							require.True(t, adminHandler.shouldCheckAdminRBAC(), "expected invalid token to still require auth")
						}
						require.True(t, metricsHandler.shouldCheckAdminRBAC(), "expected invalid token to still require auth")
					} else {
						require.False(t, adminHandler.shouldCheckAdminRBAC())

						require.False(t, metricsHandler.shouldCheckAdminRBAC())
					}

				})
			}
		})
	}
}

func TestHandlerRecoverLog(t *testing.T) {
	testCases := []struct {
		name       string
		panicArg   any
		shouldWarn bool
	}{
		{name: "string panic", panicArg: "test panic", shouldWarn: true},
		{name: "error panic", panicArg: fmt.Errorf("test error panic"), shouldWarn: true},
		{name: "ErrAbortHandler", panicArg: http.ErrAbortHandler, shouldWarn: false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := BootstrapStartupConfigForTest(t) // share config between both servers in test to share a groupID
			sc, closeFn := StartServerWithConfig(t, &config)
			defer closeFn()

			ctx := base.TestCtx(t)
			require.NoError(t, sc.WaitForRESTAPIs(ctx))
			startWarnCount := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value()
			handler := makeHandlerWithOptions(sc, regularPrivs, nil, nil, func(_ *handler) error {
				panic(tc.panicArg)
			}, handlerOptions{})
			r := mux.NewRouter()
			r.Use(withServerType(publicServer))
			r.Handle("/", handler)
			server := httptest.NewServer(r)
			defer server.Close()

			client := server.Client()
			resp, err := client.Get(server.URL)
			if tc.shouldWarn {
				require.ErrorIs(t, err, io.EOF)
				require.Equal(t, startWarnCount+1, base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value())
			} else {
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				require.Equal(t, startWarnCount, base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().WarnCount.Value())
			}
		})
	}
}
