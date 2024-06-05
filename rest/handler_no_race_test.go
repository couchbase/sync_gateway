// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build !race

package rest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

// AssertLogContains can hit the race detector due to swapping the global loggers
func TestHTTPLoggingRedaction(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)
	base.LongRunningTest(t)
	rt := NewRestTester(t, nil)
	defer rt.Close()

	keyspace := rt.GetSingleKeyspace()

	cases := []struct {
		name, method, path, expectedLog string
		admin                           bool
	}{
		{
			name:        "docid",
			method:      http.MethodGet,
			path:        "/db/test",
			expectedLog: "/db/<ud>test</ud>",
		},
		{
			name:        "local",
			method:      http.MethodGet,
			path:        "/db/_local/test",
			expectedLog: "/db/_local/<ud>test</ud>",
		},
		{
			name:        "raw-docid",
			method:      http.MethodGet,
			path:        fmt.Sprintf("/%s/_raw/test", keyspace),
			expectedLog: fmt.Sprintf("/%s/_raw/<ud>test</ud>", keyspace),
			admin:       true,
		},
		{
			name:        "revtree-docid",
			method:      http.MethodGet,
			path:        fmt.Sprintf("/%s/_revtree/test", keyspace),
			expectedLog: fmt.Sprintf("/%s/_revtree/<ud>test</ud>", keyspace),
			admin:       true,
		},
		{
			name:        "docid-attach",
			method:      http.MethodGet,
			path:        fmt.Sprintf("/%s/test/attach", keyspace),
			expectedLog: fmt.Sprintf("/%s/<ud>test</ud>/<ud>attach</ud>", keyspace),
		},
		{
			name:        "docid-attach-equalnames",
			method:      http.MethodGet,
			path:        fmt.Sprintf("/%s/test/test", keyspace),
			expectedLog: fmt.Sprintf("/%s/<ud>test</ud>/<ud>test</ud>", keyspace),
		},
		{
			name:        "user",
			method:      http.MethodGet,
			path:        "/db/_user/foo",
			expectedLog: "/db/_user/<ud>foo</ud>",
			admin:       true,
		},
		{
			name:        "userSession",
			method:      http.MethodDelete,
			path:        "/db/_user/foo/_session",
			expectedLog: "/db/_user/<ud>foo</ud>/_session",
			admin:       true,
		},
		{
			name:        "role",
			method:      http.MethodGet,
			path:        "/db/_role/foo",
			expectedLog: "/db/_role/<ud>foo</ud>",
			admin:       true,
		},
		{
			name:        "CBG-2059",
			method:      http.MethodGet,
			path:        fmt.Sprintf("/%s/db", keyspace),
			expectedLog: fmt.Sprintf("/%s/<ud>db</ud>", keyspace),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base.AssertLogContains(t, tc.expectedLog, func() {
				if tc.admin {
					_ = rt.SendAdminRequest(tc.method, tc.path, "")
				} else {
					_ = rt.SendRequest(tc.method, tc.path, "")
				}
			})
		})
	}
}
