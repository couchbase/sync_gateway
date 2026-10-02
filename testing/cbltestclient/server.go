// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// EnvTestServerURL points at a test server that is already running.
	EnvTestServerURL = "SG_TEST_CBL_TEST_SERVER_URL"
	// EnvRequireTestServer makes a missing test server fail the test instead of skipping it.  CI
	// sets this, so a test server that stops being built can't quietly stop being tested.
	EnvRequireTestServer = "SG_TEST_REQUIRE_CBL_TEST_SERVER"

	// CBLVersion is the Couchbase Lite version the test server must be built against.  Keep it in
	// step with integration-test/cbl_test_server.py.
	CBLVersion = "4.1.2"

	// readyTimeout is how long a test server has to answer GET /.
	readyTimeout = 60 * time.Second
	// readyPollInterval is how often it is asked during that time.
	readyPollInterval = 50 * time.Millisecond
)

// ErrNoTestServer is returned when EnvTestServerURL is unset.  GetServer turns it into a
// skip, or into a failure when EnvRequireTestServer is set.
var ErrNoTestServer = errors.New("cbltestclient: no Couchbase Lite test server available")

// shared is the test server this process uses, reused for the lifetime of the test binary.  A
// server is clean again once Reset has run, so there is nothing to gain from one per test.
var shared struct {
	mutex  sync.Mutex
	server *Server
}

// Server is a running Couchbase Lite test server.
type Server struct {
	// Version is the Couchbase Lite version the server reported.
	Version string
	// URL is the base URL of its control API.
	URL string
	// Client talks to it.  Its session has already been created.
	Client *Client
}

// GetServer returns the shared test server, connecting to it on first use.
//
// If no test server can be found the test is skipped, unless EnvRequireTestServer is set, in which
// case it fails.
//
// The server is shared with every other test in this binary, so tests using it must not run in
// parallel.
func GetServer(t testing.TB) *Server {
	t.Helper()
	ctx := base.TestCtx(t)

	shared.mutex.Lock()
	defer shared.mutex.Unlock()
	if shared.server != nil {
		return shared.server
	}

	server, err := launch(ctx)
	if err != nil {
		if errors.Is(err, ErrNoTestServer) && !requireTestServer() {
			t.Skipf("Skipping test that needs a real Couchbase Lite client: %v", err)
		}
		t.Fatalf("Could not get a Couchbase Lite test server: %v", err)
	}
	shared.server = server
	return server
}

// requireTestServer reports whether a missing test server should fail rather than skip.
func requireTestServer() bool {
	required, _ := strconv.ParseBool(os.Getenv(EnvRequireTestServer))
	return required
}

// launch connects to the test server named by EnvTestServerURL.
func launch(ctx context.Context) (*Server, error) {
	url := os.Getenv(EnvTestServerURL)
	if url == "" {
		return nil, fmt.Errorf("%w: set %s to the URL of a running test server", ErrNoTestServer, EnvTestServerURL)
	}
	server := &Server{URL: url}
	if err := server.connect(ctx); err != nil {
		return nil, err
	}
	return server, nil
}

// connect waits for the server to answer, checks it is the Couchbase Lite version this package
// expects, and creates the session every other endpoint needs.
func (s *Server) connect(ctx context.Context) error {
	client, info, err := s.waitUntilReady(ctx)
	if err != nil {
		return err
	}
	// A CI build reports a build number as well ("4.1.2-2").
	if reported, _, _ := strings.Cut(info.Version, "-"); reported != CBLVersion {
		return fmt.Errorf("cbltestclient: test server at %s reports Couchbase Lite %s, wanted %s", s.URL, info.Version, CBLVersion)
	}

	s.Version = info.Version
	s.Client = client
	// The test server keeps one session and clears its sessions directory when a new one is
	// created, so this happens once per server and never per test.
	if err := client.NewSession(ctx); err != nil {
		return fmt.Errorf("cbltestclient: creating session on %s: %w", s.URL, err)
	}
	return nil
}

// waitUntilReady polls GET / until the server answers or the deadline passes.
func (s *Server) waitUntilReady(ctx context.Context) (*Client, ServerInfo, error) {
	deadline := time.Now().Add(readyTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		client, info, err := NewClient(ctx, s.URL)
		if err == nil {
			return client, info, nil
		}
		lastErr = err
		time.Sleep(readyPollInterval)
	}
	return nil, ServerInfo{}, fmt.Errorf("cbltestclient: test server at %s was not ready within %s: %w", s.URL, readyTimeout, lastErr)
}
