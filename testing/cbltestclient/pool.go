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
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

// pool holds the test servers this process has started, one per Couchbase Lite version, reused for
// the lifetime of the test binary.  Starting a server costs a process launch and a database open,
// and the server is stateless between tests once Reset has run, so there is nothing to gain from
// one per test.
var pool = &serverPool{servers: map[string]*Server{}}

type serverPool struct {
	mutex   sync.Mutex
	servers map[string]*Server
}

// GetServer returns a running test server for the given Couchbase Lite version, starting one if
// this process has not already.  Pass an empty version for DefaultVersion.
//
// If no test server build can be found the test is skipped, unless EnvRequireTestServer is set, in
// which case it fails - that is what stops CI from quietly losing its real Couchbase Lite coverage
// when a build step breaks.
//
// The returned server is shared with every other test in this binary.  Call ShutdownPool from
// TestMain to stop them.
func GetServer(t testing.TB, version string) *Server {
	t.Helper()
	if version == "" {
		version = DefaultVersion()
	}
	ctx := base.TestCtx(t)

	server, err := pool.get(ctx, version)
	if err == nil {
		return server
	}
	requireOrSkipTestServer(t, err)
	return nil
}

// requireOrSkipTestServer skips the test when no test server build could be found, or fails it
// when EnvRequireTestServer says a missing one is not acceptable.  Any other error always fails.
func requireOrSkipTestServer(t testing.TB, err error) {
	t.Helper()
	if errors.Is(err, ErrNoTestServer) && !requireTestServer() {
		t.Skipf("Skipping test that needs a real Couchbase Lite client: %v", err)
	}
	t.Fatalf("Could not get a Couchbase Lite test server: %v", err)
}

// get returns the pooled server for a version, starting or connecting to one on first use.
func (p *serverPool) get(ctx context.Context, version string) (*Server, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if server, found := p.servers[version]; found {
		return server, nil
	}

	server, err := p.startLocked(ctx, version)
	if err != nil {
		return nil, err
	}
	p.servers[version] = server
	return server, nil
}

// startLocked connects to an externally managed test server for the version if one is configured,
// and otherwise launches one.  The caller holds p.mutex.
func (p *serverPool) startLocked(ctx context.Context, version string) (*Server, error) {
	if url, found := ExternalServerURL(version); found {
		base.InfofCtx(ctx, base.KeySGTest, "Using externally managed Couchbase Lite %s test server at %s", version, base.MD(url))
		return connectExternal(ctx, url, version)
	}

	binary, err := ResolveBinary(version)
	if err != nil {
		return nil, err
	}
	if err := p.checkCanRunAnotherLocked(binary, version); err != nil {
		return nil, err
	}

	base.InfofCtx(ctx, base.KeySGTest, "Starting Couchbase Lite %s test server from %s", version, base.MD(binary.Path))
	return startServer(ctx, binary)
}

// checkCanRunAnotherLocked reports whether a second self-managed test server can coexist with the
// ones already running.  The caller holds p.mutex.
//
// Two things stop it.  A test server without --port has its port compiled in, so the second one
// cannot bind.  A test server without --files-dir uses a fixed data directory on Linux and macOS,
// and wipes its sessions directory on startup, so the second one destroys the first one's state.
// Both are fixed upstream in couchbaselabs/couchbase-lite-tests; until a build carrying those
// flags is in use, only one server can run at a time.
func (p *serverPool) checkCanRunAnotherLocked(binary BinaryInfo, version string) error {
	var running []string
	for existing, server := range p.servers {
		if !server.external {
			running = append(running, existing)
		}
	}
	if len(running) == 0 {
		return nil
	}

	if !binary.SupportsPortFlag {
		return fmt.Errorf("cbltestclient: cannot start a Couchbase Lite %s test server alongside %v: %s does not support --port, so both would try to bind the same port. Build a newer test server, or run one by hand and point %s at it",
			version, running, binary.Path, EnvTestServerURLs)
	}
	// Windows resolves the fixed data directory name against the working directory, and each
	// server runs from its own install directory, so instances there are already isolated.
	if !binary.SupportsFilesDirFlag && runtime.GOOS != "windows" {
		return fmt.Errorf("cbltestclient: cannot start a Couchbase Lite %s test server alongside %v: %s does not support --files-dir, so both would share one data directory and wipe each other's sessions. Build a newer test server, or run one by hand and point %s at it",
			version, running, binary.Path, EnvTestServerURLs)
	}
	return nil
}

// ShutdownPool stops every test server this process started and removes their data directories.
// Externally managed servers are left running.
//
// Call it from TestMain after the tests have run.  For a package using the bucket pool that means
// base.TestBucketPoolOptions.TeardownFuncs, since base.TestBucketPoolMain calls os.Exit and so is
// the last thing to get a say.
func ShutdownPool(ctx context.Context) {
	pool.mutex.Lock()
	servers := make([]*Server, 0, len(pool.servers))
	for _, server := range pool.servers {
		servers = append(servers, server)
	}
	clear(pool.servers)
	pool.mutex.Unlock()

	for _, server := range servers {
		server.stop(ctx)
	}
}

// requireTestServer reports whether a missing test server should fail rather than skip.
func requireTestServer() bool {
	required, _ := strconv.ParseBool(os.Getenv(EnvRequireTestServer))
	return required
}
