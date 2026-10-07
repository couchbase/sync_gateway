// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
)

const (
	// EnvCBLVersion sets the Couchbase Lite version used by tests that don't name one, so a whole
	// suite can be run against another version.
	EnvCBLVersion = "SG_TEST_CBL_VERSION"
	// EnvTestServerDir overrides the directory built test servers are installed in.
	EnvTestServerDir = "SG_TEST_CBL_TEST_SERVER_DIR"
	// EnvRequireTestServer makes a missing test server fail the test instead of skipping it.  CI
	// sets this, so a test server that stops being built can't quietly stop being tested.
	EnvRequireTestServer = "SG_TEST_REQUIRE_CBL_TEST_SERVER"

	// DefaultCBLVersion is the Couchbase Lite version used when neither the test nor
	// EnvCBLVersion names one.  Keep it in step with integration-test/cbl_test_server.py.
	DefaultCBLVersion = "4.1.2"
)

// ErrNoTestServer is returned when no test server has been built for a version.  NewServer turns
// it into a skip, or into a failure when EnvRequireTestServer is set.
var ErrNoTestServer = errors.New("cbltestclient: no Couchbase Lite test server available")

// DefaultVersion returns the Couchbase Lite version used by tests that don't name one.
func DefaultVersion() string {
	if version := os.Getenv(EnvCBLVersion); version != "" {
		return version
	}
	return DefaultCBLVersion
}

// ResolveBinary returns the test server executable integration-test/cbl_test_server.py installed
// for a Couchbase Lite version.  It never builds one: that means downloading Couchbase Lite and
// running a C++ build, which does not belong inside `go test`.
func ResolveBinary(version string) (string, error) {
	path := filepath.Join(installDir(version), "bin", "testserver")
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("%w for Couchbase Lite %s: build one with `uv run integration-test/cbl_test_server.py --cbl-version %s`",
			ErrNoTestServer, version, version)
	}
	return path, nil
}

// installDir returns where the build script installs the test server for a Couchbase Lite
// version.  It has to match default_install_dir in integration-test/cbl_test_server.py.
func installDir(version string) string {
	root := os.Getenv(EnvTestServerDir)
	if root == "" {
		userCache, err := os.UserCacheDir()
		if err != nil {
			// ResolveBinary reports the resulting miss with the instructions for building one
			userCache = os.TempDir()
		}
		root = filepath.Join(userCache, "sync_gateway", "cbl-test-server")
	}
	return filepath.Join(root, version, runtime.GOOS+"-"+runtime.GOARCH)
}
