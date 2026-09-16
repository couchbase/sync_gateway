// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Environment variables controlling where a test server comes from.
const (
	// EnvTestServerURLs points at test servers that are already running, as a comma-separated list
	// of "<cbl version>=<url>" pairs.  Used in preference to anything this package would launch,
	// so several Couchbase Lite versions can be exercised by running them by hand.
	EnvTestServerURLs = "SG_TEST_CBL_TEST_SERVER_URLS"
	// EnvTestServerBinaries points at test server executables, as a comma-separated list of
	// "<cbl version>=<path>" pairs.
	EnvTestServerBinaries = "SG_TEST_CBL_TEST_SERVER_BINARIES"
	// EnvTestServerDir overrides the directory built test servers are cached in.
	EnvTestServerDir = "SG_TEST_CBL_TEST_SERVER_DIR"
	// EnvCBLVersion overrides the Couchbase Lite version used when a caller doesn't name one.
	EnvCBLVersion = "SG_TEST_CBL_VERSION"
	// EnvRequireTestServer makes a missing test server fail the test instead of skipping it.  CI
	// sets this, so a test server that stops being built can't quietly stop being tested.
	EnvRequireTestServer = "SG_TEST_REQUIRE_CBL_TEST_SERVER"
)

// DefaultCBLVersion is the Couchbase Lite version used when a caller doesn't name one and
// EnvCBLVersion is unset.  It is also what the bootstrap script builds by default - keep the two
// in step.
const DefaultCBLVersion = "4.1.2"

const (
	// portFlag tells a test server which port to listen on.  Without it the port is compiled in,
	// so only one server can run on a host.
	portFlag = "--port"
	// filesDirFlag tells a test server where to keep its databases.  Without it, servers on Linux
	// and macOS share one fixed directory and wipe each other's sessions on startup.
	filesDirFlag = "--files-dir"
)

// ErrNoTestServer is returned when no test server build could be found for a version.  Callers
// that take a testing.TB turn it into a skip, or into a failure when EnvRequireTestServer is set.
var ErrNoTestServer = errors.New("cbltestclient: no Couchbase Lite test server available")

// testServerExecutable is the name the test server is installed under.
var testServerExecutable = "testserver" + exeSuffix()

func exeSuffix() string {
	if runtime.GOOS == "windows" {
		return ".exe"
	}
	return ""
}

// BinaryInfo describes a test server this package can launch.
type BinaryInfo struct {
	// Path is the executable to run.
	Path string
	// Version is the Couchbase Lite version it was built against, as asked for - the running
	// server's own report of its version is checked separately, once it is up.
	Version string
	// SupportsPortFlag reports whether the executable accepts --port.  Without it only one
	// instance can run on a host, because the port is compiled in.
	SupportsPortFlag bool
	// SupportsFilesDirFlag reports whether the executable accepts --files-dir.  Without it, two
	// instances on Linux or macOS share /tmp/CBL-C-TestServer and each wipes the other's session
	// directory on startup.
	SupportsFilesDirFlag bool
}

// DefaultVersion returns the Couchbase Lite version to use when a caller doesn't name one.
func DefaultVersion() string {
	if version := os.Getenv(EnvCBLVersion); version != "" {
		return version
	}
	return DefaultCBLVersion
}

// ExternalServerURL returns the URL of an already-running test server for version, if
// EnvTestServerURLs names one.  The version recorded there is a claim by whoever set the variable,
// so it is verified against the server's own report before use.
func ExternalServerURL(version string) (string, bool) {
	urls, err := parseVersionMap(os.Getenv(EnvTestServerURLs))
	if err != nil {
		return "", false
	}
	url, found := urls[version]
	return url, found
}

// ResolveBinary finds a test server executable for the given Couchbase Lite version, looking at
// EnvTestServerBinaries first and then at the cache the bootstrap script populates.
//
// It never builds anything: building the test server means fetching a Couchbase Lite tarball and
// running a C++ build, which has no business happening inside a `go test` run.  A missing build
// returns ErrNoTestServer, whose message names the command that creates one.
func ResolveBinary(version string) (BinaryInfo, error) {
	if binaries, err := parseVersionMap(os.Getenv(EnvTestServerBinaries)); err != nil {
		return BinaryInfo{}, err
	} else if path, found := binaries[version]; found {
		if _, err := os.Stat(path); err != nil {
			return BinaryInfo{}, fmt.Errorf("cbltestclient: %s names %q for Couchbase Lite %s, but it can't be used: %w", EnvTestServerBinaries, path, version, err)
		}
		return newBinaryInfo(path, version)
	}

	cached := filepath.Join(CacheDir(version), "bin", testServerExecutable)
	if _, err := os.Stat(cached); err == nil {
		return newBinaryInfo(cached, version)
	}

	return BinaryInfo{}, fmt.Errorf("%w for Couchbase Lite %s: build one with `uv run integration-test/cbl_test_server.py --cbl-version %s`, or point %s or %s at an existing one",
		ErrNoTestServer, version, version, EnvTestServerBinaries, EnvTestServerURLs)
}

// CacheDir returns the directory a built test server for version is installed in.  The executable
// lives in its "bin" subdirectory, because the test server resolves its assets directory relative
// to the executable as "<exe dir>/../assets".
func CacheDir(version string) string {
	root := os.Getenv(EnvTestServerDir)
	if root == "" {
		userCache, err := os.UserCacheDir()
		if err != nil {
			// no usable cache directory is not worth failing over here - ResolveBinary will report
			// the resulting miss with the instructions for building one
			userCache = os.TempDir()
		}
		root = filepath.Join(userCache, "sync_gateway", "cbl-test-server")
	}
	return filepath.Join(root, version, runtime.GOOS+"-"+runtime.GOARCH)
}

// newBinaryInfo works out which command line flags an executable supports by looking for them in
// it.  The test server has no --help, and running it to find out would bind its port and, on a
// build that parses no arguments at all, never exit - so the flags are read out of the executable
// rather than asked for.
func newBinaryInfo(path, version string) (BinaryInfo, error) {
	executable, err := os.ReadFile(path)
	if err != nil {
		return BinaryInfo{}, fmt.Errorf("cbltestclient: reading test server %q: %w", path, err)
	}
	return BinaryInfo{
		Path:                 path,
		Version:              version,
		SupportsPortFlag:     bytes.Contains(executable, []byte(portFlag)),
		SupportsFilesDirFlag: bytes.Contains(executable, []byte(filesDirFlag)),
	}, nil
}

// parseVersionMap parses a "<version>=<value>,<version>=<value>" environment variable.
func parseVersionMap(value string) (map[string]string, error) {
	parsed := map[string]string{}
	for _, entry := range strings.Split(value, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		version, target, found := strings.Cut(entry, "=")
		if !found {
			return nil, fmt.Errorf("cbltestclient: %q is not in <cbl version>=<value> form", entry)
		}
		parsed[strings.TrimSpace(version)] = strings.TrimSpace(target)
	}
	return parsed, nil
}
