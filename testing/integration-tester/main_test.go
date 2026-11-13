// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build !windows

package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestMain(m *testing.M) {
	flag.Parse()
	initLogger(testing.Verbose())
	os.Exit(m.Run())
}

// writeScript writes an executable shell script to path.
func writeScript(t *testing.T, path, body string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o755)) //nolint:gosec
}

// processExists reports whether pid is still running.
func processExists(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

func TestRosmarTestsStopOnInterrupt(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	pidFile := filepath.Join(dir, "gotestsum.pid")
	binDir := filepath.Join(dir, "bin")
	// A fake gotestsum with a child process, standing in for go test and the test binary. The pid is written
	// after the fork so the SIGINT cannot race it, since a child forked during the group kill can survive it.
	writeScript(t, filepath.Join(binDir, "gotestsum"), "sleep 300 &\necho $$ > "+pidFile+"\nwait\n")
	t.Setenv("PATH", binDir+":"+os.Getenv("PATH"))

	// Registered before the test signals itself, so the SIGINT cancels ctx instead of killing the test binary.
	ctx, stop := interruptContext()
	defer stop()

	nonFatal = errCollector{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runRosmarTests(ctx, nil, []pkg{sgModule + "/base"})
	}()

	var pid int
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		data, err := os.ReadFile(pidFile)
		require.NoError(c, err)
		pid, err = strconv.Atoi(strings.TrimSpace(string(data)))
		require.NoError(c, err)
	}, 10*time.Second, 50*time.Millisecond)
	t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })

	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGINT))

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "runRosmarTests did not return after SIGINT")
	}
	assert.False(t, processExists(pid), "gotestsum still running after SIGINT")
	assert.Empty(t, nonFatal.errs, "an interrupted run should not record errors")
}

// fakeStartCBS installs a fake start_cbs.py that fails until attempt succeedOnAttempt, or always fails when
// succeedOnAttempt is 0. It returns a function reporting how many attempts were made.
func fakeStartCBS(t *testing.T, succeedOnAttempt int) func() int {
	dir := t.TempDir()
	t.Chdir(dir)
	countFile := filepath.Join(dir, "attempts")
	writeScript(t, filepath.Join(dir, startCBSScript), `
echo attempt >> "`+countFile+`"
n=$(wc -l < "`+countFile+`")
if [ "`+strconv.Itoa(succeedOnAttempt)+`" -eq 0 ] || [ "$n" -lt "`+strconv.Itoa(succeedOnAttempt)+`" ]; then
	echo "simulated allocation failure" >&2
	exit 1
fi
while [ $# -gt 0 ]; do
	if [ "$1" = "--env-file" ]; then env_file="$2"; fi
	shift
done
printf "export CBS_CLUSTER_ID=abc\nexport SG_TEST_COUCHBASE_SERVER_URL='couchbase://127.0.0.1'\n" > "$env_file"
`)
	// Each attempt appends a line, so a concurrent read never sees a partly written count.
	return func() int {
		data, _ := os.ReadFile(countFile) //nolint:gosec
		return strings.Count(string(data), "\n")
	}
}

// setAllocateRetryDelay overrides allocateRetryDelay for the duration of the test.
func setAllocateRetryDelay(t *testing.T, delay time.Duration) {
	oldDelay := allocateRetryDelay
	allocateRetryDelay = delay
	t.Cleanup(func() { allocateRetryDelay = oldDelay })
}

func allocateTestCluster(ctx context.Context) (string, string, error) {
	return allocateCluster(ctx, "test", "enterprise-7.6.12", ProtocolCouchbase, false)
}

func TestAllocateClusterRetries(t *testing.T) {
	attempts := fakeStartCBS(t, 2)
	setAllocateRetryDelay(t, time.Millisecond)

	clusterID, connStr, err := allocateTestCluster(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "abc", clusterID)
	assert.Equal(t, "couchbase://127.0.0.1", connStr)
	assert.Equal(t, 2, attempts())
}

func TestAllocateClusterGivesUpAfterAllAttempts(t *testing.T) {
	attempts := fakeStartCBS(t, 0)
	setAllocateRetryDelay(t, time.Millisecond)

	clusterID, _, err := allocateTestCluster(t.Context())
	require.ErrorContains(t, err, "start_cbs.py")
	assert.Empty(t, clusterID)
	assert.Equal(t, 3, attempts())
}

func TestAllocateClusterStopsRetryingWhenCancelled(t *testing.T) {
	attempts := fakeStartCBS(t, 0)
	setAllocateRetryDelay(t, time.Hour)
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	// Observe the retry warning, which is logged just before allocateCluster waits to retry.
	core, logs := observer.New(zapcore.WarnLevel)
	oldLogger := logger
	logger = zap.New(core).Sugar()
	t.Cleanup(func() { logger = oldLogger })

	done := make(chan error, 1)
	go func() {
		_, _, err := allocateTestCluster(ctx)
		done <- err
	}()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, logs.FilterMessageSnippet("retrying in").Len())
	}, 10*time.Second, 10*time.Millisecond)
	cancel(errors.New("test cancelled"))

	select {
	case err := <-done:
		require.ErrorContains(t, err, "start_cbs.py")
	case <-time.After(10 * time.Second):
		require.FailNow(t, "allocateCluster did not return after cancellation")
	}
	assert.Equal(t, 1, attempts())
}

func TestAllocateClusterCancelledBeforeFirstAttempt(t *testing.T) {
	attempts := fakeStartCBS(t, 1)
	ctx, cancel := context.WithCancelCause(t.Context())
	cancelErr := errors.New("test cancelled")
	cancel(cancelErr)

	clusterID, _, err := allocateTestCluster(ctx)
	require.ErrorIs(t, err, cancelErr)
	assert.Empty(t, clusterID)
	assert.Equal(t, 0, attempts())
}

func TestBuildPackageListFiltersByTargetTest(t *testing.T) {
	testCases := []struct {
		name          string
		targetPackage string
		targetTest    string
		expected      []pkg
	}{
		{
			name:          "all tests",
			targetPackage: "base,db",
			targetTest:    "ALL",
			expected:      []pkg{sgModule + "/base", sgModule + "/db"},
		},
		{
			name:          "test in one package",
			targetPackage: "...",
			targetTest:    "^TestChannelCacheSize$",
			expected:      []pkg{sgModule + "/db/changecachetest"},
		},
		{
			name:          "subtest pattern",
			targetPackage: "base,db/changecachetest",
			targetTest:    "^TestChannelCacheSize$/sub",
			expected:      []pkg{sgModule + "/db/changecachetest"},
		},
		{
			name:          "alternation across packages",
			targetPackage: "base,db/changecachetest",
			targetTest:    "^TestChannelCacheSize$/sub|^TestFixJSONNumbers$",
			expected:      []pkg{sgModule + "/base", sgModule + "/db/changecachetest"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, buildPackageList(tc.targetPackage, tc.targetTest))
		})
	}
}

func TestTopLevelRunPattern(t *testing.T) {
	testCases := []struct {
		run      string
		expected string
	}{
		{run: "TestA", expected: "TestA"},
		{run: "TestA/sub", expected: "TestA"},
		{run: "TestA/sub/deeper", expected: "TestA"},
		{run: "TestA|TestB", expected: "(?:TestA)|(?:TestB)"},
		{run: "TestA/x|TestB/y", expected: "(?:TestA)|(?:TestB)"},
		{run: "(TestA|TestB)/x", expected: "(TestA|TestB)"},
		{run: "Test[/|]A/x", expected: "Test[/|]A"},
		{run: "Test[(]A/x", expected: "Test[(]A"},
		{run: `Test\/A/x`, expected: `Test\/A`},
	}
	for _, tc := range testCases {
		t.Run(tc.run, func(t *testing.T) {
			assert.Equal(t, tc.expected, topLevelRunPattern(tc.run))
		})
	}
}

func TestFileHasMatchingTest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x_test.go")
	require.NoError(t, os.WriteFile(path, []byte(`package x

import "testing"

type suite struct{}

func (suite) TestMethod(t *testing.T) {}
func helperTestSetup()                 {}
func TestPlain(t *testing.T)           {}
func ExampleThing()                    {}
func FuzzParse(f *testing.F)           {}
`), 0o600))

	testCases := []struct {
		pattern  string
		expected bool
	}{
		{pattern: "^TestPlain$", expected: true},
		{pattern: "Plain", expected: true},
		{pattern: "^ExampleThing$", expected: true},
		{pattern: "^FuzzParse$", expected: true},
		{pattern: "TestMethod", expected: false},
		{pattern: "helperTestSetup", expected: false},
		{pattern: "TestMissing", expected: false},
	}
	for _, tc := range testCases {
		t.Run(tc.pattern, func(t *testing.T) {
			assert.Equal(t, tc.expected, fileHasMatchingTest(path, regexp.MustCompile(tc.pattern)))
		})
	}
}

func TestFilterPackagesByTestIncludesBuildTaggedFiles(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	files := map[string]string{
		"go.mod":       "module example.com/m\n\ngo 1.27\n",
		"m.go":         "package m\n",
		"m_test.go":    "package m\n\nimport \"testing\"\n\nfunc TestAlways(t *testing.T) {}\n",
		"m_ee_test.go": "//go:build cb_sg_enterprise\n\npackage m\n\nimport \"testing\"\n\nfunc TestEEOnly(t *testing.T) {}\n",
	}
	for name, content := range files {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600))
	}

	m := []pkg{"example.com/m"}
	assert.Equal(t, m, filterPackagesByTest(m, "^TestAlways$"))
	assert.Equal(t, m, filterPackagesByTest(m, "^TestEEOnly$"), "test in a file excluded by build tags should count")
	assert.Empty(t, filterPackagesByTest(m, "^TestMissing$"))
}
