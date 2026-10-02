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
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// EnvTestServerDir overrides the directory built test servers are installed in.
	EnvTestServerDir = "SG_TEST_CBL_TEST_SERVER_DIR"

	// shutdownTimeout is how long a test server has to exit after being interrupted, before it is
	// killed.
	shutdownTimeout = 10 * time.Second
	// logTailLines is how many lines of a test server's output are kept for failure messages.
	logTailLines = 200
)

// ResolveBinary returns the test server executable integration-test/cbl_test_server.py installed.
// It never builds one: that means downloading Couchbase Lite and running a C++ build, which does
// not belong inside `go test`.
func ResolveBinary() (string, error) {
	path := filepath.Join(installDir(), "bin", "testserver")
	if _, err := os.Stat(path); err != nil {
		return "", fmt.Errorf("%w: build one with `uv run integration-test/cbl_test_server.py --cbl-version %s`, or point %s at a running one",
			ErrNoTestServer, CBLVersion, EnvTestServerURL)
	}
	return path, nil
}

// installDir returns where the build script installs the test server.  It has to match
// default_install_dir in integration-test/cbl_test_server.py.
func installDir() string {
	root := os.Getenv(EnvTestServerDir)
	if root == "" {
		userCache, err := os.UserCacheDir()
		if err != nil {
			// ResolveBinary reports the resulting miss with the instructions for building one
			userCache = os.TempDir()
		}
		root = filepath.Join(userCache, "sync_gateway", "cbl-test-server")
	}
	return filepath.Join(root, CBLVersion, runtime.GOOS+"-"+runtime.GOARCH)
}

// StopServer stops the test server this process started and removes its data directory.  A server
// named by EnvTestServerURL is left running.
//
// Call it from TestMain after the tests have run.  For a package using the bucket pool that means
// base.TestBucketPoolOptions.TeardownFuncs, since base.TestBucketPoolMain calls os.Exit.
func StopServer(ctx context.Context) {
	shared.mutex.Lock()
	defer shared.mutex.Unlock()
	if shared.server != nil {
		shared.server.stop(ctx)
		shared.server = nil
	}
}

// startServer runs the test server at binary on a free port with its own data directory, and
// waits for it to answer.
func startServer(ctx context.Context, binary string) (*Server, error) {
	port, err := freeLoopbackPort()
	if err != nil {
		return nil, err
	}
	filesDir, err := os.MkdirTemp("", "cbl-test-server-")
	if err != nil {
		return nil, fmt.Errorf("cbltestclient: creating test server files directory: %w", err)
	}
	cmd := exec.Command(binary, "--port", strconv.Itoa(port), "--files-dir", filesDir)
	// The test server finds its assets relative to the executable.
	cmd.Dir = filepath.Dir(binary)
	tail := &logTail{ctx: ctx}
	cmd.Stdout = tail
	cmd.Stderr = tail

	base.InfofCtx(ctx, base.KeySGTest, "Starting Couchbase Lite test server from %s", base.MD(binary))
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(filesDir)
		return nil, fmt.Errorf("cbltestclient: starting test server %s: %w", binary, err)
	}

	server := &Server{
		URL:      fmt.Sprintf("http://127.0.0.1:%d", port),
		cmd:      cmd,
		filesDir: filesDir,
		logTail:  tail,
		exited:   make(chan struct{}),
	}
	// Only one goroutine may call Wait, so everything else waits on exited.
	go func() {
		server.waitErr = cmd.Wait()
		close(server.exited)
	}()

	if err := server.connect(ctx); err != nil {
		server.stop(ctx)
		return nil, err
	}
	return server, nil
}

// hasExited reports whether a started server's process has ended.
func (s *Server) hasExited() bool {
	if s.exited == nil {
		return false
	}
	select {
	case <-s.exited:
		return true
	default:
		return false
	}
}

// LogTail returns the last lines of the test server's output, for a failure message.  It is empty
// for a server named by EnvTestServerURL, whose output this package never sees.
func (s *Server) LogTail() string {
	if s.logTail == nil {
		return ""
	}
	return s.logTail.String()
}

// stop shuts a started server down and removes its data directory.  It does nothing for a server
// named by EnvTestServerURL.
func (s *Server) stop(ctx context.Context) {
	if s.external || s.cmd == nil {
		return
	}

	// Interrupt first so the server can close its database cleanly.
	if err := s.cmd.Process.Signal(os.Interrupt); err != nil {
		_ = s.cmd.Process.Kill()
	}
	select {
	case <-s.exited:
	case <-time.After(shutdownTimeout):
		base.InfofCtx(ctx, base.KeySGTest, "Couchbase Lite test server %s did not exit within %s, killing it", base.MD(s.URL), shutdownTimeout)
		_ = s.cmd.Process.Kill()
		<-s.exited
	}

	if err := os.RemoveAll(s.filesDir); err != nil {
		base.InfofCtx(ctx, base.KeySGTest, "Could not remove Couchbase Lite test server files directory %s: %v", base.MD(s.filesDir), err)
	}
}

// freeLoopbackPort returns a loopback port nothing is listening on.  Something else can take it
// before the test server binds it, in which case the server exits and waitUntilReady reports it.
func freeLoopbackPort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("cbltestclient: finding a free port: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		return 0, fmt.Errorf("cbltestclient: releasing port %d: %w", port, err)
	}
	return port, nil
}

// logTail copies a test server's output into the Go test log, and keeps the last lines for a
// failure message.  Writes arrive in whatever chunks the pipe delivers, so they are put back
// together into lines.
type logTail struct {
	ctx     context.Context
	mutex   sync.Mutex
	lines   []string
	partial []byte
}

var _ io.Writer = &logTail{}

func (l *logTail) Write(p []byte) (int, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.partial = append(l.partial, p...)
	for newline := bytes.IndexByte(l.partial, '\n'); newline >= 0; newline = bytes.IndexByte(l.partial, '\n') {
		line := string(bytes.TrimSuffix(l.partial[:newline], []byte("\r")))
		l.partial = l.partial[newline+1:]
		base.InfofCtx(l.ctx, base.KeySGTest, "[cbl] %s", line)
		l.lines = append(l.lines, line)
		if len(l.lines) > logTailLines {
			l.lines = l.lines[len(l.lines)-logTailLines:]
		}
	}
	return len(p), nil
}

func (l *logTail) String() string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if len(l.lines) == 0 {
		return ""
	}
	return "Couchbase Lite test server output:\n" + strings.Join(l.lines, "\n")
}
