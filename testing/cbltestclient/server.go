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
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// readyTimeout is how long a test server has to answer GET /.
	readyTimeout = 60 * time.Second
	// readyPollInterval is how often it is asked during that time.
	readyPollInterval = 50 * time.Millisecond
	// shutdownTimeout is how long a test server has to exit after being interrupted, before it is
	// killed.
	shutdownTimeout = 10 * time.Second
	// maxStartAttempts is how many times a server that exits before it is ready is started again.
	// The free port is only free when it is picked, so another process can take it first.
	maxStartAttempts = 3
	// logTailLines is how many lines of a test server's output are kept for failure messages.
	logTailLines = 200
)

// Options configures NewServer.
type Options struct {
	// Version is the Couchbase Lite version to run.  Empty means DefaultVersion.
	Version string
	// Databases are created empty once the server is ready.  More can be made later with
	// Client.Reset, which deletes every database first.
	Databases map[string]DatabaseSpec
}

// Server is a Couchbase Lite test server process that belongs to one test.
type Server struct {
	// Version is the Couchbase Lite version the server reported.
	Version string
	// URL is the base URL of its control API.
	URL string
	// Client talks to it.  Its session has already been created.
	Client *Client

	cmd      *exec.Cmd
	filesDir string
	logTail  *logTail
	// exited is closed once the process has been reaped, and waitErr holds why it ended.
	exited  chan struct{}
	waitErr error
}

// NewServer starts a test server for this test, on its own port and with its own data
// directory, and creates the databases in opts.  When the test ends the server is stopped and
// its data directory deleted, so nothing it made outlives the test.  A test can start as many as
// it needs, of any versions that have been built, and tests that use them can run in parallel.
//
// If no test server has been built for the version the test is skipped, unless
// EnvRequireTestServer is set, in which case it fails.
func NewServer(t testing.TB, opts Options) *Server {
	t.Helper()
	version := opts.Version
	if version == "" {
		version = DefaultVersion()
	}
	ctx := base.TestCtx(t)

	binary, err := ResolveBinary(version)
	if err != nil {
		if errors.Is(err, ErrNoTestServer) && !requireTestServer() {
			t.Skipf("Skipping test that needs a real Couchbase Lite client: %v", err)
		}
		t.Fatalf("Could not get a Couchbase Lite test server: %v", err)
	}

	var server *Server
	// Only an early exit is worth another attempt: it most often means something took the port.
	for attempt := 1; server == nil && attempt <= maxStartAttempts && (err == nil || errors.Is(err, errExitedBeforeReady)); attempt++ {
		server, err = startServer(ctx, binary, version)
	}
	if server == nil {
		t.Fatalf("Could not start Couchbase Lite %s test server: %v", version, err)
	}
	t.Cleanup(func() { server.stop(ctx) })

	if len(opts.Databases) > 0 {
		if err := server.Client.Reset(ctx, t.Name(), opts.Databases); err != nil {
			t.Fatalf("Could not create databases on the Couchbase Lite test server: %v\n%s", err, server.LogTail())
		}
	}
	return server
}

// requireTestServer reports whether a missing test server should fail rather than skip.
func requireTestServer() bool {
	required, _ := strconv.ParseBool(os.Getenv(EnvRequireTestServer))
	return required
}

// errExitedBeforeReady is returned when a test server exits before it answers, most often because
// something else took its port.
var errExitedBeforeReady = errors.New("cbltestclient: test server exited before it was ready")

// startServer runs the test server at binary on a free port with its own data directory, and
// waits for it to answer.
func startServer(ctx context.Context, binary, version string) (*Server, error) {
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
	tail := &logTail{ctx: ctx, version: version}
	cmd.Stdout = tail
	cmd.Stderr = tail

	base.DebugfCtx(ctx, base.KeySGTest, "Starting Couchbase Lite %s test server on port %d", version, port)
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

	if err := server.connect(ctx, version); err != nil {
		server.stop(ctx)
		return nil, err
	}
	return server, nil
}

// connect waits for the server to answer, checks it is the Couchbase Lite version that was asked
// for, and creates the session every other endpoint needs.
func (s *Server) connect(ctx context.Context, version string) error {
	client, info, err := s.waitUntilReady(ctx)
	if err != nil {
		return err
	}
	// A CI build reports a build number as well ("4.1.2-2").
	if reported, _, _ := strings.Cut(info.Version, "-"); reported != version {
		return fmt.Errorf("cbltestclient: test server at %s reports Couchbase Lite %s, wanted %s", s.URL, info.Version, version)
	}

	s.Version = info.Version
	s.Client = client
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
		if s.hasExited() {
			return nil, ServerInfo{}, fmt.Errorf("%w: %v\n%s", errExitedBeforeReady, s.waitErr, s.LogTail())
		}
		client, info, err := NewClient(ctx, s.URL)
		if err == nil {
			return client, info, nil
		}
		lastErr = err
		time.Sleep(readyPollInterval)
	}
	return nil, ServerInfo{}, fmt.Errorf("cbltestclient: test server at %s was not ready within %s: %w\n%s", s.URL, readyTimeout, lastErr, s.LogTail())
}

// hasExited reports whether the server's process has ended.
func (s *Server) hasExited() bool {
	select {
	case <-s.exited:
		return true
	default:
		return false
	}
}

// LogTail returns the last lines of the test server's output, for a failure message.
func (s *Server) LogTail() string {
	return s.logTail.String()
}

// stop shuts the server down and deletes its data directory.  It is safe to call more than once.
func (s *Server) stop(ctx context.Context) {
	// Interrupt first so the server can close its databases cleanly.
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
// before the test server binds it, in which case the server exits and NewServer starts it again.
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
	version string
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
		base.InfofCtx(l.ctx, base.KeySGTest, "[cbl %s] %s", l.version, line)
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
