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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const (
	// readyTimeout is how long a freshly launched test server has to answer GET /.
	readyTimeout = 60 * time.Second
	// readyPollInterval is how often it is asked during that time.
	readyPollInterval = 50 * time.Millisecond
	// shutdownTimeout is how long a test server has to exit after being interrupted, before it is
	// killed outright.
	shutdownTimeout = 10 * time.Second
	// logTailLines is how many lines of a test server's output are retained to be dumped when a
	// test fails.
	logTailLines = 200
	// DefaultTestServerPort is the port a test server binds when it can't be told to use another.
	DefaultTestServerPort = 8080
)

// Server is a running Couchbase Lite test server process, or an externally managed one this
// package only talks to.
type Server struct {
	// Version is the Couchbase Lite version the server reported at startup.
	Version string
	// URL is the base URL of its control API.
	URL string
	// Client talks to it.  Its session has already been created.
	Client *Client

	// external is true for a server named by EnvTestServerURLs, which this package must not stop.
	external bool
	cmd      *exec.Cmd
	filesDir string
	logTail  *logTail
	// exited is closed once the process has been reaped, and waitErr holds why it ended.  Only one
	// goroutine may call Wait, so it is called once here and everything else waits on the channel.
	exited  chan struct{}
	waitErr error

	mutex sync.Mutex
	// databases is every database name registered against this server, so that Reset - which
	// deletes every database there is - can be issued once for all of them rather than once per
	// caller.
	databases map[string]DatabaseSpec
	stopped   bool
}

// startServer launches a test server from binary and waits for it to answer.
func startServer(ctx context.Context, binary BinaryInfo) (*Server, error) {
	// A build without --port has its port compiled in, so there is nothing to choose: ask for a
	// free one only when the server can be told to use it.
	port := DefaultTestServerPort
	var args []string
	if binary.SupportsPortFlag {
		chosen, err := freeLoopbackPort()
		if err != nil {
			return nil, err
		}
		port = chosen
		args = append(args, "--port", strconv.Itoa(port))
	}

	filesDir, err := os.MkdirTemp("", "cbl-test-server-")
	if err != nil {
		return nil, fmt.Errorf("cbltestclient: creating test server files directory: %w", err)
	}
	if binary.SupportsFilesDirFlag {
		args = append(args, "--files-dir", filesDir)
	}
	// The test server resolves its assets relative to the executable, so it has to be run from
	// where it was installed.  Running it there also isolates the data directory on Windows, where
	// the fixed directory name is resolved against the working directory.
	cmd := exec.Command(binary.Path, args...)
	cmd.Dir = filepath.Dir(binary.Path)

	// Writing into the tail rather than reading from StdoutPipe leaves the copying to os/exec, so
	// Wait cannot close the pipe out from under a reader that is still going.
	tail := newLogTail(ctx, binary.Version)
	cmd.Stdout = tail
	cmd.Stderr = tail

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("cbltestclient: starting test server %s: %w", binary.Path, err)
	}

	server := &Server{
		URL:       fmt.Sprintf("http://127.0.0.1:%d", port),
		cmd:       cmd,
		filesDir:  filesDir,
		logTail:   tail,
		exited:    make(chan struct{}),
		databases: map[string]DatabaseSpec{},
	}
	go func() {
		server.waitErr = cmd.Wait()
		close(server.exited)
	}()

	if err := server.connect(ctx, binary.Version); err != nil {
		server.stop(ctx)
		return nil, err
	}
	return server, nil
}

// connectExternal attaches to a test server someone else is running.
func connectExternal(ctx context.Context, url, version string) (*Server, error) {
	server := &Server{
		URL:       url,
		external:  true,
		databases: map[string]DatabaseSpec{},
	}
	if err := server.connect(ctx, version); err != nil {
		return nil, err
	}
	return server, nil
}

// connect waits for the server to answer, checks it is the Couchbase Lite build the caller asked
// for, and creates the session every other endpoint needs.
func (s *Server) connect(ctx context.Context, wantVersion string) error {
	client, info, err := s.waitUntilReady(ctx)
	if err != nil {
		return err
	}

	if !info.IsEnterprise() {
		return fmt.Errorf("cbltestclient: test server at %s is not an Enterprise Edition build (%q)", s.URL, info.AdditionalInfo)
	}
	// The reported version carries a build number for a CI build ("4.1.2-2"), so compare the
	// release part rather than the whole string.
	if reported, _, _ := strings.Cut(info.Version, "-"); reported != wantVersion {
		return fmt.Errorf("cbltestclient: test server at %s reports Couchbase Lite %s, wanted %s", s.URL, info.Version, wantVersion)
	}

	s.Version = info.Version
	s.Client = client
	// The test server keeps one session and clears its whole sessions directory when a new one is
	// created, so this happens once per process and never per test.
	if err := client.NewSession(ctx, ""); err != nil {
		return fmt.Errorf("cbltestclient: creating session on %s: %w", s.URL, err)
	}
	return nil
}

// waitUntilReady polls GET / until the server answers or the deadline passes.
func (s *Server) waitUntilReady(ctx context.Context) (*Client, ServerInfo, error) {
	deadline := time.Now().Add(readyTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		// A server that failed to start - most often because something else holds its port - never
		// answers, so notice it rather than waiting out the whole timeout.
		if s.hasExited() {
			return nil, ServerInfo{}, fmt.Errorf("cbltestclient: test server exited before it was ready: %v\n%s", s.waitErr, s.logTail.String())
		}
		client, err := NewClient(ctx, s.URL)
		if err == nil {
			var info ServerInfo
			if info, err = client.ServerInfo(ctx); err == nil {
				return client, info, nil
			}
		}
		lastErr = err
		time.Sleep(readyPollInterval)
	}
	return nil, ServerInfo{}, fmt.Errorf("cbltestclient: test server at %s was not ready within %s: %w\n%s", s.URL, readyTimeout, lastErr, s.logTail.String())
}

// hasExited reports whether a self-managed server's process has already ended.
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

// RegisterDatabases records databases this caller wants to exist after the next Reset.  Because
// Reset deletes every database on the server, callers sharing a server have to reset together.
func (s *Server) RegisterDatabases(databases map[string]DatabaseSpec) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for name, spec := range databases {
		if _, duplicate := s.databases[name]; duplicate {
			return fmt.Errorf("cbltestclient: database %q is already registered on the test server at %s", name, s.URL)
		}
		s.databases[name] = spec
	}
	return nil
}

// UnregisterDatabases forgets databases registered by RegisterDatabases, so a later Reset no
// longer recreates them.
func (s *Server) UnregisterDatabases(names ...string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, name := range names {
		delete(s.databases, name)
	}
}

// Reset deletes every database on the server and recreates the registered ones.  testName is
// written into the test server's log to mark where the test begins.
func (s *Server) Reset(ctx context.Context, testName string) error {
	s.mutex.Lock()
	databases := make(map[string]DatabaseSpec, len(s.databases))
	for name, spec := range s.databases {
		databases[name] = spec
	}
	s.mutex.Unlock()
	return s.Client.Reset(ctx, testName, databases)
}

// LogTail returns the last lines of the test server's output, for including in a failure message.
// An externally managed server has none, since this package never sees its output.
func (s *Server) LogTail() string {
	if s.logTail == nil {
		return ""
	}
	return s.logTail.String()
}

// stop shuts a self-managed server down and removes its data directory.  It is a no-op for an
// externally managed one, and safe to call more than once.
func (s *Server) stop(ctx context.Context) {
	s.mutex.Lock()
	if s.stopped || s.external || s.cmd == nil || s.cmd.Process == nil {
		s.stopped = true
		s.mutex.Unlock()
		return
	}
	s.stopped = true
	s.mutex.Unlock()

	// Interrupt first so the server can close its database cleanly; a server that ignores it is
	// killed once shutdownTimeout has passed.
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

	if s.filesDir != "" {
		if err := os.RemoveAll(s.filesDir); err != nil {
			base.InfofCtx(ctx, base.KeySGTest, "Could not remove Couchbase Lite test server files directory %s: %v", base.MD(s.filesDir), err)
		}
	}
}

// freeLoopbackPort returns a loopback port nothing is listening on.  There is a window between
// closing the listener and the test server binding, which is why the caller retries.
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

// logTail mirrors a test server's output into the Go test log and keeps the last few lines for a
// failure message.  It is the process's Stdout and Stderr, so writes arrive in whatever chunks the
// pipe delivers and have to be reassembled into lines.
type logTail struct {
	ctx     context.Context
	version string
	mutex   sync.Mutex
	lines   []string
	partial []byte
}

var _ io.Writer = &logTail{}

func newLogTail(ctx context.Context, version string) *logTail {
	return &logTail{ctx: ctx, version: version}
}

// Write records complete lines and holds back whatever follows the last newline until the rest of
// it arrives.
func (l *logTail) Write(p []byte) (int, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	l.partial = append(l.partial, p...)
	for {
		newline := bytes.IndexByte(l.partial, '\n')
		if newline < 0 {
			return len(p), nil
		}
		l.appendLocked(string(bytes.TrimSuffix(l.partial[:newline], []byte("\r"))))
		l.partial = l.partial[newline+1:]
	}
}

// appendLocked records one line, dropping the oldest once the tail is full.  The caller holds the
// mutex.
func (l *logTail) appendLocked(line string) {
	base.InfofCtx(l.ctx, base.KeySGTest, "[cbl %s] %s", l.version, line)
	l.lines = append(l.lines, line)
	if len(l.lines) > logTailLines {
		l.lines = l.lines[len(l.lines)-logTailLines:]
	}
}

// String returns the retained tail of the server's output.
func (l *logTail) String() string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if len(l.lines) == 0 {
		return ""
	}
	return "Couchbase Lite test server output:\n" + strings.Join(l.lines, "\n")
}
