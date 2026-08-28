// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package sgtest

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

const (
	// how long StallingListener waits for a connection count to be reached before failing the test
	stallingListenerTimeout      = 30 * time.Second
	stallingListenerPollInterval = 10 * time.Millisecond
)

// RequireNonZeroHTTPTimeouts asserts the transport-level timeouts are set on the given transport.  A zero
// value means no limit, letting a remote that accepts a connection and then stalls block the caller.  These
// cover the exchange up to the response headers; the body read is bounded by Client.Timeout or the request
// context, if at all.
//
// TLSHandshakeTimeout is not checked: http.Transport honours it only on its own TLS path, so a transport
// with a custom DialTLSContext enforces that deadline in its dialer instead.  net.Dialer.Timeout lives
// inside the DialContext closure and can't be read back, so a non-nil DialContext stands in for it.
func RequireNonZeroHTTPTimeouts(t testing.TB, transport *http.Transport) {
	t.Helper()
	require.NotNil(t, transport.DialContext, "DialContext must be set, otherwise net.Dialer.Timeout is lost")
	timeouts := []struct {
		name  string
		value time.Duration
	}{
		// bounds the wait for response headers once the request has been written
		{name: "ResponseHeaderTimeout", value: transport.ResponseHeaderTimeout},
		// bounds the wait for a 100-continue response before sending the request body
		{name: "ExpectContinueTimeout", value: transport.ExpectContinueTimeout},
		// bounds how long an idle pooled connection is retained
		{name: "IdleConnTimeout", value: transport.IdleConnTimeout},
	}
	for _, timeout := range timeouts {
		require.NotEqual(t, time.Duration(0), timeout.value, "%s must not be 0, which means no limit", timeout.name)
	}
}

// StallingListener is a loopback TCP listener that accepts connections, drains whatever is written to it,
// and never writes a byte back.  It stands in for the remote that broke ISGR: one that completes the TCP
// handshake and then goes silent, so a caller with no deadline waits forever.  How that presents depends on
// how far the client got - a stalled TLS handshake, or a stalled wait for response headers.
//
// Accepted connections are held open until the test finishes.
type StallingListener struct {
	listener net.Listener
	mutex    sync.Mutex
	conns    []net.Conn
	accepted int
	closed   int
}

// NewStallingListener starts a StallingListener and registers cleanup that closes it along with every
// connection it accepted.
func NewStallingListener(t testing.TB) *StallingListener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	l := &StallingListener{listener: listener}

	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return // listener closed by cleanup
			}
			l.mutex.Lock()
			l.conns = append(l.conns, conn)
			l.accepted++
			l.mutex.Unlock()
			wg.Go(func() {
				// drain without ever responding, so this only returns once the peer hangs up - or once
				// cleanup closes the connection from this side
				_, _ = io.Copy(io.Discard, conn)
				l.mutex.Lock()
				l.closed++
				l.mutex.Unlock()
			})
		}
	})

	t.Cleanup(func() {
		assert.NoError(t, listener.Close())
		l.mutex.Lock()
		for _, conn := range l.conns {
			_ = conn.Close()
		}
		l.mutex.Unlock()
		wg.Wait()
	})
	return l
}

// Addr returns the host:port to dial.
func (l *StallingListener) Addr() string {
	return l.listener.Addr().String()
}

// RequireAcceptedConnections waits for the listener to have accepted at least count connections, so a test
// can tell "the client gave up before connecting" apart from "the client connected and then gave up".
func (l *StallingListener) RequireAcceptedConnections(t testing.TB, count int) {
	t.Helper()
	l.requireCount(t, count, "accepted", func() int { return l.accepted })
}

// RequireClosedConnections waits for at least count of the accepted connections to have been closed.  Call
// it before the test finishes, while the only party that can have closed one is the client - that makes it
// an assertion that a client which gave up also hung up, rather than leaking the connection.
func (l *StallingListener) RequireClosedConnections(t testing.TB, count int) {
	t.Helper()
	l.requireCount(t, count, "closed", func() int { return l.closed })
}

// requireCount polls until the given counter reaches count, and fails the test if it never does.  Polling
// rather than signalling keeps this helper free of the deadlock a bounded channel would risk during cleanup.
func (l *StallingListener) requireCount(t testing.TB, count int, name string, get func() int) {
	t.Helper()
	read := func() int {
		l.mutex.Lock()
		defer l.mutex.Unlock()
		return get()
	}
	deadline := time.Now().Add(stallingListenerTimeout)
	for time.Now().Before(deadline) {
		if read() >= count {
			return
		}
		time.Sleep(stallingListenerPollInterval)
	}
	require.GreaterOrEqual(t, read(), count, fmt.Sprintf("timed out after %s waiting for %d %s connections", stallingListenerTimeout, count, name))
}
