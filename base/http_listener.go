/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	// Default amount of time allowed to read request headers.
	// If ReadHeaderTimeout is not defined in JSON config file,
	// the value of DefaultReadHeaderTimeout is used.
	DefaultReadHeaderTimeout = 5 * time.Second

	// Default maximum amount of time to wait for the next request
	// when keep-alives are enabled. If IdleTimeout is not defined
	// in JSON config file, the value of DefaultIdleTimeout is used.
	DefaultIdleTimeout = 90 * time.Second
)

// Creates a TLS config, loading the certificate and key from disk. Returns nil if certFile is empty.
func MakeTLSConfig(certFile, keyFile string, tlsMinVersion uint16) (*tls.Config, error) {
	if certFile == "" {
		return nil, nil
	} else if cert, err := tls.LoadX509KeyPair(certFile, keyFile); err != nil {
		return nil, err
	} else {
		return &tls.Config{
			MinVersion:   tlsMinVersion,
			Certificates: []tls.Certificate{cert},
		}, nil
	}
}

// This is like a combination of http.ListenAndServe and http.ListenAndServeTLS, which also
// uses ThrottledListen to limit the number of open HTTP connections.
func ListenAndServeHTTP(ctx context.Context, addr string, connLimit uint, certFile, keyFile string, handler http.Handler,
	readTimeout, writeTimeout, readHeaderTimeout, idleTimeout time.Duration, http2Enabled bool,
	tlsMinVersion uint16) (serveFn func() error, listenerAddr net.Addr, server *http.Server, err error) {
	config, err := MakeTLSConfig(certFile, keyFile, tlsMinVersion)
	if config != nil {
		protocolsEnabled := []string{"http/1.1"}
		if http2Enabled {
			protocolsEnabled = []string{"h2", "http/1.1"}
		}
		config.NextProtos = protocolsEnabled
		InfofCtx(ctx, KeyHTTP, "Protocols enabled: %v on %v", config.NextProtos, SD(addr))
	}

	// Callback that turns off TCP NODELAY option when a client transitions to a WebSocket:
	connStateFunc := func(clientConn net.Conn, state http.ConnState) {
		if state == http.StateHijacked {
			turnOffNoDelay(ctx, clientConn)
		}
	}

	listener, err := ThrottledListen("tcp", addr, connLimit)
	if err != nil {
		return nil, nil, nil, err
	}
	if config != nil {
		listener = tls.NewListener(listener, config)
	}
	server = &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		ConnState:         connStateFunc,
	}

	serveFn = func() error {
		defer func() {
			_ = listener.Close()
		}()
		err := server.Serve(listener)
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}

	return serveFn, listener.Addr(), server, nil
}

type throttledListener struct {
	net.Listener
	active uint
	limit  uint
	lock   *sync.Cond
}

// Equivalent to net.Listen except that the returned listener allows only a limited number of open
// connections at a time. When the limit is reached it will block until some are closed before
// accepting any more.
// If the 'limit' parameter is 0, there is no limit and the behavior is identical to net.Listen.
func ThrottledListen(protocol string, addr string, limit uint) (net.Listener, error) {
	listener, err := net.Listen(protocol, addr)
	if err != nil || limit <= 0 {
		return listener, err
	}
	return &throttledListener{
		Listener: listener,
		limit:    limit,
		lock:     sync.NewCond(&sync.Mutex{}),
	}, nil
}

func (tl *throttledListener) Accept() (net.Conn, error) {
	conn, err := tl.Listener.Accept()
	if err == nil {
		// Wait until the number of active connections drops below the limit:
		tl.lock.L.Lock()
		for tl.active >= tl.limit {
			tl.lock.Wait()
		}
		tl.active++
		tl.lock.L.Unlock()
	}
	return &throttleConn{conn, tl}, err
}

func (tl *throttledListener) connFinished() {
	tl.lock.L.Lock()
	tl.active--
	if tl.active == tl.limit-1 {
		tl.lock.Signal()
	}
	tl.lock.L.Unlock()
}

// Wrapper for net.Conn that notifies the throttledListener when it's been closed
type throttleConn struct {
	net.Conn
	listener *throttledListener
}

func (conn *throttleConn) Close() error {
	err := conn.Conn.Close()
	conn.listener.connFinished()
	return err
}
