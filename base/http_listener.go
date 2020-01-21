package base

import (
	"crypto/tls"
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

// This is like a combination of http.ListenAndServe and http.ListenAndServeTLS, which also
// uses ThrottledListen to limit the number of open HTTP connections.
func ListenAndServeHTTP(addr string, connLimit int, certFile *string, keyFile *string, handler http.Handler,
	readTimeout *int, writeTimeout *int, readHeaderTimeout *int, idleTimeout *int, http2Enabled bool,
	tlsMinVersion uint16) error {
	var config *tls.Config
	if certFile != nil {
		config = &tls.Config{}
		config.MinVersion = tlsMinVersion
		protocolsEnabled := []string{"http/1.1"}
		if http2Enabled {
			protocolsEnabled = []string{"h2", "http/1.1"}
		}
		config.NextProtos = protocolsEnabled
		Infof(KeyHTTP, "Protocols enabled: %v on %v", config.NextProtos, SD(addr))
		config.Certificates = make([]tls.Certificate, 1)
		var err error
		config.Certificates[0], err = tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			return err
		}
	}
	listener, err := ThrottledListen("tcp", addr, connLimit)
	if err != nil {
		return err
	}
	if config != nil {
		listener = tls.NewListener(listener, config)
	}
	defer func() { _ = listener.Close() }()
	server := &http.Server{Addr: addr, Handler: handler}
	if readTimeout != nil {
		server.ReadTimeout = time.Duration(*readTimeout) * time.Second
	}
	if writeTimeout != nil {
		server.WriteTimeout = time.Duration(*writeTimeout) * time.Second
	}
	if readHeaderTimeout != nil {
		server.ReadHeaderTimeout = time.Duration(*readHeaderTimeout) * time.Second
	} else {
		server.ReadHeaderTimeout = DefaultReadHeaderTimeout
	}
	if idleTimeout != nil {
		server.IdleTimeout = time.Duration(*idleTimeout) * time.Second
	} else {
		server.IdleTimeout = DefaultIdleTimeout
	}
	return server.Serve(listener)
}

type throttledListener struct {
	net.Listener
	active int
	limit  int
	lock   *sync.Cond
}

// Equivalent to net.Listen except that the returned listener allows only a limited number of open
// connections at a time. When the limit is reached it will block until some are closed before
// accepting any more.
// If the 'limit' parameter is 0, there is no limit and the behavior is identical to net.Listen.
func ThrottledListen(protocol string, addr string, limit int) (net.Listener, error) {
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
