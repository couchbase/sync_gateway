package base

import (
	"crypto/tls"
	"expvar"
	"net"
	"net/http"
	"sync"
	"time"
)

var httpListenerExpvars *expvar.Map
var maxWaitExpvar, maxActiveExpvar IntMax

func init() {
	httpListenerExpvars = expvar.NewMap("syncGateway_httpListener")
	httpListenerExpvars.Set("max_wait", &maxWaitExpvar)
	httpListenerExpvars.Set("max_active", &maxActiveExpvar)
}

// This is like a combination of http.ListenAndServe and http.ListenAndServeTLS, which also
// uses ThrottledListen to limit the number of open HTTP connections.
func ListenAndServeHTTP(addr string, connLimit int, certFile *string, keyFile *string, handler http.Handler, readTimeout *int, writeTimeout *int, http2Enabled bool) error {
	var config *tls.Config
	if certFile != nil {
		config = &tls.Config{}
		config.MinVersion = tls.VersionTLS10 // Disable SSLv3 due to POODLE vulnerability
		protocolsEnabled := []string{"http/1.1"}
		if http2Enabled {
			protocolsEnabled = []string{"h2", "http/1.1"}
		}
		config.NextProtos = protocolsEnabled
		LogTo("HTTP", "Protocols enabled: %v on %v", config.NextProtos, addr)
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
	defer listener.Close()
	server := &http.Server{Addr: addr, Handler: handler}
	if readTimeout != nil {
		server.ReadTimeout = time.Duration(*readTimeout) * time.Second
	}
	if writeTimeout != nil {
		server.WriteTimeout = time.Duration(*writeTimeout) * time.Second
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
		waitStart := time.Now()
		tl.lock.L.Lock()
		for tl.active >= tl.limit {
			tl.lock.Wait()
		}
		tl.active++
		maxActiveExpvar.SetIfMax(int64(tl.active))
		tl.lock.L.Unlock()
		waitTime := time.Since(waitStart)
		maxWaitExpvar.SetIfMax(int64(waitTime))
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
