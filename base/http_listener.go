package base

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"
)

type ClientCertConfig struct {
	CA_cert string `json:"ca_cert"`         // Path to client Certificate Authority cert file
	State   string `json:"state,omitempty"` // "disable", "enable", or "mandatory"
}

// This is like a combination of http.ListenAndServe and http.ListenAndServeTLS, which also
// uses ThrottledListen to limit the number of open HTTP connections.
func ListenAndServeHTTP(addr string, connLimit int, certFile *string, keyFile *string, clientCerts *ClientCertConfig, handler http.Handler, readTimeout *int, writeTimeout *int, http2Enabled bool, tlsMinVersion uint16) error {
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

		if clientCerts != nil && clientCerts.State != "disable" {
			// TLS client certificate support:
			clientCAData, err := ioutil.ReadFile(clientCerts.CA_cert)
			if err != nil {
				return err
			}
			config.ClientCAs, err = parseCertsFromPEM(clientCAData, true)
			if config.ClientCAs == nil {
				return fmt.Errorf("Couldn't read certs from %s: %s", clientCerts.CA_cert, err.Error())
			}
			switch clientCerts.State {
			case "mandatory":
				config.ClientAuth = tls.RequireAndVerifyClientCert
				Infof(KeyHTTP, "HTTP client certs are mandatory, using CA from %s, on %v", clientCerts.CA_cert, SD(addr))
			case "enabled":
				config.ClientAuth = tls.VerifyClientCertIfGiven
				Infof(KeyHTTP, "HTTP client certs enabled, using CA from %s, on %v", clientCerts.CA_cert, SD(addr))
			default:
				return fmt.Errorf(`Invalid client_cert_auth.state: %q (must be "disable", "enable", or "mandatory")`, clientCerts.State)
			}
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

func parseCertsFromPEM(pemCerts []byte, mustBeCA bool) (certs *x509.CertPool, err error) {
	// Based on x509.CertPool.AppendCertsFromPem() (src/crypto/x509.cert_pool.go),
	// but returns errors.
	for len(pemCerts) > 0 {
		// Read the next PEM block:
		var block *pem.Block
		block, pemCerts = pem.Decode(pemCerts)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			continue
		}

		// Parse the cert:
		var cert *x509.Certificate
		cert, err = x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, err
		}
		if mustBeCA && !cert.IsCA {
			return nil, fmt.Errorf("Must contain only CA certs")
		}

		// Add the cert:
		if certs == nil {
			certs = x509.NewCertPool()
		}
		certs.AddCert(cert)
	}

	if certs == nil {
		err = fmt.Errorf("No certificates found in PEM file")
	}
	return
}
