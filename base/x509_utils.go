package base

import (
	"context"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"sync/atomic"
)

// loadCertificatesFromFile returns a new CertPool with all the PEM-format certificates in the given file.
func loadCertificatesFromFile(path string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	certs, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load CA certificate from %s: %w", MD(path).Redact(), err)
	}
	ok := pool.AppendCertsFromPEM(certs)
	if !ok {
		return pool, fmt.Errorf("did not load any valid certificate from file %s: %w", MD(path).Redact(), err)
	}
	return pool, nil
}

// systemCertPool is used to lazy-load the system cert pool in getSystemCertPool
var systemCertPool atomic.Value

// getSystemCertPool returns a CertPool for the system root store.
func getSystemCertPool() *x509.CertPool {
	val := systemCertPool.Load()
	if val != nil {
		return val.(*x509.CertPool)
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		// Panic here is appropriate to ensure we fail closed and don't return a nil pool, which would disable
		// cert validation. Code inspection suggests that, as of Go 1.18.2, SystemCertPool always returns a nil err.
		PanicfCtx(context.TODO(), "Failed to load system X509 cert pool! %v", err)
	}
	systemCertPool.CompareAndSwap(nil, pool)
	return pool
}
