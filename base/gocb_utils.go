package base

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
)

// GoCBv2SecurityConfig returns a gocb.SecurityConfig to use when connecting given a CA Cert path.
func GoCBv2SecurityConfig(tlsSkipVerify *bool, caCertPath string) (sc gocb.SecurityConfig, err error) {
	var certPool *x509.CertPool = nil
	if tlsSkipVerify == nil || !*tlsSkipVerify { // Add certs if ServerTLSSkipVerify is not set
		certPool, err = getRootCAs(caCertPath)
		if err != nil {
			return sc, err
		}
		tlsSkipVerify = BoolPtr(false)
	}
	sc.TLSRootCAs = certPool
	sc.TLSSkipVerify = *tlsSkipVerify
	return sc, nil
}

// GoCBv2Authenticator returns a gocb.Authenticator to use when connecting given a set of credentials.
func GoCBv2Authenticator(username, password, certPath, keyPath string) (a gocb.Authenticator, err error) {
	if certPath != "" && keyPath != "" {
		cert, certLoadErr := tls.LoadX509KeyPair(certPath, keyPath)
		if certLoadErr != nil {
			return nil, certLoadErr
		}
		return gocb.CertificateAuthenticator{
			ClientCertificate: &cert,
		}, nil
	}

	return gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	}, nil
}

// GoCBv2TimeoutsConfig returns a gocb.TimeoutsConfig to use when connecting.
func GoCBv2TimeoutsConfig(bucketOpTimeout, viewQueryTimeout *time.Duration) (tc gocb.TimeoutsConfig) {

	opTimeout := DefaultGocbV2OperationTimeout
	if bucketOpTimeout != nil {
		opTimeout = *bucketOpTimeout
	}
	tc.KVTimeout = opTimeout
	tc.ManagementTimeout = opTimeout
	tc.ConnectTimeout = opTimeout

	if viewQueryTimeout != nil {
		tc.QueryTimeout = *viewQueryTimeout
		tc.ViewTimeout = *viewQueryTimeout
	}
	return tc
}

// goCBv2FailFastRetryStrategy represents a strategy that will never retry.
type goCBv2FailFastRetryStrategy struct{}

var _ gocb.RetryStrategy = &goCBv2FailFastRetryStrategy{}

func (rs *goCBv2FailFastRetryStrategy) RetryAfter(req gocb.RetryRequest, reason gocb.RetryReason) gocb.RetryAction {
	return &gocb.NoRetryRetryAction{}
}

// GOCBCORE Utilities

// CertificateAuthenticator allows for certificate auth in gocbcore
type CertificateAuthenticator struct {
	ClientCertificate *tls.Certificate
}

func (ca CertificateAuthenticator) SupportsTLS() bool {
	return true
}
func (ca CertificateAuthenticator) SupportsNonTLS() bool {
	return false
}
func (ca CertificateAuthenticator) Certificate(req gocbcore.AuthCertRequest) (*tls.Certificate, error) {
	return ca.ClientCertificate, nil
}
func (ca CertificateAuthenticator) Credentials(req gocbcore.AuthCredsRequest) ([]gocbcore.UserPassPair, error) {
	return []gocbcore.UserPassPair{{
		Username: "",
		Password: "",
	}}, nil
}

// GoCBCoreAuthConfig returns a gocbcore.AuthProvider to use when connecting given a set of credentials via a gocbcore agent.
func GoCBCoreAuthConfig(username, password, certPath, keyPath string) (a gocbcore.AuthProvider, err error) {
	if certPath != "" && keyPath != "" {
		cert, certLoadErr := tls.LoadX509KeyPair(certPath, keyPath)
		if certLoadErr != nil {
			return nil, err
		}
		return CertificateAuthenticator{
			ClientCertificate: &cert,
		}, nil
	}

	return &gocbcore.PasswordAuthProvider{
		Username: username,
		Password: password,
	}, nil
}

func GoCBCoreTLSRootCAProvider(tlsSkipVerify *bool, caCertPath string) (wrapper func() *x509.CertPool, err error) {
	var certPool *x509.CertPool = nil
	if tlsSkipVerify == nil || !*tlsSkipVerify { // Add certs if ServerTLSSkipVerify is not set
		certPool, err = getRootCAs(caCertPath)
		if err != nil {
			return nil, err
		}
	}

	return func() *x509.CertPool {
		return certPool
	}, nil
}

// getRootCAs gets generates a cert pool from the certs at caCertPath. If caCertPath is empty, the systems cert pool is used.
// If an error happens when retrieving the system cert pool, it is logged (not returned) and an empty (not nil) cert pool is returned.
func getRootCAs(caCertPath string) (*x509.CertPool, error) {
	if caCertPath != "" {
		rootCAs := x509.NewCertPool()

		caCert, err := ioutil.ReadFile(caCertPath)
		if err != nil {
			return nil, err
		}

		ok := rootCAs.AppendCertsFromPEM(caCert)
		if !ok {
			return nil, errors.New("invalid CA cert")
		}

		return rootCAs, nil
	}

	rootCAs, err := x509.SystemCertPool()
	if err != nil {
		rootCAs = x509.NewCertPool()
		WarnfCtx(context.Background(), "Could not retrieve root CAs: %v", err)
	}
	return rootCAs, nil
}
