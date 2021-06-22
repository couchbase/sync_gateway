package base

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io/ioutil"
	"time"

	"github.com/couchbase/gocb"
	"github.com/couchbase/gocbcore"
)

// GoCBv2SecurityConfig returns a gocb.SecurityConfig to use when connecting given a CA Cert path.
func GoCBv2SecurityConfig(caCertPath string) (sc gocb.SecurityConfig, err error) {
	if caCertPath != "" {
		roots := x509.NewCertPool()
		cacert, err := ioutil.ReadFile(caCertPath)
		if err != nil {
			return sc, err
		}
		ok := roots.AppendCertsFromPEM(cacert)
		if !ok {
			return sc, errors.New("Invalid CA cert")
		}
		sc.TLSRootCAs = roots
	} else {
		sc.TLSSkipVerify = true
	}
	return sc, nil
}

// GoCBv2AuthenticatorConfig returns a gocb.Authenticator to use when connecting given a set of credentials.
func GoCBv2AuthenticatorConfig(username, password, certPath, keyPath string) (a gocb.Authenticator, isX509 bool, err error) {
	if certPath != "" && keyPath != "" {
		cert, certLoadErr := tls.LoadX509KeyPair(certPath, keyPath)
		if certLoadErr != nil {
			return nil, false, err
		}
		return gocb.CertificateAuthenticator{
			ClientCertificate: &cert,
		}, true, nil
	}

	return gocb.PasswordAuthenticator{
		Username: username,
		Password: password,
	}, false, nil
}

// GoCBv2TimeoutsConfig returns a gocb.TimeoutsConfig to use when connecting.
func GoCBv2TimeoutsConfig(bucketOpTimeout, viewQueryTimeout *time.Duration) (tc gocb.TimeoutsConfig) {
	if bucketOpTimeout != nil {
		tc.KVTimeout = *bucketOpTimeout
		tc.ManagementTimeout = *bucketOpTimeout
		tc.ConnectTimeout = *bucketOpTimeout
	}
	if viewQueryTimeout != nil {
		tc.QueryTimeout = *viewQueryTimeout
		tc.ViewTimeout = *viewQueryTimeout
	}
	return tc
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

func GoCBCoreTLSRootCAProvider(caCertPath string) (func() *x509.CertPool, error) {
	rootCAs, err := getRootCAs(caCertPath)
	if err != nil {
		return nil, err
	}

	return func() *x509.CertPool {
		return rootCAs
	}, nil
}

func getRootCAs(caCertPath string) (*x509.CertPool, error) {
	if caCertPath != "" {
		rootCAs := x509.NewCertPool()

		caCert, err := ioutil.ReadFile(caCertPath)
		if err != nil {
			return nil, err
		}

		ok := rootCAs.AppendCertsFromPEM(caCert)
		if !ok {
			return nil, errors.New("Invalid CA cert")
		}

		return rootCAs, nil
	}

	// We're purposefully ignoring the error here Due to the fact that the main error case is that this call is not
	// supported in Windows.
	rootCAs, _ := x509.SystemCertPool()
	return rootCAs, nil
}
