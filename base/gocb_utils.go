// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
)

// GoCBv2SecurityConfig returns a gocb.SecurityConfig to use when connecting given a CA Cert path.
func GoCBv2SecurityConfig(ctx context.Context, tlsSkipVerify *bool, caCertPath string) (sc gocb.SecurityConfig, err error) {
	var certPool *x509.CertPool = nil
	if tlsSkipVerify == nil || !*tlsSkipVerify { // Add certs if ServerTLSSkipVerify is not set
		certPool, err = getRootCAs(ctx, caCertPath)
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
func GoCBCoreAuthConfig(username, password, certPath, keyPath string) (gocbcore.AuthProvider, error) {
	if certPath != "" && keyPath != "" {
		cert, certLoadErr := tls.LoadX509KeyPair(certPath, keyPath)
		if certLoadErr != nil {
			return nil, certLoadErr
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

func GoCBCoreTLSRootCAProvider(ctx context.Context, tlsSkipVerify *bool, caCertPath string) (wrapper func() *x509.CertPool, err error) {
	var certPool *x509.CertPool = nil
	if tlsSkipVerify == nil || !*tlsSkipVerify { // Add certs if ServerTLSSkipVerify is not set
		certPool, err = getRootCAs(ctx, caCertPath)
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
func getRootCAs(ctx context.Context, caCertPath string) (*x509.CertPool, error) {
	if caCertPath != "" {
		rootCAs := x509.NewCertPool()

		caCert, err := os.ReadFile(caCertPath)
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
		WarnfCtx(ctx, "Could not retrieve root CAs: %v", err)
	}
	return rootCAs, nil
}
