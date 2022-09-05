// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestGoCBv2SecurityConfig(t *testing.T) {
	// Mock fake root CA and client certificates for verification
	_, _, rootCertPath, _ := mockCertificatesAndKeys(t)

	tests := []struct {
		name           string
		tlsSkipVerify  *bool
		caCertPath     string
		expectCertPool bool // True if should not be empty, false if nil (true on windows asserts empty due to no System Root Pool)
		expectError    bool
	}{
		{
			name:           "TLS Skip Verify",
			tlsSkipVerify:  BoolPtr(true),
			caCertPath:     "",
			expectCertPool: false,
			expectError:    false,
		},
		{
			name:           "File does not exist",
			tlsSkipVerify:  BoolPtr(false),
			caCertPath:     "/var/lib/couchbase/unknown.root.ca.pem",
			expectCertPool: false,
			expectError:    true,
		},
		{
			name:           "Normal CA",
			tlsSkipVerify:  BoolPtr(false),
			caCertPath:     rootCertPath,
			expectCertPool: true,
			expectError:    false,
		},
		{
			name:           "Normal CA, TLSSkipVerify not set",
			tlsSkipVerify:  nil,
			caCertPath:     rootCertPath,
			expectCertPool: true,
			expectError:    false,
		},
		{
			name:           "Get root pool",
			tlsSkipVerify:  nil,
			caCertPath:     "",
			expectCertPool: true,
			expectError:    false,
		},
	}
	//
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sc, err := GoCBv2SecurityConfig(test.tlsSkipVerify, test.caCertPath)
			if test.expectError {
				assert.Error(t, err)
				assert.Nil(t, sc.TLSRootCAs)
				return
			}
			require.NotNil(t, sc)

			expectTLSSkipVerify := false
			if test.tlsSkipVerify != nil {
				expectTLSSkipVerify = *test.tlsSkipVerify
			}

			assert.Equal(t, expectTLSSkipVerify, sc.TLSSkipVerify)
			if test.expectCertPool == false {
				assert.Nil(t, sc.TLSRootCAs)
			} else { // Expect populated cert pool
				assert.NotEmpty(t, sc.TLSRootCAs)
			}
		})
	}
}

// Regression test for CBG-2230. Ensure that we return an error, rather than nil/nil, when given an invalid path to
// x.509 certs.
func TestGoCBCoreAuthConfigInvalidPaths(t *testing.T) {
	_, err := GoCBCoreAuthConfig("", "", "/non/existent/cert", "/non/existent/key")
	assert.Error(t, err)
}
