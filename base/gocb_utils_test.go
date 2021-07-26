package base

import (
	"crypto/x509"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestGoCBv2SecurityConfig(t *testing.T) {
	// Mock fake root CA and client certificates for verification
	certPath, _, _, rootCertPath, _ := mockCertificatesAndKeys(t)

	// Remove the keys and certificates after verification
	defer func() {
		require.NoError(t, os.RemoveAll(certPath))
		require.False(t, DirExists(certPath), "Directory: %v shouldn't exists", certPath)
	}()

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
			} else if runtime.GOOS == "windows" && test.caCertPath == "" { // expect empty cert pool when getting root pool on windows
				assert.Equal(t, x509.NewCertPool(), sc.TLSRootCAs)
			} else { // Expect populated cert pool
				assert.NotEmpty(t, sc.TLSRootCAs)
			}
		})
	}
}
