package auth

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"
)

// These are not in jwt_test.go to allow use in tests from other packages.

type JWTHeaders map[jose.HeaderKey]interface{}

// CreateTestJWT creates and signs a valid JWT with the given headers and claims.
// The key must be valid for use with gopkg.in/square/go-jose.v2 (https://pkg.go.dev/gopkg.in/square/go-jose.v2#readme-supported-key-types),
// and the alg must match the key.
func CreateTestJWT(t *testing.T, alg jose.SignatureAlgorithm, key interface{}, headers JWTHeaders, claims map[string]interface{}) string {
	t.Helper()

	signerOpts := new(jose.SignerOptions)
	for key, val := range headers {
		signerOpts.WithHeader(key, val)
	}

	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: alg,
		Key:       key,
	}, signerOpts)
	require.NoError(t, err, "failed to create signer")

	tok, err := jwt.Signed(signer).Claims(claims).CompactSerialize()
	require.NoError(t, err, "failed to serialize JWT")
	return tok
}
