package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"
)

func dummyCallbackURL(s string, b bool) string {
	return ""
}

type headers map[jose.HeaderKey]interface{}

func createTestJWT(t *testing.T, alg jose.SignatureAlgorithm, key interface{}, headers headers, claims jwt.Claims) string {
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

const anyError = "SGW_TEST_ANY_ERROR"

func TestJWTVerifyToken(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)

	test := func(provider LocalJWTAuthProvider, token string, expectedError string) func(t *testing.T) {
		return func(t *testing.T) {
			_, err := provider.verifyToken(base.TestCtx(t), token, dummyCallbackURL)
			switch expectedError {
			case "":
				assert.NoError(t, err)
			case anyError:
				assert.Error(t, err)
			default:
				assert.Error(t, err)
				assert.Contains(t, err.Error(), expectedError)
			}
		}
	}

	testRSAKeypair, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	testRSAJWK := jose.JSONWebKey{
		Key:       testRSAKeypair.Public(),
		Use:       "sig",
		Algorithm: "RS256",
		KeyID:     "rsa",
	}

	// Copy of testRSAJWK with use="enc"
	testEncRSAJWK := jose.JSONWebKey{
		Key:       testRSAKeypair.Public(),
		Use:       "enc",
		Algorithm: "RS256",
		KeyID:     "rsa-enc",
	}

	// Valid RSA keypair that isn't registered with the provider
	testExtraKeypair, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	testExtraJWK := jose.JSONWebKey{
		Key:       testRSAKeypair.Public(),
		Use:       "sig",
		Algorithm: "RS256",
		KeyID:     "rsa",
	}

	testECKeypair, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	testECJWK := jose.JSONWebKey{
		Key:       testECKeypair.Public(),
		Use:       "sig",
		Algorithm: "ES256",
		KeyID:     "ec",
	}

	common := JWTConfigCommon{
		Issuer:   "testIss",
		ClientID: base.StringPtr("testAud"),
	}
	baseProvider := LocalJWTAuthProvider{
		JWTConfigCommon: common,
		Algorithms:      JWTAlgList{"RS256", "ES256"},
		Keys:            []jose.JSONWebKey{testRSAJWK, testECJWK, testEncRSAJWK},
	}

	t.Run("garbage", test(baseProvider, "INVALID", anyError))

	t.Run("string with two dots", test(baseProvider, "foo.bar.baz", anyError))

	// "e30" is "{}" url-base64-encoded
	t.Run("empty JSON objects", test(baseProvider, "e30.e30.e30", anyError))

	t.Run("valid RSA", test(baseProvider, createTestJWT(t, jose.RS256, testRSAKeypair, headers{
		"kid": testRSAJWK.KeyID,
	}, jwt.Claims{
		Issuer:   "testIss",
		Audience: jwt.Audience{"testAud"},
	}), ""))

	t.Run("valid EC", test(baseProvider, createTestJWT(t, jose.ES256, testECKeypair, headers{
		"kid": testECJWK.KeyID,
	}, jwt.Claims{
		Issuer:   "testIss",
		Audience: jwt.Audience{"testAud"},
	}), ""))

	invalidSignature := createTestJWT(t, jose.RS256, testRSAKeypair, headers{
		"kid": testRSAJWK.KeyID,
	}, jwt.Claims{
		Issuer:   "testIss",
		Audience: jwt.Audience{"testAud"},
	})
	invalidSignature += "INVALID"
	t.Run("valid JWT invalid signature", test(baseProvider, invalidSignature, anyError))

	t.Run("valid JWT signed with an unknown key", test(baseProvider, createTestJWT(t, jose.RS256, testExtraKeypair, headers{
		"kid": testExtraJWK.KeyID,
	}, jwt.Claims{
		Issuer:   "testIss",
		Audience: jwt.Audience{"testAud"},
	}), anyError))

	t.Run("valid JWT signed with a mismatching KID", test(baseProvider, createTestJWT(t, jose.RS256, testExtraKeypair, headers{
		"kid": testRSAJWK.KeyID,
	}, jwt.Claims{
		Issuer:   "testIss",
		Audience: jwt.Audience{"testAud"},
	}), anyError))

	t.Run("valid RSA signed with key with use=enc", test(baseProvider, createTestJWT(t, jose.RS256, testRSAKeypair, headers{
		"kid": testEncRSAJWK.KeyID,
	}, jwt.Claims{
		Issuer:   "testIss",
		Audience: jwt.Audience{"testAud"},
	}), anyError))

	// header: alg=none
	t.Run("valid JWT with alg none", test(baseProvider, `eyJhbGciOiJub25lIn0.eyJhdWQiOlsidGVzdEF1ZCJdLCJpc3MiOiJ0ZXN0SXNzIn0.`, "unsupported signing algorithm"))
	// header: alg=HS256
	t.Run("valid JWT with alg HS256", test(baseProvider, `eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOlsidGVzdEF1ZCJdLCJpc3MiOiJ0ZXN0SXNzIn0.gbdmOrzJ2CT01ABybPN-_dwXwv8_8iMEj4HNPtBqQjI`, "unsupported signing algorithm"))
	// header: alg=RS256, kid=rsa
	t.Run("valid JWT with no signature", test(baseProvider, `eyJhbGciOiJSUzI1NiIsImtpZCI6InJzYSJ9.eyJhdWQiOlsidGVzdEF1ZCJdLCJpc3MiOiJ0ZXN0SXNzIn0.`, anyError))

	t.Run("valid RSA with invalid issuer", test(baseProvider, createTestJWT(t, jose.RS256, testRSAKeypair, headers{
		"kid": testRSAJWK.KeyID,
	}, jwt.Claims{
		Issuer:   "nonsense",
		Audience: jwt.Audience{"testAud"},
	}), "invalid issuer"))
}
