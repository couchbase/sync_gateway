// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2"
)

func dummyCallbackURL(_ string, _ bool) string {
	return ""
}

const anyError = "SGW_TEST_ANY_ERROR"

func TestJWTVerifyToken(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth)

	test := func(provider *LocalJWTAuthProvider, token string, expectedError string) func(t *testing.T) {
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

	const (
		testIssuer   = "testIssuer"
		testClientID = "testAud"
	)

	common := JWTConfigCommon{
		Issuer:   testIssuer,
		ClientID: base.StringPtr(testClientID),
	}
	baseProvider := LocalJWTAuthConfig{
		JWTConfigCommon: common,
		Algorithms:      []string{"RS256", "ES256"},
		Keys:            []jose.JSONWebKey{testRSAJWK, testECJWK, testEncRSAJWK},
		SkipExpiryCheck: base.BoolPtr(true),
	}.BuildProvider("test")
	providerWithExpiryCheck := LocalJWTAuthConfig{
		JWTConfigCommon: common,
		Algorithms:      []string{"RS256", "ES256"},
		Keys:            []jose.JSONWebKey{testRSAJWK, testECJWK, testEncRSAJWK},
		SkipExpiryCheck: base.BoolPtr(false),
	}.BuildProvider("test")

	t.Run("garbage", test(baseProvider, "INVALID", anyError))

	t.Run("string with two dots", test(baseProvider, "foo.bar.baz", anyError))

	// "e30" is "{}" url-base64-encoded
	t.Run("empty JSON objects", test(baseProvider, "e30.e30.e30", anyError))

	t.Run("valid RSA", test(baseProvider, CreateTestJWT(t, jose.RS256, testRSAKeypair, JWTHeaders{
		"kid": testRSAJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
	}), ""))

	t.Run("valid EC", test(baseProvider, CreateTestJWT(t, jose.ES256, testECKeypair, JWTHeaders{
		"kid": testECJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
	}), ""))

	t.Run("valid + expiry check", test(providerWithExpiryCheck, CreateTestJWT(t, jose.RS256, testRSAKeypair, JWTHeaders{
		"kid": testRSAJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
		"exp": time.Now().Add(time.Hour).Unix(),
		"iat": time.Now().Add(-time.Hour).Unix(),
		"nbf": time.Now().Add(-time.Hour).Unix(),
	}), ""))

	t.Run("valid but expired", test(providerWithExpiryCheck, CreateTestJWT(t, jose.RS256, testRSAKeypair, JWTHeaders{
		"kid": testRSAJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
		// 2000-01-01T00:00:00Z
		"exp": 946684800,
	}), "token is expired"))

	t.Run("valid but issued in the future", test(providerWithExpiryCheck, CreateTestJWT(t, jose.RS256, testRSAKeypair, JWTHeaders{
		"kid": testRSAJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
		// 3000-01-01T00:00:00Z
		"exp": 32503680000,
		"nbf": 32503680000,
	}), "before the nbf (not before) time"))

	invalidSignature := CreateTestJWT(t, jose.RS256, testRSAKeypair, JWTHeaders{
		"kid": testRSAJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
	})
	invalidSignature += "INVALID"
	t.Run("valid JWT invalid signature", test(baseProvider, invalidSignature, anyError))

	t.Run("valid JWT signed with an unknown key", test(baseProvider, CreateTestJWT(t, jose.RS256, testExtraKeypair, JWTHeaders{
		"kid": testExtraJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
	}), anyError))

	t.Run("valid JWT signed with a mismatching KID", test(baseProvider, CreateTestJWT(t, jose.RS256, testExtraKeypair, JWTHeaders{
		"kid": testRSAJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
	}), anyError))

	t.Run("valid RSA signed with key with use=enc", test(baseProvider, CreateTestJWT(t, jose.RS256, testRSAKeypair, JWTHeaders{
		"kid": testEncRSAJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
	}), anyError))

	// header: alg=none
	t.Run("valid JWT with alg none", test(baseProvider, `eyJhbGciOiJub25lIn0.eyJhdWQiOlsidGVzdEF1ZCJdLCJpc3MiOiJ0ZXN0SXNzdWVyIn0.`, "id token signed with unsupported algorithm"))
	// header: alg=HS256
	t.Run("valid JWT with alg HS256", test(baseProvider, `eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOlsidGVzdEF1ZCJdLCJpc3MiOiJ0ZXN0SXNzdWVyIn0.aPSuXUVKN1FNS53mw0Xw3a-SU2GVS98gHWEVrzTnQYM`, "id token signed with unsupported algorithm"))
	// header: alg=RS256, kid=rsa
	t.Run("valid JWT with no signature", test(baseProvider, `eyJhbGciOiJSUzI1NiIsImtpZCI6InJzYSJ9.eyJhdWQiOlsidGVzdEF1ZCJdLCJpc3MiOiJ0ZXN0SXNzdWVyIn0.`, "failed to verify signature"))

	t.Run("valid RSA with invalid issuer", test(baseProvider, CreateTestJWT(t, jose.RS256, testRSAKeypair, JWTHeaders{
		"kid": testRSAJWK.KeyID,
	}, map[string]interface{}{
		"iss": "nonsense",
		"aud": []string{testClientID},
	}), "id token issued by a different provider"))
}
