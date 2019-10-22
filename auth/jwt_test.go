package auth

import (
	"fmt"
	"strings"
	"testing"

	"github.com/coreos/go-oidc/jose"
	"github.com/stretchr/testify/assert"
)

const jwtSeparator = "."

func mockGoodToken() string {
	// Mock up a payload or claim for token
	claims := func() map[string]interface{} {
		audience := [...]string{"ebay", "comcast", "linkedin"}
		claims := make(map[string]interface{})
		claims["id"] = "CB00912"
		claims["iss"] = "https://accounts.google.com"
		claims["sub"] = "1234567890"
		claims["name"] = "John Wick"
		claims["aud"] = audience
		claims["iat"] = 1516239022
		claims["exp"] = 1586239022
		claims["email"] = "johnwick@couchbase.com"
		return claims
	}
	header := getStandardHeaderAsJSON()
	payload, _ := toJson(claims())
	token := getBearerToken(header, payload, "secret")
	return token
}

func mockTokenWithBadIssuer() string {
	// Mock up a payload or claim for token
	claims := func() map[string]interface{} {
		audience := [...]string{"ebay", "comcast", "linkedin"}
		// Bad issuer; expected to be a type string
		issuer := 1234567890
		claims := make(map[string]interface{})
		claims["id"] = "CB00912"
		claims["iss"] = issuer
		claims["sub"] = "1234567890"
		claims["name"] = "John Wick"
		claims["aud"] = audience
		claims["iat"] = 1516239022
		claims["exp"] = 1586239022
		claims["email"] = "johnwick@couchbase.com"
		return claims
	}
	header := getStandardHeaderAsJSON()
	payload, _ := toJson(claims())
	token := getBearerToken(header, payload, "secret")
	return token
}

func mockTokenWithNoIssuer() string {
	// Mock up a payload or claim for token
	claims := func() map[string]interface{} {
		audience := [...]string{"ebay", "comcast", "linkedin"}
		// Bad issuer; expected to be a type string
		claims := make(map[string]interface{})
		claims["id"] = "CB00912"
		claims["sub"] = "1234567890"
		claims["name"] = "John Wick"
		claims["aud"] = audience
		claims["iat"] = 1516239022
		claims["exp"] = 1586239022
		claims["email"] = "johnwick@couchbase.com"
		return claims
	}
	header := getStandardHeaderAsJSON()
	payload, _ := toJson(claims())
	token := getBearerToken(header, payload, "secret")
	return token
}

func mockGoodTokenWithNoAudience() string {
	// Mock up a payload or claim for token
	claims := func() map[string]interface{} {
		claims := make(map[string]interface{})
		claims["id"] = "CB00912"
		claims["iss"] = "https://accounts.google.com"
		claims["sub"] = "1234567890"
		claims["name"] = "John Wick"
		claims["iat"] = 1516239022
		claims["exp"] = 1586239022
		claims["email"] = "johnwick@couchbase.com"
		return claims
	}
	header := getStandardHeaderAsJSON()
	payload, _ := toJson(claims())
	token := getBearerToken(header, payload, "secret")
	return token
}

func mockTokenWithSingleAudience() string {
	// Mock up a payload or claim for token
	claims := func() map[string]interface{} {
		claims := make(map[string]interface{})
		claims["id"] = "CB00912"
		claims["iss"] = "https://accounts.google.com"
		claims["sub"] = "1234567890"
		claims["name"] = "John Wick"
		claims["aud"] = "comcast"
		claims["iat"] = 1516239022
		claims["exp"] = 1586239022
		claims["email"] = "johnwick@couchbase.com"
		return claims
	}
	header := getStandardHeaderAsJSON()
	payload, _ := toJson(claims())
	token := getBearerToken(header, payload, "secret")
	return token
}

func TestGetJWTIssuerIdentityExpiry(t *testing.T) {
	tests := map[int]struct {
		sequence int
		name     string
		token    string
		jwt      jose.JWT
		issuer   string
		audience int
		err      error
	}{
		1: {name: "Test GetJWTIssuer with no identity", token: ""},
		2: {name: "Test GetJWTIssuer with no claims", token: mockGoodToken()},
		3: {name: "Test GetJWTIssuer with bad issuer in JWT claims", token: mockTokenWithBadIssuer()},
		4: {name: "Test GetJWTIssuer with no issuer in JWT claims", token: mockTokenWithNoIssuer()},
		5: {name: "Test GetJWTIssuer with no audience in JWT claims", token: mockGoodTokenWithNoAudience()},
		6: {name: "Test GetJWTIssuer with single audience in JWT claims", token: mockTokenWithSingleAudience(), audience: 1},
		7: {name: "Test GetJWTExpiry with good token", token: mockGoodToken()},
		8: {name: "Test GetJWTIdentity; ID, Email, and ExpiresAt", token: mockGoodToken()},
		9: {name: "Test GetJWTExpiry with no identity", token: ""},
	}

	for seq, tc := range tests {
		t.Run(fmt.Sprintf("%v: %v", seq, tc.name), func(t *testing.T) {
			if tc.token != "" {
				jws, err := jose.ParseJWS(tc.token)
				assert.NotNil(t, jws)
				assert.NoError(t, err)

				parts := strings.Split(tc.token, jwtSeparator)
				assert.NotNil(t, parts)
				assert.Equal(t, parts[0], jws.RawHeader)
				assert.Equal(t, parts[1], jws.RawPayload)

				assert.NotNil(t, jws.Header)
				assert.NotNil(t, jws.Payload)
				assert.NotNil(t, jws.Signature)

				if seq == 2 { // Shouldn't set claims in jwt.
					tc.jwt = jose.JWT{
						RawHeader: jws.RawHeader,
						Header:    jws.Header,
						Signature: jws.Signature}
				} else {
					tc.jwt = jose.JWT{
						RawHeader:  jws.RawHeader,
						Header:     jws.Header,
						RawPayload: jws.RawPayload,
						Payload:    jws.Payload,
						Signature:  jws.Signature}
				}
			} else {
				tc.jwt = jose.JWT{}
			}

			switch seq {
			case 1:
			case 2:
			case 3:
			case 4:
				issuer, audiences, err := GetJWTIssuer(tc.jwt)
				assert.Error(t, err)
				assert.Nil(t, audiences)
				assert.Equal(t, tc.audience, len(audiences))
				assert.Empty(t, issuer)
			case 5:
				issuer, audiences, err := GetJWTIssuer(tc.jwt)
				assert.NoError(t, err)
				assert.Nil(t, audiences)
				assert.Equal(t, tc.audience, len(audiences))
				assert.Empty(t, issuer)
			case 6:
				issuer, audiences, err := GetJWTIssuer(tc.jwt)
				assert.NoError(t, err)
				assert.NotNil(t, audiences)
				assert.Equal(t, tc.audience, len(audiences))
				assert.NotNil(t, issuer)
			case 7:
				expiresAt, err := GetJWTExpiry(tc.jwt)
				assert.NoError(t, err)
				assert.NotNil(t, expiresAt)
			case 8:
				identity, err := GetJWTIdentity(tc.jwt)
				assert.NoError(t, err)
				assert.Empty(t, identity.Name)
				assert.NotEmpty(t, identity.ID)
				assert.NotEmpty(t, identity.Email)
				assert.NotEmpty(t, identity.ExpiresAt)
			case 9:
				expiresAt, err := GetJWTExpiry(tc.jwt)
				assert.Error(t, err)
				assert.NotNil(t, expiresAt)
			}
		})
	}
}
