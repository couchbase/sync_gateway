package auth

import (
	"log"
	"strings"
	"testing"

	"github.com/coreos/go-oidc/jose"
	"github.com/stretchr/testify/assert"
)

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
	header := GetStandardHeaderAsJSON()
	payload, _ := toJson(claims())
	token := GetBearerToken(header, payload, "secret")
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
	header := GetStandardHeaderAsJSON()
	payload, _ := toJson(claims())
	token := GetBearerToken(header, payload, "secret")
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
	header := GetStandardHeaderAsJSON()
	payload, _ := toJson(claims())
	token := GetBearerToken(header, payload, "secret")
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
	header := GetStandardHeaderAsJSON()
	payload, _ := toJson(claims())
	token := GetBearerToken(header, payload, "secret")
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
	header := GetStandardHeaderAsJSON()
	payload, _ := toJson(claims())
	token := GetBearerToken(header, payload, "secret")
	return token
}

func TestGetJWTIdentity(t *testing.T) {
	// Parse the mocked JWS token.
	token := mockGoodToken()
	jws, err := jose.ParseJWS(token)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(token, ".")
	assert.NotNil(t, parts)
	assert.Equal(t, parts[0], jws.RawHeader)
	assert.Equal(t, parts[1], jws.RawPayload)

	assert.NotNil(t, jws.Header)
	assert.NotNil(t, jws.Payload)
	assert.NotNil(t, jws.Signature)

	jwt := jose.JWT{
		RawHeader:  jws.RawHeader,
		Header:     jws.Header,
		RawPayload: jws.RawPayload,
		Payload:    jws.Payload,
		Signature:  jws.Signature}

	// Check Identity; ID, Email, and ExpiresAt.
	identity, err := GetJWTIdentity(jwt)
	assert.NoError(t, err)
	assert.Empty(t, identity.Name)

	assert.NotEmpty(t, identity.ID)
	assert.NotEmpty(t, identity.Email)
	assert.NotEmpty(t, identity.ExpiresAt)
}

func TestGetJWTExpiry(t *testing.T) {
	// Parse the mocked JWS token.
	token := mockGoodToken()
	jws, err := jose.ParseJWS(token)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(token, ".")
	assert.NotNil(t, parts)
	assert.Equal(t, parts[0], jws.RawHeader)
	assert.Equal(t, parts[1], jws.RawPayload)

	assert.NotNil(t, jws.Header)
	assert.NotNil(t, jws.Payload)
	assert.NotNil(t, jws.Signature)

	jwt := jose.JWT{
		RawHeader:  jws.RawHeader,
		Header:     jws.Header,
		RawPayload: jws.RawPayload,
		Payload:    jws.Payload,
		Signature:  jws.Signature}

	// Check claim (Identity.ExpiresAt) for the JWT
	expiresAt, err := GetJWTExpiry(jwt)
	assert.NoError(t, err)
	assert.NotNil(t, expiresAt)
}

func TestGetJWTExpiryWithNoIdentity(t *testing.T) {
	jwt := jose.JWT{}
	expiresAt, err := GetJWTExpiry(jwt)
	assert.Error(t, err)
	assert.NotNil(t, expiresAt)
}

func TestGetJWTIssuer(t *testing.T) {
	// Parse the mocked JWS token.
	token := mockGoodToken()
	jws, err := jose.ParseJWS(token)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(token, ".")
	assert.NotNil(t, parts)
	assert.Equal(t, parts[0], jws.RawHeader)
	assert.Equal(t, parts[1], jws.RawPayload)

	assert.NotNil(t, jws.Header)
	assert.NotNil(t, jws.Payload)
	assert.NotNil(t, jws.Signature)

	jwt := jose.JWT{
		RawHeader:  jws.RawHeader,
		Header:     jws.Header,
		RawPayload: jws.RawPayload,
		Payload:    jws.Payload,
		Signature:  jws.Signature}

	// Check JWT issuer details; contains multiple entries for audience.
	issuer, audiences, err := GetJWTIssuer(jwt)
	assert.NoError(t, err)
	assert.NotNil(t, audiences)
	assert.True(t, len(audiences) > 0)
	assert.NotNil(t, issuer)
}

func TestGetJWTIssuerWithSingleAudience(t *testing.T) {
	// Parse the mocked JWS token.
	token := mockTokenWithSingleAudience()
	jws, err := jose.ParseJWS(token)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(token, ".")
	assert.NotNil(t, parts)
	assert.Equal(t, parts[0], jws.RawHeader)
	assert.Equal(t, parts[1], jws.RawPayload)

	assert.NotNil(t, jws.Header)
	assert.NotNil(t, jws.Payload)
	assert.NotNil(t, jws.Signature)

	jwt := jose.JWT{
		RawHeader:  jws.RawHeader,
		Header:     jws.Header,
		RawPayload: jws.RawPayload,
		Payload:    jws.Payload,
		Signature:  jws.Signature}

	// Check JWT issuer details
	issuer, audiences, err := GetJWTIssuer(jwt)
	log.Printf("audiences:%v", audiences)
	assert.NoError(t, err)
	assert.NotNil(t, audiences)
	assert.Equal(t, 1, len(audiences))
	assert.NotNil(t, issuer)
}

func TestGetJWTIssuerWithNoAudience(t *testing.T) {
	// Parse the mocked JWS token.
	token := mockGoodTokenWithNoAudience()
	jws, err := jose.ParseJWS(token)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(token, ".")
	assert.NotNil(t, parts)
	assert.Equal(t, parts[0], jws.RawHeader)
	assert.Equal(t, parts[1], jws.RawPayload)

	assert.NotNil(t, jws.Header)
	assert.NotNil(t, jws.Payload)
	assert.NotNil(t, jws.Signature)

	jwt := jose.JWT{
		RawHeader:  jws.RawHeader,
		Header:     jws.Header,
		RawPayload: jws.RawPayload,
		Payload:    jws.Payload,
		Signature:  jws.Signature}

	// Check JWT issuer details
	issuer, audiences, err := GetJWTIssuer(jwt)
	assert.NoError(t, err)
	assert.Nil(t, audiences)
	assert.Equal(t, 0, len(audiences))
	assert.Empty(t, issuer)
}

func TestGetJWTIssuerWithNoIssuer(t *testing.T) {
	// Parse the mocked JWS token.
	token := mockTokenWithNoIssuer()
	jws, err := jose.ParseJWS(token)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(token, ".")
	assert.NotNil(t, parts)
	assert.Equal(t, parts[0], jws.RawHeader)
	assert.Equal(t, parts[1], jws.RawPayload)

	assert.NotNil(t, jws.Header)
	assert.NotNil(t, jws.Payload)
	assert.NotNil(t, jws.Signature)

	jwt := jose.JWT{
		RawHeader:  jws.RawHeader,
		Header:     jws.Header,
		RawPayload: jws.RawPayload,
		Payload:    jws.Payload,
		Signature:  jws.Signature}

	// Check JWT issuer details
	issuer, audiences, err := GetJWTIssuer(jwt)
	assert.Error(t, err)
	assert.Nil(t, audiences)
	assert.Equal(t, 0, len(audiences))
	assert.Empty(t, issuer)
}

func TestGetJWTIssuerWithBadIssuer(t *testing.T) {
	// Parse the mocked JWS token.
	token := mockTokenWithBadIssuer()
	jws, err := jose.ParseJWS(token)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(token, ".")
	assert.NotNil(t, parts)
	assert.Equal(t, parts[0], jws.RawHeader)
	assert.Equal(t, parts[1], jws.RawPayload)

	assert.NotNil(t, jws.Header)
	assert.NotNil(t, jws.Payload)
	assert.NotNil(t, jws.Signature)

	jwt := jose.JWT{
		RawHeader:  jws.RawHeader,
		Header:     jws.Header,
		RawPayload: jws.RawPayload,
		Payload:    jws.Payload,
		Signature:  jws.Signature}

	// Check JWT issuer details
	issuer, audiences, err := GetJWTIssuer(jwt)
	assert.Error(t, err)
	assert.Nil(t, audiences)
	assert.Equal(t, 0, len(audiences))
	assert.Empty(t, issuer)
}

func TestGetJWTIssuerWithNoClaims(t *testing.T) {
	// Parse the mocked JWS token.
	token := mockGoodToken()
	jws, err := jose.ParseJWS(token)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(token, ".")
	assert.NotNil(t, parts)
	assert.Equal(t, parts[0], jws.RawHeader)
	assert.Equal(t, parts[1], jws.RawPayload)

	assert.NotNil(t, jws.Header)
	assert.NotNil(t, jws.Payload)
	assert.NotNil(t, jws.Signature)

	jwt := jose.JWT{
		RawHeader: jws.RawHeader,
		Header:    jws.Header,
		Signature: jws.Signature}

	// Check JWT issuer details
	issuer, audiences, err := GetJWTIssuer(jwt)
	assert.Error(t, err)
	assert.Nil(t, audiences)
	assert.Equal(t, 0, len(audiences))
	assert.Empty(t, issuer)
}

func TestGetJWTIssuerWithNoIdentity(t *testing.T) {
	jwt := jose.JWT{}
	issuer, audiences, err := GetJWTIssuer(jwt)
	assert.Error(t, err)
	assert.Nil(t, audiences)
	assert.Equal(t, 0, len(audiences))
	assert.Empty(t, issuer)
}
