package auth

import (
	"log"
	"strings"
	"testing"

	"github.com/coreos/go-oidc/jose"
	"github.com/stretchr/testify/assert"
)

/**
JWT Token Mocked from https://jwt.io/#debugger
HEADER:ALGORITHM & TOKEN TYPE
{
  "alg": "HS256",
  "typ": "JWT"
}
PAYLOAD:DATA
{
  "id":"CB00912",
  "iss": "Couchbase, Inc.",
  "sub": "1234567890",
  "name": "John Wick",
  "aud": ["ebay", "comcast", "linkedin"],
  "iat": 1516239022,
  "exp": 1586239022,
  "email": "johnwick@couchbase.com"
}
VERIFY SIGNATURE:
HMACSHA256(
  base64UrlEncode(header) + "." +
  base64UrlEncode(payload),
  your-256-bit-secret)
*/

const token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IkNCMDA5MTIiLCJpc3MiOiJDb3VjaGJhc2UsIEluYy4iLCJzd" +
	"WIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gV2ljayIsImF1ZCI6WyJlYmF5IiwiY29tY2FzdCIsImxpbmtlZGluIl0sImlhdCI6M" +
	"TUxNjIzOTAyMiwiZXhwIjoxNTg2MjM5MDIyLCJlbWFpbCI6ImpvaG53aWNrQGNvdWNoYmFzZS5jb20ifQ.X7A3MAlaZscwth20plFDxv" +
	"OQ3VXBNnV-9JK0z4g0Z6U"

const tokenWithSingleAudience = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IkNCMDA5MTIiLCJpc3MiOiJDb3VjaGJh" +
	"c2UsIEluYy4iLCJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gV2ljayIsImF1ZCI6ImxpbmtlZGluIiwiaWF0IjoxNTE2MjM5M" +
	"DIyLCJleHAiOjE1ODYyMzkwMjIsImVtYWlsIjoiam9obndpY2tAY291Y2hiYXNlLmNvbSJ9.SaltmbXl3_0IyE3g1MIikjQRXuyNLUhZw" +
	"-P575pg-ac"

const tokenWithNoAudience = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IkNCMDA5MTIiLCJpc3MiOiJDb3VjaGJhc2Us" +
	"IEluYy4iLCJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gV2ljayIsImlhdCI6MTUxNjIzOTAyMiwiZXhwIjoxNTg2MjM5MDIyL" +
	"CJlbWFpbCI6ImpvaG53aWNrQGNvdWNoYmFzZS5jb20ifQ.2TdaiunHtgTY1RZsr0ItdmNLMWX5BgcdB6teiGdK_1o"

const tokenWithNoIssuer = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IkNCMDA5MTIiLCJzdWIiOiIxMjM0NTY3ODkwIi" +
	"wibmFtZSI6IkpvaG4gV2ljayIsImF1ZCI6WyJlYmF5IiwiY29tY2FzdCIsImxpbmtlZGluIl0sImlhdCI6MTUxNjIzOTAyMiwiZXhwIjo" +
	"xNTg2MjM5MDIyLCJlbWFpbCI6ImpvaG53aWNrQGNvdWNoYmFzZS5jb20ifQ.lCT0AE2EL8d9lJkBtVM7FI4QCHrnuKgSeiOZfEGtANM"

const tokenWithBadIssuer = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IkNCMDA5MTIiLCJpc3MiOnsidmFsdWUiOiJDb" +
	"3VjaGJhc2UsIEluYy4ifSwic3ViIjoiMTIzNDU2Nzg5MCIsIm5hbWUiOiJKb2huIFdpY2siLCJhdWQiOlsiZWJheSIsImNvbWNhc3QiLC" +
	"JsaW5rZWRpbiJdLCJpYXQiOjE1MTYyMzkwMjIsImV4cCI6MTU4NjIzOTAyMiwiZW1haWwiOiJqb2hud2lja0Bjb3VjaGJhc2UuY29tIn0" +
	".vquz1bpub2XakltcmRhiAmynRqmniO4I1uMuIPsvVR4"

func TestGetJWTIdentity(t *testing.T) {
	// Parse the mocked JWS token.
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
	jws, err := jose.ParseJWS(tokenWithSingleAudience)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(tokenWithSingleAudience, ".")
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
	jws, err := jose.ParseJWS(tokenWithNoAudience)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(tokenWithNoAudience, ".")
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
	jws, err := jose.ParseJWS(tokenWithNoIssuer)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(tokenWithNoIssuer, ".")
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
	jws, err := jose.ParseJWS(tokenWithBadIssuer)
	assert.NotNil(t, jws)
	assert.NoError(t, err)

	// Verify the header, payload, and signature.
	parts := strings.Split(tokenWithBadIssuer, ".")
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
