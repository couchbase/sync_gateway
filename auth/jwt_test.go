package auth

import (
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

	// Check JWT issuer details
	issuer, audiences, err := GetJWTIssuer(jwt)
	assert.NoError(t, err)
	assert.NotNil(t, audiences)
	assert.True(t, len(audiences) > 0)
	assert.NotNil(t, issuer)
}
