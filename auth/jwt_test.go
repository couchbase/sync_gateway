package auth

import (
	"testing"

	"github.com/coreos/go-oidc/jose"
	"github.com/stretchr/testify/assert"
)

func mockHeader() *jose.JOSEHeader {
	header := jose.JOSEHeader{
		jose.HeaderKeyAlgorithm: jose.AlgHS256,
		jose.HeaderMediaType:    "JWT"}
	return &header
}

func mockClaims() *jose.Claims {
	audience := [3]string{"ebay", "comcast", "linkedin"}
	claims := &jose.Claims{
		"id":    "CB00912",
		"iss":   "https://accounts.google.com",
		"sub":   "1234567890",
		"name":  "John Wick",
		"aud":   audience,
		"iat":   1516239022,
		"exp":   1586239022,
		"email": "johnwick@couchbase.com"}
	return claims
}

func mockToken(t *testing.T) jose.JWT {
	header, claims := mockHeader(), mockClaims()
	jwt, err := jose.NewJWT(*header, *claims)
	assert.NoError(t, err)
	return jwt
}

// Full encoded JWT token string in format: header.claims.signature
func mockTokenAsString(t *testing.T) string {
	token := mockToken(t)
	return token.Encode()
}

func mockTokenWithNoClaims(t *testing.T) jose.JWT {
	header := mockHeader()
	jwt, err := jose.NewJWT(*header, nil)
	assert.NoError(t, err)
	return jwt
}

func mockTokenWithBadIss(t *testing.T) jose.JWT {
	header, claims := mockHeader(), mockClaims()
	delete(*claims, "iss")
	claims.Add("iss", 0x0123456789ABCDEF)
	jwt, err := jose.NewJWT(*header, *claims)
	assert.NoError(t, err)
	return jwt
}

func mockTokenWithBadIssURL(t *testing.T) jose.JWT {
	header, claims := mockHeader(), mockClaims()
	delete(*claims, "iss")
	claims.Add("iss", "Couchbase, Inc.") // Expected to be a valid URL.
	jwt, err := jose.NewJWT(*header, *claims)
	assert.NoError(t, err)
	return jwt
}

func mockTokenWithNoIss(t *testing.T) jose.JWT {
	header, claims := mockHeader(), mockClaims()
	delete(*claims, "iss")
	jwt, err := jose.NewJWT(*header, *claims)
	assert.NoError(t, err)
	return jwt
}

func mockTokenWithNoAud(t *testing.T) jose.JWT {
	header, claims := mockHeader(), mockClaims()
	delete(*claims, "aud")
	jwt, err := jose.NewJWT(*header, *claims)
	assert.NoError(t, err)
	return jwt
}

func mockTokenWithSingleAud(t *testing.T) jose.JWT {
	header, claims := mockHeader(), mockClaims()
	delete(*claims, "aud")
	claims.Add("aud", "comcast")
	jwt, err := jose.NewJWT(*header, *claims)
	assert.NoError(t, err)
	return jwt
}

func TestGetJWTIssuer(t *testing.T) {
	tests := []struct {
		name              string
		input             jose.JWT
		expectedIssuer    string
		expectedAudiences []string
		expectedErr       string
	}{
		{
			name:              "Test GetJWTIssuer with bad issuer in JWT claims",
			input:             mockTokenWithBadIss(t),
			expectedIssuer:    "",
			expectedAudiences: nil,
			expectedErr:       "Failed to parse 'iss' claim: unable to parse claim as string: iss",
		},
		{
			name:              "Test GetJWTIssuer with no issuer in JWT claims",
			input:             mockTokenWithNoIss(t),
			expectedIssuer:    "",
			expectedAudiences: nil,
			expectedErr:       "Missing required 'iss' claim",
		},
		{
			name:              "Test GetJWTIssuer with no audience in JWT claims",
			input:             mockTokenWithNoAud(t),
			expectedIssuer:    "",
			expectedAudiences: nil,
			expectedErr:       "Missing required 'aud' claim",
		},
		{
			name:              "Test GetJWTIssuer with a single audience in JWT claims",
			input:             mockTokenWithSingleAud(t),
			expectedIssuer:    "https://accounts.google.com",
			expectedAudiences: []string{"comcast"},
			expectedErr:       "",
		},
		{
			name:              "Test GetJWTIssuer with an empty JWT",
			input:             jose.JWT{},
			expectedIssuer:    "",
			expectedAudiences: nil,
			expectedErr:       "failed to parse JWT claims: malformed JWT claims, unable to decode: unexpected end of JSON input",
		},
		{
			name:              "Test GetJWTIssuer with no claims in JWT",
			input:             mockTokenWithNoClaims(t),
			expectedIssuer:    "",
			expectedAudiences: nil,
			expectedErr:       "Missing required 'iss' claim",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			issuer, audiences, err := GetJWTIssuer(tc.input)
			if err != nil {
				assert.Contains(t, err.Error(), tc.expectedErr)
			}
			assert.Equal(t, len(tc.expectedAudiences), len(audiences))
			assert.Equal(t, tc.expectedIssuer, issuer)
		})
	}
}

func TestGetJWTExpiry(t *testing.T) {
	tests := []struct {
		name        string
		input       jose.JWT
		expiresAt   string
		expectedErr string
	}{
		{
			name:        "Test GetJWTExpiry with a good JWT token",
			input:       mockToken(t),
			expiresAt:   "2020-04-07 05:57:02 +0000 UTC",
			expectedErr: "",
		},
		{
			name:        "Test GetJWTExpiry with no JWT identity",
			input:       jose.JWT{},
			expiresAt:   "0001-01-01 00:00:00 +0000 UTC",
			expectedErr: "malformed JWT claims, unable to decode: unexpected end of JSON input",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			expiresAt, err := GetJWTExpiry(tc.input)
			if err != nil {
				assert.Contains(t, err.Error(), tc.expectedErr)
			}
			assert.Equal(t, tc.expiresAt, expiresAt.String())
		})
	}
}

func TestGetJWTIdentity(t *testing.T) {
	identity, err := GetJWTIdentity(mockToken(t))
	assert.NoError(t, err)
	assert.Empty(t, identity.Name)
	assert.NotEmpty(t, identity.ID)
	assert.NotEmpty(t, identity.Email)
	assert.NotEmpty(t, identity.ExpiresAt)
}
