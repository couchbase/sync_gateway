package auth

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseJWT(t *testing.T) {
	header := base64.RawURLEncoding.EncodeToString([]byte("header"))
	payload := base64.RawURLEncoding.EncodeToString([]byte("payload"))
	signature := base64.RawURLEncoding.EncodeToString([]byte("signature"))
	jwt := header + "." + payload + "." + signature
	payloadBytes, err := parseJWT(jwt)
	require.NoError(t, err, "malformed JSON Web Token")
	assert.Equal(t, "payload", string(payloadBytes))
}
