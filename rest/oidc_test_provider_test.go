package rest

import (
	"encoding/base64"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
	"log"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCreateJWTToken(t *testing.T) {
	subject := "alice"
	issueURL := "http://localhost:4984/default/_oidc_testing"
	ttl := 5 * time.Minute
	scopes := make(map[string]struct{})
	token, err := createJWTToken(subject, issueURL, ttl, scopes)
	assert.NoError(t, err, "Couldn't to create JSON Web Token for OpenID Connect")
	log.Printf("Token: %s", token)
}

func TestExtractSubjectFromRefreshToken(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAuth)()
	// Extract subject from invalid refresh token
	sub, err := extractSubjectFromRefreshToken("invalid_refresh_token")
	require.Error(t, err, "invalid refresh token error")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))
	assert.Empty(t, sub, "couldn't extract subject from refresh token")

	// Extract subject from a valid refresh token
	subject := "subject"
	accessToken := base64.StdEncoding.EncodeToString([]byte(subject))
	refreshToken := base64.StdEncoding.EncodeToString([]byte(subject + ":::" + accessToken))
	sub, err = extractSubjectFromRefreshToken(refreshToken)
	require.NoError(t, err, "invalid refresh token error")
	assert.Equal(t, subject, sub)
}
