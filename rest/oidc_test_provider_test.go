package rest

import (
	"log"
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
