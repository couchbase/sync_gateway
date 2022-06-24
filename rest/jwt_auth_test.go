package rest

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/square/go-jose.v2"
)

func TestLocalJWTAuthenticationE2E(t *testing.T) {
	const (
		testIssuer             = "test_issuer"
		testClientID           = "test_aud"
		testProviderName       = "test"
		testSubject            = "bilbo"
		testUsernamePrefix     = "test_prefix"
		testUsernameClaim      = "username"
		testUsernameClaimValue = "frodo"
	)

	testRSAKeypair, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	testRSAJWK := jose.JSONWebKey{
		Key:       testRSAKeypair.Public(),
		Use:       "sig",
		Algorithm: "RS256",
		KeyID:     "rsa",
	}

	runTest := func(preExistingUser bool, register bool, usernamePrefix, usernameClaim string) func(*testing.T) {
		return func(t *testing.T) {
			t.Logf("TEST: parameters: preExistingUser=%t register=%t usernamePrefix=%q usernameClaim=%q", preExistingUser, register, usernamePrefix, usernameClaim)
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth, base.KeyHTTP)

			var expectedUsername string
			switch {
			case usernamePrefix != "" && usernameClaim != "":
				expectedUsername = usernamePrefix + "_" + testUsernameClaimValue
			case usernamePrefix != "" && usernameClaim == "":
				expectedUsername = usernamePrefix + "_" + testSubject
			case usernamePrefix == "" && usernameClaim != "":
				expectedUsername = testUsernameClaimValue
			case usernamePrefix == "" && usernameClaim == "":
				expectedUsername = testProviderName + "_" + testSubject
			}
			t.Logf("TEST: expected username %q", expectedUsername)

			providers := auth.LocalJWTConfig{
				testProviderName: &auth.LocalJWTAuthProvider{
					JWTConfigCommon: auth.JWTConfigCommon{
						Issuer:        testIssuer,
						ClientID:      base.StringPtr(testClientID),
						Register:      register,
						UsernameClaim: usernameClaim,
						UserPrefix:    usernamePrefix,
					},
					Algorithms: auth.JWTAlgList{"RS256"},
					Keys:       []jose.JSONWebKey{testRSAJWK},
				},
			}
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{LocalJWTConfig: providers}}}
			restTester := NewRestTester(t, &restTesterConfig)
			require.NoError(t, restTester.SetAdminParty(false))
			defer restTester.Close()

			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL

			authenticator := restTester.ServerContext().Database("db").Authenticator(base.TestCtx(t))
			if preExistingUser {
				user, err := authenticator.RegisterNewUser(expectedUsername, "")
				require.NoError(t, err, "Failed to create test user")
				t.Logf("TEST: created user %q", user.Name())
			}

			claims := map[string]interface{}{
				"iss": testIssuer,
				"sub": testSubject,
				"aud": []string{testClientID},
			}
			if usernameClaim != "" {
				claims[testUsernameClaim] = testUsernameClaimValue
			}
			token := auth.CreateTestJWT(t, jose.RS256, testRSAKeypair, auth.JWTHeaders{
				"kid": testRSAJWK.KeyID,
			}, claims)

			req, err := http.NewRequest(http.MethodPost, mockSyncGatewayURL+"/db/_session", bytes.NewBufferString("{}"))
			require.NoError(t, err)

			req.Header.Set("Authorization", BearerToken+" "+token)

			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer res.Body.Close()

			if !preExistingUser && !register {
				require.Equal(t, http.StatusUnauthorized, res.StatusCode)
				return
			}

			require.Equal(t, http.StatusOK, res.StatusCode)

			user, err := authenticator.GetUser(expectedUsername)
			require.NoError(t, err)
			require.NotNil(t, user, "user was nil")
			assert.Equal(t, testIssuer, user.OIDCIssuer())
		}
	}

	for _, register := range []bool{true, false} {
		for _, preExisting := range []bool{true, false} {
			for _, usernamePrefix := range []string{"", testUsernamePrefix} {
				for _, usernameClaim := range []string{"", testUsernameClaim} {
					var testNameParts []string
					if register {
						testNameParts = append(testNameParts, "register")
					}
					if preExisting {
						testNameParts = append(testNameParts, "pre-existing user")
					}
					if usernamePrefix != "" {
						testNameParts = append(testNameParts, "username prefix")
					}
					if usernameClaim != "" {
						testNameParts = append(testNameParts, "username claim")
					}
					if len(testNameParts) == 0 {
						testNameParts = []string{"base"}
					}
					t.Run(strings.Join(testNameParts, "__"), runTest(preExisting, register, usernamePrefix, usernameClaim))
				}
			}
		}
	}
}
