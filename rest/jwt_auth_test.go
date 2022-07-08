package rest

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
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
					Algorithms:      []string{"RS256"},
					Keys:            []jose.JSONWebKey{testRSAJWK},
					SkipExpiryCheck: base.BoolPtr(true),
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
			assert.NoError(t, res.Body.Close())

			if !preExistingUser && !register {
				require.Equal(t, http.StatusUnauthorized, res.StatusCode)
				return
			}

			require.Equal(t, http.StatusOK, res.StatusCode)

			user, err := authenticator.GetUser(expectedUsername)
			require.NoError(t, err)
			require.NotNil(t, user, "user was nil")
			assert.Equal(t, testIssuer, user.JWTIssuer())
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

// Tests a subset of the cases covered by auth.TestJWTVerifyToken.
func TestLocalJWTAuthenticationEdgeCases(t *testing.T) {
	testRSAKeypair, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	testRSAJWK := jose.JSONWebKey{
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
		testProviderName = "test"
		testIssuer       = "testIssuer"
		testSubject      = "bilbo"
		testClientID     = "testAud"
	)

	common := auth.JWTConfigCommon{
		Issuer:   testIssuer,
		ClientID: base.StringPtr(testClientID),
	}
	baseProvider := auth.LocalJWTAuthProvider{
		JWTConfigCommon: common,
		Algorithms:      []string{"RS256", "ES256"},
		Keys:            []jose.JSONWebKey{testRSAJWK, testECJWK},
	}

	runTest := func(provider *auth.LocalJWTAuthProvider, token string, createUserName string, expectedStatus int) func(*testing.T) {
		return func(t *testing.T) {
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth, base.KeyHTTP)
			restTesterConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{LocalJWTConfig: auth.LocalJWTConfig{
				testProviderName: provider,
			}}}}
			restTester := NewRestTester(t, &restTesterConfig)
			require.NoError(t, restTester.SetAdminParty(false))
			defer restTester.Close()

			mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
			defer mockSyncGateway.Close()
			mockSyncGatewayURL := mockSyncGateway.URL

			if createUserName != "" {
				authn := restTester.GetDatabase().Authenticator(base.TestCtx(t))
				_, err = authn.RegisterNewUser(createUserName, "test@sgwdev.com")
				require.NoError(t, err, "Failed to register test user %s", createUserName)
			}

			req, err := http.NewRequest(http.MethodPost, mockSyncGatewayURL+"/db/_session", bytes.NewBufferString("{}"))
			require.NoError(t, err)

			req.Header.Set("Authorization", BearerToken+" "+token)

			res, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			assert.NoError(t, res.Body.Close())

			assert.Equal(t, expectedStatus, res.StatusCode)
		}
	}

	testUsername := testProviderName + "_" + testSubject

	t.Run("valid - RSA", runTest(&baseProvider, auth.CreateTestJWT(t, jose.RS256, testRSAKeypair, auth.JWTHeaders{
		"alg": jose.RS256,
		"kid": testRSAJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
		"sub": testSubject,
		"exp": time.Now().Add(time.Hour).Unix(),
	}), testUsername, http.StatusOK))

	t.Run("valid - EC", runTest(&baseProvider, auth.CreateTestJWT(t, jose.ES256, testECKeypair, auth.JWTHeaders{
		"alg": jose.ES256,
		"kid": testECJWK.KeyID,
	}, map[string]interface{}{
		"iss": testIssuer,
		"aud": []string{testClientID},
		"sub": testSubject,
		"exp": time.Now().Add(time.Hour).Unix(),
	}), testUsername, http.StatusOK))

	t.Run("garbage", runTest(&baseProvider, "garbage", testUsername, http.StatusUnauthorized))

	// header: alg=none
	t.Run("valid JWT with alg none", runTest(
		&baseProvider,
		`eyJhbGciOiJub25lIn0.eyJhdWQiOlsidGVzdEF1ZCJdLCJpc3MiOiJ0ZXN0SXNzIn0.`,
		testUsername,
		http.StatusUnauthorized))
	// header: alg=HS256
	t.Run("valid JWT with alg HS256", runTest(&baseProvider,
		`eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOlsidGVzdEF1ZCJdLCJpc3MiOiJ0ZXN0SXNzIn0.gbdmOrzJ2CT01ABybPN-_dwXwv8_8iMEj4HNPtBqQjI`,
		testUsername,
		http.StatusUnauthorized))
}

func TestLocalJWTAndOIDCCoexistence(t *testing.T) {
	const (
		clientID          = "aud1"
		localIssuer       = "iss_local"
		subject           = "noah"
		oidcProviderName  = "testOIDC"
		localProviderName = "testLocal"
		oidcUserPrefix    = "oidc"
		localUserPrefix   = "local"
	)

	testRSAKeypair, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	testRSAJWK := jose.JSONWebKey{
		Key:       testRSAKeypair.Public(),
		Use:       "sig",
		Algorithm: "RS256",
		KeyID:     "rsa",
	}

	mockAuthServer, err := newMockAuthServer()
	require.NoError(t, err, "Error creating mock oauth2 server")
	mockAuthServer.Start()
	defer mockAuthServer.Shutdown()
	mockAuthServer.options.issuer = mockAuthServer.URL + "/" + oidcProviderName

	runTest := func(t *testing.T, token string, expectedUsername, expectedIssuer string) {
		config := &DbConfig{
			OIDCConfig: &auth.OIDCOptions{
				Providers: auth.OIDCProviderMap{
					oidcProviderName: &auth.OIDCProvider{
						JWTConfigCommon: auth.JWTConfigCommon{
							Issuer:     "TEST", // replaced by refreshProviderConfig
							ClientID:   base.StringPtr(clientID),
							Register:   true,
							UserPrefix: oidcUserPrefix,
						},
					},
				},
			},
			LocalJWTConfig: auth.LocalJWTConfig{
				localProviderName: &auth.LocalJWTAuthProvider{
					JWTConfigCommon: auth.JWTConfigCommon{
						Issuer:     localIssuer,
						ClientID:   base.StringPtr(clientID),
						Register:   true,
						UserPrefix: localUserPrefix,
					},
					Algorithms: []string{"RS256"},
					Keys:       auth.JSONWebKeys{testRSAJWK},
				},
			},
			Unsupported: &db.UnsupportedOptions{
				OidcTestProvider: &db.OidcTestProviderOptions{
					Enabled: true,
				},
			},
		}

		refreshProviderConfig(config.OIDCConfig.Providers, mockAuthServer.URL)

		base.SetUpTestLogging(t, base.LevelDebug, base.KeyAuth, base.KeyHTTP)

		restTesterConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: *config}}
		restTester := NewRestTester(t, &restTesterConfig)
		require.NoError(t, restTester.SetAdminParty(false))
		defer restTester.Close()

		mockSyncGateway := httptest.NewServer(restTester.TestPublicHandler())
		defer mockSyncGateway.Close()
		mockSyncGatewayURL := mockSyncGateway.URL

		req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/%s/_session", mockSyncGatewayURL, restTester.GetDatabase().Name), bytes.NewBufferString("{}"))
		require.NoError(t, err)
		req.Header.Set("Authorization", BearerToken+" "+token)
		res, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		assert.NoError(t, res.Body.Close())
		require.Equal(t, http.StatusOK, res.StatusCode)

		user, err := restTester.GetDatabase().Authenticator(base.TestCtx(t)).GetUser(expectedUsername)
		require.NoError(t, err)
		require.NotNil(t, user, "User not found")
		require.Equal(t, expectedIssuer, user.JWTIssuer())
	}

	t.Run("OIDC", func(t *testing.T) {
		token, err := mockAuthServer.makeToken(claimsAuthentic())
		require.NoError(t, err)
		t.Log(token)
		runTest(t, token, oidcUserPrefix+"_"+subject, mockAuthServer.options.issuer)
	})

	t.Run("Local", func(t *testing.T) {
		token := auth.CreateTestJWT(t, "RS256", testRSAKeypair, auth.JWTHeaders{
			"kid": testRSAJWK.KeyID,
			"alg": testRSAJWK.Algorithm,
		}, map[string]interface{}{
			"iss": localIssuer,
			"sub": subject,
			"aud": []string{clientID},
			"exp": time.Now().Add(time.Hour).Unix(),
		})

		runTest(t, token, localUserPrefix+"_"+subject, localIssuer)
	})
}

// Sanity checks that roles_claim/channels_claim also work with Local-JWTs. More extensive coverage in oidc_api_test.go.
func TestLocalJWTRolesChannels(t *testing.T) {
	testRSAKeypair, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	testRSAJWK := jose.JSONWebKey{
		Key:       testRSAKeypair.Public(),
		Use:       "sig",
		Algorithm: "RS256",
		KeyID:     "rsa",
	}

	const (
		testProviderName = "test"
		testIssuer       = "testIssuer"
		testSubject      = "bilbo"
		testClientID     = "testAud"
	)

	baseProvider := auth.LocalJWTAuthProvider{
		JWTConfigCommon: auth.JWTConfigCommon{
			Issuer:        testIssuer,
			ClientID:      base.StringPtr(testClientID),
			RolesClaim:    "roles",
			ChannelsClaim: "channels",
			Register:      true,
		},
		Algorithms: []string{"RS256"},
		Keys:       []jose.JSONWebKey{testRSAJWK},
	}

	restTesterConfig := RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{LocalJWTConfig: auth.LocalJWTConfig{
		testProviderName: &baseProvider,
	}}}}
	restTester := NewRestTester(t, &restTesterConfig)
	require.NoError(t, restTester.SetAdminParty(false))
	defer restTester.Close()

	token := auth.CreateTestJWT(t, jose.RS256, testRSAKeypair, auth.JWTHeaders{
		"alg": jose.RS256,
		"kid": testRSAJWK.KeyID,
	}, map[string]interface{}{
		"iss":      testIssuer,
		"aud":      []string{testClientID},
		"sub":      testSubject,
		"roles":    []string{"jwt_only_role"},
		"channels": []string{"jwt_only_channel"},
		"exp":      time.Now().Add(time.Hour).Unix(),
	})

	reqTime := time.Now()
	res := restTester.SendRequestWithHeaders(http.MethodPost, "/db/_session", "{}", map[string]string{
		"Authorization": BearerToken + " " + token,
	})
	requireStatus(t, res, http.StatusOK)

	authn := restTester.GetDatabase().Authenticator(base.TestCtx(t))
	user, err := authn.GetUser(testProviderName + "_" + testSubject)
	require.NoError(t, err)
	require.NotNil(t, user)

	assert.Contains(t, user.RoleNames(), "jwt_only_role")
	assert.Contains(t, user.Channels().AllKeys(), "jwt_only_channel")
	assert.Equal(t, testIssuer, user.JWTIssuer())
	assert.Greater(t, user.JWTLastUpdated(), reqTime)
}
