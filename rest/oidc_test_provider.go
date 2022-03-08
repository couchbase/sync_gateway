//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"
)

// This is the private RSA Key that will be used to sign all tokens
const base64EncodedPrivateKey = "MIICXQIBAAKBgQC5HMLzcjKXQhU39ItitqV9EcSgq7SVmt9LRwF+sNgbJOjciJhIJNVYZJZ4tY8aN9lbaMxObuH5gu6B7qlvz5ghy8LD9HRqClu/GSJVW4pQTYffKNAVpuoJIVnjk1DScvSpnL5AM9Qq0MOAM/H9urTIUwMk5JJhD8RXJIvENbJAIQIDAQABAoGAW4PsnY6HlGAHPXKYtmS1y+9M1mINFSlL21tvUcL8E+9bcCvXnVMYZmrUOTkJVlzmCFr3Jo+LCF/CqlnjSnPHMZal1/uObbuH9prumBMK48R6V/0JWxRrtjgw0r/LVwI4BBMhO0BnMCncmuOCbV1xGe8WqwAwiHrSG4zuixJwDkECQQDQO2Yzubfzd3SGdcQVydyUD1cSGd0RvCyUwAJQJyif6MkFrSE2DOduNW1gaknOLIGESBjoGnF+nSF3XcFRloWvAkEA45Ojx0CgkJbHc+m7Gr7hlpgJvLC4iX6vo64lpos0pw9eCW9RCmasjtPR2HtOiU4QssmBYD+8qBPxizgwJD3bLwJAeZO0wE6W0FfWeQsZSX9qgifStobTRB+SB+dzckjqtzK6682BroUqOnaHPdvQ68egdxOBN0L5MOudNoxO6svvkQJBAI+YMNcgqC+Tc/ZnnG+b0au78yjkOQxIq3qT/52+aFKhF6zMWE4/ytG0RcxawYtRfqfRDZk1nkxPiTFXGslDXnECQQCdqQV9HRBPoUXI2sX1zPpaMxLQUS1QqpSAN4fQwybXnxbPsHiPFmkkxLjl6qZaPE+m5HVo2QKAC2EBv5JVw26g"

const (
	testProviderKeyIdentifier = "sync_gateway_oidc_test_provider" // Identifier for test provider private keys
	testProviderAud           = "sync_gateway"                    // Audience for test provider
	defaultIdTokenTTL         = 3600                              // Default ID token expiry

	// Keys to access values from user consent form.
	formKeyUsername      = "username"
	formKeyTokenTTL      = "tokenttl"
	formKeyAuthenticated = "authenticated"
	formKeyIdTokenFormat = "identity-token-formats"

	// Headers
	headerLocation = "Location"
)

type TokenRequestType uint8

const (
	TokenRequestAuthCode TokenRequestType = iota
	TokenRequestRefresh
)

var testProviderAudiences = []string{testProviderAud} // Audiences in array format for test provider validation

// identityTokenFormat represents a specific token format supported by an OpenID Connect provider.
type identityTokenFormat string

const (
	// defaultFormat represents the common format of ID token supported by most of the providers such as
	// Google, Salesforce, Okta etc.
	defaultFormat identityTokenFormat = "defaultFormat"

	// ibmCloudAppIDFormat represents a specific identity token format supported by IBM Cloud App ID.
	// It contains a version value and key id in token header in addition to token type and algorithm.
	// See https://cloud.ibm.com/docs/appid?topic=appid-tokens
	ibmCloudAppIDFormat identityTokenFormat = "ibmCloudAppIDFormat"

	// microsoftAzureADV2Format represents a specific identity token format supported by Microsoft identity
	// platform. It supports two versions of tokens; v1.0 and v2.0. The v2.0 tokens are similar to default
	// token format supported by most of the other providers listed above. But v1.0 tokens emits a legacy
	// claim 'x5t' for compatibility purposes that has the same use and value as 'kid'.
	// See https://docs.microsoft.com/en-us/azure/active-directory/develop/id-tokens
	microsoftAzureADV2Format identityTokenFormat = "microsoftAzureADV2Format"

	// yahooFormat represents a specific identity token format supported by Yahoo!.
	// The JOSE header of ID tokens issued from Yahoo! contains only two fields:
	// 1. alg - Identifies the cryptographic algorithm used to secure the JWS.
	// 2. kid - The hint indicating which key was used to secure the JWS.
	// See https://developer.yahoo.com/oauth2/guide/openid_connect/decode_id_token.html
	yahooFormat identityTokenFormat = "yahooFormat"
)

// This is the HTML template used to display the testing OP internal authentication form
const loginHtml = `
<h1>{{.Title}}</h1>
<div>
   This OIDC test provider can be used in development or test to simulate an OIDC Provider service.<br>
   This provider is enabled per database by adding the following database properties to the Sync Gateway config
   <pre>
"oidc": {
  "default_provider":"sync_gateway",
  "providers": {
    "sync_gateway": {
    	"issuer":"http://localhost:4984/db/_oidc_testing",
    	"client_id":"sync_gateway",
    	"validation_key":"R75hfd9lasdwertwerutecw8",
    	"callback_url":"http://localhost:4984/db/_oidc_callback"
    }
},
"unsupported": {
    "oidc_test_provider": {
        "enabled":true
    }
},
</pre>
</div>
<div>
   To simulate a successful user authentication, enter a username and click "Return a valid authorization code for this user".<br>
   To simulate a failed user authentication, enter a username and click "Return an authorization error for this user".<br><br><br><br>
</div>
<form action="authenticate?{{.Query}}" method="POST" enctype="multipart/form-data">
   <div>Username:<input type="text" name="username" cols="80"></div>
   <div>ID Token TTL (seconds):<input type="text" name="tokenttl" cols="30" value="3600"></div>
   <div>
      <label>Identity Token Format:</label>
      <select id="identity-token-formats" name="identity-token-formats">
         <option name="identity-token-format" value="defaultFormat" selected="">Default</option>
         <option name="identity-token-format" value="ibmCloudAppIDFormat">IBM Cloud AppID</option>
         <option name="identity-token-format" value="microsoftAzureADV2Format">Microsoft Azure AD V2</option>
         <option name="identity-token-format" value="yahooFormat">Yahoo!</option>
      </select>
   </div>
   <div>
   <input type="checkbox" name="offline" value="offlineAccess">Allow Offline Access
   <div>
   <div><input type="submit" name="authenticated" value="Return a valid authorization code for this user"></div>
   <div><input type="submit" name="notauthenticated" value="Return an authorization error for this user"></div>
</form>
`

type Page struct {
	Title string
	Query string
}

type AuthState struct {
	CallbackURL         string
	TokenTTL            time.Duration
	Scopes              map[string]struct{}
	IdentityTokenFormat identityTokenFormat
}

var authCodeTokenMap = make(map[string]AuthState)

/*
 * Returns the OpenID provider configuration info
 */
func (h *handler) handleOidcProviderConfiguration() error {
	if h.db.DatabaseContext.Options.UnsupportedOptions != nil && h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider != nil &&
		!h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider.Enabled {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	issuerUrl := issuerUrl(h)
	base.DebugfCtx(h.ctx(), base.KeyAuth, "handleOidcProviderConfiguration issuerURL = %s", issuerUrl)

	config := &auth.ProviderMetadata{
		Issuer:                            issuerUrl,
		AuthorizationEndpoint:             fmt.Sprintf("%s/%s", issuerUrl, "authorize"),
		TokenEndpoint:                     fmt.Sprintf("%s/%s", issuerUrl, "token"),
		JwksUri:                           fmt.Sprintf("%s/%s", issuerUrl, "certs"),
		ResponseTypesSupported:            []string{"code"},
		SubjectTypesSupported:             []string{"public"},
		IdTokenSigningAlgValuesSupported:  []string{"RS256"},
		ScopesSupported:                   []string{"openid", "email", "profile"},
		TokenEndpointAuthMethodsSupported: []string{"client_secret_basic"},
		ClaimsSupported:                   []string{"email", "sub", "exp", "iat", "iss", "aud", "nickname"},
	}

	if bytes, err := base.JSONMarshal(config); err == nil {
		h.response.Header().Set("Expires", "0")
		_, _ = h.response.Write(bytes)
	}

	return nil
}

type AuthorizeParameters struct {
	UserID           string `json:"sub"`
	Aud              string `json:"aud"`
	Email            string `json:"email"`
	ErrorDescription string `json:"error_description"`
}

/*
 * From OAuth 2.0 spec
 * Handle OAuth 2.0 Authorize request
 * This might display a login page with a form to collect user credentials
 * which is part of an internal authentication flow
 */
func (h *handler) handleOidcTestProviderAuthorize() error {
	if h.db.DatabaseContext.Options.UnsupportedOptions != nil && h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider != nil &&
		!h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider.Enabled {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	requestParams := h.rq.URL.RawQuery

	base.InfofCtx(h.ctx(), base.KeyAuth, "handleOidcTestProviderAuthorize() raw authorize request raw query params = %v", requestParams)

	scope := h.getQueryValues().Get(requestParamScope)
	if scope == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "missing scope parameter")
	}

	err := validateAuthRequestScope(scope)
	if err != nil {
		return err
	}

	p := &Page{Title: "Oidc Test Provider", Query: requestParams}
	t := template.New("Test Login")
	if t, err := t.Parse(loginHtml); err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, err.Error())
	} else {
		err := t.Execute(h.response, p)
		if err != nil {
			return err
		}
	}

	return nil
}

func validateAuthRequestScope(scope string) error {
	requestedScopes := strings.Split(scope, " ")

	openidScope := false
	for _, sc := range requestedScopes {
		if sc == "openid" {
			openidScope = true
		}
	}

	if !openidScope {
		return base.HTTPErrorf(http.StatusBadRequest, "scope parameter must contain 'openid'")
	}

	return nil
}

type OidcTokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	RefreshToken string `json:"refresh_token,omitempty"`
	ExpiresIn    int    `json:"expires_in"`
	IdToken      string `json:"id_token"`
}

/*
 * From OAuth 2.0 spec
 * Return tokens for Auth code flow
 */
func (h *handler) handleOidcTestProviderToken() error {
	if h.db.DatabaseContext.Options.UnsupportedOptions != nil && h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider != nil &&
		!h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider.Enabled {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	base.InfofCtx(h.ctx(), base.KeyAuth, "handleOidcTestProviderToken() called")

	// determine the grant_type being requested
	grantType := h.rq.FormValue("grant_type")

	if grantType == "authorization_code" {
		return handleAuthCodeRequest(h)
	} else if grantType == "refresh_token" {
		return handleRefreshTokenRequest(h)
	}
	return base.HTTPErrorf(http.StatusBadRequest, "grant_type must be \"authorization_code\" or \"refresh_token\"")
}

/*
 * From OAuth 2.0 spec
 * Return public certificates for signing keys
 */
func (h *handler) handleOidcTestProviderCerts() error {
	if h.db.DatabaseContext.Options.UnsupportedOptions != nil && h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider != nil &&
		!h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider.Enabled {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	base.InfofCtx(h.ctx(), base.KeyAuth, "handleOidcTestProviderCerts() called")

	privateKey, err := privateKey()
	if err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, "Error getting private RSA Key")
	}

	jwks := &jose.JSONWebKeySet{
		Keys: []jose.JSONWebKey{
			{
				Key:   &privateKey.PublicKey,
				KeyID: testProviderKeyIdentifier,
				Use:   "sig",
			},
		},
	}

	if bytes, err := json.Marshal(jwks); err == nil {
		_, _ = h.response.Write(bytes)
	}
	return nil
}

/*
 * This is not part of the OAuth 2.0 spec, it is used to handle the
 * user credentials entered in the login form
 * Authenticate the user credentials POST'd from the Web form
 * against the db users
 * Return an OAuth 2.0 Authorization Response
 */
func (h *handler) handleOidcTestProviderAuthenticate() error {
	if h.db.DatabaseContext.Options.UnsupportedOptions != nil && h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider != nil &&
		!h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider.Enabled {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	requestParams := h.getQueryValues()
	username := h.rq.FormValue(formKeyUsername)
	tokenTtl, err := strconv.Atoi(h.rq.FormValue(formKeyTokenTTL))
	idTokenFormat := h.rq.FormValue(formKeyIdTokenFormat)

	if err != nil {
		tokenTtl = defaultIdTokenTTL
	}

	tokenDuration := time.Duration(tokenTtl) * time.Second
	authenticated := h.rq.FormValue(formKeyAuthenticated)
	redirectURI := requestParams.Get(requestParamRedirectURI)
	base.DebugfCtx(h.ctx(), base.KeyAuth, "handleOidcTestProviderAuthenticate() called.  username: %s authenticated: %s", username, authenticated)

	if username == "" || authenticated == "" {
		base.DebugfCtx(h.ctx(), base.KeyAuth, "user did not enter valid credentials -- username or authenticated is empty")
		error := "?error=invalid_request&error_description=User failed authentication"
		h.setHeader(headerLocation, requestParams.Get(requestParamRedirectURI)+error)
		h.response.WriteHeader(http.StatusFound)
		return nil
	}

	scope := requestParams.Get(requestParamScope)
	scopeMap := scopeStringToMap(scope)

	// Generate the return code by base64 encoding the username
	code := base64.StdEncoding.EncodeToString([]byte(username))
	authCodeTokenMap[username] = AuthState{
		CallbackURL:         redirectURI,
		TokenTTL:            tokenDuration,
		Scopes:              scopeMap,
		IdentityTokenFormat: identityTokenFormat(idTokenFormat),
	}

	locationURL, err := url.Parse(redirectURI)
	if err != nil {
		return err
	}

	query := locationURL.Query()
	query.Set(requestParamCode, code)
	if state := requestParams.Get(requestParamState); state != "" {
		query.Set(requestParamState, state)
	}
	locationURL.RawQuery = query.Encode()
	h.setHeader(headerLocation, locationURL.String())
	h.response.WriteHeader(http.StatusFound)

	return nil
}

func scopeStringToMap(scope string) map[string]struct{} {
	scopes := strings.Split(scope, " ")
	scopesMap := make(map[string]struct{})
	for _, sc := range scopes {
		scopesMap[sc] = struct{}{}
	}
	return scopesMap
}

// Creates a signed JWT for the requesting subject and issuer URL
func createJWT(subject string, issuerUrl string, authState AuthState) (token string, err error) {

	key, err := privateKey()
	if err != nil {
		return "", base.HTTPErrorf(http.StatusInternalServerError, "Error getting private RSA Key: %v", err)
	}
	signingKey := jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       key,
	}
	signerOptions := jose.SignerOptions{}

	switch authState.IdentityTokenFormat {
	case ibmCloudAppIDFormat:
		signerOptions.WithType("JWT")
		signerOptions.WithHeader("kid", testProviderKeyIdentifier)
		signerOptions.WithHeader("ver", 4)
	case microsoftAzureADV2Format:
		signerOptions.WithType("JWT")
		signerOptions.WithHeader("kid", testProviderKeyIdentifier)
		signerOptions.WithHeader("x5t", testProviderKeyIdentifier)
	case yahooFormat:
		signerOptions.WithHeader("kid", testProviderKeyIdentifier)
	default:
		signerOptions.WithType("JWT")
	}
	signer, err := jose.NewSigner(signingKey, &signerOptions)

	if err != nil {
		return "", base.HTTPErrorf(http.StatusInternalServerError, "Error creating signer: %v", err)
	}

	now := time.Now()
	claims := jwt.Claims{
		Subject:  subject,
		Issuer:   issuerUrl,
		IssuedAt: jwt.NewNumericDate(now),
		Expiry:   jwt.NewNumericDate(now.Add(authState.TokenTTL)),
		Audience: jwt.Audience{testProviderAud},
	}

	var customClaims CustomClaims

	if _, ok := authState.Scopes["email"]; ok {
		customClaims.Email = subject + "@syncgatewayoidctesting.com"
	}

	if _, ok := authState.Scopes["profile"]; ok {
		customClaims.Nickname = "slim jim"
	}

	token, err = jwt.Signed(signer).Claims(claims).Claims(customClaims).CompactSerialize()
	if err != nil {
		return "", err
	}
	return
}

type CustomClaims struct {
	Email    string `json:"email,omitempty"`
	Nickname string `json:"nickname,omitempty"`
}

// Generates the issuer URL based on the scheme and host in the client request
// this should ensure that the testing provider works for local clients and
// clients in front of a load balancer.
func issuerUrl(h *handler) string {
	return issuerUrlForDB(h, h.db.Name)
}

func issuerUrlForDB(h *handler, dbname string) string {
	scheme := "http"

	if h.rq.TLS != nil {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s/%s/%s", scheme, h.rq.Host, dbname, "_oidc_testing")
}

// Return the internal test RSA private key, this is decoded from a base64 encoded string
// stored as a constant above
func privateKey() (key *rsa.PrivateKey, err error) {
	decodedPrivateKey, err := base64.StdEncoding.DecodeString(base64EncodedPrivateKey)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusInternalServerError, "Error decoding private RSA Key")
	}
	key, err = x509.ParsePKCS1PrivateKey(decodedPrivateKey)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusInternalServerError, "Error parsing private RSA Key")
	}
	return
}

func handleAuthCodeRequest(h *handler) error {
	// Validate the token request
	code := h.rq.FormValue(requestParamCode)
	subject, err := base64.StdEncoding.DecodeString(code)
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "OIDC Invalid Auth Token: %v", code)
	}

	// Check for subject in map of known authenticated users
	authState, ok := authCodeTokenMap[string(subject)]
	if !ok {
		return base.HTTPErrorf(http.StatusBadRequest, "OIDC Invalid Auth Token: %v", code)
	}
	return writeTokenResponse(h, string(subject), issuerUrl(h), authState, TokenRequestAuthCode)
}

func handleRefreshTokenRequest(h *handler) error {
	// Validate the refresh request
	refreshToken := h.rq.FormValue("refresh_token")

	// extract the subject from the refresh token
	subject, err := extractSubjectFromRefreshToken(refreshToken)

	// Check for subject in map of known authenticated users
	authState, ok := authCodeTokenMap[subject]
	if !ok {
		return base.HTTPErrorf(http.StatusBadRequest, "OIDC Invalid Refresh Token: %v", refreshToken)
	}

	if err != nil {
		return err
	}
	return writeTokenResponse(h, subject, issuerUrl(h), authState, TokenRequestRefresh)
}

func writeTokenResponse(h *handler, subject string, issuerUrl string, authState AuthState, requestType TokenRequestType) error {

	accessToken := base64.StdEncoding.EncodeToString([]byte(subject))

	var refreshToken string
	if requestType == TokenRequestAuthCode {
		refreshToken = base64.StdEncoding.EncodeToString([]byte(subject + ":::" + accessToken))
	}

	idToken, err := createJWT(subject, issuerUrl, authState)
	if err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, "Unable to generate OIDC Auth Token")
	}
	h.setHeader("Cache-Control", "no-store")
	h.setHeader("Pragma", "no-cache")
	h.setHeader("Content-Type", "application/json")

	tokenResponse := &OidcTokenResponse{
		AccessToken:  accessToken,
		TokenType:    "Bearer",
		RefreshToken: refreshToken,
		ExpiresIn:    int(authState.TokenTTL.Seconds()),
		IdToken:      idToken,
	}

	if bytes, err := base.JSONMarshal(tokenResponse); err == nil {
		_, _ = h.response.Write(bytes)
	}

	return nil
}

func extractSubjectFromRefreshToken(refreshToken string) (string, error) {
	decodedToken, err := base64.StdEncoding.DecodeString(refreshToken)
	if err != nil {
		base.DebugfCtx(context.Background(), base.KeyAuth, "invalid refresh token provided, error: %v", err)
		return "", base.HTTPErrorf(http.StatusBadRequest, "Invalid OIDC Refresh Token")
	}

	components := strings.Split(string(decodedToken), ":::")
	subject := components[0]
	base.DebugfCtx(context.Background(), base.KeyAuth, "subject extracted from refresh token = %v", subject)

	if len(components) != 2 || subject == "" {
		return "", base.HTTPErrorf(http.StatusBadRequest, "OIDC Refresh Token does not contain subject")
	}
	return subject, nil
}
