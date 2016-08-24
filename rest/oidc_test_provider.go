//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"text/template"
	"time"

	"strconv"

	"github.com/coreos/go-oidc/jose"
	"github.com/coreos/go-oidc/key"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
)

//This is the private RSA Key that will be used to sign all tokens
const base64EncodedPrivateKey = "MIICXQIBAAKBgQC5HMLzcjKXQhU39ItitqV9EcSgq7SVmt9LRwF+sNgbJOjciJhIJNVYZJZ4tY8aN9lbaMxObuH5gu6B7qlvz5ghy8LD9HRqClu/GSJVW4pQTYffKNAVpuoJIVnjk1DScvSpnL5AM9Qq0MOAM/H9urTIUwMk5JJhD8RXJIvENbJAIQIDAQABAoGAW4PsnY6HlGAHPXKYtmS1y+9M1mINFSlL21tvUcL8E+9bcCvXnVMYZmrUOTkJVlzmCFr3Jo+LCF/CqlnjSnPHMZal1/uObbuH9prumBMK48R6V/0JWxRrtjgw0r/LVwI4BBMhO0BnMCncmuOCbV1xGe8WqwAwiHrSG4zuixJwDkECQQDQO2Yzubfzd3SGdcQVydyUD1cSGd0RvCyUwAJQJyif6MkFrSE2DOduNW1gaknOLIGESBjoGnF+nSF3XcFRloWvAkEA45Ojx0CgkJbHc+m7Gr7hlpgJvLC4iX6vo64lpos0pw9eCW9RCmasjtPR2HtOiU4QssmBYD+8qBPxizgwJD3bLwJAeZO0wE6W0FfWeQsZSX9qgifStobTRB+SB+dzckjqtzK6682BroUqOnaHPdvQ68egdxOBN0L5MOudNoxO6svvkQJBAI+YMNcgqC+Tc/ZnnG+b0au78yjkOQxIq3qT/52+aFKhF6zMWE4/ytG0RcxawYtRfqfRDZk1nkxPiTFXGslDXnECQQCdqQV9HRBPoUXI2sX1zPpaMxLQUS1QqpSAN4fQwybXnxbPsHiPFmkkxLjl6qZaPE+m5HVo2QKAC2EBv5JVw26g"

const (
	testProviderKeyIdentifier = "sync_gateway_oidc_test_provider" // Identifier for test provider private keys
	testProviderAud           = "sync_gateway"                    // Audience for test provider
	defaultIdTokenTTL         = 3600                              // Default ID token expiry
)

type TokenRequestType uint8

const (
	TokenRequest_AuthCode TokenRequestType = iota
	TokenRequest_Refresh
)

var testProviderAudiences = []string{testProviderAud} // Audiences in array format for test provider validation

//This is the HTML template used to display the testing OP internal authentication form
const login_html = `
<h1>{{.Title}}</h1>
<div>This OIDC test provider can be used in development or test to simulate an OIDC Provider service.<br>
This provider is enabled per database by adding the following database proeprties to the Sync Gateway config
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
    <div><input type="checkbox" name="offline" value="offlineAccess">Allow Offline Access<div>
    <div><input type="submit" name="authenticated" value="Return a valid authorization code for this user"></div>
    <div><input type="submit" name="notauthenticated" value="Return an authorization error for this user"></div>
</form>
`

type Page struct {
	Title string
	Query string
}

type AuthState struct {
	CallbackURL string
	TokenTTL    time.Duration
	Scopes      map[string]struct{}
}

var authCodeTokenMap map[string]AuthState = make(map[string]AuthState)

/*
 * Returns the OpenID provider configuration info
 */
func (h *handler) handleOidcProviderConfiguration() error {
	if !h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider.Enabled {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	issuerUrl := issuerUrl(h)
	base.LogTo("OIDC+", "handleOidcProviderConfiguration issuerURL = %s", issuerUrl)

	config := &auth.OidcProviderConfiguration{
		Issuer:                            issuerUrl,
		AuthEndpoint:                      fmt.Sprintf("%s/%s", issuerUrl, "authorize"),
		TokenEndpoint:                     fmt.Sprintf("%s/%s", issuerUrl, "token"),
		JwksUri:                           fmt.Sprintf("%s/%s", issuerUrl, "certs"),
		ResponseTypesSupported:            []string{"code"},
		SubjectTypesSupported:             []string{"public"},
		IDTokenSigningAlgValues:           []string{"RS256"},
		ScopesSupported:                   []string{"openid", "email", "profile"},
		TokenEndpointAuthMethodsSupported: []string{"client_secret_basic"},
		ClaimsSupported:                   []string{"email", "sub", "exp", "iat", "iss", "aud", "nickname"},
	}

	if bytes, err := json.Marshal(config); err == nil {
		h.response.Write(bytes)
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
	if !h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider.Enabled {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	requestParams := h.rq.URL.RawQuery

	base.LogTo("OIDC", "handleOidcTestProviderAuthorize() raw authorize request raw query params = %v", requestParams)

	scope := h.rq.URL.Query().Get("scope")
	if scope == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "missing scope parameter")
	}

	err := validateAuthRequestScope(scope)
	if err != nil {
		return err
	}

	p := &Page{Title: "Oidc Test Provider", Query: requestParams}
	t := template.New("Test Login")
	if t, err := t.Parse(login_html); err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, err.Error())
	} else {
		t.Execute(h.response, p)
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
	if !h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider.Enabled {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	base.LogTo("OIDC", "handleOidcTestProviderToken() called")

	//determine the grant_type being requested
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
	if !h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider.Enabled {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	base.LogTo("OIDC", "handleOidcTestProviderCerts() called")

	privateKey, err := privateKey()
	if err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, "Error getting private RSA Key")
	}

	oidcPrivateKey := key.PrivateKey{
		KeyID:      testProviderKeyIdentifier,
		PrivateKey: privateKey,
	}

	jwk := oidcPrivateKey.JWK()

	h.response.Write([]byte("{\r\n\"keys\":[\r\n"))

	if bytes, err := jwk.MarshalJSON(); err == nil {
		h.response.Write(bytes)
	}

	h.response.Write([]byte("\r\n]\r\n}"))

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
	if !h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider.Enabled {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	requestParams := h.rq.URL.Query()
	username := h.rq.FormValue("username")
	tokenttl, err := strconv.Atoi(h.rq.FormValue("tokenttl"))
	if err != nil {
		tokenttl = defaultIdTokenTTL
	}

	tokenDuration := time.Duration(tokenttl) * time.Second

	authenticated := h.rq.FormValue("authenticated")

	redirect_uri := requestParams.Get("redirect_uri")

	base.LogTo("OIDC+", "handleOidcTestProviderAuthenticate() called.  username: %s authenticated: %s", username, authenticated)

	if username == "" || authenticated == "" {
		base.LogTo("OIDC+", "user did not enter valid credentials -- username or authenticated is empty")
		error := "?error=invalid_request&error_description=User failed authentication"
		h.setHeader("Location", requestParams.Get("redirect_uri")+error)
		h.response.WriteHeader(http.StatusFound)
		return nil

	}

	scope := requestParams.Get("scope")
	scopeMap := scopeStringToMap(scope)

	//Generate the return code by base64 encoding the username
	code := base64.StdEncoding.EncodeToString([]byte(username))

	authCodeTokenMap[username] = AuthState{CallbackURL: redirect_uri, TokenTTL: tokenDuration, Scopes: scopeMap}

	location_url, err := url.Parse(redirect_uri)
	if err != nil {
		return err
	}
	query := location_url.Query()
	query.Set("code", code)
	query.Set("state", "af0ifjsldkj")
	location_url.RawQuery = query.Encode()
	h.setHeader("Location", location_url.String())
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

//Creates a signed JWT token for the requesting subject and issuer URL
func createJWTToken(subject string, issuerUrl string, tokenttl time.Duration, scopesMap map[string]struct{}, unsignedToken bool) (jwt *jose.JWT, err error) {

	privateKey, err := privateKey()
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusInternalServerError, "Error getting private RSA Key")
	}

	now := time.Now()
	expiresIn := tokenttl
	expiryTime := now.Add(expiresIn)

	cl := jose.Claims{
		"sub": subject,
		"iat": now.Unix(),
		"exp": expiryTime.Unix(),
		"iss": issuerUrl,
		"aud": testProviderAud,
	}

	if _, ok := scopesMap["email"]; ok {
		cl["email"] = subject + "@syncgatewayoidctesting.com"
	}

	if _, ok := scopesMap["profile"]; ok {
		cl["nickname"] = "slim jim"
	}

	signer := jose.NewSignerRSA(testProviderKeyIdentifier, *privateKey)
	if !unsignedToken {
		jwt, err = jose.NewSignedJWT(cl, signer)
		if err != nil {
			return nil, err
		}

	} else {

		header := jose.JOSEHeader{
			"alg": signer.Alg(),
			"kid": signer.ID(),
		}
		unsignedJWT, err := jose.NewJWT(header, cl)
		if err != nil {
			return nil, err
		}
		jwt = &unsignedJWT
	}
	return
}

//Generates the issuer URL based on the scheme and host in the client request
//this should ensure that the testing provider works for local clients and
//clients in front of a load balancer
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

//Return the internal test RSA private key, this is decoded from a base64 encoded string
//stored as a constant above
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

	//Validate the token request
	code := h.rq.FormValue("code")

	subject, err := base64.StdEncoding.DecodeString(code)
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "OIDC Invalid Auth Token: %v", code)
	}

	//Check for subject in map of known authenticated users
	authState, ok := authCodeTokenMap[string(subject)]
	if !ok {
		return base.HTTPErrorf(http.StatusBadRequest, "OIDC Invalid Auth Token: %v", code)
	}

	return writeTokenResponse(h, string(subject), issuerUrl(h), authState.TokenTTL, authState.Scopes, TokenRequest_AuthCode)
}

func handleRefreshTokenRequest(h *handler) error {
	//Validate the refresh request
	refreshToken := h.rq.FormValue("refresh_token")

	//extract the subject from the refresh token
	subject, err := extractSubjectFromRefreshToken(refreshToken)

	//Check for subject in map of known authenticated users
	authState, ok := authCodeTokenMap[subject]
	if !ok {
		return base.HTTPErrorf(http.StatusBadRequest, "OIDC Invalid Refresh Token: %v", refreshToken)
	}

	if err != nil {
		return err
	}
	return writeTokenResponse(h, subject, issuerUrl(h), authState.TokenTTL, authState.Scopes, TokenRequest_Refresh)
}

func writeTokenResponse(h *handler, subject string, issuerUrl string, tokenttl time.Duration, scopesMap map[string]struct{}, requestType TokenRequestType) error {

	accessToken := base64.StdEncoding.EncodeToString([]byte(subject))

	var refreshToken string
	if requestType == TokenRequest_AuthCode {
		refreshToken = base64.StdEncoding.EncodeToString([]byte(subject + ":::" + accessToken))
	}

	idToken, err := createJWTToken(subject, issuerUrl, tokenttl, scopesMap, h.db.DatabaseContext.Options.UnsupportedOptions.OidcTestProvider.UnsignedIDToken)
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
		ExpiresIn:    int(tokenttl.Seconds()),
		IdToken:      idToken.Encode(),
	}

	if bytes, err := json.Marshal(tokenResponse); err == nil {
		h.response.Write(bytes)
	}

	return nil
}

func extractSubjectFromRefreshToken(refreshToken string) (string, error) {
	decodedToken, err := base64.StdEncoding.DecodeString(refreshToken)
	if err != nil {
		return "", base.HTTPErrorf(http.StatusBadRequest, "Invalid OIDC Refresh Token")
	}

	components := strings.Split(string(decodedToken), ":::")

	subject := components[0]

	base.LogTo("OIDC+", "subject extracted from refresh token = %v", subject)

	if len(components) != 2 || subject == "" {
		return "", base.HTTPErrorf(http.StatusBadRequest, "OIDC Refresh Token does not contain subject")
	}

	return subject, nil
}
