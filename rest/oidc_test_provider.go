package rest

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/coreos/go-oidc/jose"
	"github.com/couchbase/sync_gateway/base"
	"net/http"
	"text/template"
)

const login_html = `
<h1>{{.Title}}</h1>
<div>This OIDC test provider can be used in development or test to simulate an OIDC Provider service.<br>
This provider is enabled per database by adding the following database proeprties to the Sync Gateway config
<pre>
"oidc": {
    "issuer":"http://localhost:4984/db/_oidc_testing",
    "client_id":"sync_gateway",
    "validation_key":"R75hfd9lasdwertwerutecw8",
    "callback_url":"http://localhost:4984/db/_oidc_callback"
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
    <div><input type="checkbox" name="offline" value="offlineAccess">Allow Offline Access<div>
    <div><input type="submit" name="authenticated" value="Return a valid authorization code for this user"></div>
    <div><input type="submit" name="notauthenticated" value="Return an authorization error for this user"></div>
</form>
`

type Page struct {
	Title string
	Query string
}

type OidcProviderConfiguration struct {
	Issuer                 string   `json:"issuer"`
	AuthEndpoint           string   `json:"authorization_endpoint"`
	TokenEndpoint          string   `json:"token_endpoint"`
	JwksUri                string   `json:"jwks_uri"`
	ResponseTypesSupported []string `json:"response_types_supported"`
	SubjectTypesSupported  []string `json:"subject_types_supported"`
	ItsaValuesSupported    []string `json:"id_token_signing_alg_values_supported"`
	ScopesSupported        []string `json:"scopes_supported"`
	CalimsSupported        []string `json:"claims_supported"`
}

type AuthState struct {
	Username    string
	CallbackURL string
	IDToken     string
}

var authCodeTokenMap map[string]AuthState = make(map[string]AuthState)

/*
 * Returns the OpenID provider configuration info
 */
func (h *handler) handleOidcProviderConfiguration() error {
	if !h.db.DatabaseContext.Options.UnsupportedOptions.EnableOidcTestProvider {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}
	base.LogTo("OIDC+", "handleOidcProviderConfiguration() called")

	scheme := "http"

	if h.rq.TLS != nil {
		scheme = "https"
	}
	issuerUrl := fmt.Sprintf("%s://%s/%s/%s", scheme, h.rq.Host, h.db.Name, "_oidc_testing")

	base.LogTo("OIDC+", "issuerURL = %s", issuerUrl)

	config := &OidcProviderConfiguration{
		Issuer:                 issuerUrl,
		AuthEndpoint:           fmt.Sprintf("%s/%s", issuerUrl, "authorize"),
		TokenEndpoint:          fmt.Sprintf("%s/%s", issuerUrl, "token"),
		JwksUri:                fmt.Sprintf("%s/%s", issuerUrl, "certs"),
		ResponseTypesSupported: []string{"code"},
		SubjectTypesSupported:  []string{"public"},
		ItsaValuesSupported:    []string{"RS256"},
		ScopesSupported:        []string{"openid"},
		CalimsSupported:        []string{"email"},
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
 * Handle OAuth 2.0 Authoriaze request
 * Display a login page with a form to collect user credentials
 */
func (h *handler) handleOidcTestProviderAuthorize() error {
	if !h.db.DatabaseContext.Options.UnsupportedOptions.EnableOidcTestProvider {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	base.LogTo("OIDC+", "handleOidcTestProviderAuthorize() called")

	requestParams := h.rq.URL.RawQuery

	base.LogTo("OIDC", "raw authorize request raw query params = %v", requestParams)

	p := &Page{Title: "Oidc Test Provider", Query: requestParams}
	t := template.New("Test Login")
	if t, err := t.Parse(login_html); err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, err.Error())
	} else {
		t.Execute(h.response, p)
	}

	return nil
}

type OidcTokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	IdToken      string `json:"id_token"`
}

/*
 * From OAuth 2.0 spec
 * Handle the login form submission and authenticate the user credentials
 * then callback Depending party with login tokens or error message
 */
func (h *handler) handleOidcTestProviderToken() error {
	if !h.db.DatabaseContext.Options.UnsupportedOptions.EnableOidcTestProvider {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	base.LogTo("OIDC", "handleOidcTestProviderToken() called")

	//Validate the token request
	code := h.getQuery("code")

	//Check for code in map of generated codes
	if _, ok := authCodeTokenMap[code]; !ok {
		//return base.HTTPErrorf(http.StatusBadRequest, "OIDC Invalid Auth Token: %v",code)
	}

	accessToken := base64.StdEncoding.EncodeToString([]byte(code))
	refreshToken := base64.StdEncoding.EncodeToString([]byte(accessToken))

	idToken := createJWTToken()

	h.setHeader("Cache-Control", "no-store")
	h.setHeader("Pragma", "no-cache")

	tokenResponse := &OidcTokenResponse{
		AccessToken:  accessToken,
		TokenType:    "Bearer",
		RefreshToken: refreshToken,
		ExpiresIn:    3600,
		IdToken:      idToken.Encode(),
	}

	//IdToken:      "eyJhbGciOiJSUzI1NiIsImtpZCI6IjFlOWdkazcifQ.ewogImlzcyI6ICJodHRwOi8vc2VydmVyLmV4YW1wbGUuY29tIiwKICJzdWIiOiAiMjQ4Mjg5NzYxMDAxIiwKICJhdWQiOiAiczZCaGRSa3F0MyIsCiAibm9uY2UiOiAibi0wUzZfV3pBMk1qIiwKICJleHAiOiAxMzExMjgxOTcwLAogImlhdCI6IDEzMTEyODA5NzAKfQ.ggW8hZ1EuVLuxNuuIJKX_V8a_OMXzR0EHR9R6jgdqrOOF4daGU96Sr_P6qJp6IcmD3HP99Obi1PRs-cwh3LO-p146waJ8IhehcwL7F09JdijmBqkvPeB2T9CJNqeGpe-gccMg4vfKjkM8FcGvnzZUN4_KSP0aAp1tOJ1zZwgjxqGByKHiOtX7TpdQyHE5lcMiKPXfEIQILVq0pc_E2DzL7emopWoaoZTF_m0_N0YzFC6g6EJbOEoRoSK5hoDalrcvRYLSrQAZZKflyuVCyixEoV9GfNQC3_osjzw2PAithfubEEBLuVVk4XUVrWOLrLl0nx7RkKU8NXNHq-rvKMzqg",

	if bytes, err := json.Marshal(tokenResponse); err == nil {
		h.response.Write(bytes)
		base.LogTo("OIDC", "Returned ID token: %s", bytes)
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
	if !h.db.DatabaseContext.Options.UnsupportedOptions.EnableOidcTestProvider {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}

	base.LogTo("OIDC+", "handleOidcTestProviderAuthenticate() called")

	requestParams := h.rq.URL.Query()
	username := h.rq.FormValue("username")
	authenticated := h.rq.FormValue("authenticated")
	location := requestParams.Get("redirect_uri")

	if username != "" {
		if authenticated != "" {
			//Generate the return code by base64 encoding the username
			code := base64.StdEncoding.EncodeToString([]byte(username))

			authCodeTokenMap[code] = AuthState{Username: username, CallbackURL: location}

			query := "?code=" + code + "&state=af0ifjsldkj"
			h.setHeader("Location", location+query)
			h.response.WriteHeader(http.StatusFound)

			return nil
		} else {
			base.LogTo("OIDC+", "user was not authenticated")
			error := "?error=invalid_request&error_description=User failed authentication"
			h.setHeader("Location", location+error)
			h.response.WriteHeader(http.StatusFound)
		}

	} else {
		base.LogTo("OIDC+", "user did not enter valid credentials")
		error := "?error=invalid_request&error_description=User failed authentication"
		h.setHeader("Location", requestParams.Get("redirect_uri")+error)
		h.response.WriteHeader(http.StatusFound)
	}

	return nil
}

func createJWTToken() *jose.JWT {

	size := 1024
	priv, err := rsa.GenerateKey(rand.Reader, size)

	cl := jose.Claims{"foo": "bar"}

	jwk := jose.JWK{
		ID:     "sync_gateway",
		Alg:    "RS256",
		Secret: []byte("R75hfd9lasdwertwerutecw8"),
	}

	signer := jose.NewSignerRSA(jwk.ID, *priv)

	jwt, err := jose.NewSignedJWT(cl, signer)

	if err == nil {
		return jwt
	}

	return nil
}
