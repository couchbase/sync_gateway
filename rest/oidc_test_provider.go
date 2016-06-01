package rest

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/sync_gateway/base"
	"net/http"
	"text/template"
)

const login_html = `
<h1>{{.Title}}</h1>

<form action="authenticate?{{.Query}}" method="POST" enctype="multipart/form-data">
    <div>Username:<input type="text" name="username" cols="80"></div>
    <div>   Login:<input type="text" name="password" cols="80"></div>
	    <div><input type="submit" value="Save"></div>
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

/*
 * Returns the OpenID provider configuration info
 */
func (h *handler) handleOidcProviderConfiguration() error {
	if !h.db.DatabaseContext.Options.UnsupportedOptions.EnableOidcTestProvider {
		return base.HTTPErrorf(http.StatusForbidden, "OIDC test provider is not enabled")
	}
	base.LogTo("Oidc+", "handleOidcProviderConfiguration() called")

	scheme := "http"

	if h.rq.TLS != nil {
		scheme = "https"
	}
	issuerUrl := fmt.Sprintf("%s://%s/%s/%s", scheme, h.rq.Host, h.db.Name, "_oidc_testing")

	base.LogTo("Oidc+", "issuerURL = %s", issuerUrl)

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

	base.LogTo("Oidc+", "handleOidcTestProviderAuthorize() called")

	requestParams := h.rq.URL.RawQuery

	base.LogTo("Oidc", "raw authorize request raw query params = %v", requestParams)

	p := &Page{Title: "Oidc Testing Login", Query: requestParams}
	t := template.New("Test Login")
	if t, err := t.Parse(login_html); err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, err.Error())
	} else {
		t.Execute(h.response, p)
	}

	return nil
}

type OidcTokenResponse struct {
	AccessToken  string   `json:"access_token"`
	TokenType    string   `json:"token_type"`
	RefreshToken string   `json:"refresh_token"`
	ExpiresIn    int     `json:"expires_in"`
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

	base.LogTo("Oidc", "handleOidcTestProviderToken() called")

	h.setHeader("Cache-Control", "no-store")
	h.setHeader("Pragma", "no-cache")

	tokenResponse := &OidcTokenResponse{
		AccessToken:  "SlAV32hkKG",
		TokenType:    "Bearer",
		RefreshToken: "8xLOxBtZp8",
		ExpiresIn:    3600,
		IdToken:      "eyJhbGciOiJSUzI1NiIsImtpZCI6IjFlOWdkazcifQ.ewogImlzcyI6ICJodHRwOi8vc2VydmVyLmV4YW1wbGUuY29tIiwKICJzdWIiOiAiMjQ4Mjg5NzYxMDAxIiwKICJhdWQiOiAiczZCaGRSa3F0MyIsCiAibm9uY2UiOiAibi0wUzZfV3pBMk1qIiwKICJleHAiOiAxMzExMjgxOTcwLAogImlhdCI6IDEzMTEyODA5NzAKfQ.ggW8hZ1EuVLuxNuuIJKX_V8a_OMXzR0EHR9R6jgdqrOOF4daGU96Sr_P6qJp6IcmD3HP99Obi1PRs-cwh3LO-p146waJ8IhehcwL7F09JdijmBqkvPeB2T9CJNqeGpe-gccMg4vfKjkM8FcGvnzZUN4_KSP0aAp1tOJ1zZwgjxqGByKHiOtX7TpdQyHE5lcMiKPXfEIQILVq0pc_E2DzL7emopWoaoZTF_m0_N0YzFC6g6EJbOEoRoSK5hoDalrcvRYLSrQAZZKflyuVCyixEoV9GfNQC3_osjzw2PAithfubEEBLuVVk4XUVrWOLrLl0nx7RkKU8NXNHq-rvKMzqg",
	}

	if bytes, err := json.Marshal(tokenResponse); err == nil {
		h.response.Write(bytes)
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

	base.LogTo("Oidc+", "handleOidcTestProviderAuthenticate() called")

	requestParams := h.rq.URL.Query()

	username := h.rq.FormValue("username")
	password := h.rq.FormValue("password")

	if username != "" && password != "" {
		if user := h.db.Authenticator().AuthenticateUser(username, password); user == nil {
			//Build call back URL
			base.LogTo("Oidc", "authenticate  redirect_uri = %v", requestParams.Get("redirect_uri"))

			//fragmentQuery := "#access_token=SlAV32hkKG&token_type=bearer&id_token=eyNiJ9.eyJ1cI6IjIifX0.DeWt4QZXso&expires_in=3600&state=af0ifjsldkj"
			query := "?code=SplxlOBeZQQYbYS6WxSbIA&state=af0ifjsldkj"
			h.setHeader("Location", requestParams.Get("redirect_uri")+query)
			h.response.WriteHeader(http.StatusFound)

			return nil
		} else {
			base.LogTo("Oidc+", "user was not authenticated")
		}
	} else {
		base.LogTo("Oidc+", "user did not enter valid credentials")
	}

	//Build an error response

	return nil
}
