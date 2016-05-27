package rest

import (
	"github.com/couchbase/sync_gateway/base"
	"encoding/json"
	"text/template"
	"net/http"
	"fmt"
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
	ResponseTypesSupported []string `json:"response_types_supported"`
	SubjectTypesSupported  []string `json:"subject_types_supported"`
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

	issuerUrl := fmt.Sprintf("%s://%s/%s/%s", h.rq.URL.Scheme, h.rq.Host, h.db.Name,"_oidc_testing")

	base.LogTo("Oidc+", "issuerURL = %s",issuerUrl)

	config := &OidcProviderConfiguration{
		Issuer:                 issuerUrl,
		AuthEndpoint:           fmt.Sprintf("%s/%s",issuerUrl,"authorize"),
		TokenEndpoint:          fmt.Sprintf("%s/%s",issuerUrl,"token"),
		ResponseTypesSupported: []string{"code"},
		SubjectTypesSupported:  []string{"public"},
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

	base.LogTo("Oidc","raw authorize request raw query params = %v",requestParams)

	p := &Page{Title: "Oidc Testing Login", Query: requestParams}
	t := template.New("Test Login")
	if t, err := t.Parse(login_html); err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, err.Error())
	} else {
		t.Execute(h.response, p)
	}

	return nil
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

	if(username != "" && password != "") {
		if user := h.db.Authenticator().AuthenticateUser(username, password); user == nil {
			//Build call back URL
			base.LogTo("Oidc","authenticate  redirect_uri = %v",requestParams.Get("redirect_uri"))

			fragmentQuery := "#access_token=SlAV32hkKG&token_type=bearer&id_token=eyNiJ9.eyJ1cI6IjIifX0.DeWt4QZXso&expires_in=3600&state=af0ifjsldkj"
			h.setHeader("Location", requestParams.Get("redirect_uri")+fragmentQuery)
			h.response.WriteHeader(http.StatusFound)

			return nil
		} else {
			base.LogTo("Oidc+","user was not authenticated")
		}
	} else {
		base.LogTo("Oidc+","user did not enter valid credentials")
	}

	//Build an error response

	return nil
}
