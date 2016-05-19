package rest

import (
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

type AuthorizeParameters struct {
	UserID           string `json:"sub"`
	Aud              string `json:"aud"`
	Email            string `json:"email"`
	ErrorDescription string `json:"error_description"`
}

/*
OpenID Connect uses the following OAuth 2.0 request parameters with the Authorization Code Flow:

scope
REQUIRED. OpenID Connect requests MUST contain the openid scope value. If the openid scope value is not present, the behavior is entirely unspecified. Other scope values MAY be present. Scope values used that are not understood by an implementation SHOULD be ignored. See Sections 5.4 and 11 for additional scope values defined by this specification.
response_type
REQUIRED. OAuth 2.0 Response Type value that determines the authorization processing flow to be used, including what parameters are returned from the endpoints used. When using the Authorization Code Flow, this value is code.
client_id
REQUIRED. OAuth 2.0 Client Identifier valid at the Authorization Server.
redirect_uri
REQUIRED. Redirection URI to which the response will be sent. This URI MUST exactly match one of the Redirection URI values for the Client pre-registered at the OpenID Provider, with the matching performed as described in Section 6.2.1 of [RFC3986] (Simple String Comparison). When using this flow, the Redirection URI SHOULD use the https scheme; however, it MAY use the http scheme, provided that the Client Type is confidential, as defined in Section 2.1 of OAuth 2.0, and provided the OP allows the use of http Redirection URIs in this case. The Redirection URI MAY use an alternate scheme, such as one that is intended to identify a callback into a native application.
state
RECOMMENDED. Opaque value used to maintain state between the request and the callback. Typically, Cross-Site Request Forgery (CSRF, XSRF) mitigation is done by cryptographically binding the value of this parameter with a browser cookie.
OpenID Connect also uses the following OAuth 2.0 request parameter, which is defined in OAuth 2.0 Multiple Response Type Encoding Practices [OAuth.Responses]:

response_mode
OPTIONAL. Informs the Authorization Server of the mechanism to be used for returning parameters from the Authorization Endpoint. This use of this parameter is NOT RECOMMENDED when the Response Mode that would be requested is the default mode specified for the Response Type.
This specification also defines the following request parameters:

nonce
OPTIONAL. String value used to associate a Client session with an ID Token, and to mitigate replay attacks. The value is passed through unmodified from the Authentication Request to the ID Token. Sufficient entropy MUST be present in the nonce values used to prevent attackers from guessing values. For implementation notes, see Section 15.5.2.
display
OPTIONAL. ASCII string value that specifies how the Authorization Server displays the authentication and consent user interface pages to the End-User. The defined values are:
page
The Authorization Server SHOULD display the authentication and consent UI consistent with a full User Agent page view. If the display parameter is not specified, this is the default display mode.
popup
The Authorization Server SHOULD display the authentication and consent UI consistent with a popup User Agent window. The popup User Agent window should be of an appropriate size for a login-focused dialog and should not obscure the entire window that it is popping up over.
touch
The Authorization Server SHOULD display the authentication and consent UI consistent with a device that leverages a touch interface.
wap
The Authorization Server SHOULD display the authentication and consent UI consistent with a "feature phone" type display.
The Authorization Server MAY also attempt to detect the capabilities of the User Agent and present an appropriate display.
prompt
OPTIONAL. Space delimited, case sensitive list of ASCII string values that specifies whether the Authorization Server prompts the End-User for reauthentication and consent. The defined values are:
none
The Authorization Server MUST NOT display any authentication or consent user interface pages. An error is returned if an End-User is not already authenticated or the Client does not have pre-configured consent for the requested Claims or does not fulfill other conditions for processing the request. The error code will typically be login_required, interaction_required, or another code defined in Section 3.1.2.6. This can be used as a method to check for existing authentication and/or consent.
login
The Authorization Server SHOULD prompt the End-User for reauthentication. If it cannot reauthenticate the End-User, it MUST return an error, typically login_required.
consent
The Authorization Server SHOULD prompt the End-User for consent before returning information to the Client. If it cannot obtain consent, it MUST return an error, typically consent_required.
select_account
The Authorization Server SHOULD prompt the End-User to select a user account. This enables an End-User who has multiple accounts at the Authorization Server to select amongst the multiple accounts that they might have current sessions for. If it cannot obtain an account selection choice made by the End-User, it MUST return an error, typically account_selection_required.
The prompt parameter can be used by the Client to make sure that the End-User is still present for the current session or to bring attention to the request. If this parameter contains none with any other value, an error is returned.
max_age
OPTIONAL. Maximum Authentication Age. Specifies the allowable elapsed time in seconds since the last time the End-User was actively authenticated by the OP. If the elapsed time is greater than this value, the OP MUST attempt to actively re-authenticate the End-User. (The max_age request parameter corresponds to the OpenID 2.0 PAPE [OpenID.PAPE] max_auth_age request parameter.) When max_age is used, the ID Token returned MUST include an auth_time Claim Value.
ui_locales
OPTIONAL. End-User's preferred languages and scripts for the user interface, represented as a space-separated list of BCP47 [RFC5646] language tag values, ordered by preference. For instance, the value "fr-CA fr en" represents a preference for French as spoken in Canada, then French (without a region designation), followed by English (without a region designation). An error SHOULD NOT result if some or all of the requested locales are not supported by the OpenID Provider.
id_token_hint
OPTIONAL. ID Token previously issued by the Authorization Server being passed as a hint about the End-User's current or past authenticated session with the Client. If the End-User identified by the ID Token is logged in or is logged in by the request, then the Authorization Server returns a positive response; otherwise, it SHOULD return an error, such as login_required. When possible, an id_token_hint SHOULD be present when prompt=none is used and an invalid_request error MAY be returned if it is not; however, the server SHOULD respond successfully when possible, even if it is not present. The Authorization Server need not be listed as an audience of the ID Token when it is used as an id_token_hint value.
If the ID Token received by the RP from the OP is encrypted, to use it as an id_token_hint, the Client MUST decrypt the signed ID Token contained within the encrypted ID Token. The Client MAY re-encrypt the signed ID token to the Authentication Server using a key that enables the server to decrypt the ID Token, and use the re-encrypted ID token as the id_token_hint value.
login_hint
OPTIONAL. Hint to the Authorization Server about the login identifier the End-User might use to log in (if necessary). This hint can be used by an RP if it first asks the End-User for their e-mail address (or other identifier) and then wants to pass that value as a hint to the discovered authorization service. It is RECOMMENDED that the hint value match the value used for discovery. This value MAY also be a phone number in the format specified for the phone_number Claim. The use of this parameter is left to the OP's discretion.
acr_values
OPTIONAL. Requested Authentication Context Class Reference values. Space-separated string that specifies the acr values that the Authorization Server is being requested to use for processing this Authentication Request, with the values appearing in order of preference. The Authentication Context Class satisfied by the authentication performed is returned as the acr Claim Value, as specified in Section 2. The acr Claim is requested as a Voluntary Claim by this parameter.

*/

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

	base.LogTo("Oidc", "raw authenticate request query params = %v", requestParams)

	//Build call back URL
	base.LogTo("Oidc", "raw authenticate  redirect_uri = %v", requestParams.Get("redirect_uri"))

	h.setHeader("Location", requestParams.Get("redirect_uri"))
	h.response.WriteHeader(http.StatusFound)
	return nil
}
