package rest

import (
	"encoding/json"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
)

const googleTokenInfoURL = "https://www.googleapis.com/oauth2/v3/tokeninfo?id_token="

type GoogleResponse struct {
	UserID           string `json:"sub"`
	Aud              string `json:"aud"`
	Email            string `json:"email"`
	ErrorDescription string `json:"error_description"`
}

// POST /_google creates a google-based login session and sets its cookie.
func (h *handler) handleGooglePOST() error {
	// CORS not allowed for login #115 #762
	originHeader := h.rq.Header["Origin"]
	if len(originHeader) > 0 {
		matched := matchedOrigin(h.server.config.CORS.LoginOrigin, originHeader)
		if matched == "" {
			return base.HTTPErrorf(http.StatusBadRequest, "No CORS")
		}
	}

	var params struct {
		IDToken string `json:"id_token"`
	}

	err := h.readJSONInto(&params)
	if err != nil {
		return err
	}

	//validate the google id token
	googleResponse, err := verifyGoogle(params.IDToken, h.server.config.Google.AppClientID)
	if err != nil {
		return err
	}

	createUserIfNeeded := h.server.config.Google.Register
	return h.makeSessionFromNameAndEmail(googleResponse.UserID, googleResponse.Email, createUserIfNeeded)
}

func verifyGoogle(idToken string, allowedAppID []string) (*GoogleResponse, error) {
	destUrl := googleTokenInfoURL + idToken

	res, err := http.Get(destUrl)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	decoder := json.NewDecoder(res.Body)

	var response GoogleResponse
	err = decoder.Decode(&response)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway, "Invalid response from Google token verifier")
	}

	if response.ErrorDescription != "" {
		return nil, base.HTTPErrorf(http.StatusUnauthorized, response.ErrorDescription)
	}

	if !isValidAud(response.Aud, allowedAppID) {
		return nil, base.HTTPErrorf(http.StatusUnauthorized, "Invalid application id, please add it in the config")
	}

	return &response, nil
}

func isValidAud(aud string, allowedAppID []string) bool {
	for _, s := range allowedAppID {
		if aud == s {
			return true
		}
	}
	return false
}
