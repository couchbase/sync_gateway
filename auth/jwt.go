//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/coreos/go-oidc/jose"
	"github.com/coreos/go-oidc/oidc"
)

// Config options for Json Web Token validation
type JWTOptions struct {
	ValidationKey *string `json:"validation_key"`           // Key used to validate signed tokens
	SigningMethod *string `json:"signing_method,omitempty"` // Algorithm used for signing.  Can be specified for additional security to handle scenario described here: https://auth0.com/blog/2015/03/31/critical-vulnerabilities-in-json-web-token-libraries/
}

// Parses and validates a JWT token, based on the client definition provided.
func ValidateJWT(idToken string, client *oidc.Client) (jose.JWT, error) {

	jwt, err := jose.ParseJWT(idToken)
	if err != nil {
		return jose.JWT{}, err
	}

	return jwt, client.VerifyJWT(jwt)
}

// Extracts the JWT Identity Claims (includes ID, Email, Expiry) from a JWT.
func GetJWTIdentity(jwt jose.JWT) (identity *oidc.Identity, err error) {

	claims, err := jwt.Claims()
	if err != nil {
		return identity, err
	}

	return oidc.IdentityFromClaims(claims)
}

// Returns the "exp" claim (Identity.ExpiresAt) for the JWT, as a time.Time.
func GetJWTExpiry(jwt jose.JWT) (expiresAt time.Time, err error) {

	identity, err := GetJWTIdentity(jwt)
	if err != nil {
		return expiresAt, err
	}

	return identity.ExpiresAt, nil
}

func GetJWTIssuer(jwt jose.JWT) (issuer string, audiences []string, err error) {

	claims, err := jwt.Claims()
	if err != nil {
		return "", audiences, fmt.Errorf("failed to parse JWT claims: %v", err)
	}

	iss, ok, err := claims.StringClaim("iss")
	if err != nil {
		return "", audiences, fmt.Errorf("Failed to parse 'iss' claim: %v", err)
	} else if !ok {
		return "", audiences, errors.New("Missing required 'iss' claim")
	}

	if aud, ok, err := claims.StringClaim("aud"); err == nil && ok {
		audiences = append(audiences, aud)
	} else if aud, ok, err := claims.StringsClaim("aud"); err == nil && ok {
		audiences = aud
	} else {
		return "", audiences, errors.New("Missing required 'aud' claim.")
	}

	return iss, audiences, nil
}
