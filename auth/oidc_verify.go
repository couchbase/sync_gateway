/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package auth

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coreos/go-oidc"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
	pkgerrors "github.com/pkg/errors"
)

const (
	issuerGoogleAccounts         = "https://accounts.google.com"
	issuerGoogleAccountsNoScheme = "accounts.google.com"
)

// Identity claims required for claims verification
type Identity struct {
	Issuer   string
	Audience []string
	Subject  string
	Expiry   time.Time
	IssuedAt time.Time
	Email    string
	Claims   map[string]interface{}
}

type IdentityJson struct {
	Issuer    string                 `json:"iss"`
	Subject   string                 `json:"sub"`
	Audience  audience               `json:"aud"`
	Expiry    jsonTime               `json:"exp"`
	IssuedAt  jsonTime               `json:"iat"`
	NotBefore *jsonTime              `json:"nbf"`
	Email     string                 `json:"email"`
	Claims    map[string]interface{} `json:"-"`
}

// UnmarshalIdentityJSON raw claim bytes as IdentityJson
func UnmarshalIdentityJSON(claims []byte) (*IdentityJson, error) {
	if len(claims) <= 0 {
		return nil, errors.New("can't extract identity claims from an empty byte slice")
	}
	identity := IdentityJson{}
	if err := json.Unmarshal(claims, &identity); err != nil {
		return nil, err
	}
	decoder := base.JSONDecoder(bytes.NewReader(claims))
	decoder.UseNumber()
	if err := decoder.Decode(&identity.Claims); err != nil {
		return nil, err
	}
	delete(identity.Claims, "iss")
	delete(identity.Claims, "sub")
	delete(identity.Claims, "aud")
	delete(identity.Claims, "exp")
	delete(identity.Claims, "iat")
	delete(identity.Claims, "nbf")
	delete(identity.Claims, "email")
	return &identity, nil
}

// VerifyClaims parses a raw ID Token and verifies the claim.
func VerifyClaims(rawIDToken, clientID, issuer string) (*Identity, error) {
	payload, err := parseJWT(rawIDToken)
	if err != nil {
		return nil, fmt.Errorf("oidc: malformed jwt: %v", err)
	}

	identityJson, err := UnmarshalIdentityJSON(payload)
	if err != nil {
		return nil, fmt.Errorf("oidc: failed to unmarshal claims: %v", err)
	}

	identity := &Identity{
		Issuer:   identityJson.Issuer,
		Subject:  identityJson.Subject,
		Audience: []string(identityJson.Audience),
		Expiry:   time.Time(identityJson.Expiry),
		IssuedAt: time.Time(identityJson.IssuedAt),
		Email:    identityJson.Email,
		Claims:   identityJson.Claims,
	}

	// Check issuer. Google sometimes returns "accounts.google.com" as the issuer claim instead of the required
	// "https://accounts.google.com". Detect this case and allow it only for Google. We will not add hooks to let
	// other providers go off spec like this.
	if (identity.Issuer != issuer) && !(issuer == issuerGoogleAccounts && identity.Issuer == issuerGoogleAccountsNoScheme) {
		return nil, fmt.Errorf("oidc: id token issued by a different provider, expected %q got %q", issuer, identity.Issuer)
	}

	// Provided client ID must be part of the audience.
	if !base.ContainsString(identity.Audience, clientID) {
		return nil, fmt.Errorf("oidc: expected audience %q got %q", clientID, identity.Audience)
	}

	// Make sure token is not expired.
	now := time.Now()
	if identity.Expiry.Before(now) {
		return nil, fmt.Errorf("oidc: token is expired (Token Expiry: %v)", identity.Expiry)
	}

	// If nbf claim is provided in token, ensure that it is indeed in the past.
	if identityJson.NotBefore != nil {
		nbfTime := time.Time(*identityJson.NotBefore)
		leeway := 1 * time.Minute

		if now.Add(leeway).Before(nbfTime) {
			return nil, fmt.Errorf("oidc: current time %v before the nbf (not before) time: %v", now, nbfTime)
		}

	}
	return identity, nil
}

func parseJWT(p string) ([]byte, error) {
	parts := strings.Split(p, ".")
	if len(parts) < 2 {
		return nil, fmt.Errorf("oidc: malformed jwt, expected 3 parts got %d", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("oidc: malformed jwt payload: %v", err)
	}
	return payload, nil
}

type audience []string

func (a *audience) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) == nil {
		*a = audience{s}
		return nil
	}
	var auds []string
	if err := json.Unmarshal(b, &auds); err != nil {
		return err
	}
	*a = audience(auds)
	return nil
}

type jsonTime time.Time

func (j *jsonTime) UnmarshalJSON(b []byte) error {
	var n json.Number
	if err := json.Unmarshal(b, &n); err != nil {
		return err
	}
	var unix int64

	if t, err := n.Int64(); err == nil {
		unix = t
	} else {
		f, err := n.Float64()
		if err != nil {
			return err
		}
		unix = int64(f)
	}
	*j = jsonTime(time.Unix(unix, 0))
	return nil
}

// getIdentity returns identity claims extracted from an ID token.
func getIdentity(idToken *oidc.IDToken) (identity *Identity, ok bool, identityErr error) {
	if idToken == nil {
		return nil, false, errors.New("can't extract identity claims from a nil token")
	}
	identity = &Identity{
		Issuer:   idToken.Issuer,
		Audience: idToken.Audience,
		Subject:  idToken.Subject,
		Expiry:   idToken.Expiry,
		IssuedAt: idToken.IssuedAt,
	}
	claims := map[string]interface{}{}
	if err := idToken.Claims(&claims); err != nil {
		identityErr = pkgerrors.Wrap(err, "failed to extract identity claims from token")
	}
	identity.Claims = claims
	if claim, found := claims["email"]; found {
		var ok bool
		if identity.Email, ok = claim.(string); !ok {
			return identity, true, fmt.Errorf("oidc: can't cast claim %q as string", "email")
		}
	}
	return identity, true, identityErr
}
