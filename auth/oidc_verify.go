package auth

import (
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
}

type IdentityJson struct {
	Issuer    string    `json:"iss"`
	Subject   string    `json:"sub"`
	Audience  audience  `json:"aud"`
	Expiry    jsonTime  `json:"exp"`
	IssuedAt  jsonTime  `json:"iat"`
	NotBefore *jsonTime `json:"nbf"`
	Email     string    `json:"email"`
}

// VerifyClaims parses a raw ID Token and verifies the claim.
func VerifyClaims(rawIDToken, clientID, issuer string) (*Identity, error) {
	payload, err := parseJWT(rawIDToken)
	if err != nil {
		return nil, fmt.Errorf("oidc: malformed jwt: %v", err)
	}
	var identityJson IdentityJson
	if err := json.Unmarshal(payload, &identityJson); err != nil {
		return nil, fmt.Errorf("oidc: failed to unmarshal claims: %v", err)
	}

	identity := &Identity{
		Issuer:   identityJson.Issuer,
		Subject:  identityJson.Subject,
		Audience: []string(identityJson.Audience),
		Expiry:   time.Time(identityJson.Expiry),
		IssuedAt: time.Time(identityJson.IssuedAt),
		Email:    identityJson.Email,
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

func GetIdentity(idToken *oidc.IDToken) (identity *Identity, identityErr error) {
	if idToken == nil {
		return nil, errors.New("can't extract identity from a nil token")
	}
	var claims struct {
		Email string `json:"email"`
	}
	if err := idToken.Claims(&claims); err != nil {
		identityErr = pkgerrors.Wrap(err, "failed to get email from token")
	}
	identity = &Identity{
		Issuer:   idToken.Issuer,
		Audience: idToken.Audience,
		Subject:  idToken.Subject,
		Expiry:   idToken.Expiry,
		IssuedAt: idToken.IssuedAt,
		Email:    claims.Email,
	}
	return identity, identityErr
}
