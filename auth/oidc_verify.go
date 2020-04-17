package auth

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/go-oidc"
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
	Claims   []byte
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
// VerifyClaims does NOT do signature validation, which is the callers responsibility.
// Functionality is cloned from https://github.com/coreos/go-oidc/blob/v2/verify.go
func VerifyClaims(config *oidc.Config, rawIDToken, issuer string) (*Identity, error) {
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
		Claims:   payload,
		Email:    identityJson.Email,
	}

	// Check issuer.
	if !config.SkipIssuerCheck && identity.Issuer != issuer {
		// Google sometimes returns "accounts.google.com" as the issuer claim instead of
		// the required "https://accounts.google.com". Detect this case and allow it only
		// for Google.
		//
		// We will not add hooks to let other providers go off spec like this.
		if !(issuer == issuerGoogleAccounts && identity.Issuer == issuerGoogleAccountsNoScheme) {
			return nil, fmt.Errorf("oidc: id token issued by a different provider, expected %q got %q", issuer, identity.Issuer)
		}
	}

	// If a client ID has been provided, make sure it's part of the audience. SkipClientIDCheck must be true if ClientID is empty.
	//
	// This check DOES NOT ensure that the ClientID is the party to which the ID Token was issued (i.e. Authorized party).
	if !config.SkipClientIDCheck {
		if config.ClientID != "" {
			if !contains(identity.Audience, config.ClientID) {
				return nil, fmt.Errorf("oidc: expected audience %q got %q", config.ClientID, identity.Audience)
			}
		} else {
			return nil, fmt.Errorf("oidc: invalid configuration, clientID must be provided or SkipClientIDCheck must be set")
		}
	}

	// If a SkipExpiryCheck is false, make sure token is not expired.
	if !config.SkipExpiryCheck {
		now := time.Now
		if config.Now != nil {
			now = config.Now
		}
		nowTime := now()

		if identity.Expiry.Before(nowTime) {
			return nil, fmt.Errorf("oidc: token is expired (Token Expiry: %v)", identity.Expiry)
		}

		// If nbf claim is provided in token, ensure that it is indeed in the past.
		if identityJson.NotBefore != nil {
			nbfTime := time.Time(*identityJson.NotBefore)
			leeway := 1 * time.Minute

			if nowTime.Add(leeway).Before(nbfTime) {
				return nil, fmt.Errorf("oidc: current time %v before the nbf (not before) time: %v", nowTime, nbfTime)
			}
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

func contains(sli []string, ele string) bool {
	for _, s := range sli {
		if s == ele {
			return true
		}
	}
	return false
}

func GetIdentity(idToken *oidc.IDToken) (identity *Identity, identityErr error) {
	var claims struct {
		Email string `json:"email"`
	}
	if err := idToken.Claims(&claims); err != nil {
		identityErr = pkgerrors.Wrap(err, "Failed to get email from token")
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
