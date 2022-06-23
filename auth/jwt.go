package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"gopkg.in/square/go-jose.v2"
	"gopkg.in/square/go-jose.v2/jwt"
)

// SupportedAlgorithms is list of signing algorithms explicitly supported
// by github.com/coreos/go-oidc package. If a provider supports other algorithms,
// such as HS256 or none, those values won't be passed to the IDTokenVerifier.
var SupportedAlgorithms = map[jose.SignatureAlgorithm]bool{
	jose.RS256: true,
	jose.RS384: true,
	jose.RS512: true,
	jose.ES256: true,
	jose.ES384: true,
	jose.ES512: true,
	jose.PS256: true,
	jose.PS384: true,
	jose.PS512: true,
}

// JWTConfigCommon groups together configuration options common to both OIDC and local JWT authentication.
type JWTConfigCommon struct {
	Issuer         string  `json:"issuer"`                    // OIDC Issuer
	Register       bool    `json:"register"`                  // If true, server will register new user accounts
	ClientID       *string `json:"client_id,omitempty"`       // Client ID
	UserPrefix     string  `json:"user_prefix,omitempty"`     // Username prefix for users created for this provider
	DisableSession bool    `json:"disable_session,omitempty"` // Disable Sync Gateway session creation on successful OIDC authentication
	// UsernameClaim allows to specify a claim other than subject to use as the Sync Gateway username.
	// The specified claim must be a string - numeric claims may be unmarshalled inconsistently between
	// Sync Gateway and the underlying OIDC library.
	UsernameClaim string `json:"username_claim"`

	// RolesClaim and ChannelsClaim allow specifying a claim (which must be a string or string[]) to add roles/channels
	// to users. These are added in addition to any other roles/channels the user may have (via the admin API or the
	// sync function). If the claim is absent from the access/ID token, no roles/channels will be added.
	RolesClaim    string `json:"roles_claim"`
	ChannelsClaim string `json:"channels_claim"`
}

// ValidFor returns whether the issuer matches, and one of the audiences matches
func (j JWTConfigCommon) ValidFor(issuer string, audiences audience) bool {
	if j.Issuer != issuer {
		return false
	}
	// Validation enforces that ClientID is non-nil for OIDC.
	if j.ClientID == nil || *j.ClientID == "" {
		return true
	}
	for _, aud := range audiences {
		if aud == *j.ClientID {
			return true
		}
	}
	return false
}

var ErrNoMatchingProvider = errors.New("no matching OIDC/JWT provider")

type (
	JWTAlgorithm string
	JWTAlgList   []JWTAlgorithm
)

type LocalJWTAuthProvider struct {
	JWTConfigCommon

	Algorithms JWTAlgList        `json:"algorithms"`
	Keys       []jose.JSONWebKey `json:"keys"`
}

func (l *LocalJWTAuthProvider) verifyToken(ctx context.Context, token string, _ OIDCCallbackURLFunc) (*Identity, error) {
	jws, err := jose.ParseSigned(token)
	if err != nil {
		return nil, err
	}
	switch len(jws.Signatures) {
	case 0:
		return nil, fmt.Errorf("JWT not signed")
	case 1:
	default:
		return nil, fmt.Errorf("multiple signatures on JWT not supported")
	}
	sig := jws.Signatures[0]
	alg := sig.Header.Algorithm
	kid := sig.Header.KeyID

	algSupported := false
	for _, validAlg := range l.Algorithms {
		if string(validAlg) == alg {
			algSupported = true
			break
		}
	}
	if !algSupported {
		return nil, fmt.Errorf("unsupported signing algorithm")
	}

	var key *jose.JSONWebKey
	if len(l.Keys) == 1 {
		key = &l.Keys[0]
	} else {
		for i, test := range l.Keys {
			if test.KeyID == kid {
				key = &l.Keys[i]
				break
			}
		}
	}
	if key == nil {
		return nil, fmt.Errorf("no matching key")
	}
	if key.Algorithm != alg {
		return nil, fmt.Errorf("key alg mismatch (expected %s got %s)", alg, key.Algorithm)
	}
	if key.Use != "" && key.Use != "sig" {
		return nil, fmt.Errorf("invalid key use")
	}

	rawPayload, err := jws.Verify(key.Key)
	if err != nil {
		return nil, err
	}

	identityJSON, err := UnmarshalIdentityJSON(rawPayload)
	if err != nil {
		return nil, err
	}

	if err := identityJSON.ToClaims().Validate(jwt.Expected{
		Issuer:   l.Issuer,
		Audience: jwt.Audience{*l.ClientID},
		Time:     time.Now(),
	}); err != nil {
		return nil, err
	}

	return &Identity{
		Issuer:   identityJSON.Issuer,
		Audience: identityJSON.Audience,
		Subject:  identityJSON.Subject,
		Expiry:   time.Time(identityJSON.Expiry),
		IssuedAt: time.Time(identityJSON.IssuedAt),
		Email:    identityJSON.Email,
		Claims:   identityJSON.Claims,
	}, nil
}

func (l *LocalJWTAuthProvider) common() JWTConfigCommon {
	return l.JWTConfigCommon
}

type LocalJWTConfig map[string]*LocalJWTAuthProvider

type jwtAuthenticator interface {
	verifyToken(ctx context.Context, token string, callbackURLFunc OIDCCallbackURLFunc) (*Identity, error)
	common() JWTConfigCommon
}

func (auth *Authenticator) AuthenticateUntrustedJWT(rawToken string, oidcProviders OIDCProviderMap, localJWT LocalJWTConfig, callbackURLFunc OIDCCallbackURLFunc) (User, PrincipalConfig, error) {
	token, err := jwt.ParseSigned(rawToken)
	if err != nil {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error parsing JWT in AuthenticateUntrustedJWT: %v", err)
		return nil, PrincipalConfig{}, err
	}
	issuer, audiences, err := getIssuerWithAudience(token)
	if err != nil {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error extracting issuer/audiences in AuthenticateUntrustedJWT: %v", err)
		return nil, PrincipalConfig{}, err
	}

	var authenticator jwtAuthenticator
	// can't do `authenticator, ok = getProviderWhenSingle()` because it'll return a non-nil pointer to a nil OIDCProvider
	if single, ok := oidcProviders.getProviderWhenSingle(); ok {
		authenticator = single
	}
	if authenticator == nil {
		for _, provider := range oidcProviders {
			if provider.ValidFor(issuer, audiences) {
				base.TracefCtx(auth.LogCtx, base.KeyAuth, "Using OIDC provider %v", base.UD(provider.Issuer))
				authenticator = provider
				break
			}
		}
	}
	if authenticator == nil {
		for _, provider := range localJWT {
			if provider.ValidFor(issuer, audiences) {
				base.TracefCtx(auth.LogCtx, base.KeyAuth, "Using local JWT provider %v", base.UD(provider.Issuer))
				authenticator = provider
				break
			}
		}
	}
	if authenticator == nil {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "No matching JWT/OIDC provider for issuer %v and audiences %v", base.UD(issuer), base.UD(audiences))
		return nil, PrincipalConfig{}, ErrNoMatchingProvider
	}

	var identity *Identity
	identity, err = authenticator.verifyToken(context.TODO(), rawToken, callbackURLFunc)
	if err != nil {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "JWT invalid: %v", err)
		return nil, PrincipalConfig{}, err
	}

	user, updates, _, err := auth.authenticateJWTIdentity(identity, authenticator.common())
	return user, updates, err
}
