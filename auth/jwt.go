// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package auth

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/coreos/go-oidc"
	"github.com/couchbase/sync_gateway/base"
	"gopkg.in/square/go-jose.v2"
)

type jwtAuthenticator interface {
	verifyToken(ctx context.Context, token string, callbackURLFunc OIDCCallbackURLFunc) (*Identity, error)
	common() JWTConfigCommon
}

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
func (j JWTConfigCommon) ValidFor(ctx context.Context, issuer string, audiences audience) bool {
	if j.Issuer != issuer {
		return false
	}
	// Nil ClientID is invalid (checked by config validation), but empty-string disables audience checking
	if j.ClientID == nil {
		base.ErrorfCtx(ctx, "JWTConfigCommon.ClientID nil - should never happen (for issuer %v)", base.UD(j.Issuer))
		return false
	}
	if *j.ClientID == "" {
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

// JWK 'use' value
const keyUseSigning = "sig"

// JSONWebKeys implements oidc.KeySet for an in-memory JSONWebKey array.
type JSONWebKeys []jose.JSONWebKey

func (j JSONWebKeys) VerifySignature(ctx context.Context, jwt string) (payload []byte, err error) {
	jws, err := jose.ParseSigned(jwt)
	if err != nil {
		return nil, err
	}

	switch len(jws.Signatures) {
	case 0:
		base.DebugfCtx(ctx, base.KeyAuth, "Rejecting JWT - not signed")
		return nil, fmt.Errorf("JWT not signed")
	case 1:
	default:
		base.DebugfCtx(ctx, base.KeyAuth, "Rejecting JWT - multiple signatures on JWT not supported")
		return nil, fmt.Errorf("multiple signatures on JWT not supported")
	}

	jwtHeaderKID := jws.Signatures[0].Header.KeyID
	jwtHeaderALG := jws.Signatures[0].Header.Algorithm

	if jwtHeaderKID == "" && len(j) != 1 {
		base.DebugfCtx(ctx, base.KeyAuth, "Rejecting JWT - no 'kid' specified and multiple keys configured")
		return nil, fmt.Errorf("no 'kid' specified and multiple keys configured")
	}

	for i, key := range j {
		if jwtHeaderKID != "" && jwtHeaderKID != key.KeyID {
			continue
		}
		if jwtHeaderALG != key.Algorithm {
			continue
		}
		if key.Use != "" && key.Use != keyUseSigning {
			continue
		}
		payload, err := jws.Verify(&j[i])
		if err != nil {
			base.DebugfCtx(ctx, base.KeyAuth, "Rejecting JWT - JWS verify failed: %v", err)
			return nil, err
		}
		return payload, nil
	}

	base.DebugfCtx(ctx, base.KeyAuth, "Rejecting JWT - no matching keys found (token alg: %s, kid: %v)", jwtHeaderALG, base.UD(jwtHeaderKID))
	return nil, errors.New("failed to verify id token signature")
}

type LocalJWTAuthConfig struct {
	JWTConfigCommon
	Algorithms      []string    `json:"algorithms"`
	Keys            JSONWebKeys `json:"keys"`
	JWKSURI         string      `json:"jwks_uri"`
	SkipExpiryCheck *bool       `json:"skip_expiry_check"`
}

// BuildProvider prepares a LocalJWTAuthProvider from this config, initialising keySet.
func (l LocalJWTAuthConfig) BuildProvider(ctx context.Context, name string) *LocalJWTAuthProvider {
	var prov *LocalJWTAuthProvider
	// validation ensures these are truly mutually exclusive
	if len(l.Keys) > 0 {
		prov = &LocalJWTAuthProvider{
			LocalJWTAuthConfig: l,
			name:               name,
			keySet:             l.Keys,
		}
	} else {
		prov = &LocalJWTAuthProvider{
			LocalJWTAuthConfig: l,
			name:               name,
			keySet:             oidc.NewRemoteKeySet(ctx, l.JWKSURI),
		}
	}
	prov.initUserPrefix(ctx)
	return prov
}

type LocalJWTAuthProvider struct {
	LocalJWTAuthConfig

	name string
	// keySet has the keys for this config, either in-memory or from oidc.NewRemoteKeySet.
	keySet oidc.KeySet
}

func (l *LocalJWTAuthProvider) verifyToken(ctx context.Context, token string, _ OIDCCallbackURLFunc) (*Identity, error) {
	verifier := oidc.NewVerifier(l.Issuer, l.keySet, &oidc.Config{
		ClientID:             *l.ClientID,
		SkipClientIDCheck:    *l.ClientID == "",
		SupportedSigningAlgs: l.Algorithms,
		SkipExpiryCheck:      base.BoolDefault(l.SkipExpiryCheck, false),
	})

	idToken, err := verifier.Verify(ctx, token)
	if err != nil {
		return nil, err
	}
	base.DebugfCtx(ctx, base.KeyAuth, "Local JWT ID Token successfully parsed and verified (iss: %v; sub: %v)", base.UD(idToken.Issuer), base.UD(idToken.Subject))

	var claims map[string]interface{}
	if err := idToken.Claims(&claims); err != nil {
		base.WarnfCtx(ctx, "Failed to unmarshal ID token claims: %v", err)
	}
	email, _ := claims["email"].(string)
	return &Identity{
		Issuer:   idToken.Issuer,
		Audience: idToken.Audience,
		Subject:  idToken.Subject,
		Expiry:   idToken.Expiry,
		IssuedAt: idToken.IssuedAt,
		Email:    email,
		Claims:   claims,
	}, nil
}

func (l *LocalJWTAuthProvider) common() JWTConfigCommon {
	return l.JWTConfigCommon
}

func (l *LocalJWTAuthProvider) initUserPrefix(ctx context.Context) {
	if l.UserPrefix != "" || l.UsernameClaim != "" {
		return
	}

	issuerURL, err := url.ParseRequestURI(l.Issuer)
	if err != nil {
		base.WarnfCtx(ctx, "Unable to parse issuer URI when initializing user prefix - using provider name")
		l.UserPrefix = l.name
		return
	}
	l.UserPrefix = issuerURL.Host + issuerURL.Path

	// If the prefix contains forward slash or underscore, it's not valid as-is for a username: forward slash
	// breaks the REST API, underscore breaks uniqueness of "[prefix]_[sub]".  URL encode the prefix to cover
	// this scenario
	l.UserPrefix = url.QueryEscape(l.UserPrefix)
}

type LocalJWTConfig map[string]LocalJWTAuthConfig
type LocalJWTProviderMap map[string]*LocalJWTAuthProvider
