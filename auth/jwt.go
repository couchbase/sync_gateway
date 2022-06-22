package auth

import (
	"gopkg.in/square/go-jose.v2"
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

type (
	JWTAlgorithm string
	JWTAlgList   []JWTAlgorithm
)

type LocalJWTAuthProvider struct {
	JWTConfigCommon

	Algorithms JWTAlgList        `json:"algorithms"`
	Keys       []jose.JSONWebKey `json:"keys"`
}

type LocalJWTConfig map[string]*LocalJWTAuthProvider
