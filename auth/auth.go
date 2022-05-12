//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	pkgerrors "github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/square/go-jose.v2/jwt"
)

/** Manages user authentication for a database. */
type Authenticator struct {
	bucket          base.Bucket
	channelComputer ChannelComputer
	AuthenticatorOptions
	bcryptCostChanged bool
}

type AuthenticatorOptions struct {
	ClientPartitionWindow    time.Duration
	ChannelsWarningThreshold *uint32
	SessionCookieName        string
	BcryptCost               int
	LogCtx                   context.Context
}

// Interface for deriving the set of channels and roles a User/Role has access to.
// The instantiator of an Authenticator must provide an implementation.
type ChannelComputer interface {
	ComputeChannelsForPrincipal(context.Context, Principal) (ch.TimedSet, error)
	ComputeRolesForUser(context.Context, User) (ch.TimedSet, error)
}

type userByEmailInfo struct {
	Username string
}

const (
	PrincipalUpdateMaxCasRetries = 20                 // Maximum number of attempted retries on cas failure updating principal
	DefaultBcryptCost            = bcrypt.DefaultCost // The default bcrypt cost to use for hashing passwords
)

// Constants used in CalculateMaxHistoryEntriesPerGrant
const (
	maximumHistoryBytes          = 1024 * 1024 // 1MB
	averageHistoryKeyBytes       = 250         // This is an estimate of key size in bytes, this includes channel name, the unix timestamp, "entries" key
	averageHistoryEntryPairBytes = 14          // Assume each sequence is 7 digits
	minHistoryEntriesPerGrant    = 1           // Floor of history entries count to ensure there is at least 1 entry
	maxHistoryEntriesPerGrant    = 10          // Ceiling of history entries count to ensure there is no more than 10 entries
)

const (
	GuestUserReadOnly = "Anonymous access is read-only"
)

// Creates a new Authenticator that stores user info in the given Bucket.
func NewAuthenticator(bucket base.Bucket, channelComputer ChannelComputer, options AuthenticatorOptions) *Authenticator {
	return &Authenticator{
		bucket:               bucket,
		channelComputer:      channelComputer,
		AuthenticatorOptions: options,
	}
}

func DefaultAuthenticatorOptions() AuthenticatorOptions {
	return AuthenticatorOptions{
		ClientPartitionWindow: base.DefaultClientPartitionWindow,
		SessionCookieName:     DefaultCookieName,
		BcryptCost:            DefaultBcryptCost,
		LogCtx:                context.Background(),
	}
}

func docIDForUserEmail(email string) string {
	return base.UserEmailPrefix + email
}

func (auth *Authenticator) GetPrincipal(name string, isUser bool) (Principal, error) {
	if isUser {
		return auth.GetUser(name)
	}
	return auth.GetRole(name)
}

// Looks up the information for a user.
// If the username is "" it will return the default (guest) User object, not nil.
// By default the guest User has access to everything, i.e. Admin Party! This can
// be changed by altering its list of channels and saving the changes via SetUser.
func (auth *Authenticator) GetUser(name string) (User, error) {
	princ, err := auth.getPrincipal(docIDForUser(name), func() Principal { return &userImpl{} })
	if err != nil {
		return nil, err
	} else if princ == nil {
		if name == "" {
			princ = auth.defaultGuestUser()
		} else {
			return nil, nil
		}
	}
	princ.(*userImpl).auth = auth
	return princ.(User), err
}

// Looks up the information for a role.
func (auth *Authenticator) GetRole(name string) (Role, error) {
	role, err := auth.GetRoleIncDeleted(name)
	if role != nil && role.IsDeleted() {
		return nil, err
	}
	return role, err
}

func (auth *Authenticator) GetRoleIncDeleted(name string) (Role, error) {
	princ, err := auth.getPrincipal(docIDForRole(name), func() Principal { return &roleImpl{} })
	role, _ := princ.(Role)
	return role, err
}

// Common implementation of GetUser and GetRole. factory() parameter returns a new empty instance.
func (auth *Authenticator) getPrincipal(docID string, factory func() Principal) (Principal, error) {
	var princ Principal

	cas, err := auth.bucket.Update(docID, 0, func(currentValue []byte) ([]byte, *uint32, bool, error) {
		// Be careful: this block can be invoked multiple times if there are races!
		if currentValue == nil {
			princ = nil
			return nil, nil, false, base.ErrUpdateCancel
		}

		princ = factory()
		if err := base.JSONUnmarshal(currentValue, princ); err != nil {
			return nil, nil, false, pkgerrors.WithStack(base.RedactErrorf("base.JSONUnmarshal() error for doc ID: %s in getPrincipal().  Error: %v", base.UD(docID), err))
		}
		changed := false
		if princ.Channels() == nil && !princ.IsDeleted() {
			// Channel list has been invalidated by a doc update -- rebuild it:
			if err := auth.rebuildChannels(princ); err != nil {
				base.WarnfCtx(auth.LogCtx, "RebuildChannels returned error: %v", err)
				return nil, nil, false, err
			}
			changed = true
		}
		if user, ok := princ.(User); ok {
			if user.RoleNames() == nil {
				if err := auth.rebuildRoles(user); err != nil {
					base.WarnfCtx(auth.LogCtx, "RebuildRoles returned error: %v", err)
					return nil, nil, false, err
				}
				changed = true
			}
		}

		if changed {
			// Save the updated doc:
			updatedBytes, marshalErr := base.JSONMarshal(princ)
			if marshalErr != nil {
				marshalErr = pkgerrors.WithStack(base.RedactErrorf("base.JSONUnmarshal() error for doc ID: %s in getPrincipal(). Error: %v", base.UD(docID), marshalErr))
			}
			return updatedBytes, nil, false, marshalErr
		} else {
			// Principal is valid, so stop the update
			return nil, nil, false, base.ErrUpdateCancel
		}
	})

	if err != nil && err != base.ErrUpdateCancel {
		return nil, err
	}

	// If a principal was found, set the cas
	if princ != nil {
		princ.SetCas(cas)
	}

	return princ, nil
}

func (auth *Authenticator) rebuildChannels(princ Principal) error {
	channels := princ.ExplicitChannels().Copy()

	if auth.channelComputer != nil {
		viewChannels, err := auth.channelComputer.ComputeChannelsForPrincipal(auth.LogCtx, princ)
		if err != nil {
			base.WarnfCtx(auth.LogCtx, "channelComputer.ComputeChannelsForPrincipal returned error for %v: %v", base.UD(princ), err)
			return err
		}
		channels.Add(viewChannels)
	}
	// always grant access to the public document channel
	channels.AddChannel(ch.DocumentStarChannel, 1)

	channelHistory := auth.calculateHistory(princ.Name(), princ.GetChannelInvalSeq(), princ.InvalidatedChannels(), channels, princ.ChannelHistory())

	if len(channelHistory) != 0 {
		princ.SetChannelHistory(channelHistory)
	}

	base.InfofCtx(auth.LogCtx, base.KeyAccess, "Recomputed channels for %q: %s", base.UD(princ.Name()), base.UD(channels))
	princ.SetChannelInvalSeq(0)
	princ.setChannels(channels)

	return nil
}

// Calculates history for either roles or channels
func (auth *Authenticator) calculateHistory(princName string, invalSeq uint64, invalGrants ch.TimedSet, newGrants ch.TimedSet, currentHistory TimedSetHistory) TimedSetHistory {
	// Initialize history if currently empty
	if currentHistory == nil {
		currentHistory = map[string]GrantHistory{}
	}

	// Iterate over invalidated grants
	for previousName, previousInfo := range invalGrants {

		// Check if the invalidated grant exists in the new set
		// If principal still has access to this grant then we don't need to build any history for it so skip
		if _, ok := newGrants[previousName]; ok {
			continue
		}

		// If we got here we know the grant has been revoked from the principal

		// Start building history for the principal. If it currently doesn't exist initialize it.
		currentHistoryForGrant, ok := currentHistory[previousName]
		if !ok {
			currentHistoryForGrant = GrantHistory{}
		}

		// Add grant to history
		currentHistoryForGrant.UpdatedAt = time.Now().Unix()
		currentHistoryForGrant.Entries = append(currentHistoryForGrant.Entries, GrantHistorySequencePair{
			StartSeq: previousInfo.Sequence,
			EndSeq:   invalSeq,
		})
		currentHistory[previousName] = currentHistoryForGrant
	}

	if prunedHistory := currentHistory.PruneHistory(auth.ClientPartitionWindow); len(prunedHistory) > 0 {
		base.DebugfCtx(auth.LogCtx, base.KeyCRUD, "rebuildChannels: Pruned principal history on %s for %s", base.UD(princName), base.UD(prunedHistory))
	}

	// Ensure no entries are larger than the allowed threshold
	maxHistoryEntriesPerGrant := CalculateMaxHistoryEntriesPerGrant(len(currentHistory))
	for grantName, grantHistory := range currentHistory {
		if len(grantHistory.Entries) > maxHistoryEntriesPerGrant {
			grantHistory.Entries[1].StartSeq = grantHistory.Entries[0].StartSeq
			grantHistory.Entries = grantHistory.Entries[1:]
			currentHistory[grantName] = grantHistory
		}
	}

	return currentHistory
}

func CalculateMaxHistoryEntriesPerGrant(channelCount int) int {
	maxEntries := 0

	if channelCount != 0 {
		maxEntries = (maximumHistoryBytes/channelCount - averageHistoryKeyBytes) / averageHistoryEntryPairBytes
	}

	// Even if we can fit it limit entries to 10
	maxEntries = base.MinInt(maxEntries, maxHistoryEntriesPerGrant)

	// In the event maxEntries is negative or 0 we should set a floor of 1 entry
	maxEntries = base.MaxInt(maxEntries, minHistoryEntriesPerGrant)

	return maxEntries
}

func (auth *Authenticator) rebuildRoles(user User) error {
	var roles ch.TimedSet
	if auth.channelComputer != nil {
		var err error
		roles, err = auth.channelComputer.ComputeRolesForUser(auth.LogCtx, user)
		if err != nil {
			base.WarnfCtx(auth.LogCtx, "channelComputer.ComputeRolesForUser failed on user %s: %v", base.UD(user.Name()), err)
			return err
		}
	}
	if roles == nil {
		roles = ch.TimedSet{} // it mustn't be nil; nil means it's unknown
	}

	if explicit := user.ExplicitRoles(); explicit != nil {
		roles.Add(explicit)
	}

	roleHistory := auth.calculateHistory(user.Name(), user.GetRoleInvalSeq(), user.InvalidatedRoles(), roles, user.RoleHistory())

	if len(roleHistory) != 0 {
		user.SetRoleHistory(roleHistory)
	}

	base.InfofCtx(auth.LogCtx, base.KeyAccess, "Computed roles for %q: %s", base.UD(user.Name()), base.UD(roles))
	user.SetRoleInvalSeq(0)
	user.setRolesSince(roles)
	return nil
}

// Looks up a User by email address.
func (auth *Authenticator) GetUserByEmail(email string) (User, error) {
	var info userByEmailInfo
	_, err := auth.bucket.Get(docIDForUserEmail(email), &info)
	if base.IsDocNotFoundError(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return auth.GetUser(info.Username)
}

// CAS-safe save of the information for a user/role.  For updates, expects the incoming principal to have
// p.Cas to be set correctly (done automatically for principals retrieved via auth functions).
func (auth *Authenticator) Save(p Principal) error {
	if err := p.validate(); err != nil {
		return err
	}

	casOut, writeErr := auth.bucket.WriteCas(p.DocID(), 0, 0, p.Cas(), p, 0)
	if writeErr != nil {
		return writeErr
	}
	p.SetCas(casOut)

	if user, ok := p.(User); ok {
		if user.Email() != "" {
			info := userByEmailInfo{user.Name()}
			if err := auth.bucket.Set(docIDForUserEmail(user.Email()), 0, nil, info); err != nil {
				return err
			}
			// FIX: Fail if email address is already registered to another user
			// FIX: Unregister old email address if any
		}
	}
	base.InfofCtx(auth.LogCtx, base.KeyAuth, "Saved principal w/ name:%s, seq: #%d", base.UD(p.Name()), p.Sequence())
	return nil
}

// Used for resync
func (auth *Authenticator) UpdateSequenceNumber(p Principal, seq uint64) error {
	p.SetSequence(seq)
	casOut, writeErr := auth.bucket.WriteCas(p.DocID(), 0, 0, p.Cas(), p, 0)
	if writeErr != nil {
		return writeErr
	}
	p.SetCas(casOut)

	return nil
}

// Invalidates the channel list of a user/role by setting the ChannelInvalSeq to a non-zero value
func (auth *Authenticator) InvalidateChannels(name string, isUser bool, invalSeq uint64) error {
	var princ Principal
	var docID string

	if isUser {
		princ = &userImpl{}
		docID = docIDForUser(name)
	} else {
		princ = &roleImpl{}
		docID = docIDForRole(name)
	}

	base.InfofCtx(auth.LogCtx, base.KeyAccess, "Invalidate access of %q", base.UD(name))

	if auth.bucket.IsSupported(sgbucket.DataStoreFeatureSubdocOperations) {
		err := auth.bucket.SubdocInsert(docID, "channel_inval_seq", 0, invalSeq)
		if err != nil && err != base.ErrNotFound && err != base.ErrAlreadyExists {
			return err
		}
		return nil
	}

	_, err := auth.bucket.Update(docID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
		// If user/role doesn't exist cancel update
		if current == nil {
			return nil, nil, false, base.ErrUpdateCancel
		}

		err = base.JSONUnmarshal(current, &princ)
		if err != nil {
			return nil, nil, false, err
		}

		if princ.Channels() == nil {
			return nil, nil, false, base.ErrUpdateCancel
		}

		princ.SetChannelInvalSeq(invalSeq)

		updated, err = base.JSONMarshal(princ)

		return updated, nil, false, err
	})

	if err == base.ErrUpdateCancel {
		return nil
	}

	return err
}

// Invalidates the role list of a user by setting the RoleInvalSeq property to a non-zero value
func (auth *Authenticator) InvalidateRoles(username string, invalSeq uint64) error {
	docID := docIDForUser(username)

	base.InfofCtx(auth.LogCtx, base.KeyAccess, "Invalidate roles of %q", base.UD(username))

	if auth.bucket.IsSupported(sgbucket.DataStoreFeatureSubdocOperations) {
		err := auth.bucket.SubdocInsert(docID, "role_inval_seq", 0, invalSeq)
		if err != nil && err != base.ErrNotFound && err != base.ErrAlreadyExists {
			return err
		}
		return nil
	}

	_, err := auth.bucket.Update(docID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
		// If user doesn't exist cancel update
		if current == nil {
			return nil, nil, false, base.ErrUpdateCancel
		}

		var user userImpl
		err = base.JSONUnmarshal(current, &user)
		if err != nil {
			return nil, nil, false, base.ErrUpdateCancel
		}

		// If user's roles are invalidated already we can cancel update
		if user.RoleNames() == nil {
			return nil, nil, false, base.ErrUpdateCancel
		}

		user.SetRoleInvalSeq(invalSeq)

		updated, err = base.JSONMarshal(&user)
		return updated, nil, false, err
	})

	if err == base.ErrUpdateCancel {
		return nil
	}

	return err
}

// Updates user email and writes user doc
func (auth *Authenticator) UpdateUserEmail(u User, email string) error {

	updateUserEmailCallback := func(currentPrincipal Principal) (updatedPrincipal Principal, err error) {
		currentUser, ok := currentPrincipal.(User)
		if !ok {
			return nil, base.ErrUpdateCancel
		}

		if currentUser.Email() == email {
			return currentUser, base.ErrUpdateCancel
		}

		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Updating user %s email to: %v", base.UD(u.Name()), base.UD(email))
		err = currentUser.SetEmail(email)
		if err != nil {
			return nil, err
		}
		return currentUser, nil
	}

	return auth.casUpdatePrincipal(u, updateUserEmailCallback)
}

// rehashPassword will check the bcrypt cost of the given hash
// and will reset the user's password if the configured cost has since changed
// Callers must verify password is correct before calling this
func (auth *Authenticator) rehashPassword(user User, password string) error {

	// Exit early if bcryptCost has not been set
	if !auth.bcryptCostChanged {
		return nil
	}
	var hashCost int
	rehashPasswordCallback := func(currentPrincipal Principal) (updatedPrincipal Principal, err error) {

		currentUserImpl, ok := currentPrincipal.(*userImpl)
		if !ok {
			return nil, base.ErrUpdateCancel
		}

		hashCost, costErr := bcrypt.Cost(currentUserImpl.PasswordHash_)
		if costErr == nil && hashCost != auth.BcryptCost {
			// the cost of the existing hash is different than the configured bcrypt cost.
			// We'll re-hash the password to adopt the new cost:
			err = currentUserImpl.SetPassword(password)
			if err != nil {
				return nil, err
			}
			return currentUserImpl, nil
		} else {
			return nil, base.ErrUpdateCancel
		}
	}

	if err := auth.casUpdatePrincipal(user, rehashPasswordCallback); err != nil {
		return err
	}

	base.DebugfCtx(auth.LogCtx, base.KeyAuth, "User account %q changed password hash cost from %d to %d",
		base.UD(user.Name()), hashCost, auth.BcryptCost)
	return nil
}

type casUpdatePrincipalCallback func(p Principal) (updatedPrincipal Principal, err error)

// Updates principal using the specified callback function, then does a cas-safe write of the updated principal
// to the bucket.  On CAS failure, reloads the principal and reapplies the update, with up to PrincipalUpdateMaxCasRetries
func (auth *Authenticator) casUpdatePrincipal(p Principal, callback casUpdatePrincipalCallback) error {
	var err error
	for i := 1; i <= PrincipalUpdateMaxCasRetries; i++ {
		updatedPrincipal, err := callback(p)
		if err != nil {
			if err == base.ErrUpdateCancel {
				return nil
			} else {
				return err
			}
		}

		saveErr := auth.Save(updatedPrincipal)
		if saveErr == nil {
			return nil
		}

		if !base.IsCasMismatch(saveErr) {
			return err
		}

		base.InfofCtx(auth.LogCtx, base.KeyAuth, "CAS mismatch in casUpdatePrincipal, retrying.  Principal:%s", base.UD(p.Name()))

		switch p.(type) {
		case User:
			p, err = auth.GetUser(p.Name())
		case Role:
			p, err = auth.GetRole(p.Name())
		default:
			return fmt.Errorf("Unsupported principal type in casUpdatePrincipal (%T)", p)
		}

		if err != nil {
			return fmt.Errorf("Error reloading principal after CAS failure: %w", err)
		}
	}
	base.InfofCtx(auth.LogCtx, base.KeyAuth, "Unable to update principal after %d attempts.  Principal:%s Error:%v", PrincipalUpdateMaxCasRetries, base.UD(p.Name()), err)
	return err
}

func (auth *Authenticator) DeleteUser(user User) error {
	if user.Email() != "" {
		if err := auth.bucket.Delete(docIDForUserEmail(user.Email())); err != nil {
			base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error deleting document ID for user email %s. Error: %v", base.UD(user.Email()), err)
		}
	}
	return auth.bucket.Delete(user.DocID())
}

func (auth *Authenticator) DeleteRole(role Role, purge bool, deleteSeq uint64) error {
	if purge {
		return auth.bucket.Delete(role.DocID())
	}
	return auth.casUpdatePrincipal(role, func(p Principal) (updatedPrincipal Principal, err error) {
		if p == nil || p.IsDeleted() {
			return p, base.ErrUpdateCancel
		}
		p.setDeleted(true)
		p.SetSequence(deleteSeq)

		channelHistory := auth.calculateHistory(p.Name(), deleteSeq, p.Channels(), nil, p.ChannelHistory())
		if len(channelHistory) != 0 {
			p.SetChannelHistory(channelHistory)
		}

		p.SetChannelInvalSeq(deleteSeq)
		return p, nil

	})
}

// Authenticates a user given the username and password.
// If the username and password are both "", it will return a default empty User object, not nil.
func (auth *Authenticator) AuthenticateUser(username string, password string) (User, error) {
	user, err := auth.GetUser(username)
	if err != nil && !base.IsDocNotFoundError(err) {
		return nil, err
	}

	if user == nil || !user.Authenticate(password) {
		return nil, nil
	}
	return user, nil
}

// Authenticates a user based on a JWT token string and a set of providers.  Attempts to match the
// issuer in the token with a provider.
// Used to authenticate a JWT token coming from an insecure source (e.g. client request)
// If the token is validated but the user for the username defined in the subject claim doesn't exist,
// creates the user when autoRegister=true.
func (auth *Authenticator) AuthenticateUntrustedJWT(token string, providers OIDCProviderMap, callbackURLFunc OIDCCallbackURLFunc) (User, error) {

	base.DebugfCtx(auth.LogCtx, base.KeyAuth, "AuthenticateUntrustedJWT called with token: %s", base.UD(token))
	var provider *OIDCProvider
	var issuer string

	provider, ok := providers.getProviderWhenSingle()

	if !ok {
		// Parse JWT (needed to determine issuer/provider)
		jwt, err := jwt.ParseSigned(token)
		if err != nil {
			base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error parsing JWT in AuthenticateUntrustedJWT: %v", err)
			return nil, err
		}

		// Extract issuer and audience(s) from JSON Web Token.
		var audiences []string
		issuer, audiences, err = getIssuerWithAudience(jwt)
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "JWT issuer: %v, audiences: %v", base.UD(issuer), base.UD(audiences))
		if err != nil {
			base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error getting issuer and audience from token: %v", err)
			return nil, err
		}

		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Call GetProviderForIssuer w/ providers: %+v", base.UD(providers))
		provider = providers.GetProviderForIssuer(auth.LogCtx, issuer, audiences)
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Provider for issuer: %+v", base.UD(provider))
	}

	if provider == nil {
		return nil, base.RedactErrorf("No provider found for issuer %v", base.UD(issuer))
	}

	identity, verifyErr := verifyToken(auth.LogCtx, token, provider, callbackURLFunc)
	if verifyErr != nil {
		return nil, verifyErr
	}
	user, _, err := auth.authenticateOIDCIdentity(identity, provider)
	return user, err
}

// verifyToken verifies claims and signature on the token; ensure that it's been signed by the provider.
// Returns identity claims extracted from the token if the verification is successful and an identity error if not.
func verifyToken(ctx context.Context, token string, provider *OIDCProvider, callbackURLFunc OIDCCallbackURLFunc) (identity *Identity, err error) {
	// Get client for issuer
	client, err := provider.GetClient(ctx, callbackURLFunc)
	if err != nil {
		return nil, fmt.Errorf("OIDC initialization error: %w", err)
	}

	// Verify claims and signature on the JWT; ensure that it's been signed by the provider.
	idToken, err := client.verifyJWT(token)
	if err != nil {
		base.DebugfCtx(ctx, base.KeyAuth, "Client %v could not verify JWT. Error: %v", base.UD(client), err)
		return nil, err
	}

	identity, ok, err := getIdentity(idToken)
	if err != nil {
		base.DebugfCtx(ctx, base.KeyAuth, "Error getting identity from token (Identity: %v, Error: %v)", base.UD(identity), err)
	}
	if !ok {
		return nil, err
	}

	return identity, nil
}

// Authenticates a user based on a JWT token obtained directly from a provider (auth code flow, refresh flow).
// Verifies the token claims, but doesn't require signature verification if allow_unsigned_provider_tokens is enabled.
// If the token is validated but the user for the username defined in the subject claim doesn't exist,
// creates the user when autoRegister=true.
func (auth *Authenticator) AuthenticateTrustedJWT(token string, provider *OIDCProvider, callbackURLFunc OIDCCallbackURLFunc) (user User,
	tokenExpiry time.Time, err error) {
	base.DebugfCtx(auth.LogCtx, base.KeyAuth, "AuthenticateTrustedJWT called with token: %s", base.UD(token))

	var identity *Identity
	if provider.AllowUnsignedProviderTokens {
		// Verify claims - ensures that the token we received from the provider is valid for Sync Gateway
		identity, err = VerifyClaims(token, provider.ClientID, provider.Issuer)
		if err != nil {
			base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error verifying raw token in AuthenticateTrustedJWT: %v", err)
			return nil, time.Time{}, err
		}
	} else {
		// Verify claims and signature on the JWT.
		var verifyErr error
		identity, verifyErr = verifyToken(auth.LogCtx, token, provider, callbackURLFunc)
		if verifyErr != nil {
			return nil, time.Time{}, verifyErr
		}
	}

	return auth.authenticateOIDCIdentity(identity, provider)
}

// Obtains a Sync Gateway User for the JWT. Expects that the JWT has already been verified for OIDC compliance.
func (auth *Authenticator) authenticateOIDCIdentity(identity *Identity, provider *OIDCProvider) (user User, tokenExpiry time.Time, err error) {
	if identity == nil || identity.Subject == "" {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Empty subject found in OIDC identity: %v", base.UD(identity))
		return nil, time.Time{}, errors.New("subject not found in OIDC identity")
	}
	username, err := getOIDCUsername(provider, identity)
	if err != nil {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error retrieving OIDCUsername: %v", err)
		return nil, time.Time{}, err
	}
	base.DebugfCtx(auth.LogCtx, base.KeyAuth, "OIDCUsername: %v", base.UD(username))

	user, err = auth.GetUser(username)
	if err != nil {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error retrieving user for username %q: %v", base.UD(username), err)
		return nil, time.Time{}, err
	}

	// If user found, check whether the email needs to be updated (e.g. user has changed email in external auth system)
	if user != nil && identity.Email != "" && user.Email() != identity.Email {
		err = auth.UpdateUserEmail(user, identity.Email)
		if err != nil {
			base.WarnfCtx(auth.LogCtx, "Unable to set user email to %v for OIDC", base.UD(identity.Email))
		}
	}

	// Auto-registration. This will normally be done when token is originally returned
	// to client by oidc callback, but also needed here to handle clients obtaining their own tokens.
	if user == nil && provider.Register {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Registering new user: %v with email: %v", base.UD(username), base.UD(identity.Email))
		var err error
		user, err = auth.RegisterNewUser(username, identity.Email)
		if err != nil && !base.IsCasMismatch(err) {
			base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error registering new user: %v", err)
			return nil, time.Time{}, err
		}
	}

	return user, identity.Expiry, nil
}

// Registers a new user account based on the given verified username and optional email address.
// Password will be random. The user will have access to no channels.  If the user already exists,
// returns the existing user along with the cas failure error
func (auth *Authenticator) RegisterNewUser(username, email string) (User, error) {
	secret, err := base.GenerateRandomSecret()
	if err != nil {
		return nil, err
	}

	user, err := auth.NewUser(username, secret, base.Set{})
	if err != nil {
		return nil, err
	}

	if len(email) > 0 {
		if err := user.SetEmail(email); err != nil {
			base.WarnfCtx(auth.LogCtx, "Skipping SetEmail for user %q - Invalid email address provided: %q", base.UD(username), base.UD(email))
		}
	}

	err = auth.Save(user)
	if base.IsCasMismatch(err) {
		return auth.GetUser(username)
	} else if err != nil {
		return nil, err
	}

	return user, err
}
