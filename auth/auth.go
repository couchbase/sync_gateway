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
	"net/http"
	"time"

	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	pkgerrors "github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/square/go-jose.v2/jwt"
)

// Authenticator manages user authentication for a database.

type Authenticator struct {
	datastore       base.DataStore
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

	// Collections defines the set of collections used by the authenticator when rebuilding channels.
	// Channels are only recomputed for collections included in this set.
	// This can be used to limit (re-)computation of channel access to only those collections relevant to
	// a given operation. If not specified, is set to _default._default in NewAuthenticator constructor.
	Collections map[string]map[string]struct{}
	// MetaKeys generates key formats used to persist auth users, roles and sessions
	MetaKeys *base.MetadataKeys
}

// Interface for deriving the set of channels and roles a User/Role has access to.
// The instantiator of an Authenticator must provide an implementation.
type ChannelComputer interface {
	// ComputeChannelsForPrincipal returns the set of channels granted to the principal by documents in scope.collection
	ComputeChannelsForPrincipal(ctx context.Context, p Principal, scope string, collection string) (ch.TimedSet, error)

	// ComputeRolesForUser returns the set of roles granted to the user by documents in all collections
	ComputeRolesForUser(ctx context.Context, u User) (ch.TimedSet, error)
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

var defaultCollectionMap = map[string]map[string]struct{}{base.DefaultScope: {base.DefaultCollection: struct{}{}}}

// Creates a new Authenticator that stores user info in the given Bucket.  Uses the default metadataKeys format
func NewAuthenticator(datastore base.DataStore, channelComputer ChannelComputer, options AuthenticatorOptions) *Authenticator {

	if len(options.Collections) == 0 {
		options.Collections = defaultCollectionMap
	}

	if options.MetaKeys == nil {
		options.MetaKeys = base.DefaultMetadataKeys
	}

	return &Authenticator{
		datastore:            datastore,
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
	princ, err := auth.getPrincipal(auth.DocIDForUser(name), func() Principal {
		return &userImpl{
			roleImpl: roleImpl{
				docID: auth.DocIDForUser(name),
			},
		}
	})
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
	docID := auth.DocIDForRole(name)
	princ, err := auth.getPrincipal(docID, func() Principal {
		return &roleImpl{
			docID: docID,
		}
	})
	role, _ := princ.(Role)
	return role, err
}

// Common implementation of GetUser and GetRole. factory() parameter returns a new empty instance.
// Returns (nil, nil) if the principal doesn't exist, or (nil, error) if getting it failed for some reason.
func (auth *Authenticator) getPrincipal(docID string, factory func() Principal) (Principal, error) {
	var princ Principal

	cas, err := auth.datastore.Update(docID, 0, func(currentValue []byte) ([]byte, *uint32, bool, error) {
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
		if !princ.IsDeleted() {
			// Check whether any collection's channel list has been invalidated by a doc update -- if so, rebuild it:
			channelsChanged, err := auth.rebuildChannels(princ)
			if err != nil {
				base.WarnfCtx(auth.LogCtx, "RebuildChannels returned error: %v", err)
				return nil, nil, false, err
			}
			if channelsChanged {
				changed = true
			}
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

// Rebuild channels computes the full set of channels for all collections defined for the authenticator.
// For each collection in Authenticator.collections:
//   - if there is no CollectionAccess on the principal for the collection, rebuilds channels for that collection
//   - If CollectionAccess on the principal has been invalidated, rebuilds channels for that collection
func (auth *Authenticator) rebuildChannels(princ Principal) (changed bool, err error) {

	changed = false
	for scope, collections := range auth.Collections {
		for collection, _ := range collections {
			// If collection channels are nil, they have been invalidated and must be rebuilt
			if princ.CollectionChannels(scope, collection) == nil {
				err := auth.rebuildCollectionChannels(princ, scope, collection)
				if err != nil {
					return changed, err
				}
				changed = true
			}
		}
	}
	return changed, nil
}

func (auth *Authenticator) rebuildCollectionChannels(princ Principal, scope, collection string) error {

	// For the default collection, rebuild the top-level channels properties on the principal.  Otherwise rebuild the appropriate entry
	// in the principal CollectionAccess map.
	var ca PrincipalCollectionAccess
	if base.IsDefaultCollection(scope, collection) {
		ca = princ
	} else {
		ca = princ.getOrCreateCollectionAccess(scope, collection)
	}

	channels := ca.ExplicitChannels().Copy()

	if auth.channelComputer != nil {
		viewChannels, err := auth.channelComputer.ComputeChannelsForPrincipal(auth.LogCtx, princ, scope, collection)
		if err != nil {
			base.WarnfCtx(auth.LogCtx, "channelComputer.ComputeChannelsForPrincipal returned error for %v: %v", base.UD(princ), err)
			return err
		}
		channels.Add(viewChannels)
	}

	if userCollectionAccess, ok := ca.(UserCollectionAccess); ok {
		if jwt := userCollectionAccess.JWTChannels(); jwt != nil {
			channels.Add(jwt)
		}
	}

	// always grant access to the public document channel
	channels.AddChannel(ch.DocumentStarChannel, 1)

	channelHistory := auth.calculateHistory(princ.Name(), ca.GetChannelInvalSeq(), ca.InvalidatedChannels(), channels, ca.ChannelHistory())

	if len(channelHistory) != 0 {
		ca.SetChannelHistory(channelHistory)
	}

	base.InfofCtx(auth.LogCtx, base.KeyAccess, "Recomputed channels for %q (%s.%s): %s", base.UD(princ.Name()), base.MD(scope), base.MD(collection), base.UD(channels))
	ca.SetChannelInvalSeq(0)
	ca.setChannels(channels)

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
	maxEntries = base.Min(maxEntries, maxHistoryEntriesPerGrant)

	// In the event maxEntries is negative or 0 we should set a floor of 1 entry
	maxEntries = base.Max(maxEntries, minHistoryEntriesPerGrant)

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

	if jwt := user.JWTRoles(); jwt != nil {
		roles.Add(jwt)
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
	_, err := auth.datastore.Get(auth.MetaKeys.UserEmailKey(email), &info)
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

	casOut, writeErr := auth.datastore.WriteCas(p.DocID(), 0, 0, p.Cas(), p, 0)
	if writeErr != nil {
		return writeErr
	}
	p.SetCas(casOut)

	if user, ok := p.(User); ok {
		if user.Email() != "" {
			info := userByEmailInfo{user.Name()}
			if err := auth.datastore.Set(auth.MetaKeys.UserEmailKey(user.Email()), 0, nil, info); err != nil {
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
	casOut, writeErr := auth.datastore.WriteCas(p.DocID(), 0, 0, p.Cas(), p, 0)
	if writeErr != nil {
		return writeErr
	}
	p.SetCas(casOut)

	return nil
}

func (auth *Authenticator) InvalidateDefaultChannels(name string, isUser bool, invalSeq uint64) error {
	return auth.InvalidateChannels(name, isUser, base.DefaultScope, base.DefaultCollection, invalSeq)
}

// Invalidates the channel list of a user/role by setting the ChannelInvalSeq to a non-zero value
func (auth *Authenticator) InvalidateChannels(name string, isUser bool, scope string, collection string, invalSeq uint64) error {
	var princ Principal
	var docID string

	if isUser {
		docID = auth.DocIDForUser(name)
		princ = &userImpl{
			roleImpl: roleImpl{
				docID: docID,
			},
		}
	} else {
		docID = auth.DocIDForRole(name)
		princ = &roleImpl{
			docID: docID,
		}
	}

	base.InfofCtx(auth.LogCtx, base.KeyAccess, "Invalidate access of %q", base.UD(name))

	subdocPath := "channel_inval_seq"
	if scope != base.DefaultScope || collection != base.DefaultCollection {
		subdocPath = "collection_access." + scope + "." + collection + "." + subdocPath
	}

	if subdocStore, ok := base.AsSubdocStore(auth.datastore); ok {
		err := subdocStore.SubdocInsert(docID, subdocPath, 0, invalSeq)
		if err != nil && err != base.ErrNotFound && err != base.ErrAlreadyExists && err != base.ErrPathNotFound {
			return err
		}
		return nil
	}

	_, err := auth.datastore.Update(docID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
		// If user/role doesn't exist cancel update
		if current == nil {
			return nil, nil, false, base.ErrUpdateCancel
		}

		err = base.JSONUnmarshal(current, &princ)
		if err != nil {
			return nil, nil, false, err
		}

		if princ.CollectionChannels(scope, collection) == nil {
			return nil, nil, false, base.ErrUpdateCancel
		}

		princ.setCollectionChannelInvalSeq(scope, collection, invalSeq)

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
	docID := auth.DocIDForUser(username)
	base.InfofCtx(auth.LogCtx, base.KeyAccess, "Invalidate roles of %q", base.UD(username))

	if subdocStore, ok := base.AsSubdocStore(auth.datastore); ok {
		err := subdocStore.SubdocInsert(docID, "role_inval_seq", 0, invalSeq)
		if err != nil && err != base.ErrNotFound && err != base.ErrAlreadyExists {
			return err
		}
		return nil
	}

	_, err := auth.datastore.Update(docID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
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
		if err := auth.datastore.Delete(auth.MetaKeys.UserEmailKey(user.Email())); err != nil {
			base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error deleting document ID for user email %s. Error: %v", base.UD(user.Email()), err)
		}
	}
	return auth.datastore.Delete(user.DocID())
}

func (auth *Authenticator) DeleteRole(role Role, purge bool, deleteSeq uint64) error {
	if purge {
		return auth.datastore.Delete(role.DocID())
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

func (auth *Authenticator) AuthenticateUntrustedJWT(rawToken string, oidcProviders OIDCProviderMap, localJWT LocalJWTProviderMap, callbackURLFunc OIDCCallbackURLFunc) (User, PrincipalConfig, error) {
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

	if single, ok := oidcProviders.getProviderWhenSingle(); ok && len(localJWT) == 0 {
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
		return nil, PrincipalConfig{}, base.HTTPErrorf(http.StatusUnauthorized, "Invalid JWT")
	}

	user, updates, _, err := auth.authenticateJWTIdentity(identity, authenticator.common())
	return user, updates, err
}

// Authenticates a user based on a JWT token obtained directly from a provider (auth code flow, refresh flow).
// Verifies the token claims, but doesn't require signature verification if allow_unsigned_provider_tokens is enabled.
// If the token is validated but the user for the username defined in the subject claim doesn't exist,
// creates the user when autoRegister=true.
func (auth *Authenticator) AuthenticateTrustedJWT(token string, provider *OIDCProvider, callbackURLFunc OIDCCallbackURLFunc) (user User,
	updates PrincipalConfig, tokenExpiry time.Time, err error) {
	base.DebugfCtx(auth.LogCtx, base.KeyAuth, "AuthenticateTrustedJWT called with token: %s", base.UD(token))

	var identity *Identity
	if provider.AllowUnsignedProviderTokens {
		// Verify claims - ensures that the token we received from the provider is valid for Sync Gateway
		identity, err = VerifyClaims(token, base.StringDefault(provider.ClientID, ""), provider.Issuer)
		if err != nil {
			base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error verifying raw token in AuthenticateTrustedJWT: %v", err)
			return nil, PrincipalConfig{}, time.Time{}, err
		}
	} else {
		// Verify claims and signature on the JWT.
		var verifyErr error
		identity, verifyErr = provider.verifyToken(auth.LogCtx, token, callbackURLFunc)
		if verifyErr != nil {
			return nil, PrincipalConfig{}, time.Time{}, verifyErr
		}
	}

	return auth.authenticateJWTIdentity(identity, provider.JWTConfigCommon)
}

// authenticateOIDCIdentity obtains a Sync Gateway User for the JWT. Expects that the JWT has already been verified for OIDC compliance.
// TODO: possibly move this function to oidc.go
func (auth *Authenticator) authenticateJWTIdentity(identity *Identity, provider JWTConfigCommon) (user User, updates PrincipalConfig, tokenExpiry time.Time, err error) {
	// Note: any errors returned from this function will be converted to 403s with a generic message, so we need to
	// separately log them to ensure they're preserved for debugging.
	if identity == nil || identity.Subject == "" {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Empty subject found in OIDC identity: %v", base.UD(identity))
		return nil, PrincipalConfig{}, time.Time{}, errors.New("subject not found in OIDC identity")
	}
	username, err := getJWTUsername(provider, identity)
	if err != nil {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error retrieving OIDCUsername: %v", err)
		return nil, PrincipalConfig{}, time.Time{}, err
	}
	base.DebugfCtx(auth.LogCtx, base.KeyAuth, "OIDCUsername: %v", base.UD(username))

	var jwtRoles, jwtChannels base.Set
	if provider.RolesClaim != "" {
		jwtRoles, err = getJWTClaimAsSet(identity, provider.RolesClaim)
		if err != nil {
			return nil, PrincipalConfig{}, time.Time{}, fmt.Errorf("failed to find JWT roles: %w", err)
		}
	} else {
		jwtRoles = base.Set{}
	}
	if provider.ChannelsClaim != "" {
		jwtChannels, err = getJWTClaimAsSet(identity, provider.ChannelsClaim)
		if err != nil {
			return nil, PrincipalConfig{}, time.Time{}, fmt.Errorf("failed to find JWT channels: %w", err)
		}
	} else {
		jwtChannels = base.Set{}
	}

	user, err = auth.GetUser(username)
	if err != nil {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error retrieving user for username %q: %v", base.UD(username), err)
		return nil, PrincipalConfig{}, time.Time{}, err
	}

	// Auto-registration. This will normally be done when token is originally returned
	// to client by oidc callback, but also needed here to handle clients obtaining their own tokens.
	if user == nil && provider.Register {
		base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Registering new user: %v with email: %v", base.UD(username), base.UD(identity.Email))
		var err error
		user, err = auth.RegisterNewUser(username, identity.Email)
		if err != nil && !base.IsCasMismatch(err) {
			base.DebugfCtx(auth.LogCtx, base.KeyAuth, "Error registering new user: %v", err)
			return nil, PrincipalConfig{}, time.Time{}, err
		}
	}

	if user != nil {
		now := time.Now()
		updates = PrincipalConfig{
			Name:           base.StringPtr(user.Name()),
			Email:          &identity.Email,
			JWTIssuer:      &provider.Issuer,
			JWTRoles:       jwtRoles,
			JWTChannels:    jwtChannels,
			JWTLastUpdated: &now,
		}
	}

	return user, updates, identity.Expiry, nil
}

// Registers a new user account based on the given verified username and optional email address.
// Password will be random. The user will have access to no channels.  If the user already exists,
// returns the existing user along with the cas failure error
func (auth *Authenticator) RegisterNewUser(username, email string) (User, error) {
	secret, err := base.GenerateRandomSecret()
	if err != nil {
		return nil, err
	}

	user, err := auth.NewUserNoChannels(username, secret)
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

func (a *Authenticator) DocIDForUser(username string) string {
	return a.MetaKeys.UserKey(username)
}

func (a *Authenticator) DocIDForRole(name string) string {
	return a.MetaKeys.RoleKey(name)
}

func (a *Authenticator) DocIDForUserEmail(email string) string {
	return a.MetaKeys.UserEmailKey(email)
}

func (a *Authenticator) DocIDForSession(sessionID string) string {
	return a.MetaKeys.SessionKey(sessionID)
}
