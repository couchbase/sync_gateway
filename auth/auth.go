//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

import (
	"encoding/json"
	"fmt"
	"golang.org/x/crypto/bcrypt"

	"github.com/coreos/go-oidc/jose"
	"github.com/coreos/go-oidc/oidc"
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	pkgerrors "github.com/pkg/errors"
)

/** Manages user authentication for a database. */
type Authenticator struct {
	bucket            base.Bucket
	channelComputer   ChannelComputer
	sessionCookieName string // Custom per-database session cookie name
}

// Interface for deriving the set of channels and roles a User/Role has access to.
// The instantiator of an Authenticator must provide an implementation.
type ChannelComputer interface {
	ComputeChannelsForPrincipal(Principal) (ch.TimedSet, error)
	ComputeRolesForUser(User) (ch.TimedSet, error)
	UseGlobalSequence() bool
}

type userByEmailInfo struct {
	Username string
}

const PrincipalUpdateMaxCasRetries = 20 // Maximum number of attempted retries on cas failure updating principal

// Creates a new Authenticator that stores user info in the given Bucket.
func NewAuthenticator(bucket base.Bucket, channelComputer ChannelComputer) *Authenticator {
	return &Authenticator{
		bucket:            bucket,
		channelComputer:   channelComputer,
		sessionCookieName: DefaultCookieName,
	}
}

func (auth *Authenticator) SessionCookieName() string {
	return auth.sessionCookieName
}

func (auth *Authenticator) SetSessionCookieName(cookieName string) {
	auth.sessionCookieName = cookieName
}

func docIDForUserEmail(email string) string {
	return "_sync:useremail:" + email
}

func (auth *Authenticator) UnmarshalPrincipal(data []byte, defaultName string, defaultSeq uint64, isUser bool) (Principal, error) {
	if isUser {
		return auth.UnmarshalUser(data, defaultName, defaultSeq)
	}
	return auth.UnmarshalRole(data, defaultName, defaultSeq)
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
	princ, err := auth.getPrincipal(docIDForRole(name), func() Principal { return &roleImpl{} })
	role, _ := princ.(Role)
	return role, err
}

// Common implementation of GetUser and GetRole. factory() parameter returns a new empty instance.
func (auth *Authenticator) getPrincipal(docID string, factory func() Principal) (Principal, error) {
	var princ Principal

	cas, err := auth.bucket.Update(docID, 0, func(currentValue []byte) ([]byte, *uint32, error) {
		// Be careful: this block can be invoked multiple times if there are races!
		if currentValue == nil {
			princ = nil
			return nil, nil, base.ErrUpdateCancel
		}

		princ = factory()
		if err := json.Unmarshal(currentValue, princ); err != nil {
			return nil, nil, pkgerrors.WithStack(base.RedactErrorf("json.Unmarshal() error for doc ID: %s in getPrincipal().  Error: %v", base.UD(docID), err))
		}
		changed := false
		if princ.Channels() == nil {
			// Channel list has been invalidated by a doc update -- rebuild it:
			if err := auth.rebuildChannels(princ); err != nil {
				base.Warnf(base.KeyAll, "RebuildChannels returned error: %v", err)
				return nil, nil, err
			}
			changed = true
		}
		if user, ok := princ.(User); ok {
			if user.RoleNames() == nil {
				if err := auth.rebuildRoles(user); err != nil {
					base.Warnf(base.KeyAll, "RebuildRoles returned error: %v", err)
					return nil, nil, err
				}
				changed = true
			}
		}

		if changed {
			// Save the updated doc:
			updatedBytes, marshalErr := json.Marshal(princ)
			if marshalErr != nil {
				marshalErr = pkgerrors.WithStack(base.RedactErrorf("json.Unmarshal() error for doc ID: %s in getPrincipal(). Error: %v", base.UD(docID), marshalErr))
			}
			return updatedBytes, nil, marshalErr
		} else {
			// Principal is valid, so stop the update
			return nil, nil, base.ErrUpdateCancel
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

	// Changes for vbucket sequence management.  We can't determine relative ordering of sequences
	// across vbuckets.  To avoid redundant channel backfills during changes processing, we maintain
	// the previous vb/seq for a channel in PreviousChannels.  If that channel is still present during
	// this rebuild, we reuse the vb/seq from PreviousChannels (using UpdateIfPresent).  If PreviousChannels
	// is nil, reverts to normal sequence handling.

	previousChannels := princ.PreviousChannels().Copy()
	if previousChannels != nil {
		channels.UpdateIfPresent(previousChannels)
	}

	if auth.channelComputer != nil {
		viewChannels, err := auth.channelComputer.ComputeChannelsForPrincipal(princ)
		if err != nil {
			base.Warnf(base.KeyAll, "channelComputer.ComputeChannelsForPrincipal returned error for %v: %v", base.UD(princ), err)
			return err
		}
		if previousChannels != nil {
			viewChannels.UpdateIfPresent(previousChannels)
		}
		channels.Add(viewChannels)
	}
	// always grant access to the public document channel
	channels.AddChannel(ch.DocumentStarChannel, 1)

	base.Infof(base.KeyAccess, "Computed channels for %q: %s", base.UD(princ.Name()), base.UD(channels))
	princ.SetPreviousChannels(nil)
	princ.setChannels(channels)

	return nil

}

func (auth *Authenticator) rebuildRoles(user User) error {
	var roles ch.TimedSet
	if auth.channelComputer != nil {
		var err error
		roles, err = auth.channelComputer.ComputeRolesForUser(user)
		if err != nil {
			base.Warnf(base.KeyAll, "channelComputer.ComputeRolesForUser failed on user %s: %v", base.UD(user.Name()), err)
			return err
		}
	}
	if roles == nil {
		roles = ch.TimedSet{} // it mustn't be nil; nil means it's unknown
	}

	if explicit := user.ExplicitRoles(); explicit != nil {
		roles.Add(explicit)
	}

	base.Infof(base.KeyAccess, "Computed roles for %q: %s", base.UD(user.Name()), base.UD(roles))
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
			if err := auth.bucket.Set(docIDForUserEmail(user.Email()), 0, info); err != nil {
				return err
			}
			//FIX: Fail if email address is already registered to another user
			//FIX: Unregister old email address if any
		}
	}
	base.Infof(base.KeyAuth, "Saved %s: %s", base.UD(p.DocID()), base.UD(p))
	return nil
}

// Invalidates the channel list of a user/role by saving its Channels() property as nil.
func (auth *Authenticator) InvalidateChannels(p Principal) error {
	invalidateChannelsCallback := func(p Principal) (updatedPrincipal Principal, err error) {

		if p == nil || p.Channels() == nil {
			return p, base.ErrUpdateCancel
		}

		base.Infof(base.KeyAccess, "Invalidate access of %q", base.UD(p.Name()))
		if auth.channelComputer != nil && !auth.channelComputer.UseGlobalSequence() {
			p.SetPreviousChannels(p.Channels())
		}
		p.setChannels(nil)
		return p, nil
	}
	return auth.casUpdatePrincipal(p, invalidateChannelsCallback)
}

// Invalidates the role list of a user by saving its Roles() property as nil.
func (auth *Authenticator) InvalidateRoles(user User) error {

	invalidateRolesCallback := func(p Principal) (updatedPrincipal Principal, err error) {
		user, ok := p.(User)
		if !ok {
			return p, base.ErrUpdateCancel
		}
		if user == nil || user.RoleNames() == nil {
			return p, base.ErrUpdateCancel
		}
		base.Infof(base.KeyAccess, "Invalidate roles of %q", base.UD(user.Name()))
		user.setRolesSince(nil)
		return user, nil
	}

	return auth.casUpdatePrincipal(user, invalidateRolesCallback)
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

		base.Debugf(base.KeyAuth, "Updating user %s email to: %v", base.UD(u.Name()), base.UD(email))
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
	if !bcryptCostChanged {
		return nil
	}
	var hashCost int
	rehashPasswordCallback := func(currentPrincipal Principal) (updatedPrincipal Principal, err error) {

		currentUserImpl, ok := currentPrincipal.(*userImpl)
		if !ok {
			return nil, base.ErrUpdateCancel
		}

		hashCost, costErr := bcrypt.Cost(currentUserImpl.PasswordHash_)
		if costErr == nil && hashCost != bcryptCost {
			// the cost of the existing hash is different than the configured bcrypt cost.
			// We'll re-hash the password to adopt the new cost:
			currentUserImpl.SetPassword(password)
			return currentUserImpl, nil
		} else {
			return nil, base.ErrUpdateCancel
		}
	}

	if err := auth.casUpdatePrincipal(user, rehashPasswordCallback); err != nil {
		return err
	}

	base.Debugf(base.KeyAuth, "User account %q changed password hash cost from %d to %d",
		base.UD(user.Name()), hashCost, bcryptCost)
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

		base.Infof(base.KeyAuth, "CAS mismatch in casUpdatePrincipal, retrying.  Principal:%s", base.UD(p.Name()))

		switch p.(type) {
		case User:
			p, err = auth.GetUser(p.Name())
		case Role:
			p, err = auth.GetRole(p.Name())
		default:
			return fmt.Errorf("Unsupported principal type in casUpdatePrincipal (%T)", p)
		}

		if err != nil {
			return err
		}
	}
	base.Infof(base.KeyAuth, "Unable to update principal after %d attempts.  Principal:%s Error:%v", PrincipalUpdateMaxCasRetries, base.UD(p.Name()), err)
	return err
}

// Deletes a user/role.
func (auth *Authenticator) Delete(p Principal) error {
	if user, ok := p.(User); ok {
		if user.Email() != "" {
			auth.bucket.Delete(docIDForUserEmail(user.Email()))
		}
	}
	return auth.bucket.Delete(p.DocID())
}

// Authenticates a user given the username and password.
// If the username and password are both "", it will return a default empty User object, not nil.
func (auth *Authenticator) AuthenticateUser(username string, password string) User {
	user, _ := auth.GetUser(username)
	if user == nil || !user.Authenticate(password) {
		return nil
	}
	return user
}

// Authenticates a user based on a JWT token string and a set of providers.  Attempts to match the
// issuer in the token with a provider.
// Used to authenticate a JWT token coming from an insecure source (e.g. client request)
// If the token is validated but the user for the username defined in the subject claim doesn't exist,
// creates the user when autoRegister=true.
func (auth *Authenticator) AuthenticateUntrustedJWT(token string, providers OIDCProviderMap, callbackURLFunc OIDCCallbackURLFunc) (User, jose.JWT, error) {

	base.Debugf(base.KeyAuth, "AuthenticateJWT called with token: %s", base.UD(token))

	// Parse JWT (needed to determine issuer/provider)
	jwt, err := jose.ParseJWT(token)
	if err != nil {
		base.Debugf(base.KeyAuth, "Error parsing JWT in AuthenticateJWT: %v", err)
		return nil, jose.JWT{}, err
	}

	// Get client for issuer
	issuer, audiences, err := GetJWTIssuer(jwt)
	base.Debugf(base.KeyAuth, "JWT issuer: %v, audiences: %v", base.UD(issuer), base.UD(audiences))
	if err != nil {
		base.Debugf(base.KeyAuth, "Error getting JWT issuer: %v", err)
		return nil, jose.JWT{}, err
	}

	base.Debugf(base.KeyAuth, "Call GetProviderForIssuer w/ providers: %+v", base.UD(providers))
	provider := providers.GetProviderForIssuer(issuer, audiences)
	base.Debugf(base.KeyAuth, "Provider for issuer: %+v", base.UD(provider))

	if provider == nil {
		return nil, jose.JWT{}, base.RedactErrorf("No provider found for issuer %v", base.UD(issuer))
	}

	// VerifyJWT validates the claims and signature on the JWT
	client := provider.GetClient(callbackURLFunc)
	if client == nil {
		return nil, jose.JWT{}, fmt.Errorf("OIDC client was not initialized")
	}

	err = client.VerifyJWT(jwt)
	if err != nil {
		base.Debugf(base.KeyAuth, "Client %v could not verify JWT. Error: %v", base.UD(client), err)
		return nil, jwt, err
	}

	return auth.authenticateJWT(jwt, provider)
}

// Authenticates a user based on a JWT token obtained directly from a provider (auth code flow, refresh flow).
// Verifies the token claims, but doesn't require signature verification.
// If the token is validated but the user for the username defined in the subject claim doesn't exist,
// creates the user when autoRegister=true.
func (auth *Authenticator) AuthenticateTrustedJWT(token string, provider *OIDCProvider, callbackURLFunc OIDCCallbackURLFunc) (User, jose.JWT, error) {

	// Parse JWT
	jwt, err := jose.ParseJWT(token)
	if err != nil {
		base.Debugf(base.KeyAuth, "Error parsing JWT in AuthenticateTrustedJWT: %v", err)
		return nil, jose.JWT{}, err
	}

	// Verify claims - ensures that the token we received from the provider is valid for Sync Gateway
	if err := oidc.VerifyClaims(jwt, provider.Issuer, *provider.ClientID); err != nil {
		return nil, jose.JWT{}, err
	}
	return auth.authenticateJWT(jwt, provider)
}

// Obtains a Sync Gateway User for the JWT.  Expects that the JWT has already been verified for OIDC compliance.
func (auth *Authenticator) authenticateJWT(jwt jose.JWT, provider *OIDCProvider) (User, jose.JWT, error) {

	// Extract identity from token
	identity, identityErr := GetJWTIdentity(jwt)
	base.Debugf(base.KeyAuth, "JWT identity: %+v", base.UD(identity))
	if identityErr != nil {
		base.Debugf(base.KeyAuth, "Error getting JWT identity. Error: %v", identityErr)
		return nil, jwt, identityErr
	}

	if domainErr := ValidateAllowedDomain(provider, identity.Email); domainErr != nil {
		base.Debugf(base.KeyAuth, "Error validating user %s : %v", base.UD(identity.Email), domainErr)
		return nil, jwt, domainErr
	}

	username := GetOIDCUsername(provider, identity.ID)
	base.Debugf(base.KeyAuth, "OIDCUsername: %v", base.UD(username))

	user, userErr := auth.GetUser(username)
	if userErr != nil {
		base.Debugf(base.KeyAuth, "Failed to get OIDC User from %v.  Error: %v", base.UD(username), userErr)
		return nil, jwt, userErr
	}

	// If user found, check whether the email needs to be updated (e.g. user has changed email in
	// external auth system)
	if user != nil && identity.Email != "" {
		err := auth.UpdateUserEmail(user, identity.Email)
		if err != nil {
			base.Warnf(base.KeyAll, "Unable to set user email to %v for OIDC", base.UD(identity.Email))
		}
	}

	// Auto-registration.  This will normally be done when token is originally returned
	// to client by oidc callback, but also needed here to handle clients obtaining their own tokens.
	if user == nil && provider.Register {
		base.Debugf(base.KeyAuth, "Registering new user: %v with email: %v", base.UD(username), base.UD(identity.Email))
		var err error
		user, err = auth.RegisterNewUser(username, identity.Email)
		if err != nil && !base.IsCasMismatch(err) {
			base.Debugf(base.KeyAuth, "Error registering new user: %v", err)
			return nil, jwt, err
		}
	}

	return user, jwt, nil
}

// Registers a new user account based on the given verified username and optional email address.
// Password will be random. The user will have access to no channels.  If the user already exists,
// returns the existing user along with the cas failure error
func (auth *Authenticator) RegisterNewUser(username, email string) (User, error) {
	user, err := auth.NewUser(username, base.GenerateRandomSecret(), base.Set{})
	if err != nil {
		return nil, err
	}

	if len(email) > 0 {
		user.SetEmail(email)
	}

	err = auth.Save(user)
	if base.IsCasMismatch(err) {
		return auth.GetUser(username)
	} else if err != nil {
		return nil, err
	}

	return user, err
}

func (auth *Authenticator) UpdateRoleVbucketSequences(docID string, sequence uint64) error {
	return auth.updateVbucketSequences(docID, func() Principal { return &roleImpl{} }, sequence)
}

func (auth *Authenticator) UpdateUserVbucketSequences(docID string, sequence uint64) error {
	return auth.updateVbucketSequences(docID, func() Principal { return &userImpl{} }, sequence)
}

// Updates any entries in the admin (explicit) channel set that have
// their sequence set to zero, to the provided sequence, and invalidates the user channels.
// Used during distributed index processing, where the sequence value is the vb sequence, and
// isn't known at initial write time. Using bucket.Update for cas handling, similar to updatePrincipal.
func (auth *Authenticator) updateVbucketSequences(docID string, factory func() Principal, seq uint64) error {

	sequence := ch.NewVbSimpleSequence(seq)
	_, err := auth.bucket.Update(docID, 0, func(currentValue []byte) ([]byte, *uint32, error) {
		// Be careful: this block can be invoked multiple times if there are races!
		if currentValue == nil {
			return nil, nil, base.ErrUpdateCancel
		}
		princ := factory()
		if err := json.Unmarshal(currentValue, princ); err != nil {
			return nil, nil, err
		}
		channelsChanged := false
		for channel, vbSeq := range princ.ExplicitChannels() {
			seq := vbSeq.Sequence
			if seq == 0 {
				switch p := princ.(type) {
				case *roleImpl:
					p.ExplicitChannels_[channel] = sequence
				case *userImpl:
					p.ExplicitChannels_[channel] = sequence
				}
				channelsChanged = true
			}
		}
		// Invalidate calculated channels if changed.
		if channelsChanged {
			princ.setChannels(nil)
		}

		// If user, also check for explicit roles.
		rolesChanged := false
		if userPrinc, ok := princ.(*userImpl); ok {
			for role, vbSeq := range userPrinc.ExplicitRoles() {
				seq := vbSeq.Sequence
				if seq == 0 {
					userPrinc.ExplicitRoles_[role] = sequence
					rolesChanged = true
				}
			}
			// Invalidate calculated roles if changed.
			if rolesChanged {
				userPrinc.setRolesSince(nil)
			}
		}

		princ.SetSequence(seq)

		if channelsChanged || rolesChanged {
			// Save the updated principal doc.
			updatedBytes, marshalErr := json.Marshal(princ)
			return updatedBytes, nil, marshalErr
		} else {
			// No entries found requiring update, so cancel update.
			return nil, nil, base.ErrUpdateCancel
		}
	})

	if err != nil && err != base.ErrUpdateCancel {
		return err
	}
	return nil
}
