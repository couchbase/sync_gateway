/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

func (db *DatabaseContext) DeleteRole(ctx context.Context, name string, purge bool) error {
	authenticator := db.Authenticator(ctx)

	role, err := authenticator.GetRole(name)
	if err != nil {
		return err
	}

	if role == nil {
		return base.ErrNotFound
	}

	seq, err := db.sequences.nextSequence(ctx)
	if err != nil {
		return err
	}

	return authenticator.DeleteRole(role, purge, seq)
}

// UpdatePrincipal updates or creates a principal from a PrincipalConfig structure.
func (dbc *DatabaseContext) UpdatePrincipal(ctx context.Context, updates *auth.PrincipalConfig, isUser bool, allowReplace bool) (replaced bool, err error) {
	// Sanity checking
	if !base.AllOrNoneNil(updates.JWTIssuer, updates.JWTRoles, updates.JWTChannels) {
		return false, fmt.Errorf("must either specify all OIDC properties or none")
	}

	// Get the existing principal, or if this is a POST make sure there isn't one:
	var princ auth.Principal
	var user auth.User
	authenticator := dbc.Authenticator(ctx)

	// Retry handling for cas failure during principal update.  Limiting retry attempts
	// to PrincipalUpdateMaxCasRetries defensively to avoid unexpected retry loops.
	for i := 1; i <= auth.PrincipalUpdateMaxCasRetries; i++ {
		if isUser {
			user, err = authenticator.GetUser(*updates.Name)
			princ = user
		} else {
			princ, err = authenticator.GetRole(*updates.Name)
		}
		if err != nil {
			return replaced, err
		}

		changed := false
		replaced = (princ != nil)
		if !replaced {
			if updates.Name == nil || *updates.Name == "" {
				return replaced, fmt.Errorf("UpdatePrincipal: cannot create principal with empty name")
			}
			// If user/role didn't exist already, instantiate a new one:
			if isUser {
				isValid, reason := updates.IsPasswordValid(dbc.AllowEmptyPassword)
				if !isValid {
					err = base.HTTPErrorf(http.StatusBadRequest, "Error creating user: %s", reason)
					return replaced, err
				}
				user, err = authenticator.NewUserNoChannels(*updates.Name, "")
				princ = user
			} else {
				princ, err = authenticator.NewRoleNoChannels(*updates.Name)
			}
			if err != nil {
				return replaced, fmt.Errorf("Error creating user/role: %w", err)
			}
			changed = true
		} else if !allowReplace {
			err = base.HTTPErrorf(http.StatusConflict, "Already exists")
			return
		} else if isUser && updates.Password != nil {
			isValid, reason := updates.IsPasswordValid(dbc.AllowEmptyPassword)
			if !isValid {
				err = base.HTTPErrorf(http.StatusBadRequest, "Error updating user/role: %s", reason)
				return replaced, err
			}
		}

		// Ensure the caller isn't trying to set all_channels or roles explicitly - it'll get recomputed automatically.
		if len(updates.Channels) > 0 && !princ.Channels().Equals(updates.Channels) {
			return false, base.HTTPErrorf(http.StatusBadRequest, "all_channels is read-only")
		}
		if user, ok := princ.(auth.User); ok {
			if len(updates.RoleNames) > 0 && !user.RoleNames().Equals(base.SetFromArray(updates.RoleNames)) {
				return false, base.HTTPErrorf(http.StatusBadRequest, "roles is read-only")
			}
		}

		updatedExplicitChannels := princ.CollectionExplicitChannels(base.DefaultScope, base.DefaultCollection)
		if updatedExplicitChannels == nil {
			updatedExplicitChannels = ch.TimedSet{}
		}
		if updates.ExplicitChannels != nil && !updatedExplicitChannels.Equals(updates.ExplicitChannels) {
			changed = true
		}
		collectionAccessChanged, err := dbc.RequiresCollectionAccessUpdate(ctx, princ, updates.CollectionAccess)
		if err != nil {
			return false, err
		} else if collectionAccessChanged {
			changed = true
		}

		var updatedExplicitRoles, updatedJWTRoles, updatedJWTChannels ch.TimedSet

		// Then the user-specific fields like roles:
		if isUser {
			if updates.Email != nil && *updates.Email != user.Email() {
				if err := user.SetEmail(*updates.Email); err != nil {
					base.WarnfCtx(ctx, "Skipping SetEmail for user %q - Invalid email address provided: %q", base.UD(updates.Name), base.UD(*updates.Email))
				}
				changed = true
			}
			if updates.Password != nil {
				err = user.SetPassword(*updates.Password)
				if err != nil {
					return false, err
				}
				changed = true
			}
			if updates.Disabled != nil && *updates.Disabled != user.Disabled() {
				user.SetDisabled(*updates.Disabled)
				changed = true
			}

			updatedExplicitRoles = user.ExplicitRoles()
			if updatedExplicitRoles == nil {
				updatedExplicitRoles = ch.TimedSet{}
			}
			if updates.ExplicitRoleNames != nil && !updatedExplicitRoles.Equals(updates.ExplicitRoleNames) {
				changed = true
			}

			if updates.JWTIssuer != nil && *updates.JWTIssuer != user.JWTIssuer() {
				user.SetJWTIssuer(*updates.JWTIssuer)
				changed = true
			}

			updatedJWTRoles = user.JWTRoles()
			if updatedJWTRoles == nil {
				updatedJWTRoles = ch.TimedSet{}
			}
			if updates.JWTRoles != nil && !updatedJWTRoles.Equals(updates.JWTRoles) {
				changed = true
			}

			updatedJWTChannels = user.JWTChannels()
			if updatedJWTChannels == nil {
				updatedJWTChannels = ch.TimedSet{}
			}
			if updates.JWTChannels != nil && !updatedJWTChannels.Equals(updates.JWTChannels) {
				changed = true
			}
		}

		// And finally save the Principal if anything has changed:
		if !changed {
			return replaced, nil
		}

		// Update the persistent sequence number of this principal (only allocate a sequence when needed - issue #673):
		nextSeq := uint64(0)

		nextSeq, err = dbc.sequences.nextSequence(ctx)
		if err != nil {
			return replaced, err
		}
		princ.SetSequence(nextSeq)

		// Now update the Principal object from the properties in the request, first the channels:
		if updates.ExplicitChannels != nil && updatedExplicitChannels.UpdateAtSequence(updates.ExplicitChannels, nextSeq) {
			princ.SetExplicitChannels(updatedExplicitChannels, nextSeq)
		}
		if collectionAccessChanged {
			dbc.UpdateCollectionExplicitChannels(ctx, princ, updates.CollectionAccess, nextSeq)
		}

		if isUser {
			if updates.ExplicitRoleNames != nil && updatedExplicitRoles.UpdateAtSequence(updates.ExplicitRoleNames, nextSeq) {
				user.SetExplicitRoles(updatedExplicitRoles, nextSeq)
			}
			var hasJWTUpdates bool
			if updates.JWTRoles != nil && updatedJWTRoles.UpdateAtSequence(updates.JWTRoles, nextSeq) {
				user.SetJWTRoles(updatedJWTRoles, nextSeq)
				hasJWTUpdates = true
			}
			if updates.JWTChannels != nil && updatedJWTChannels.UpdateAtSequence(updates.JWTChannels, nextSeq) {
				user.SetJWTChannels(updatedJWTChannels, nextSeq)
				hasJWTUpdates = true
			}
			if hasJWTUpdates {
				user.SetJWTLastUpdated(time.Now())
			}
		}
		err = authenticator.Save(princ)
		// On cas error, retry.  Otherwise break out of loop
		if base.IsCasMismatch(err) {
			base.InfofCtx(ctx, base.KeyAuth, "CAS mismatch updating principal %s - will retry", base.UD(princ.Name()))
		} else {
			return replaced, err
		}
	}

	base.ErrorfCtx(ctx, "CAS mismatch updating principal %s - exceeded retry count. Latest failure: %v", base.UD(princ.Name()), err)
	return replaced, err
}

// UpdateCollectionExplicitChannels identifies whether a config update requires an update to the principal's collectionAccess.
func (dbc *DatabaseContext) UpdateCollectionExplicitChannels(ctx context.Context, princ auth.Principal, updates map[string]map[string]*auth.CollectionAccessConfig, seq uint64) {
	authenticator := dbc.Authenticator(ctx)
	base.InfofCtx(ctx, base.KeyAuth, "History at UpdateCollectionExplicitChannels", princ.ChannelHistory())

	for scopeName, scope := range updates {
		if scope == nil {
			// TODO: do we need the ability to delete a whole scope at once?  Probably not necessary
			//existed := princ.DeleteScopeExplicitChannels(scopeName)
			//if existed {
			//	updated = true
			//}
			base.InfofCtx(ctx, base.KeyAuth, "Scope %s did not specify any collections during principal update - ignoring", base.MD(scopeName))
		} else {
			for collectionName, updatedCollectionAccess := range scope {
				if updatedCollectionAccess == nil {
					if princ.CollectionExplicitChannels(scopeName, collectionName) != nil {
						princ.SetCollectionExplicitChannels(scopeName, collectionName, nil, seq)
					}
				} else {
					updatedExplicitChannels := princ.CollectionExplicitChannels(scopeName, collectionName)
					if updatedExplicitChannels == nil {
						updatedExplicitChannels = ch.TimedSet{}
					}
					expChannels := princ.CollectionExplicitChannels(scopeName, collectionName).Copy()
					allExplicitChannels := expChannels.Copy()
					allExplicitChannels.Add(updatedExplicitChannels)
					changed := updatedExplicitChannels.UpdateAtSequence(updatedCollectionAccess.ExplicitChannels_, seq)
					if changed {
						princ.SetCollectionExplicitChannels(scopeName, collectionName, updatedExplicitChannels, seq)
						history := authenticator.CalculateHistory(princ.Name(), princ.GetChannelInvalSeq(), expChannels, updatedExplicitChannels, princ.ChannelHistory())
						for channel, hist := range history {
							if _, ok := allExplicitChannels[channel]; ok {
								hist.AdminAssigned = true
								history[channel] = hist
							}
						}
						princ.SetChannelHistory(history)
					}
				}
			}
		}
	}
}

// RequiresCollectionAccessUpdate returns true if the provided map of CollectionAccessConfig requires an update to the principal
func (dbc *DatabaseContext) RequiresCollectionAccessUpdate(ctx context.Context, princ auth.Principal, updates map[string]map[string]*auth.CollectionAccessConfig) (bool, error) {
	requiresUpdate := false
	for scopeName, scope := range updates {
		if scope != nil {
			for collectionName, updatedCollectionAccess := range scope {
				_, err := dbc.GetDatabaseCollection(scopeName, collectionName)
				if err != nil {
					return false, base.HTTPErrorf(http.StatusNotFound, "keyspace specified in collection_access (%s) not found", fmt.Sprintf("%s.%s.%s", dbc.Name, scopeName, collectionName))
				}
				if updatedCollectionAccess.Channels_ != nil {
					return false, base.HTTPErrorf(http.StatusBadRequest, "collection_access.all_channels is read-only")
				}
				if updatedCollectionAccess.JWTChannels_ != nil {
					return false, base.HTTPErrorf(http.StatusBadRequest, "collection_access.jwt_channels is read-only")
				}
				if updatedCollectionAccess.JWTLastUpdated != nil {
					return false, base.HTTPErrorf(http.StatusBadRequest, "collection_access.jwt_last_updated is read-only")
				}
				if updatedCollectionAccess == nil {
					if princ.CollectionExplicitChannels(scopeName, collectionName) != nil {
						requiresUpdate = true
					}
				} else {
					if !princ.CollectionExplicitChannels(scopeName, collectionName).Equals(updatedCollectionAccess.ExplicitChannels_) {
						requiresUpdate = true
					}
				}
			}
		}
	}

	return requiresUpdate, nil
}
