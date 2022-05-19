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

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

// PrincipalConfig represents a user/role as a JSON object.
// Used to define a user/role within DbConfig, and structures the request/response body in the admin REST API
// for /db/_user/*
type PrincipalConfig struct {
	Name             *string  `json:"name,omitempty"`
	ExplicitChannels base.Set `json:"admin_channels,omitempty"`
	// Fields below only apply to Users, not Roles:
	Email             string   `json:"email,omitempty"`
	Disabled          *bool    `json:"disabled,omitempty"`
	Password          *string  `json:"password,omitempty"`
	ExplicitRoleNames []string `json:"admin_roles,omitempty"`
	RoleNames         []string `json:"roles,omitempty"`
	// Fields below are read-only
	Channels base.Set `json:"all_channels,omitempty"`
}

// AsPrincipalUpdates converts this PrincipalConfig into a PrincipalUpdates structure.
func (p PrincipalConfig) AsPrincipalUpdates() auth.PrincipalUpdates {
	roles := base.SetFromArray(p.ExplicitRoleNames)
	return auth.PrincipalUpdates{
		Name:              *p.Name,
		ExplicitChannels:  &p.ExplicitChannels,
		Email:             base.StringPtr(p.Email),
		Disabled:          p.Disabled,
		Password:          p.Password,
		ExplicitRoleNames: &roles,
	}
}

func (db *DatabaseContext) DeleteRole(ctx context.Context, name string, purge bool) error {
	authenticator := db.Authenticator(ctx)

	role, err := authenticator.GetRole(name)
	if err != nil {
		return err
	}

	if role == nil {
		return base.ErrNotFound
	}

	seq, err := db.sequences.nextSequence()
	if err != nil {
		return err
	}

	return authenticator.DeleteRole(role, purge, seq)
}

// UpdatePrincipal updates or creates a principal from auth.PrincipalUpdates structure.
func (dbc *DatabaseContext) UpdatePrincipal(ctx context.Context, updates auth.PrincipalUpdates, isUser bool, allowReplace bool) (replaced bool, err error) {
	// Get the existing principal, or if this is a POST make sure there isn't one:
	var princ auth.Principal
	var user auth.User
	authenticator := dbc.Authenticator(ctx)

	// Retry handling for cas failure during principal update.  Limiting retry attempts
	// to PrincipalUpdateMaxCasRetries defensively to avoid unexpected retry loops.
	for i := 1; i <= auth.PrincipalUpdateMaxCasRetries; i++ {
		if isUser {
			user, err = authenticator.GetUser(updates.Name)
			princ = user
		} else {
			princ, err = authenticator.GetRole(updates.Name)
		}
		if err != nil {
			return replaced, err
		}

		changed := false
		replaced = (princ != nil)
		if !replaced {
			if updates.Name == "" {
				return replaced, fmt.Errorf("UpdatePrincipal: cannot create principal with empty name")
			}
			// If user/role didn't exist already, instantiate a new one:
			if isUser {
				isValid, reason := updates.IsPasswordValid(dbc.AllowEmptyPassword)
				if !isValid {
					err = base.HTTPErrorf(http.StatusBadRequest, "Error creating user: %s", reason)
					return replaced, err
				}
				user, err = authenticator.NewUser(updates.Name, "", nil)
				princ = user
			} else {
				princ, err = authenticator.NewRole(updates.Name, nil)
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

		updatedExplicitChannels := princ.ExplicitChannels()
		if updatedExplicitChannels == nil {
			updatedExplicitChannels = ch.TimedSet{}
		}
		if updates.ExplicitChannels != nil && !updatedExplicitChannels.Equals(*updates.ExplicitChannels) {
			changed = true
		}

		var updatedExplicitRoles ch.TimedSet

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
			if updates.ExplicitRoleNames != nil && !updatedExplicitRoles.Equals(*updates.ExplicitRoleNames) {
				changed = true
			}
		}

		// And finally save the Principal if anything has changed:
		if !changed {
			return replaced, nil
		}

		// Update the persistent sequence number of this principal (only allocate a sequence when needed - issue #673):
		nextSeq := uint64(0)
		var err error
		nextSeq, err = dbc.sequences.nextSequence()
		if err != nil {
			return replaced, err
		}
		princ.SetSequence(nextSeq)

		// Now update the Principal object from the properties in the request, first the channels:
		if updates.ExplicitChannels != nil && updatedExplicitChannels.UpdateAtSequence(*updates.ExplicitChannels, nextSeq) {
			princ.SetExplicitChannels(updatedExplicitChannels, nextSeq)
		}

		if isUser {
			if updates.ExplicitRoleNames != nil && updatedExplicitRoles.UpdateAtSequence(*updates.ExplicitRoleNames, nextSeq) {
				user.SetExplicitRoles(updatedExplicitRoles, nextSeq)
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
