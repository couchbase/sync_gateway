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

// Struct that configures settings of a User/Role, for UpdatePrincipal.
// Also used in the rest package as a JSON object that defines a User/Role within a DbConfig
// and structures the request/response body in the admin REST API for /db/_user/*
type PrincipalConfig struct {
	Name             *string  `json:"name,omitempty"`
	ExplicitChannels base.Set `json:"admin_channels,omitempty"`
	Channels         base.Set `json:"all_channels,omitempty"`
	// Fields below only apply to Users, not Roles:
	Email             string   `json:"email,omitempty"`
	Disabled          *bool    `json:"disabled,omitempty"`
	Password          *string  `json:"password,omitempty"`
	ExplicitRoleNames []string `json:"admin_roles,omitempty"`
	RoleNames         []string `json:"roles,omitempty"`
}

// Check if the password in this PrincipalConfig is valid.  Only allow
// empty passwords if allowEmptyPass is true.
func (p PrincipalConfig) IsPasswordValid(allowEmptyPass bool) (isValid bool, reason string) {
	// if it's an anon user, they should not have a password
	if p.Name == nil {
		if p.Password != nil {
			return false, "Anonymous users should not have a password"
		} else {
			return true, ""
		}
	}

	/*
		if allowEmptyPass && ( p.Password == nil || len(*p.Password) == 0) {
			return true, ""
		}

		if p.Password == nil || (p.Password != nil && len(*p.Password) < 3) {
			return false, "Passwords must be at least three 3 characters"
		}
	*/

	if p.Password == nil || len(*p.Password) == 0 {
		if !allowEmptyPass {
			return false, "Empty passwords are not allowed "
		}
	} else if len(*p.Password) < 3 {
		return false, "Passwords must be at least three 3 characters"
	}

	return true, ""
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

// Updates or creates a principal from a PrincipalConfig structure.
func (dbc *DatabaseContext) UpdatePrincipal(ctx context.Context, newInfo PrincipalConfig, isUser bool, allowReplace bool) (replaced bool, err error) {
	// Get the existing principal, or if this is a POST make sure there isn't one:
	var princ auth.Principal
	var user auth.User
	authenticator := dbc.Authenticator(ctx)

	// Retry handling for cas failure during principal update.  Limiting retry attempts
	// to PrincipalUpdateMaxCasRetries defensively to avoid unexpected retry loops.
	for i := 1; i <= auth.PrincipalUpdateMaxCasRetries; i++ {
		if isUser {
			user, err = authenticator.GetUser(*newInfo.Name)
			princ = user
		} else {
			princ, err = authenticator.GetRole(*newInfo.Name)
		}
		if err != nil {
			return replaced, err
		}

		changed := false
		replaced = (princ != nil)
		if !replaced {
			// If user/role didn't exist already, instantiate a new one:
			if isUser {
				isValid, reason := newInfo.IsPasswordValid(dbc.AllowEmptyPassword)
				if !isValid {
					err = base.HTTPErrorf(http.StatusBadRequest, "Error creating user: %s", reason)
					return replaced, err
				}
				user, err = authenticator.NewUser(*newInfo.Name, "", nil)
				princ = user
			} else {
				princ, err = authenticator.NewRole(*newInfo.Name, nil)
			}
			if err != nil {
				return replaced, fmt.Errorf("Error creating user/role: %w", err)
			}
			changed = true
		} else if !allowReplace {
			err = base.HTTPErrorf(http.StatusConflict, "Already exists")
			return
		} else if isUser && newInfo.Password != nil {
			isValid, reason := newInfo.IsPasswordValid(dbc.AllowEmptyPassword)
			if !isValid {
				err = base.HTTPErrorf(http.StatusBadRequest, "Error updating user/role: %s", reason)
				return replaced, err
			}
		}

		// Ensure the caller isn't trying to set all_channels explicitly - it'll get recomputed automatically.
		if len(newInfo.Channels) > 0 && !princ.Channels().Equals(newInfo.Channels) {
			return false, base.HTTPErrorf(http.StatusBadRequest, "all_channels is read-only")
		}

		updatedChannels := princ.ExplicitChannels()
		if updatedChannels == nil {
			updatedChannels = ch.TimedSet{}
		}
		if !updatedChannels.Equals(newInfo.ExplicitChannels) {
			changed = true
		}

		var updatedRoles ch.TimedSet

		// Then the user-specific fields like roles:
		if isUser {
			if newInfo.Email != user.Email() {
				if err := user.SetEmail(newInfo.Email); err != nil {
					base.WarnfCtx(ctx, "Skipping SetEmail for user %q - Invalid email address provided: %q", base.UD(*newInfo.Name), base.UD(newInfo.Email))
				}
				changed = true
			}
			if newInfo.Password != nil {
				err = user.SetPassword(*newInfo.Password)
				if err != nil {
					return false, err
				}
				changed = true
			}
			if newInfo.Disabled != nil && *newInfo.Disabled != user.Disabled() {
				user.SetDisabled(*newInfo.Disabled)
				changed = true
			}

			updatedRoles = user.ExplicitRoles()
			if updatedRoles == nil {
				updatedRoles = ch.TimedSet{}
			}
			if !updatedRoles.Equals(base.SetFromArray(newInfo.ExplicitRoleNames)) {
				changed = true
			}
		}

		// And finally save the Principal:
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
		if updatedChannels.UpdateAtSequence(newInfo.ExplicitChannels, nextSeq) {
			princ.SetExplicitChannels(updatedChannels, nextSeq)
		}

		if isUser {
			if updatedRoles.UpdateAtSequence(base.SetFromArray(newInfo.ExplicitRoleNames), nextSeq) {
				user.SetExplicitRoles(updatedRoles, nextSeq)
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
