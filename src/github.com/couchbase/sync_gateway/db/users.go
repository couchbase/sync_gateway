package db

import (
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
	Channels         base.Set `json:"all_channels"`
	// Fields below only apply to Users, not Roles:
	Email             string   `json:"email,omitempty"`
	Disabled          bool     `json:"disabled,omitempty"`
	Password          *string  `json:"password,omitempty"`
	ExplicitRoleNames []string `json:"admin_roles,omitempty"`
	RoleNames         []string `json:"roles,omitempty"`
}

func (dbc *DatabaseContext) GetPrincipal(name string, isUser bool) (info *PrincipalConfig, err error) {
	var princ auth.Principal
	if isUser {
		princ, err = dbc.Authenticator().GetUser(name)
	} else {
		princ, err = dbc.Authenticator().GetRole(name)
	}
	if princ == nil {
		return
	}
	info = new(PrincipalConfig)
	info.Name = &name
	info.ExplicitChannels = princ.ExplicitChannels().AsSet()
	if user, ok := princ.(auth.User); ok {
		info.Channels = user.InheritedChannels().AsSet()
		info.Email = user.Email()
		info.Disabled = user.Disabled()
		info.ExplicitRoleNames = user.ExplicitRoles().AllChannels()
		info.RoleNames = user.RoleNames().AllChannels()
	} else {
		info.Channels = princ.Channels().AsSet()
	}
	return
}

// Updates or creates a principal from a PrincipalConfig structure.
func (dbc *DatabaseContext) UpdatePrincipal(newInfo PrincipalConfig, isUser bool, allowReplace bool) (replaced bool, err error) {
	// Get the existing principal, or if this is a POST make sure there isn't one:
	var princ auth.Principal
	var user auth.User
	authenticator := dbc.Authenticator()
	if isUser {
		if newInfo.Password != nil && len(*(newInfo.Password)) < 3 {
			err = base.HTTPErrorf(http.StatusBadRequest, "Passwords must be at least three 3 characters")
			return
		}
		user, err = authenticator.GetUser(*newInfo.Name)
		princ = user
	} else {
		princ, err = authenticator.GetRole(*newInfo.Name)
	}
	if err != nil {
		return
	}

	changed := false
	replaced = (princ != nil)
	if !replaced {
		// If user/role didn't exist already, instantiate a new one:
		if isUser {
			user, err = authenticator.NewUser(*newInfo.Name, "", nil)
			princ = user
		} else {
			princ, err = authenticator.NewRole(*newInfo.Name, nil)
		}
		if err != nil {
			return
		}
		changed = true
	} else if !allowReplace {
		err = base.HTTPErrorf(http.StatusConflict, "Already exists")
		return
	}

	// Update the persistent sequence number of this principal:
	nextSeq, err := dbc.sequences.nextSequence()
	if err != nil {
		return
	}
	princ.SetSequence(nextSeq)

	// Now update the Principal object from the properties in the request, first the channels:
	updatedChannels := princ.ExplicitChannels()
	if updatedChannels == nil {
		updatedChannels = ch.TimedSet{}
	}
	if updatedChannels.UpdateAtSequence(newInfo.ExplicitChannels, nextSeq) {
		princ.SetExplicitChannels(updatedChannels)
		changed = true
	}

	// Then the user-specific fields like roles:
	if isUser {
		if newInfo.Email != user.Email() {
			user.SetEmail(newInfo.Email)
			changed = true
		}
		if newInfo.Password != nil {
			user.SetPassword(*newInfo.Password)
			changed = true
		}
		if newInfo.Disabled != user.Disabled() {
			user.SetDisabled(newInfo.Disabled)
			changed = true
		}

		updatedRoles := user.ExplicitRoles()
		if updatedRoles == nil {
			updatedRoles = ch.TimedSet{}
		}
		if updatedRoles.UpdateAtSequence(base.SetFromArray(newInfo.ExplicitRoleNames), nextSeq) {
			user.SetExplicitRoles(updatedRoles)
			changed = true
		}
	}

	// And finally save the Principal:
	if changed {
		err = authenticator.Save(princ)
	}
	return
}
