package db

import (
	"net/http"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	ch "github.com/couchbaselabs/sync_gateway/channels"
)

// Struct that configures settings of a User/Role, for UpdatePrincipal.
// Also used in the rest package as a JSON object that defines a User/Role within a DbConfig
// and structures the request/response body in the admin REST API for /db/_user/*
type PrincipalConfig struct {
	Name              *string  `json:"name,omitempty"`
	ExplicitChannels  base.Set `json:"admin_channels,omitempty"`
	Channels          base.Set `json:"all_channels"`
	Email             string   `json:"email,omitempty"`
	Disabled          bool     `json:"disabled,omitempty"`
	Password          *string  `json:"password,omitempty"`
	ExplicitRoleNames []string `json:"admin_roles,omitempty"`
	RoleNames         []string `json:"roles,omitempty"`
}

// Updates or creates a principal from a PrincipalConfig structure.
func (dbc *DatabaseContext) UpdatePrincipal(newInfo PrincipalConfig, isUser bool, allowReplace bool) (replaced bool, err error) {
	// Get the existing principal, or if this is a POST make sure there isn't one:
	var princ auth.Principal
	var user auth.User
	authenticator := dbc.Authenticator()
	if isUser {
		user, err = authenticator.GetUser(*newInfo.Name)
		princ = user
	} else {
		princ, err = authenticator.GetRole(*newInfo.Name)
	}
	if err != nil {
		return
	}

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
	} else if !allowReplace {
		err = base.HTTPErrorf(http.StatusConflict, "Already exists")
		return
	}

	// Now update the Principal object from the properties in the request, first the channels:
	updatedChannels := princ.ExplicitChannels()
	if updatedChannels == nil {
		updatedChannels = ch.TimedSet{}
	}
	lastSeq, err := dbc.LastSequence()
	if err != nil {
		return
	}
	updatedChannels.UpdateAtSequence(newInfo.ExplicitChannels, lastSeq+1)
	princ.SetExplicitChannels(updatedChannels)

	// Then the user-specific fields like roles:
	if isUser {
		user.SetEmail(newInfo.Email)
		if newInfo.Password != nil {
			user.SetPassword(*newInfo.Password)
		}
		user.SetDisabled(newInfo.Disabled)

		updatedRoles := user.ExplicitRoles()
		if updatedRoles == nil {
			updatedRoles = ch.TimedSet{}
		}
		updatedRoles.UpdateAtSequence(base.SetFromArray(newInfo.ExplicitRoleNames), lastSeq+1)
		user.SetExplicitRoles(updatedRoles)
	}

	// And finally save the Principal:
	err = authenticator.Save(princ)
	return
}
