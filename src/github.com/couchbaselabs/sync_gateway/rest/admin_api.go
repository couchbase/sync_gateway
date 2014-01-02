//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	ch "github.com/couchbaselabs/sync_gateway/channels"
	"github.com/couchbaselabs/sync_gateway/db"
)

//////// DATABASE MAINTENANCE:

// "Create" a database (actually just register an existing bucket)
func (h *handler) handleCreateDB() error {
	h.assertAdminOnly()
	dbName := h.PathVar("newdb")
	var config *DbConfig
	if err := h.readJSONInto(&config); err != nil {
		return err
	}
	if err := config.setup(dbName); err != nil {
		return err
	}
	if _, err := h.server.AddDatabaseFromConfig(config); err != nil {
		return err
	}
	return base.HTTPErrorf(http.StatusCreated, "created")
}

// Get admin database info
func (h *handler) handleAdminInfo() error {
	if h.rq.Method == "HEAD" {
		return nil
	}
	lastSeq, err := h.db.LastSequence()
	if err != nil {
		return err
	}
	response := db.Body{
		"db_name":              h.db.Name,
		"doc_count":            h.db.DocCount(),
		"update_seq":           lastSeq,
		"config" : 				h.db.DatabaseContext.Config,
	}
	h.writeJSON(response)
	return nil
}

// "Delete" a database (it doesn't actually do anything to the underlying bucket)
func (h *handler) handleDeleteDB() error {
	h.assertAdminOnly()
	if !h.server.RemoveDatabase(h.db.Name) {
		return base.HTTPErrorf(http.StatusNotFound, "missing")
	}
	return nil
}

// raw document access for admin api

func (h *handler) handleGetRawDoc() error {
	h.assertAdminOnly()
	docid := h.PathVar("docid")
	doc, err := h.db.GetDoc(docid)
	h.writeJSON(doc)
	return err
}




//////// USERS & ROLES:

func internalUserName(name string) string {
	if name == "GUEST" {
		return ""
	}
	return name
}

func externalUserName(name string) string {
	if name == "" {
		return "GUEST"
	}
	return name
}

func marshalPrincipal(princ auth.Principal) ([]byte, error) {
	name := externalUserName(princ.Name())
	info := PrincipalConfig{
		Name:             &name,
		ExplicitChannels: princ.ExplicitChannels().AsSet(),
	}
	if user, ok := princ.(auth.User); ok {
		info.Channels = user.InheritedChannels().AsSet()
		info.Email = user.Email()
		info.Disabled = user.Disabled()
		info.ExplicitRoleNames = user.ExplicitRoleNames()
		info.RoleNames = user.RoleNames()
	} else {
		info.Channels = princ.Channels().AsSet()
	}
	return json.Marshal(info)
}

// Updates or creates a principal from a PrincipalConfig structure.
func updatePrincipal(dbc *db.DatabaseContext, newInfo PrincipalConfig, isUser bool, allowReplace bool) (replaced bool, err error) {
	// Get the existing principal, or if this is a POST make sure there isn't one:
	var princ auth.Principal
	var user auth.User
	authenticator := dbc.Authenticator()
	if isUser {
		user, err = authenticator.GetUser(internalUserName(*newInfo.Name))
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
			user, err = authenticator.NewUser(internalUserName(*newInfo.Name), "", nil)
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

	// Then the roles:
	if isUser {
		user.SetEmail(newInfo.Email)
		if newInfo.Password != nil {
			user.SetPassword(*newInfo.Password)
		}
		user.SetDisabled(newInfo.Disabled)
		user.SetExplicitRoleNames(newInfo.ExplicitRoleNames)
	}

	// And finally save the Principal:
	err = authenticator.Save(princ)
	return
}

// Handles PUT and POST for a user or a role.
func (h *handler) updatePrincipal(name string, isUser bool) error {
	h.assertAdminOnly()
	// Unmarshal the request body into a PrincipalConfig struct:
	body, _ := ioutil.ReadAll(h.rq.Body)
	var newInfo PrincipalConfig
	var err error
	if err = json.Unmarshal(body, &newInfo); err != nil {
		return err
	}

	if h.rq.Method == "POST" {
		// On POST, take the name from the "name" property in the request body:
		if newInfo.Name == nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Missing name property")
		}
	} else {
		// ON PUT, verify the name matches, if given:
		if newInfo.Name == nil {
			newInfo.Name = &name
		} else if *newInfo.Name != name {
			return base.HTTPErrorf(http.StatusBadRequest, "Name mismatch (can't change name)")
		}
	}

	replaced, err := updatePrincipal(h.db.DatabaseContext, newInfo, isUser, h.rq.Method != "POST")
	if err != nil {
		return err
	}
	status := http.StatusOK
	if !replaced {
		status = http.StatusCreated
	}
	h.response.WriteHeader(status)
	return nil
}

// Handles PUT or POST to /_user/*
func (h *handler) putUser() error {
	username := mux.Vars(h.rq)["name"]
	return h.updatePrincipal(username, true)
}

// Handles PUT or POST to /_role/*
func (h *handler) putRole() error {
	rolename := mux.Vars(h.rq)["name"]
	return h.updatePrincipal(rolename, false)
}

func (h *handler) deleteUser() error {
	h.assertAdminOnly()
	user, err := h.db.Authenticator().GetUser(mux.Vars(h.rq)["name"])
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	return h.db.Authenticator().Delete(user)
}

func (h *handler) deleteRole() error {
	h.assertAdminOnly()
	role, err := h.db.Authenticator().GetRole(mux.Vars(h.rq)["name"])
	if role == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	return h.db.Authenticator().Delete(role)
}

func (h *handler) getUserInfo() error {
	h.assertAdminOnly()
	user, err := h.db.Authenticator().GetUser(internalUserName(mux.Vars(h.rq)["name"]))
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}

	bytes, err := marshalPrincipal(user)
	h.response.Write(bytes)
	return err
}

func (h *handler) getRoleInfo() error {
	h.assertAdminOnly()
	role, err := h.db.Authenticator().GetRole(mux.Vars(h.rq)["name"])
	if role == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	bytes, err := marshalPrincipal(role)
	h.response.Write(bytes)
	return err
}

func (h *handler) getUsers() error {
	users, _, err := h.db.AllPrincipalIDs()
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(users)
	h.response.Write(bytes)
	return err
}

func (h *handler) getRoles() error {
	_, roles, err := h.db.AllPrincipalIDs()
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(roles)
	h.response.Write(bytes)
	return err
}
