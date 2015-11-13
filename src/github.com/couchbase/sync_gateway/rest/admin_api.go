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
	"net/http"

	"github.com/gorilla/mux"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"time"
	"sync/atomic"
)

const kDefaultDBOnlineDelay = 0

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

// Take a DB online, first reload the DB config
func (h *handler) handleDbOnline() error {
	h.assertAdminOnly()
	dbState := atomic.LoadUint32(&h.db.State)
	//If the DB is already trasitioning to: online or is online silently return
	if (dbState == db.DBOnline || dbState == db.DBStarting) {
		return nil
	}

	//If the DB is currently re-syncing return an error asking the user to retry later
	if (dbState == db.DBResyncing) {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Database _resync is in progress, this may take some time, try again later")
	}

	body, err := h.readBody()
	if err != nil {
		return err
	}

	var input struct {
		Delay int           `json:"delay"`
	}

	input.Delay = kDefaultDBOnlineDelay;

	json.Unmarshal(body, &input);

	base.LogTo("CRUD", "Taking Database : %v, online in %v seconds", h.db.Name, input.Delay)

	timer := time.NewTimer(time.Duration(input.Delay) * time.Second)
	go func() {
		<-timer.C

		//We can only transition to Online from Offline state
		if atomic.CompareAndSwapUint32(&h.db.State, db.DBOffline, db.DBStarting) {
			//Take a write lock on the Database context, so that we can cycle the underlying Database
			// without any other call running concurrently
			h.db.AccessLock.Lock()
			defer h.db.AccessLock.Unlock()

			//Remove the database from the server context
			h.server.RemoveDatabase(h.db.Name)

			if _, err := h.server.AddDatabaseFromConfig(h.server.config.Databases[h.db.Name]); err != nil {
				base.LogError(err)
				return
			}

			//Set DB state to DBOnline, this wil cause new API requests to be be rejected
			atomic.StoreUint32(&h.server.databases_[h.db.Name].State, db.DBOnline)
		} else {
			base.LogTo("CRUD", "Unable to take Database : %v, online after %v seconds, database must be in Offline state", h.db.Name, input.Delay)
		}
	}()

	return nil
}

//Take a DB offline
func (h *handler) handleDbOffline() error {
	h.assertAdminOnly()
	return h.db.TakeDbOffline()
}

// Get admin database info
func (h *handler) handleGetDbConfig() error {
	h.writeJSON(h.server.GetDatabaseConfig(h.db.Name))
	return nil
}

// PUT a new database config
func (h *handler) handlePutDbConfig() error {
	h.assertAdminOnly()
	dbName := h.db.Name
	var config *DbConfig
	if err := h.readJSONInto(&config); err != nil {
		return err
	}
	if err := config.setup(dbName); err != nil {
		return err
	}
	h.server.lock.Lock()
	defer h.server.lock.Unlock()
	h.server.config.Databases[dbName] = config

	return base.HTTPErrorf(http.StatusCreated, "created")
}

// "Delete" a database (it doesn't actually do anything to the underlying bucket)
func (h *handler) handleDeleteDB() error {
	h.assertAdminOnly()
	if !h.server.RemoveDatabase(h.db.Name) {
		return base.HTTPErrorf(http.StatusNotFound, "missing")
	}
	h.response.Write([]byte("{}"))
	return nil
}

// raw document access for admin api

func (h *handler) handleGetRawDoc() error {
	h.assertAdminOnly()
	docid := h.PathVar("docid")
	doc, err := h.db.GetDoc(docid)
	if doc != nil {
		h.writeJSON(doc)
	}
	return err
}

func (h *handler) handleGetLogging() error {
	h.writeJSON(base.GetLogKeys())
	return nil
}

func (h *handler) handleSetLogging() error {
	body, err := h.readBody()
	if err != nil {
		return nil
	}
	if h.getQuery("level") != "" {
		base.SetLogLevel(int(getRestrictedIntQuery(h.rq.URL.Query(), "level", uint64(base.LogLevel()), 1, 3, false)))
		if len(body) == 0 {
			return nil // empty body is OK if request is just setting the log level
		}
	}
	var keys map[string]bool
	if err := json.Unmarshal(body, &keys); err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid JSON or non-boolean values")
	}
	base.UpdateLogKeys(keys, h.rq.Method == "PUT")
	return nil
}

//////// USERS & ROLES:

func internalUserName(name string) string {
	if name == base.GuestUsername {
		return ""
	}
	return name
}

func externalUserName(name string) string {
	if name == "" {
		return base.GuestUsername
	}
	return name
}

func marshalPrincipal(princ auth.Principal) ([]byte, error) {
	name := externalUserName(princ.Name())
	info := db.PrincipalConfig{
		Name:             &name,
		ExplicitChannels: princ.ExplicitChannels().AsSet(),
	}
	if user, ok := princ.(auth.User); ok {
		info.Channels = user.InheritedChannels().AsSet()
		info.Email = user.Email()
		info.Disabled = user.Disabled()
		info.ExplicitRoleNames = user.ExplicitRoles().AllChannels()
		info.RoleNames = user.RoleNames().AllChannels()
	} else {
		info.Channels = princ.Channels().AsSet()
	}
	return json.Marshal(info)
}

// Handles PUT and POST for a user or a role.
func (h *handler) updatePrincipal(name string, isUser bool) error {
	h.assertAdminOnly()
	// Unmarshal the request body into a PrincipalConfig struct:
	body, _ := h.readBody()
	var newInfo db.PrincipalConfig
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

	internalName := internalUserName(*newInfo.Name)
	newInfo.Name = &internalName
	replaced, err := h.db.UpdatePrincipal(newInfo, isUser, h.rq.Method != "POST")
	if err != nil {
		return err
	} else if replaced {
		// on update with a new password, remove previous user sessions
		if newInfo.Password != nil {
			err = h.db.DeleteUserSessions(*newInfo.Name)
			if err != nil {
				return err
			}
		}
		h.writeStatus(http.StatusOK, "OK")
	} else {
		h.writeStatus(http.StatusCreated, "Created")
	}
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
