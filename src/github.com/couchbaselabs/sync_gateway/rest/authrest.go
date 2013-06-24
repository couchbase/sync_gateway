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
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime/pprof"
	"time"

	"github.com/gorilla/mux"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
)

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

//////// USER & ROLE REQUESTS:

// Public serialization of User/Role as used in the admin REST API.
type PrincipalJSON struct {
	Name              *string  `json:"name,omitempty"`
	ExplicitChannels  base.Set `json:"admin_channels,omitempty"`
	Channels          base.Set `json:"all_channels"`
	Email             string   `json:"email,omitempty"`
	Disabled          bool     `json:"disabled,omitempty"`
	Password          *string  `json:"password,omitempty"`
	ExplicitRoleNames []string `json:"admin_roles,omitempty"`
	RoleNames         []string `json:"roles,omitempty"`
}

func marshalPrincipal(princ auth.Principal) ([]byte, error) {
	name := externalUserName(princ.Name())
	info := PrincipalJSON{
		Name:             &name,
		ExplicitChannels: princ.ExplicitChannels().AsSet(),
		Channels:         princ.Channels().AsSet(),
	}
	if user, ok := princ.(auth.User); ok {
		info.Email = user.Email()
		info.Disabled = user.Disabled()
		info.ExplicitRoleNames = user.ExplicitRoleNames()
		info.RoleNames = user.RoleNames()
	}
	return json.Marshal(info)
}

// Handles PUT and POST for a user or a role.
func updatePrincipal(r http.ResponseWriter, rq *http.Request, context *context, name string, isUser bool) error {
	// Unmarshal the request body into a PrincipalJSON struct:
	body, _ := ioutil.ReadAll(rq.Body)
	var newInfo PrincipalJSON
	var err error
	if err = json.Unmarshal(body, &newInfo); err != nil {
		return err
	}

	var princ auth.Principal
	var user auth.User
	if rq.Method == "POST" {
		// On POST, take the name from the "name" property in the request body:
		if newInfo.Name == nil {
			return &base.HTTPError{http.StatusBadRequest, "Missing name property"}
		}
		name = *newInfo.Name
	} else {
		// ON PUT, get the existing user/role (if any):
		if newInfo.Name != nil && *newInfo.Name != name {
			return &base.HTTPError{http.StatusBadRequest, "Name mismatch (can't change name)"}
		}
		if isUser {
			user, err = context.auth.GetUser(internalUserName(name))
			princ = user
		} else {
			princ, err = context.auth.GetRole(name)
		}
		if err != nil {
			return err
		}
	}

	if princ == nil {
		// If user/role didn't exist already, instantiate a new one:
		if isUser {
			user, err = context.auth.NewUser(internalUserName(name), "", nil)
			princ = user
		} else {
			princ, err = context.auth.NewRole(name, nil)
		}
		if err != nil {
			return err
		}
	}

	// Now update the Principal object from the properties in the request:
	princ.ExplicitChannels().UpdateAtSequence(newInfo.ExplicitChannels,
		context.dbcontext.LastSequence()+1)
	if isUser {
		user.SetEmail(newInfo.Email)
		if newInfo.Password != nil {
			user.SetPassword(*newInfo.Password)
		}
		user.SetDisabled(newInfo.Disabled)
		user.SetExplicitRoleNames(newInfo.ExplicitRoleNames)
	}

	// And finally save the Principal:
	if err = context.auth.Save(princ); err != nil {
		return err
	}
	r.WriteHeader(http.StatusCreated)
	return nil
}

// Handles PUT or POST to /user/*
func putUser(r http.ResponseWriter, rq *http.Request, context *context) error {
	username := mux.Vars(rq)["name"]
	return updatePrincipal(r, rq, context, username, true)
}

// Handles PUT or POST to /role/*
func putRole(r http.ResponseWriter, rq *http.Request, context *context) error {
	rolename := mux.Vars(rq)["name"]
	return updatePrincipal(r, rq, context, rolename, false)
}

func deleteUser(r http.ResponseWriter, rq *http.Request, context *context) error {
	user, err := context.auth.GetUser(mux.Vars(rq)["name"])
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	return context.auth.Delete(user)
}

func deleteRole(r http.ResponseWriter, rq *http.Request, context *context) error {
	role, err := context.auth.GetRole(mux.Vars(rq)["name"])
	if role == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	return context.auth.Delete(role)
}

func getUserInfo(r http.ResponseWriter, rq *http.Request, context *context) error {
	user, err := context.auth.GetUser(internalUserName(mux.Vars(rq)["name"]))
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}

	bytes, err := marshalPrincipal(user)
	r.Write(bytes)
	return err
}

func getRoleInfo(r http.ResponseWriter, rq *http.Request, context *context) error {
	role, err := context.auth.GetRole(mux.Vars(rq)["name"])
	if role == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	bytes, err := marshalPrincipal(role)
	r.Write(bytes)
	return err
}

func getUsers(r http.ResponseWriter, rq *http.Request, context *context) error {
	users, _, err := context.dbcontext.AllPrincipalIDs()
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(users)
	r.Write(bytes)
	return err
}

func getRoles(r http.ResponseWriter, rq *http.Request, context *context) error {
	_, roles, err := context.dbcontext.AllPrincipalIDs()
	if err != nil {
		return err
	}
	bytes, err := json.Marshal(roles)
	r.Write(bytes)
	return err
}

//////// SESSION:

// Generates a login session for a user and returns the session ID and cookie name.
func createUserSession(r http.ResponseWriter, rq *http.Request, context *context) error {
	body, err := ioutil.ReadAll(rq.Body)
	if err != nil {
		return err
	}
	var params struct {
		Name string `json:"name"`
		TTL  int    `json:"ttl"`
	}
	err = json.Unmarshal(body, &params)
	if err != nil {
		return err
	}
	ttl := time.Duration(params.TTL) * time.Second
	if params.Name == "" || ttl < 1.0 {
		return &base.HTTPError{http.StatusBadRequest, "Invalid name or ttl"}
	}
	session, err := context.auth.CreateSession(params.Name, ttl)
	if err != nil {
		return err
	}
	var response struct {
		SessionID  string    `json:"session_id"`
		Expires    time.Time `json:"expires"`
		CookieName string    `json:"cookie_name"`
	}
	response.SessionID = session.ID
	response.Expires = session.Expiration
	response.CookieName = auth.CookieName
	bytes, _ := json.Marshal(response)
	r.Header().Set("Content-Type", "application/json")
	r.Write(bytes)
	return nil
}

func handleProfiling(r http.ResponseWriter, rq *http.Request) {
	err := func() error {
		var params struct {
			File string `json:"file"`
		}
		body, err := ioutil.ReadAll(rq.Body)
		if err != nil {
			return err
		}
		if len(body) > 0 {
			if err = json.Unmarshal(body, &params); err != nil {
				return err
			}
		}

		if params.File != "" {
			base.Log("Profiling to %s ...", params.File)
			f, err := os.Create(params.File)
			if err != nil {
				return err
			}
			pprof.StartCPUProfile(f)
		} else {
			base.Log("...ending profile.")
			pprof.StopCPUProfile()
		}
		return nil
	}()
	if err != nil {
		status, _ := base.ErrorAsHTTPStatus(err)
		r.WriteHeader(status)
	}
}

//////// HTTP HANDLER:

func renderError(err error, r http.ResponseWriter) {
	status, message := base.ErrorAsHTTPStatus(err)
	r.Header().Set("Content-Type", "application/json")
	r.WriteHeader(status)
	jsonOut, _ := json.Marshal(map[string]interface{}{"error": status, "reason": message})
	r.Write(jsonOut)
}

type authHandler func(http.ResponseWriter, *http.Request, *context) error

func handleAuthReq(sc *serverContext, fun authHandler) func(http.ResponseWriter, *http.Request) {
	return func(r http.ResponseWriter, rq *http.Request) {
		base.LogTo("HTTP", "(admin) %s %s", rq.Method, rq.URL)
		dbContext := sc.databases[mux.Vars(rq)["db"]]
		if dbContext == nil {
			r.WriteHeader(http.StatusNotFound)
			r.Write([]byte(fmt.Sprintf(`{"error":"No such database"}`)))
			return
		}
		err := fun(r, rq, dbContext)
		if err != nil {
			renderError(err, r)
		}
	}
}

// Starts a simple REST listener that will get and set user credentials.
func createAuthHandler(sc *serverContext) http.Handler {
	r := mux.NewRouter()
	r.StrictSlash(true)

	r.HandleFunc("/{db}/_session",
		handleAuthReq(sc, createUserSession)).Methods("POST")

	r.HandleFunc("/{db}/user",
		handleAuthReq(sc, getUsers)).Methods("GET", "HEAD")
	r.HandleFunc("/{db}/user/{name}",
		handleAuthReq(sc, getUserInfo)).Methods("GET", "HEAD")
	r.HandleFunc("/{db}/user/{name}",
		handleAuthReq(sc, putUser)).Methods("PUT")
	r.HandleFunc("/{db}/user/{name}",
		handleAuthReq(sc, deleteUser)).Methods("DELETE")
	r.HandleFunc("/{db}/user/",
		handleAuthReq(sc, putUser)).Methods("POST")

	r.HandleFunc("/{db}/role",
		handleAuthReq(sc, getRoles)).Methods("GET", "HEAD")
	r.HandleFunc("/{db}/role/{name}",
		handleAuthReq(sc, getRoleInfo)).Methods("GET", "HEAD")
	r.HandleFunc("/{db}/role/{name}",
		handleAuthReq(sc, putRole)).Methods("PUT")
	r.HandleFunc("/{db}/role/{name}",
		handleAuthReq(sc, deleteRole)).Methods("DELETE")
	r.HandleFunc("/{db}/role/",
		handleAuthReq(sc, putRole)).Methods("POST")

	// The routes below are part of the CouchDB REST API but should only be available to admins,
	// so the handlers are moved to the admin port.
	r.Handle("/{db}/", makeAdminHandler(sc, (*handler).handleDeleteDB)).Methods("DELETE")
	dbr := r.PathPrefix("/{db}/").Subrouter()
	dbr.Handle("/_compact",
		makeAdminHandler(sc, (*handler).handleCompact)).Methods("POST")
	dbr.Handle("/_vacuum",
		makeAdminHandler(sc, (*handler).handleVacuum)).Methods("POST")
	dbr.Handle("/_dump/{view}",
		makeAdminHandler(sc, (*handler).handleDump)).Methods("GET")

	// These routes are available on both the regular and admin ports; the admin port is useful
	// because it gives 'superuser' access.
	dbr.Handle("/_all_docs",
		makeAdminHandler(sc, (*handler).handleAllDocs)).Methods("GET", "HEAD", "POST")
	dbr.Handle("/_changes",
		makeAdminHandler(sc, (*handler).handleChanges)).Methods("GET", "HEAD")

	r.HandleFunc("/_profile", handleProfiling).Methods("POST")

	return r
}

func StartAuthListener(addr string, sc *serverContext) {
	go http.ListenAndServe(addr, createAuthHandler(sc))
}
