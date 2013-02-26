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
	"time"

	"github.com/gorilla/mux"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/db"
)

//////// USER REQUESTS:

// Handles PUT or POST to /username
func putUserCommon(r http.ResponseWriter, rq *http.Request, a *auth.Authenticator, username string) error {
	body, _ := ioutil.ReadAll(rq.Body)
	var user auth.User
	err := json.Unmarshal(body, &user)
	if err != nil {
		return err
	}
	if user.AdminChannels == nil {
		return &base.HTTPError{http.StatusBadRequest, "Missing admin_channels property"}
	}
	user.AllChannels = nil // Force it to be recomputed

	if rq.Method == "POST" {
		username = user.Name
		if username == "" {
			return &base.HTTPError{http.StatusBadRequest, "Missing name property"}
		}
	} else if user.Name == "" {
		user.Name = username
	} else if user.Name != username {
		return &base.HTTPError{http.StatusBadRequest, "Name mismatch (can't change name)"}
	}
	return a.SaveUser(&user)
}

func postUser(r http.ResponseWriter, rq *http.Request, auth *auth.Authenticator) error {
	return putUserCommon(r, rq, auth, "")
}

func putUser(r http.ResponseWriter, rq *http.Request, auth *auth.Authenticator) error {
	muxed := mux.Vars(rq)
	username := muxed["name"]
	if username == "GUEST" {
		username = "" //todo handle this at model layer?
	}
	return putUserCommon(r, rq, auth, username)
}

// Generates a login session for a user and returns the session ID and cookie name.
func createUserSession(r http.ResponseWriter, rq *http.Request, authenticator *auth.Authenticator) error {
	body, err := ioutil.ReadAll(rq.Body)
	if err != nil {
		return err
	}
	var params struct {
		Name string        `json:"name"`
		TTL  time.Duration `json:"ttl"`
	}
	err = json.Unmarshal(body, &params)
	if err != nil {
		return err
	}
	if params.Name == "" || params.TTL < 0 {
		return &base.HTTPError{http.StatusBadRequest, "Invalid name or ttl"}
	}
	session, err := authenticator.CreateSession(params.Name, params.TTL)
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

func deleteUser(r http.ResponseWriter, rq *http.Request, auth *auth.Authenticator) error {
	muxed := mux.Vars(rq)
	username := muxed["name"]
	user, err := auth.GetUser(username)
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	return auth.DeleteUser(user)
}

func getUserInfo(r http.ResponseWriter, rq *http.Request, auth *auth.Authenticator) error {
	muxed := mux.Vars(rq)
	username := muxed["name"]
	if username == "GUEST" {
		username = "" //todo handle this at model layer?
	}
	user, err := auth.GetUser(username)
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	bytes, _ := json.Marshal(user)
	r.Write(bytes)
	return nil
}

// DESIGN DOCUMENTS:

func (h *handler) handleGetDesignDoc() error {
	docid := h.PathVars()["docid"]
	value, err := h.db.GetSpecial("design", docid)
	if err != nil {
		return err
	}
	if value == nil {
		return kNotFoundError
	}
	value["_id"] = "_design/" + docid
	value.FixJSONNumbers()
	h.writeJSON(value)
	return nil
}

func (h *handler) handlePutDesignDoc() error {
	docid := h.PathVars()["docid"]
	body, err := h.readJSON()
	if err == nil {
		body.FixJSONNumbers()
		var revid string
		revid, err = h.db.PutSpecial("design", docid, body)
		if err == nil {
			h.writeJSONStatus(http.StatusCreated, db.Body{"ok": true, "id": "_design/" + docid, "rev": revid})
		}
	}
	return err
}

func (h *handler) handleDelDesignDoc() error {
	docid := h.PathVars()["docid"]
	return h.db.DeleteSpecial("design", docid, h.getQuery("rev"))
}

// HTTP HANDLER:

func renderError(err error, r http.ResponseWriter) {
	status, message := base.ErrorAsHTTPStatus(err)
	r.Header().Set("Content-Type", "application/json")
	r.WriteHeader(status)
	jsonOut, _ := json.Marshal(map[string]interface{}{"error": status, "reason": message})
	r.Write(jsonOut)
}

type authHandler func(http.ResponseWriter, *http.Request, *auth.Authenticator) error

func handleAuthReq(auth *auth.Authenticator, fun authHandler) func(http.ResponseWriter, *http.Request) {
	return func(r http.ResponseWriter, rq *http.Request) {
		err := fun(r, rq, auth)
		if err != nil {
			renderError(err, r)
		}
	}
}

// Starts a simple REST listener that will get and set user credentials.
func createAuthHandler(c *context) http.Handler {
	auth := c.auth
	r := mux.NewRouter()
	// r.StrictSlash(true)

	r.HandleFunc("/{db}/_session",
		handleAuthReq(auth, createUserSession)).Methods("POST")
	r.HandleFunc("/{db}/user/{name}",
		handleAuthReq(auth, getUserInfo)).Methods("GET", "HEAD")
	r.HandleFunc("/{db}/user/{name}",
		handleAuthReq(auth, putUser)).Methods("PUT")
	r.HandleFunc("/{db}/user/{name}",
		handleAuthReq(auth, deleteUser)).Methods("DELETE")
	r.HandleFunc("/{db}/user",
		handleAuthReq(auth, postUser)).Methods("POST")

	dbr := r.PathPrefix("/{db}/").Subrouter()
	dbr.Handle("/_design/{docid}",
		makeHandler(c, (*handler).handleGetDesignDoc)).Methods("GET", "HEAD")
	dbr.Handle("/_design/{docid}",
		makeHandler(c, (*handler).handlePutDesignDoc)).Methods("PUT")
	dbr.Handle("/_design/{docid}",
		makeHandler(c, (*handler).handleDelDesignDoc)).Methods("DELETE")

	// http.Handle("/", r);
	return r
}

func StartAuthListener(addr string, c *context) {
	go http.ListenAndServe(addr, createAuthHandler(c))
}
