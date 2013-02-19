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
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
)

// Handles PUT or POST to /username
func putUser(r http.ResponseWriter, rq *http.Request, a *auth.Authenticator, username string) error {
	body, _ := ioutil.ReadAll(rq.Body)
	var user auth.User
	err := json.Unmarshal(body, &user)
	if err != nil {
		return err
	}
	if user.AdminChannels == nil {
		return &base.HTTPError{http.StatusBadRequest, "Missing admin_channels property"}
	}
	user.AllChannels = nil		// Force it to be recomputed

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

// Starts a simple REST listener that will get and set user credentials.
func StartAuthListener(addr string, auth *auth.Authenticator) {
	handler := func(r http.ResponseWriter, rq *http.Request) {
		splitPath := strings.Split(rq.URL.Path, "/")
		method := rq.Method

		var err error
		if splitPath[1] == "_session" {
			// /_session: Generate login session for user
			switch method {
			case "POST":
				err = createUserSession(r, rq, auth)
			default:
				err = kBadMethodError
			}
		} else if splitPath[1] == "user" {
			if len(splitPath) == 2 || splitPath[2] == "" {
				// /user URL: Supports POSTing user info
				switch method {
				case "POST":
					err = putUser(r, rq, auth, "")
				default:
					err = kBadMethodError
				}
			} else {
				// user request
				username := splitPath[2]
				log.Printf("AUTH: %s %q", method, username)
				if username == "GUEST" {
					username = ""
				}
				switch method {
				case "GET":
					user, _ := auth.GetUser(username)
					if user == nil {
						err = kNotFoundError
						break
					}
					bytes, _ := json.Marshal(user)
					r.Write(bytes)
				case "PUT":
					err = putUser(r, rq, auth, username)
				case "DELETE":
					user, _ := auth.GetUser(username)
					if user == nil || auth.DeleteUser(user) != nil {
						err = kNotFoundError
					}
				default:
					err = kBadMethodError
				}
			}
		}
		if err != nil {
			status, message := base.ErrorAsHTTPStatus(err)
			r.WriteHeader(status)
			r.Header().Set("Content-Type", "application/json")
			r.WriteHeader(status)
			jsonOut, _ := json.Marshal(map[string]interface{}{"error": status, "reason": message})
			r.Write(jsonOut)
		}
	}
	go http.ListenAndServe(addr, http.HandlerFunc(handler))
}
