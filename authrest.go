//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package basecouch

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

// Handles PUT or POST to /username
func putUser(r http.ResponseWriter, rq *http.Request, auth *Authenticator, username string) int {
	body, _ := ioutil.ReadAll(rq.Body)
	var user User
	err := json.Unmarshal(body, &user)
	if err != nil || user.Channels == nil {
		return http.StatusBadRequest
	}

	if rq.Method == "POST" {
		username = user.Name
		if username == "" {
			return http.StatusBadRequest
		}
	} else if user.Name == "" {
		user.Name = username
	} else if user.Name != username {
		return http.StatusBadRequest
	}

	err = auth.SaveUser(&user)
	if err != nil {
		return http.StatusBadRequest
	}
	return http.StatusOK
}

// Generates a login session for a user and returns the session ID and cookie name.
func createUserSession(r http.ResponseWriter, rq *http.Request, auth *Authenticator) int {
	body, err := ioutil.ReadAll(rq.Body)
	if err != nil {
		return http.StatusBadRequest
	}
	var params struct {
		Name string			`json:"name"`
		TTL time.Duration	`json:"ttl"`
	}
	err = json.Unmarshal(body, &params)
	if err != nil || params.Name == "" || params.TTL < 0 {
		return http.StatusBadRequest
	}
	session := auth.CreateSession(params.Name, params.TTL)
	var response struct {
		SessionID string	`json:"session_id"`
		Expires time.Time	`json:"expires"`
		CookieName string	`json:"cookie_name"`
	}
	response.SessionID = session.id
	response.Expires = session.expiration
	response.CookieName = kCookieName
	bytes, _ := json.Marshal(response)
	r.Header().Set("Content-Type", "application/json")
	r.Write(bytes)
	return 0 // already wrote status
}

// Starts a simple REST listener that will get and set user credentials.
func StartAuthListener(addr string, auth *Authenticator) {
	handler := func(r http.ResponseWriter, rq *http.Request) {
		username := rq.URL.Path[1:]
		method := rq.Method
		log.Printf("AUTH: %s %q", method, username)
		status := http.StatusOK
		if rq.URL.Path == "/" {
			// Root URL: Supports POSTing user info
			switch method {
			case "POST":
				status = putUser(r, rq, auth, "")
			default:
				status = http.StatusMethodNotAllowed
			}
		} else if username == "_session" {
			// /_session: Generate login session for user
			switch method {
			case "POST":
				status = createUserSession(r, rq, auth)
			default:
				status = http.StatusMethodNotAllowed
			}
		} else {
			// Otherwise: Interpret path as username.
			if username == "GUEST" {
				username = ""
			}
			switch method {
			case "GET":
				user, _ := auth.GetUser(username)
				if user == nil {
					status = http.StatusNotFound
					break
				}
				bytes, _ := json.Marshal(user)
				r.Write(bytes)
			case "PUT":
				status = putUser(r, rq, auth, username)
			case "DELETE":
				user, _ := auth.GetUser(username)
				if user == nil || auth.DeleteUser(user) != nil {
					status = http.StatusNotFound
				}
			default:
				status = http.StatusMethodNotAllowed
			}
		}
		if status > 0 {
			r.WriteHeader(status)
		}
	}
	go http.ListenAndServe(addr, http.HandlerFunc(handler))
}
