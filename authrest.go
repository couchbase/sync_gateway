//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package channelsync

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

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

// Starts a simple REST listener that will get and set user credentials.
func StartAuthListener(addr string, auth *Authenticator) {
	handler := func(r http.ResponseWriter, rq *http.Request) {
		username := rq.URL.Path[1:]
		method := rq.Method
		log.Printf("AUTH: %s %q", method, username)
		if rq.URL.Path == "/" {
			switch method {
			case "POST":
				putUser(r, rq, auth, "")
			default:
				r.WriteHeader(http.StatusMethodNotAllowed)
			}
		} else {
			if username == "GUEST" {
				username = ""
			}
			switch method {
			case "GET":
				user, _ := auth.GetUser(username)
				if user == nil {
					r.WriteHeader(http.StatusNotFound)
					return
				}
				bytes, _ := json.Marshal(user)
				r.Write(bytes)
			case "PUT":
				putUser(r, rq, auth, username)
			case "DELETE":
				if auth.DeleteUser(username) != nil {
					r.WriteHeader(http.StatusNotFound)
				}
			}
		}
	}
	go http.ListenAndServe(addr, http.HandlerFunc(handler))
}
