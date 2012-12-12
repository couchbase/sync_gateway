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
"fmt"
	"net/http"
	"regexp"

	"github.com/couchbaselabs/go-couchbase"
)

/** Persistent information about a user. */
type User struct {
	Name     string   `json:"name,omitempty"`
	Password string   `json:"password,omitempty"` //FIX: Do NOT store plaintext passwords!
	Channels []string `json:"channels"`
}

/** Manages user authentication for a database. */
type Authenticator struct {
	bucket *couchbase.Bucket
}

// Creates a new Authenticator that stores user info in the given Bucket.
func NewAuthenticator(bucket *couchbase.Bucket) *Authenticator {
	return &Authenticator{
		bucket: bucket,
	}
}

func docIDForUser(username string) string {
	return "user:" + username
}

// Looks up the information for a user.
// If the username is "" it will return the default (guest) User object, not nil.
// By default the guest User has access to everything, i.e. Admin Party! This can
// be changed by altering its list of channels and saving the changes via SetUser.
func (auth *Authenticator) GetUser(username string) (*User, error) {
	var user *User
	err := auth.bucket.Get(docIDForUser(username), &user)
	if user == nil && username == "" {
		return &User{username, "", []string{"*"}}, nil
	}
	return user, err
}

// Saves the information for a user.
func (auth *Authenticator) SaveUser(user *User) error {
	if err := user.Validate(); err != nil {
		return err
	}
	return auth.bucket.Set(docIDForUser(user.Name), 0, user)
}

// Deletes a user.
func (auth *Authenticator) DeleteUser(username string) error {
	return auth.bucket.Delete(docIDForUser(username))
}

// Authenticates a user given the username and password.
// If the username and password are both "", it will return a default empty User object, not nil.
func (auth *Authenticator) AuthenticateUser(username string, password string) *User {
	user, _ := auth.GetUser(username)
	if user == nil || user.Password != password {
		return nil
	}
	return user
}

//////// USER OBJECT API:

func (user *User) Validate() error {
	if match,_ := regexp.MatchString(`\w*`, user.Name); !match {
		return &HTTPError{http.StatusBadRequest, fmt.Sprintf("Invalid username %q", user.Name)}
	} else if (user.Name == "") != (user.Password == "") {
		return &HTTPError{http.StatusBadRequest, "Invalid password"}
	}
	return nil
}

// Returns true if the User is allowed to access the channel.
// A nil User means access control is disabled, so the function will return true.
func (user *User) CanSeeChannel(channel string) bool {
	if user == nil {
		return true
	} else if user.Channels != nil {
		for _, allowedCh := range user.Channels {
			if allowedCh == channel || allowedCh == "*" { // "*" is wildcard channel for user
				return true
			}
		}
	}
	return false
}

// Returns true if the User is allowed to access all of the given channels.
// A nil User means access control is disabled, so the function will return true.
func (user *User) CanSeeAllChannels(channels []string) bool {
	if channels != nil {
		for _, channel := range channels {
			if !user.CanSeeChannel(channel) {
				return false
			}
		}
	}
	return true
}

// Returns true if the User is allowed to access any of the given channels.
// A nil User means access control is disabled, so the function will return true.
func (user *User) CanSeeAnyChannels(channels []string) bool {
	if channels != nil {
		for _, channel := range channels {
			if user.CanSeeChannel(channel) {
				return true
			}
		}
	}
	// If user has wildcard access, allow it anyway
	return user.CanSeeChannel("*")
}

// Returns an HTTP 403 error if the User is not allowed to access all the given channels.
// A nil User means access control is disabled, so the function will return nil.
func (user *User) AuthorizeAllChannels(channels []string) error {
	var forbidden []string
	for _, channel := range channels {
		if !user.CanSeeChannel(channel) {
			if forbidden == nil {
				forbidden = make([]string, 0, len(channels))
			}
			forbidden = append(forbidden, channel)
		}
	}
	if forbidden != nil {
		if user.Name == "" {
			return &HTTPError{http.StatusUnauthorized, "login required"}
		} else {
			msg := fmt.Sprintf("You are not allowed to see channels %v", forbidden)
			return &HTTPError{http.StatusForbidden, msg}
		}
	}
	return nil
}

// Returns an HTTP 403 error if the User is not allowed to access any of the given channels.
// A nil User means access control is disabled, so the function will return nil.
func (user *User) AuthorizeAnyChannels(channels []string) error {
	if !user.CanSeeAnyChannels(channels) {
		if user.Name == "" {
			return &HTTPError{http.StatusUnauthorized, "login required"}
		} else {
			return &HTTPError{http.StatusForbidden, "You are not allowed to see this"}
		}
	}
	return nil
}

// Returns an HTTP 403 error if the User is not allowed to access any of the document's channels.
// A nil User means access control is disabled, so the function will return nil.
func (user *User) AuthorizeAnyDocChannels(channels ChannelMap) error {
	if user == nil {
		return nil
	} else if user.Channels != nil {
		for _, channel := range user.Channels {
			if channel == "*" {
				return nil
			}
			value, exists := channels[channel]
			if exists && value == nil {
				return nil // yup, it's in this channel
			}
		}
	}
	return &HTTPError{http.StatusForbidden, "You are not allowed to see this"}
}

//////// COOKIE-BASED AUTH:

/*
// A user login session (used with cookie-based auth.)
type Session struct {
	id string
	user User
	expiration time.Time
}

const kCookieName = "BaseCouchSession"

func (s *Authenticator) authenticateCookie(cookie *http.Cookie) User {
	if cookie == nil {
		return nil
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	session, found := s.sessions[cookie.Value]
	if !found {
		return nil
	}
	if session.expiration.Before(time.Now()) {
		delete(s.sessions, cookie.Value)
		return nil
	}
	return session.user
}

func (s *Authenticator) createSession(channels []string, ttl time.Duration, r http.ResponseWriter) Session{
	s.lock.Lock()
	defer s.lock.Unlock()

	// Create a random unused session ID:
	var sessionID string
	for {
		randomBytes := make([]byte, 20)
		n, err := io.ReadFull(rand.Reader, randomBytes)
		if n < len(randomBytes) || err != nil {
			panic("RNG failed, can't create session")
		}
		sessionID = fmt.Sprintf("%x", randomBytes)
		if _, found := s.sessions[sessionID]; !found {
			break
		}
	}

	expiration := time.Now().Add(ttl)
	session := &Session{
		id: sessionID
		channels: channels,
		expiration: expiration,
	}
	s.sessions[sessionID] = session
	return session
}

func (s *Authenticator) makeSessionCookie(s *Session) *http.Cookie {
	if session == nil {
		return nil
	}
	return &http.Cookie{
		Name: kCookieName,
		Value: s.id,
		Expires: s.expiration,
	}
}
*/
