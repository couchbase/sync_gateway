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
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/dchest/passwordhash"
)

/** Persistent information about a user. */
type User struct {
	Name         string                     `json:"name,omitempty"`
	Email        string                     `json:"email,omitempty"`
	PasswordHash *passwordhash.PasswordHash `json:"passwordhash,omitempty"`
	Channels     []string                   `json:"channels"`

	Password	 *string					`json:"password,omitempty"`
}

/** Manages user authentication for a database. */
type Authenticator struct {
	bucket *couchbase.Bucket
	lock sync.Mutex
	sessions map[string]*LoginSession
}

type userByEmailInfo struct {
	Username string
}


var kValidEmailRegexp *regexp.Regexp

func init() {
	var err error
	kValidEmailRegexp, err = regexp.Compile(`^[-.\w]+@\w[-.\w]+$`)
	if err != nil {panic("Bad IsValidEmail regexp")}
}

func IsValidEmail(email string) bool {
	return kValidEmailRegexp.MatchString(email)
}


// Creates a new Authenticator that stores user info in the given Bucket.
func NewAuthenticator(bucket *couchbase.Bucket) *Authenticator {
	return &Authenticator{
		bucket: bucket,
		sessions: map[string]*LoginSession{},
	}
}

func docIDForUser(username string) string {
	return "user:" + username
}

func docIDForUserEmail(email string) string {
	return "useremail:" + email
}

// Looks up the information for a user.
// If the username is "" it will return the default (guest) User object, not nil.
// By default the guest User has access to everything, i.e. Admin Party! This can
// be changed by altering its list of channels and saving the changes via SetUser.
func (auth *Authenticator) GetUser(username string) (*User, error) {
	var user *User
	err := auth.bucket.Get(docIDForUser(username), &user)
	if err != nil && !IsDocNotFoundError(err) {
		return nil, err
	}
	if user == nil && username == "" {
		user = &User{Name: username, Channels: []string{"*"}}
	}
	return user, nil
}

func (auth *Authenticator) GetUserByEmail(email string) (*User, error) {
	var info userByEmailInfo
	err := auth.bucket.Get(docIDForUserEmail(email), &info)
	if IsDocNotFoundError(err) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return auth.GetUser(info.Username)
}

// Saves the information for a user.
func (auth *Authenticator) SaveUser(user *User) error {
	user.Channels = SimplifyChannels(user.Channels, true)
	if user.Password != nil {
		user.SetPassword(*user.Password)
		user.Password = nil
	}
	if err := user.Validate(); err != nil {
		return err
	}
	if err := auth.bucket.Set(docIDForUser(user.Name), 0, user); err != nil {
		return err
	}
	if user.Email != "" {
		info := userByEmailInfo{user.Name}
		if err := auth.bucket.Set(docIDForUserEmail(user.Email), 0, info); err != nil {
			return err
		}
		//FIX: Fail if email address is already registered to another user
		//FIX: Unregister old email address if any
	}
	return nil
}

// Deletes a user.
func (auth *Authenticator) DeleteUser(user *User) error {
	if user.Email != "" {
		auth.bucket.Delete(docIDForUserEmail(user.Email))
	}
	return auth.bucket.Delete(docIDForUser(user.Name))
}

// Authenticates a user given the username and password.
// If the username and password are both "", it will return a default empty User object, not nil.
func (auth *Authenticator) AuthenticateUser(username string, password string) *User {
	user, _ := auth.GetUser(username)
	if user == nil || !user.Authenticate(password) {
		return nil
	}
	return user
}

func GenerateRandomSecret() string {
	randomBytes := make([]byte, 20)
	n, err := io.ReadFull(rand.Reader, randomBytes)
	if n < len(randomBytes) || err != nil {
		panic("RNG failed, can't create password")
	}
	return fmt.Sprintf("%x", randomBytes)
}

//////// USER OBJECT API:

// Creates a new User object.
func NewUser(username string, password string, channels []string) (*User, error) {
	user := &User{Name: username, Channels: SimplifyChannels(channels, true)}
	user.SetPassword(password)
	if err := user.Validate(); err != nil {
		return nil, err
	}
	return user, nil
}

// Checks whether this User object contains valid data; if not, returns an error.
func (user *User) Validate() error {
	if match, _ := regexp.MatchString(`^\w*$`, user.Name); !match {
		return &HTTPError{http.StatusBadRequest, fmt.Sprintf("Invalid username %q", user.Name)}
	} else if (user.Name == "") != (user.PasswordHash == nil) {
		return &HTTPError{http.StatusBadRequest, "Invalid password"}
	} else if user.Email != "" && !IsValidEmail(user.Email) {
		return &HTTPError{http.StatusBadRequest, "Invalid email address"}
	}
	return nil
}

// Returns true if the given password is correct for this user.
func (user *User) Authenticate(password string) bool {
	if user == nil {
		return false
	} else if user.PasswordHash == nil {
		if password != "" {
			return false
		}
	} else if !user.PasswordHash.EqualToPassword(password) {
		return false
	}
	return true
}

// Changes a user's password to the given string.
func (user *User) SetPassword(password string) {
	if password == "" {
		user.PasswordHash = nil
	} else {
		user.PasswordHash = passwordhash.New(password)
	}
}


//////// USER CHANNEL AUTHORIZATION:


func (user *User) unauthError(message string) error {
	if user.Name == "" {
		return &HTTPError{http.StatusUnauthorized, "login required"}
	}
	return &HTTPError{http.StatusForbidden, message}
}

// Returns true if the User is allowed to access the channel.
// A nil User means access control is disabled, so the function will return true.
func (user *User) CanSeeChannel(channel string) bool {
	return user == nil || channel == "*" || ContainsChannel(user.Channels, channel) ||
		ContainsChannel(user.Channels, "*")
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
	return ContainsChannel(user.Channels, "*")
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
		return user.unauthError(fmt.Sprintf("You are not allowed to see channels %v", forbidden))
	}
	return nil
}

// Returns an HTTP 403 error if the User is not allowed to access any of the given channels.
// A nil User means access control is disabled, so the function will return nil.
func (user *User) AuthorizeAnyChannels(channels []string) error {
	if !user.CanSeeAnyChannels(channels) {
		return user.unauthError("You are not allowed to see this")
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
	return user.unauthError("You are not allowed to see this")
}

//////// COOKIE-BASED AUTH:


// A user login session (used with cookie-based auth.)
type LoginSession struct {
	id string
	username string
	expiration time.Time
}

const kCookieName = "BaseCouchSession"

func (auth *Authenticator) AuthenticateCookie(rq *http.Request) (*User, error) {
	cookie, _ := rq.Cookie(kCookieName)
	if cookie == nil {
		return nil, nil
	}

	auth.lock.Lock()
	defer auth.lock.Unlock()

	session, found := auth.sessions[cookie.Value]
	if found && session.expiration.Before(time.Now()) {
		delete(auth.sessions, cookie.Value)
		found = false
	}
	if !found {
		return nil, &HTTPError{http.StatusUnauthorized, "Invalid session cookie"}
	}
	return auth.GetUser(session.username)
}

func (auth *Authenticator) CreateSession(username string, ttl time.Duration) *LoginSession {
	auth.lock.Lock()
	defer auth.lock.Unlock()

	// Create a random unused session ID:
	var sessionID string
	for {
		sessionID = GenerateRandomSecret()
		if _, found := auth.sessions[sessionID]; !found {
			break
		}
	}

	expiration := time.Now().Add(ttl)
	session := &LoginSession{
		id: sessionID,
		username: username,
		expiration: expiration,
	}
	auth.sessions[sessionID] = session
	return session
}

func (auth *Authenticator) MakeSessionCookie(session *LoginSession) *http.Cookie {
	if session == nil {
		return nil
	}
	return &http.Cookie{
		Name: kCookieName,
		Value: session.id,
		Expires: session.expiration,
	}
}
