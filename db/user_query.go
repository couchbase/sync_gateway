/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// Timeout for N1QL, JavaScript and GraphQL queries. (Applies to REST and BLIP requests.)
const UserQueryTimeout = 60 * time.Second

//////// QUERY PARAMETERS/ARGUMENTS:

// Value of query parameter "context.user"
type userQueryUserInfo struct {
	Name     string   `json:"name"`
	Email    string   `json:"email"`
	Channels []string `json:"channels"`
	Roles    []string `json:"roles"`
}

// Checks that `args` contains exactly the same keys as the list `parameterNames`.
// Adds the special "context" parameter containing user data.
// `queryType` and `queryName` are used in generating the error messages.
func (db *Database) checkQueryArguments(args map[string]interface{}, parameterNames []string, queryType string, queryName string) error {
	// Make sure each specified parameter has a value in `args`:
	for _, paramName := range parameterNames {
		if _, found := args[paramName]; !found {
			return base.HTTPErrorf(http.StatusBadRequest, "%s %q parameter %q is missing", queryType, queryName, paramName)
		}
	}

	// Any extra parameters in `args` are illegal:
	if len(args) > len(parameterNames) {
		for _, paramName := range parameterNames {
			delete(args, paramName)
		}
		for badKey, _ := range args {
			return base.HTTPErrorf(http.StatusBadRequest, "%s %q has no parameter %q", queryType, queryName, badKey)
		}
	}
	return nil
}

func (db *Database) createUserArgument() *userQueryUserInfo {
	if user := db.user; user != nil {
		return &userQueryUserInfo{
			Name:     user.Name(),
			Email:    user.Email(),
			Channels: user.Channels().AllKeys(),
			Roles:    user.RoleNames().AllKeys(),
		}
	} else {
		return nil
	}

}

//////// AUTHORIZATION:

// Permissions for a user query
type UserQueryAllow struct {
	Channels []string `json:"channels,omitempty"` // Names of channel(s) that grant access to query
	Roles    []string `json:"roles,omitempty"`    // Names of role(s) that have access to query
	Users    base.Set `json:"users,omitempty"`    // Names of user(s) that have access to query
}

// Authorizes a User against the UserQueryAllow object:
// - The user's name must be contained in Users, OR
// - The user must have a role contained in Roles, OR
// - The user must have access to a channel contained in Channels.
// In Roles and Channels, patterns of the form `$param` or `$(param)` are expanded using `args`.
func (allow *UserQueryAllow) authorize(user auth.User, args map[string]interface{}, what string, name string) error {
	if user == nil {
		return nil // User is admin
	} else if allow != nil { // No Allow object means admin-only
		if allow.Users.Contains(user.Name()) {
			return nil // User is explicitly allowed
		}
		userRoles := user.RoleNames()
		for _, rolePattern := range allow.Roles {
			if role, err := allow.expandPattern(rolePattern, args, user); err != nil {
				return err
			} else if userRoles.Contains(role) {
				return nil // User has one of the allowed roles
			}
		}
		// Check if the user has access to one of the given channels.
		for _, channelPattern := range allow.Channels {
			if channelPattern == channels.AllChannelWildcard {
				return nil
			} else if channel, err := allow.expandPattern(channelPattern, args, user); err != nil {
				return err
			} else if user.CanSeeChannel(channel) {
				return nil // User has access to one of the allowed channels
			}
		}
	}
	return user.UnauthError(fmt.Sprintf("you are not allowed to call %s %q", what, name))
}

// Returns the appropriate HTTP error for when a function/query doesn't exist.
// For security reasons, we don't let a non-admin user know what function names exist;
// so instead of a 404 we return the same 401/403 error as if they didn't have access to it.
func missingError(user auth.User, what string, name string) error {
	if user == nil {
		return base.HTTPErrorf(http.StatusNotFound, "no such %s %q", what, name)
	} else {
		return user.UnauthError(fmt.Sprintf("you are not allowed to call %s %q", what, name))
	}
}

// Expands patterns of the form `$param` or `$(param)` in `pattern`, looking up each such
// `param` in the `args` map and substituting its value.
// (`$$` is replaced with `$`.)
// It is an error if any `param` has no value, or if its value is not a string or integer.
func (allow *UserQueryAllow) expandPattern(pattern string, args map[string]interface{}, user auth.User) (string, error) {
	if strings.IndexByte(pattern, '$') < 0 {
		return pattern, nil
	}
	var err error
	channel := kChannelPropertyRegexp.ReplaceAllStringFunc(pattern, func(arg string) string {
		arg = arg[1:]
		if arg == "$" {
			return arg
		}
		if arg[0] == '(' {
			arg = arg[1 : len(arg)-1]
		}
		// Look up the parameter:
		if strings.HasPrefix(arg, "user.") {
			// Treat "user." the same as "context.user.":
			arg = "context." + arg
		}
		if strings.HasPrefix(arg, "context.user.") {
			// Hacky special case for "context.user.":
			if user == nil {
				return ""
			}
			switch arg {
			case "context.user.name":
				return user.Name()
			case "context.user.email":
				return user.Email()
			}
		}
		if value, found := args[arg]; !found {
			logCtx := context.TODO()
			base.WarnfCtx(logCtx, "Bad config: Invalid channel/role pattern %q in 'allow'", pattern)
			err = base.HTTPErrorf(http.StatusInternalServerError, "Server query configuration is invalid; details in log")
			return ""
		} else if valueStr, ok := value.(string); ok {
			return valueStr
		} else if reflect.ValueOf(value).CanInt() || reflect.ValueOf(value).CanUint() {
			return fmt.Sprintf("%v", value)
		} else {
			err = base.HTTPErrorf(http.StatusBadRequest, "Value of parameter '%s' must be a string or int", arg)
			return ""
		}
	})
	return channel, err
}

// Regexp that matches a property pattern -- either `$xxx` or `$(xxx)` where `xxx` is one or more
// alphanumeric characters or underscore. It also matches `$$` so it can be replaced with `$`.
var kChannelPropertyRegexp = regexp.MustCompile(`\$(\w+|\([^)]+\)|\$)`)
