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

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
)

//////// QUERY CONFIGURATION:

// Top level user-query config object: the map of names to queries.
type UserQueryMap = map[string]*UserQuery

// Defines a N1QL query that a client can invoke by name.
// (The name is the key in the UserQueryMap.)
type UserQuery struct {
	Statement  string          `json:"statement"`            // N1QL / SQL++ query string
	Parameters []string        `json:"parameters,omitempty"` // Names of N1QL '$'-parameters
	Allow      *UserQueryAllow `json:"allow,omitempty"`      // Permissions (admin-only if nil)
}

// Permissions for a user query
type UserQueryAllow struct {
	Channels []string `json:"channels,omitempty"` // Names of channel(s) that grant access to query
	Roles    []string `json:"roles,omitempty"`    // Names of role(s) that have access to query
	Users    base.Set `json:"users,omitempty"`    // Names of user(s) that have access to query
}

// Name of built-in user-info query parameter
const userQueryUserParam = "user"

// Value of user-info query parameter
type userQueryUserInfo struct {
	Name     string   `json:"name"`
	Email    string   `json:"email"`
	Channels []string `json:"channels"`
	Roles    []string `json:"roles"`
}

//////// RUNNING A QUERY:

// Runs a named N1QL query on behalf of a user, presumably invoked via a REST or BLIP API.
func (db *Database) UserQuery(name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	// Look up the query name in the server config:
	query, found := db.Options.UserQueries[name]
	if !found {
		return nil, base.HTTPErrorf(http.StatusNotFound, "No such query '%s'", name)
	}

	// Check that the user is authorized:
	if err := query.Allow.authorize(db.user, params, "query", name); err != nil {
		return nil, err
	}

	// Check that the config does not use the reserved parameter name "user":
	if _, found := params[userQueryUserParam]; found {
		logCtx := context.TODO()
		base.WarnfCtx(logCtx, "Bad config: query %q uses reserved parameter name '$%s'", name, userQueryUserParam)
		return nil, base.HTTPErrorf(http.StatusInternalServerError, "Server %s configuration is invalid")
	}

	// Make sure each specified parameter has a value in `params`:
	if err := checkQueryArguments(params, query.Parameters, "query", name); err != nil {
		return nil, err
	}

	// Add `user` parameter, for query's use in filtering the output:
	if user := db.user; user != nil {
		params[userQueryUserParam] = &userQueryUserInfo{
			Name:     user.Name(),
			Email:    user.Email(),
			Channels: user.Channels().AllKeys(),
			Roles:    user.RoleNames().AllKeys(),
		}
	}

	// Run the query:
	return db.N1QLQueryWithStats(db.Ctx, QueryTypeUserPrefix+name, query.Statement, params,
		base.RequestPlus, false)
}

// Checks that `params` contains exactly the same keys as the list `parameterNames`.
// `queryType` and `queryName` are used in generating the error messages.
func checkQueryArguments(params map[string]interface{}, parameterNames []string, queryType string, queryName string) error {
	// Make sure each specified parameter has a value in `params`:
	for _, paramName := range parameterNames {
		if _, found := params[paramName]; !found {
			return base.HTTPErrorf(http.StatusBadRequest, "%s %q parameter %q is missing", queryType, queryName, paramName)
		}
	}
	// Any extra parameters in `params` are illegal:
	if len(params) != len(parameterNames) {
		for _, paramName := range parameterNames {
			delete(params, paramName)
		}
		for badKey, _ := range params {
			return base.HTTPErrorf(http.StatusBadRequest, "%s %q has no parameter %q", queryType, queryName, badKey)
		}
	}
	return nil
}

//////// AUTHORIZATION:

// Authorizes a User against the UserQueryAllow object:
// - The user's name must be contained in Users, OR
// - The user must have a role contained in Roles, OR
// - The user must have access to a channel contained in Channels.
// In Roles and Channels, patterns of the form `$param` or `$(param)` are expanded using `params`.
func (allow *UserQueryAllow) authorize(user auth.User, params map[string]interface{}, queryType string, queryName string) error {
	if user == nil {
		return nil // User is admin
	} else if allow != nil { // No Allow object means admin-only
		if allow.Users.Contains(user.Name()) {
			return nil // User is explicitly allowed
		}
		userRoles := user.RoleNames()
		for _, rolePattern := range allow.Roles {
			if role, err := allow.expandPattern(rolePattern, params); err != nil {
				return err
			} else if userRoles.Contains(role) {
				return nil // User has one of the allowed roles
			}
		}
		// Check if the user has access to one of the given channels.
		for _, channelPattern := range allow.Channels {
			if channel, err := allow.expandPattern(channelPattern, params); err != nil {
				return err
			} else if user.CanSeeChannel(channel) {
				return nil // User has access to one of the allowed channels
			}
		}
	}
	return user.UnauthError(fmt.Sprintf("Unauthorized call to %s %q", queryType, queryName))
}

// Expands patterns of the form `$param` or `$(param)` in `pattern`, looking up each such
// `param` in the `params` map and substituting its value. `$$` is replaced with `$`.
// It is an error if any `param` has no value, or if its value is not a string or integer.
func (allow *UserQueryAllow) expandPattern(pattern string, params map[string]interface{}) (string, error) {
	if strings.IndexByte(pattern, '$') < 0 {
		return pattern, nil
	}
	var err error
	channel := kChannelPropertyRegexp.ReplaceAllStringFunc(pattern, func(param string) string {
		param = param[1:]
		if param == "$" {
			return param
		}
		if param[0] == '(' {
			param = param[1 : len(param)-1]
		}
		// Look up the parameter:
		if value, found := params[param]; !found {
			logCtx := context.TODO()
			base.WarnfCtx(logCtx, "Bad config: Invalid channel/role pattern %q in 'allow'", pattern)
			err = base.HTTPErrorf(http.StatusInternalServerError, "Server query configuration is invalid")
			return ""
		} else if valueStr, ok := value.(string); ok {
			return valueStr
		} else if reflect.ValueOf(value).CanInt() || reflect.ValueOf(value).CanUint() {
			return fmt.Sprintf("%v", value)
		} else if param == userQueryUserParam {
			// Special case: for `$user`, get value of params["user"]["name"]
			return value.(*userQueryUserInfo).Name
		} else {
			err = base.HTTPErrorf(http.StatusBadRequest, "Value of parameter '%s' must be a string or int", param)
			return ""
		}
	})
	return channel, err
}

// Regexp that matches a property pattern -- either `$xxx` or `$(xxx)` where `xxx` is one or more
// alphanumeric characters or underscore. It also matches `$$` so it can be subtituted with `$`.
var kChannelPropertyRegexp = regexp.MustCompile(`\$(\w+|\([^)]+\)|\$)`)
