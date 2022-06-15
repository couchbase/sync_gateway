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
	"github.com/couchbase/sync_gateway/base"
)

// Name of built-in user-info query parameter
const userQueryUserParam = "user"

// Value of user-info query parameter
type userQueryUserInfo struct {
	name     string   `json:"name"`
	email    string   `json:"email"`
	channels []string `json:"channels"`
	roles    []string `json:"roles"`
}

// Regexp that matches a property pattern -- either `$xxx` or `$(xxx)` where `xxx` is one or more
// alphanumeric characters or underscore. It also matches `$$` so it can be subtituted with `$`.
var kChannelPropertyRegexp = regexp.MustCompile(`\$(\w+|\([^)]+\)|\$)`)

// Runs a named query on behalf of a user, presumably invoked via a REST or BLIP API.
func (db *Database) UserQuery(name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	// Look up the query name in the server config:
	query, found := db.Options.UserQueries[name]
	if !found {
		return nil, base.HTTPErrorf(http.StatusNotFound, "No such query '%s'", name)
	}

	// Make sure each specified parameter has a value in `params`:
	for _, paramName := range query.Parameters {
		if paramName == userQueryUserParam {
			logCtx := context.TODO()
			base.WarnfCtx(logCtx, "Bad config: Query %q uses reserved parameter name '$user'", name)
			return nil, base.HTTPErrorf(http.StatusInternalServerError, "Server query configuration is invalid")
		}
		if _, found := params[paramName]; !found {
			return nil, base.HTTPErrorf(http.StatusBadRequest, "Parameter '%s' is missing", paramName)
		}
	}

	// Any extra parameters in `params` are illegal:
	if len(params) != len(query.Parameters) {
		for _, paramName := range query.Parameters {
			delete(params, paramName)
		}
		for badKey, _ := range params {
			return nil, base.HTTPErrorf(http.StatusBadRequest, "Unknown parameter '%s'", badKey)
		}
	}

	// Add `user` parameter, for query's use in filtering the output:
	params[userQueryUserParam] = &userQueryUserInfo{
		name:     db.user.Name(),
		email:    db.user.Email(),
		channels: db.user.Channels().AllKeys(),
		roles:    db.user.RoleNames().AllKeys(),
	}

	// Verify the user has access to at least one of the given channels.
	// Channel names in the config are parameter-expanded like the query string.
	authorized := false
	for channelPattern, _ := range query.Channels {
		if channel, err := expandChannelPattern(name, channelPattern, params); err != nil {
			return nil, err
		} else if db.user.CanSeeChannel(channel) {
			authorized = true
			break
		}
	}
	if !authorized {
		return nil, db.user.UnauthError("You do not have access to this query")
	}

	// Run the query:
	return db.N1QLQueryWithStats(db.Ctx, QueryTypeUserPrefix+name, query.Statement, params,
		base.RequestPlus, false)
}

// Expands patterns of the form `$param` or `$(param)` in `channelPattern`, looking up each such
// `param` in the `params` map and substituting its value. `$$` is replaced with `$`.
// It is an error if any `param` has no value, or if its value is not a string or integer.
func expandChannelPattern(queryName string, channelPattern string, params map[string]interface{}) (string, error) {
	if strings.IndexByte(channelPattern, '$') < 0 {
		return channelPattern, nil
	}
	var err error
	channel := kChannelPropertyRegexp.ReplaceAllStringFunc(channelPattern, func(param string) string {
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
			base.WarnfCtx(logCtx, "Bad config: Query %q has invalid channel pattern %q", queryName, channelPattern)
			err = base.HTTPErrorf(http.StatusInternalServerError, "Server query configuration is invalid")
			return ""
		} else if valueStr, ok := value.(string); ok {
			return valueStr
		} else if reflect.ValueOf(value).CanInt() || reflect.ValueOf(value).CanUint() {
			return fmt.Sprintf("%v", value)
		} else if param == userQueryUserParam {
			// Special case: for `$user`, get value of params["user"]["name"]
			return value.(*userQueryUserInfo).name
		} else {
			err = base.HTTPErrorf(http.StatusBadRequest, "Value of parameter '%s' must be a string or int", param)
			return ""
		}
	})
	return channel, err
}
