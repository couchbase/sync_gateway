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
	"net/http"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

const (
	UserQueryUserParam    = "user"     // Reserved parameter name passed to N1QL query
	UserQueryUserName     = "name"     // Sub-key in "user" giving the user's name
	UserQueryUserChannels = "channels" // Sub-key in "user" giving the user's channels
	UserQueryUserRoles    = "roles"    // Sub-key in "user" giving the user's roles
)

// Runs a query on behalf of a user, presumably invoked via a REST or BLIP API.
func (db *Database) UserQuery(name string, params map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	query, found := db.Options.UserQueries[name]
	if !found {
		return nil, base.HTTPErrorf(http.StatusNotFound, "No such query '%s'", name)
	}

	if err := db.user.AuthorizeAnyChannel(query.Channels); err != nil {
		return nil, err
	}

	// Make sure each specified parameter has a value in `params`:
	for _, paramName := range query.Parameters {
		if paramName == UserQueryUserParam {
			logCtx := context.TODO()
			base.WarnfCtx(logCtx, "Bad config: Query %q uses reserved parameter name 'user'", name)
			return nil, base.HTTPErrorf(http.StatusInternalServerError, "Bad server config: query parameter 'user' is reserved")
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
	params[UserQueryUserParam] = map[string]interface{}{
		UserQueryUserName:     db.user.Name(),
		UserQueryUserChannels: db.user.Channels().AllKeys(),
		UserQueryUserRoles:    db.user.RoleNames().AllKeys(),
	}

	// Run the query:
	return db.N1QLQueryWithStats(db.Ctx, QueryTypeUserPrefix+name, query.Statement, params,
		base.RequestPlus, false)
}
