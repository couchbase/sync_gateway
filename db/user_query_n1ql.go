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
	"errors"
	"net/http"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

//////// QUERY CONFIGURATION:

// Top level user-query config object: the map of names to queries.
type UserQueryMap = map[string]*UserQueryConfig

// Defines a N1QL query that a client can invoke by name.
// (The name is the key in the UserQueryMap.)
type UserQueryConfig struct {
	Statement  string          `json:"statement"`            // N1QL / SQL++ query string
	Parameters []string        `json:"parameters,omitempty"` // Names of N1QL '$'-parameters
	Allow      *UserQueryAllow `json:"allow,omitempty"`      // Permissions (admin-only if nil)
}

//////// RUNNING A QUERY:

// Runs a named N1QL query on behalf of a user, presumably invoked via a REST or BLIP API.
func (db *Database) UserN1QLQuery(ctx context.Context, name string, args map[string]interface{}) (sgbucket.QueryResultIterator, error) {
	if err := db.CheckTimeout(ctx); err != nil {
		return nil, err
	}
	// Look up the query name in the server config:
	query, found := db.Options.UserQueries[name]
	if !found {
		return nil, missingError(db.user, "query", name)
	}

	// Validate the arguments:
	if args == nil {
		args = map[string]interface{}{}
	}
	if err := db.checkQueryArguments(args, query.Parameters, "query", name); err != nil {
		return nil, err
	}
	if userArg := db.createUserArgument(); userArg != nil {
		args["user"] = userArg
	} else {
		args["user"] = map[string]interface{}{}
	}

	// Check that the user is authorized:
	if err := query.Allow.authorize(db.user, args, "query", name); err != nil {
		return nil, err
	}

	// Run the query:
	iter, err := db.N1QLQueryWithStats(ctx, QueryTypeUserPrefix+name, query.Statement, args,
		base.RequestPlus, false)
	if err != nil {
		// Return a friendlier error:
		var qe *gocb.QueryError
		if errors.As(err, &qe) {
			base.WarnfCtx(ctx, "Error running query %q: %v", name, err)
			return nil, base.HTTPErrorf(http.StatusInternalServerError, "Query %q: %s", name, qe.Errors[0].Message)
		} else {
			base.WarnfCtx(ctx, "Unknown error running query %q: %T %#v", name, err, err)
			return nil, base.HTTPErrorf(http.StatusInternalServerError, "Unknown error running query %q (see logs)", name)
		}
	}
	// Do a final timeout check, so the caller will know not to do any more work if time's up:
	return iter, db.CheckTimeout(ctx)
}
