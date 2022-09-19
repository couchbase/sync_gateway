/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package functions

import (
	"context"
	"errors"
	"net/http"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/graphql-go/graphql"
)

// implements UserFunctionInvocation and resolver
type n1qlInvocation struct {
	*functionImpl
	db       *db.Database
	ctx      context.Context
	args     map[string]interface{}
	n1qlArgs map[string]interface{}
}

type n1qlUserArgument struct {
	Name     *string  `json:"name,omitempty"`
	Email    *string  `json:"email,omitempty"`
	Channels []string `json:"channels,omitempty"`
	Roles    []string `json:"roles,omitempty"`
}

func (fn *n1qlInvocation) Iterate() (sgbucket.QueryResultIterator, error) {
	var userArg n1qlUserArgument
	if user := fn.db.User(); user != nil {
		userArg.Name = base.StringPtr(user.Name())
		userArg.Email = base.StringPtr(user.Email())
		userArg.Channels = user.Channels().AllKeys()
		userArg.Roles = user.RoleNames().AllKeys()
	}
	if fn.n1qlArgs == nil {
		fn.n1qlArgs = map[string]interface{}{}
	}
	fn.n1qlArgs["args"] = fn.args
	fn.n1qlArgs["user"] = &userArg

	// Run the N1QL query:
	iter, err := fn.db.N1QLQueryWithStats(fn.ctx, db.QueryTypeUserFunctionPrefix+fn.name, fn.Code, fn.n1qlArgs,
		base.RequestPlus, false)

	if err != nil {
		// Return a friendlier error:
		var qe *gocb.QueryError
		if errors.As(err, &qe) {
			base.WarnfCtx(fn.ctx, "Error running query %q: %v", fn.name, err)
			return nil, base.HTTPErrorf(http.StatusInternalServerError, "Query %q: %s", fn.name, qe.Errors[0].Message)
		} else {
			base.WarnfCtx(fn.ctx, "Unknown error running query %q: %T %#v", fn.name, err, err)
			return nil, base.HTTPErrorf(http.StatusInternalServerError, "Unknown error running query %q (see logs)", fn.name)
		}
	}
	// Do a final timeout check, so the caller will know not to do any more work if time's up:
	return iter, fn.db.CheckTimeout(fn.ctx)
}

func (fn *n1qlInvocation) Run() (interface{}, error) {
	rows, err := fn.Iterate()
	if err != nil {
		return nil, err
	}
	defer func() {
		if rows != nil {
			_ = rows.Close()
		}
	}()
	result := []interface{}{}
	var row interface{}
	for rows.Next(&row) {
		result = append(result, row)
	}
	err = rows.Close()
	rows = nil // prevent 'defer' from closing again
	return result, err
}

func (fn *n1qlInvocation) Resolve(params graphql.ResolveParams) (interface{}, error) {
	fn.n1qlArgs = map[string]interface{}{
		"parent": params.Source,
		"info":   resolverInfo(params),
	}
	// Run the query:
	result, err := fn.Run()
	if err != nil {
		return nil, err
	}
	if !isGraphQLListType(params.Info.ReturnType) {
		// GraphQL result type is not a list (array), but N1QL always returns an array.
		// So use the first row of the result as the value, if there is one.
		if rows, ok := result.([]interface{}); ok {
			if len(rows) > 0 {
				result = rows[0]
			} else {
				return nil, nil
			}
		}
		if isGraphQLScalarType(params.Info.ReturnType) {
			// GraphQL result type is a scalar, but a N1QL row is always an object.
			// Use the single field of the object, if any, as the result:
			row := result.(map[string]interface{})
			if len(row) != 1 {
				return nil, base.HTTPErrorf(http.StatusInternalServerError, "resolver %q returns scalar type %s, but its N1QL query returns %d columns, not 1", fn.name, params.Info.ReturnType, len(row))
			}
			for _, value := range row {
				result = value
			}
		}
	}
	return result, nil
}
