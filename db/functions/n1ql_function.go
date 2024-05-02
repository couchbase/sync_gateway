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
	"fmt"
	"net/http"
	"regexp"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// implements UserFunctionInvocation and resolver
type n1qlInvocation struct {
	*functionImpl
	db       *db.Database
	ctx      context.Context
	args     map[string]any
	n1qlArgs map[string]any
}

type n1qlUserArgument struct {
	Name     *string  `json:"name,omitempty"`
	Email    *string  `json:"email,omitempty"`
	Channels []string `json:"channels,omitempty"`
	Roles    []string `json:"roles,omitempty"`
}

var n1qlQueryRegex = regexp.MustCompile(`^\s*\(*(?i:SELECT)\b`)

func validateN1QLQuery(query string) error {
	if n1qlQueryRegex.MatchString(query) {
		return nil
	} else {
		return fmt.Errorf("only SELECT queries are allowed")
	}
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
		fn.n1qlArgs = map[string]any{}
	}
	fn.n1qlArgs["args"] = fn.args
	fn.n1qlArgs["user"] = &userArg

	// Run the N1QL query:
	// TODO: Multi-collection support for user functions is not implemented.
	iter, err := db.N1QLQueryWithStats(fn.ctx, fn.db.Bucket.DefaultDataStore(), db.QueryTypeUserFunctionPrefix+fn.name, fn.Code, fn.n1qlArgs,
		base.RequestPlus, false, fn.db.DbStats, fn.db.Options.SlowQueryWarningThreshold)

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
	return iter, db.CheckTimeout(fn.ctx)
}

func (fn *n1qlInvocation) Run(ctx context.Context) (any, error) {
	rows, err := fn.Iterate()
	if err != nil {
		return nil, err
	}
	defer func() {
		if rows != nil {
			_ = rows.Close()
		}
	}()
	result := []any{}
	var row any
	for rows.Next(ctx, &row) {
		result = append(result, row)
	}
	err = rows.Close()
	rows = nil // prevent 'defer' from closing again
	return result, err
}
