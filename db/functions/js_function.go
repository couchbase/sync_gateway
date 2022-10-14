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
	_ "embed"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/db"
	"github.com/graphql-go/graphql"
	_ "github.com/robertkrimen/otto/underscore"
)

// implements UserFunctionInvocation and resolver
type jsInvocation struct {
	*functionImpl
	db   *db.Database
	ctx  context.Context
	args map[string]any
}

func (fn *jsInvocation) Iterate() (sgbucket.QueryResultIterator, error) {
	return nil, nil
}

func (fn *jsInvocation) Run() (any, error) {
	return fn.call(db.MakeUserCtx(fn.db.User()), fn.args)
}

func (fn *jsInvocation) Resolve(params graphql.ResolveParams) (any, error) {
	return fn.call(
		params.Source,                // parent
		params.Args,                  // args
		db.MakeUserCtx(fn.db.User()), // context
		resolverInfo(params))         // info
}

func (fn *jsInvocation) ResolveType(params graphql.ResolveTypeParams) (any, error) {
	info := map[string]any{}
	return fn.call(db.MakeUserCtx(fn.db.User()), params.Value, info)
}

func (fn *jsInvocation) call(jsArgs ...any) (any, error) {
	return fn.compiled.WithTask(func(task sgbucket.JSServerTask) (result any, err error) {
		runner := task.(*jsRunner)
		return runner.CallWithDB(fn.db,
			fn.ctx,
			jsArgs...)
	})
}
