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
	db              *db.Database
	ctx             context.Context
	args            map[string]interface{}
	mutationAllowed bool
}

func (fn *jsInvocation) Iterate() (sgbucket.QueryResultIterator, error) {
	return nil, nil
}

func (fn *jsInvocation) Run() (interface{}, error) {
	return fn.call(db.MakeUserCtx(fn.db.User()), fn.args)
}

func (fn *jsInvocation) Resolve(params graphql.ResolveParams) (interface{}, error) {
	return fn.call(db.MakeUserCtx(fn.db.User()), params.Args, params.Source, resolverInfo(params))
}

func (fn *jsInvocation) ResolveType(params graphql.ResolveTypeParams) (interface{}, error) {
	info := map[string]interface{}{}
	return fn.call(db.MakeUserCtx(fn.db.User()), params.Value, info)
}

func (fn *jsInvocation) call(jsArgs ...interface{}) (interface{}, error) {
	return fn.compiled.WithTask(func(task sgbucket.JSServerTask) (result interface{}, err error) {
		runner := task.(*jsRunner)
		return runner.CallWithDB(fn.db,
			fn.mutationAllowed,
			fn.ctx,
			jsArgs...)
	})
}
