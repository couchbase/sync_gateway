//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"fmt"
	"net/http"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/graphql-go/graphql"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

// Note: This source file was originally adapted from channels/channelmapper.go and channels/sync_runner.go

// Subset of graphql.ResolveInfo, which is marshalable to JSON (and thereby to Otto)
type jsResolveInfo struct {
	FieldName      string                 `json:"fieldName"`
	RootValue      interface{}            `json:"rootValue"`
	VariableValues map[string]interface{} `json:"variableValues"`
}

type jsResolveContext struct {
	User *string `json:"user,omitempty"`
}

// Number of ResolverRunner tasks (and Otto contexts) to cache
const kGQTaskCacheSize = 2

//////// GRAPHQL RESOLVER:

// An object that can run a JavaScript GraphQL resolve function, as found in the GraphQL config.
type GraphQLResolver struct {
	*sgbucket.JSServer // "Superclass"
	Name               string
}

// Creates a GraphQLResolver given its name and JavaScript source code.
func NewGraphQLResolver(name string, fnSource string) *GraphQLResolver {
	return &GraphQLResolver{
		JSServer: sgbucket.NewJSServer(fnSource, kGQTaskCacheSize,
			func(fnSource string) (sgbucket.JSServerTask, error) {
				return NewGraphQLResolverRunner(name, fnSource)
			}),
		Name: name,
	}
}

// Calls a GraphQLResolver. `params` is the parameter struct passed by the go-graphql API,
// and mutationAllowed is true iff the resolver is allowed to make changes to the database;
// the `save` callback checks this.
func (res *GraphQLResolver) Resolve(db *Database, params *graphql.ResolveParams, mutationAllowed bool) (interface{}, error) {
	context := jsResolveContext{}
	if db.user != nil {
		name := db.user.Name()
		context.User = &name
	}
	info := jsResolveInfo{
		FieldName:      params.Info.FieldName,
		RootValue:      params.Info.RootValue,
		VariableValues: params.Info.VariableValues,
	}

	return res.WithTask(func(task sgbucket.JSServerTask) (interface{}, error) {
		runner := task.(*graphQLResolverRunner)
		runner.currentDB = db
		runner.mutationAllowed = mutationAllowed
		return task.Call(params.Source, params.Args, &context, &info)
	})
}

//////// RUNNER:

// The outermost JavaScript code. Evaluating it returns a function, which is then called by the
// Runner every time it's invoked.
// `%s` is replaced with the resolver.
const kGraphQLResolverFuncWrapper = `
	function() {
		function resolveFn(parent, args, context, info) {
			%s; // <-- The actual JS code from the config file goes here
		}

		// The "context.app" object the resolver script calls:
		var _app = {
			get: _get,
			query: _query,
			save: _save
		};

		// This is the JS function invoked by the 'Call(...)' in GraphQLResolver.Resolve(), above
		return function (parent, args, context, info) {
			if (context)
				context.app = _app;
			else
				context = {app: _app};
			return resolveFn(parent, args, context, info);
		}
	}()`

func wrappedFuncSource(funcSource string) string {
	return fmt.Sprintf(kGraphQLResolverFuncWrapper, funcSource)
}

// An object that runs a specific JS GraphQuery resolver function. Not thread-safe!
// Owned by a GraphQLResolver, which arbitrates access to it.
type graphQLResolverRunner struct {
	sgbucket.JSRunner           // "Superclass"
	name              string    // Name of the resolver
	currentDB         *Database // Database instance for this call
	mutationAllowed   bool      // Whether save() is allowed during this call
}

// Creates a graphQLResolverRunner given its name and JavaScript source code.
func NewGraphQLResolverRunner(name string, funcSource string) (*graphQLResolverRunner, error) {
	ctx := context.Background()
	runner := &graphQLResolverRunner{name: name}
	err := runner.InitWithLogging("",
		func(s string) {
			base.ErrorfCtx(ctx, base.KeyJavascript.String()+": GraphQLResolver %s: %s", name, base.UD(s))
		},
		func(s string) {
			base.InfofCtx(ctx, base.KeyJavascript, "GraphQLResolver %s: %s", name, base.UD(s))
		})
	if err != nil {
		return nil, err
	}

	// Implementation of the 'get(docID)' callback:
	runner.DefineNativeFunction("_get", func(call otto.FunctionCall) otto.Value {
		docID := ottoStringParam(call, 0, "app.get")
		doc, err := runner.do_get(docID, nil)
		return ottoResult(call, doc, err)
	})

	// Implementation of the 'query(n1ql,params)' callback:
	runner.DefineNativeFunction("_query", func(call otto.FunctionCall) otto.Value {
		queryName := ottoStringParam(call, 0, "app.query")
		params := ottoObjectParam(call, 1, true, "app.query")
		result, err := runner.do_query(queryName, params)
		return ottoResult(call, result, err)
	})

	// Implementation of the 'save(docID,doc)' callback:
	runner.DefineNativeFunction("_save", func(call otto.FunctionCall) otto.Value {
		docID := ottoStringParam(call, 0, "app.save")
		doc := ottoObjectParam(call, 1, false, "app.save")
		err := runner.do_save(docID, doc)
		return ottoResult(call, nil, err)
	})

	// Set (and compile) the function:
	if _, err := runner.SetFunction(funcSource); err != nil {
		fmt.Printf("*** Error: Resolver fn failed to compile: %v", err) //TEMP
		return nil, base.HTTPErrorf(http.StatusInternalServerError, "Error compiling GraphQL resolver %s: %v", name, err)
	}

	// Function that runs before every call:
	runner.Before = func() {
		if runner.currentDB == nil {
			panic("GraphQLResolverRunner can't run without a currentDB")
		}
		fmt.Printf("*** GQ runner %s about to run\n", runner.name)
	}
	// Function that runs after every call:
	runner.After = func(jsResult otto.Value, err error) (interface{}, error) {
		runner.currentDB = nil
		if err != nil {
			fmt.Printf("*** GQ runner %s failed: %+v\n", runner.name, err)
			return nil, err
		}
		result, _ := jsResult.Export()
		fmt.Printf("*** GQ runner %s finished: %v\n", runner.name, result)
		return result, nil
	}

	return runner, nil
}

func (runner *graphQLResolverRunner) SetFunction(funcSource string) (bool, error) {
	funcSource = wrappedFuncSource(funcSource)
	return runner.JSRunner.SetFunction(funcSource)
}

// Implementation of JS `app.get(docID, docType)` function
func (runner *graphQLResolverRunner) do_get(docID string, docType *string) (interface{}, error) {
	fmt.Printf("*** GQ get(%q)\n", docID)
	rev, err := runner.currentDB.GetRev(docID, "", false, nil)
	if err != nil {
		status, _ := base.ErrorAsHTTPStatus(err)
		if status == http.StatusNotFound {
			// Not-found is not an error; just return null.
			return nil, nil
		}
		return nil, err
	}
	body, err := rev.Body()
	if err != nil {
		return nil, err
	}
	if docType != nil && body["type"] != *docType {
		return nil, nil
	}
	body["id"] = docID
	return body, nil
}

// Implementation of JS `app.query(name, params)` function
func (runner *graphQLResolverRunner) do_query(queryName string, params map[string]interface{}) ([]interface{}, error) {
	fmt.Printf("*** GQ query(%q, %+v)\n", queryName, params)

	// defer func() {
	// 	if x := recover(); x != nil {
	// 		fmt.Printf("*** run time panic: %+v\n", pkgerrors.WithStack(x.(error)))
	// 	}
	// }()

	results, err := runner.currentDB.UserQuery(queryName, params)
	if err != nil {
		return nil, err
	}
	result := []interface{}{}
	var row interface{}
	for results.Next(&row) {
		result = append(result, row)
	}
	if err = results.Close(); err != nil {
		return nil, err
	}
	return result, err
}

// Implementation of JS `app.save(docID, body)` function
func (runner *graphQLResolverRunner) do_save(docID string, body map[string]interface{}) error {
	fmt.Printf("*** GQ save(%q, %v)\n", docID, body)
	if !runner.mutationAllowed {
		return fmt.Errorf("A read-only request is not allowed to mutate the database")
	}

	// TODO: Currently this is "last writer wins": get the current revision if any, and pass it
	// to Put so that the save always succeeds.
	rev, err := runner.currentDB.GetRev(docID, "", false, []string{})
	if err == nil {
		body["_rev"] = rev.RevID
	}

	_, _, err = runner.currentDB.Put(docID, body)
	return err
}

//////// OTTO UTILITIES:

// Returns a parameter of `call` as a Go string, or throws a JS exception if it's not a string.
func ottoStringParam(call otto.FunctionCall, arg int, what string) string {
	val := call.Argument(arg)
	if !val.IsString() {
		panic(call.Otto.MakeTypeError(fmt.Sprintf("%s() param %d must be a string", what, arg+1)))
	}
	return val.String()

}

// Returns a parameter of `call` as a Go map, or throws a JS exception if it's not a map.
// If `optional` is true, the parameter is allowed not to exist, in which case `nil` is returned.
func ottoObjectParam(call otto.FunctionCall, arg int, optional bool, what string) map[string]interface{} {
	val := call.Argument(arg)
	if !val.IsObject() {
		if optional && val.IsUndefined() {
			return nil
		}
		panic(call.Otto.MakeTypeError(fmt.Sprintf("%s() param %d must be an object", what, arg+1)))
	}
	obj, err := val.Export()
	if err != nil {
		panic(call.Otto.MakeTypeError("Yikes, couldn't export JS value"))
	}
	return obj.(map[string]interface{})

}

// Returns `result` back to Otto; or if `err` is non-nil, throws it.
func ottoResult(call otto.FunctionCall, result interface{}, err error) otto.Value {
	if err == nil {
		val, _ := call.Otto.ToValue(result)
		return val
	} else {
		panic(call.Otto.MakeCustomError("GoError", err.Error())) //TODO: Improve
	}
}
