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
	"log"
	"net/http"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

// Note: This source file was originally adapted from channels/channelmapper.go and channels/sync_runner.go

// Subset of graphql.ResolveInfo, which is marshalable to JSON (and thereby to Otto)
type jsGQResolveInfo struct {
	FieldName      string                 `json:"fieldName"`
	RootValue      interface{}            `json:"rootValue"`
	VariableValues map[string]interface{} `json:"variableValues"`
	Subfields      []string               `json:"subfields"`
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
	// Collect the 'subfields', the fields the query wants from the value being resolved:
	subfields := []string{}
	if len(params.Info.FieldASTs) > 0 {
		for _, sel := range params.Info.FieldASTs[0].SelectionSet.Selections {
			if subfield, ok := sel.(*ast.Field); ok {
				if subfield.Name.Kind == "Name" {
					subfields = append(subfields, subfield.Name.Value)
				}
			}
		}
	}
	//log.Printf("-- %q : subfields = %v", params.Info.FieldName, subfields)
	context := jsResolveContext{}
	if db.user != nil {
		name := db.user.Name()
		context.User = &name
	}
	info := jsGQResolveInfo{
		FieldName:      params.Info.FieldName,
		RootValue:      params.Info.RootValue,
		VariableValues: params.Info.VariableValues,
		Subfields:      subfields,
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
// Runner every time it's invoked. (The reason the first few lines are ""-style strings is to make
// sure the resolver code ends up on line 1, which makes line numbers reported in syntax errors
// accurate.)
// `%s` is replaced with the resolver.
const kGraphQLResolverFuncWrapper = "function() {" +
	"	function resolveFn(parent, args, context, info) {" +
	"		%s;" + // <-- The actual JS code from the config file goes here
	`	}

		// This is the JS function invoked by the 'Call(...)' in GraphQLResolver.Resolve(), above
		return function (parent, args, context, info) {
			// The "context.app" object the resolver script calls:
			var _app = {
				get: _get,
				query: _query,
				save: _save
			};

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
		log.Printf("*** Error: Resolver fn %s failed to compile: %v", name, err) //TEMP
		return nil, base.HTTPErrorf(http.StatusInternalServerError, "Error compiling GraphQL resolver %s: %v", name, err)
	}

	// Function that runs before every call:
	runner.Before = func() {
		if runner.currentDB == nil {
			panic("GraphQLResolverRunner can't run without a currentDB")
		}
		//log.Printf("*** GQ runner %s about to run", runner.name)
	}
	// Function that runs after every call:
	runner.After = func(jsResult otto.Value, err error) (interface{}, error) {
		runner.currentDB = nil
		if err != nil {
			log.Printf("*** GQ runner %s failed: %+v", runner.name, err)
			return nil, err
		}
		result, _ := jsResult.Export()
		//log.Printf("*** GQ runner %s finished: %v", runner.name, result)
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
	//log.Printf("*** GQ get(%q)", docID)
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
	//log.Printf("*** GQ query(%q, %+v)", queryName, params)

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
	//log.Printf("*** GQ save(%q, %v)", docID, body)
	if !runner.mutationAllowed {
		return fmt.Errorf("a read-only request is not allowed to mutate the database")
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
