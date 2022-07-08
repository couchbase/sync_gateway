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
	"log"
	"net/http"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

//////// USER FUNCTION CONFIGURATION:

// Top level user-function config object: the map of names to queries.
type UserFunctionMap = map[string]*UserFunctionConfig

// Defines a JavaScript function that a client can invoke by name.
// (The name is the key in the UserFunctionMap.)
type UserFunctionConfig struct {
	SourceCode string          `json:"javascript"`           // Javascript source
	Parameters []string        `json:"parameters,omitempty"` // Names of parameters
	Allow      *UserQueryAllow `json:"allow,omitempty"`      // Permissions (admin-only if nil)

	compiled *sgbucket.JSServer // Compiled form of the function (instantiated lazily)
}

//////// RUNNING A USER FUNCTION:

func (db *Database) CallUserFunction(name string, params map[string]interface{}, mutationAllowed bool) (interface{}, error) {
	config, found := db.Options.UserFunctions[name]
	if !found {
		return nil, base.HTTPErrorf(http.StatusNotFound, "No such user function")
	}

	// Check that the user is authorized:
	if err := config.Allow.authorize(db.user, params, "function", name); err != nil {
		return nil, err
	}

	// Make sure each specified parameter has a value in `params`:
	if err := checkQueryArguments(params, config.Parameters, "function", name); err != nil {
		return nil, err
	}

	// Compile and run the function:
	compiled := config.compiled
	if compiled == nil {
		compiled = sgbucket.NewJSServer(config.SourceCode, kUserFnTaskCacheSize,
			func(fnSource string) (sgbucket.JSServerTask, error) {
				return NewUserFunctionRunner(name, "user function", kUserFunctionFuncWrapper, fnSource)
			})
		// This is a race condition if two threads find the script uncompiled and both compile it.
		// However, the effect is simply that two identical UserFunction instances are created and
		// one of them (the first one stored to config.compiled) is thrown away; harmless.
		config.compiled = compiled
	}

	return compiled.WithTask(func(task sgbucket.JSServerTask) (interface{}, error) {
		runner := task.(*userFunctionRunner)
		runner.currentDB = db
		runner.mutationAllowed = mutationAllowed
		return task.Call(params, newUserFunctionJSContext(db))
	})
}

// The `context` parameter passed to the JS function
type userFunctionJSContext struct {
	User *string `json:"user,omitempty"`
}

func newUserFunctionJSContext(db *Database) *userFunctionJSContext {
	context := &userFunctionJSContext{}
	if db.user != nil {
		name := db.user.Name()
		context.User = &name
	}
	return context
}

//////// JAVASCRIPT RUNNER:

// Number of userFunctionRunner tasks (and Otto contexts) to cache, per function
const kUserFnTaskCacheSize = 2

// The outermost JavaScript code. Evaluating it returns a function, which is then called by the
// Runner every time it's invoked. (The reason the first few lines are ""-style strings is to make
// sure the resolver code ends up on line 1, which makes line numbers reported in syntax errors
// accurate.)
// `%s` is replaced with the resolver.
const kUserFunctionFuncWrapper = "function() {" +
	"	function userFn(args, context) {" +
	"		%s;" + // <-- The actual JS code from the config file goes here
	`	}

		// This is the JS function invoked by the 'Call(...)' in UserFunction.Resolve(), above
		return function (args, context) {
			context.app = {
				get: _get,
				query: _query,
				save: _save
			};
			return userFn(args, context);
		}
	}()`

// An object that runs a specific JS GraphQuery resolver function. Not thread-safe!
// Owned by a UserFunction, which arbitrates access to it.
type userFunctionRunner struct {
	sgbucket.JSRunner           // "Superclass"
	kind              string    // "user function", "GraphQL resolver", etc
	name              string    // Name of this function or resolver
	funcWrapper       string    // JS code that wraps around the user function
	currentDB         *Database // Database instance (updated before every call)
	mutationAllowed   bool      // Whether save() is allowed (updated before every call)
}

// Creates a userFunctionRunner given its name and JavaScript source code.
func NewUserFunctionRunner(name string, kind string, funcWrapper string, funcSource string) (*userFunctionRunner, error) {
	ctx := context.Background()
	runner := &userFunctionRunner{
		name:        name,
		kind:        kind,
		funcWrapper: funcWrapper,
	}
	err := runner.InitWithLogging("",
		func(s string) {
			base.ErrorfCtx(ctx, base.KeyJavascript.String()+": %s %s: %s", kind, name, base.UD(s))
		},
		func(s string) {
			base.InfofCtx(ctx, base.KeyJavascript, "%s %s: %s", kind, name, base.UD(s))
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

	// Set (and compile) the JS function:
	if _, err := runner.JSRunner.SetFunction(fmt.Sprintf(runner.funcWrapper, funcSource)); err != nil {
		log.Printf("*** Error: %s %s failed to compile: %v", kind, name, err) //TEMP
		return nil, base.HTTPErrorf(http.StatusInternalServerError, "Error compiling %s %q: %v", kind, name, err)
	}

	// Function that runs before every call:
	runner.Before = func() {
		if runner.currentDB == nil {
			panic("UserFunctionRunner can't run without a currentDB")
		}
		//log.Printf("*** UserFn runner %s about to run", runner.name)
	}
	// Function that runs after every call:
	runner.After = func(jsResult otto.Value, err error) (interface{}, error) {
		runner.currentDB = nil
		if err != nil {
			log.Printf("*** %s %s failed: %+v", runner.kind, runner.name, err)
			return nil, err
		}
		return jsResult.Export()
	}

	return runner, nil
}

//////// CALLBACK FUNCTION IMPLEMENTATIONS:

// Implementation of JS `app.get(docID, docType)` function
func (runner *userFunctionRunner) do_get(docID string, docType *string) (interface{}, error) {
	//log.Printf("*** UserFn get(%q)", docID)
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
func (runner *userFunctionRunner) do_query(queryName string, params map[string]interface{}) ([]interface{}, error) {
	//log.Printf("*** UserFn query(%q, %+v)", queryName, params)

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
func (runner *userFunctionRunner) do_save(docID string, body map[string]interface{}) error {
	//log.Printf("*** UserFn save(%q, %v)", docID, body)
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
