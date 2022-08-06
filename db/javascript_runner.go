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
	"regexp"
	"strconv"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

// An object that runs a specific JS function (user function or GraphQL resolver).
// Not thread-safe! Owned by an sgbucket.JSServer, which arbitrates access to it.
type javaScriptRunner struct {
	sgbucket.JSRunner           // "Superclass"
	kind              string    // "user function", "GraphQL resolver", etc
	name              string    // Name of this function or resolver
	currentDB         *Database // Database instance (updated before every call)
	mutationAllowed   bool      // Whether save() is allowed (updated before every call)
}

// Creates a javaScriptRunner given its name and JavaScript source code.
func newJavaScriptRunner(name string, kind string, funcSource string) (*javaScriptRunner, error) {
	ctx := context.Background()
	runner := &javaScriptRunner{
		name: name,
		kind: kind,
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

	// Implementation of the 'func(name,params)' callback:
	runner.DefineNativeFunction("_func", func(call otto.FunctionCall) otto.Value {
		funcName := ottoStringParam(call, 0, "app.func")
		params := ottoObjectParam(call, 1, true, "app.func")
		result, err := runner.do_func(funcName, params)
		return ottoResult(call, result, err)
	})

	// Implementation of the 'get(docID)' callback:
	runner.DefineNativeFunction("_get", func(call otto.FunctionCall) otto.Value {
		docID := ottoStringParam(call, 0, "app.get")
		doc, err := runner.do_get(docID, nil)
		return ottoResult(call, doc, err)
	})

	// Implementation of the 'graphql(query,params)' callback:
	runner.DefineNativeFunction("_graphql", func(call otto.FunctionCall) otto.Value {
		query := ottoStringParam(call, 0, "app.graphql")
		params := ottoObjectParam(call, 1, true, "app.graphql")
		result, err := runner.do_graphql(query, params)
		return ottoResult(call, result, err)
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
	if _, err := runner.JSRunner.SetFunction(funcSource); err != nil {
		return nil, base.HTTPErrorf(http.StatusInternalServerError, "Error compiling %s %q: %v", kind, name, err)
	}

	// Function that runs before every call:
	runner.Before = func() {
		if runner.currentDB == nil {
			panic("javaScriptRunner can't run without a currentCtx or currentDB")
		}
		//log.Printf("*** javaScriptRunner %s about to run", runner.name)
	}
	// Function that runs after every call:
	runner.After = func(jsResult otto.Value, err error) (interface{}, error) {
		defer func() {
			runner.currentDB = nil
		}()
		if err != nil {
			base.ErrorfCtx(context.Background(), base.KeyJavascript.String()+": %s %s failed: %#v", runner.kind, runner.name, err)
			return nil, runner.convertError(err)
		}
		return jsResult.Export()
	}

	return runner, nil
}

type jsError struct {
	err    error
	runner *javaScriptRunner
}

func (jserr *jsError) Error() string {
	return fmt.Sprintf("%v (while calling %s %q)", jserr.err, jserr.runner.kind, jserr.runner.name)
}

func (jserr *jsError) Unwrap() error {
	return jserr.err
}

var HttpErrRE = regexp.MustCompile(`^HTTP:\s*(\d+)\s+(.*)`)

func (runner *javaScriptRunner) convertError(err error) error {
	// Unfortunately there is no API on otto.Error to get the name & message separately.
	// Instead, look for the name as a prefix. (See the `ottoResult` function below)
	str := err.Error()
	if strings.HasPrefix(str, "HTTP:") {
		m := HttpErrRE.FindStringSubmatch(str)
		status, _ := strconv.ParseInt(m[1], 10, 0)
		message := m[2]
		if status == http.StatusUnauthorized && (runner.currentDB.user == nil || runner.currentDB.user.Name() != "") {
			status = http.StatusForbidden
		}
		return base.HTTPErrorf(int(status), "%s (while calling %s %q)", message, runner.kind, runner.name)
	}
	return &jsError{err, runner}
}

//////// DATABASE CALLBACK FUNCTION IMPLEMENTATIONS:

// Implementation of JS `app.func(name, params)` function
func (runner *javaScriptRunner) do_func(funcName string, params map[string]interface{}) (interface{}, error) {
	//log.Printf("*** UserFn func(%q, %+v)", funcName, params)
	return runner.currentDB.CallUserFunction(funcName, params, runner.mutationAllowed)
}

// Implementation of JS `app.get(docID, docType)` function
func (runner *javaScriptRunner) do_get(docID string, docType *string) (interface{}, error) {
	//log.Printf("*** UserFn get(%q)", docID)
	if err := runner.currentDB.CheckTimeout(); err != nil {
		return nil, err
	}
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

// Implementation of JS `app.graphql(name, params)` function
func (runner *javaScriptRunner) do_graphql(query string, params map[string]interface{}) (interface{}, error) {
	//log.Printf("*** UserFn graphql(%q, %+v)", funcName, params)
	return runner.currentDB.UserGraphQLQuery(query, "", params, runner.mutationAllowed)
}

// Implementation of JS `app.query(name, params)` function
func (runner *javaScriptRunner) do_query(queryName string, params map[string]interface{}) ([]interface{}, error) {
	//log.Printf("*** UserFn query(%q, %+v)", queryName, params)

	results, err := runner.currentDB.UserN1QLQuery(queryName, params)
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
func (runner *javaScriptRunner) do_save(docID string, body map[string]interface{}) error {
	//log.Printf("*** UserFn save(%q, %v)", docID, body)
	if err := runner.currentDB.CheckTimeout(); err != nil {
		return err
	}
	if !runner.mutationAllowed {
		return base.HTTPErrorf(http.StatusForbidden, "a read-only request is not allowed to mutate the database")
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

// Returns `result` back to Otto; or if `err` is non-nil, "throws" it via a Go panic
func ottoResult(call otto.FunctionCall, result interface{}, err error) otto.Value {
	if err == nil {
		val, _ := call.Otto.ToValue(result)
		return val
	} else {
		// (javaScriptRunner.convertError clumsily takes these apart back into errors)
		if status, msg := base.ErrorAsHTTPStatus(err); status != 500 && status != 200 {
			panic(call.Otto.MakeCustomError("HTTP", fmt.Sprintf("%d %s", status, msg)))
		} else {
			panic(call.Otto.MakeCustomError("Go", err.Error()))
		}
	}
}

//////// JAVASCRIPT CODE:

// The outermost JavaScript code. Evaluating it returns a function, which is then called by the
// Runner every time it's invoked. (The reason the first few lines are ""-style strings is to make
// sure the resolver code ends up on line 1, which makes line numbers reported in syntax errors
// accurate.)
// `%[1]s` is replaced with the function's parameter list.
// `%[2]s` is replaced with the function's body.
const kJavaScriptWrapper = "function() {" +
	"	function userFn(%[1]s) {" + // <-- The parameter list of the JS function goes here
	"		%[2]s" + // <-- The actual JS code from the config file goes here
	`	}

		// Prototype of the user object:
		function User(info) {
			this.name = info.name;
			this.roles = info.roles;
			this.channels = info.channels;
		}

		User.prototype.requireAdmin = function() {
			throw("HTTP: 403 Forbidden");
		}

		User.prototype.requireName = function(name) {
			var allowed;
			if (Array.isArray(name)) {
				allowed = name.indexOf(this.name) != -1;
			} else {
				allowed = this.name == name;
			}
			if (!allowed)
				throw("HTTP: 401 Unauthorized");
		}

		User.prototype.requireRole = function(role) {
			if (Array.isArray(role)) {
				for (var i = 0; i < role.length; ++i) {
					if (this.roles[role[i]] !== undefined)
						return;
				}
			} else {
				if (this.roles[role] !== undefined)
					return;
			}
			throw("HTTP: 401 Unauthorized");
		}

		User.prototype.requireAccess = function(channel) {
			if (Array.isArray(channel)) {
				for (var i = 0; i < channel.length; ++i) {
					if (this.channels.indexOf(channel[i]) != -1)
						return;
				}
			} else {
				if (this.channels.indexOf(channel) != -1)
					return;
			}
			throw("HTTP: 401 Unauthorized");
		}

		// Admin prototype makes all the "require..." functions no-ops:
		function Admin() { }
		Admin.prototype.requireAdmin = Admin.prototype.requireName =
			Admin.prototype.requireRole = Admin.prototype.requireAccess = function() { }

		function MakeUser(info) {
			if (info && info.name !== undefined) {
				return new User(info);
			} else {
				return new Admin();
			}
		}

		// App object contains the native Go functions to access the database:
		var App = {
			func:    _func,
			get:     _get,
			graphql: _graphql,
			query:   _query,
			save:    _save
		};

		// Return the JS function that will be invoked repeatedly by the runner:
		return function (%[1]s) {
			context.user = MakeUser(context.user);
			context.app = Object.create(App);
			return userFn(%[1]s);
		};
	}()`
