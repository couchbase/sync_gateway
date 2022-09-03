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
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

// Number of Otto contexts to cache per function, i.e. the number of goroutines that can be simultaneously running each function.
const kUserFunctionCacheSize = 2

// The outermost JavaScript code. Evaluating it returns a function, which is then called by the
// Runner every time it's invoked. Contains `%s` where the actual function source code goes.
//go:embed user_js_wrapper.js
var kJavaScriptWrapper string

// Creates a JSServer instance wrapping a userJSRunner, for user JS functions and GraphQL resolvers.
func newUserFunctionJSServer(name string, what string, sourceCode string) (*sgbucket.JSServer, error) {
	js := fmt.Sprintf(kJavaScriptWrapper, sourceCode)
	jsServer := sgbucket.NewJSServer(js, 0, kUserFunctionCacheSize,
		func(fnSource string, timeout time.Duration) (sgbucket.JSServerTask, error) {
			return newUserJavaScriptRunner(name, what, fnSource)
		})
	// Call WithTask to force a task to be instantiated, which will detect syntax errors in the script. Otherwise the error only gets detected the first time a client calls the function.
	var err error
	_, err = jsServer.WithTask(func(sgbucket.JSServerTask) (interface{}, error) {
		return nil, nil
	})
	return jsServer, err
}

func newUserFunctionJSContext(db *Database) map[string]interface{} {
	return makeUserCtx(db.user)
}

// An object that runs a user JavaScript function or a GraphQL resolver.
// Not thread-safe! Owned by an sgbucket.JSServer, which arbitrates access to it.
type userJSRunner struct {
	sgbucket.JSRunner           // "Superclass"
	kind              string    // "user function", "GraphQL resolver", etc
	name              string    // Name of this function or resolver
	currentDB         *Database // Database instance (updated before every call)
	mutationAllowed   bool      // Whether save() is allowed (updated before every call)
}

// Creates a userJSRunner given its name and JavaScript source code.
func newUserJavaScriptRunner(name string, kind string, funcSource string) (*userJSRunner, error) {
	ctx := context.Background()
	runner := &userJSRunner{
		name: name,
		kind: kind,
	}
	err := runner.InitWithLogging("", 0,
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
		sudo := ottoBoolParam(call, 2)
		result, err := runner.do_func(funcName, params, sudo)
		return ottoJSONResult(call, result, err)
	})

	// Implementation of the 'get(docID)' callback:
	runner.DefineNativeFunction("_get", func(call otto.FunctionCall) otto.Value {
		docID := ottoStringParam(call, 0, "app.get")
		sudo := ottoBoolParam(call, 1)
		doc, err := runner.do_get(docID, nil, sudo)
		return ottoJSONResult(call, doc, err)
	})

	// Implementation of the 'graphql(query,params)' callback:
	runner.DefineNativeFunction("_graphql", func(call otto.FunctionCall) otto.Value {
		query := ottoStringParam(call, 0, "app.graphql")
		params := ottoObjectParam(call, 1, true, "app.graphql")
		sudo := ottoBoolParam(call, 2)
		result, err := runner.do_graphql(query, params, sudo)
		return ottoJSONResult(call, result, err)
	})

	// Implementation of the 'save(docID,doc)' callback:
	runner.DefineNativeFunction("_save", func(call otto.FunctionCall) otto.Value {
		docID := ottoStringParam(call, 0, "app.save")
		doc := ottoObjectParam(call, 1, false, "app.save")
		sudo := ottoBoolParam(call, 2)
		err := runner.do_save(docID, doc, sudo)
		return ottoResult(call, nil, err)
	})

	// Implementation of the 'delete(docID,doc)' callback:
	runner.DefineNativeFunction("_delete", func(call otto.FunctionCall) otto.Value {
		docID := ottoStringParam(call, 0, "app.delete")
		sudo := ottoBoolParam(call, 1)
		err := runner.do_delete(docID, sudo)
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

// Calls a javaScriptRunner's JavaScript function.
func (runner *userJSRunner) CallWithDB(db *Database, mutationAllowed bool, args ...interface{}) (result interface{}, err error) {
	runner.currentDB = db
	runner.mutationAllowed = mutationAllowed
	ctx := db.Ctx
	var timeout time.Duration
	if ctx != nil {
		if deadline, exists := ctx.Deadline(); exists {
			timeout = time.Until(deadline)
			if timeout <= 0 {
				return nil, sgbucket.ErrJSTimeout
			}
		}
	}
	runner.SetTimeout(timeout)
	return runner.Call(args...)
}

// JavaScript error returned by a userJavaScriptRunner
type jsError struct {
	err        error
	runnerKind string
	runnerName string
}

func (jserr *jsError) Error() string {
	return fmt.Sprintf("%v (while calling %s %q)", jserr.err, jserr.runnerKind, jserr.runnerName)
}

func (jserr *jsError) Unwrap() error {
	return jserr.err
}

var HttpErrRE = regexp.MustCompile(`^HTTP:\s*(\d+)\s+(.*)`)

func (runner *userJSRunner) convertError(err error) error {
	if err == sgbucket.ErrJSTimeout {
		return base.HTTPErrorf(408, "Timeout in JavaScript")
	}
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
	return &jsError{err, runner.kind, runner.name}
}

//////// DATABASE CALLBACK FUNCTION IMPLEMENTATIONS:

// Implementation of JS `app.func(name, params)` function
func (runner *userJSRunner) do_func(funcName string, params map[string]interface{}, sudo bool) (interface{}, error) {
	if sudo {
		user := runner.currentDB.user
		runner.currentDB.user = nil
		defer func() { runner.currentDB.user = user }()
	}
	return runner.currentDB.CallUserFunction(funcName, params, runner.mutationAllowed)
}

// Implementation of JS `app.get(docID, docType)` function
func (runner *userJSRunner) do_get(docID string, docType *string, sudo bool) (interface{}, error) {
	if err := runner.currentDB.CheckTimeout(); err != nil {
		return nil, err
	}
	if sudo {
		user := runner.currentDB.user
		runner.currentDB.user = nil
		defer func() { runner.currentDB.user = user }()
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
	body["_id"] = docID
	body["_rev"] = rev.RevID
	return body, nil
}

// Implementation of JS `app.graphql(query, params)` function
func (runner *userJSRunner) do_graphql(query string, params map[string]interface{}, sudo bool) (interface{}, error) {
	if sudo {
		user := runner.currentDB.user
		runner.currentDB.user = nil
		defer func() { runner.currentDB.user = user }()
	}
	return runner.currentDB.UserGraphQLQuery(query, "", params, runner.mutationAllowed)
}

// Implementation of JS `app.save(docID, body)` function
func (runner *userJSRunner) do_save(docID string, body map[string]interface{}, sudo bool) error {
	if err := runner.currentDB.CheckTimeout(); err != nil {
		return err
	}
	if !runner.mutationAllowed {
		return base.HTTPErrorf(http.StatusForbidden, "a read-only request is not allowed to mutate the database")
	}
	if sudo {
		user := runner.currentDB.user
		runner.currentDB.user = nil
		defer func() { runner.currentDB.user = user }()
	}

	delete(body, "_id")
	if _, found := body["_rev"]; found {
		// If caller provided `_rev` property, use MVCC as normal:
		_, _, err := runner.currentDB.Put(docID, body)
		return err
	} else {
		// If caller didn't provide a `_rev` property, fall back to "last writer wins":
		// get the current revision if any, and pass it to Put so that the save always succeeds.
		for {
			rev, err := runner.currentDB.GetRev(docID, "", false, []string{})
			if err != nil {
				if status, _ := base.ErrorAsHTTPStatus(err); status != http.StatusNotFound {
					return err
				}
			}
			if rev.RevID == "" {
				delete(body, "_rev")
			} else {
				body["_rev"] = rev.RevID
			}

			_, _, err = runner.currentDB.Put(docID, body)
			if err == nil {
				return nil
			} else if status, _ := base.ErrorAsHTTPStatus(err); status != http.StatusConflict {
				return err
			}
			// on conflict (race condition), retry...
		}
	}
}

// Implementation of JS `app.delete(docID)` function
func (runner *userJSRunner) do_delete(docID string, sudo bool) error {
	return runner.do_save(docID, map[string]interface{}{"_deleted": true}, sudo)
}

//////// OTTO UTILITIES:

// Returns a parameter of `call` as a Go string, or throws a JS exception if it's not a string.
func ottoBoolParam(call otto.FunctionCall, arg int) bool {
	result, _ := call.Argument(arg).ToBoolean()
	return result
}

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

// Returns `result` back to Otto in JSON form; or if `err` is non-nil, "throws" it via a Go panic
func ottoJSONResult(call otto.FunctionCall, result interface{}, err error) otto.Value {
	if err == nil && result != nil {
		if j, err := json.Marshal(result); err == nil {
			val, _ := call.Otto.ToValue(string(j))
			return val
		}
	}
	return ottoResult(call, result, err)
}
