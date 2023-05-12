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
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/robertkrimen/otto"
)

func (runner *jsRunner) defineNativeCallbacks() {
	// Implementation of the 'delete(docID)' callback:
	runner.DefineNativeFunction("_delete", func(call otto.FunctionCall) otto.Value {
		var docID string
		var doc map[string]any
		if arg0 := call.Argument(0); arg0.IsString() {
			docID = arg0.String()
		} else if arg0.IsObject() {
			doc = ottoObjectParam(call, 0, false, "user.delete")
		} else {
			panic(call.Otto.MakeTypeError("user.delete() arg 1 must be a string or object"))
		}
		sudo := ottoBoolParam(call, 1)
		ok, err := runner.do_delete(docID, doc, sudo)
		return ottoResult(call, ok, err)
	})

	// Implementation of the 'function(name,params)' callback:
	runner.DefineNativeFunction("_func", func(call otto.FunctionCall) otto.Value {
		funcName := ottoStringParam(call, 0, "user.function")
		params := ottoObjectParam(call, 1, true, "user.function")
		sudo := ottoBoolParam(call, 2)
		result, err := runner.do_func(funcName, params, sudo)
		return ottoJSONResult(call, result, err)
	})

	// Implementation of the 'get(docID)' callback:
	runner.DefineNativeFunction("_get", func(call otto.FunctionCall) otto.Value {
		docID := ottoStringParam(call, 0, "user.get")
		sudo := ottoBoolParam(call, 1)
		doc, err := runner.do_get(docID, nil, sudo)
		return ottoJSONResult(call, doc, err)
	})

	// Implementation of the 'graphql(query,params)' callback:
	runner.DefineNativeFunction("_graphql", func(call otto.FunctionCall) otto.Value {
		query := ottoStringParam(call, 0, "user.graphql")
		params := ottoObjectParam(call, 1, true, "user.graphql")
		sudo := ottoBoolParam(call, 2)
		result, err := runner.do_graphql(query, params, sudo)
		return ottoJSONResult(call, result, err)
	})

	// Implementation of the 'save(doc,docID?)' callback:
	runner.DefineNativeFunction("_save", func(call otto.FunctionCall) otto.Value {
		doc := ottoObjectParam(call, 0, false, "user.save")
		docID := ottoOptionalStringParam(call, 1, "user.save")
		sudo := ottoBoolParam(call, 2)
		docID, err := runner.do_save(doc, docID, sudo)
		return ottoResult(call, docID, err)
	})

	// Implementation of the '_requireMutating()' callback:
	runner.DefineNativeFunction("_requireMutating", func(call otto.FunctionCall) otto.Value {
		err := runner.checkMutationAllowed("requireMutating")
		return ottoResult(call, nil, err)
	})
}

func (runner *jsRunner) checkMutationAllowed(what string) error {
	if roFn, ok := runner.ctx.Value(readOnlyKey).(string); ok {
		name := fmt.Sprintf("%s %q", runner.kind, runner.name)
		if name == roFn {
			return base.HTTPErrorf(http.StatusForbidden, "%q called from non-mutating %s", what, roFn)
		} else {
			return base.HTTPErrorf(http.StatusForbidden, "%q called by %s %q from non-mutating %s", what, runner.kind, runner.name, roFn)
		}
	} else {
		return nil
	}
}

// Enters admin/sudo mode, and returns a function that when called will exit it.
func (runner *jsRunner) enterSudo() func() {
	user := runner.currentDB.User()
	ctx := runner.ctx
	runner.currentDB.SetUser(nil)
	runner.ctx = context.WithValue(ctx, readOnlyKey, nil)
	// Return the 'exitSudo' function that the caller will defer:
	return func() {
		runner.currentDB.SetUser(user)
		runner.ctx = ctx
	}
}

//////// DATABASE CALLBACK FUNCTION IMPLEMENTATIONS:

// Implementation of JS `user.delete(doc)` function
// Parameter can be either a docID or a document body with _id (and optionally _rev)
func (runner *jsRunner) do_delete(docID string, body map[string]any, sudo bool) (bool, error) {
	if !sudo {
		if err := runner.checkMutationAllowed("user.delete"); err != nil {
			return false, err
		}
	}
	tombstone := map[string]any{"_deleted": true}
	if body != nil {
		if _id, ok := body["_id"].(string); ok {
			docID = _id
		} else {
			return false, base.HTTPErrorf(400, "Missing doc._id in delete() call")
		}
		if revID, ok := body["_rev"]; ok {
			tombstone["_rev"] = revID
		}
	}
	tombstone["_id"] = docID

	id, err := runner.do_save(tombstone, &docID, sudo)
	return (id != nil), err
}

// Implementation of JS `user.function(name, params)` function
func (runner *jsRunner) do_func(funcName string, params map[string]any, sudo bool) (any, error) {
	if sudo {
		exitSudo := runner.enterSudo()
		defer exitSudo()
	}
	return runner.currentDB.CallUserFunction(funcName, params, true, runner.ctx)
}

// Implementation of JS `user.get(docID, docType?)` function
func (runner *jsRunner) do_get(docID string, docType *string, sudo bool) (any, error) {
	if err := db.CheckTimeout(runner.ctx); err != nil {
		return nil, err
	} else if sudo {
		user := runner.currentDB.User()
		runner.currentDB.SetUser(nil)
		defer func() { runner.currentDB.SetUser(user) }()
	}
	collection, err := runner.currentDB.GetDefaultDatabaseCollectionWithUser()
	if err != nil {
		return nil, err
	}
	rev, err := collection.GetRev(runner.ctx, docID, "", false, nil)
	if err != nil {
		status, _ := base.ErrorAsHTTPStatus(err)
		if status == http.StatusNotFound {
			// Not-found is not an error; just return null.
			return nil, nil
		}
		return nil, err
	}
	body, err := rev.UnmarshalBody() // This call will go away with V8
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

// Implementation of JS `user.graphql(query, params)` function
func (runner *jsRunner) do_graphql(query string, params map[string]any, sudo bool) (any, error) {
	if sudo {
		exitSudo := runner.enterSudo()
		defer exitSudo()
	}
	return runner.currentDB.UserGraphQLQuery(query, "", params, true, runner.ctx)
}

// Implementation of JS `user.save(body, docID?)` function
func (runner *jsRunner) do_save(body map[string]any, docIDPtr *string, sudo bool) (*string, error) {
	if err := db.CheckTimeout(runner.ctx); err != nil {
		return nil, err
	} else if sudo {
		exitSudo := runner.enterSudo()
		defer exitSudo()
	} else if err := runner.checkMutationAllowed("user.put"); err != nil {
		return nil, err
	}

	// The optional `docID` parameter takes precedence over a `_id` key in the body.
	// If neither is present, make up a new random docID.
	var docID string
	if docIDPtr != nil {
		docID = *docIDPtr
	} else if _id, found := body["_id"].(string); found {
		docID = _id
	} else {
		var err error
		docID, err = base.GenerateRandomID()
		if err != nil {
			return nil, err
		}
	}
	delete(body, "_id")
	collection, err := runner.currentDB.GetDefaultDatabaseCollectionWithUser()
	if err != nil {
		return nil, err
	}
	if _, found := body["_rev"]; found {
		// If caller provided `_rev` property, use MVCC as normal:
		_, _, err := collection.Put(runner.ctx, docID, body)
		if err == nil {
			return &docID, err // success
		} else if status, _ := base.ErrorAsHTTPStatus(err); status == http.StatusConflict {
			return nil, nil // conflict: no error, but returns null
		} else {
			return nil, err
		}

	} else {
		// If caller didn't provide a `_rev` property, fall back to "last writer wins":
		// get the current revision if any, and pass it to Put so that the save always succeeds.
		for {
			rev, err := collection.GetRev(runner.ctx, docID, "", false, []string{})
			if err != nil {
				if status, _ := base.ErrorAsHTTPStatus(err); status != http.StatusNotFound {
					return nil, err
				}
			}
			if rev.RevID == "" {
				delete(body, "_rev")
			} else {
				body["_rev"] = rev.RevID
			}

			_, _, err = collection.Put(runner.ctx, docID, body)
			if err == nil {
				break // success!
			} else if status, _ := base.ErrorAsHTTPStatus(err); status != http.StatusConflict {
				return nil, err
			}
			// on conflict (race condition), retry...
		}
	}
	return &docID, nil
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

// Returns a parameter of `call` as a Go string or nil.
func ottoOptionalStringParam(call otto.FunctionCall, arg int, what string) *string {
	val := call.Argument(arg)
	if val.IsString() {
		return base.StringPtr(val.String())
	} else if val.IsNull() || val.IsUndefined() {
		return nil
	} else {
		panic(call.Otto.MakeTypeError(fmt.Sprintf("%s() param %d must be a string or null", what, arg+1)))
	}
}

// Returns a parameter of `call` as a Go map, or throws a JS exception if it's not a map.
// If `optional` is true, the parameter is allowed not to exist, in which case `nil` is returned.
func ottoObjectParam(call otto.FunctionCall, arg int, optional bool, what string) map[string]any {
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
	return obj.(map[string]any)
}

// Returns `result` back to Otto; or if `err` is non-nil, "throws" it via a Go panic
func ottoResult(call otto.FunctionCall, result any, err error) otto.Value {
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
func ottoJSONResult(call otto.FunctionCall, result any, err error) otto.Value {
	if err == nil && result != nil {
		if j, err := json.Marshal(result); err == nil {
			val, _ := call.Otto.ToValue(string(j))
			return val
		}
	}
	return ottoResult(call, result, err)
}
