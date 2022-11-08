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
		docID := ottoStringParam(call, 0, "user.delete")
		doc := ottoObjectParam(call, 1, true, "user.delete")
		sudo := ottoBoolParam(call, 2)
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

	// Implementation of the 'save(docID,doc)' callback:
	runner.DefineNativeFunction("_save", func(call otto.FunctionCall) otto.Value {
		docID := ottoOptionalStringParam(call, 0, "user.save")
		doc := ottoObjectParam(call, 1, false, "user.save")
		sudo := ottoBoolParam(call, 2)
		docID, err := runner.do_save(docID, doc, sudo)
		return ottoResult(call, docID, err)
	})
}

//////// DATABASE CALLBACK FUNCTION IMPLEMENTATIONS:

// Implementation of JS `user.delete(docID)` function
func (runner *jsRunner) do_delete(docID string, body map[string]any, sudo bool) (bool, error) {
	tombstone := map[string]any{"_deleted": true}
	if body != nil {
		if revID, ok := body["_rev"]; ok {
			tombstone["_rev"] = revID
		}
	}
	id, err := runner.do_save(&docID, tombstone, sudo)
	return (id != nil), err
}

// Implementation of JS `user.function(name, params)` function
func (runner *jsRunner) do_func(funcName string, params map[string]any, sudo bool) (any, error) {
	if sudo {
		user := runner.currentDB.User()
		runner.currentDB.SetUser(nil)
		defer func() { runner.currentDB.SetUser(user) }()
	}
	return runner.currentDB.CallUserFunction(funcName, params, runner.mutationAllowed, runner.ctx)
}

// Implementation of JS `user.get(docID, docType)` function
func (runner *jsRunner) do_get(docID string, docType *string, sudo bool) (any, error) {
	if err := db.CheckTimeout(runner.ctx); err != nil {
		return nil, err
	}
	if sudo {
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

// Implementation of JS `user.graphql(query, params)` function
func (runner *jsRunner) do_graphql(query string, params map[string]any, sudo bool) (any, error) {
	if sudo {
		user := runner.currentDB.User()
		runner.currentDB.SetUser(nil)
		defer func() { runner.currentDB.SetUser(user) }()
	}
	return runner.currentDB.UserGraphQLQuery(query, "", params, runner.mutationAllowed, runner.ctx)
}

// Implementation of JS `user.save(docID, body)` function
func (runner *jsRunner) do_save(docIDPtr *string, body map[string]any, sudo bool) (*string, error) {
	if err := db.CheckTimeout(runner.ctx); err != nil {
		return nil, err
	}
	if !runner.mutationAllowed {
		return nil, base.HTTPErrorf(http.StatusForbidden, "a read-only request is not allowed to mutate the database")
	}
	if sudo {
		user := runner.currentDB.User()
		runner.currentDB.SetUser(nil)
		defer func() { runner.currentDB.SetUser(user) }()
	}

	var docID string
	if docIDPtr == nil {
		var err error
		docID, err = base.GenerateRandomID()
		if err != nil {
			return nil, err
		}
	} else {
		docID = *docIDPtr
	}

	delete(body, "_id")
	collection := runner.currentDB.GetSingleDatabaseCollectionWithUser()
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
