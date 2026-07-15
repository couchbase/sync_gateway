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
	"errors"
	"fmt"
	"net/http"

	"github.com/dop251/goja"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

type jsContextKey string

var readOnlyKey = jsContextKey("readOnly") // Context key preventing mutation; val is fn name

func (runner *jsRunner) defineNativeCallbacks(_ context.Context) {
	vm := runner.VM()

	// Implementation of the 'delete(docID)' callback:
	runner.DefineNativeFunction("_delete", func(call goja.FunctionCall) goja.Value {
		var docID string
		var doc map[string]any
		arg0 := call.Argument(0)
		if _, isString := arg0.(goja.String); isString {
			docID = arg0.String()
		} else if _, isObject := arg0.(*goja.Object); isObject {
			doc = jsObjectParam(vm, call, 0, false, "user.delete")
		} else {
			panic(vm.NewTypeError("user.delete() arg 1 must be a string or object"))
		}
		sudo := jsBoolParam(call, 1)
		ok, err := runner.do_delete(docID, doc, sudo)
		return jsResult(vm, ok, err)
	})

	// Implementation of the 'function(name,params)' callback:
	runner.DefineNativeFunction("_func", func(call goja.FunctionCall) goja.Value {
		funcName := jsStringParam(vm, call, 0, "user.function")
		params := jsObjectParam(vm, call, 1, true, "user.function")
		sudo := jsBoolParam(call, 2)
		result, err := runner.do_func(runner.ctx, funcName, params, sudo)
		return jsJSONResult(vm, result, err)
	})

	// Implementation of the 'get(docID)' callback:
	runner.DefineNativeFunction("_get", func(call goja.FunctionCall) goja.Value {
		docID := jsStringParam(vm, call, 0, "user.get")
		sudo := jsBoolParam(call, 1)
		doc, err := runner.do_get(docID, nil, sudo)
		return jsJSONResult(vm, doc, err)
	})

	// Implementation of the 'save(doc,docID?)' callback:
	runner.DefineNativeFunction("_save", func(call goja.FunctionCall) goja.Value {
		doc := jsObjectParam(vm, call, 0, false, "user.save")
		docID := jsOptionalStringParam(vm, call, 1, "user.save")
		sudo := jsBoolParam(call, 2)
		docID, err := runner.do_save(doc, docID, sudo)
		return jsResult(vm, docID, err)
	})

	// Implementation of the '_requireMutating()' callback:
	runner.DefineNativeFunction("_requireMutating", func(call goja.FunctionCall) goja.Value {
		err := runner.checkMutationAllowed("requireMutating")
		return jsResult(vm, nil, err)
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
func (runner *jsRunner) do_func(ctx context.Context, funcName string, params map[string]any, sudo bool) (any, error) {
	if sudo {
		exitSudo := runner.enterSudo()
		ctx = runner.ctx
		defer exitSudo()
	}
	return runner.currentDB.CallUserFunction(ctx, funcName, params, true)
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
	ctx := collection.AddCollectionContext(runner.ctx)
	if err != nil {
		return nil, err
	}
	rev, err := collection.GetRev(ctx, docID, "", false, nil)
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
	ctx := collection.AddCollectionContext(runner.ctx)
	if _, found := body["_rev"]; found {
		// If caller provided `_rev` property, use MVCC as normal:
		_, _, err := collection.Put(ctx, docID, body)
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
			rev, err := collection.GetRev(ctx, docID, "", false, []string{})
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

			_, _, err = collection.Put(ctx, docID, body)
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

//////// JS UTILITIES:

// makeJSError builds a JS Error object whose `name` is `name` and whose message is `msg`,
// so that its rendered form (via Error.prototype.toString(), i.e. "name: message") matches
// what javaScriptRunner.convertError expects to parse back apart into a Go error.
func makeJSError(vm *goja.Runtime, name string, msg string) *goja.Object {
	err := vm.NewGoError(errors.New(msg))
	_ = err.Set("name", name)
	return err
}

// Returns a parameter of `call` as a Go bool.
func jsBoolParam(call goja.FunctionCall, arg int) bool {
	return call.Argument(arg).ToBoolean()
}

// Returns a parameter of `call` as a Go string, or throws a JS exception if it's not a string.
func jsStringParam(vm *goja.Runtime, call goja.FunctionCall, arg int, what string) string {
	val := call.Argument(arg)
	if _, isString := val.(goja.String); !isString {
		panic(vm.NewTypeError(fmt.Sprintf("%s() param %d must be a string", what, arg+1)))
	}
	return val.String()
}

// Returns a parameter of `call` as a Go string or nil.
func jsOptionalStringParam(vm *goja.Runtime, call goja.FunctionCall, arg int, what string) *string {
	val := call.Argument(arg)
	if _, isString := val.(goja.String); isString {
		return base.Ptr(val.String())
	} else if goja.IsNull(val) || goja.IsUndefined(val) {
		return nil
	} else {
		panic(vm.NewTypeError(fmt.Sprintf("%s() param %d must be a string or null", what, arg+1)))
	}
}

// Returns a parameter of `call` as a Go map, or throws a JS exception if it's not a map.
// If `optional` is true, the parameter is allowed not to exist, in which case `nil` is returned.
func jsObjectParam(vm *goja.Runtime, call goja.FunctionCall, arg int, optional bool, what string) map[string]any {
	val := call.Argument(arg)
	obj, isObject := val.(*goja.Object)
	if !isObject {
		if optional && goja.IsUndefined(val) {
			return nil
		}
		panic(vm.NewTypeError(fmt.Sprintf("%s() param %d must be an object", what, arg+1)))
	}
	exported, ok := sgbucket.ExportValue(obj).(map[string]any)
	if !ok {
		panic(vm.NewTypeError("Yikes, couldn't export JS value"))
	}
	return exported
}

// Returns `result` back to JS; or if `err` is non-nil, "throws" it via a Go panic
func jsResult(vm *goja.Runtime, result any, err error) goja.Value {
	if err == nil {
		return sgbucket.ToJSValue(vm, result)
	} else {
		// (javaScriptRunner.convertError clumsily takes these apart back into errors)
		if status, msg := base.ErrorAsHTTPStatus(err); status != 500 && status != 200 {
			panic(makeJSError(vm, "HTTP", fmt.Sprintf("%d %s", status, msg)))
		} else {
			panic(makeJSError(vm, "Go", err.Error()))
		}
	}
}

// Returns `result` back to JS in JSON form; or if `err` is non-nil, "throws" it via a Go panic
func jsJSONResult(vm *goja.Runtime, result any, err error) goja.Value {
	if err == nil && result != nil {
		if j, err := json.Marshal(result); err == nil {
			return vm.ToValue(string(j))
		}
	}
	return jsResult(vm, result, err)
}
