//go:build cb_sg_v8

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
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/couchbase/sg-bucket/js"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	v8 "github.com/couchbasedeps/v8go" // Docs: https://pkg.go.dev/github.com/couchbasedeps/v8go
)

// The JavaScript code run in a context, that defines the API, embedded in a string constant.
// `main.js` is generated by WebPack from the TypeScript sources; see engine/webpack.config.js.
//
//go:embed engine/dist/main.js
var kJavaScriptCode string

// Used for JSON-encoding configuration for engine.
// Equivalent to TypeScript type `Config` in types.ts
type jsConfig struct {
	Functions *FunctionsConfig `json:"functions,omitempty"`
	GraphQL   *GraphQLConfig   `json:"graphql,omitempty"`
}

// js.Template implementation; a "subclass" of js.BasicTemplate.
type functionServiceTemplate struct {
	*js.V8BasicTemplate                    // "Superclass"
	upstreamTemplate    *v8.ObjectTemplate // Template of object holding JS callbacks
	jsonConfig          *v8.Value          // JSON configuration string
}

// Returns a ServiceFactory function that instantiates a "functions" js.Service
func makeService(functions *FunctionsConfig, graphql *GraphQLConfig) js.TemplateFactory {
	return func(base *js.V8BasicTemplate) (js.V8Template, error) {
		// Initialize the BaseService and compile the JS code:
		if err := base.SetScript(kJavaScriptCode + "\n SG_Engine.main;"); err != nil {
			return nil, err
		}

		// Create the top-level 'process' namespace that Apollo's libraries expect:
		process := base.NewObjectTemplate()
		if err := process.Set("env", base.NewObjectTemplate()); err != nil {
			return nil, err
		} else if err := base.Global().Set("process", process); err != nil {
			return nil, err
		}

		// Create the template for the "upstream" object containing the callbacks to Go:
		upstream := base.NewObjectTemplate()
		if err := upstream.Set("query", base.NewCallback(doQuery)); err != nil {
			return nil, err
		} else if err := upstream.Set("get", base.NewCallback(doGet)); err != nil {
			return nil, err
		} else if err := upstream.Set("save", base.NewCallback(doSave)); err != nil {
			return nil, err
		} else if err := upstream.Set("delete", base.NewCallback(doDelete)); err != nil {
			return nil, err
		}

		// Create a JS string of the configuration JSON:
		jsonConfig, err := json.Marshal(jsConfig{Functions: functions, GraphQL: graphql})
		if err != nil {
			return nil, err
		}
		jsonConfigVal, err := base.NewString(string(jsonConfig))
		if err != nil {
			return nil, err
		}

		// Wrap the service in a js.Service implementation:
		return &functionServiceTemplate{
			V8BasicTemplate:  base,
			upstreamTemplate: upstream,
			jsonConfig:       jsonConfigVal,
		}, nil
	}
}

//////// EVALUATOR

// A JavaScript execution context in an Environment. This is what actually does the work.
// It wraps a js.Runner that was instantiated from the functionService.
// **Not thread-safe! Must be called only on one goroutine at a time. In fact, all Evaluators created from the same Environment must be called only one one goroutine.**
type evaluator struct {
	runner          *js.V8Runner      // The V8 context
	api             *v8.Object        // JavaScript `API` object (see api.ts)
	functionFn      *v8.Function      // JavaScript `API.callFunction()` function
	graphqlFn       *v8.Function      // JavaScript `API.graphql()` function
	delegate        evaluatorDelegate // Provides native callbacks like the CRUD API
	user            *userCredentials  // Name & capabilities of the calling user
	mutationAllowed bool              // Is the Evaluator allowed to make mutating calls?
}

// Pluggable implementation of native functionality the JS code calls -- CRUD, query, logging.
type evaluatorDelegate interface {
	// Return an error if evaluation has gone on too long.
	checkTimeout() error

	// Perform a N1QL query.
	query(fnName string, n1ql string, args map[string]any, asAdmin bool) (rowsAsJSON string, err error)

	// Get a document.
	get(docID string, collection string, asAdmin bool) (doc map[string]any, err error)
	// Save/create a document.
	save(doc map[string]any, docID string, collection string, asAdmin bool) (saved bool, err error)
	// Delete a document.
	delete(docID string, revID string, collection string, asAdmin bool) (ok bool, err error)
}

// Returns an evaluator for use with a Service.
func makeEvaluator(service *js.Service, dbc *db.Database, delegate evaluatorDelegate, user *userCredentials, ctx context.Context) (*evaluator, error) {
	runner, err := service.GetRunner()
	if err != nil {
		return nil, err
	}
	v8Runner := runner.(*js.V8Runner)
	if v8Runner == nil {
		panic("functions/graphql only supports V8 engine")
	}

	if dbc.Options.JavascriptTimeout > 0 {
		var cancelFn context.CancelFunc
		ctx, cancelFn = context.WithTimeout(ctx, dbc.Options.JavascriptTimeout)
		defer cancelFn()
	}
	runner.SetContext(ctx)

	eval, ok := v8Runner.Client.(*evaluator)
	if !ok {
		if eval, err = newEvaluator(v8Runner); err != nil {
			runner.Return()
			return nil, err
		}
	}
	eval.setup(delegate, user)
	return eval, nil
}

// Creates an Evaluator, wrapping a js.V8Runner.
// Usually it's better to call makeEvaluator since that will reuse an existing instance.
func newEvaluator(runner *js.V8Runner) (*evaluator, error) {
	eval := &evaluator{runner: runner}
	runner.Client = eval

	// Call the JS initialization code, passing it the configuration and the Upstream.
	// This returns the JS `API` object.
	fnService := runner.Template().(*functionServiceTemplate)
	if fnService == nil {
		panic("Couldn't cast to functionService")
	}
	upstream, err := runner.NewInstance(fnService.upstreamTemplate)
	if err != nil {
		return nil, err
	}
	eval.api, err = runner.RunAsObject(fnService.jsonConfig, upstream)
	if err != nil {
		return nil, err
	}

	// Check the API.errors property for configuration errors:
	if errorsVal, err := eval.api.Get("errors"); err != nil {
		return nil, err
	} else if errorArray, _ := errorsVal.AsArray(); errorArray != nil {
		var errors *base.MultiError
		len := errorArray.Length()
		for i := uint32(0); i < len; i++ {
			if errorVal, err := errorArray.GetIdx(i); err == nil {
				errors = errors.Append(fmt.Errorf(errorVal.String()))
			}
		}
		return nil, errors
	}

	eval.functionFn = mustGetV8Fn(eval.api, "callFunction")
	eval.graphqlFn = mustGetV8Fn(eval.api, "graphql")
	return eval, nil
}

func (eval *evaluator) setup(delegate evaluatorDelegate, user *userCredentials) {
	if delegate == nil {
		panic("nil delegate!")
	}
	eval.delegate = delegate
	eval.user = user
}

// Call this when finished using an evaluator.
func (eval *evaluator) close() {
	eval.delegate = nil
	eval.user = nil
	eval.runner.Return() // This must be the last thing the evaluator does
}

// Configures whether the Evaluator is allowed to mutate the database; if false (the default), calls to `save()` and `delete()` will fail.
func (eval *evaluator) setMutationAllowed(allowed bool) {
	eval.mutationAllowed = allowed
}

// Calls a named function. Result is returned as JSON data.
func (eval *evaluator) callFunction(name string, args map[string]any) ([]byte, error) {
	return eval.wrapCall(func() (*v8.Value, error) {
		user, roles, channels, err := eval.v8Credentials()
		if err != nil {
			return nil, err
		}
		jsonArgs, err := eval.runner.NewJSONString(args)
		if err != nil {
			return nil, err
		}
		args, err := eval.runner.ConvertArgs(name, jsonArgs, user, roles, channels, eval.mutationAllowed)
		if err != nil {
			return nil, err
		}
		// Calling JS method API.callFunction (api.ts)
		return eval.runner.Call(eval.functionFn, eval.api, args...)
	})
}

// Performs a GraphQL query. Result is an object of the usual GraphQL result shape, i.e. with `data` and/or `errors` properties. It is returned as a JSON string.
func (eval *evaluator) callGraphQL(query string, operationName string, variables map[string]any) ([]byte, error) {
	return eval.wrapCall(func() (*v8.Value, error) {
		user, roles, channels, err := eval.v8Credentials()
		if err != nil {
			return nil, err
		}
		jsonVariables, err := eval.runner.NewJSONString(variables)
		if err != nil {
			return nil, err
		}
		args, err := eval.runner.ConvertArgs(query, operationName, jsonVariables, user, roles, channels, eval.mutationAllowed)
		if err != nil {
			return nil, err
		}
		// Calling JS method API.callGraphQL (api.ts)
		return eval.runner.Call(eval.graphqlFn, eval.api, args...)
	})
}

// wraps a callback that makes a JS call. Converts the return value and error appropriately,
// and uses WithScopedValues so temporary values get cleaned up.
func (eval *evaluator) wrapCall(fn func() (*v8.Value, error)) (result []byte, err error) {
	eval.runner.WithTemporaryValues(func() {
		if v8Result, v8Err := fn(); v8Err == nil {
			if resultStr, ok := js.StringToGo(v8Result); ok {
				result = []byte(resultStr)
			}
		} else {
			err = unpackError(v8Err)
		}
	})
	return
}

// Encodes credentials as 3 parameters to pass to JS.
func (eval *evaluator) v8Credentials() (user *v8.Value, roles *v8.Value, channels *v8.Value, err error) {
	undef := eval.runner.UndefinedValue()
	if eval.user != nil {
		user, err = eval.runner.NewString(eval.user.Name)
		if err != nil {
			return
		}
	} else {
		user = undef
	}
	if eval.user != nil && len(eval.user.Roles) > 0 {
		roles, err = eval.runner.NewString(strings.Join(eval.user.Roles, ","))
		if err != nil {
			return
		}
	} else {
		roles = undef
	}
	if eval.user != nil && len(eval.user.Channels) > 0 {
		channels, err = eval.runner.NewString(strings.Join(eval.user.Channels, ","))
		if err != nil {
			return
		}
	} else {
		channels = undef
	}
	return
}

//////// EVALUATOR CALLBACK IMPLEMENTATIONS:

// query(fnName: string, n1ql: string, argsJSON: string | undefined, asAdmin: boolean) : string;
func doQuery(r *js.V8Runner, this *v8.Object, args []*v8.Value) (any, error) {
	fnName := args[0].String()
	n1ql := args[1].String()
	fnArgs, err := jsonToGoMap(args[2])
	if err != nil {
		return nil, err
	}
	asAdmin := args[3].Boolean()
	eval := r.Client.(*evaluator)
	return eval.delegate.query(fnName, n1ql, fnArgs, asAdmin)
}

// get(docID: string, collection: string, asAdmin: boolean) : string | null;
func doGet(r *js.V8Runner, this *v8.Object, args []*v8.Value) (any, error) {
	docID := args[0].String()
	collection := args[1].String()
	asAdmin := args[2].Boolean()
	eval := r.Client.(*evaluator)
	return returnAsJSON(eval.delegate.get(docID, collection, asAdmin))
}

// save(docJSON: string, docID: string | undefined, collection: string, asAdmin: boolean) : string | null;
func doSave(r *js.V8Runner, this *v8.Object, args []*v8.Value) (any, error) {
	var docID string
	doc, err := jsonToGoMap(args[0])
	if err != nil {
		return nil, err
	}
	if arg1 := args[1]; arg1.IsString() {
		docID = arg1.String()
	} else if _id, found := doc["_id"].(string); found {
		docID = _id
	} else {
		docID, err = base.GenerateRandomID()
		if err != nil {
			return nil, err
		}
	}
	collection := args[2].String()
	asAdmin := args[3].Boolean()

	eval := r.Client.(*evaluator)
	if saved, err := eval.delegate.save(doc, docID, collection, asAdmin); saved && err == nil {
		return docID, nil
	} else {
		return nil, err
	}
}

// delete(docID: string, revID: string | undefined, collection: string, asAdmin: boolean) : boolean;
func doDelete(r *js.V8Runner, this *v8.Object, args []*v8.Value) (any, error) {
	docID := args[0].String()
	var revID string
	if arg1 := args[1]; arg1.IsString() {
		revID = arg1.String()
	}
	collection := args[2].String()
	asAdmin := args[3].Boolean()
	eval := r.Client.(*evaluator)
	return eval.delegate.delete(docID, revID, collection, asAdmin)
}

//////// UTILITIES

// Simple utility to wrap a function that returns a value and an error; returns just the value, panicking if there was an error.
// This is kind of equivalent to those 3-prong to 2-prong electric plug adapters...
// Needless to say, it should only be used if you know the error cannot occur, or that if it occurs something is very, very wrong.
func mustSucceed[T any](result T, err error) T {
	if err != nil {
		log.Fatalf("ASSERTION FAILURE: expected a %T, got %v", result, err)
	}
	return result
}

func mustNoError(err error) {
	if err != nil {
		log.Fatalf("ASSERTION FAILURE: unexpected error %v", err)
	}
}

// Gets a named method of a JS object. Panics if not found.
func mustGetV8Fn(owner *v8.Object, name string) *v8.Function {
	fnVal := mustSucceed(owner.Get(name))
	return mustSucceed(fnVal.AsFunction())
}

// Converts a result and error to a JSON-encoded string and error.
// - If `err` is non-nil on input, just passes it through.
// - Otherwise it JSON-encodes the `result` and returns it.
func returnAsJSON(result any, err error) (any, error) {
	if result == nil || err != nil {
		return nil, err
	} else if jsonBytes, err := json.Marshal(result); err != nil {
		return nil, err
	} else {
		return string(jsonBytes), nil
	}
}

// Converts & parses a JS string of JSON into a Go map.
func jsonToGoMap(val *v8.Value) (map[string]any, error) {
	if val.IsNullOrUndefined() {
		return nil, nil
	} else if jsonStr, ok := js.StringToGo(val); !ok {
		return nil, fmt.Errorf("unexpected error: instead of a string, got %s", val.DetailString())
	} else {
		var result map[string]any
		err := json.Unmarshal([]byte(jsonStr), &result)
		return result, err
	}
}

// (This detects the way HTTP statuses are encoded into error messages by the class HTTPError in types.ts.)
var kHTTPErrRegexp = regexp.MustCompile(`^(Error:\s*)?\[(\d\d\d)\]\s+(.*)`)

// Looks for an HTTP status in an error message and returns it as an HTTPError; else returns nil.
func unpackErrorStr(jsErrMessage string) error {
	if m := kHTTPErrRegexp.FindStringSubmatch(jsErrMessage); m != nil {
		if status, err := strconv.ParseUint(m[2], 10, 16); err == nil {
			return &base.HTTPError{Status: int(status), Message: m[3]}
		}
	}
	return nil
}

// Postprocesses an error returned from running JS code, detecting the HTTPError type defined in types.ts. If the error is a v8.JSError, looks for an HTTP status in brackets at the start of the message; if found, returns a base.HTTPError.
func unpackError(err error) error {
	if jsErr, ok := err.(*v8.JSError); ok {
		if unpackedErr := unpackErrorStr(jsErr.Message); unpackedErr != nil {
			return unpackedErr
		}
	}
	return err
}
