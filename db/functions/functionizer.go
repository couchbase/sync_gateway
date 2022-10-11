package functions

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/couchbase/sync_gateway/db"
	v8 "rogchap.com/v8go"
)

/* TypeScript Upstream interface:
export interface Upstream {
	query(fnName: string,
		n1ql: string,
		args: Args | undefined,
		user: User) : Promise<any[]>;
	get(docID: string, user: User) : Promise<object | null>;
	save(docID: string, doc: object, user: User) : Promise<string>;
	delete(docID: string, user: User) : Promise<boolean>;
} */

/* TypeScript Database (API) interface:
export type Credentials = [string, string[], string[]];
export interface Database {
    // Creates an execution context given a user's name, roles and channels.
    makeContext(credentials: Credentials | null) : Context;

    // Calls a named function.
    callFunction(name: string, args: Args | undefined, credentials: Credentials | null) : Promise<any>;

    // Runs a N1QL query. Called by functions of type "query".
    query(fnName: string, n1ql: string, args: Args | undefined, context: Context) : Promise<any[]>;

    // Runs a GraphQL query.
    graphql(query: string, args: Args | undefined, context: Context) : Promise<gq.ExecutionResult>;

    // The compiled GraphQL schema.
    readonly schema?: gq.GraphQLSchema;
} */

// The interface the JS code calls -- provides query and CRUD operations.
type Upstream interface {
	query(fnName string, n1ql string, args map[string]any, asAdmin bool) (rows []any, err error)
	get(docID string, asAdmin bool) (doc map[string]any, err error)
	save(docID string, doc map[string]any, asAdmin bool) (revID string, err error)
	delete(docID string, doc map[string]any) (err error)
}

type Functionizer struct {
	upstream           Upstream
	vm                 *v8.Isolate        // A V8 virtual machine. NOT THREAD SAFE.
	global             *v8.ObjectTemplate // The global namespace (a template)
	script             *v8.UnboundScript  // The compiled JS code; a template run in each new context
	jsUpstreamTemplate *v8.ObjectTemplate
}

func NewFunctionizer(sourceCode string, sourceFilename string, upstream Upstream) (*Functionizer, error) {
	vm := v8.NewIsolate()
	fnz := &Functionizer{
		upstream: upstream,
		vm:       vm,
		global:   v8.NewObjectTemplate(vm),
	}
	var err error
	fnz.script, err = vm.CompileUnboundScript(sourceCode, sourceFilename, v8.CompileOptions{})
	if err != nil {
		return nil, err
	}

	// Create the JS "upstream" object template, with Go callback methods:
	fnz.jsUpstreamTemplate = v8.NewObjectTemplate(vm)
	fnz.jsUpstreamTemplate.SetInternalFieldCount(1)

	fnz.defineFunction(fnz.jsUpstreamTemplate, "query", func(info *v8.FunctionCallbackInfo) *v8.Value {
		var rows []any
		fnName := info.Args()[0].String()
		n1ql := info.Args()[1].String()
		args, err := v8ObjectToGo(info.Args()[2])
		if err == nil {
			asAdmin := info.Args()[3].Boolean()
			rows, err = fnz.upstream.query(fnName, n1ql, args, asAdmin)
		}
		return returnObjectToV8(info, rows, err)
	})
	fnz.defineFunction(fnz.jsUpstreamTemplate, "get", func(info *v8.FunctionCallbackInfo) *v8.Value {
		return v8Throw(vm, fmt.Errorf("get is unimplemented")) //TODO
	})
	fnz.defineFunction(fnz.jsUpstreamTemplate, "save", func(info *v8.FunctionCallbackInfo) *v8.Value {
		return v8Throw(vm, fmt.Errorf("save is unimplemented")) //TODO
	})
	fnz.defineFunction(fnz.jsUpstreamTemplate, "delete", func(info *v8.FunctionCallbackInfo) *v8.Value {
		return v8Throw(vm, fmt.Errorf("delete is unimplemented")) //TODO
	})

	return fnz, nil
}

func (fnz *Functionizer) Close() {
	fnz.vm.Dispose()
}

func (fnz *Functionizer) defineFunction(owner *v8.ObjectTemplate, name string, callback v8.FunctionCallback) {
	fn := v8.NewFunctionTemplate(fnz.vm, callback)
	owner.Set(name, fn, v8.ReadOnly)
}

//////// CONTEXT:

type FunContext struct {
	fnz       *Functionizer
	ctx       *v8.Context
	api       *v8.Object
	graphqlFn *v8.Function

	dbc *db.Database
}

func (fnz *Functionizer) newContext(dbc *db.Database) (*FunContext, error) {
	// Create a context and run the script in it, returning the JS initializer function:
	ctx := v8.NewContext(fnz.vm, fnz.global)
	result, err := fnz.script.Run(ctx)
	if err != nil {
		return nil, err
	}

	fct := &FunContext{
		fnz: fnz,
		ctx: ctx,
		dbc: dbc,
	}

	initFn, err := result.AsFunction()
	if err != nil {
		return nil, err
	}

	// Instantiate my Upstream object with the native callbacks.
	// The object's internal field 0 points to the FunContext.
	upstream, err := fnz.jsUpstreamTemplate.NewInstance(ctx)
	if err != nil {
		return nil, err
	}
	upstream.SetInternalField(0, fct)

	// Call the JS initializer, passing it the Upstream, to get back the API object:
	apiVal, err := initFn.Call(nil, upstream)
	if err != nil {
		return nil, err
	}
	fct.api = mustSucceed(apiVal.AsObject())
	fct.graphqlFn = mustGetV8Fn(fct.api, "graphql")

	return fct, nil
}

func (fnc *FunContext) newFnContext() (*v8.Object, error) {

}

// Invokes a GraphQL query
func (fnc *FunContext) graphql(query string, operationName string, variables map[string]any, mutationAllowed bool) {
	result, err := fnc.graphqlFn.Call(fnc.api,
		goToV8String(fnc.ctx.Isolate(), query),
		goToV8String(fnc.ctx.Isolate(), operationName),
		mustSucceed(goToV8Object(fnc.ctx, variables)),
		mustSucceed(fnc.newFnContext()))
}

//////// UTILITIES

func goToV8String(i *v8.Isolate, str string) *v8.Value {
	return mustSucceed(v8.NewValue(i, str))
}

// Converts a V8 object represented as a Value into a Go map.
func v8ObjectToGo(val *v8.Value) (map[string]any, error) {
	var err error
	if obj, err := val.AsObject(); err == nil {
		if jsonBytes, err := obj.MarshalJSON(); err == nil {
			var result map[string]any
			if err = json.Unmarshal(jsonBytes, &result); err == nil {
				return result, nil
			}
		}
	}
	return nil, err
}

func goToV8Object(ctx *v8.Context, obj any) (*v8.Value, error) {
	var err error
	if jsonBytes, err := json.Marshal(obj); err == nil {
		if val, err := v8.JSONParse(ctx, string(jsonBytes)); err == nil {
			return val, nil
		}
	}
	return nil, err
}

func returnObjectToV8(info *v8.FunctionCallbackInfo, result any, err error) *v8.Value {
	if err == nil {
		if v8obj, err := goToV8Object(info.Context(), result); err == nil {
			return v8obj
		}
	}
	return v8Throw(info.Context().Isolate(), err)
}

func v8Throw(i *v8.Isolate, err error) *v8.Value {
	return i.ThrowException(goToV8String(i, err.Error()))
}

func mustGetV8Fn(owner *v8.Object, name string) *v8.Function {
	fnVal := mustSucceed(owner.Get(name))
	return mustSucceed(fnVal.AsFunction())
}

func mustSucceed[T any](result T, err error) T {
	if err != nil {
		log.Fatalf("ASSERTION FAILURE: expected a %T, got %v", result, err)
	}
	return result
}

func assertNoErr(err error, what string, args ...any) {
	if err != nil {
		log.Fatalf("ASSERTION FAILURE: %s (error: %v)", fmt.Sprintf(what, args...), err) //TEMP
	}
}
