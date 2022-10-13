package functions

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/couchbase/sync_gateway/db"
	v8 "rogchap.com/v8go"
)

/* TypeScript Upstream interface:
export interface NativeAPI {
	query(fnName: string,
		n1ql: string,
		argsJSON: string | undefined,
		asAdmin: boolean) : string;
	get(docID: string,
		asAdmin: boolean) : string | null;
	save(docJSON: string,
		docID: string,
		asAdmin: boolean) : string;
	delete(docID: string,
		revID: string | undefined,
		asAdmin: boolean) : boolean;
}

// TypeScript exported API:
export class API {
	callFunction(name: string,
				argsJSON: string | undefined,
				user: string | undefined,
				roles?: string[],
				channels?: string[]) : Promise<string>;
	graphql(query: string,
			variablesJSON: string | undefined,
			user: string | undefined,
			roles?: string[],
			channels?: string[]) : Promise<string>;
}
*/

// The interface the JS code calls -- provides query and CRUD operations.
type FunctionizerDelegate interface {
	query(fnName string, n1ql string, args map[string]any, asAdmin bool) (rows []any, err error)
	get(docID string, asAdmin bool) (doc map[string]any, err error)
	save(docID string, doc map[string]any, asAdmin bool) (revID string, err error)
	delete(docID string, doc map[string]any, asAdmin bool) (err error)
}

type Functionizer struct {
	delegate         FunctionizerDelegate
	vm               *v8.Isolate        // A V8 virtual machine. NOT THREAD SAFE.
	global           *v8.ObjectTemplate // The global namespace (a template)
	script           *v8.UnboundScript  // The compiled JS code; a template run in each new context
	jsNativeTemplate *v8.ObjectTemplate
}

func NewFunctionizer(sourceCode string, sourceFilename string, delegate FunctionizerDelegate) (*Functionizer, error) {
	vm := v8.NewIsolate()
	fnz := &Functionizer{
		delegate: delegate,
		vm:       vm,
		global:   v8.NewObjectTemplate(vm),
	}
	var err error
	fnz.script, err = vm.CompileUnboundScript(sourceCode, sourceFilename, v8.CompileOptions{})
	if err != nil {
		return nil, err
	}

	// Create the JS "NativeAPI" object template, with Go callback methods:
	fnz.jsNativeTemplate = v8.NewObjectTemplate(vm)
	fnz.jsNativeTemplate.SetInternalFieldCount(1)

	fnz.defineFunction(fnz.jsNativeTemplate, "query", func(info *v8.FunctionCallbackInfo) *v8.Value {
		var rows []any
		fnName := info.Args()[0].String()
		n1ql := info.Args()[1].String()
		args, err := v8JSONToGo(info.Args()[2])
		if err == nil {
			asAdmin := info.Args()[3].Boolean()
			rows, err = fnz.delegate.query(fnName, n1ql, args, asAdmin)
		}
		return returnJSONToV8(info, rows, err)
	})
	fnz.defineFunction(fnz.jsNativeTemplate, "get", func(info *v8.FunctionCallbackInfo) *v8.Value {
		return v8Throw(vm, fmt.Errorf("get is unimplemented")) //TODO
	})
	fnz.defineFunction(fnz.jsNativeTemplate, "save", func(info *v8.FunctionCallbackInfo) *v8.Value {
		return v8Throw(vm, fmt.Errorf("save is unimplemented")) //TODO
	})
	fnz.defineFunction(fnz.jsNativeTemplate, "delete", func(info *v8.FunctionCallbackInfo) *v8.Value {
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
	ctx       *v8.Context
	iso       *v8.Isolate
	api       *v8.Object
	graphqlFn *v8.Function

	dbc *db.Database
}

func (fnz *Functionizer) NewContext(dbc *db.Database) (*FunContext, error) {
	// Create a context and run the script in it, returning the JS initializer function:
	ctx := v8.NewContext(fnz.vm, fnz.global)
	result, err := fnz.script.Run(ctx)
	if err != nil {
		return nil, err
	}

	fct := &FunContext{
		ctx: ctx,
		iso: fnz.vm,
		dbc: dbc,
	}

	initFn, err := result.AsFunction()
	if err != nil {
		return nil, err
	}

	// Instantiate my Upstream object with the native callbacks.
	// The object's internal field 0 points to the FunContext.
	upstream, err := fnz.jsNativeTemplate.NewInstance(ctx)
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

// Performs a GraphQL query. Returns a JSON string.
func (fnc *FunContext) CallGraphql(query string, operationName string, variables map[string]any) (string, error) {
	result, err := fnc.graphqlFn.Call(fnc.api,
		goToV8String(fnc.iso, query),
		goToV8String(fnc.iso, operationName),
		mustSucceed(goToV8JSON(fnc.ctx, variables)),
		fnc.makeCredentials())
	if err != nil {
		return "", err
	}
	return result.String(), nil
}

// Calls a named function. Returns a JSON string.
func (fnc *FunContext) CallFunction(name string, args map[string]any) (string, error) {
	result, err := fnc.graphqlFn.Call(fnc.api,
		goToV8String(fnc.iso, name),
		mustSucceed(goToV8JSON(fnc.ctx, args)),
		fnc.makeCredentials())
	if err != nil {
		return "", err
	}
	return result.String(), nil
}

func (fnc *FunContext) makeCredentials() *v8.Value {
	if user := fnc.dbc.User(); user != nil {
		credentials := []any{
			user.Name(),
			user.Channels().AllKeys(),
			user.RoleNames().AllKeys(),
		}
		jsonCred := mustSucceed(json.Marshal(credentials))
		return goToV8String(fnc.iso, string(jsonCred))
	} else {
		return v8.Undefined(fnc.iso)
	}
}

//////// UTILITIES

func goToV8String(i *v8.Isolate, str string) *v8.Value {
	return mustSucceed(v8.NewValue(i, str))
}

// Converts a V8 object represented as a Value into a Go map.
func v8JSONToGo(val *v8.Value) (map[string]any, error) {
	var err error
	jsonStr := val.String()
	var result map[string]any
	if err = json.Unmarshal([]byte(jsonStr), &result); err == nil {
		return result, nil
	}
	return nil, err
}

func goToV8JSON(ctx *v8.Context, obj any) (*v8.Value, error) {
	var err error
	if obj == nil {
		return nil, nil
	}
	if jsonBytes, err := json.Marshal(obj); err != nil {
		return v8.NewValue(ctx.Isolate(), string(jsonBytes))
	}
	return nil, err
}

func returnJSONToV8(info *v8.FunctionCallbackInfo, result any, err error) *v8.Value {
	if err == nil {
		if v8obj, err := goToV8JSON(info.Context(), result); err == nil {
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
