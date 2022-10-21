package functions

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/couchbase/sync_gateway/base"
	v8 "rogchap.com/v8go"
)

// V8 docs: https://pkg.go.dev/rogchap.com/v8go#Promise

/* TypeScript Upstream interface:
export interface NativeAPI {
	query(fnName: string, n1ql: string, argsJSON: string | undefined, asAdmin: boolean) : string;
	get(docID: string, asAdmin: boolean) : string | null;
	save(docJSON: string, docID: string, asAdmin: boolean) : string;
	delete(docID: string, revID: string | undefined, asAdmin: boolean) : boolean;
}

// TypeScript exported API:
export class API {
	callFunction(name: string,
				argsJSON: string | undefined,
				user?: string,
				roles?: string,
				channels?: string,
				mutationAllowed: boolean) : strong | Promise<string>;
	graphql(query: string,
			variablesJSON: string | undefined,
			user?: string,
			roles?: string,
			channels?: string,
            mutationAllowed: boolean) : Promise<string>;
}

export type Config = {
    functions?: FunctionsConfig;
    graphql?:   GraphQLConfig;
}

*/

// The JavaScript code run in a context, that defines the API.
//
//go:embed engine/dist/main.js
var kJavaScriptCode string

type jsConfig struct {
	Functions *FunctionsConfig `json:"functions,omitempty"`
	Graphql   *GraphQLConfig   `json:"graphql,omitempty"`
}

// Represents a V8 JavaScript VM.
type Environment struct {
	jsonConfig       *v8.Value          // JSON-encoded functions/graphql config
	vm               *v8.Isolate        // A V8 virtual machine. NOT THREAD SAFE.
	global           *v8.ObjectTemplate // The global namespace (a template)
	script           *v8.UnboundScript  // Compiled JS code; template run in each new context
	jsNativeTemplate *v8.ObjectTemplate // Template of JS "NativeAPI" object
	curEvaluator     *Evaluator
}

func NewEnvironment(functions *FunctionsConfig, graphql *GraphQLConfig) (*Environment, error) {
	// Encode the config as JSON:
	jsonConfig, err := json.Marshal(jsConfig{functions, graphql})
	if err != nil {
		return nil, err
	}

	// Create a V8 VM ("isolate"):
	vm := v8.NewIsolate()
	env := &Environment{
		vm:         vm,
		global:     v8.NewObjectTemplate(vm),
		jsonConfig: goToV8String(vm, string(jsonConfig)),
	}

	// Compile the engine's JS code:
	env.script, err = vm.CompileUnboundScript(kJavaScriptCode+"\n SG_Engine.main;", "main.js", v8.CompileOptions{})
	if err != nil {
		return nil, err
	}

	// Create the JS "NativeAPI" object template, with Go callback methods:
	env.jsNativeTemplate = v8.NewObjectTemplate(vm)
	env.defineMethod(env.jsNativeTemplate, "query", doQuery)
	env.defineMethod(env.jsNativeTemplate, "get", doGet)
	env.defineMethod(env.jsNativeTemplate, "save", doSave)
	env.defineMethod(env.jsNativeTemplate, "delete", doDelete)
	env.defineMethod(env.jsNativeTemplate, "log", doLog)

	return env, nil
}

func (env *Environment) Close() {
	env.vm.Dispose()
}

func (env *Environment) getEvaluator(ctx *v8.Context) *Evaluator {
	if env.curEvaluator == nil {
		panic(fmt.Sprintf("Unknown v8.Context passed to Environment.getEvaluator: %v, expected none", ctx))
	}
	if ctx != env.curEvaluator.ctx {
		panic(fmt.Sprintf("Unknown v8.Context passed to Environment.getEvaluator: %v, expected %v", ctx, env.curEvaluator.ctx))
	}
	return env.curEvaluator
}

type v8Method = func(*Evaluator, *v8.FunctionCallbackInfo) (*v8.Value, error)

func (env *Environment) defineMethod(owner *v8.ObjectTemplate, name string, callback v8Method) {
	env.defineFunction(owner, name, func(info *v8.FunctionCallbackInfo) *v8.Value {
		eval := env.getEvaluator(info.Context())
		if result, err := callback(eval, info); err == nil {
			return result
		} else {
			return v8Throw(info.Context().Isolate(), err)
		}
	})
}

func (env *Environment) defineFunction(owner *v8.ObjectTemplate, name string, callback v8.FunctionCallback) {
	fn := v8.NewFunctionTemplate(env.vm, callback)
	owner.Set(name, fn, v8.ReadOnly)
}

//
//////// EVALUATOR:
//

// The interface the JS code calls -- provides query and CRUD operations.
type EvaluatorDelegate interface {
	query(fnName string, n1ql string, args map[string]any, asAdmin bool) (rows []any, err error)
	get(docID string, asAdmin bool) (doc map[string]any, err error)
	save(doc map[string]any, docID string, asAdmin bool) (saved bool, err error)
	delete(docID string, revID string, asAdmin bool) (ok bool, err error)
	log(level base.LogLevel, message string)
}

type UserCredentials struct {
	Name     string
	Roles    []string
	Channels []string
}

// An execution context in a VM (Environment).
type Evaluator struct {
	env             *Environment
	ctx             *v8.Context
	iso             *v8.Isolate
	api             *v8.Object
	functionFn      *v8.Function
	graphqlFn       *v8.Function
	delegate        EvaluatorDelegate // Provides CRUD API
	user            *UserCredentials
	mutationAllowed bool
}

func (env *Environment) NewEvaluator(delegate EvaluatorDelegate, user *UserCredentials) (*Evaluator, error) {
	// Create a V8 Context and run the script in it, returning the JS initializer function:
	ctx := v8.NewContext(env.vm, env.global)
	result, err := env.script.Run(ctx)
	if err != nil {
		return nil, err
	}
	initFn, err := result.AsFunction()
	if err != nil {
		return nil, err
	}

	eval := &Evaluator{
		env:      env,
		ctx:      ctx,
		iso:      env.vm,
		delegate: delegate,
		user:     user,
	}

	// Instantiate my Upstream object with the native callbacks.
	// The object's internal field 0 points to the FunContext.
	upstream, err := env.jsNativeTemplate.NewInstance(ctx)
	if err != nil {
		return nil, err
	}
	upstream.SetInternalField(0, eval)

	// Call the JS initializer, passing it the Upstream, to get back the API object:
	env.curEvaluator = eval
	apiVal, err := initFn.Call(initFn, env.jsonConfig, upstream)
	if err != nil {
		return nil, err
	}
	eval.api = mustSucceed(apiVal.AsObject())
	eval.functionFn = mustGetV8Fn(eval.api, "callFunction")
	eval.graphqlFn = mustGetV8Fn(eval.api, "graphql")
	return eval, nil
}

func (eval *Evaluator) Close() {
	if eval.env.curEvaluator == eval {
		eval.env.curEvaluator = nil
	}
	eval.ctx.Close()
	eval.ctx = nil
	eval.iso = nil
	eval.env = nil
}

func (eval *Evaluator) SetMutationAllowed(allowed bool) {
	eval.mutationAllowed = allowed
}

// Calls a named function. Returns a JSON string.
func (eval *Evaluator) CallFunction(name string, args map[string]any) ([]byte, error) {
	user, roles, channels := eval.v8Credentials()
	// Calling JS method API.callFunction (api.ts)
	v8Result, err := eval.functionFn.Call(eval.api,
		goToV8String(eval.iso, name),
		mustSucceed(goToV8JSON(eval.ctx, args)),
		user, roles, channels,
		mustSucceed(v8.NewValue(eval.iso, eval.mutationAllowed)))

	var result string
	if err == nil {
		if v8Result, err = resolvePromise(v8Result); err == nil {
			result, err = v8ToGoString(v8Result)
			if err == nil {
				return []byte(result), nil
			}
		}
	}
	return nil, unpackJSError(err)
}

// Performs a GraphQL query. Returns a JSON string.
func (eval *Evaluator) CallGraphQL(query string, operationName string, variables map[string]any) ([]byte, error) {
	user, roles, channels := eval.v8Credentials()
	// Calling JS method API.callGraphQL (api.ts)
	v8Result, err := eval.graphqlFn.Call(eval.api,
		goToV8String(eval.iso, query),
		goToV8String(eval.iso, operationName),
		mustSucceed(goToV8JSON(eval.ctx, variables)),
		user, roles, channels,
		mustSucceed(v8.NewValue(eval.iso, eval.mutationAllowed)))
	var result string
	if err == nil {
		if v8Result, err = resolvePromise(v8Result); err == nil {
			result, err = v8ToGoString(v8Result)
			if err == nil {
				return []byte(result), nil
			}
		}
	}
	return nil, unpackJSError(err)
}

func (eval *Evaluator) v8Credentials() (user *v8.Value, roles *v8.Value, channels *v8.Value) {
	undef := v8.Undefined(eval.iso)
	user = undef
	if eval.user != nil {
		user = goToV8String(eval.iso, eval.user.Name)
	} else {
		user = undef
	}
	if eval.user != nil && len(eval.user.Roles) > 0 {
		roles = goToV8String(eval.iso, strings.Join(eval.user.Roles, ","))
	} else {
		roles = undef
	}
	if eval.user != nil && len(eval.user.Channels) > 0 {
		channels = goToV8String(eval.iso, strings.Join(eval.user.Channels, ","))
	} else {
		channels = undef
	}
	return
}

//////// NATIVE CALLBACK IMPLEMENTATIONS:

// 	query(fnName: string, n1ql: string, argsJSON: string | undefined, asAdmin: boolean) : string;
func doQuery(eval *Evaluator, info *v8.FunctionCallbackInfo) (*v8.Value, error) {
	var rows []any
	fnName := info.Args()[0].String()
	n1ql := info.Args()[1].String()
	args, err := v8JSONToGo(info.Args()[2])
	if err != nil {
		return nil, err
	}
	asAdmin := info.Args()[3].Boolean()
	rows, err = eval.delegate.query(fnName, n1ql, args, asAdmin)
	return returnJSONToV8(info, rows, err)
}

// 	get(docID: string, asAdmin: boolean) : string | null;
func doGet(eval *Evaluator, info *v8.FunctionCallbackInfo) (*v8.Value, error) {
	docID := info.Args()[0].String()
	asAdmin := info.Args()[1].Boolean()

	result, err := eval.delegate.get(docID, asAdmin)
	return returnJSONToV8(info, result, err)
}

// 	save(docJSON: string, docID: string | undefined, asAdmin: boolean) : string;
func doSave(eval *Evaluator, info *v8.FunctionCallbackInfo) (*v8.Value, error) {
	var docID string
	doc, err := v8JSONToGo(info.Args()[0])
	if err != nil {
		return nil, err
	}
	if arg1 := info.Args()[1]; arg1.IsString() {
		docID = arg1.String()
	} else if _id, found := doc["_id"].(string); found {
		docID = _id
	} else {
		docID, err = base.GenerateRandomID()
		if err != nil {
			return nil, err
		}
	}
	asAdmin := info.Args()[2].Boolean()

	if saved, err := eval.delegate.save(doc, docID, asAdmin); saved {
		return returnValueToV8(info, docID, err)
	} else {
		return v8.Null(info.Context().Isolate()), err
	}
}

// 	delete(docID: string, revID: string | undefined, asAdmin: boolean) : boolean;
func doDelete(eval *Evaluator, info *v8.FunctionCallbackInfo) (*v8.Value, error) {
	docID := info.Args()[0].String()
	var revID string
	if arg1 := info.Args()[1]; arg1.IsString() {
		revID = arg1.String()
	}
	asAdmin := info.Args()[2].Boolean()

	ok, err := eval.delegate.delete(docID, revID, asAdmin)
	return returnValueToV8(info, ok, err)
}

func doLog(eval *Evaluator, info *v8.FunctionCallbackInfo) (*v8.Value, error) {
	var level base.LogLevel
	var message []string
	for i, arg := range info.Args() {
		if i == 0 {
			level = base.LogLevel(arg.Integer())
		} else {
			message = append(message, arg.DetailString())
		}
	}
	eval.delegate.log(level, strings.Join(message, " "))
	return nil, nil
}

//////// UTILITIES

func goToV8String(i *v8.Isolate, str string) *v8.Value {
	return mustSucceed(v8.NewValue(i, str))
}

func v8ToGoString(val *v8.Value) (string, error) {
	if val.IsString() {
		return val.String(), nil
	} else {
		return "", fmt.Errorf("JavaScript returned non-string")
	}
}

// Converts a V8 string of JSON into a Go map.
func v8JSONToGo(val *v8.Value) (map[string]any, error) {
	var err error
	if jsonStr, err := v8ToGoString(val); err == nil {
		var result map[string]any
		if err = json.Unmarshal([]byte(jsonStr), &result); err == nil {
			return result, nil
		}
	}
	return nil, err
}

func goToV8JSON(ctx *v8.Context, obj any) (*v8.Value, error) {
	if obj == nil {
		return v8.Null(ctx.Isolate()), nil
	} else if jsonBytes, err := json.Marshal(obj); err != nil {
		return nil, err
	} else {
		return v8.NewValue(ctx.Isolate(), string(jsonBytes))
	}
}

func returnJSONToV8(info *v8.FunctionCallbackInfo, result any, err error) (*v8.Value, error) {
	if err == nil {
		return goToV8JSON(info.Context(), result)
	}
	return nil, err
}

// 'result' must be a number, bool or string
func returnValueToV8(info *v8.FunctionCallbackInfo, result any, err error) (*v8.Value, error) {
	if err == nil {
		return v8.NewValue(info.Context().Isolate(), result)
	}
	return nil, err
}

func v8Throw(i *v8.Isolate, err error) *v8.Value {
	return i.ThrowException(goToV8String(i, err.Error()))
}

func mustGetV8Fn(owner *v8.Object, name string) *v8.Function {
	fnVal := mustSucceed(owner.Get(name))
	return mustSucceed(fnVal.AsFunction())
}

func resolvePromise(val *v8.Value) (*v8.Value, error) {
	if !val.IsPromise() {
		return val, nil
	}
	switch p, _ := val.AsPromise(); p.State() {
	case v8.Fulfilled:
		return p.Result(), nil
	case v8.Rejected:
		errVal := p.Result()
		return nil, fmt.Errorf("promise rejected: %s", errVal.DetailString())
	default:
		log.Fatalf("Promise still pending!!") //FIXME
		return nil, nil
	}
}

// This detects the way HTTP statuses are encoded into error messages by the class HTTPError in types.ts.
var kHTTPErrRegexp = regexp.MustCompile(`^\[(\d\d\d)\]\s+(.*)`)

func unpackJSError(err error) error {
	if jsErr, ok := err.(*v8.JSError); ok {
		if m := kHTTPErrRegexp.FindStringSubmatch(jsErr.Message); m != nil {
			if status, err := strconv.ParseUint(m[1], 10, 16); err == nil {
				return &base.HTTPError{Status: int(status), Message: m[2]}
			}
		}
	}
	return err
}

func mustSucceed[T any](result T, err error) T {
	if err != nil {
		log.Fatalf("ASSERTION FAILURE: expected a %T, got %v", result, err)
	}
	return result
}
