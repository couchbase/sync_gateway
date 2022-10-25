package functions

import (
	_ "embed"
	"encoding/json"
	"fmt"
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
	env.defineEvaluatorCallback(env.jsNativeTemplate, "query", doQuery)
	env.defineEvaluatorCallback(env.jsNativeTemplate, "get", doGet)
	env.defineEvaluatorCallback(env.jsNativeTemplate, "save", doSave)
	env.defineEvaluatorCallback(env.jsNativeTemplate, "delete", doDelete)
	env.defineEvaluatorCallback(env.jsNativeTemplate, "log", doLog)

	return env, nil
}

func (env *Environment) Close() {
	env.vm.Dispose()
}

// Returns the Evaluator that owns the given V8 Context.
func (env *Environment) getEvaluator(ctx *v8.Context) *Evaluator {
	// (This is kind of a hack, because we expect only one Evaluator at a time.)
	if env.curEvaluator == nil {
		panic(fmt.Sprintf("Unknown v8.Context passed to Environment.getEvaluator: %v, expected none", ctx))
	}
	if ctx != env.curEvaluator.ctx {
		panic(fmt.Sprintf("Unknown v8.Context passed to Environment.getEvaluator: %v, expected %v", ctx, env.curEvaluator.ctx))
	}
	return env.curEvaluator
}

//
//////// EVALUATOR:
//

// The interface the JS code calls -- provides query and CRUD operations.
type EvaluatorDelegate interface {
	// Return an error if evaluation has gone on too long.
	checkTimeout() error

	// Log a message
	log(level base.LogLevel, message string)

	// Perform a N1QL query.
	query(fnName string, n1ql string, args map[string]any, asAdmin bool) (rowsAsJSON string, err error)
	// Get a document.
	get(docID string, asAdmin bool) (doc map[string]any, err error)
	// Save/create a document.
	save(doc map[string]any, docID string, asAdmin bool) (saved bool, err error)
	// Delete a document.
	delete(docID string, revID string, asAdmin bool) (ok bool, err error)
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

	// Call the JS initialization code, passing it the Upstream and the configuration.
	// This returns the JS `API` object.
	env.curEvaluator = eval
	apiVal, err := initFn.Call(initFn, env.jsonConfig, upstream)
	if err != nil {
		return nil, err
	}
	eval.api = mustSucceed(apiVal.AsObject())

	// Check the API.errors property for configuration errors:
	if errorsVal, err := eval.api.Get("errors"); err != nil {
		return nil, err
	} else if errorsObj, _ := errorsVal.AsObject(); errorsObj != nil {
		var errors base.MultiError
		var i uint32
		for i = 0; errorsObj.HasIdx(i); i++ {
			if errorVal, err := errorsObj.GetIdx(i); err == nil {
				errors.Append(fmt.Errorf(errorVal.String()))
			}
		}
		return nil, &errors
	}

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

func (eval *Evaluator) GetUser() *UserCredentials {
	return eval.user
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

	var result []byte
	if err == nil {
		if v8Result, err = eval.resolvePromise(v8Result); err == nil {
			result, err = v8ToGoString(v8Result)
			if err == nil {
				return result, nil
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
	var result []byte
	if err == nil {
		if v8Result, err = eval.resolvePromise(v8Result); err == nil {
			result, err = v8ToGoString(v8Result)
			if err == nil {
				return result, nil
			}
		}
	}
	return nil, unpackJSError(err)
}

// Encodes credentials as 3 parameters to pass to JS.
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

func (eval *Evaluator) resolvePromise(val *v8.Value) (*v8.Value, error) {
	if !val.IsPromise() {
		return val, nil
	}
	for {
		switch p, _ := val.AsPromise(); p.State() {
		case v8.Fulfilled:
			return p.Result(), nil
		case v8.Rejected:
			errStr := p.Result().DetailString()
			err := unpackJSErrorStr(errStr)
			if err == nil {
				err = fmt.Errorf("%s", errStr)
			}
			return nil, err
		case v8.Pending:
			eval.ctx.PerformMicrotaskCheckpoint() // run VM to make progress on the promise
			if err := eval.delegate.checkTimeout(); err != nil {
				return nil, err
			}
			// go round the loop again...
		default:
			return nil, fmt.Errorf("illegal v8.Promise state %d", p)
		}
	}
}

//////// NATIVE CALLBACK IMPLEMENTATIONS:

// For convenience, Evaluator callbacks get passed the Evaluator instance,
// and get to return Go values -- allowed values are nil, numbers, bool, string.
type evaluatorCallback = func(*Evaluator, *v8.FunctionCallbackInfo) (any, error)

// Registers a Go function as a Java function property of an owner object.
func (env *Environment) defineEvaluatorCallback(owner *v8.ObjectTemplate, name string, callback evaluatorCallback) {
	fn := v8.NewFunctionTemplate(env.vm, func(info *v8.FunctionCallbackInfo) *v8.Value {
		eval := env.getEvaluator(info.Context())
		var err error
		if err = eval.delegate.checkTimeout(); err == nil {
			var result any
			if result, err = callback(eval, info); err == nil { // Finally call the fn!
				if result != nil {
					var v8Result *v8.Value
					if v8Result, err = v8.NewValue(env.vm, result); err == nil {
						return v8Result
					}
				} else {
					return v8.Undefined(env.vm)
				}
			}
		}
		return v8Throw(env.vm, err)
	})
	owner.Set(name, fn, v8.ReadOnly)
}

// 	query(fnName: string, n1ql: string, argsJSON: string | undefined, asAdmin: boolean) : string;
func doQuery(eval *Evaluator, info *v8.FunctionCallbackInfo) (any, error) {
	fnName := info.Args()[0].String()
	n1ql := info.Args()[1].String()
	args, err := v8JSONToGo(info.Args()[2])
	if err != nil {
		return nil, err
	}
	asAdmin := info.Args()[3].Boolean()
	return eval.delegate.query(fnName, n1ql, args, asAdmin)
}

// 	get(docID: string, asAdmin: boolean) : string | null;
func doGet(eval *Evaluator, info *v8.FunctionCallbackInfo) (any, error) {
	docID := info.Args()[0].String()
	asAdmin := info.Args()[1].Boolean()
	return returnAsJSON(eval.delegate.get(docID, asAdmin))
}

// 	save(docJSON: string, docID: string | undefined, asAdmin: boolean) : string;
func doSave(eval *Evaluator, info *v8.FunctionCallbackInfo) (any, error) {
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

	if saved, err := eval.delegate.save(doc, docID, asAdmin); saved && err == nil {
		return docID, nil
	} else {
		return nil, err
	}
}

// 	delete(docID: string, revID: string | undefined, asAdmin: boolean) : boolean;
func doDelete(eval *Evaluator, info *v8.FunctionCallbackInfo) (any, error) {
	docID := info.Args()[0].String()
	var revID string
	if arg1 := info.Args()[1]; arg1.IsString() {
		revID = arg1.String()
	}
	asAdmin := info.Args()[2].Boolean()
	return eval.delegate.delete(docID, revID, asAdmin)
}

//  log(level: number, ...args: string[])
func doLog(eval *Evaluator, info *v8.FunctionCallbackInfo) (any, error) {
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
