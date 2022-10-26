package functions

import (
	_ "embed"
	"fmt"
	"strings"

	"github.com/couchbase/sync_gateway/base"
	v8 "rogchap.com/v8go" // Docs: https://pkg.go.dev/rogchap.com/v8go
)

// A JavaScript execution context in an Environment. This is what actually does the work.
// An Environment has its own JS global state, but all instances on an Environment share a single JavaScript thread.
// **NOTE:** The current implementation allows for only one evaluator on an Environment at once.
// **Not thread-safe! Must be called only on one goroutine at a time. In fact, all Evaluators created from the same Environment must be called only one one goroutine.**
type evaluator struct {
	ctx             *v8.Context       // V8 object managing this execution context
	env             *environment      // The owning Environment object
	iso             *v8.Isolate       // The hosting JavaScript VM
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

// Name and capabilities of the current user. (Admin is represented by a nil `*userCredentials`.)
type userCredentials struct {
	Name     string
	Roles    []string
	Channels []string
}

// Constructs an Evaluator.
func (env *environment) newEvaluator(delegate evaluatorDelegate, user *userCredentials) (*evaluator, error) {
	// Create a V8 Context and run the script in it, returning the JS `main` function:
	ctx := v8.NewContext(env.vm, env.global)
	result, err := env.script.Run(ctx)
	if err != nil {
		return nil, err
	}
	mainFn, err := result.AsFunction()
	if err != nil {
		return nil, err
	}

	eval := &evaluator{
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
	apiVal, err := mainFn.Call(mainFn, env.jsonConfig, upstream)
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

// Disposes the V8 resources for an Evaluator. Always call this when done.
func (eval *evaluator) close() {
	if eval.env.curEvaluator == eval {
		eval.env.curEvaluator = nil
	}
	eval.ctx.Close()
	eval.ctx = nil
	eval.iso = nil
	eval.env = nil
}

// Configures whether the Evaluator is allowed to mutate the database; if false (the default), calls to `save()` and `delete()` will fail.
func (eval *evaluator) setMutationAllowed(allowed bool) {
	eval.mutationAllowed = allowed
}

// Returns the current user.
func (eval *evaluator) GetUser() *userCredentials {
	return eval.user
}

// Calls a named function. Result is returned as a JSON string.
func (eval *evaluator) callFunction(name string, args map[string]any) ([]byte, error) {
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

// Performs a GraphQL query. Result is an object of the usual GraphQL result shape, i.e. with `data` and/or `errors` properties. It is returned as a JSON string.
func (eval *evaluator) callGraphQL(query string, operationName string, variables map[string]any) ([]byte, error) {
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
func (eval *evaluator) v8Credentials() (user *v8.Value, roles *v8.Value, channels *v8.Value) {
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

// Postprocesses a result from V8: if it's a Promise it returns its resolved value or error. If the Promise hasn't completed yet, it lets V8 run until it completes.
func (eval *evaluator) resolvePromise(val *v8.Value) (*v8.Value, error) {
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

//////// `NativeAPI` CALLBACK IMPLEMENTATIONS:

// For convenience, Evaluator callbacks get passed the Evaluator instance,
// and return Go values -- allowed types are nil, numbers, bool, string.
type evaluatorCallback = func(*evaluator, *v8.FunctionCallbackInfo) (any, error)

// Registers an Evaluator callback as a JS function property of an owner object.
func (env *environment) defineEvaluatorCallback(owner *v8.ObjectTemplate, name string, callback evaluatorCallback) {
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
func doQuery(eval *evaluator, info *v8.FunctionCallbackInfo) (any, error) {
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
func doGet(eval *evaluator, info *v8.FunctionCallbackInfo) (any, error) {
	docID := info.Args()[0].String()
	asAdmin := info.Args()[1].Boolean()
	return returnAsJSON(eval.delegate.get(docID, asAdmin))
}

// 	save(docJSON: string, docID: string | undefined, asAdmin: boolean) : string | null;
func doSave(eval *evaluator, info *v8.FunctionCallbackInfo) (any, error) {
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
func doDelete(eval *evaluator, info *v8.FunctionCallbackInfo) (any, error) {
	docID := info.Args()[0].String()
	var revID string
	if arg1 := info.Args()[1]; arg1.IsString() {
		revID = arg1.String()
	}
	asAdmin := info.Args()[2].Boolean()
	return eval.delegate.delete(docID, revID, asAdmin)
}

//  log(sgLogLevel: number, ...args: string[])
func doLog(eval *evaluator, info *v8.FunctionCallbackInfo) (any, error) {
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
