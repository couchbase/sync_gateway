package js

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"rogchap.com/v8go"
	v8 "rogchap.com/v8go" // Docs: https://pkg.go.dev/rogchap.com/v8go
)

// A Runner represents a Service instantiated in a VM, in its own sandboxed V8 context,
// ready to run the Service's code.
// **NOT thread-safe!**
type Runner struct {
	template  Template        // The Service template I'm created from
	id        serviceID       // The service ID in its VM
	vm        *VM             // The owning VM object
	ctx       *v8.Context     // V8 object managing this execution context
	mainFn    *v8.Function    // The entry-point function (returned by the Service's script)
	goContext context.Context // context.Context value for use by Go callbacks
	Client    any             // You can put whatever you want here, to point back to your state
}

func (r *Runner) Template() Template { return r.template }
func (r *Runner) VM() *VM            { return r.vm }

func newRunner(vm *VM, template Template, id serviceID) (*Runner, error) {
	// Create a V8 Context and run the setup script in it:
	ctx := v8.NewContext(vm.iso, template.Global())
	if _, err := vm.setupScript.Run(ctx); err != nil {
		return nil, errors.Wrap(err, "Unexpected error in JavaScript initialization code")
	}

	// Now run the service's script, which returns the service's main function:
	result, err := template.Script().Run(ctx)
	if err != nil {
		ctx.Close()
		return nil, fmt.Errorf("JavaScript error initializing %s: %w", template.Name(), err)
	}
	mainFn, err := result.AsFunction()
	if err != nil {
		ctx.Close()
		return nil, fmt.Errorf("%s's script did not return a function: %w", template.Name(), err)
	}

	return &Runner{
		template: template,
		id:       id,
		vm:       vm,
		ctx:      ctx,
		mainFn:   mainFn,
	}, nil
}

// Always call this when finished with a Runner acquired from Service.GetRunner.
func (r *Runner) Return() {
	r.vm.returnRunner(r)
}

// Disposes the V8 resources for a runner.
func (r *Runner) close() {
	if r.vm != nil && r.vm.curRunner == r {
		r.vm.curRunner = nil
	}
	if r.ctx != nil {
		r.ctx.Close()
		r.ctx = nil
	}
	r.vm = nil
}

//////// GO CONTEXT AND TIMEOUT    (not to be confused with v8.Context!)

// Associates a Go Context with this Runner.
// If this Context has a deadline, JS calls will abort if it expires.
func (r *Runner) SetContext(ctx context.Context) { r.goContext = ctx }

// Returns the Go context.Context associated with this Runner; nil by default.
func (r *Runner) Context() context.Context { return r.goContext }

// Returns the Go context.Context associated with this Runner, else `context.TODO()`.
func (r *Runner) ContextOrDefault() context.Context {
	if r.goContext != nil {
		return r.goContext
	} else {
		return context.TODO()
	}
}

// Returns the remaining duration until the Runner's Go Context's deadline, or nil if none.
func (r *Runner) Timeout() *time.Duration {
	if r.goContext != nil {
		if deadline, hasDeadline := r.goContext.Deadline(); hasDeadline {
			timeout := time.Until(deadline)
			return &timeout
		}
	}
	return nil
}

//////// CALLING JAVASCRIPT FUNCTIONS

// Runs the Service's function, returning the result as a V8 Value.
// If the function throws an exception, it's returned as a v8.JSError.
func (r *Runner) Run(args ...v8.Valuer) (*v8.Value, error) {
	return r.Call(r.mainFn, r.mainFn, args...)
}

// A convenience wrapper around Runner.Run that casts the return value to a V8 Object.
func (r *Runner) RunAsObject(args ...v8.Valuer) (*v8.Object, error) {
	if val, err := r.Run(args...); err != nil {
		return nil, err
	} else if obj, err := val.AsObject(); err != nil {
		return nil, err
	} else {
		return obj, nil
	}
}

// Calls a v8.Function, presumably one that you defined in the Service's Template.
// Use this instead of the v8.Function.Call method, because it:
// - waits for Promises to resolve, and returns the resolved value or rejection error;
// - stops with a DeadlineExceeded error if the Runner's Go Context times out.
func (r *Runner) Call(fn *v8.Function, this v8.Valuer, args ...v8.Valuer) (*v8.Value, error) {
	return r.ResolvePromise(r.JustCall(fn, this, args...))
}

// Calls a V8 function; like Runner.Call except it does _not_ resolve Promises.
func (r *Runner) JustCall(fn *v8.Function, this v8.Valuer, args ...v8.Valuer) (*v8.Value, error) {
	if timeout := r.Timeout(); timeout != nil {
		if *timeout <= 0 {
			// Already timed out
			return nil, context.DeadlineExceeded
		} else {
			// Future timeout; start a goroutine that will make the VM's thread terminate:
			timer := time.NewTimer(*timeout)
			completed := make(chan bool)
			defer close(completed)
			iso := r.vm.iso
			go func() {
				defer timer.Stop()

				select {
				case <-completed:
					return
				case <-timer.C:
					iso.TerminateExecution()
				}
			}()
		}
	}
	val, err := fn.Call(this, args...)
	if jsErr, ok := err.(*v8go.JSError); ok && strings.HasPrefix(jsErr.Message, "ExecutionTerminated:") {
		err = context.DeadlineExceeded
	}
	return val, err
}

// Postprocesses a V8 Value returned from Runner.JustCall.
// - If it's a Promise, it returns its resolved value or error.
// - If the Promise hasn't completed yet, it lets V8 run until it completes.
// - Otherwise it
// Returns a DeadlineExceeded error if the Runner's Go Context has timed out,
// or times out while waiting on a Promise.
func (r *Runner) ResolvePromise(val *v8.Value, err error) (*v8.Value, error) {
	if err != nil || !val.IsPromise() {
		return val, err
	}
	for {
		switch p, _ := val.AsPromise(); p.State() {
		case v8.Fulfilled:
			return p.Result(), nil
		case v8.Rejected:
			return nil, errors.New(p.Result().DetailString())
		case v8.Pending:
			r.ctx.PerformMicrotaskCheckpoint() // run VM to make progress on the promise
			deadline, hasDeadline := r.ContextOrDefault().Deadline()
			if hasDeadline && time.Now().After(deadline) {
				return nil, context.DeadlineExceeded
			}
			// go round the loop again...
		default:
			return nil, fmt.Errorf("illegal v8.Promise state %d", p) // impossible
		}
	}
}

//////// CONVERTING GO VALUES TO JAVASCRIPT:

// Converts its arguments to an array of V8 values by calling Runner.NewValue on each one.
// Useful for calling JS functions.
func (r *Runner) ConvertArgs(args ...any) ([]v8.Valuer, error) {
	v8args := make([]v8.Valuer, len(args))
	for i, arg := range args {
		var err error
		if v8args[i], err = r.NewValue(arg); err != nil {
			return nil, err
		}
	}
	return v8args, nil
}

// Converts a Go value to a JavaScript value (*v8.Value). Supported types are:
// - nil (converted to `null`)
// - boolean
// - int, int32, int64, uint32, uint64, float64
// - *big.Int
// - json.Number
// - string
// - JSONString -- will be parsed by V8's JSON.parse
// - Any JSON Marshalable type -- will be marshaled to JSON then parsed by V8's JSON.parse
func (r *Runner) NewValue(val any) (v8Val *v8.Value, err error) {
	v8Val, err = newValue(r.vm.iso, val)
	if err != nil {
		if jsonStr, ok := val.(JSONString); ok {
			return r.JSONParse(string(jsonStr))
		} else if jsonData, jsonErr := json.Marshal(val); jsonErr == nil {
			return r.JSONParse(string(jsonData))
		}
	}
	return v8Val, err
}

// A string type that Runner.NewValue will treat specially, by parsing it as JSON and converting
// it to a JavaScript object.
type JSONString string

// Creates a JavaScript number value.
func (r *Runner) NewInt(i int) *v8.Value { return newInt(r.vm.iso, i) }

// Creates a JavaScript string value.
func (r *Runner) NewString(str string) *v8.Value { return newString(r.vm.iso, str) }

// Marshals a Go value to JSON and returns it as a V8 string.
func (r *Runner) NewJSONString(val any) (*v8.Value, error) { return newJSONString(r.ctx, val) }

// Parses a JSON string to a V8 value, by calling JSON.parse() on it.
func (r *Runner) JSONParse(json string) (*v8.Value, error) { return v8.JSONParse(r.ctx, json) }

// Returns a value representing JavaScript 'undefined'.
func (r *Runner) Undefined() *v8.Value { return v8.Undefined(r.vm.iso) }

//////// CONVERTING JAVASCRIPT VALUES BACK TO GO:

// Encodes a V8 value as a JSON string.
func (r *Runner) JSONStringify(val *v8.Value) (string, error) { return v8.JSONStringify(r.ctx, val) }

//////// INSTANTIATING TEMPLATES:

// Creates a V8 Object from a template previously created by BasicTemplate.NewObjectTemplate.
// (Not needed if you added the template as a property of the global object.)
func (r *Runner) NewInstance(o *v8.ObjectTemplate) (*v8.Object, error) {
	return o.NewInstance(r.ctx)
}

// Creates a V8 Function from a template previously created by BasicTemplate.NewCallback.
// (Not needed if you added the template as a property of the global object.)
func (r *Runner) NewFunctionInstance(f *v8.FunctionTemplate) *v8.Function {
	return f.GetFunction(r.ctx)
}
