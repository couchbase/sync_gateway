package js

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"
	v8 "rogchap.com/v8go" // Docs: https://pkg.go.dev/rogchap.com/v8go
)

// A JavaScript execution Runner in a VM. This is what actually does the work.
// A VM has its own JS global state, but all instances on a VM share a single JavaScript thread.
// **NOTE:** The current implementation allows for only one Runner on a VM at once.
// **Not thread-safe! Must be called only on one goroutine at a time. In fact, all Evaluators created from the same VM must be called only one one goroutine.**
type Runner struct {
	service   Service         // The Service I'm created from
	vm        *VM             // The owning VM object
	ctx       *v8.Context     // V8 object managing this execution context
	mainFn    *v8.Function    // The entry-point function (returned by the Service's script)
	goContext context.Context // Context value for use by Go callbacks
	Client    any             // You can put whatever you want here, to point back to your state
}

// Creates a Runner on a Service
func newRunner(vm *VM, service Service) (*Runner, error) {
	// Create a V8 Context and run the setup script in it
	ctx := v8.NewContext(vm.iso, service.Global())
	if _, err := vm.setupScript.Run(ctx); err != nil {
		return nil, errors.Wrap(err, "js.Runner failed to initialize underscore.js library")
	}

	// Now run the service's script, which returns the service's main function:
	result, err := service.Script().Run(ctx)
	if err != nil {
		ctx.Close()
		return nil, err
	}
	mainFn, err := result.AsFunction()
	if err != nil {
		ctx.Close()
		return nil, err
	}

	return &Runner{
		service: service,
		vm:      vm,
		ctx:     ctx,
		mainFn:  mainFn,
	}, nil
}

// Always call this when finished with a Runner.
func (r *Runner) Return() {
	r.vm.returnRunner(r)
}

// Disposes the V8 resources for a runner.
func (r *Runner) close() {
	if r.vm != nil && r.vm.curRunner == r {
		r.vm.curRunner = nil
	}
	r.ctx.Close()
	r.ctx = nil
	r.vm = nil
}

// Associates a Context value with this Runner, for use by callbacks.
func (r *Runner) SetContext(ctx context.Context) { r.goContext = ctx }

// Returns the Context associated with this Runner.
func (r *Runner) Context() context.Context { return r.goContext }

// Returns the Context associated with this Runner, else `context.TODO()`
func (r *Runner) ContextOrDefault() context.Context {
	if r.goContext != nil {
		return r.goContext
	} else {
		return context.TODO()
	}
}

// Runs the Service's script, returning the result as a V8 Value.
func (r *Runner) Run(args ...v8.Valuer) (*v8.Value, error) {
	return r.ResolvePromise(r.mainFn.Call(r.mainFn, args...))
}

// Runs the Service's script, returning the result as a V8 Object.
func (r *Runner) RunAsObject(args ...v8.Valuer) (*v8.Object, error) {
	if val, err := r.Run(args...); err != nil {
		return nil, err
	} else if obj, err := val.AsObject(); err != nil {
		return nil, err
	} else {
		return obj, nil
	}
}

func (r *Runner) Service() Service                                     { return r.service }
func (r *Runner) NewValue(val any) (*v8.Value, error)                  { return r.vm.NewValue(val) }
func (r *Runner) NewString(str string) *v8.Value                       { return r.vm.NewString(str) }
func (r *Runner) NewJSONString(val any) (*v8.Value, error)             { return goToV8JSON(r.ctx, val) }
func (r *Runner) NewInstance(o *v8.ObjectTemplate) (*v8.Object, error) { return o.NewInstance(r.ctx) }

func (r *Runner) NewCallback(cb ServiceCallback) *v8.Function {
	return r.vm.NewCallback(cb).GetFunction(r.ctx)
}

// Converts a Go value to a V8 value by marshaling to JSON and then parsing in V8.
func (r *Runner) GoToV8(val any) (*v8.Value, error) {
	j, err := json.Marshal(val)
	if err != nil {
		return nil, err
	}
	log.Printf("*** GoToV8: %s", j) //TEMP
	return r.JSONParse(string(j))
}

// Encodes a V8 value as a JSON string.
func (r *Runner) JSONStringify(val *v8.Value) (string, error) { return v8.JSONStringify(r.ctx, val) }

// Parses a JSON string to a V8 value.
func (r *Runner) JSONParse(json string) (*v8.Value, error) { return v8.JSONParse(r.ctx, json) }

// Postprocesses a result from V8: if it's a Promise it returns its resolved value or error. If the Promise hasn't completed yet, it lets V8 run until it completes.
func (r *Runner) ResolvePromise(val *v8.Value, err error) (*v8.Value, error) {
	if err != nil {
		return nil, err
	}
	if !val.IsPromise() {
		return val, nil
	}
	deadline, hasDeadline := r.ContextOrDefault().Deadline()
	for {
		switch p, _ := val.AsPromise(); p.State() {
		case v8.Fulfilled:
			return p.Result(), nil
		case v8.Rejected:
			errStr := p.Result().DetailString()
			err = UnpackErrorStr(errStr)
			if err == nil {
				err = fmt.Errorf("%s", errStr)
			}
			return nil, err
		case v8.Pending:
			r.ctx.PerformMicrotaskCheckpoint() // run VM to make progress on the promise
			if hasDeadline && time.Now().After(deadline) {
				return nil, context.DeadlineExceeded
			}
			// go round the loop again...
		default:
			return nil, fmt.Errorf("illegal v8.Promise state %d", p)
		}
	}
}
