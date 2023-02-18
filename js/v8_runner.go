/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package js

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	v8 "github.com/snej/v8go" // Docs: https://pkg.go.dev/github.com/snej/v8go
)

type V8Runner struct {
	baseRunner
	v8vm     *v8VM
	template V8Template   // The Service template I'm created from
	ctx      *v8.Context  // V8 object managing this execution context
	mainFn   *v8.Function // The entry-point function (returned by the Service's script)
	Client   any          // You can put whatever you want here, to point back to your state
}

func newV8Runner(vm *v8VM, template V8Template, id serviceID) (*V8Runner, error) {
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

	return &V8Runner{
		baseRunner: baseRunner{
			id: id,
			vm: vm,
		},
		v8vm:     vm,
		template: template,
		ctx:      ctx,
		mainFn:   mainFn,
	}, nil
}

func (r *V8Runner) Template() V8Template { return r.template }

// Always call this when finished with a v8Runner acquired from Service.GetRunner.
func (r *V8Runner) Return() {
	r.v8vm.returnRunner(r)
}

// Disposes the V8 resources for a runner.
func (r *V8Runner) close() {
	if r.vm != nil && r.v8vm.curRunner == r {
		r.v8vm.curRunner = nil
	}
	if r.ctx != nil {
		r.ctx.Close()
		r.ctx = nil
	}
	r.vm = nil
}

//////// CALLING JAVASCRIPT FUNCTIONS

// Runs the Service's function, returning the result as a V8 Value.
// If the function throws an exception, it's returned as a v8.JSError.
func (r *V8Runner) Run(args ...any) (any, error) {
	v8args, err := r.ConvertArgs(args...)
	if err == nil {
		var result *v8.Value
		if result, err = r.RunWithV8Args(v8args...); err == nil {
			return ValueToGo(result)
		}
	}
	return nil, err
}

// Runs the Service's function -- the same as `Run`, but requires that the args already be
// V8 Values or Objects.
// If the function throws an exception, it's returned as a v8.JSError.
func (r *V8Runner) RunWithV8Args(args ...v8.Valuer) (*v8.Value, error) {
	return r.Call(r.mainFn, r.mainFn, args...)
}

// A convenience wrapper around RunWithV8Args() that returns a v8.Object.
func (r *V8Runner) RunAsObject(args ...v8.Valuer) (*v8.Object, error) {
	if val, err := r.RunWithV8Args(args...); err != nil {
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
// - stops with a DeadlineExceeded error if the v8Runner's Go Context times out.
func (r *V8Runner) Call(fn *v8.Function, this v8.Valuer, args ...v8.Valuer) (*v8.Value, error) {
	return r.ResolvePromise(r.JustCall(fn, this, args...))
}

// Calls a V8 function; like v8Runner.Call except it does _not_ resolve Promises.
func (r *V8Runner) JustCall(fn *v8.Function, this v8.Valuer, args ...v8.Valuer) (*v8.Value, error) {
	timedOut := false
	if timeout := r.Timeout(); timeout != nil {
		if *timeout <= 0 {
			// Already timed out
			return nil, context.DeadlineExceeded
		} else {
			// Future timeout; start a goroutine that will make the VM's thread terminate:
			timer := time.NewTimer(*timeout)
			completed := make(chan bool)
			defer close(completed)
			iso := r.v8vm.iso
			go func() {
				defer timer.Stop()

				select {
				case <-completed:
					return
				case <-timer.C:
					timedOut = true
					iso.TerminateExecution()
				}
			}()
		}
	}
	val, err := fn.Call(this, args...)
	if jsErr, ok := err.(*v8.JSError); ok && strings.HasPrefix(jsErr.Message, "ExecutionTerminated:") {
		if timedOut {
			err = context.DeadlineExceeded
		}
	}
	return val, err
}

// Postprocesses a V8 Value returned from v8Runner.JustCall.
// - If it's a Promise, it returns its resolved value or error.
// - If the Promise hasn't completed yet, it lets V8 run until it completes.
// - Otherwise it
// Returns a DeadlineExceeded error if the v8Runner's Go Context has timed out,
// or times out while waiting on a Promise.
func (r *V8Runner) ResolvePromise(val *v8.Value, err error) (*v8.Value, error) {
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

func (r *V8Runner) WithTemporaryValues(fn func()) {
	r.ctx.WithTemporaryValues(fn)
}

//////// CONVERTING GO VALUES TO JAVASCRIPT:

// Converts its arguments to an array of V8 values by calling v8Runner.NewValue on each one.
// Useful for calling JS functions.
func (r *V8Runner) ConvertArgs(args ...any) ([]v8.Valuer, error) {
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
func (r *V8Runner) NewValue(val any) (v8Val *v8.Value, err error) {
	if val == nil {
		return r.NullValue(), nil // v8.NewValue panics if given nil :-p
	}
	v8Val, err = r.ctx.NewValue(val)
	if err != nil {
		if jsonStr, ok := val.(JSONString); ok {
			if jsonStr != "" {
				return r.JSONParse(string(jsonStr))
			} else {
				return r.NullValue(), nil
			}
		} else if jsonData, jsonErr := json.Marshal(val); jsonErr == nil {
			return r.JSONParse(string(jsonData))
		}
	}
	return v8Val, err
}

// A string type that v8Runner.NewValue will treat specially, by parsing it as JSON and converting
// it to a JavaScript object.
type JSONString string

// Creates a JavaScript number value.
func (r *V8Runner) NewInt(i int) *v8.Value { return mustSucceed(r.ctx.NewValue(i)) }

// Creates a JavaScript string value.
func (r *V8Runner) NewString(str string) *v8.Value { return newString(r.v8vm.iso, str) }

// Marshals a Go value to JSON and returns it as a V8 string.
func (r *V8Runner) NewJSONString(val any) (*v8.Value, error) { return newJSONString(r.ctx, val) }

// Parses a JSON string to a V8 value, by calling JSON.parse() on it.
func (r *V8Runner) JSONParse(json string) (*v8.Value, error) { return v8.JSONParse(r.ctx, json) }

// Returns a value representing JavaScript 'undefined'.
func (r *V8Runner) UndefinedValue() *v8.Value { return v8.Undefined(r.v8vm.iso) }

// Returns a value representing JavaScript 'null'.
func (r *V8Runner) NullValue() *v8.Value { return v8.Null(r.v8vm.iso) }

//////// CONVERTING JAVASCRIPT VALUES BACK TO GO:

// Encodes a V8 value as a JSON string.
func (r *V8Runner) JSONStringify(val *v8.Value) (string, error) { return v8.JSONStringify(r.ctx, val) }

//////// INSTANTIATING TEMPLATES:

// Creates a V8 Object from a template previously created by BasicTemplate.NewObjectTemplate.
// (Not needed if you added the template as a property of the global object.)
func (r *V8Runner) NewInstance(o *v8.ObjectTemplate) (*v8.Object, error) {
	return o.NewInstance(r.ctx)
}

// Creates a V8 Function from a template previously created by BasicTemplate.NewCallback.
// (Not needed if you added the template as a property of the global object.)
func (r *V8Runner) NewFunctionInstance(f *v8.FunctionTemplate) *v8.Function {
	return f.GetFunction(r.ctx)
}
