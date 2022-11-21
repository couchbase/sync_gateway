package js

import (
	"fmt"

	v8 "rogchap.com/v8go" // Docs: https://pkg.go.dev/rogchap.com/v8go
)

// A JavaScript execution Runner in a VM. This is what actually does the work.
// A VM has its own JS global state, but all instances on a VM share a single JavaScript thread.
// **NOTE:** The current implementation allows for only one Runner on a VM at once.
// **Not thread-safe! Must be called only on one goroutine at a time. In fact, all Evaluators created from the same VM must be called only one one goroutine.**
type Runner struct {
	service      Service      // The Service I'm created from
	vm           *VM          // The owning VM object
	ctx          *v8.Context  // V8 object managing this execution context
	mainFn       *v8.Function // The entry-point function (returned by the Service's script)
	Delegate     any          // User may put whatever they want here, to point back to their state
	CheckTimeout func() error
}

// Creates a Runner on a Service
func newRunner(vm *VM, service Service) (*Runner, error) {
	//log.Printf("*** newRunner") //TEMP
	// Create a V8 Context and run the script in it, returning the JS `main` function:
	ctx := v8.NewContext(vm.iso, service.Global())
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

	runner := &Runner{
		service: service,
		vm:      vm,
		ctx:     ctx,
		mainFn:  mainFn,
	}
	return runner, nil
}

// Always call this when finished with a Runner.
func (r *Runner) Return() {
	r.vm.returnRunner(r)
}

// Disposes the V8 resources for a runner.
func (r *Runner) close() {
	//log.Printf("*** close Runner %p", r) //TEMP
	if r.vm != nil && r.vm.curRunner == r {
		r.vm.curRunner = nil
	}
	r.ctx.Close()
	r.ctx = nil
	r.vm = nil
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

// Postprocesses a result from V8: if it's a Promise it returns its resolved value or error. If the Promise hasn't completed yet, it lets V8 run until it completes.
func (r *Runner) ResolvePromise(val *v8.Value, err error) (*v8.Value, error) {
	if err != nil {
		return nil, err
	}
	if !val.IsPromise() {
		return val, nil
	}
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
			if r.CheckTimeout != nil {
				if err := r.CheckTimeout(); err != nil {
					return nil, err
				}
			}
			// go round the loop again...
		default:
			return nil, fmt.Errorf("illegal v8.Promise state %d", p)
		}
	}
}
