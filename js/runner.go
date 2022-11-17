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
	module *Module      // The Module I'm created from
	vm     *VM          // The owning VM object
	ctx    *v8.Context  // V8 object managing this execution context
	mainFn *v8.Function // The entry-point function (returned by the Module's script)

	Delegate any // User may put whatever they want here, to point back to their state
}

// Constructs an context.
func (vm *VM) NewRunner(moduleName string) (*Runner, error) {
	module := vm.modules[moduleName]
	if module == nil {
		return nil, fmt.Errorf("js.VM has no module %q", moduleName)
	}
	// Create a V8 Context and run the script in it, returning the JS `main` function:
	ctx := v8.NewContext(vm.iso, module.global)
	result, err := module.script.Run(ctx)
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
		module: module,
		vm:     vm,
		ctx:    ctx,
		mainFn: mainFn,
	}
	vm.curRunner = runner
	return runner, nil
}

// Disposes the V8 resources for an context. Always call this when done.
func (r *Runner) Close() {
	if r.vm.curRunner == r {
		r.vm.curRunner = nil
	}
	r.ctx.Close()
	r.ctx = nil
	r.vm.Return()
	r.vm = nil
}

func (r *Runner) Run(args ...v8.Valuer) (*v8.Value, error) {
	return r.ResolvePromise(r.mainFn.Call(r.mainFn, args...))
}

func (r *Runner) NewValue(val any) (*v8.Value, error)      { return r.module.NewValue(val) }
func (r *Runner) NewString(str string) *v8.Value           { return r.module.NewString(str) }
func (r *Runner) NewJSONString(val any) (*v8.Value, error) { return goToV8JSON(r.ctx, val) }

func (r *Runner) NewCallback(cb ModuleCallback) *v8.Function {
	return r.module.NewCallback(cb).GetFunction(r.ctx)
}

func (r *Runner) GetObject(name string) (*v8.Object, error) {
	if obj := r.module.registeredObjs[name]; obj != nil {
		return obj.NewInstance(r.ctx)
	} else {
		return nil, fmt.Errorf("js.Runner has no object registered as %q", name)
	}
}

func (r *Runner) GetValue(name string) *v8.Value {
	value := r.module.registeredVals[name]
	if value == nil {
		panic(fmt.Sprintf("GetValue: no value named %q", name))
	}
	return value
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
			// if err := c.delegate.checkTimeout(); err != nil {	//TODO
			// 	return nil, err
			// }
			// go round the loop again...
		default:
			return nil, fmt.Errorf("illegal v8.Promise state %d", p)
		}
	}
}
