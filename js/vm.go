package js

import (
	_ "embed"
	"fmt"

	v8 "rogchap.com/v8go" // Docs: https://pkg.go.dev/rogchap.com/v8go
)

//////// ENVIRONMENT

// Represents a V8 JavaScript VM (aka "Isolate".)
// This doesn't do much on its own; its purpose is to perform shared initialization that makes creating an context faster.
// **Not thread-safe! Must be called only on one goroutine at a time.**
type VM struct {
	iso       *v8.Isolate        // A V8 virtual machine. NOT THREAD SAFE.
	modules   map[string]*Module // Available modules
	curRunner *Runner            // Runner that's currently running in the iso
	pool      *VMPool            // Pool to return this to, or nil
}

// Constructs an Environment given the configuration.
func NewVM(modules map[string]ModuleConfig) (*VM, error) {
	// Create a V8 VM ("isolate"):
	iso := v8.NewIsolate()
	vm := &VM{
		iso:     iso,
		modules: map[string]*Module{},
	}
	for name, config := range modules {
		var err error
		vm.modules[name], err = newModule(config, name, vm)
		if err != nil {
			return nil, err
		}
	}
	return vm, nil
}

// Cleans up a VM by disposing the V8 runtime.
// You should call this when you're done with it.
func (vm *VM) Close() {
	if vm.pool != nil {
		panic("closing a js.VM that belongs to a Pool")
	}
	vm.iso.Dispose()
	vm.iso = nil
}

// Call this when finished using a VM. Returns it to the VMPool it came from, or else closes it.
func (vm *VM) Return() {
	if vm.pool != nil {
		vm.pool.ReturnVM(vm)
	} else {
		vm.Close()
	}
}

// Returns the Runner that owns the given V8 Context.
func (vm *VM) GetRunner(ctx *v8.Context) *Runner {
	// IMPORTANT: This is kind of a hack, because we expect only one context at a time.
	// If there were to be multiple Evaluators, we'd need to maintain a map from contexts to Evaluators.
	if vm.curRunner == nil {
		panic(fmt.Sprintf("Unknown v8.Context passed to VM.GetRunner: %v, expected none", ctx))
	}
	if ctx != vm.curRunner.ctx {
		panic(fmt.Sprintf("Unknown v8.Context passed to VM.GetRunner: %v, expected %v", ctx, vm.curRunner.ctx))
	}
	return vm.curRunner
}

// Wraps a Go function in a V8 function template.
func (vm *VM) CreateCallback(callback ModuleCallback) *v8.FunctionTemplate {
	return v8.NewFunctionTemplate(vm.iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		c := vm.GetRunner(info.Context())
		result, err := callback(c, info)
		if err == nil {
			if result != nil {
				var v8Result *v8.Value
				if v8Result, err = v8.NewValue(vm.iso, result); err == nil {
					return v8Result
				}
			} else {
				return v8.Undefined(vm.iso)
			}
		}
		return v8Throw(vm.iso, err)
	})
}

//////// VM POOL

// Thread-safe wrapper that vends `VM` objects.
type VMPool struct {
	modules map[string]ModuleConfig
	vms     chan *VM
}

// Initializes a `vmPool`, a thread-safe wrapper around `VM`.
// `maxEnvironments` is the maximum number of `VM` objects (and V8 instances!) it will cache.
func (pool *VMPool) Init(maxVMs int) {
	pool.modules = map[string]ModuleConfig{}
	pool.vms = make(chan *VM, maxVMs)

}

func (pool *VMPool) AddModule(name string, mod ModuleConfig) {
	pool.modules[name] = mod
}

// Produces a `VM` that can be used by this goroutine.
// It should be returned when done by calling `returnEvaluator` to avoid wasting memory.
func (pool *VMPool) GetVM() (*VM, error) {
	var vm *VM
	select {
	case vm = <-pool.vms:
		break
	default:
		// If pool is empty, create a new VM.
		// (If this bloats memory, it might be better to block until one is returned...)
		var err error
		if vm, err = NewVM(pool.modules); err != nil {
			return nil, err
		}
		vm.pool = pool
	}
	return vm, nil
}

// Instantiates a Runner for a named Module.
func (pool *VMPool) GetRunner(moduleName string) (*Runner, error) {
	vm, err := pool.GetVM()
	if err != nil {
		return nil, err
	}
	runner, err := vm.NewRunner(moduleName)
	if err != nil {
		pool.ReturnVM(vm)
	}
	return runner, err
}

// Returns a used `VM` back to the pool for reuse.
func (pool *VMPool) ReturnVM(vm *VM) {
	if vm != nil && vm.pool == pool {
		select {
		case pool.vms <- vm:
		default:
			// Drop it on the floor if the pool is already full
			vm.pool = nil
			vm.Close()
		}
	}
}

func (pool *VMPool) WithRunner(moduleName string, fn func(*Runner) (any, error)) (any, error) {
	runner, err := pool.GetRunner(moduleName)
	if err != nil {
		return nil, err
	}
	defer runner.Close()
	return fn(runner)
}

// Frees up the cached V8 VMs of an `vmPool`
func (pool *VMPool) Close() {
	for {
		select {
		case vm := <-pool.vms:
			vm.pool = nil
			vm.Close()
		default:
			return
		}
	}
}
