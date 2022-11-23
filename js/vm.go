package js

import (
	_ "embed"
	"fmt"

	"github.com/pkg/errors"
	v8 "rogchap.com/v8go"
)

// v8go docs:			https://pkg.go.dev/rogchap.com/v8go
// General V8 API docs: https://v8.dev/docs/embed

// Top-level object that represents a V8 JavaScript VM (aka "Isolate".)
// This doesn't do much on its own; execution in V8 occurs in "Contexts" within an Isolate
// which are sandboxes with their own globals that can't interact with each other.
//
// **Not thread-safe!** Methods of a VM must be called only on one goroutine at a time.
// The VMPool takes care of this by vending VM instances that are known not to be in use.
type VM struct {
	iso          *v8.Isolate           // A V8 virtual machine. NOT THREAD SAFE.
	underscore   *v8.UnboundScript     // Precompiled Underscore.js library
	config       ServicesConfiguration // Factory for services
	services     map[string]Service    // Available services
	runners      map[string]*Runner    // Available runners
	curRunner    *Runner               // Runner that's currently running in the iso
	returnToPool *VMPool               // Pool to return this to, or nil
}

type ServicesConfiguration map[string]ServiceFactory

// The Underscore.js utility library <https://underscorejs.org>
//
//go:embed underscore-umd.js
var underscoreJS string

// Creates an Environment that can run a set of Services.
func NewVM(services ServicesConfiguration) *VM {
	return &VM{
		iso:      v8.NewIsolate(),      // The V8 VM
		config:   services,             // Factory fn for each service
		services: map[string]Service{}, // Instantiated Services
		runners:  map[string]*Runner{}, // Cached reusable Runners
	}
}

// Call this when finished using a VM. Returns it to the VMPool it came from, or else closes it.
func (vm *VM) Release() {
	if vm.returnToPool != nil {
		vm.returnToPool.returnVM(vm)
	} else {
		// If it doesn't bleong to a pool, really dispose it:
		for _, runner := range vm.runners {
			runner.close()
		}
		vm.iso.Dispose()
		vm.iso = nil
	}
}

// Instantiates the named Service or returns the existing instance.
func (vm *VM) getService(serviceName string) (Service, error) {
	service := vm.services[serviceName]
	if service == nil {
		factory := vm.config[serviceName]
		if factory == nil {
			return nil, fmt.Errorf("js.VM has no service %q", serviceName)
		}

		// Compile Underscore.js if it's not already done:
		if vm.underscore == nil {
			// Include the Underscore.js library:
			var err error
			vm.underscore, err = vm.iso.CompileUnboundScript(underscoreJS, "underscore-umd.js", v8.CompileOptions{})
			if err != nil {
				return nil, errors.Wrapf(err, "Couldn't compile underscore.js: %w", err)
			}
		}

		base := &BasicService{
			name:   serviceName,
			vm:     vm,
			global: v8.NewObjectTemplate(vm.iso),
		}
		var err error
		service, err = factory(base)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to initialize JS service %q: %w", serviceName, err)
		} else if service == nil {
			return nil, fmt.Errorf("js.ServiceFactory %q returned nil", serviceName)
		} else if service.Script() == nil {
			return nil, fmt.Errorf("js.ServiceFactory %q failed to initialize Service's script", serviceName)
		}
		vm.services[serviceName] = service
	}
	return service, nil
}

// Produces a Runner object that can run the given service.
// Be sure to call Runner.Return when done.
func (vm *VM) GetRunner(serviceName string) (*Runner, error) {
	runner := vm.runners[serviceName]
	if runner == nil {
		service, err := vm.getService(serviceName)
		if service == nil {
			return nil, err
		}
		runner, err = newRunner(vm, service)
		if err != nil {
			return nil, err
		}
	}
	vm.curRunner = runner
	return runner, nil
}

// Called by Runner.Return; either closes its V8 resources or saves it for reuse.
// Also returns the VM to its Pool (or closes it.)
func (vm *VM) returnRunner(r *Runner) {
	if vm.curRunner == r {
		vm.curRunner = nil
	} else if r.vm != vm {
		panic("Runner returned to wrong VM!")
	}
	if r.service.Reusable() {
		vm.runners[r.service.Name()] = r
	} else {
		r.close()
	}
	vm.Release()
}

// Returns the Runner that owns the given V8 Context.
func (vm *VM) currentRunner(ctx *v8.Context) *Runner {
	// IMPORTANT: This is kind of a hack, but we can get away with it because a VM has only one
	// active Runner at a time. If it were to be multiple Runners, we'd need to maintain a map
	// from Contexts to Runners.
	if vm.curRunner == nil {
		panic(fmt.Sprintf("Unknown v8.Context passed to VM.currentRunner: %v, expected none", ctx))
	}
	if ctx != vm.curRunner.ctx {
		panic(fmt.Sprintf("Unknown v8.Context passed to VM.currentRunner: %v, expected %v", ctx, vm.curRunner.ctx))
	}
	return vm.curRunner
}

// Wraps a Go function in a V8 function template.
func (vm *VM) NewCallback(callback ServiceCallback) *v8.FunctionTemplate {
	return v8.NewFunctionTemplate(vm.iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		c := vm.currentRunner(info.Context())
		result, err := callback(c, info)
		if err == nil {
			if result != nil {
				var v8Result *v8.Value
				switch result := result.(type) {
				case *v8.Value:
					return result
				default:
					if v8Result, err = v8.NewValue(vm.iso, result); err == nil {
						return v8Result
					}
				}
			} else {
				return v8.Undefined(vm.iso)
			}
		}
		return v8Throw(vm.iso, err)
	})
}

func (vm *VM) NewObjectTemplate() *v8.ObjectTemplate { return v8.NewObjectTemplate(vm.iso) }
func (vm *VM) NewString(str string) *v8.Value        { return mustSucceed(v8.NewValue(vm.iso, str)) }

// Converts a Go value to a v8.Value. Must be a number, string, bool or nil.
func (vm *VM) NewValue(val any) (*v8.Value, error) {
	if val != nil {
		return v8.NewValue(vm.iso, val)
	} else {
		return v8.Undefined(vm.iso), nil
	}
}

// Converts a Go value or a v8.Value or v8.Object to a v8.Value
func (vm *VM) AsValue(val any) (*v8.Value, error) {
	switch val := val.(type) {
	case *v8.Value:
		return val, nil
	case *v8.Object:
		return val.Value, nil
	default:
		return vm.NewValue(val)
	}
}
