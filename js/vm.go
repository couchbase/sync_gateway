package js

import (
	_ "embed"
	"fmt"
	"sync"

	v8 "rogchap.com/v8go" // Docs: https://pkg.go.dev/rogchap.com/v8go
)

// Represents a V8 JavaScript VM (aka "Isolate".)
// This doesn't do much on its own; its purpose is to perform shared initialization that makes creating an context faster.
// **Not thread-safe! Must be called only on one goroutine at a time.**
type VM struct {
	iso          *v8.Isolate           // A V8 virtual machine. NOT THREAD SAFE.
	config       ServicesConfiguration // Factory for services
	services     map[string]Service    // Available services
	runners      map[string]*Runner    // Available runners
	curRunner    *Runner               // Runner that's currently running in the iso
	returnToPool *VMPool               // Pool to return this to, or nil
}

type ServicesConfiguration map[string]ServiceFactory

// Creates an Environment that can run a set of services.
func NewVM(services ServicesConfiguration) *VM {
	return &VM{
		iso:      v8.NewIsolate(),      // The V8 VM
		config:   services,             // Factory fn for each service
		services: map[string]Service{}, // Instantiated Services
		runners:  map[string]*Runner{}, // Cached reusable Runners
	}
}

// Cleans up a VM by disposing the V8 runtime.
// You should call this when you're done with it.
func (vm *VM) Close() {
	//log.Printf("*** VM.Close %p", vm)
	if vm.returnToPool != nil {
		panic("closing a js.VM that belongs to a Pool")
	}
	for _, runner := range vm.runners {
		runner.close()
	}
	vm.iso.Dispose()
	vm.iso = nil
}

// Call this when finished using a VM. Returns it to the VMPool it came from, or else closes it.
func (vm *VM) Return() {
	if vm.returnToPool != nil {
		vm.returnToPool.returnVM(vm)
	} else {
		vm.Close()
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
		base := &BasicService{
			name:   serviceName,
			vm:     vm,
			global: v8.NewObjectTemplate(vm.iso),
		}
		var err error
		service, err = factory(base)
		if err != nil {
			return nil, err
		}
		if service == nil {
			panic("js.ServiceFactory returned nil")
		} else if service.Script() == nil {
			panic("js.ServiceFactory did not initialize Service's script")
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
	vm.Return()
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

func (vm *VM) NewObjectTemplate() *v8.ObjectTemplate { return v8.NewObjectTemplate(vm.iso) }
func (vm *VM) NewString(str string) *v8.Value        { return mustSucceed(v8.NewValue(vm.iso, str)) }

func (vm *VM) NewValue(val any) (*v8.Value, error) {
	if val != nil {
		return v8.NewValue(vm.iso, val)
	} else {
		return v8.Undefined(vm.iso), nil
	}
}

//////// VM POOL

// Thread-safe wrapper that vends `VM` objects.
type VMPool struct {
	services ServicesConfiguration
	vms      sync.Pool
}

// Initializes a `vmPool`, a thread-safe wrapper around `VM`.
// `maxEnvironments` is the maximum number of `VM` objects (and V8 instances!) it will cache.
func (pool *VMPool) Init(maxVMs int) {
	pool.services = ServicesConfiguration{}
}

func (pool *VMPool) AddService(name string, s ServiceFactory) {
	if pool.services[name] != nil {
		panic(fmt.Sprintf("Duplicate Service name %q", name))
	}
	pool.services[name] = s
}

// Produces a `VM` that can be used by this goroutine.
// It should be returned when done by calling `returnEvaluator` to avoid wasting memory.
func (pool *VMPool) GetVM() (*VM, error) {
	// Pop a VM from the channel, or block until one's available:
	vm, ok := pool.vms.Get().(*VM)
	if !ok {
		// Nothing in the pool, so create a real VM instance:
		vm = NewVM(pool.services)
	}
	vm.returnToPool = pool
	return vm, nil
}

// Returns a used `VM` back to the pool for reuse.
func (pool *VMPool) returnVM(vm *VM) {
	if vm != nil && vm.returnToPool == pool {
		vm.returnToPool = nil
		pool.vms.Put(vm)
	}
}

// Instantiates a Runner for a named Service.
func (pool *VMPool) GetRunner(serviceName string) (*Runner, error) {
	vm, err := pool.GetVM()
	if err != nil {
		return nil, err
	}
	runner, err := vm.GetRunner(serviceName)
	if err != nil {
		pool.returnVM(vm)
	}
	return runner, err
}

func (pool *VMPool) WithRunner(serviceName string, fn func(*Runner) (any, error)) (any, error) {
	runner, err := pool.GetRunner(serviceName)
	if err != nil {
		return nil, err
	}
	defer runner.Return()
	return fn(runner)
}

// Frees up the cached V8 VMs of an `vmPool`
func (pool *VMPool) Close() {
	for {
		if vm := pool.vms.Get(); vm != nil {
			vm.(*VM).Close()
		} else {
			break
		}
	}
}
