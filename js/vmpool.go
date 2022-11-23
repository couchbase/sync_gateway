package js

import (
	"fmt"
	"sync"
)

//////// VM POOL

// Thread-safe wrapper that vends `VM` objects.
type VMPool struct {
	services ServicesConfiguration
	vms      sync.Pool
}

// Initializes a `VMPool`, a thread-safe wrapper around `VM`.
// `maxVMs` is the maximum number of `VM` objects (and V8 instances!) it will cache.
func (pool *VMPool) Init(maxVMs int) {
	pool.services = ServicesConfiguration{}
}

// Adds a new Service. Only call this before instantiating any VMs.
// This method is NOT thread-safe.
func (pool *VMPool) AddService(name string, s ServiceFactory) {
	if pool.services[name] != nil {
		panic(fmt.Sprintf("Duplicate Service name %q", name))
	}
	pool.services[name] = s
}

// Produces a `VM` that can be used by this goroutine.
// You MUST call VM.Return when done.
func (pool *VMPool) GetVM() (*VM, error) {
	// Pop a VM from the channel:
	vm, ok := pool.vms.Get().(*VM)
	if !ok {
		// Nothing in the pool, so create a real VM instance:
		vm = NewVM(pool.services)
	}
	vm.returnToPool = pool
	return vm, nil
}

// Returns a used `VM` back to the pool for reuse; called by VM.Return
func (pool *VMPool) returnVM(vm *VM) {
	if vm != nil && vm.returnToPool == pool {
		vm.returnToPool = nil
		pool.vms.Put(vm)
	}
}

// Instantiates a Runner for a named Service, in an available VM.
// You MUST call Runner.Return when done (this will return the associated VM too.)
func (pool *VMPool) GetRunner(serviceName string) (*Runner, error) {
	if vm, err := pool.GetVM(); err != nil {
		return nil, err
	} else if runner, err := vm.GetRunner(serviceName); err != nil {
		pool.returnVM(vm)
		return nil, err
	} else {
		return runner, err
	}
}

// Invokes `fn`, passing it a Runner for the given serviceName, then returns the Runner.
// Returns whatever `fn` does.
func (pool *VMPool) WithRunner(serviceName string, fn func(*Runner) (any, error)) (any, error) {
	runner, err := pool.GetRunner(serviceName)
	if err != nil {
		return nil, err
	}
	defer runner.Return()
	return fn(runner)
}

// Tears down a VMPool, freeing up its cached V8 VMs.
// It's a good idea to call this, as the VMs may be holding onto a lot of memory and this will
// clean up that memory sooner than Go's GC will.
func (pool *VMPool) Close() {
	for {
		if x := pool.vms.Get(); x != nil {
			vm := x.(*VM)
			vm.returnToPool = nil
			vm.Release() // actually closes it
		} else {
			break
		}
	}
}
