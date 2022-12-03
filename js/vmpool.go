package js

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/couchbase/sync_gateway/base"
)

//////// VM POOL

// A thread-safe host for Services and Runners that owns a set of VMs and allocates an available
// one when a Runner is needed.
type VMPool struct {
	vms      chan *VM               // Cache of idle VMs
	counter  chan int               // Each item in this channel represents availability of a VM
	services *servicesConfiguration // Defines the services
	maxVMs   int                    // Max number of simultaneously in-use VMs
	inUse    int32                  // Number of VMs currently in use ("checked out".) *ATOMIC*
}

// Creates a `VMPool`, a thread-safe host for Services and Runners
// `maxVMs` is the maximum number of V8 instances (VM objects) it will provide; a reasonable value
// for this is the number of CPU cores.
func NewVMPool(maxVMs int) *VMPool {
	pool := new(VMPool)
	pool.Init(maxVMs)
	return pool
}

// Initializes a `VMPool`, a thread-safe host for Services and Runners
// `maxVMs` is the maximum number of V8 instances (VM objects) it will provide; a reasonable value
// for this is the number of CPU cores.
func (pool *VMPool) Init(maxVMs int) {
	pool.maxVMs = maxVMs
	pool.vms = make(chan *VM, maxVMs)
	pool.services = &servicesConfiguration{}
	pool.counter = make(chan int, maxVMs)
	for i := 0; i < maxVMs; i++ {
		pool.counter <- i
	}
	base.InfofCtx(context.Background(), base.KeyJavascript, "js.VMPool: Init, max %d VMs", maxVMs)
}

// Tears down a VMPool, freeing up its cached V8 VMs.
// It's a good idea to call this, as the VMs may be holding onto a lot of memory and this will
// clean up that memory sooner than Go's GC will.
func (pool *VMPool) Close() {
	if inUse := pool.InUseCount(); inUse > 0 {
		base.WarnfCtx(context.Background(), "A js.VMPool is being closed with %d VMs still in use", inUse)
	}

	// First stop all waiting `Get` calls:
	close(pool.counter)

	// Now pull all the VMs out of the pool and close them.
	// This isn't necessary, but it frees up memory sooner.
	for pool.popAndClose() {
	}
}

// Returns the number of VMs currently in use.
func (pool *VMPool) InUseCount() int { return int(atomic.LoadInt32(&pool.inUse)) }

// Closes all idle V8 VMs cached by this pool. It will reallocate them when it needs to.
func (pool *VMPool) PurgeUnusedVMs() {
	for i := pool.InUseCount(); i >= 0 && pool.popAndClose(); i-- {
	}
}

//////// INTERNALS:

func (pool *VMPool) registerService(factory TemplateFactory) serviceID {
	if pool.services == nil {
		panic("You are calling an uninitialized VMPool")
	}
	return pool.services.addService(factory)
}

// Produces an idle `VM` that can be used by this goroutine.
// You MUST call returnVM, or VM.release, when done.
func (pool *VMPool) getVM() (*VM, error) {
	// Read from the channel; this blocks until less than `maxVMs` VMs are in use:
	if _, ok := <-pool.counter; !ok {
		return nil, fmt.Errorf("the VMPool has been closed")
	}

	// Pop a VM from the channel:
	vm := pool.pop()
	if vm == nil {
		// Nothing in the pool, so create a new VM instance.
		vm = newVM(pool.services)
	}

	vm.returnToPool = pool
	inUse := atomic.AddInt32(&pool.inUse, 1)
	base.DebugfCtx(context.Background(), base.KeyJavascript, "js.VMPool.Get: %d/%d VMs now in use", inUse, pool.maxVMs)
	return vm, nil
}

// Returns a used `VM` back to the pool for reuse; called by VM.Return
func (pool *VMPool) returnVM(vm *VM) {
	if vm != nil && vm.returnToPool == pool {
		vm.returnToPool = nil
		pool.push(vm)
		// Write to the channel to signify another VM is available:
		inUse := atomic.AddInt32(&pool.inUse, -1)
		base.DebugfCtx(context.Background(), base.KeyJavascript, "js.VMPool.return: %d/%d VMs now in use", inUse, pool.maxVMs)
		pool.counter <- 0
	}
}

// Instantiates a Runner for a named Service, in an available VM.
// You MUST call Runner.Return when done -- it will return the associated VM too.
func (pool *VMPool) getRunner(service *Service) (*Runner, error) {
	if vm, err := pool.getVM(); err != nil {
		return nil, err
	} else if runner, err := vm.getRunner(service); err != nil {
		pool.returnVM(vm)
		return nil, err
	} else {
		return runner, err
	}
}

//////// Low-level pool management -- uses a channel as a thread-safe queue of available VMs.

// just adds a VM to the pool
func (pool *VMPool) push(vm *VM) {
	pool.vms <- vm
}

// just gets a VM from the pool
func (pool *VMPool) pop() *VM {
	select {
	case vm := <-pool.vms:
		return vm
	default:
		return nil
	}
}

// gets a VM from the pool and closes the VM. Returns false if none are left.
func (pool *VMPool) popAndClose() bool {
	if vm := pool.pop(); vm != nil {
		vm.returnToPool = nil
		vm.release() // actually closes it
		return true
	} else {
		return false
	}

}
