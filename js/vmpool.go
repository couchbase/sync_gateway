package js

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/couchbase/sync_gateway/base"
)

//////// VM POOL

// Thread-safe wrapper that vends `VM` objects.
type VMPool struct {
	vms        sync.Pool // Collection of *weak* references to unused VMs
	counter    chan int  // Each item in this channel represents availability of a VM
	maxVMs     int
	inUse      int32 // Number of VMs currently in use ("checked out".) *ATOMIC*
	numCreated int32
	services   ServicesConfiguration // Defines the services
}

// Initializes a `VMPool`, a thread-safe wrapper around a set of `VM`s.
// `maxVMs` is the maximum number of `VM` objects (and V8 instances!) it will provide.
func (pool *VMPool) Init(maxVMs int) {
	pool.maxVMs = maxVMs
	pool.services = ServicesConfiguration{}
	pool.counter = make(chan int, maxVMs)
	for i := 0; i < maxVMs; i++ {
		pool.counter <- i
	}
	base.InfofCtx(context.Background(), base.KeyJavascript, "js.VMPool: Init, max %d VMs", maxVMs)
}

// Adds a new Service. Only call this before instantiating any VMs.
// This method is NOT thread-safe.
func (pool *VMPool) AddService(name string, s ServiceFactory) {
	if pool.services[name] != nil {
		panic(fmt.Sprintf("Duplicate Service name %q", name))
	}
	pool.services[name] = s
}

// Produces an idle `VM` that can be used by this goroutine.
// You MUST call VM.Return when done.
func (pool *VMPool) GetVM() (*VM, error) {
	// Read from the channel; this blocks until less than `maxVMs` VMs are in use:
	if _, ok := <-pool.counter; !ok {
		return nil, fmt.Errorf("the VMPool has been closed")
	}

	// Pop a VM from the channel:
	vm, ok := pool.vms.Get().(*VM)
	if !ok {
		// Nothing in the pool, so create a new VM instance.
		vm = NewVM(pool.services)
		atomic.AddInt32(&pool.numCreated, 1)
	}
	vm.returnToPool = pool
	inUse := atomic.AddInt32(&pool.inUse, 1)
	base.InfofCtx(context.Background(), base.KeyJavascript, "js.VMPool.Get: %d/%d VMs now in use", inUse, pool.maxVMs)
	return vm, nil
}

// Returns a used `VM` back to the pool for reuse; called by VM.Return
func (pool *VMPool) returnVM(vm *VM) {
	if vm != nil && vm.returnToPool == pool {
		vm.returnToPool = nil
		pool.vms.Put(vm)
		// Write to the channel to signify another VM is available:
		inUse := atomic.AddInt32(&pool.inUse, -1)
		base.InfofCtx(context.Background(), base.KeyJavascript, "js.VMPool.return: %d/%d VMs now in use", inUse, pool.maxVMs)
		pool.counter <- 0
	}
}

// Instantiates a Runner for a named Service, in an available VM.
// You MUST call Runner.Return when done -- it will return the associated VM too.
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

// Invokes `fn`, passing it a Runner for the given serviceName. Returns whatever `fn` does.
func (pool *VMPool) WithRunner(serviceName string, fn func(*Runner) (any, error)) (any, error) {
	runner, err := pool.GetRunner(serviceName)
	if err != nil {
		return nil, err
	}
	defer runner.Return()
	return fn(runner)
}

// Returns the number of VMs currently in use
func (pool *VMPool) InUseCount() int { return int(pool.inUse) }

// Tears down a VMPool, freeing up its cached V8 VMs.
// It's a good idea to call this, as the VMs may be holding onto a lot of memory and this will
// clean up that memory sooner than Go's GC will.
func (pool *VMPool) Close() {
	base.InfofCtx(context.Background(), base.KeyJavascript, "Closing js.VMPool that created %d VMs (max %d at a time)", atomic.LoadInt32(&pool.numCreated), pool.maxVMs)
	if inUse := atomic.LoadInt32(&pool.inUse); inUse > 0 {
		base.WarnfCtx(context.Background(), "A js.VMPool is being closed with %d VMs still in use", inUse)
	}

	// First stop all waiting `Get` calls:
	close(pool.counter)

	// Now pull all the VMs out of the pool and close them.
	// This isn't necessary but it frees up memory sooner.
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
