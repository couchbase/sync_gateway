package js

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

//////// VM POOL

// A thread-safe ServiceHost for Services and Runners that owns a set of VMs
// and allocates an available one when a Runner is needed.
type VMPool struct {
	maxInUse  int                    // Max number of simultaneously in-use VMs
	services  *servicesConfiguration // Defines the services (owned VMs also have references)
	tickets   chan bool              // Each item in this channel represents availability of a VM
	mutex     sync.Mutex             // Must be locked to access fields below
	vms_      []*VM                  // LIFO cache of idle VMs, recently used at end
	curInUse_ int                    // Current number of VMs "checked out"
}

// Creates a `VMPool`, a thread-safe ServiceHost for Services and Runners.
// `maxVMs` is the maximum number of V8 instances (VM objects) it will provide; a reasonable value
// for this is the number of CPU cores.
func NewVMPool(maxVMs int) *VMPool {
	pool := new(VMPool)
	pool.Init(maxVMs)
	return pool
}

// Initializes a `VMPool`, a thread-safe ServiceHost for Services and Runners
// `maxVMs` is the maximum number of V8 instances (VM objects) it will provide; a reasonable value
// for this is the number of CPU cores.
func (pool *VMPool) Init(maxVMs int) {
	pool.maxInUse = maxVMs
	pool.services = &servicesConfiguration{}
	pool.vms_ = make([]*VM, 0, maxVMs)
	pool.tickets = make(chan bool, maxVMs)
	for i := 0; i < maxVMs; i++ {
		pool.tickets <- true
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
	close(pool.tickets)

	// Now pull all the VMs out of the pool and close them.
	// This isn't necessary, but it frees up memory sooner.
	n := pool.closeAll()
	base.InfofCtx(context.Background(), base.KeyJavascript,
		"js.VMPool.Close: Closed pool with %d V8 VM(s)", n)
}

// Returns the number of VMs currently in use.
func (pool *VMPool) InUseCount() int {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	return pool.curInUse_
}

// Closes all idle V8 VMs cached by this pool. It will reallocate them when it needs to.
func (pool *VMPool) PurgeUnusedVMs() {
	n := pool.closeAll()
	base.InfofCtx(context.Background(), base.KeyJavascript,
		"js.VMPool.PurgeUnusedVMs: Closed %d idle V8 VM(s)", n)
}

//////// INTERNALS:

func (pool *VMPool) registerService(factory TemplateFactory) serviceID {
	if pool.services == nil {
		panic("You forgot to initialize a VMPool") // failed to call Init or NewVMPool
	}
	return pool.services.addService(factory)
}

// Produces an idle `VM` that can be used by this goroutine.
// You MUST call returnVM, or VM.release, when done.
func (pool *VMPool) getVM(service *Service) (*VM, error) {
	// Pull a ticket; this blocks until less than `maxVMs` VMs are in use:
	if _, ok := <-pool.tickets; !ok {
		return nil, fmt.Errorf("the VMPool has been closed")
	}

	// Pop a VM from the channel:
	vm, inUse := pool.pop(service)
	if vm == nil {
		// Nothing in the pool, so create a new VM instance.
		vm = newVM(pool.services)
		base.InfofCtx(context.Background(), base.KeyJavascript,
			"js.VMPool.getVM: No VMs free; created a new one")
	}

	vm.returnToPool = pool
	base.DebugfCtx(context.Background(), base.KeyJavascript,
		"js.VMPool.getVM: %d/%d VMs now in use", inUse, pool.maxInUse)
	return vm, nil
}

// Returns a used `VM` back to the pool for reuse; called by VM.Return
func (pool *VMPool) returnVM(vm *VM) {
	if vm != nil && vm.returnToPool == pool {
		vm.returnToPool = nil

		inUse := pool.push(vm)
		base.DebugfCtx(context.Background(), base.KeyJavascript,
			"js.VMPool.returnVM: %d/%d VMs now in use", inUse, pool.maxInUse)

		// Return a ticket to the channel:
		pool.tickets <- true
	}
}

// Instantiates a Runner for a named Service, in an available VM.
// You MUST call Runner.Return when done -- it will return the associated VM too.
func (pool *VMPool) getRunner(service *Service) (*Runner, error) {
	if vm, err := pool.getVM(service); err != nil {
		return nil, err
	} else if runner, err := vm.getRunner(service); err != nil {
		pool.returnVM(vm)
		return nil, err
	} else {
		return runner, err
	}
}

//////// POOL MANAGEMENT --  The low level stuff that requires a mutex.

// A VMPool will stop caching a VM that hasn't been used for this long.
const kVMStaleDuration = time.Minute

// Just gets a VM from the pool, and increments the in-use count. Thread-safe.
func (pool *VMPool) pop(service *Service) (vm *VM, inUse int) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if n := len(pool.vms_); n > 0 {
		pool.curInUse_ += 1
		inUse = pool.curInUse_

		// Find the most recently-used VM that already has this service:
		vms := pool.vms_
		var i int
		for i = n - 1; i >= 0; i -= 1 {
			if vms[i].hasInitializedService(service) {
				break
			}
		}
		if i < 0 {
			// If no VM has this service, choose the most recently used one:
			i = n - 1
		}

		// Delete this VM from the array before returning it:
		vm = vms[i]
		copy(vms[i:], vms[i+1:])
		vms = vms[:n-1]

		// If the oldest VM in the pool hasn't been used in a while, get rid of it:
		if n > 1 {
			oldest := vms[0]
			if stale := time.Since(oldest.lastReturned); stale > kVMStaleDuration {
				vms = vms[1:]
				oldest.returnToPool = nil
				oldest.Close()
				base.DebugfCtx(context.Background(), base.KeyJavascript,
					"js.VMPool.pop: Disposed a stale VM not used in %v", stale)
			}
		}
		pool.vms_ = vms
	}
	return
}

// Just adds a VM to the pool, and decrements the in-use count. Thread-safe.
func (pool *VMPool) push(vm *VM) (inUse int) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	vm.lastReturned = time.Now()
	pool.vms_ = append(pool.vms_, vm)
	pool.curInUse_ -= 1
	return pool.curInUse_
}

// Removes all idle VMs from the pool and closes them. (Does not alter the in-use count.)
func (pool *VMPool) closeAll() int {
	pool.mutex.Lock()
	vms := pool.vms_
	pool.vms_ = nil
	pool.mutex.Unlock()

	for _, vm := range vms {
		vm.returnToPool = nil
		vm.Close()
	}
	return len(vms)
}
