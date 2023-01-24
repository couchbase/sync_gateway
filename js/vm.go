package js

import (
	"time"
)

// Represents a single-threaded JavaScript virtual machine.
// This doesn't do much on its own; it acts as a ServiceHost for Service and Runner objects.
//
// **Not thread-safe!** A VM instance must be used only on one goroutine at a time.
// A Service whose ServiceHost is a VM can only be used on a single goroutine; any concurrent
// use will trigger a panic in VM.getRunner.
// The VMPool takes care of this, by vending VM instances that are known not to be in use.
type VM interface {
	Close()
	FindService(name string) *Service
	ValidateJavascriptFunction(jsFunc string, minArgs int, maxArgs int) error

	registerService(factory TemplateFactory, name string) serviceID
	hasInitializedService(*Service) bool
	getRunner(*Service) (Runner, error)
	withRunner(*Service, func(Runner) (any, error)) (any, error)
	setReturnToPool(*VMPool)
	getReturnToPool() *VMPool
	getLastReturned() time.Time
}

type baseVM struct {
	services     *servicesConfiguration // Factories for services
	returnToPool *VMPool                // Pool to return me to, or nil
	lastReturned time.Time              // Time that v8VM was last returned to its pool
}

func (vm *baseVM) close() {
	if vm.returnToPool != nil {
		panic("Don't Close a VM that belongs to a VMPool")
	}
}

func (vm *baseVM) setReturnToPool(pool *VMPool) { vm.returnToPool = pool }
func (vm *baseVM) getReturnToPool() *VMPool     { return vm.returnToPool }
func (vm *baseVM) getLastReturned() time.Time   { return vm.lastReturned }
