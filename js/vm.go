package js

import (
	"context"
	"time"
)

// An opaque object identifying a JavaScript engine (V8 or Otto)
type VMType struct {
	name    string
	factory func(*servicesConfiguration) VM
}

func (vmType *VMType) String() string { return vmType.name }

// Represents a single-threaded JavaScript virtual machine.
// This doesn't do much on its own; it acts as a ServiceHost for Service and Runner objects.
//
// **Not thread-safe!** A VM instance must be used only on one goroutine at a time.
// A Service whose ServiceHost is a VM can only be used on a single goroutine; any concurrent
// use will trigger a panic in VM.getRunner.
// The VMPool takes care of this, by vending VM instances that are known not to be in use.
type VM interface {
	Type() *VMType
	Close()
	FindService(name string) *Service

	registerService(*Service)
	hasInitializedService(*Service) bool
	getRunner(*Service) (Runner, error)
	withRunner(*Service, func(Runner) (any, error)) (any, error)
	setReturnToPool(*VMPool)
	getReturnToPool() *VMPool
	getLastReturned() time.Time
}

// Syntax-checks a string containing a JavaScript function definition
func ValidateJavascriptFunction(vm VM, jsFunc string, minArgs int, maxArgs int) error {
	service := vm.FindService("ValidateJavascriptFunction")
	if service == nil {
		service = NewService(vm, "ValidateJavascriptFunction", `
			function(jsFunc, minArgs, maxArgs) {
				var fn = Function('"use strict"; return ' + jsFunc)()
				var typ = typeof(fn);
				if (typ !== 'function') {
					throw "code is not a function, but a " + typ;
				} else if (fn.length < minArgs) {
					throw "function must have at least " + minArgs + " parameters";
				} else if (fn.length > maxArgs) {
					throw "function must have no more than " + maxArgs + " parameters";
				}
			}
		`)
	}
	_, err := service.Run(context.Background(), jsFunc, minArgs, maxArgs)
	return err
}

// Creates a JavaScript virtual machine of the given type.
// This object should be used only on a single goroutine at a time.
func NewVM(typ *VMType) VM {
	return typ.factory(&servicesConfiguration{})
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

func (vm *baseVM) registerService(service *Service) { vm.services.addService(service) }
func (vm *baseVM) setReturnToPool(pool *VMPool)     { vm.returnToPool = pool }
func (vm *baseVM) getReturnToPool() *VMPool         { return vm.returnToPool }
func (vm *baseVM) getLastReturned() time.Time       { return vm.lastReturned }
