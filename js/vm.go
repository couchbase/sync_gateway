/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package js

import (
	"context"
	"time"
)

//////// ENGINE

// An opaque object identifying a JavaScript engine (V8 or Otto)
type Engine struct {
	name            string
	languageVersion int
	factory         func(*Engine, *servicesConfiguration) VM
}

// The name identifying this engine ("V8" or "Otto")
func (engine *Engine) String() string { return engine.name }

// The edition number of the ECMAScript spec supported by this Engine.
// For example a value of 6 is 6th Edition, better known as ES2015, aka "Modern JavaScript".
// See https://en.wikipedia.org/wiki/ECMAScript_version_history
func (engine *Engine) LanguageVersion() int { return engine.languageVersion }

// Language version (ECMAScript edition #) of ES2015
const ES2015 = 6

// Creates a JavaScript virtual machine of the given type.
// This object should be used only on a single goroutine at a time.
func (engine *Engine) NewVM() VM {
	return engine.newVM(&servicesConfiguration{})
}

func (engine *Engine) newVM(services *servicesConfiguration) VM {
	return engine.factory(engine, services)
}

//////// VM

// Represents a single-threaded JavaScript virtual machine.
// This doesn't do much on its own; it acts as a ServiceHost for Service and Runner objects.
//
// **Not thread-safe!** A VM instance must be used only on one goroutine at a time.
// A Service whose ServiceHost is a VM can only be used on a single goroutine; any concurrent
// use will trigger a panic in VM.getRunner.
// The VMPool takes care of this, by vending VM instances that are known not to be in use.
type VM interface {
	Engine() *Engine
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

//////// BASEVM

// A base "class" containing shared properties and methods for use by VM implementations.
type baseVM struct {
	engine       *Engine
	services     *servicesConfiguration // Factories for services
	returnToPool *VMPool                // Pool to return me to, or nil
	lastReturned time.Time              // Time that v8VM was last returned to its pool
	closed       bool
}

func (vm *baseVM) Engine() *Engine { return vm.engine }

func (vm *baseVM) close() {
	if vm.returnToPool != nil {
		panic("Don't Close a VM that belongs to a VMPool")
	}
	vm.services = nil
	vm.closed = true
}

func (vm *baseVM) registerService(service *Service) {
	if vm.services == nil {
		if vm.closed {
			panic("Using an already-closed js.VM")
		} else {
			panic("You forgot to initialize a js.VM") // Must call NewVM()
		}
	}
	vm.services.addService(service)
}

func (vm *baseVM) setReturnToPool(pool *VMPool) { vm.returnToPool = pool }
func (vm *baseVM) getReturnToPool() *VMPool     { return vm.returnToPool }
func (vm *baseVM) getLastReturned() time.Time   { return vm.lastReturned }
