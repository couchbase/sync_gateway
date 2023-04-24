//go:build cb_sg_v8

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
	_ "embed"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	v8 "github.com/snej/v8go"
)

// v8go docs:			https://pkg.go.dev/github.com/snej/v8go
// General V8 API docs: https://v8.dev/docs/embed

type v8VM struct {
	*baseVM
	iso         *v8.Isolate       // A V8 virtual machine. NOT THREAD SAFE.
	setupScript *v8.UnboundScript // JS code to set up a new v8.Context
	templates   []V8Template      // Already-created Templates, indexed by serviceID
	runners     []*V8Runner       // Available Runners, indexed by serviceID. nil if in-use
	curRunner   *V8Runner         // Currently active v8Runner, if any
}

// The initial heap size in bytes of a V8 VM (Isolate).
// Changing this variable affects subsequently created V8 VMs.
var V8InitialHeap uint64 = 4 * 1024 * 1024

// The maximum heap size in bytes of a V8 VM; if it exceeds this, the script is terminated.
// Changing this variable affects subsequently created V8 VMs.
var V8MaxHeap uint64 = 32 * 1024 * 1024

// The maximum size in bytes of V8's JavaScript call stack.
// If you change this, do so before creating any V8 VMs, otherwise it'll have no effect.
// (Warning: When I set this to values >= 800, V8 crashed the process on heap overflow,
// instead of just terminating the script. This was on macOS; YMMV on other platforms.)
var V8StackSizeLimit = 400 * 1024

const v8VMName = "V8"

// Returns the Engine with the given name, else nil.
// Valid names are "V8" and "Otto", which map to the instances `V8` and `Otto`.
func EngineNamed(name string) *Engine {
	switch name {
	case v8VMName:
		return V8
	case ottoVMName:
		return Otto
	default:
		return nil
	}
}

// A VMType for instantiating V8-based VMs and VMPools.
var V8 = &Engine{
	name:            v8VMName,
	languageVersion: 13, // ES2022. Can't find exact version on the V8 website --Jens 1/2023
	factory:         v8VMFactory,
}

var v8Init sync.Once

func v8VMFactory(engine *Engine, services *servicesConfiguration) VM {
	v8Init.Do(func() {
		v8.SetFlags(fmt.Sprintf("--stack_size=%d", V8StackSizeLimit/1024))
	})
	return &v8VM{
		baseVM:    &baseVM{engine: engine, services: services},
		iso:       v8.NewIsolateWith(V8InitialHeap, V8MaxHeap), // The V8 v8VM
		templates: []V8Template{},                              // Instantiated Services
		runners:   []*V8Runner{},                               // Cached reusable Runners
	}
}

// Shuts down a v8VM. It's a good idea to call this explicitly when done with it,
// _unless_ it belongs to a VMPool.
func (vm *v8VM) Close() {
	vm.baseVM.close()
	if cur := vm.curRunner; cur != nil {
		cur.Return()
	}
	for _, runner := range vm.runners {
		if runner != nil {
			runner.close()
		}
	}
	vm.templates = nil
	if vm.iso != nil {
		vm.iso.Dispose()
		vm.iso = nil
	}
}

// Looks up an already-registered service by name. Returns nil if not found.
func (vm *v8VM) FindService(name string) *Service {
	return vm.services.findServiceNamed(name)
}

//////// INTERNALS:

// Must be called when finished using a v8VM belonging to a VMPool!
// (Harmless no-op when called on a standalone v8VM.)
func (vm *v8VM) release() {
	if vm.returnToPool != nil {
		vm.lastReturned = time.Now()
		vm.returnToPool.returnVM(vm)
	}
}

// Returns a Template for the given Service.
func (vm *v8VM) getTemplate(service *Service) (V8Template, error) {
	var tmpl V8Template
	if !vm.services.hasService(service) {
		return nil, fmt.Errorf("unknown js.Service instance passed to VM")
	}
	if int(service.id) < len(vm.templates) {
		tmpl = vm.templates[service.id]
	}
	if tmpl == nil {
		if vm.setupScript == nil {
			// The setup script points console logging to SG, and loads the Underscore.js library:
			var err error
			vm.setupScript, err = vm.iso.CompileUnboundScript(kSetupLoggingJS+kUnderscoreJS, "setupScript.js", v8.CompileOptions{})
			if err != nil {
				return nil, errors.Wrapf(err, "Couldn't compile setup script")
			}
		}

		var err error
		tmpl, err = newV8Template(vm, service)
		if err != nil {
			return nil, err
		}

		for int(service.id) >= len(vm.templates) {
			vm.templates = append(vm.templates, nil)
		}
		vm.templates[service.id] = tmpl
	}
	return tmpl, nil
}

func (vm *v8VM) hasInitializedService(service *Service) bool {
	id := int(service.id)
	return id < len(vm.templates) && vm.templates[id] != nil
}

// Produces a v8Runner object that can run the given service.
// Be sure to call Runner.Return when done.
// Since v8VM is single-threaded, calling getRunner when a v8Runner already exists and hasn't been
// returned yet is assumed to be illegal concurrent access; it will trigger a panic.
func (vm *v8VM) getRunner(service *Service) (Runner, error) {
	if vm.iso == nil {
		return nil, fmt.Errorf("the js.VM has been closed")
	}
	if vm.curRunner != nil {
		panic("illegal access to v8VM: already has a v8Runner")
	}
	var runner *V8Runner
	index := int(service.id)
	if index < len(vm.runners) {
		runner = vm.runners[index]
		vm.runners[index] = nil
	}
	if runner == nil {
		tmpl, err := vm.getTemplate(service)
		if tmpl == nil {
			return nil, err
		}
		runner, err = newV8Runner(vm, tmpl, service.id)
		if err != nil {
			return nil, err
		}
	} else {
		runner.vm = vm
	}
	vm.curRunner = runner
	vm.iso.Lock()
	return runner, nil
}

func (vm *v8VM) withRunner(service *Service, fn func(Runner) (any, error)) (any, error) {
	runner, err := vm.getRunner(service)
	if err != nil {
		vm.release()
		return nil, err
	}
	defer runner.Return()
	var result any
	runner.(*V8Runner).WithTemporaryValues(func() {
		result, err = fn(runner)
	})
	return result, err
}

// Called by v8Runner.Return; either closes its V8 resources or saves it for reuse.
// Also returns the v8VM to its Pool, if it came from one.
func (vm *v8VM) returnRunner(r *V8Runner) {
	r.goContext = nil
	if vm.curRunner == r {
		vm.iso.Unlock()
		vm.curRunner = nil
	} else if r.vm != vm {
		panic("v8Runner returned to wrong v8VM!")
	}
	if r.template.Reusable() {
		for int(r.id) >= len(vm.runners) {
			vm.runners = append(vm.runners, nil)
		}
		vm.runners[r.id] = r
	} else {
		r.close()
	}
	vm.release()
}

// Returns the v8Runner that owns the given V8 Context.
func (vm *v8VM) currentRunner(ctx *v8.Context) *V8Runner {
	// IMPORTANT: This is kind of a hack, but we can get away with it because a v8VM has only one
	// active v8Runner at a time. If it were to be multiple Runners, we'd need to maintain a map
	// from Contexts to Runners.
	if vm.curRunner == nil {
		panic(fmt.Sprintf("Unknown v8.Context passed to v8VM.currentRunner: %v, expected none", ctx))
	}
	if ctx != vm.curRunner.ctx {
		panic(fmt.Sprintf("Unknown v8.Context passed to v8VM.currentRunner: %v, expected %v", ctx, vm.curRunner.ctx))
	}
	return vm.curRunner
}

// The Underscore.js utility library, version 1.13.6, downloaded 2022-Nov-23 from
// <https://underscorejs.org/underscore-umd-min.js>
//
//go:embed underscore-umd-min.js
var kUnderscoreJS string
