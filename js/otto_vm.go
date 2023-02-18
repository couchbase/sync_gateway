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
	"fmt"
	"time"
)

type ottoVM struct {
	*baseVM
	runners   []*OttoRunner // Available Runners, indexed by serviceID. nil if in-use
	curRunner *OttoRunner   // Currently active Runner, if any
}

const ottoVMName = "Otto"

// An Engine for instantiating Otto-based VMs and VMPools.
var Otto = &Engine{
	name:            ottoVMName,
	languageVersion: 5, // https://github.com/robertkrimen/otto#caveat-emptor
	factory: func(engine *Engine, services *servicesConfiguration) VM {
		return &ottoVM{
			baseVM:  &baseVM{engine: engine, services: services}, // "superclass"
			runners: []*OttoRunner{},                             // Cached reusable Runners
		}
	},
}

func (vm *ottoVM) Close() {
	vm.baseVM.close()
	if cur := vm.curRunner; cur != nil {
		cur.Return()
	}
	vm.curRunner = nil
	vm.runners = nil
}

// Looks up an already-registered service by name. Returns nil if not found.
func (vm *ottoVM) FindService(name string) *Service {
	return vm.services.findServiceNamed(name)
}

// Must be called when finished using a VM belonging to a VMPool!
// (Harmless no-op when called on a standalone VM.)
func (vm *ottoVM) release() {
	if vm.returnToPool != nil {
		vm.lastReturned = time.Now()
		vm.returnToPool.returnVM(vm)
	}
}

func (vm *ottoVM) hasInitializedService(service *Service) bool {
	id := int(service.id)
	return id < len(vm.runners) && vm.runners[id] != nil
}

func (vm *ottoVM) getRunner(service *Service) (Runner, error) {
	if vm.closed {
		return nil, fmt.Errorf("the js.VM has been closed")
	}
	if vm.curRunner != nil {
		panic("illegal access to v8VM: already has a v8Runner")
	}
	if !vm.services.hasService(service) {
		return nil, fmt.Errorf("unknown js.Service instance passed to VM")
	}
	if service.v8Init != nil {
		return nil, fmt.Errorf("js.Service has custom initialization not supported by Otto")
	}

	// Use an existing Runner or create a new one:
	var runner *OttoRunner
	if int(service.id) < len(vm.runners) {
		runner = vm.runners[service.id]
		vm.runners[service.id] = nil
	}
	if runner == nil {
		var err error
		runner, err = newOttoRunner(vm, service)
		if err != nil {
			return nil, fmt.Errorf("unexpected error initializing JavaScript service %q: %w", service.Name(), err)
		}
		for int(service.id) >= len(vm.runners) {
			vm.runners = append(vm.runners, nil)
		}
	}
	vm.curRunner = runner
	return runner, nil
}

func (vm *ottoVM) withRunner(service *Service, fn func(Runner) (any, error)) (any, error) {
	runner, err := vm.getRunner(service)
	if err != nil {
		return nil, err
	}
	defer runner.Return()
	return fn(runner)
}

func (vm *ottoVM) returnRunner(r *OttoRunner) {
	r.goContext = nil
	if vm.curRunner == r {
		vm.curRunner = nil
	} else if r.vm != vm {
		panic("OttoRunner returned to wrong v8VM!")
	}
	vm.runners[r.id] = r
	vm.release()
}
