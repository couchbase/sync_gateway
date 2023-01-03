package js

import (
	_ "embed"
	"fmt"

	"github.com/pkg/errors"
	v8 "rogchap.com/v8go"
)

// v8go docs:			https://pkg.go.dev/rogchap.com/v8go
// General V8 API docs: https://v8.dev/docs/embed

// Represents a JavaScript virtual machine, an "Isolate" in V8 terminology.
// This doesn't do much on its own; it acts as a ServiceHost for Service and Runner objects.
//
// **Not thread-safe!** A VM instance must be used only on one goroutine at a time.
// A Service whose ServiceHost is a VM can only be used on a single goroutine; any concurrent
// use will trigger a panic in VM.getRunner.
// The VMPool takes care of this, by vending VM instances that are known not to be in use.
type VM struct {
	iso          *v8.Isolate            // A V8 virtual machine. NOT THREAD SAFE.
	setupScript  *v8.UnboundScript      // JS code to set up a new v8.Context
	services     *servicesConfiguration // Factories for services
	templates    []Template             // Already-created Templates, indexed by serviceID
	runners      []*Runner              // Available Runners, indexed by serviceID
	curRunner    *Runner                // Currently active Runner, if any
	returnToPool *VMPool                // Pool to return me to, or nil
}

// Creates a JavaScript virtual machine that can run Services.
// This object should be used only on a single goroutine at a time.
func NewVM() *VM {
	return newVM(&servicesConfiguration{})
}

// Shuts down a VM. It's a good idea to call this explicitly when done with it,
// _unless_ it belongs to a VMPool.
func (vm *VM) Close() {
	if vm.returnToPool != nil {
		panic("Don't Close a VM that belongs to a VMPool")
	}
	for _, runner := range vm.runners {
		runner.close()
	}
	vm.templates = nil
	if vm.iso != nil {
		vm.iso.Dispose()
		vm.iso = nil
	}
}

//////// INTERNALS:

func newVM(services *servicesConfiguration) *VM {
	return &VM{
		iso:       v8.NewIsolate(), // The V8 VM
		services:  services,        // Factory fn for each service
		templates: []Template{},    // Instantiated Services
		runners:   []*Runner{},     // Cached reusable Runners
	}
}

// Must be called when finished using a VM belonging to a VMPool!
// (Harmless no-op when called on a standalone VM.)
func (vm *VM) release() {
	if vm.returnToPool != nil {
		vm.returnToPool.returnVM(vm)
	}
}

func (vm *VM) registerService(factory TemplateFactory) serviceID {
	if vm.iso == nil {
		panic("You forgot to initialize a js.VM") // Must call NewVM()
	}
	return vm.services.addService(factory)
}

// Returns a Template for the given Service.
func (vm *VM) getTemplate(service *Service) (Template, error) {
	var tmpl Template
	if int(service.id) < len(vm.templates) {
		tmpl = vm.templates[service.id]
	}
	if tmpl == nil {
		factory := vm.services.getService(service.id)
		if factory == nil {
			return nil, fmt.Errorf("js.VM has no service %q (%d)", service.name, int(service.id))
		}

		if vm.setupScript == nil {
			// The setup script points console logging to SG, and loads the Underscore.js library:
			var err error
			vm.setupScript, err = vm.iso.CompileUnboundScript(kSetupLoggingJS+kUnderscoreJS, "setupScript.js", v8.CompileOptions{})
			if err != nil {
				return nil, errors.Wrapf(err, "Couldn't compile setup script")
			}
		}

		var err error
		tmpl, err = newTemplate(vm, service.name, factory)
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

// Produces a Runner object that can run the given service.
// Be sure to call Runner.Return when done.
// Since VM is single-threaded, calling getRunner when a Runner already exists and hasn't been
// returned yet is assumed to be illegal concurrent access; it will trigger a panic.
func (vm *VM) getRunner(service *Service) (*Runner, error) {
	if vm.curRunner != nil {
		panic("VM already has a Runner")
	}
	var runner *Runner
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
		runner, err = newRunner(vm, tmpl, service.id)
		if err != nil {
			return nil, err
		}
	} else {
		runner.vm = vm
	}
	vm.curRunner = runner
	return runner, nil
}

// Called by Runner.Return; either closes its V8 resources or saves it for reuse.
// Also returns the VM to its Pool, if it came from one.
func (vm *VM) returnRunner(r *Runner) {
	r.goContext = nil
	if vm.curRunner == r {
		vm.curRunner = nil
	} else if r.vm != vm {
		panic("Runner returned to wrong VM!")
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

// The Underscore.js utility library, version 1.13.6, downloaded 2022-Nov-23 from
// <https://underscorejs.org/underscore-umd-min.js>
//
//go:embed underscore-umd-min.js
var kUnderscoreJS string
