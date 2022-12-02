package js

import (
	_ "embed"
	"fmt"

	"github.com/pkg/errors"
	v8 "rogchap.com/v8go"
)

// v8go docs:			https://pkg.go.dev/rogchap.com/v8go
// General V8 API docs: https://v8.dev/docs/embed

// Top-level object that represents a JavaScript VM, an "Isolate" in V8 terminology.
// This doesn't do much on its own; it acts as a container for Service and Runner objects.
//
// **Not thread-safe!** A VM instance must be used only on one goroutine at a time.
// The VMPool takes care of this, by vending VM instances that are known not to be in use.
type VM struct {
	iso          *v8.Isolate            // A V8 virtual machine. NOT THREAD SAFE.
	setupScript  *v8.UnboundScript      // JS code to set up a new v8.Context
	config       *servicesConfiguration // Factory for services
	services     []Template             // Already-created Templates
	runners      []*Runner              // Available Runners
	curRunner    *Runner                // Runner that's currently running in the iso
	returnToPool *VMPool                // Pool to return this to, or nil
}

// Creates a JavaScript virtual machine that can run Services.
func NewVM() *VM {
	return newVM(&servicesConfiguration{})
}

// Creates a VM owned by a VMPool.
func newVM(services *servicesConfiguration) *VM {
	return &VM{
		iso:      v8.NewIsolate(), // The V8 VM
		config:   services,        // Factory fn for each service
		services: []Template{},    // Instantiated Services
		runners:  []*Runner{},     // Cached reusable Runners
	}
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
	vm.iso.Dispose()
	vm.iso = nil
}

// Must be called when finished using a VM!
// Returns it to the VMPool it came from, or else closes it.
func (vm *VM) Release() {
	if vm.returnToPool != nil {
		vm.returnToPool.returnVM(vm)
	}
}

func (vm *VM) registerService(factory TemplateFactory) serviceID {
	return vm.config.addService(factory)
}

// Returns a Template for the given Service.
func (vm *VM) getTemplate(service *Service) (Template, error) {
	if int(service.id) < len(vm.services) {
		return vm.services[service.id], nil
	} else {
		factory := vm.config.getService(service.id)
		if factory == nil {
			return nil, fmt.Errorf("js.VM has no service %q (%d)", service.name, int(service.id))
		}

		// Compile Underscore.js if it's not already done:
		if vm.setupScript == nil {
			// The setup script points console logging to SG, and loads the Underscore.js library:
			var err error
			vm.setupScript, err = vm.iso.CompileUnboundScript(kSetupLoggingJS+kUnderscoreJS, "setupScript.js", v8.CompileOptions{})
			if err != nil {
				return nil, errors.Wrapf(err, "Couldn't compile setup script")
			}
		}

		basic := &BasicTemplate{
			name:   service.name,
			vm:     vm,
			global: v8.NewObjectTemplate(vm.iso),
		}
		basic.defineSgLog() // Maybe make this optional?
		tmpl, err := factory(basic)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to initialize JS service %q", service.name)
		} else if tmpl == nil {
			return nil, fmt.Errorf("js.TemplateFactory %q returned nil", service.name)
		} else if tmpl.Script() == nil {
			return nil, fmt.Errorf("js.TemplateFactory %q failed to initialize Service's script", service.name)
		}

		for int(service.id) >= len(vm.services) {
			vm.services = append(vm.services, nil)
		}
		vm.services[service.id] = tmpl
		return tmpl, nil
	}
}

// Produces a Runner object that can run the given service.
// Be sure to call Runner.Return when done.
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
// Also returns the VM to its Pool (or closes it.)
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
	vm.Release()
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

//////// V8 UTILITIES:

// The Underscore.js utility library, version 1.13.6, downloaded 2022-Nov-23 from
// <https://underscorejs.org/underscore-umd-min.js>
//
//go:embed underscore-umd-min.js
var kUnderscoreJS string

const kSetupLoggingJS = `
	console.trace = function(...args) {sg_log(5, ...args);};
	console.debug = function(...args) {sg_log(4, ...args);};
	console.log   = function(...args) {sg_log(3, ...args);};
	console.warn  = function(...args) {sg_log(2, ...args);};
	console.error = function(...args) {sg_log(1, ...args);};
`
