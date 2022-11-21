package js

import (
	v8 "rogchap.com/v8go"
)

// Interface defining a JavaScript service belonging to a VM that can be instantiated by a Runner.
type Service interface {
	Name() string
	Reusable() bool
	Global() *v8.ObjectTemplate
	Script() *v8.UnboundScript
}

type ServiceFactory func(vm *VM) (Service, error)

// Base implementation of Service.
// "Subclasses" can be created by embedding this in anothe struct.
type BaseService struct {
	name   string             // Name of the service
	vm     *VM                // Its vm
	global *v8.ObjectTemplate // Its global namespace (an object template)
	script *v8.UnboundScript  // Compiled JS code; evaluated in each new Runner
}

// Initializes a BaseService. ServiceFactory functions will call this.
// - `vm` is the VM instance passed to the ServiceFactory.
// - `name` is the name of the service.
// - `sourceCode` is the JavaScript source code implementing the service.
//    When run, this is expected to return a function, which will be cached &  called by the Runner.
func (service *BaseService) Init(vm *VM, name string, sourceCode string) error {
	var err error
	service.name = name
	service.vm = vm
	service.global = v8.NewObjectTemplate(vm.iso)
	service.script, err = vm.iso.CompileUnboundScript(sourceCode, name+".js", v8.CompileOptions{})
	return err
}

// Service interface implementation:
func (s *BaseService) Name() string               { return s.name }
func (s *BaseService) Reusable() bool             { return true }
func (s *BaseService) Global() *v8.ObjectTemplate { return s.global }
func (s *BaseService) Script() *v8.UnboundScript  { return s.script }

func (s *BaseService) VM() *VM                                { return s.vm }
func (s *BaseService) NewObjectTemplate() *v8.ObjectTemplate  { return s.vm.NewObjectTemplate() }
func (s *BaseService) NewString(str string) *v8.Value         { return s.vm.NewString(str) }
func (s *BaseService) NewValue(val any) (*v8.Value, error)    { return s.vm.NewValue(val) }
func (s *BaseService) SetGlobal(name string, value any) error { return s.global.Set(name, value) }

// For convenience, Service callbacks get passed the Runner instance,
// and return Go values -- allowed types are nil, numbers, bool, string.
type ServiceCallback = func(*Runner, *v8.FunctionCallbackInfo) (any, error)

func (s *BaseService) NewCallback(cb ServiceCallback) *v8.FunctionTemplate {
	return s.vm.NewCallback(cb)
}
