package js

import (
	v8 "rogchap.com/v8go"
)

// The configuration of a Module, given when the VMPool is created.
type ModuleConfig struct {
	SourceCode string // JavaScript source code
	OnSetup    func(*Module) error
	// Callbacks  map[string]ModuleCallback // named callbacks
}

// For convenience, Module callbacks get passed the Runner instance,
// and return Go values -- allowed types are nil, numbers, bool, string.
type ModuleCallback = func(*Runner, *v8.FunctionCallbackInfo) (any, error)

// An instantiated Module associated with a specific `vm`.
// This is still not directly runnable; it needs to be associated with a Runner first.
type Module struct {
	name   string             // Name of the module
	vm     *VM                // Its vm
	global *v8.ObjectTemplate // Its global namespace (an object template)
	script *v8.UnboundScript  // Compiled JS code; template run in each new Runner
	// callbacks map[string]*v8.FunctionTemplate // JS->Go callback funcs
	registeredObjs map[string]*v8.ObjectTemplate
	registeredVals map[string]*v8.Value
}

// Instantiates a Module using a vm.
func newModule(config ModuleConfig, name string, vm *VM) (*Module, error) {
	module := &Module{
		name:   name,
		vm:     vm,
		global: v8.NewObjectTemplate(vm.iso),
		// callbacks: map[string]*v8.FunctionTemplate{},
		registeredObjs: map[string]*v8.ObjectTemplate{},
		registeredVals: map[string]*v8.Value{},
	}
	var err error
	module.script, err = vm.iso.CompileUnboundScript(config.SourceCode, name+".js", v8.CompileOptions{})
	if err != nil {
		return nil, err
	}
	if config.OnSetup != nil {
		if err = config.OnSetup(module); err != nil {
			return nil, err
		}
	}
	// for name, fn := range config.Callbacks {
	// 	module.callbacks[name] = vm.createCallback(fn)
	// }
	return module, nil
}

func (m *Module) Iso() *v8.Isolate                      { return m.vm.iso }
func (m *Module) NewObjectTemplate() *v8.ObjectTemplate { return v8.NewObjectTemplate(m.vm.iso) }
func (m *Module) NewString(str string) *v8.Value        { return mustSucceed(v8.NewValue(m.vm.iso, str)) }

func (m *Module) NewValue(val any) (*v8.Value, error) {
	if val != nil {
		return v8.NewValue(m.vm.iso, val)
	} else {
		return v8.Undefined(m.vm.iso), nil
	}
}

func (m *Module) SetGlobal(name string, value any) error { return m.global.Set(name, value) }

func (m *Module) NewCallback(cb ModuleCallback) *v8.FunctionTemplate {
	return m.vm.CreateCallback(cb)
}

func (m *Module) RegisterObject(name string, obj *v8.ObjectTemplate) {
	m.registeredObjs[name] = obj
}

func (m *Module) RegisterValue(name string, val any) error {
	if val == nil {
		panic("RegisterValue given nil")
	}
	v8Val, err := m.NewValue(val)
	if err == nil {
		m.registeredVals[name] = v8Val
	}
	return err
}
