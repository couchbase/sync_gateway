package js

import (
	"github.com/couchbase/sync_gateway/base"
	v8 "rogchap.com/v8go"
)

// Interface defining a JavaScript service belonging to a VM that can be instantiated by a Runner.
type Service interface {
	// The Service's registered name.
	Name() string
	// The global namespace
	Global() *v8.ObjectTemplate
	// The compiled script the Runner will run.
	Script() *v8.UnboundScript
	// If true, Runners from this Service can be reused for multiple calls.
	Reusable() bool
}

// Factory/initialization function for services.
// `base` is a BasicService that doesn't have a script yet. The function must call SetScript on it.
// The function may either return `base` itself, or its own Service implementation that wraps it.
type ServiceFactory func(base *BasicService) (Service, error)

// Base implementation of Service.
// "Subclasses" can be created by embedding this in another struct.
type BasicService struct {
	name   string
	vm     *VM
	global *v8.ObjectTemplate
	script *v8.UnboundScript
}

// Returns a ServiceFactory function for a BasicService with the given JavaScript.
func BasicServiceFactory(fnSource string) ServiceFactory {
	fnSource = `(` + fnSource + `)` // Allows a JS `function` stmt to be evaluated as a value
	return func(service *BasicService) (Service, error) {
		return service, service.SetScript(fnSource)
	}
}

func (s *BasicService) Name() string               { return s.name }
func (s *BasicService) Global() *v8.ObjectTemplate { return s.global }
func (s *BasicService) Script() *v8.UnboundScript  { return s.script }
func (s *BasicService) Reusable() bool             { return true }

// Compiles JavaScript source code that becomes the service's script.
// A ServiceFactory is responsible for calling this.
func (service *BasicService) SetScript(sourceCode string) error {
	var err error
	service.script, err = service.vm.iso.CompileUnboundScript(sourceCode, service.name+".js", v8.CompileOptions{})
	return err
}

// Defines a global `sg_log` function that writes to SG's log.
func (service *BasicService) defineConsole() {
	service.global.Set("sg_log", service.NewCallback(func(r *Runner, info *v8.FunctionCallbackInfo) (any, error) {
		args := info.Args()
		if len(args) >= 2 {
			level := base.LogLevel(args[0].Integer())
			msg := args[1].String()
			extra := ""
			for i := 2; i < len(args); i++ {
				extra += " "
				extra += args[i].DetailString()
			}
			key := base.KeyJavascript
			if level <= base.LevelWarn {
				key = base.KeyAll // replicates behavior of base.WarnFctx, ErrorFctx
			}
			base.LogfTo(r.ContextOrDefault(), level, key, "%s%s", msg, base.UD(extra))
		}
		return nil, nil
	}))
}

func (s *BasicService) NewObjectTemplate() *v8.ObjectTemplate { return s.vm.NewObjectTemplate() }
func (s *BasicService) NewString(str string) *v8.Value        { return s.vm.NewString(str) }
func (s *BasicService) NewValue(val any) (*v8.Value, error)   { return s.vm.NewValue(val) }

// For convenience, Service callbacks get passed the Runner instance,
// and return Go values -- allowed types are nil, numbers, bool, string, v8.Value, v8.Object.
type ServiceCallback = func(*Runner, *v8.FunctionCallbackInfo) (any, error)

// Creates a JS function-template object that calls back to Go code.
func (s *BasicService) NewCallback(cb ServiceCallback) *v8.FunctionTemplate {
	return s.vm.NewCallback(cb)
}

// Defines a JS global function that calls back to Go code.
func (s *BasicService) GlobalCallback(name string, cb ServiceCallback) {
	s.global.Set(name, s.NewCallback(cb), v8.ReadOnly)
}
