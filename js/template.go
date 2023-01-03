package js

import (
	"fmt"

	"github.com/couchbase/sync_gateway/base"
	"github.com/pkg/errors"
	v8 "rogchap.com/v8go"
)

// A Template manages a Service's initial JS runtime environment -- its script code, main function
// and global functions & variables -- in the form of V8 "template" objects.
//
// A Template belongs to both a Service and a VM. It's created the first time the Service needs to
// create a Runner in that VM.
//
// Once the Template is initialized, each Runner's V8 Context is very cheaply initialized by
// making references to these templates, without needing to compile or run anything.
//
// Many clients -- those that just need to run a single JS function with no callbacks -- can
// ignore this interface entirely.
//
// Very few clients will need to implement this interface; most can just use the BasicTemplate
// struct that implements it, which is passed to NewCustomService's callback.
type Template interface {
	// The JavaScript global namespace.
	// Any named property set on this object by the Set method is exposed as a global
	// variable/context/function.
	Global() *v8.ObjectTemplate

	// The compiled script. When the Service is instantiated as a Runner, _before_ the runner is
	// called, this script is run and its return value becomes the Runner's function entry point.
	Script() *v8.UnboundScript

	// The Service's name.
	Name() string

	// If this returns true, a Runner instance will be cached by a VM and reused for the next call.
	// This is faster, but it means that any changes to the Runner's global variables will persist.
	Reusable() bool
}

// Returns a TemplateFactory function for a BasicService with the given JavaScript.
func BasicTemplateFactory(fnSource string) TemplateFactory {
	fnSource = `(` + fnSource + `)` // Allows a JS `function` stmt to be evaluated as a value
	return func(service *BasicTemplate) (Template, error) {
		return service, service.SetScript(fnSource)
	}
}

// Base implementation of Template.
// Manages a Service's initial JS runtime environment -- its script code, main function
// and global functions & variables -- in the form of V8 "template" objects.
//
// "Subclasses" can be created by embedding this in another struct; this is useful if you create
// additional object or function templates and need to store references to them for use by the
// Runner.
type BasicTemplate struct {
	vm      *VM
	global  *v8.ObjectTemplate
	script  *v8.UnboundScript
	name    string
	oneShot bool
}

func (t *BasicTemplate) Global() *v8.ObjectTemplate { return t.global }
func (t *BasicTemplate) Script() *v8.UnboundScript  { return t.script }
func (t *BasicTemplate) Name() string               { return t.name }
func (t *BasicTemplate) Reusable() bool             { return !t.oneShot }

// Compiles JavaScript source code that becomes the service's script.
// A TemplateFactory function (the callback passed to NewCustomService) must call this.
// The source code can be any JavaScript, but running it must return a JS function object;
// that is, the last statement of the script must be or return a function expression.
// This statement cannot simply be a function; that's a syntax error. To return a function,
// wrap it in parentheses, e.g. `(function(arg1,arg2…) {…body…; return result;});`,
// or give the function a name and simply return it by name.
func (t *BasicTemplate) SetScript(jsSourceCode string) error {
	var err error
	t.script, err = t.vm.iso.CompileUnboundScript(jsSourceCode, t.name+".js", v8.CompileOptions{})
	return err
}

// Sets the Template's Reusable property, which defaults to true.
// If your Service's script is considered untrusted, or if it modifies global variables and needs
// them reset on each run, you can set this to false.
func (t *BasicTemplate) SetReusable(reuseable bool) { t.oneShot = !reuseable }

// Creates a JS template object, which will be instantiated as a real object in a Runner's context.
func (t *BasicTemplate) NewObjectTemplate() *v8.ObjectTemplate { return v8.NewObjectTemplate(t.vm.iso) }

// A Go function that can be registered as a callback from JavaScript.
// For convenience, Service callbacks get passed the Runner instance,
// and return Go values -- allowed types are nil, numbers, bool, string, v8.Value, v8.Object.
type TemplateCallback = func(r *Runner, this *v8.Object, args []*v8.Value) (result any, err error)

// Defines a JS global function that calls calls into the given Go function.
func (t *BasicTemplate) GlobalCallback(name string, cb TemplateCallback) {
	t.global.Set(name, t.NewCallback(cb), v8.ReadOnly)
}

// Creates a JS function-template object that calls into the given Go function.
func (t *BasicTemplate) NewCallback(callback TemplateCallback) *v8.FunctionTemplate {
	vm := t.vm
	return v8.NewFunctionTemplate(vm.iso, func(info *v8.FunctionCallbackInfo) *v8.Value {
		runner := vm.currentRunner(info.Context())
		result, err := callback(runner, info.This(), info.Args())
		if err == nil {
			if v8Result, newValErr := runner.NewValue(result); err == nil {
				return v8Result
			} else {
				err = errors.Wrap(newValErr, "Could not convert a callback's result to JavaScript")
			}
		}
		return v8Throw(vm.iso, err)
	})
}

// Converts a Go value to a JavaScript value (*v8.Value). Supported types are:
// - nil (converted to `null`)
// - boolean
// - int, int32, int64, uint32, uint64, float64
// - *big.Int
// - json.Number
// - string
func (t *BasicTemplate) NewValue(val any) (*v8.Value, error) { return newValue(t.vm.iso, val) }

// Creates a JS string value.
func (t *BasicTemplate) NewString(str string) *v8.Value { return newString(t.vm.iso, str) }

//////// INTERNALS:

func newTemplate(vm *VM, name string, factory TemplateFactory) (Template, error) {
	basicTmpl := &BasicTemplate{
		name:   name,
		vm:     vm,
		global: v8.NewObjectTemplate(vm.iso),
	}
	basicTmpl.defineSgLog() // Maybe make this optional?
	tmpl, err := factory(basicTmpl)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to initialize JS service %q", name)
	} else if tmpl == nil {
		return nil, fmt.Errorf("js.TemplateFactory %q returned nil", name)
	} else if tmpl.Script() == nil {
		return nil, fmt.Errorf("js.TemplateFactory %q failed to initialize Service's script", name)
	} else {
		return tmpl, nil
	}
}

// Defines a global `sg_log` function that writes to SG's log.
func (service *BasicTemplate) defineSgLog() {
	service.global.Set("sg_log", service.NewCallback(func(r *Runner, this *v8.Object, args []*v8.Value) (any, error) {
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

// Sets up the standard console logging functions, delegating to `sg_log`.
const kSetupLoggingJS = `
	console.trace = function(...args) {sg_log(5, ...args);};
	console.debug = function(...args) {sg_log(4, ...args);};
	console.log   = function(...args) {sg_log(3, ...args);};
	console.warn  = function(...args) {sg_log(2, ...args);};
	console.error = function(...args) {sg_log(1, ...args);};
`
