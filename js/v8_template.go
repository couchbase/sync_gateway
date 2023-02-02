package js

import (
	"fmt"

	"github.com/couchbase/sync_gateway/base"
	"github.com/pkg/errors"
	v8 "rogchap.com/v8go"
)

// A V8Template manages a Service's initial JS runtime environment -- its script code, main function
// and global functions & variables -- in the form of V8 "template" objects.
//
// A V8Template belongs to both a Service and a VM. It's created the first time the Service needs to
// create a v8Runner in that VM.
//
// Once the V8Template is initialized, each v8Runner's V8 Context is very cheaply initialized by
// making references to these templates, without needing to compile or run anything.
//
// Many clients -- those that just need to run a single JS function with no callbacks -- can
// ignore this interface entirely.
//
// Very few clients will need to implement this interface; most can just use the BasicTemplate
// struct that implements it, which is passed to NewCustomService's callback.
type V8Template interface {
	// The JavaScript global namespace.
	// Any named property set on this object by the Set method is exposed as a global
	// variable/context/function.
	Global() *v8.ObjectTemplate

	// The compiled script. When the Service is instantiated as a v8Runner, _before_ the runner is
	// called, this script is run and its return value becomes the v8Runner's function entry point.
	Script() *v8.UnboundScript

	// The Service's name.
	Name() string

	// If this returns true, a v8Runner instance will be cached by a VM and reused for the next call.
	// This is faster, but it means that any changes to the v8Runner's global variables will persist.
	Reusable() bool
}

// Base implementation of Template.
// Manages a Service's initial JS runtime environment -- its script code, main function
// and global functions & variables -- in the form of V8 "template" objects.
//
// "Subclasses" can be created by embedding this in another struct; this is useful if you create
// additional object or function templates and need to store references to them for use by the
// Runner.
type V8BasicTemplate struct {
	vm      *v8VM
	global  *v8.ObjectTemplate
	script  *v8.UnboundScript
	name    string
	oneShot bool
}

func (t *V8BasicTemplate) Global() *v8.ObjectTemplate { return t.global }
func (t *V8BasicTemplate) Script() *v8.UnboundScript  { return t.script }
func (t *V8BasicTemplate) Name() string               { return t.name }
func (t *V8BasicTemplate) Reusable() bool             { return !t.oneShot }

// Compiles JavaScript source code that becomes the service's script.
// A TemplateFactory function (the callback passed to NewCustomService) must call this.
// The source code can be any JavaScript, but running it must return a JS function object;
// that is, the last statement of the script must be or return a function expression.
// This statement cannot simply be a function; that's a syntax error. To return a function,
// wrap it in parentheses, e.g. `(function(arg1,arg2…) {…body…; return result;});`,
// or give the function a name and simply return it by name.
func (t *V8BasicTemplate) SetScript(jsSourceCode string) error {
	var err error
	t.script, err = t.vm.iso.CompileUnboundScript(jsSourceCode, t.name+".js", v8.CompileOptions{})
	return err
}

// Sets the Template's Reusable property, which defaults to true.
// If your Service's script is considered untrusted, or if it modifies global variables and needs
// them reset on each run, you can set this to false.
func (t *V8BasicTemplate) SetReusable(reuseable bool) { t.oneShot = !reuseable }

// Creates a JS template object, which will be instantiated as a real object in a v8Runner's context.
func (t *V8BasicTemplate) NewObjectTemplate() *v8.ObjectTemplate {
	return v8.NewObjectTemplate(t.vm.iso)
}

// A Go function that can be registered as a callback from JavaScript.
// For convenience, Service callbacks get passed the v8Runner instance,
// and return Go values -- allowed types are nil, numbers, bool, string, v8.Value, v8.Object.
type TemplateCallback = func(r *V8Runner, this *v8.Object, args []*v8.Value) (result any, err error)

// Defines a JS global function that calls calls into the given Go function.
func (t *V8BasicTemplate) GlobalCallback(name string, cb TemplateCallback) {
	t.global.Set(name, t.NewCallback(cb), v8.ReadOnly)
}

// Creates a JS function-template object that calls into the given Go function.
func (t *V8BasicTemplate) NewCallback(callback TemplateCallback) *v8.FunctionTemplate {
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
// - integer and float types (32-bit and larger)
// - *big.Int
// - json.Number
// - string
func (t *V8BasicTemplate) NewValue(val any) (*v8.Value, error) {
	if val == nil {
		return v8.Null(t.vm.iso), nil // v8.NewValue panics if given nil :-p
	}
	return v8.NewValue(t.vm.iso, val)
}

// Creates a JS string value.
func (t *V8BasicTemplate) NewString(str string) *v8.Value {
	return newString(t.vm.iso, str)
}

//////// INTERNALS:

func newV8Template(vm *v8VM, service *Service) (V8Template, error) {
	basicTmpl := &V8BasicTemplate{
		name:   service.name,
		vm:     vm,
		global: v8.NewObjectTemplate(vm.iso),
	}
	basicTmpl.defineSgLog() // Maybe make this optional?

	var tmpl V8Template
	var err error
	if service.v8Init != nil {
		tmpl, err = service.v8Init(basicTmpl)
	} else {
		err = basicTmpl.SetScript(`(` + service.jsFunctionSource + `)`)
		tmpl = basicTmpl
	}

	if err != nil {
		return nil, fmt.Errorf("failed to initialize JS service %q: %+w", service.name, err)
	} else if tmpl == nil {
		return nil, fmt.Errorf("js.TemplateFactory %q returned nil", service.name)
	} else if tmpl.Script() == nil {
		return nil, fmt.Errorf("js.TemplateFactory %q failed to initialize Service's script", service.name)
	} else {
		return tmpl, nil
	}
}

// Defines a global `sg_log` function that writes to SG's log.
func (service *V8BasicTemplate) defineSgLog() {
	service.global.Set("sg_log", service.NewCallback(func(r *V8Runner, this *v8.Object, args []*v8.Value) (any, error) {
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
	console.info  = console.log;
	console.warn  = function(...args) {sg_log(2, ...args);};
	console.error = function(...args) {sg_log(1, ...args);};
`
