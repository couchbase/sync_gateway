package js

import (
	"context"

	"github.com/couchbase/sync_gateway/base"
	v8 "rogchap.com/v8go"
)

// A Service represents a JavaScript-based API that runs in a VM or VMPool.
type Service struct {
	host ServiceHost
	id   serviceID
	name string
}

type serviceID uint32 // internal ID, used as an array index in VM and VMPool.

// A provider of a JavaScript runtime for Services. VM and VMPool implement this.
type ServiceHost interface {
	Close()
	registerService(factory TemplateFactory, name string) serviceID
	getRunner(*Service) (*Runner, error)
}

// Creates a new Service in a ServiceHost (a VM or VMPool.)
// The name is primarily for logging; it does not need to be unique.
// The source code should be of the form `function(arg1,arg2…) {…body…; return result;}`.
// If you have a more complex script, like one that defines several functions, use NewCustomService.
func NewService(host ServiceHost, name string, jsFunctionSource string) *Service {
	return NewCustomService(host, name, BasicTemplateFactory(jsFunctionSource))
}

// Creates a new Service in a ServiceHost (a VM or VMPool.)
// The implementation can extend the Service's JavaScript template environment by defining globals
// and/or callback functions.
func NewCustomService(host ServiceHost, name string, factory TemplateFactory) *Service {
	base.DebugfCtx(context.Background(), base.KeyJavascript, "Creating JavaScript service %q", name)
	return &Service{
		host: host,
		id:   host.registerService(factory, name),
		name: name,
	}
}

// A factory/initialization function for Services that need to add JS globals or callbacks or
// otherwise extend their runtime environment. They do this by operating on its Template.
//
// The function's parameter is a BasicTemplate that doesn't have a script yet.
// The function MUST call its SetScript method.
// The function may return the Template it was given, or it may instantiate its own struct that
// implements Template (which presumably includes a pointer to the BasicTemplate) and return that.
type TemplateFactory func(base *BasicTemplate) (Template, error)

// The Service's name, given when it was created.
func (service *Service) Name() string { return service.name }

// The VM or VMPool that provides the Service's runtime environment.
func (service *Service) Host() ServiceHost { return service.host }

// Returns a Runner instance that can be used to call the Service's code.
// This may be a new instance, or (if the Service's Template is reuseable) a recycled one.
// You **MUST** call its Return method when you're through with it.
//
// - If the Service's host is a VMPool, this call will block while all the pool's VMs are in use.
// - If the host is a VM, this call will fail if there is another Runner in use belonging to any
//   Service hosted by that VM.
func (service *Service) GetRunner() (*Runner, error) {
	base.DebugfCtx(context.Background(), base.KeyJavascript, "Running JavaScript service %q", service.name)
	return service.host.getRunner(service)
}

// A convenience wrapper around GetRunner that takes care of returning the Runner.
// It simply returns whatever the callback returns.
func (service *Service) WithRunner(fn func(*Runner) (any, error)) (any, error) {
	runner, err := service.GetRunner()
	if err != nil {
		return nil, err
	}
	defer runner.Return()
	var result any
	runner.WithTemporaryValues(func() {
		result, err = fn(runner)
	})
	return result, err
}

// A high-level method that runs a service in a VM without your needing to interact with a Runner.
// The arguments can be Go types or V8 Values; any types supported by VM.NewValue.
// The result is converted back to a Go type.
// If the function throws a JavaScript exception it's converted to a Go `error`.
func (service *Service) Run(ctx context.Context, args ...any) (any, error) {
	return service.WithRunner(func(r *Runner) (any, error) {
		r.SetContext(ctx)
		v8args, err := r.ConvertArgs(args...)
		if err == nil {
			var val *v8.Value
			if val, err = r.Run(v8args...); err == nil {
				return ValueToGo(val)
			}
		}
		return nil, err
	})
}
