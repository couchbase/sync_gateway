package js

import (
	"context"
	"time"
)

// A Runner represents a Service instantiated in a VM, in its own sandboxed V8 context,
// ready to run the Service's code.
// **NOT thread-safe!**
type Runner interface {
	VM() VM
	Return()
	SetContext(ctx context.Context)
	Context() context.Context
	ContextOrDefault() context.Context
	Timeout() *time.Duration
	Run(args ...any) (any, error)
}

type baseRunner struct {
	template  V8Template      // The Service template I'm created from
	id        serviceID       // The service ID in its VM
	vm        VM              // The owning VM object
	goContext context.Context // context.Context value for use by Go callbacks
}

func (r *baseRunner) Template() V8Template { return r.template }
func (r *baseRunner) VM() VM               { return r.vm }

// Associates a Go Context with this v8Runner.
// If this Context has a deadline, JS calls will abort if it expires.
func (r *baseRunner) SetContext(ctx context.Context) { r.goContext = ctx }

// Returns the Go context.Context associated with this v8Runner; nil by default.
func (r *baseRunner) Context() context.Context { return r.goContext }

// Returns the Go context.Context associated with this v8Runner, else `context.TODO()`.
func (r *baseRunner) ContextOrDefault() context.Context {
	if r.goContext != nil {
		return r.goContext
	} else {
		return context.TODO()
	}
}

// Returns the remaining duration until the v8Runner's Go Context's deadline, or nil if none.
func (r *baseRunner) Timeout() *time.Duration {
	if r.goContext != nil {
		if deadline, hasDeadline := r.goContext.Deadline(); hasDeadline {
			timeout := time.Until(deadline)
			return &timeout
		}
	}
	return nil
}

// JavaScript max integer value: any number larger than this will lose integer precision
// (https://www.ecma-international.org/ecma-262/5.1/#sec-8.5)
const JavascriptMaxSafeInt = int64(1<<53 - 1)

// JavaScript minimum integer value: any number below than this will lose integer precision
const JavascriptMinSafeInt = -JavascriptMaxSafeInt
