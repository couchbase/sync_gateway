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
