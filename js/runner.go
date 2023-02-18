/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package js

import (
	"context"
	"time"
)

// A Runner represents a Service instantiated in a VM, in its own sandboxed V8 context,
// ready to run the Service's code.
// **NOT thread-safe!**
type Runner interface {
	// The JavaScript VM this Runner runs in.
	VM() VM
	// Returns the Runner back to its VM when you're done with it.
	Return()
	// Associates a Go `Context` with this Runner.
	// If this Context has a deadline, JS calls will abort if it expires.
	SetContext(ctx context.Context)
	// The associated `Context`, if you've set one; else nil.
	Context() context.Context
	// The associated `Context`, else the default `context.Background()` instance.
	ContextOrDefault() context.Context
	// Returns the remaining duration until the Context's deadline, or nil if none.
	Timeout() *time.Duration
	// Runs the Service's JavaScript function.
	// Arguments and the return value are translated from/to Go types for you.
	Run(args ...any) (any, error)
	// Associates a value with this Runner that you can retrieve later for your own purposes.
	SetAssociatedValue(any)
	// An optional value you've associated with this Runner; else nil.
	AssociatedValue() any
}

type baseRunner struct {
	id         serviceID       // The service ID in its VM
	vm         VM              // The owning VM object
	goContext  context.Context // context.Context value for use by Go callbacks
	associated any
}

func (r *baseRunner) VM() VM                         { return r.vm }
func (r *baseRunner) AssociatedValue() any           { return r.associated }
func (r *baseRunner) SetAssociatedValue(obj any)     { r.associated = obj }
func (r *baseRunner) SetContext(ctx context.Context) { r.goContext = ctx }
func (r *baseRunner) Context() context.Context       { return r.goContext }

func (r *baseRunner) ContextOrDefault() context.Context {
	if r.goContext != nil {
		return r.goContext
	} else {
		return context.TODO()
	}
}

func (r *baseRunner) Timeout() *time.Duration {
	if r.goContext != nil {
		if deadline, hasDeadline := r.goContext.Deadline(); hasDeadline {
			timeout := time.Until(deadline)
			return &timeout
		}
	}
	return nil
}

// The maximum integer that can be represented accurately by a JavaScript number.
// (JavaScript numbers are 64-bit IEEE floats with 53 bits of precision.)
// (https://www.ecma-international.org/ecma-262/5.1/#sec-8.5)
const JavascriptMaxSafeInt = int64(1<<53 - 1)

// The minimum integer that can be represented accurately by a JavaScript number.
// (JavaScript numbers are 64-bit IEEE floats with 53 bits of precision.)
const JavascriptMinSafeInt = -JavascriptMaxSafeInt
