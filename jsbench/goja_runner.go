// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package jsbench

import (
	"github.com/dop251/goja"
)

type GojaRunner struct {
	vm       *goja.Runtime
	fn       goja.Callable
	channels []string
}

func collectChannelsGoja(r *GojaRunner, call goja.FunctionCall) {
	for _, arg := range call.Arguments {
		v := arg.Export()
		switch vv := v.(type) {
		case string:
			r.channels = append(r.channels, vv)
		case []interface{}:
			for _, s := range vv {
				if str, ok := s.(string); ok {
					r.channels = append(r.channels, str)
				}
			}
		}
	}
}

func newGojaRunner(source string) *GojaRunner {
	vm := goja.New()
	r := &GojaRunner{vm: vm}

	_ = vm.Set("channel", func(call goja.FunctionCall) goja.Value {
		collectChannelsGoja(r, call)
		return goja.Undefined()
	})
	_ = vm.Set("access", func(call goja.FunctionCall) goja.Value {
		return goja.Undefined()
	})
	_ = vm.Set("role", func(call goja.FunctionCall) goja.Value {
		return goja.Undefined()
	})
	noop := func(call goja.FunctionCall) goja.Value { return goja.Undefined() }
	_ = vm.Set("console", map[string]interface{}{"log": noop, "error": noop})

	val, err := vm.RunString("(" + source + ")")
	if err != nil {
		panic(err)
	}
	fn, ok := goja.AssertFunction(val)
	if !ok {
		panic("compiled value is not a function")
	}
	r.fn = fn
	return r
}

func (r *GojaRunner) Call(doc, oldDoc, meta interface{}) {
	r.channels = r.channels[:0]
	if _, err := r.fn(goja.Undefined(), r.vm.ToValue(doc), r.vm.ToValue(oldDoc), r.vm.ToValue(meta)); err != nil {
		panic(err)
	}
}
