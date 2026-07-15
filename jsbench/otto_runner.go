// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package jsbench

import (
	"github.com/robertkrimen/otto"
)

type OttoRunner struct {
	vm       *otto.Otto
	fn       otto.Value
	channels []string
}

func collectChannelsOtto(r *OttoRunner, call otto.FunctionCall) {
	for _, arg := range call.ArgumentList {
		v, _ := arg.Export()
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

func newOttoRunner(source string) *OttoRunner {
	vm := otto.New()
	r := &OttoRunner{vm: vm}

	_ = vm.Set("channel", func(call otto.FunctionCall) otto.Value {
		collectChannelsOtto(r, call)
		return otto.UndefinedValue()
	})
	_ = vm.Set("access", func(call otto.FunctionCall) otto.Value {
		return otto.UndefinedValue()
	})
	_ = vm.Set("role", func(call otto.FunctionCall) otto.Value {
		return otto.UndefinedValue()
	})
	noop := func(call otto.FunctionCall) otto.Value { return otto.UndefinedValue() }
	_ = vm.Set("console", map[string]interface{}{"log": noop, "error": noop})

	fnObj, err := vm.Object("(" + source + ")")
	if err != nil {
		panic(err)
	}
	r.fn = fnObj.Value()
	return r
}

func (r *OttoRunner) Call(doc, oldDoc, meta interface{}) {
	r.channels = r.channels[:0]
	if _, err := r.fn.Call(r.fn, doc, oldDoc, meta); err != nil {
		panic(err)
	}
}
