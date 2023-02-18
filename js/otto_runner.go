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
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/couchbase/sync_gateway/base"
	"github.com/pkg/errors"
	"github.com/robertkrimen/otto"

	_ "github.com/robertkrimen/otto/underscore"
)

type OttoRunner struct {
	baseRunner            // "superclass"
	otto       *otto.Otto // An Otto virtual machine. NOT THREAD SAFE.
	fn         otto.Value // The compiled function to run
}

func newOttoRunner(vm *ottoVM, service *Service) (*OttoRunner, error) {
	ottoVM := otto.New()
	fnobj, err := ottoVM.Object("(" + service.jsFunctionSource + ")")
	if err != nil {
		return nil, err
	}
	if fnobj.Class() != "Function" {
		return nil, errors.New("JavaScript source does not evaluate to a function")
	}

	r := &OttoRunner{
		baseRunner: baseRunner{
			id: service.id,
			vm: vm,
		},
		otto: ottoVM,
		fn:   fnobj.Value(),
	}

	// Redirect JS logging to the SG console:
	err = ottoVM.Set("sg_log", func(call otto.FunctionCall) otto.Value {
		ilevel, _ := call.ArgumentList[0].ToInteger()
		message, _ := call.ArgumentList[1].ToString()
		extra := ""
		for _, arg := range call.ArgumentList[2:] {
			if arg.IsUndefined() {
				break
			}
			str, _ := arg.ToString()
			extra += str + " "
		}

		key := base.KeyJavascript
		level := base.LogLevel(ilevel)
		if level <= base.LevelWarn {
			key = base.KeyAll // replicates behavior of base.WarnFctx, ErrorFctx
		}
		base.LogfTo(r.ContextOrDefault(), level, key, "%s %s", message, base.UD(extra))
		return otto.UndefinedValue()
	})
	if err != nil {
		return nil, err
	}
	_, err = ottoVM.Run(`
		console.trace = function(msg,a,b,c,d) {sg_log(5, msg,a,b,c,d);};
		console.debug = function(msg,a,b,c,d) {sg_log(4, msg,a,b,c,d);};
		console.log   = function(msg,a,b,c,d) {sg_log(3, msg,a,b,c,d);};
		console.warn  = function(msg,a,b,c,d) {sg_log(2, msg,a,b,c,d);};
		console.error = function(msg,a,b,c,d) {sg_log(1, msg,a,b,c,d);};`)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *OttoRunner) Return() { r.vm.(*ottoVM).returnRunner(r) }

func (r *OttoRunner) Run(args ...any) (result any, err error) {
	// Translate args to Otto values:
	jsArgs := make([]any, len(args))
	for i, input := range args {
		if jsonStr, ok := input.(JSONString); ok {
			if input, err = r.jsonToValue(string(jsonStr)); err != nil {
				return nil, err
			}
		} else {
			input, _ = convertJSONNumbers(input)
		}
		jsArgs[i], err = r.otto.ToValue(input)
		if err != nil {
			return nil, fmt.Errorf("couldn't convert arg %d, %#v, to JS: %w", i, args[i], err)
		}
	}

	// If the Context has a timeout, set up a goroutine that will interrupt Otto:
	if timeoutChan := r.Context().Done(); timeoutChan != nil {
		runnerDoneChan := make(chan bool, 1)
		defer func() {
			close(runnerDoneChan)
			// Catch the panic thrown by the Interrupt fn and make it return an error:
			if caught := recover(); caught != nil {
				if caught == context.DeadlineExceeded {
					err = context.DeadlineExceeded
					return
				}
				panic(caught)
			}
		}()

		r.otto.Interrupt = make(chan func(), 1)
		go func() {
			select {
			case <-timeoutChan:
				r.otto.Interrupt <- func() {
					panic(context.DeadlineExceeded)
				}
			case <-runnerDoneChan:
				return
			}
		}()
	}

	// Finally run the function:
	resultVal, err := r.fn.Call(r.fn, jsArgs...)
	if err != nil {
		return nil, err
	}
	result, _ = resultVal.Export()
	return result, nil
}

func (runner *OttoRunner) jsonToValue(jsonStr string) (any, error) {
	if jsonStr == "" {
		return otto.NullValue(), nil
	}
	var parsed any
	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		return nil, fmt.Errorf("unparseable Runner input: %s", jsonStr)
	}
	return parsed, nil
}

// Converts json.Number objects to regular Go numeric values.
// Integers that would lose precision are left as json.Number, as are floats that can't be
// converted to float64. (These will appear to be strings in JavaScript.)
//
// The function recurses into JSON arrays and maps; if changes are made, they are copied,
// not modified in place.
//
// Note: This function is not necessary with V8, because v8go already translates json.Numbers
// into JS BigInts.
func convertJSONNumbers(value any) (result any, changed bool) {
	switch value := value.(type) {
	case json.Number:
		if asInt, err := value.Int64(); err == nil {
			if asInt <= JavascriptMaxSafeInt && asInt >= JavascriptMinSafeInt {
				return asInt, true
			} else {
				// Integer would lose precision when used in javascript - leave as json.Number
				break
			}
		} else if numErr, _ := err.(*strconv.NumError); numErr.Err == strconv.ErrRange {
			// out of range of int64
			break
		} else if asFloat, err := value.Float64(); err == nil {
			// Can't reliably detect loss of precision in float, due to number of variations in input float format
			return asFloat, true
		}
	case map[string]any:
		var copied map[string]any
		for k, v := range value {
			if newVal, changed := convertJSONNumbers(v); changed {
				if copied == nil {
					copied = make(map[string]any, len(value))
					for kk, vv := range value {
						copied[kk] = vv
					}
				}
				copied[k] = newVal
			}
		}
		if copied != nil {
			return copied, true
		}
	case []any:
		var copied []any
		for i, v := range value {
			if newVal, changed := convertJSONNumbers(v); changed {
				if copied == nil {
					copied = append(copied, value...)
				}
				copied[i] = newVal
			}
		}
		if copied != nil {
			return copied, true
		}
	default:
	}
	return value, false
}
