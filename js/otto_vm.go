package js

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/robertkrimen/otto"
)

type ottoVM struct {
	*baseVM
	runners   []*OttoRunner // Available Runners, indexed by serviceID. nil if in-use
	curRunner *OttoRunner   // Currently active Runner, if any
}

// A VMType for instantiating Otto-based VMs and VMPools.
var Otto = &VMType{
	name: "Otto",
	factory: func(services *servicesConfiguration) VM {
		return &ottoVM{
			baseVM:  &baseVM{services: services}, // "superclass"
			runners: []*OttoRunner{},             // Cached reusable Runners
		}
	},
}

func (vm *ottoVM) Type() *VMType { return Otto }

func (vm *ottoVM) Close() {
	vm.baseVM.close()
	if cur := vm.curRunner; cur != nil {
		cur.Return()
	}
	vm.curRunner = nil
	vm.runners = nil
}

// Looks up an already-registered service by name. Returns nil if not found.
func (vm *ottoVM) FindService(name string) *Service {
	return vm.services.findServiceNamed(name)
}

// Must be called when finished using a VM belonging to a VMPool!
// (Harmless no-op when called on a standalone VM.)
func (vm *ottoVM) release() {
	if vm.returnToPool != nil {
		vm.lastReturned = time.Now()
		vm.returnToPool.returnVM(vm)
	}
}

func (vm *ottoVM) hasInitializedService(service *Service) bool {
	id := int(service.id)
	return id < len(vm.runners) && vm.runners[id] != nil
}

func (vm *ottoVM) getRunner(service *Service) (Runner, error) {
	if vm.curRunner != nil {
		panic("illegal access to v8VM: already has a v8Runner")
	}
	if !vm.services.hasService(service) {
		return nil, fmt.Errorf("unknown js.Service instance passed to VM")
	}
	runner, err := newOttoRunner(vm, service)
	if err != nil {
		return nil, err
	}
	vm.curRunner = runner
	for int(service.id) >= len(vm.runners) {
		vm.runners = append(vm.runners, nil)
	}
	return runner, nil
}

func (vm *ottoVM) withRunner(service *Service, fn func(Runner) (any, error)) (any, error) {
	runner, err := vm.getRunner(service)
	if err != nil {
		return nil, err
	}
	defer runner.Return()
	return fn(runner)
}

func (vm *ottoVM) returnRunner(r *OttoRunner) {
	r.goContext = nil
	if vm.curRunner == r {
		vm.curRunner = nil
	} else if r.vm != vm {
		panic("OttoRunner returned to wrong v8VM!")
	}
	vm.runners[r.id] = r
	vm.release()
}

//////// RUNNER

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
	return &OttoRunner{
		baseRunner: baseRunner{
			id: service.id,
			vm: vm,
		},
		otto: ottoVM,
		fn:   fnobj.Value(),
	}, nil
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

// Converts json.Number values to Otto-compatible number objects.
// Integers that would lose precision are left as json.Number, as are floats that can't be
// converted to float64. These will appear to be strings in JavaScript.
//
// The function recurses into JSON arrays and maps; if changes are made, they are copied,
// not modified in place.
//
// Note: This function is not necessary with V8, because the V8 Runner converts json.Numbers
// into JS BigInt objects.
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
