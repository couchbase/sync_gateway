package js

import "testing"

// Unit-test utility. Calls the function with each supported type of VM (Otto and V8).
func TestWithVMs(t *testing.T, fn func(t *testing.T, vm VM)) {
	types := []*VMType{V8, Otto}
	for _, vmType := range types {
		t.Run(vmType.String(), func(t *testing.T) {
			vm := NewVM(vmType)
			defer vm.Close()
			fn(t, vm)
		})
	}
}

// Unit-test utility. Calls the function with a VMPool of each supported type (Otto and V8).
// The behavior will be basically identical to TestWithVMs unless your test is multi-threaded.
func TestWithVMPools(t *testing.T, maxVMs int, fn func(t *testing.T, pool *VMPool)) {
	types := []*VMType{V8, Otto}
	for _, vmType := range types {
		t.Run(vmType.String(), func(t *testing.T) {
			pool := NewVMPool(vmType, maxVMs)
			defer pool.Close()
			fn(t, pool)
		})
	}
}
