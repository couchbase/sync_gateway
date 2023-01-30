package js

import "testing"

// Unit-test utility. Calls the function with each supported type of VM (Otto and V8).
func TestWithVMs(t *testing.T, fn func(t *testing.T, vm VM)) {
	engines := []*Engine{V8, Otto}
	for _, engine := range engines {
		t.Run(engine.String(), func(t *testing.T) {
			vm := engine.NewVM()
			defer vm.Close()
			fn(t, vm)
		})
	}
}

// Unit-test utility. Calls the function with a VMPool of each supported type (Otto and V8).
// The behavior will be basically identical to TestWithVMs unless your test is multi-threaded.
func TestWithVMPools(t *testing.T, maxVMs int, fn func(t *testing.T, pool *VMPool)) {
	engines := []*Engine{V8, Otto}
	for _, engine := range engines {
		t.Run(engine.String(), func(t *testing.T) {
			pool := NewVMPool(engine, maxVMs)
			defer pool.Close()
			fn(t, pool)
		})
	}
}
