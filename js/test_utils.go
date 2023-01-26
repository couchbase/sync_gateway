package js

import "testing"

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
