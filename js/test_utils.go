/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

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
