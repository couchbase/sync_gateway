// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build !cb_sg_v8

package js

// Returns the Engine with the given name, else nil.
// Valid names are "V8" and "Otto", which map to the instances `V8` and `Otto`.
func EngineNamed(name string) *Engine {
	switch name {
	case ottoVMName:
		return Otto
	default:
		return nil
	}
}
