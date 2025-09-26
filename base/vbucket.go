// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"sync"
)

// VBNo represents a vBucket number
type VBNo uint16

// VBucketCAS is a map of vbucket number to a CAS value. This can not be copied, it is an atomic data structure.
type VBucketCAS struct {
	m sync.Map
}

// Load returns the CAS value for the specified vbucket. This is a thread safe accessor optimized for reads. If no CAS value is found, it asserts in debug builds and returns 0.
func (v *VBucketCAS) Load(ctx context.Context, vbNo VBNo) uint64 {
	rawCas, ok := v.m.Load(vbNo)
	if !ok {
		AssertfCtx(ctx, "VBucketCAS: No CAS found for vbucket %d", vbNo)
		return 0
	}
	cas, ok := rawCas.(uint64)
	if !ok {
		AssertfCtx(ctx, "VBucketCAS: Invalid CAS type %#v for vbucket %d", cas, vbNo)
		return 0
	}
	return cas
}

// Store the CAS value for the specified vbucket. This is a thread safe method optimized for reads.
func (v *VBucketCAS) Store(vbNo VBNo, cas uint64) {
	v.m.Store(vbNo, cas)
}

// Clear removes all entries from the VBucketCAS. This is a thread safe method.
func (v *VBucketCAS) Clear() {
	v.m.Clear()
}
