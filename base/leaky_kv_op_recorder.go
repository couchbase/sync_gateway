/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"slices"
	"sync"
	"sync/atomic"
)

// RecordedKVOp is a call seen by a KVOpRecorder. KVOps is the number of KV operations the call made, where known.
type RecordedKVOp struct {
	Op    string
	Key   string
	KVOps int
}

// KVOpRecorder records the KV operations seen by the Leaky test wrappers while it is armed. Tests use it to check that
// KV work done in the background is counted in the num_idle_kv_ops stat. For testing use only.
type KVOpRecorder struct {
	armed atomic.Bool
	lock  sync.Mutex
	ops   []RecordedKVOp
}

// DataStoreOp records a KV operation on a data store. Use as LeakyBucketConfig.KVOpCallback.
func (r *KVOpRecorder) DataStoreOp(op, key string) {
	r.record(RecordedKVOp{Op: op, Key: key, KVOps: 1})
}

// BootstrapOp records a call on the bootstrap connection, with the number of KV operations it reported. Use as
// LeakyBootstrapConnectionConfig.KVOpCallback.
func (r *KVOpRecorder) BootstrapOp(op, bucket, key string, kvOps int) {
	r.record(RecordedKVOp{Op: "bootstrap:" + op, Key: bucket + "/" + key, KVOps: kvOps})
}

func (r *KVOpRecorder) record(op RecordedKVOp) {
	if !r.armed.Load() {
		return
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	r.ops = append(r.ops, op)
}

// Arm clears any recorded operations and starts recording.
func (r *KVOpRecorder) Arm() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.ops = nil
	r.armed.Store(true)
}

// Disarm stops recording and returns the operations recorded since Arm.
func (r *KVOpRecorder) Disarm() []RecordedKVOp {
	r.armed.Store(false)
	r.lock.Lock()
	defer r.lock.Unlock()
	return slices.Clone(r.ops)
}

// SumKVOps returns the total number of KV operations made by ops.
func SumKVOps(ops []RecordedKVOp) (kvOps int) {
	for _, op := range ops {
		kvOps += op.KVOps
	}
	return kvOps
}
