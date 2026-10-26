// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"sync/atomic"

	sgbucket "github.com/couchbase/sg-bucket"
)

// KVOpCountingDataStore wraps a DataStore and counts the KV operations made through it. Background work that runs even
// on a node with no clients wraps its DataStore for the duration of that work, and reports the count as idle KV ops.
//
// The count includes the reads and writes that make up each attempt of Update, so CAS retries are counted. For a
// MetadataStore, the primary and fallback DataStores are wrapped instead of the MetadataStore itself, so its fallback
// reads and its own retries are counted too.
type KVOpCountingDataStore struct {
	DataStore
	kvOps *atomic.Int64
}

// NewKVOpCountingDataStore returns a KVOpCountingDataStore that wraps ds.
func NewKVOpCountingDataStore(ds DataStore) *KVOpCountingDataStore {
	kvOps := &atomic.Int64{}
	return &KVOpCountingDataStore{
		DataStore: wrapKVOpCounting(ds, func(n int) { kvOps.Add(int64(n)) }),
		kvOps:     kvOps,
	}
}

// NewIdleKVOpDataStore wraps ds like NewKVOpCountingDataStore, but records each KV operation as an idle KV op as soon
// as it is made. Long-running background work uses it so that the stat moves while the work runs.
func NewIdleKVOpDataStore(ds DataStore) DataStore {
	return wrapKVOpCounting(ds, AddIdleKVOps)
}

// wrapKVOpCounting wraps ds so that onKVOps is called with the KV operations made through it.
func wrapKVOpCounting(ds DataStore, onKVOps func(n int)) DataStore {
	if ms, ok := ds.(*MetadataStore); ok {
		return ms.withDataStores(newKVOpCountingLeaf(ms.primary, onKVOps), newKVOpCountingLeaf(ms.fallback, onKVOps))
	}
	return newKVOpCountingLeaf(ds, onKVOps)
}

// KVOps returns the number of KV operations made through this DataStore.
func (c *KVOpCountingDataStore) KVOps() int {
	return int(c.kvOps.Load())
}

// kvOpCountingLeaf counts the KV operations made on a single DataStore.
type kvOpCountingLeaf struct {
	DataStore
	count func(n int) // called with the KV operations made
}

func newKVOpCountingLeaf(ds DataStore, onKVOps func(n int)) DataStore {
	if ds == nil {
		return nil
	}
	return &kvOpCountingLeaf{DataStore: ds, count: onKVOps}
}

func (c *kvOpCountingLeaf) Get(ctx context.Context, k string, rv any) (uint64, error) {
	c.count(1)
	return c.DataStore.Get(ctx, k, rv)
}

func (c *kvOpCountingLeaf) GetRaw(ctx context.Context, k string) ([]byte, uint64, error) {
	c.count(1)
	return c.DataStore.GetRaw(ctx, k)
}

func (c *kvOpCountingLeaf) GetAndTouchRaw(ctx context.Context, k string, exp uint32) ([]byte, uint64, error) {
	c.count(1)
	return c.DataStore.GetAndTouchRaw(ctx, k, exp)
}

func (c *kvOpCountingLeaf) Touch(ctx context.Context, k string, exp uint32) (uint64, error) {
	c.count(1)
	return c.DataStore.Touch(ctx, k, exp)
}

func (c *kvOpCountingLeaf) Add(ctx context.Context, k string, exp uint32, v any) (bool, error) {
	c.count(1)
	return c.DataStore.Add(ctx, k, exp, v)
}

func (c *kvOpCountingLeaf) AddRaw(ctx context.Context, k string, exp uint32, v []byte) (bool, error) {
	c.count(1)
	return c.DataStore.AddRaw(ctx, k, exp, v)
}

func (c *kvOpCountingLeaf) Set(ctx context.Context, k string, exp uint32, opts *sgbucket.UpsertOptions, v any) error {
	c.count(1)
	return c.DataStore.Set(ctx, k, exp, opts, v)
}

func (c *kvOpCountingLeaf) SetRaw(ctx context.Context, k string, exp uint32, opts *sgbucket.UpsertOptions, v []byte) error {
	c.count(1)
	return c.DataStore.SetRaw(ctx, k, exp, opts, v)
}

func (c *kvOpCountingLeaf) WriteCas(ctx context.Context, k string, exp uint32, cas uint64, v any, opt sgbucket.WriteOptions) (uint64, error) {
	c.count(1)
	return c.DataStore.WriteCas(ctx, k, exp, cas, v, opt)
}

func (c *kvOpCountingLeaf) Delete(ctx context.Context, k string) error {
	c.count(1)
	return c.DataStore.Delete(ctx, k)
}

func (c *kvOpCountingLeaf) Remove(ctx context.Context, k string, cas uint64) (uint64, error) {
	c.count(1)
	return c.DataStore.Remove(ctx, k, cas)
}

// Update counts a read for each attempt, and a write for each attempt whose callback asks for one. The underlying
// DataStore runs the CAS retry loop, so the callback is the only place each attempt can be seen.
func (c *kvOpCountingLeaf) Update(ctx context.Context, k string, exp uint32, callback sgbucket.UpdateFunc) (uint64, error) {
	attempts := 0
	casOut, err := c.DataStore.Update(ctx, k, exp, func(current []byte) ([]byte, *uint32, bool, error) {
		attempts++
		updated, expiry, isDelete, cbErr := callback(current)
		if cbErr == nil && (updated != nil || expiry != nil || isDelete) {
			c.count(1)
		}
		return updated, expiry, isDelete, cbErr
	})
	// A read that fails returns before the callback runs.
	c.count(max(attempts, 1))
	return casOut, err
}

func (c *kvOpCountingLeaf) Incr(ctx context.Context, k string, amt, def uint64, exp uint32) (uint64, error) {
	c.count(1)
	return c.DataStore.Incr(ctx, k, amt, def, exp)
}

func (c *kvOpCountingLeaf) GetExpiry(ctx context.Context, k string) (uint32, error) {
	c.count(1)
	return c.DataStore.GetExpiry(ctx, k)
}

func (c *kvOpCountingLeaf) Exists(ctx context.Context, k string) (bool, error) {
	c.count(1)
	return c.DataStore.Exists(ctx, k)
}

func (c *kvOpCountingLeaf) SubdocInsert(ctx context.Context, k string, subdocPath string, cas uint64, value any) error {
	c.count(1)
	return c.DataStore.SubdocInsert(ctx, k, subdocPath, cas, value)
}

func (c *kvOpCountingLeaf) GetSubDocRaw(ctx context.Context, k string, subdocPath string) ([]byte, uint64, error) {
	c.count(1)
	return c.DataStore.GetSubDocRaw(ctx, k, subdocPath)
}

func (c *kvOpCountingLeaf) WriteSubDoc(ctx context.Context, k string, subdocPath string, cas uint64, value []byte) (uint64, error) {
	c.count(1)
	return c.DataStore.WriteSubDoc(ctx, k, subdocPath, cas, value)
}

func (c *kvOpCountingLeaf) WriteWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, value []byte, xattrsValues map[string][]byte, xattrsToDelete []string, opts *sgbucket.MutateInOptions) (uint64, error) {
	c.count(1)
	return c.DataStore.WriteWithXattrs(ctx, k, exp, cas, value, xattrsValues, xattrsToDelete, opts)
}

func (c *kvOpCountingLeaf) WriteTombstoneWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, xattrValue map[string][]byte, xattrsToDelete []string, deleteBody bool, opts *sgbucket.MutateInOptions) (uint64, error) {
	c.count(1)
	return c.DataStore.WriteTombstoneWithXattrs(ctx, k, exp, cas, xattrValue, xattrsToDelete, deleteBody, opts)
}

func (c *kvOpCountingLeaf) WriteResurrectionWithXattrs(ctx context.Context, k string, exp uint32, body []byte, xattrs map[string][]byte, opts *sgbucket.MutateInOptions) (uint64, error) {
	c.count(1)
	return c.DataStore.WriteResurrectionWithXattrs(ctx, k, exp, body, xattrs, opts)
}

func (c *kvOpCountingLeaf) SetXattrs(ctx context.Context, k string, xattrs map[string][]byte) (uint64, error) {
	c.count(1)
	return c.DataStore.SetXattrs(ctx, k, xattrs)
}

func (c *kvOpCountingLeaf) RemoveXattrs(ctx context.Context, k string, xattrKeys []string, cas uint64) error {
	c.count(1)
	return c.DataStore.RemoveXattrs(ctx, k, xattrKeys, cas)
}

func (c *kvOpCountingLeaf) DeleteSubDocPaths(ctx context.Context, k string, paths ...string) error {
	c.count(1)
	return c.DataStore.DeleteSubDocPaths(ctx, k, paths...)
}

func (c *kvOpCountingLeaf) GetXattrs(ctx context.Context, k string, xattrKeys []string) (map[string][]byte, uint64, error) {
	c.count(1)
	return c.DataStore.GetXattrs(ctx, k, xattrKeys)
}

func (c *kvOpCountingLeaf) GetWithXattrs(ctx context.Context, k string, xattrKeys []string) ([]byte, map[string][]byte, uint64, error) {
	c.count(1)
	return c.DataStore.GetWithXattrs(ctx, k, xattrKeys)
}

func (c *kvOpCountingLeaf) DeleteWithXattrs(ctx context.Context, k string, xattrKeys []string) error {
	c.count(1)
	return c.DataStore.DeleteWithXattrs(ctx, k, xattrKeys)
}

// WriteUpdateWithXattrs counts like Update, except that the first attempt makes no read when the caller passes the
// document it already has.
func (c *kvOpCountingLeaf) WriteUpdateWithXattrs(ctx context.Context, k string, xattrKeys []string, exp uint32, previous *sgbucket.BucketDocument, opts *sgbucket.MutateInOptions, callback sgbucket.WriteUpdateWithXattrsFunc) (uint64, error) {
	attempts := 0
	casOut, err := c.DataStore.WriteUpdateWithXattrs(ctx, k, xattrKeys, exp, previous, opts, func(doc []byte, xattrs map[string][]byte, cas uint64) (sgbucket.UpdatedDoc, error) {
		attempts++
		updated, cbErr := callback(doc, xattrs, cas)
		if cbErr == nil {
			c.count(1)
		}
		return updated, cbErr
	})
	reads := attempts
	if attempts == 0 {
		reads = 1 // a read that fails returns before the callback runs
	} else if previous != nil {
		reads-- // the first attempt uses previous instead of reading
	}
	c.count(reads)
	return casOut, err
}

func (c *kvOpCountingLeaf) UpdateXattrs(ctx context.Context, k string, exp uint32, cas uint64, xv map[string][]byte, opts *sgbucket.MutateInOptions) (uint64, error) {
	c.count(1)
	return c.DataStore.UpdateXattrs(ctx, k, exp, cas, xv, opts)
}
