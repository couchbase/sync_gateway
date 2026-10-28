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

	sgbucket "github.com/couchbase/sg-bucket"
)

// NewKVOpStatDataStore wraps ds so that every KV operation made through it is added to stat. Background work that runs
// even on a node with no clients uses NewIdleKVOpDataStore instead, so that its KV operations are counted as idle.
// A nil stat returns ds unchanged.
//
// A call that makes several KV operations, such as the CAS retries of Update or the extra steps of DeleteWithXattrs on a
// tombstone, counts each of them. For a MetadataStore, the primary and fallback DataStores are wrapped instead of the
// MetadataStore itself, so its fallback reads and its own retries are counted too.
func NewKVOpStatDataStore(ds DataStore, stat *SgwIntStat) DataStore {
	if stat == nil {
		return ds
	}
	if ms, ok := ds.(*MetadataStore); ok {
		return ms.withDataStores(newKVOpCountingLeaf(ms.primary, stat), newKVOpCountingLeaf(ms.fallback, stat))
	}
	return newKVOpCountingLeaf(ds, stat)
}

// NewIdleKVOpDataStore wraps ds so that every KV operation made through it is counted as an idle KV op.
func NewIdleKVOpDataStore(ds DataStore) DataStore {
	return NewKVOpStatDataStore(ds, SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps)
}

// kvOpCountKey is the context key for a kvOpCount.
type kvOpCountKey struct{}

// kvOpCount holds the KV operations that one DataStore call reports making. Only Couchbase Server's Collection reports
// them, from the methods that make several KV operations in one call.
type kvOpCount struct {
	n int
}

// withKVOpCount returns a context that a DataStore call reports its KV operations to, and the count it reports to.
func withKVOpCount(ctx context.Context) (context.Context, *kvOpCount) {
	count := &kvOpCount{}
	return context.WithValue(ctx, kvOpCountKey{}, count), count
}

// addKVOps adds n to the KV operation count in ctx, if there is one.
func addKVOps(ctx context.Context, n int) {
	if count, ok := ctx.Value(kvOpCountKey{}).(*kvOpCount); ok {
		count.n += n
	}
}

// kvOpCountingLeaf adds the KV operations made on a single DataStore to stat.
type kvOpCountingLeaf struct {
	DataStore
	stat *SgwIntStat
}

func newKVOpCountingLeaf(ds DataStore, stat *SgwIntStat) DataStore {
	if ds == nil {
		return nil
	}
	return &kvOpCountingLeaf{DataStore: ds, stat: stat}
}

func (c *kvOpCountingLeaf) count(n int) {
	c.stat.Add(int64(n))
}

// countReported counts the KV operations that the underlying DataStore reported. A DataStore that reports none, such as
// rosmar, is counted as estimated instead.
func (c *kvOpCountingLeaf) countReported(reported *kvOpCount, estimated int) {
	if reported.n > 0 {
		c.count(reported.n)
		return
	}
	c.count(estimated)
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

// Update counts the KV operations the underlying DataStore reports. Otherwise it estimates them from the callback, which
// is the only place each attempt of the underlying CAS retry loop can be seen: a read for each attempt, and a write for
// each attempt whose callback asks for one.
func (c *kvOpCountingLeaf) Update(ctx context.Context, k string, exp uint32, callback sgbucket.UpdateFunc) (uint64, error) {
	ctx, reported := withKVOpCount(ctx)
	attempts, writes := 0, 0
	casOut, err := c.DataStore.Update(ctx, k, exp, func(current []byte) ([]byte, *uint32, bool, error) {
		attempts++
		updated, expiry, isDelete, cbErr := callback(current)
		if cbErr == nil && (updated != nil || expiry != nil || isDelete) {
			writes++
		}
		return updated, expiry, isDelete, cbErr
	})
	// A read that fails returns before the callback runs.
	c.countReported(reported, max(attempts, 1)+writes)
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

// DeleteWithXattrs counts the KV operations the underlying DataStore reports, because on Couchbase Server a doc with no
// body or no xattrs takes more than one.
func (c *kvOpCountingLeaf) DeleteWithXattrs(ctx context.Context, k string, xattrKeys []string) error {
	ctx, reported := withKVOpCount(ctx)
	err := c.DataStore.DeleteWithXattrs(ctx, k, xattrKeys)
	c.countReported(reported, 1)
	return err
}

// WriteUpdateWithXattrs counts like Update, except that the first attempt makes no read when the caller passes the
// document it already has.
func (c *kvOpCountingLeaf) WriteUpdateWithXattrs(ctx context.Context, k string, xattrKeys []string, exp uint32, previous *sgbucket.BucketDocument, opts *sgbucket.MutateInOptions, callback sgbucket.WriteUpdateWithXattrsFunc) (uint64, error) {
	ctx, reported := withKVOpCount(ctx)
	attempts, writes := 0, 0
	casOut, err := c.DataStore.WriteUpdateWithXattrs(ctx, k, xattrKeys, exp, previous, opts, func(doc []byte, xattrs map[string][]byte, cas uint64) (sgbucket.UpdatedDoc, error) {
		attempts++
		updated, cbErr := callback(doc, xattrs, cas)
		if cbErr == nil {
			writes++
		}
		return updated, cbErr
	})
	reads := attempts
	if attempts == 0 {
		reads = 1 // a read that fails returns before the callback runs
	} else if previous != nil {
		reads-- // the first attempt uses previous instead of reading
	}
	c.countReported(reported, reads+writes)
	return casOut, err
}

func (c *kvOpCountingLeaf) UpdateXattrs(ctx context.Context, k string, exp uint32, cas uint64, xv map[string][]byte, opts *sgbucket.MutateInOptions) (uint64, error) {
	c.count(1)
	return c.DataStore.UpdateXattrs(ctx, k, exp, cas, xv, opts)
}
