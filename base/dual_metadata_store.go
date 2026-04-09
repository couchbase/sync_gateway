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

// MetadataStore is a wrapper for a primary and fallback metadata store for read operations during metadata migration
type MetadataStore struct {
	primary           DataStore // _system._mobile scope/collection
	fallback          DataStore // _default._default scope/collection
	migrationComplete atomic.Bool
}

// Compile-time assertion that *MetadataStore implements DataStore.
var _ DataStore = &MetadataStore{}

// NewMetadataStore creates a MetadataStore wrapping the given primary and fallback DataStores.
// Reads are attempted on primary first; if the key is not found, the fallback is tried.
// All writes are directed to primary only.
func NewMetadataStore(primary, fallback DataStore) *MetadataStore {
	return &MetadataStore{
		primary:  primary,
		fallback: fallback,
	}
}

// Primary will return the primary data store for dual metadata store type (_system._mobile)
func (ms *MetadataStore) Primary() DataStore {
	return ms.primary
}

// Fallback will return the fallback data store for dual metadata store type (_default._default)
func (ms *MetadataStore) Fallback() DataStore {
	return ms.fallback
}

// SetMigrationComplete will set migration complete to true to stop reads falling back to fallback datastore.
func (ms *MetadataStore) SetMigrationComplete() {
	ms.migrationComplete.Store(true)
}

// readFromFallback returns true when err indicates the key was not found in primary and metadata migration has
// not yet complete, meaning the operation should be retried against the fallback DataStore.
func (ms *MetadataStore) readFromFallback(err error) bool {
	return IsDocNotFoundError(err) && !ms.migrationComplete.Load()
}

// ---- DataStoreName ----

func (ms *MetadataStore) GetName() string {
	return ms.primary.GetName()
}

func (ms *MetadataStore) ScopeName() string {
	return ms.primary.ScopeName()
}

func (ms *MetadataStore) CollectionName() string {
	return ms.primary.CollectionName()
}

// ---- DataStore identity ----

func (ms *MetadataStore) GetCollectionID() uint32 {
	return ms.primary.GetCollectionID()
}

func (ms *MetadataStore) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	return ms.primary.IsSupported(feature)
}

// ---- KVStore – read operations (primary with fallback) ----

func (ms *MetadataStore) Get(k string, rv any) (cas uint64, err error) {
	cas, err = ms.primary.Get(k, rv)
	if ms.readFromFallback(err) {
		cas, err = ms.fallback.Get(k, rv)
	}
	return cas, err
}

func (ms *MetadataStore) GetRaw(k string) (v []byte, cas uint64, err error) {
	v, cas, err = ms.primary.GetRaw(k)
	if ms.readFromFallback(err) {
		v, cas, err = ms.fallback.GetRaw(k)
	}
	return v, cas, err
}

func (ms *MetadataStore) GetExpiry(ctx context.Context, k string) (expiry uint32, err error) {
	expiry, err = ms.primary.GetExpiry(ctx, k)
	if ms.readFromFallback(err) {
		expiry, err = ms.fallback.GetExpiry(ctx, k)
	}
	return expiry, err
}

func (ms *MetadataStore) Exists(k string) (exists bool, err error) {
	exists, err = ms.primary.Exists(k)
	if err == nil && exists {
		return true, nil
	}
	if ms.readFromFallback(err) || (!exists && err == nil && !ms.migrationComplete.Load()) {
		return ms.fallback.Exists(k)
	}
	return exists, err
}

// ---- KVStore – write operations (primary only) ----

func (ms *MetadataStore) GetAndTouchRaw(k string, exp uint32) (v []byte, cas uint64, err error) {
	return ms.primary.GetAndTouchRaw(k, exp)
}

func (ms *MetadataStore) Touch(k string, exp uint32) (cas uint64, err error) {
	return ms.primary.Touch(k, exp)
}

func (ms *MetadataStore) Add(k string, exp uint32, v any) (added bool, err error) {
	return ms.primary.Add(k, exp, v)
}

func (ms *MetadataStore) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	return ms.primary.AddRaw(k, exp, v)
}

func (ms *MetadataStore) Set(k string, exp uint32, opts *sgbucket.UpsertOptions, v any) error {
	return ms.primary.Set(k, exp, opts, v)
}

func (ms *MetadataStore) SetRaw(k string, exp uint32, opts *sgbucket.UpsertOptions, v []byte) error {
	return ms.primary.SetRaw(k, exp, opts, v)
}

func (ms *MetadataStore) WriteCas(k string, exp uint32, cas uint64, v any, opt sgbucket.WriteOptions) (casOut uint64, err error) {
	return ms.primary.WriteCas(k, exp, cas, v, opt)
}

func (ms *MetadataStore) Delete(k string) error {
	return ms.primary.Delete(k)
}

func (ms *MetadataStore) Remove(k string, cas uint64) (casOut uint64, err error) {
	return ms.primary.Remove(k, cas)
}

func (ms *MetadataStore) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	return ms.primary.Update(k, exp, callback)
}

func (ms *MetadataStore) Incr(k string, amt, def uint64, exp uint32) (casOut uint64, err error) {
	return ms.primary.Incr(k, amt, def, exp)
}

// ---- XattrStore – read operations (primary with fallback) ----

func (ms *MetadataStore) GetXattrs(ctx context.Context, k string, xattrKeys []string) (xattrs map[string][]byte, cas uint64, err error) {
	xattrs, cas, err = ms.primary.GetXattrs(ctx, k, xattrKeys)
	if ms.readFromFallback(err) {
		xattrs, cas, err = ms.fallback.GetXattrs(ctx, k, xattrKeys)
	}
	return xattrs, cas, err
}

func (ms *MetadataStore) GetWithXattrs(ctx context.Context, k string, xattrKeys []string) (v []byte, xv map[string][]byte, cas uint64, err error) {
	v, xv, cas, err = ms.primary.GetWithXattrs(ctx, k, xattrKeys)
	if ms.readFromFallback(err) {
		v, xv, cas, err = ms.fallback.GetWithXattrs(ctx, k, xattrKeys)
	}
	return v, xv, cas, err
}

// ---- XattrStore – write operations (primary only) ----

func (ms *MetadataStore) WriteWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, value []byte, xattrs map[string][]byte, xattrsToDelete []string, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	return ms.primary.WriteWithXattrs(ctx, k, exp, cas, value, xattrs, xattrsToDelete, opts)
}

func (ms *MetadataStore) WriteTombstoneWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, xv map[string][]byte, xattrsToDelete []string, deleteBody bool, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	return ms.primary.WriteTombstoneWithXattrs(ctx, k, exp, cas, xv, xattrsToDelete, deleteBody, opts)
}

func (ms *MetadataStore) WriteResurrectionWithXattrs(ctx context.Context, k string, exp uint32, body []byte, xv map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	return ms.primary.WriteResurrectionWithXattrs(ctx, k, exp, body, xv, opts)
}

func (ms *MetadataStore) SetXattrs(ctx context.Context, k string, xattrs map[string][]byte) (casOut uint64, err error) {
	return ms.primary.SetXattrs(ctx, k, xattrs)
}

func (ms *MetadataStore) RemoveXattrs(ctx context.Context, k string, xattrKeys []string, cas uint64) error {
	return ms.primary.RemoveXattrs(ctx, k, xattrKeys, cas)
}

func (ms *MetadataStore) DeleteSubDocPaths(ctx context.Context, k string, paths ...string) error {
	return ms.primary.DeleteSubDocPaths(ctx, k, paths...)
}

func (ms *MetadataStore) DeleteWithXattrs(ctx context.Context, k string, xattrKeys []string) error {
	return ms.primary.DeleteWithXattrs(ctx, k, xattrKeys)
}

func (ms *MetadataStore) WriteUpdateWithXattrs(ctx context.Context, k string, xattrKeys []string, exp uint32, previous *sgbucket.BucketDocument, opts *sgbucket.MutateInOptions, callback sgbucket.WriteUpdateWithXattrsFunc) (casOut uint64, err error) {
	return ms.primary.WriteUpdateWithXattrs(ctx, k, xattrKeys, exp, previous, opts, callback)
}

func (ms *MetadataStore) UpdateXattrs(ctx context.Context, k string, exp uint32, cas uint64, xv map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	return ms.primary.UpdateXattrs(ctx, k, exp, cas, xv, opts)
}

// ---- SubdocStore – read operations (primary with fallback) ----

func (ms *MetadataStore) GetSubDocRaw(ctx context.Context, k string, subdocKey string) (value []byte, casOut uint64, err error) {
	value, casOut, err = ms.primary.GetSubDocRaw(ctx, k, subdocKey)
	if ms.readFromFallback(err) {
		value, casOut, err = ms.fallback.GetSubDocRaw(ctx, k, subdocKey)
	}
	return value, casOut, err
}

// ---- SubdocStore – write operations (primary only) ----

func (ms *MetadataStore) SubdocInsert(ctx context.Context, k string, subdocKey string, cas uint64, value any) error {
	return ms.primary.SubdocInsert(ctx, k, subdocKey, cas, value)
}

func (ms *MetadataStore) WriteSubDoc(ctx context.Context, k string, subdocKey string, cas uint64, value []byte) (casOut uint64, err error) {
	return ms.primary.WriteSubDoc(ctx, k, subdocKey, cas, value)
}
