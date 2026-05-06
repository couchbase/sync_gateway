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
	"errors"
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

func (ms *MetadataStore) IsDualMetadataStore() bool {
	return true
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

// MigrationComplete reports whether metadata migration has finished.
func (ms *MetadataStore) MigrationComplete() bool {
	return ms.migrationComplete.Load()
}

// readFromFallback returns true when err indicates the key was not found in primary and metadata migration has
// not yet complete, meaning the operation should be retried against the fallback DataStore.
func (ms *MetadataStore) readFromFallback(ctx context.Context, err error) bool {
	if IsDocNotFoundError(err) && !ms.migrationComplete.Load() {
		DebugfCtx(ctx, KeyCRUD, "falling back to fallback datastore for read")
		return true
	}
	return false
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
	if ms.readFromFallback(context.TODO(), err) {
		_, err = ms.fallback.Get(k, rv)
	}
	return cas, err
}

func (ms *MetadataStore) GetRaw(k string) (v []byte, cas uint64, err error) {
	v, cas, err = ms.primary.GetRaw(k)
	if ms.readFromFallback(context.TODO(), err) {
		v, _, err = ms.fallback.GetRaw(k)
	}
	return v, cas, err
}

func (ms *MetadataStore) GetExpiry(ctx context.Context, k string) (expiry uint32, err error) {
	expiry, err = ms.primary.GetExpiry(ctx, k)
	if ms.readFromFallback(ctx, err) {
		expiry, err = ms.fallback.GetExpiry(ctx, k)
	}
	return expiry, err
}

func (ms *MetadataStore) Exists(k string) (exists bool, err error) {
	exists, err = ms.primary.Exists(k)
	if err != nil && !ms.readFromFallback(context.TODO(), err) {
		return false, err
	}
	if exists || ms.migrationComplete.Load() {
		return exists, err
	}
	return ms.fallback.Exists(k)
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

// Update implements a CAS-safe read-modify-write that always lands in primary, even when the
// document only exists in the fallback store. When the doc is in primary (or fallback is no
// longer consulted) the call delegates to the primary's own Update — its CAS retry loop is
// already correct. When the doc is only in fallback, the wrapper feeds the fallback value to
// the caller's callback and inserts the result into primary; if a concurrent writer wins the
// race, the loop retries against primary.
//
// A delete returned by the callback for a doc that exists only in fallback is a no-op in
// primary; the metadata migration sweep will tombstone the fallback copy.
func (ms *MetadataStore) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	for {
		primaryExists, existsErr := ms.primary.Exists(k)
		if existsErr != nil {
			return 0, existsErr
		}
		if primaryExists || ms.migrationComplete.Load() {
			return ms.primary.Update(k, exp, callback)
		}

		fallbackValue, _, fallbackErr := ms.fallback.GetRaw(k)
		if IsDocNotFoundError(fallbackErr) {
			return ms.primary.Update(k, exp, callback)
		}
		if fallbackErr != nil {
			return 0, fallbackErr
		}

		newValue, cbExpiry, isDelete, cbErr := callback(fallbackValue)
		if cbErr != nil {
			return 0, cbErr
		}
		writeExp := exp
		if cbExpiry != nil {
			writeExp = *cbExpiry
		}
		if isDelete {
			return 0, nil
		}
		casOut, err = ms.primary.WriteCas(k, writeExp, 0, newValue, 0)
		if err == nil {
			return casOut, nil
		}
		if IsCasMismatch(err) {
			continue
		}
		return 0, err
	}
}

func (ms *MetadataStore) Incr(k string, amt, def uint64, exp uint32) (casOut uint64, err error) {
	return ms.primary.Incr(k, amt, def, exp)
}

// ---- XattrStore – read operations (primary with fallback) ----

func (ms *MetadataStore) GetXattrs(ctx context.Context, k string, xattrKeys []string) (xattrs map[string][]byte, cas uint64, err error) {
	xattrs, cas, err = ms.primary.GetXattrs(ctx, k, xattrKeys)
	if ms.readFromFallback(ctx, err) {
		xattrs, _, err = ms.fallback.GetXattrs(ctx, k, xattrKeys)
	}
	return xattrs, cas, err
}

func (ms *MetadataStore) GetWithXattrs(ctx context.Context, k string, xattrKeys []string) (v []byte, xv map[string][]byte, cas uint64, err error) {
	v, xv, cas, err = ms.primary.GetWithXattrs(ctx, k, xattrKeys)
	if ms.readFromFallback(ctx, err) {
		v, xv, _, err = ms.fallback.GetWithXattrs(ctx, k, xattrKeys)
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

// WriteUpdateWithXattrs is the xattr-aware analogue of Update and follows the same
// "read-from-both, write-to-primary" pattern. If the caller has supplied a previous CAS from
// the primary (or migration is complete), the call delegates straight to the primary's own
// retry loop. Otherwise the wrapper probes primary first; on a primary miss it reads
// body+xattrs from fallback, hands them to the callback with cas=0, and inserts the result
// into primary via WriteWithXattrs, retrying on CAS race.
//
// As with Update, a tombstone result for a doc that exists only in fallback is a no-op in
// primary — the metadata migration sweep will tombstone the fallback copy.
func (ms *MetadataStore) WriteUpdateWithXattrs(ctx context.Context, k string, xattrKeys []string, exp uint32, previous *sgbucket.BucketDocument, opts *sgbucket.MutateInOptions, callback sgbucket.WriteUpdateWithXattrsFunc) (casOut uint64, err error) {
	if (previous != nil && previous.Cas != 0) || ms.migrationComplete.Load() {
		return ms.primary.WriteUpdateWithXattrs(ctx, k, xattrKeys, exp, previous, opts, callback)
	}

	for {
		primaryExists, existsErr := ms.primary.Exists(k)
		if existsErr != nil {
			return 0, existsErr
		}
		if primaryExists {
			return ms.primary.WriteUpdateWithXattrs(ctx, k, xattrKeys, exp, nil, opts, callback)
		}

		fallbackBody, fallbackXattrs, _, fallbackErr := ms.fallback.GetWithXattrs(ctx, k, xattrKeys)
		if IsDocNotFoundError(fallbackErr) {
			return ms.primary.WriteUpdateWithXattrs(ctx, k, xattrKeys, exp, nil, opts, callback)
		}
		// A doc with no/partial xattrs is still a fallback hit — surface what we have to the
		// callback rather than treat it as not-found.
		if fallbackErr != nil && !IsXattrNotFoundError(fallbackErr) && !errors.Is(fallbackErr, ErrXattrPartialFound) {
			return 0, fallbackErr
		}

		updatedDoc, cbErr := callback(fallbackBody, fallbackXattrs, 0)
		if cbErr == ErrCasFailureShouldRetry {
			continue
		}
		if cbErr != nil {
			return 0, cbErr
		}
		writeExp := exp
		if updatedDoc.Expiry != nil {
			writeExp = *updatedDoc.Expiry
		}
		if updatedDoc.IsTombstone {
			return 0, nil
		}
		casOut, err = ms.primary.WriteWithXattrs(ctx, k, writeExp, 0, updatedDoc.Doc, updatedDoc.Xattrs, nil, opts)
		if err == nil {
			return casOut, nil
		}
		if IsCasMismatch(err) || IsDocNotFoundError(err) {
			continue
		}
		return 0, err
	}
}

func (ms *MetadataStore) UpdateXattrs(ctx context.Context, k string, exp uint32, cas uint64, xv map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	return ms.primary.UpdateXattrs(ctx, k, exp, cas, xv, opts)
}

// ---- SubdocStore – read operations (primary with fallback) ----

func (ms *MetadataStore) GetSubDocRaw(ctx context.Context, k string, subdocKey string) (value []byte, casOut uint64, err error) {
	value, casOut, err = ms.primary.GetSubDocRaw(ctx, k, subdocKey)
	if ms.readFromFallback(ctx, err) {
		value, _, err = ms.fallback.GetSubDocRaw(ctx, k, subdocKey)
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

// GetMaxVbno returns the number of vBuckets on this data store.
func (ms *MetadataStore) GetMaxVbno() (uint16, error) {
	return ms.primary.GetMaxVbno()
}
