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
	"fmt"
	"math"
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

// MetadataStoreMode classifies a database's metadata store for support / observability.
type MetadataStoreMode string

const (
	// MetadataStoreModeFallbackActive means the dual store is wrapping primary+fallback and
	// fallback reads are still in play (migration not yet complete).
	MetadataStoreModeFallbackActive MetadataStoreMode = "fallback_active"
	// MetadataStoreModeFallbackInactive means the dual store is wrapping primary+fallback but
	// migration has completed, so reads no longer fall back.
	MetadataStoreModeFallbackInactive MetadataStoreMode = "fallback_inactive"
)

// GetMetadataStoreMode classifies the given datastore. Returns an empty string when the
// datastore isn't a *MetadataStore (single-store databases); callers should use JSON
// omitempty so the field is dropped in that case rather than emitted as "".
func GetMetadataStoreMode(ds DataStore) MetadataStoreMode {
	ms, ok := ds.(*MetadataStore)
	if !ok {
		return ""
	}
	if ms.MigrationComplete() {
		return MetadataStoreModeFallbackInactive
	}
	return MetadataStoreModeFallbackActive
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

func (ms *MetadataStore) GetMaxVbno(ctx context.Context) (uint16, error) {
	return ms.primary.GetMaxVbno(ctx)
}

func (ms *MetadataStore) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	return ms.primary.IsSupported(feature)
}

// ---- KVStore – read operations (primary with fallback) ----

func (ms *MetadataStore) Get(ctx context.Context, k string, rv any) (cas uint64, err error) {
	cas, err = ms.primary.Get(ctx, k, rv)
	if ms.readFromFallback(ctx, err) {
		_, err = ms.fallback.Get(ctx, k, rv)
	}
	return cas, err
}

func (ms *MetadataStore) GetRaw(ctx context.Context, k string) (v []byte, cas uint64, err error) {
	v, cas, err = ms.primary.GetRaw(ctx, k)
	if ms.readFromFallback(ctx, err) {
		v, _, err = ms.fallback.GetRaw(ctx, k)
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

func (ms *MetadataStore) Exists(ctx context.Context, k string) (exists bool, err error) {
	exists, err = ms.primary.Exists(ctx, k)
	if err != nil && !ms.readFromFallback(ctx, err) {
		return false, err
	}
	if exists || ms.migrationComplete.Load() {
		return exists, err
	}
	return ms.fallback.Exists(ctx, k)
}

// ---- KVStore – write operations (primary only) ----

func (ms *MetadataStore) GetAndTouchRaw(ctx context.Context, k string, exp uint32) (v []byte, cas uint64, err error) {
	return ms.primary.GetAndTouchRaw(ctx, k, exp)
}

func (ms *MetadataStore) Touch(ctx context.Context, k string, exp uint32) (cas uint64, err error) {
	return ms.primary.Touch(ctx, k, exp)
}

func (ms *MetadataStore) Add(ctx context.Context, k string, exp uint32, v any) (added bool, err error) {
	return ms.primary.Add(ctx, k, exp, v)
}

func (ms *MetadataStore) AddRaw(ctx context.Context, k string, exp uint32, v []byte) (added bool, err error) {
	return ms.primary.AddRaw(ctx, k, exp, v)
}

func (ms *MetadataStore) Set(ctx context.Context, k string, exp uint32, opts *sgbucket.UpsertOptions, v any) error {
	return ms.primary.Set(ctx, k, exp, opts, v)
}

func (ms *MetadataStore) SetRaw(ctx context.Context, k string, exp uint32, opts *sgbucket.UpsertOptions, v []byte) error {
	return ms.primary.SetRaw(ctx, k, exp, opts, v)
}

func (ms *MetadataStore) WriteCas(ctx context.Context, k string, exp uint32, cas uint64, v any, opt sgbucket.WriteOptions) (casOut uint64, err error) {
	return ms.primary.WriteCas(ctx, k, exp, cas, v, opt)
}

func (ms *MetadataStore) Delete(ctx context.Context, k string) error {
	return ms.primary.Delete(ctx, k)
}

func (ms *MetadataStore) Remove(ctx context.Context, k string, cas uint64) (casOut uint64, err error) {
	return ms.primary.Remove(ctx, k, cas)
}

// Update implements a CAS-safe read-modify-write that always lands in primary, even when the document only exists in the fallback DataStore.
//
// Covers two cases:
//  1. When the doc is already in primary the call simply delegates to the primary datastore's own Update. Happy-path.
//  2. When the doc is only in fallback, the wrapper feeds the fallback value to the callback and performs a CAS-safe insert of the result into primary.
//     If a concurrent writer wins the race inserting into Primary, the loop retries the update operation against primary version of the document.
//
// When the callback requests a delete on a fallback-only doc, the wrapper applies the delete
// directly to the fallback store and ignores the primary (since the only DataStore has the doc to delete was the fallback).
func (ms *MetadataStore) Update(ctx context.Context, k string, exp uint32, callback sgbucket.UpdateFunc) (uint64, error) {
	worker := func() (shouldRetry bool, err error, casOut uint64) {
		primaryExists, existsErr := ms.primary.Exists(ctx, k)
		if existsErr != nil {
			return false, existsErr, 0
		}

		// If we know something exists in Primary, do a direct Update there
		if primaryExists || ms.migrationComplete.Load() {
			casOut, updateErr := ms.primary.Update(ctx, k, exp, callback)
			return false, updateErr, casOut
		}

		// Begin fallback DataStore Get to perform Update
		fallbackValue, fallbackCasForDelete, err := ms.fallback.GetRaw(ctx, k)
		if IsDocNotFoundError(err) {
			// Neither store has the doc — let primary's own Update insert it.
			casOut, updateErr := ms.primary.Update(ctx, k, exp, callback)
			return false, updateErr, casOut
		}
		if err != nil {
			return false, err, 0
		}

		newValue, cbExpiry, isDelete, cbErr := callback(fallbackValue)
		if cbErr != nil {
			return false, cbErr, 0
		}
		writeExp := exp
		if cbExpiry != nil {
			writeExp = *cbExpiry
		}

		if isDelete {
			// Delete the fallback copy — primary never held it, so a primary tombstone is
			// pointless and would shadow the fallback for nothing.
			// Remove with the CAS we just observed; on a concurrent fallback mutation we retry the loop.
			_, removeErr := ms.fallback.Remove(ctx, k, fallbackCasForDelete)
			switch {
			case removeErr == nil, IsDocNotFoundError(removeErr):
				return false, nil, 0
			case IsCasMismatch(removeErr):
				// retry: concurrent fallback mutation underneath this Delete
				//        This shouldn't happen with the MetadataStore wrapper only routing writes into Primary,
				//        but it could be two concurrent Deletes that we should at least have one more attempt for
				return true, nil, 0
			}
			return false, removeErr, 0
		}

		// Write to primary with CAS=0 to force safe insertion
		casOut, writeErr := ms.primary.WriteCas(ctx, k, writeExp, 0, newValue, 0)
		if IsCasMismatch(writeErr) {
			// retry: concurrent writer beat us to primary
			//        retry loop will run again and be routed directly to the primary DataStore Update
			return true, nil, 0
		}
		if writeErr != nil {
			return false, writeErr, 0
		}

		// success: Only case where we can actually return a non-zero CAS - successful Primary DataStore Insert.
		return false, nil, casOut
	}

	err, casOut := RetryLoopCas(ctx, "MetadataStore.Update", worker, DefaultRetrySleeper())
	return casOut, err
}

// Incr runs an Incr op on Primary (with some fallback handling and required migration steps).
//
// The outer loop only ever re-enters when another SG node moves the counter to primary
// between our fallback.Incr and our pill-read GetRaw - exactly one extra iteration in practice.
func (ms *MetadataStore) Incr(ctx context.Context, k string, amt, def uint64, exp uint32) (casOut uint64, err error) {
	const maxAttempts = 2
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if ms.migrationComplete.Load() {
			return ms.primary.Incr(ctx, k, amt, def, exp)
		}

		// Happy-path (already migrated incr - don't allow creation of doc via MaxUint64 def)
		result, primaryErr := ms.primary.Incr(ctx, k, amt, math.MaxUint64, exp)
		if primaryErr == nil {
			return result, nil
		}
		if !IsDocNotFoundError(primaryErr) {
			return 0, primaryErr
		}

		// Primary missing - try fallback with the "don't-create" sentinel def.
		result, fbIncrErr := ms.fallback.Incr(ctx, k, amt, math.MaxUint64, exp)
		if fbIncrErr == nil {
			// Legacy unmigrated counter - pass through. The migration manager (or a later
			// wrapper call after the pill is written) will move it to primary.
			return result, nil
		}

		// Brand-new counter, create on primary at the caller's default.
		if IsDocNotFoundError(fbIncrErr) {
			return ms.primary.Incr(ctx, k, amt, def, exp)
		}
		// any other errors bubble up
		if !IsCounterNonNumeric(fbIncrErr) {
			return 0, fbIncrErr
		}

		// Fallback returned DELTA_BADVAL - we're mid-migration of the seq - the doc has
		// been poison pilled but not yet moved. We'll attempt to move it here in case the
		// migration process failed part way through.

		// 1. read pill counter value
		raw, _, fbErr := ms.fallback.GetRaw(ctx, k)
		if IsDocNotFoundError(fbErr) {
			// Sibling node finished the move between our fallback.Incr (which saw the
			// pill and returned non-numeric) and this GetRaw. Loop back to the top -
			// primary now holds the counter so the happy-path branch wins on retry.
			continue
		}
		if fbErr != nil {
			return 0, fbErr
		}
		var pill SyncSeqMigrationPill
		if jsonErr := JSONUnmarshal(raw, &pill); jsonErr != nil || !pill.SGMetadataMigrationPill {
			return 0, fmt.Errorf("MetadataStore.Incr: unrecognised fallback body for counter %s during migration", UD(k))
		}

		// 2. seed primary at pill.LastSeq via Incr(amt=0, def=pill.LastSeq). Using Incr
		// rather than AddRaw matters on Couchbase Server: AddRaw writes the doc with
		// the Binary datatype flag, which SGJSONTranscoder (used by GetCounter on
		// later reads) refuses to decode — causing the sequence allocator to fail
		// post-migration with "you must encode raw JSON data in a byte array or
		// string". Incr creates counters with the proper datatype. Race-tolerant:
		// if a sibling already promoted the counter, Incr(0, def, 0) returns the
		// existing value without overwriting.
		if _, seedErr := ms.primary.Incr(ctx, k, 0, pill.LastSeq, 0); seedErr != nil {
			return 0, seedErr
		}
		// 3. delete fallback pill
		if delErr := ms.fallback.Delete(ctx, k); delErr != nil && !IsDocNotFoundError(delErr) {
			WarnfCtx(ctx, "MetadataStore.Incr: failed to clear migrated fallback counter %s: %v", UD(k), delErr)
		}

		// 4. retry the caller's Incr on the now migrated primary
		return ms.primary.Incr(ctx, k, amt, def, exp)
	}
	return 0, fmt.Errorf("MetadataStore.Incr: gave up after %d self-heal attempts for counter %s", maxAttempts, UD(k))
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

// WriteUpdateWithXattrs is the xattr-aware analogue of Update — a CAS-safe read-modify-write that always lands in primary, even when the document only exists in the fallback DataStore.
//
// Covers two cases:
//  1. When the doc is already in primary (or the caller passes a non-zero previous.Cas, which by contract is a primary cas) the call simply delegates to the primary datastore's own WriteUpdateWithXattrs. Happy-path.
//  2. When the doc is only in fallback, the wrapper feeds the fallback body+xattrs to the callback with cas=0 and performs a CAS-safe insert of the result into primary via WriteWithXattrs.
//     If a concurrent writer wins the race inserting into Primary, the loop retries the update operation against primary version of the document.
//
// When the callback returns IsTombstone for a fallback-only doc, the wrapper writes the tombstone
// directly to fallback and ignores the primary (since the only DataStore that has the doc to tombstone was the fallback).
//
// Callers must not feed a fallback CAS back through previous.Cas — the wrapper's read methods only surface primary CAS, so any non-zero previous.Cas is treated as primary; misuse is documented but not enforced.
func (ms *MetadataStore) WriteUpdateWithXattrs(ctx context.Context, k string, xattrKeys []string, exp uint32, previous *sgbucket.BucketDocument, opts *sgbucket.MutateInOptions, callback sgbucket.WriteUpdateWithXattrsFunc) (uint64, error) {
	// If we're updating with a previous.Cas - we'll be updating on top of a prior fetch from the primary datastore, not the fallback data store.
	// Or if we've tagged MetadataStore with 'migrationComplete' - avoid the fallback effort...
	if (previous != nil && previous.Cas != 0) || ms.migrationComplete.Load() {
		return ms.primary.WriteUpdateWithXattrs(ctx, k, xattrKeys, exp, previous, opts, callback)
	}

	// Retry loop for Primary/Fallback
	worker := func() (shouldRetry bool, err error, casOut uint64) {
		primaryExists, existsErr := ms.primary.Exists(ctx, k)
		if existsErr != nil {
			return false, existsErr, 0
		}

		// If we know something exists in Primary, do a direct WriteUpdateWithXattrs there
		if primaryExists || ms.migrationComplete.Load() {
			casOut, updateErr := ms.primary.WriteUpdateWithXattrs(ctx, k, xattrKeys, exp, nil, opts, callback)
			return false, updateErr, casOut
		}

		// Begin fallback DataStore Get to perform WriteUpdateWithXattrs
		fallbackBody, fallbackXattrs, fallbackCasForTombstone, err := ms.fallback.GetWithXattrs(ctx, k, xattrKeys)
		if IsDocNotFoundError(err) {
			// Neither store has the doc — let primary's own WriteUpdateWithXattrs insert it.
			casOut, updateErr := ms.primary.WriteUpdateWithXattrs(ctx, k, xattrKeys, exp, nil, opts, callback)
			return false, updateErr, casOut
		}
		// A doc with no/partial xattrs is still a fallback hit — surface what we have to the
		// callback rather than treat it as not-found.
		if err != nil && !IsXattrNotFoundError(err) && !errors.Is(err, ErrXattrPartialFound) {
			return false, err, 0
		}

		updatedDoc, cbErr := callback(fallbackBody, fallbackXattrs, 0)
		if errors.Is(cbErr, ErrCasFailureShouldRetry) {
			// retry: callback explicitly asked us to retry
			return true, nil, 0
		}
		if cbErr != nil {
			return false, cbErr, 0
		}
		writeExp := exp
		if updatedDoc.Expiry != nil {
			writeExp = *updatedDoc.Expiry
		}

		if updatedDoc.IsTombstone {
			// Tombstone the fallback copy directly — primary never held it, so a primary tombstone is
			// pointless and would shadow the fallback for nothing.
			// deleteBody=true clears the body while preserving the requested xattrs (if any) on the fallback record.
			_, tombstoneErr := ms.fallback.WriteTombstoneWithXattrs(ctx, k, writeExp, fallbackCasForTombstone, updatedDoc.Xattrs, updatedDoc.XattrsToDelete, true, opts)
			switch {
			case tombstoneErr == nil, IsDocNotFoundError(tombstoneErr):
				return false, nil, 0
			case IsCasMismatch(tombstoneErr):
				// retry: concurrent fallback mutation underneath this Tombstone
				//        This shouldn't happen with the MetadataStore wrapper only routing writes into Primary,
				//        but it could be two concurrent Tombstones that we should at least have one more attempt for
				return true, nil, 0
			}
			return false, tombstoneErr, 0
		}

		// Write to primary with CAS=0 to force safe insertion.
		// xattrsToDelete is intentionally nil here: cas=0 is an insert and the underlying primary
		// store rejects xattrsToDelete on insert (sgbucket.ErrDeleteXattrOnDocumentInsert).
		// Anything the callback wanted dropped is already absent from updatedDoc.Xattrs.
		casOut, writeErr := ms.primary.WriteWithXattrs(ctx, k, writeExp, 0, updatedDoc.Doc, updatedDoc.Xattrs, nil, opts)
		if IsCasMismatch(writeErr) || IsDocNotFoundError(writeErr) {
			// retry: concurrent writer beat us to primary
			//        retry loop will run again and be routed directly to the primary DataStore WriteUpdateWithXattrs.
			return true, nil, 0
		}
		if writeErr != nil {
			return false, writeErr, 0
		}

		// success: Only case where we can actually return a non-zero CAS - successful Primary DataStore Insert.
		return false, nil, casOut
	}

	err, casOut := RetryLoopCas(ctx, "MetadataStore.WriteUpdateWithXattrs", worker, DefaultRetrySleeper())
	return casOut, err
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
