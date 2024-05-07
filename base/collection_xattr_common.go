// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"fmt"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

const (
	xattrMacroCas         = "cas"
	xattrMacroValueCrc32c = "value_crc32c"
)

// CAS-safe write of a document and it's associated named xattr
func (c *Collection) WriteWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, v []byte, xattrs map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {

	worker := func() (shouldRetry bool, err error, value uint64) {
		// cas=0 specifies an insert
		if cas == 0 {
			casOut, err = c.insertBodyAndXattrs(ctx, k, exp, v, xattrs, opts)
			if err != nil {
				shouldRetry = c.isRecoverableWriteError(err)
				return shouldRetry, err, uint64(0)
			}
			return false, nil, casOut
		}

		// Otherwise, replace existing value
		if v != nil {
			// Have value and xattr value - update both
			casOut, err = c.updateBodyAndXattrs(ctx, k, exp, cas, opts, v, xattrs)
			if err != nil {
				shouldRetry = c.isRecoverableWriteError(err)
				return shouldRetry, err, uint64(0)
			}
		} else {
			// Update xattr only
			casOut, err = c.UpdateXattrs(ctx, k, exp, cas, xattrs, opts)
			if err != nil {
				shouldRetry = c.isRecoverableWriteError(err)
				return shouldRetry, err, uint64(0)
			}
		}
		return false, nil, casOut
	}

	// Kick off retry loop
	err, cas = RetryLoopCas(ctx, "WriteCasWithXattr", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "WriteCasWithXattr with key %v", UD(k).Redact())
	}

	return cas, err
}

// CAS-safe update of a document's xattr (only).  Deletes the document body if deleteBody is true.
func (c *Collection) WriteTombstoneWithXattrs(ctx context.Context, k string, exp uint32, cas uint64, xattrs map[string][]byte, deleteBody bool, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {

	requiresBodyRemoval := false
	worker := func() (shouldRetry bool, err error, value uint64) {

		var casOut uint64
		var tombstoneErr error

		// If deleteBody == true, remove the body and update xattr
		if deleteBody {
			casOut, tombstoneErr = c.updateXattrsDeleteBody(ctx, k, exp, cas, xattrs, opts)
		} else {
			if cas == 0 {
				// if cas == 0, create a new server tombstone with xattr
				casOut, tombstoneErr = c.createTombstone(ctx, k, exp, cas, xattrs, opts)
				requiresBodyRemoval = !c.IsSupported(sgbucket.BucketStoreFeatureCreateDeletedWithXattr)
			} else {
				// If cas is non-zero, this is an already existing tombstone.  Update xattr only
				casOut, tombstoneErr = c.UpdateXattrs(ctx, k, exp, cas, xattrs, opts)
			}
		}

		if tombstoneErr != nil {
			shouldRetry = c.isRecoverableWriteError(tombstoneErr)
			return shouldRetry, tombstoneErr, uint64(0)
		}
		return false, nil, casOut
	}

	// Kick off retry loop
	err, cas = RetryLoopCas(ctx, "UpdateTombstoneXattrs", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "Error during UpdateTombstoneXattrs with key %v", UD(k).Redact())
		return cas, err
	}

	// In the case where the SubdocDocFlagCreateAsDeleted is not available and we are performing the creation of a
	// tombstoned document we need to perform this second operation. This is due to the fact that SubdocDocFlagMkDoc
	// will have been used above instead which will create an empty body {} which we then need to delete here. If there
	// is a CAS mismatch we exit the operation as this means there has been a subsequent update to the body.
	if requiresBodyRemoval {
		worker := func() (shouldRetry bool, err error, value uint64) {

			casOut, removeErr := c.deleteBody(ctx, k, exp, cas, opts)
			if removeErr != nil {
				// If there is a cas mismatch the body has since been updated and so we don't need to bother removing
				// body in this operation
				if IsCasMismatch(removeErr) {
					return false, nil, cas
				}

				shouldRetry = c.isRecoverableWriteError(removeErr)
				return shouldRetry, removeErr, uint64(0)
			}
			return false, nil, casOut
		}

		err, cas = RetryLoopCas(ctx, "UpdateXattrDeleteBodySecondOp", worker, DefaultRetrySleeper())
		if err != nil {
			err = pkgerrors.Wrapf(err, "Error during UpdateTombstoneXattr delete op with key %v", UD(k).Redact())
			return cas, err
		}

	}

	return cas, err
}

// WriteUpdateWithXattrs retrieves the existing doc from the bucket, invokes the callback to update
// the document, then writes the new document to the bucket.  Will repeat this process on cas
// failure.
//
// If previous document is provided, will use it on 1st iteration instead of retrieving from bucket.
// A zero CAS in `previous` is interpreted as no document existing; this can be used to short-
// circuit the initial Get when the document is unlikely to already exist.
func (c *Collection) WriteUpdateWithXattrs(ctx context.Context, k string, xattrKeys []string, exp uint32, previous *sgbucket.BucketDocument, opts *sgbucket.MutateInOptions, callback sgbucket.WriteUpdateWithXattrsFunc) (casOut uint64, err error) {

	var value []byte
	var xattrs map[string][]byte
	var cas uint64
	emptyCas := uint64(0)

	for {
		var err error
		if previous != nil {
			// If an existing value has been provided, use that as the initial value.
			// A zero CAS is interpreted as no document existing.
			if previous.Cas != 0 {
				value = previous.Body
				xattrs = previous.Xattrs
				cas = previous.Cas
			}
			previous = nil // a retry will get value from bucket, as below
		} else {
			// If no existing value has been provided, or on a retry,
			// retrieve the current value from the bucket
			value, xattrs, cas, err = c.subdocGetBodyAndXattrs(ctx, k, xattrKeys, true)

			if err != nil {
				if pkgerrors.Cause(err) != ErrNotFound {
					// Unexpected error, cancel writeupdate
					DebugfCtx(ctx, KeyCRUD, "Retrieval of existing doc failed during WriteUpdateWithXattr for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKeys), err)
					return emptyCas, err
				}
				// Key not found - initialize values
				value = nil
				xattrs = nil
			}
		}

		// Invoke callback to get updated value
		updatedDoc, err := callback(value, xattrs, cas)
		//updatedValue, updatedXattrValue, isDelete, callbackExpiry, updatedSpec, err := callback(value, xattrs, cas)

		// If it's an ErrCasFailureShouldRetry, then retry by going back through the for loop
		if err == ErrCasFailureShouldRetry {
			continue
		}

		// On any other errors, abort the Write attempt
		if err != nil {
			return emptyCas, err
		}
		if updatedDoc.Expiry != nil {
			exp = *updatedDoc.Expiry
		}
		if updatedDoc.Spec != nil {
			opts.MacroExpansion = append(opts.MacroExpansion, updatedDoc.Spec...)
		}

		var writeErr error
		// Attempt to write the updated document to the bucket.  Mark body for deletion if previous body was non-empty
		deleteBody := value != nil
		if updatedDoc.IsTombstone {
			casOut, writeErr = c.WriteTombstoneWithXattrs(ctx, k, exp, cas, updatedDoc.Xattrs, deleteBody, opts)
		} else {
			casOut, writeErr = c.WriteWithXattrs(ctx, k, exp, cas, updatedDoc.Doc, updatedDoc.Xattrs, opts)
		}

		if writeErr == nil {
			return casOut, nil
		}

		if IsCasMismatch(writeErr) {
			// Retry on cas failure.  ErrNotStored is returned in some concurrent insert races that appear to be related
			// to the timing of concurrent xattr subdoc operations.  Treating as CAS failure as these will get the usual
			// conflict/duplicate handling on retry.
		} else {
			// WriteWithXattr already handles retry on recoverable errors, so fail on any errors other than ErrKeyExists
			WarnfCtx(ctx, "Failed to update doc with xattr for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKeys), writeErr)
			return emptyCas, writeErr
		}

		// Reset value, xattr, cas for cas retry
		value = nil
		xattrs = nil
		cas = 0
	}
}

// SetXattrs performs a subdoc set on the supplied xattrs. Implements a retry for recoverable failures.
func SetXattrs(ctx context.Context, store *Collection, k string, xvs map[string][]byte) (casOut uint64, err error) {

	worker := func() (shouldRetry bool, err error, value uint64) {
		casOut, writeErr := store.SubdocSetXattrs(k, xvs)
		if writeErr == nil {
			return false, nil, casOut
		}

		shouldRetry = store.isRecoverableWriteError(writeErr)
		if shouldRetry {
			return shouldRetry, err, 0
		}

		return false, writeErr, 0
	}

	err, casOut = RetryLoopCas(ctx, "SetXattr", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SetXattr with key %v", UD(k).Redact())
	}

	return casOut, err

}

// RemoveXattr performs a cas safe subdoc delete of the provided key. Will retry if a recoverable failure occurs.
func RemoveXattrs(ctx context.Context, store *Collection, k string, xattrKeys []string, cas uint64) error {
	worker := func() (shouldRetry bool, err error, value interface{}) {
		writeErr := store.subdocDeleteXattrs(k, xattrKeys, cas)
		if writeErr == nil {
			return false, nil, nil
		}

		shouldRetry = store.isRecoverableWriteError(writeErr)
		if shouldRetry {
			return shouldRetry, err, nil
		}

		return false, err, nil
	}

	err, _ := RetryLoop(ctx, "RemoveXattrs", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "RemoveXattr with key %v xattr %v", UD(k).Redact(), UD(xattrKeys).Redact())
	}

	return err
}

// removeSubdocPaths performs a subdoc delete of the provided keys. Retries any recoverable failures. Not cas safe.
func removeSubdocPaths(ctx context.Context, store *Collection, k string, subdocPaths ...string) error {
	worker := func() (shouldRetry bool, err error, value interface{}) {
		writeErr := store.subdocRemovePaths(k, subdocPaths...)
		if writeErr == nil {
			return false, nil, nil
		}

		shouldRetry = store.isRecoverableWriteError(writeErr)
		if shouldRetry {
			return shouldRetry, err, nil
		}

		return false, err, nil
	}

	err, _ := RetryLoop(ctx, "RemoveSubdocPaths", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "DeleteXattrs with keys %q xattr %v", UD(k).Redact(), UD(strings.Join(subdocPaths, ",")).Redact())
	}

	return err
}

// Delete a document and it's associated named xattr.  Couchbase server will preserve system xattrs as part of the (CBS)
// tombstone when a document is deleted.  To remove the system xattr as well, an explicit subdoc delete operation is required.
// This is currently called only for Purge operations.
//
// The doc existing doc is expected to be in one of the following states:
//   - DocExists and XattrExists
//   - DocExists but NoXattr
//   - XattrExists but NoDoc
//   - NoDoc and NoXattr
//
// In all cases, the end state will be NoDoc and NoXattr.
// Expected errors:
//   - Temporary server overloaded errors, in which case the caller should retry
//   - If the doc is in the the NoDoc and NoXattr state, it will return a KeyNotFound error
func DeleteWithXattrs(ctx context.Context, store *Collection, k string, xattrKeys []string) error {
	// Delegate to internal method that can take a testing-related callback
	return deleteWithXattrInternal(ctx, store, k, xattrKeys, nil)
}

// A function that will be called back after the first delete attempt but before second delete attempt
// to simulate the doc having changed state (artificially injected race condition)
type deleteWithXattrRaceInjection func(k string, xattrKeys []string)

func deleteWithXattrInternal(ctx context.Context, store *Collection, k string, xattrKeys []string, callback deleteWithXattrRaceInjection) error {

	DebugfCtx(ctx, KeyCRUD, "DeleteWithXattr called with key: %v xattrKey: %v", UD(k), UD(xattrKeys))

	// Try to delete body and xattrs in single op
	// NOTE: ongoing discussion w/ KV Engine team on whether this should handle cases where the body
	// doesn't exist (eg, a tombstoned xattr doc) by just ignoring the "delete body" mutation, rather
	// than current behavior of returning gocb.ErrKeyNotFound
	mutateErr := store.deleteBodyAndXattrs(ctx, k, xattrKeys)
	if IsDocNotFoundError(mutateErr) {
		// Invoke the testing related callback.  This is a no-op in non-test contexts.
		if callback != nil {
			callback(k, xattrKeys)
		}
		// KeyNotFound indicates there is no doc body.  Try to delete only the xattr.
		return deleteDocXattrOnly(ctx, store, k, xattrKeys, callback)
	} else if IsXattrNotFoundError(mutateErr) {
		// Invoke the testing related callback.  This is a no-op in non-test contexts.
		if callback != nil {
			callback(k, xattrKeys)
		}
		// ErrXattrNotFound indicates there is no XATTR.  Try to delete only the body.
		return store.Delete(k)
	} else {
		// return error
		return mutateErr
	}

}

func deleteDocXattrOnly(ctx context.Context, store *Collection, k string, xattrKeys []string, callback deleteWithXattrRaceInjection) error {

	//  Do get w/ xattr in order to get cas
	rawBody, _, getCas, err := store.GetWithXattrs(ctx, k, xattrKeys)
	if err != nil {
		return err
	}

	// If the doc body is non-empty at this point, then give up because it seems that a doc update has been
	// interleaved with the purge.  Return error to the caller and cancel the purge.
	if rawBody != nil {
		return fmt.Errorf("DeleteWithXattr was unable to delete the doc. Another update " +
			"was received which resurrected the doc by adding a new revision, in which case this delete operation is " +
			"considered as cancelled.")
	}

	// Cas-safe delete of just the XATTR.  Use SubdocDocFlagAccessDeleted since presumably the document body
	// has been deleted.
	deleteXattrErr := store.subdocDeleteXattrs(k, xattrKeys, getCas)
	if deleteXattrErr != nil {
		// If the cas-safe delete of XATTR fails, return an error to the caller.
		// This might happen if there was a concurrent update interleaved with the purge (someone resurrected doc)
		return pkgerrors.Wrapf(deleteXattrErr, "DeleteWithXattr was unable to delete the doc.  Another update "+
			"was received which resurrected the doc by adding a new revision, in which case this delete operation is "+
			"considered as cancelled. ")
	}
	return nil

}

func xattrCasPath(xattrKey string) string {
	return xattrKey + "." + xattrMacroCas
}

func xattrCrc32cPath(xattrKey string) string {
	return xattrKey + "." + xattrMacroValueCrc32c
}
