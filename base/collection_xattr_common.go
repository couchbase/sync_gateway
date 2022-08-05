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

// SubdocXattrStore interface defines the set of operations Sync Gateway uses to manage and interact with xattrs
type SubdocXattrStore interface {
	SubdocGetXattr(k string, xattrKey string, xv interface{}) (casOut uint64, err error)
	SubdocGetBodyAndXattr(k string, xattrKey string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error)
	SubdocInsertXattr(k string, xattrKey string, exp uint32, cas uint64, xv interface{}) (casOut uint64, err error)
	SubdocInsertBodyAndXattr(k string, xattrKey string, exp uint32, v interface{}, xv interface{}) (casOut uint64, err error)
	SubdocSetXattr(k string, xattrKey string, xv interface{}) (casOut uint64, err error)
	SubdocUpdateXattr(k string, xattrKey string, exp uint32, cas uint64, xv interface{}) (casOut uint64, err error)
	SubdocUpdateBodyAndXattr(k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v interface{}, xv interface{}) (casOut uint64, err error)
	SubdocUpdateXattrDeleteBody(k, xattrKey string, exp uint32, cas uint64, xv interface{}) (casOut uint64, err error)
	SubdocDeleteXattr(k string, xattrKey string, cas uint64) error
	SubdocDeleteXattrs(k string, xattrKeys ...string) error
	SubdocDeleteBodyAndXattr(k string, xattrKey string) error
	SubdocDeleteBody(k string, xattrKey string, exp uint32, cas uint64) (casOut uint64, err error)
	GetSpec() BucketSpec
	IsSupported(feature sgbucket.DataStoreFeature) bool
	isRecoverableReadError(err error) bool
	isRecoverableWriteError(err error) bool
}

// Utilities for creating/deleting user xattr.  For test use
type UserXattrStore interface {
	WriteUserXattr(docKey string, xattrKey string, xattrVal interface{}) (uint64, error)
	DeleteUserXattr(docKey string, xattrKey string) (uint64, error)
}

// KvXattrStore is used for xattr_common functions that perform subdoc and standard kv operations
type KvXattrStore interface {
	sgbucket.KVStore
	SubdocXattrStore
}

// CAS-safe write of a document and it's associated named xattr
func WriteCasWithXattr(store SubdocXattrStore, k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v interface{}, xv interface{}) (casOut uint64, err error) {

	worker := func() (shouldRetry bool, err error, value uint64) {

		// cas=0 specifies an insert
		if cas == 0 {
			casOut, err = store.SubdocInsertBodyAndXattr(k, xattrKey, exp, v, xv)
			if err != nil {
				shouldRetry = store.isRecoverableWriteError(err)
				return shouldRetry, err, uint64(0)
			}
			return false, nil, casOut
		}

		// Otherwise, replace existing value
		if v != nil {
			// Have value and xattr value - update both
			casOut, err = store.SubdocUpdateBodyAndXattr(k, xattrKey, exp, cas, opts, v, xv)
			if err != nil {
				shouldRetry = store.isRecoverableWriteError(err)
				return shouldRetry, err, uint64(0)
			}
		} else {
			// Update xattr only
			casOut, err = store.SubdocUpdateXattr(k, xattrKey, exp, cas, xv)
			if err != nil {
				shouldRetry = store.isRecoverableWriteError(err)
				return shouldRetry, err, uint64(0)
			}
		}
		return false, nil, casOut
	}

	// Kick off retry loop
	err, cas = RetryLoopCas("WriteCasWithXattr", worker, store.GetSpec().RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "WriteCasWithXattr with key %v", UD(k).Redact())
	}

	return cas, err
}

// Single attempt to update a document and xattr.  Setting isDelete=true and value=nil will delete the document body.  Both
// update types (UpdateTombstoneXattr, WriteCasWithXattr) include recoverable error retry.
func WriteWithXattr(store SubdocXattrStore, k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, value []byte, xattrValue []byte, isDelete bool, deleteBody bool) (casOut uint64, err error) { // If this is a tombstone, we want to delete the document and update the xattr
	if isDelete {
		return UpdateTombstoneXattr(store, k, xattrKey, exp, cas, xattrValue, deleteBody)
	} else {
		// Not a delete - update the body and xattr
		return WriteCasWithXattr(store, k, xattrKey, exp, cas, opts, value, xattrValue)
	}
}

// CAS-safe update of a document's xattr (only).  Deletes the document body if deleteBody is true.
func UpdateTombstoneXattr(store SubdocXattrStore, k string, xattrKey string, exp uint32, cas uint64, xv interface{}, deleteBody bool) (casOut uint64, err error) {

	// WriteCasWithXattr always stamps the xattr with the new cas using macro expansion, into a top-level property called 'cas'.
	// This is the only use case for macro expansion today - if more cases turn up, should change the sg-bucket API to handle this more generically.
	requiresBodyRemoval := false
	worker := func() (shouldRetry bool, err error, value uint64) {

		var casOut uint64
		var tombstoneErr error

		// If deleteBody == true, remove the body and update xattr
		if deleteBody {
			casOut, tombstoneErr = store.SubdocUpdateXattrDeleteBody(k, xattrKey, exp, cas, xv)
		} else {
			if cas == 0 {
				// if cas == 0, create a new server tombstone with xattr
				casOut, tombstoneErr = store.SubdocInsertXattr(k, xattrKey, exp, cas, xv)
				// If one-step tombstone creation is not supported, set flag for document body removal
				requiresBodyRemoval = !store.IsSupported(sgbucket.DataStoreFeatureCreateDeletedWithXattr)
			} else {
				// If cas is non-zero, this is an already existing tombstone.  Update xattr only
				casOut, tombstoneErr = store.SubdocUpdateXattr(k, xattrKey, exp, cas, xv)
			}
		}

		if tombstoneErr != nil {
			shouldRetry = store.isRecoverableWriteError(tombstoneErr)
			return shouldRetry, tombstoneErr, uint64(0)
		}
		return false, nil, casOut
	}

	// Kick off retry loop
	err, cas = RetryLoopCas("UpdateTombstoneXattr", worker, store.GetSpec().RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "Error during UpdateTombstoneXattr with key %v", UD(k).Redact())
		return cas, err
	}

	// In the case where the SubdocDocFlagCreateAsDeleted is not available and we are performing the creation of a
	// tombstoned document we need to perform this second operation. This is due to the fact that SubdocDocFlagMkDoc
	// will have been used above instead which will create an empty body {} which we then need to delete here. If there
	// is a CAS mismatch we exit the operation as this means there has been a subsequent update to the body.
	if requiresBodyRemoval {
		worker := func() (shouldRetry bool, err error, value uint64) {

			casOut, removeErr := store.SubdocDeleteBody(k, xattrKey, exp, cas)
			if removeErr != nil {
				// If there is a cas mismatch the body has since been updated and so we don't need to bother removing
				// body in this operation
				if IsCasMismatch(removeErr) {
					return false, nil, cas
				}

				shouldRetry = store.isRecoverableWriteError(removeErr)
				return shouldRetry, removeErr, uint64(0)
			}
			return false, nil, casOut
		}

		err, cas = RetryLoopCas("UpdateXattrDeleteBodySecondOp", worker, store.GetSpec().RetrySleeper())
		if err != nil {
			err = pkgerrors.Wrapf(err, "Error during UpdateTombstoneXattr delete op with key %v", UD(k).Redact())
			return cas, err
		}

	}

	return cas, err
}

// WriteUpdateWithXattr retrieves the existing doc from the bucket, invokes the callback to update the document, then writes the new document to the bucket.  Will repeat this process on cas
// failure.  If previousValue/xattr/cas are provided, will use those on the first iteration instead of retrieving from the bucket.
func WriteUpdateWithXattr(store SubdocXattrStore, k string, xattrKey string, userXattrKey string, exp uint32, opts *sgbucket.MutateInOptions, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {

	var value []byte
	var xattrValue []byte
	var userXattrValue []byte
	var cas uint64
	emptyCas := uint64(0)

	// If an existing value has been provided, use that as the initial value
	if previous != nil && previous.Cas > 0 {
		value = previous.Body
		xattrValue = previous.Xattr
		cas = previous.Cas
		userXattrValue = previous.UserXattr
	}

	for {
		var err error
		// If no existing value has been provided, retrieve the current value from the bucket
		if cas == 0 {
			// Load the existing value.
			cas, err = store.SubdocGetBodyAndXattr(k, xattrKey, userXattrKey, &value, &xattrValue, &userXattrValue)

			if err != nil {
				if pkgerrors.Cause(err) != ErrNotFound {
					// Unexpected error, cancel writeupdate
					DebugfCtx(context.TODO(), KeyCRUD, "Retrieval of existing doc failed during WriteUpdateWithXattr for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), err)
					return emptyCas, err
				}
				// Key not found - initialize values
				value = nil
				xattrValue = nil
			}
		}

		// Invoke callback to get updated value
		updatedValue, updatedXattrValue, isDelete, callbackExpiry, err := callback(value, xattrValue, userXattrValue, cas)

		// If it's an ErrCasFailureShouldRetry, then retry by going back through the for loop
		if err == ErrCasFailureShouldRetry {
			cas = 0 // force the call to SubdocGetBodyAndXattr() to refresh
			continue
		}

		// On any other errors, abort the Write attempt
		if err != nil {
			return emptyCas, err
		}
		if callbackExpiry != nil {
			exp = *callbackExpiry
		}

		// Attempt to write the updated document to the bucket.  Mark body for deletion if previous body was non-empty
		deleteBody := value != nil
		casOut, writeErr := WriteWithXattr(store, k, xattrKey, exp, cas, opts, updatedValue, updatedXattrValue, isDelete, deleteBody)

		if writeErr == nil {
			return casOut, nil
		}

		if IsCasMismatch(writeErr) {
			// Retry on cas failure.  ErrNotStored is returned in some concurrent insert races that appear to be related
			// to the timing of concurrent xattr subdoc operations.  Treating as CAS failure as these will get the usual
			// conflict/duplicate handling on retry.
		} else {
			// WriteWithXattr already handles retry on recoverable errors, so fail on any errors other than ErrKeyExists
			WarnfCtx(context.TODO(), "Failed to update doc with xattr for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), writeErr)
			return emptyCas, writeErr
		}

		// Reset value, xattr, cas for cas retry
		value = nil
		xattrValue = nil
		cas = 0

	}
}

// SetXattr performs a subdoc set on the supplied xattrKey. Implements a retry for recoverable failures.
func SetXattr(store SubdocXattrStore, k string, xattrKey string, xv []byte) (casOut uint64, err error) {

	worker := func() (shouldRetry bool, err error, value uint64) {
		casOut, writeErr := store.SubdocSetXattr(k, xattrKey, xv)
		if writeErr == nil {
			return false, nil, casOut
		}

		shouldRetry = store.isRecoverableWriteError(writeErr)
		if shouldRetry {
			return shouldRetry, err, 0
		}

		return false, writeErr, 0
	}

	err, casOut = RetryLoopCas("SetXattr", worker, store.GetSpec().RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SetXattr with key %v", UD(k).Redact())
	}

	return casOut, err

}

// RemoveXattr performs a cas safe subdoc delete of the provided key. Will retry if a recoverable failure occurs.
func RemoveXattr(store SubdocXattrStore, k string, xattrKey string, cas uint64) error {
	worker := func() (shouldRetry bool, err error, value interface{}) {
		writeErr := store.SubdocDeleteXattr(k, xattrKey, cas)
		if writeErr == nil {
			return false, nil, nil
		}

		shouldRetry = store.isRecoverableWriteError(writeErr)
		if shouldRetry {
			return shouldRetry, err, nil
		}

		return false, err, nil
	}

	err, _ := RetryLoop("RemoveXattr", worker, store.GetSpec().RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "RemoveXattr with key %v xattr %v", UD(k).Redact(), UD(xattrKey).Redact())
	}

	return err
}

// DeleteXattrs performs a subdoc delete of the provided keys. Retries any recoverable failures. Not cas safe does a
// straight delete.
func DeleteXattrs(store SubdocXattrStore, k string, xattrKeys ...string) error {
	worker := func() (shouldRetry bool, err error, value interface{}) {
		writeErr := store.SubdocDeleteXattrs(k, xattrKeys...)
		if writeErr == nil {
			return false, nil, nil
		}

		shouldRetry = store.isRecoverableWriteError(writeErr)
		if shouldRetry {
			return shouldRetry, err, nil
		}

		return false, err, nil
	}

	err, _ := RetryLoop("DeleteXattrs", worker, store.GetSpec().RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "DeleteXattrs with keys %q xattr %v", UD(k).Redact(), UD(strings.Join(xattrKeys, ",")).Redact())
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
// In all cases, the end state will be NoDoc and NoXattr.
// Expected errors:
//    - Temporary server overloaded errors, in which case the caller should retry
//    - If the doc is in the the NoDoc and NoXattr state, it will return a KeyNotFound error
func DeleteWithXattr(store KvXattrStore, k string, xattrKey string) error {
	// Delegate to internal method that can take a testing-related callback
	return deleteWithXattrInternal(store, k, xattrKey, nil)
}

// A function that will be called back after the first delete attempt but before second delete attempt
// to simulate the doc having changed state (artifiically injected race condition)
type deleteWithXattrRaceInjection func(k string, xattrKey string)

func deleteWithXattrInternal(store KvXattrStore, k string, xattrKey string, callback deleteWithXattrRaceInjection) error {

	DebugfCtx(context.TODO(), KeyCRUD, "DeleteWithXattr called with key: %v xattrKey: %v", UD(k), UD(xattrKey))

	// Try to delete body and xattrs in single op
	// NOTE: ongoing discussion w/ KV Engine team on whether this should handle cases where the body
	// doesn't exist (eg, a tombstoned xattr doc) by just ignoring the "delete body" mutation, rather
	// than current behavior of returning gocb.ErrKeyNotFound
	mutateErr := store.SubdocDeleteBodyAndXattr(k, xattrKey)
	switch {
	case mutateErr == ErrNotFound:
		// Invoke the testing related callback.  This is a no-op in non-test contexts.
		if callback != nil {
			callback(k, xattrKey)
		}
		// KeyNotFound indicates there is no doc body.  Try to delete only the xattr.
		return deleteDocXattrOnly(store, k, xattrKey, callback)
	case mutateErr == ErrXattrNotFound:
		// Invoke the testing related callback.  This is a no-op in non-test contexts.
		if callback != nil {
			callback(k, xattrKey)
		}
		// ErrXattrNotFound indicates there is no XATTR.  Try to delete only the body.
		return store.Delete(k)

	default:
		// return error
		return mutateErr
	}

}

func deleteDocXattrOnly(store SubdocXattrStore, k string, xattrKey string, callback deleteWithXattrRaceInjection) error {

	//  Do get w/ xattr in order to get cas
	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	getCas, err := store.SubdocGetBodyAndXattr(k, xattrKey, "", &retrievedVal, &retrievedXattr, nil)
	if err != nil {
		return err
	}

	// If the doc body is non-empty at this point, then give up because it seems that a doc update has been
	// interleaved with the purge.  Return error to the caller and cancel the purge.
	if len(retrievedVal) != 0 {
		return fmt.Errorf("DeleteWithXattr was unable to delete the doc. Another update " +
			"was received which resurrected the doc by adding a new revision, in which case this delete operation is " +
			"considered as cancelled.")
	}

	// Cas-safe delete of just the XATTR.  Use SubdocDocFlagAccessDeleted since presumably the document body
	// has been deleted.
	deleteXattrErr := store.SubdocDeleteXattr(k, xattrKey, getCas)
	if deleteXattrErr != nil {
		// If the cas-safe delete of XATTR fails, return an error to the caller.
		// This might happen if there was a concurrent update interleaved with the purge (someone resurrected doc)
		return pkgerrors.Wrapf(deleteXattrErr, "DeleteWithXattr was unable to delete the doc.  Another update "+
			"was received which resurrected the doc by adding a new revision, in which case this delete operation is "+
			"considered as cancelled. ")
	}
	return nil

}

// AsSubdocXattrStore tries to return the given bucket as a SubdocXattrStore, based on underlying buckets.
func AsSubdocXattrStore(bucket Bucket) (SubdocXattrStore, bool) {

	var underlyingBucket Bucket
	switch typedBucket := bucket.(type) {
	case *CouchbaseBucketGoCB:
		return typedBucket, true
	case *Collection:
		return typedBucket, true
	case *LoggingBucket:
		underlyingBucket = typedBucket.GetUnderlyingBucket()
	case *LeakyBucket:
		underlyingBucket = typedBucket.GetUnderlyingBucket()
	case *TestBucket:
		underlyingBucket = typedBucket.Bucket
	default:
		// bail out for unrecognised/unsupported buckets
		return nil, false
	}

	return AsSubdocXattrStore(underlyingBucket)
}

func AsUserXattrStore(bucket Bucket) (UserXattrStore, bool) {

	var underlyingBucket Bucket
	switch typedBucket := bucket.(type) {
	case *CouchbaseBucketGoCB:
		return typedBucket, true
	case *Collection:
		return typedBucket, true
	case *LoggingBucket:
		underlyingBucket = typedBucket.GetUnderlyingBucket()
	case *LeakyBucket:
		underlyingBucket = typedBucket.GetUnderlyingBucket()
	case *TestBucket:
		underlyingBucket = typedBucket.Bucket
	default:
		// bail out for unrecognised/unsupported buckets
		return nil, false
	}

	return AsUserXattrStore(underlyingBucket)
}

func xattrCasPath(xattrKey string) string {
	return xattrKey + "." + xattrMacroCas
}

func xattrCrc32cPath(xattrKey string) string {
	return xattrKey + "." + xattrMacroValueCrc32c
}
