package base

import (
	"context"
	"errors"

	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
	"gopkg.in/couchbase/gocb.v1"
	"gopkg.in/couchbase/gocbcore.v7"
)

var _ SubdocXattrStore = &CouchbaseBucketGoCB{}

func (bucket *CouchbaseBucketGoCB) WriteCasWithXattr(k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v interface{}, xv interface{}) (casOut uint64, err error) {
	return WriteCasWithXattr(bucket, k, xattrKey, exp, cas, opts, v, xv)
}

func (bucket *CouchbaseBucketGoCB) WriteWithXattr(k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v []byte, xv []byte, isDelete bool, deleteBody bool) (casOut uint64, err error) { // If this is a tombstone, we want to delete the document and update the xattr
	return WriteWithXattr(bucket, k, xattrKey, exp, cas, opts, v, xv, isDelete, deleteBody)
}

func (bucket *CouchbaseBucketGoCB) DeleteWithXattr(k string, xattrKey string) error {
	return DeleteWithXattr(bucket, k, xattrKey)
}

func (bucket *CouchbaseBucketGoCB) GetXattr(k string, xattrKey string, xv interface{}) (casOut uint64, err error) {
	return bucket.SubdocGetXattr(k, xattrKey, xv)
}

func (bucket *CouchbaseBucketGoCB) GetSubDocRaw(k string, subdocKey string) ([]byte, uint64, error) {
	return bucket.SubdocGetRaw(k, subdocKey)
}

func (bucket *CouchbaseBucketGoCB) WriteSubDoc(k string, subdocKey string, cas uint64, value []byte) (uint64, error) {
	return bucket.SubdocWrite(k, subdocKey, cas, value)
}

func (bucket *CouchbaseBucketGoCB) GetWithXattr(k string, xattrKey string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error) {
	return bucket.SubdocGetBodyAndXattr(k, xattrKey, userXattrKey, rv, xv, uxv)
}

func (bucket *CouchbaseBucketGoCB) WriteUpdateWithXattr(k string, xattrKey string, userXattrKey string, exp uint32, opts *sgbucket.MutateInOptions, previous *sgbucket.BucketDocument, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	return WriteUpdateWithXattr(bucket, k, xattrKey, userXattrKey, exp, opts, previous, callback)
}

func (bucket *CouchbaseBucketGoCB) SetXattr(k string, xattrKey string, xv []byte) (casOut uint64, err error) {
	return SetXattr(bucket, k, xattrKey, xv)
}

func (bucket *CouchbaseBucketGoCB) RemoveXattr(k string, xattrKey string, cas uint64) (err error) {
	return RemoveXattr(bucket, k, xattrKey, cas)
}

func (bucket *CouchbaseBucketGoCB) DeleteXattrs(k string, xattrKeys ...string) (err error) {
	return DeleteXattrs(bucket, k, xattrKeys...)
}

func (bucket *CouchbaseBucketGoCB) UpdateXattr(k string, xattrKey string, exp uint32, cas uint64, xv interface{}, deleteBody bool, isDelete bool) (casOut uint64, err error) {
	return UpdateTombstoneXattr(bucket, k, xattrKey, exp, cas, xv, deleteBody)
}

// SubdocGetXattr retrieves the named xattr
func (bucket *CouchbaseBucketGoCB) SubdocGetXattr(k string, xattrKey string, xv interface{}) (casOut uint64, err error) {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		res, lookupErr := bucket.Bucket.LookupInEx(k, gocb.SubdocDocFlagAccessDeleted).
			GetEx(xattrKey, gocb.SubdocFlagXattr).Execute()
		switch lookupErr {
		case nil:
			xattrContErr := res.Content(xattrKey, xv)
			if xattrContErr != nil {
				DebugfCtx(context.TODO(), KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContErr)
			}
			cas := uint64(res.Cas())
			return false, err, cas
		case gocbcore.ErrSubDocBadMulti:
			xattrErr := res.Content(xattrKey, xv)
			DebugfCtx(context.TODO(), KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrErr)
			cas := uint64(res.Cas())
			return false, ErrXattrNotFound, cas
		case gocbcore.ErrKeyNotFound:
			DebugfCtx(context.TODO(), KeyCRUD, "No document found for key=%s", UD(k))
			return false, ErrNotFound, 0
		case gocbcore.ErrSubDocMultiPathFailureDeleted, gocb.ErrSubDocSuccessDeleted:
			xattrContentErr := res.Content(xattrKey, xv)
			if xattrContentErr != nil {
				return false, ErrXattrNotFound, uint64(0)
			}
			cas := uint64(res.Cas())
			return false, nil, cas
		default:
			shouldRetry = bucket.isRecoverableReadError(lookupErr)
			return shouldRetry, lookupErr, uint64(0)
		}

	}

	err, result := RetryLoop("SubdocGetXattr", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocGetXattr %s", UD(k).Redact())
	}

	if result == nil {
		return 0, err
	}

	cas, ok := result.(uint64)
	if !ok {
		return 0, RedactErrorf("SubdocGetXattr: Error doing type assertion of %v (%T) into uint64, Key %v", UD(result), result, UD(k))
	}

	return cas, err
}

func (bucket *CouchbaseBucketGoCB) SubdocWrite(k string, subdocKey string, cas uint64, value []byte) (uint64, error) {
	worker := func() (shouldRetry bool, err error, casOut uint64) {
		mutateInBuilder := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagMkDoc, gocb.Cas(cas), 0).
			UpsertEx(subdocKey, value, gocb.SubdocFlagNone)
		docFragment, err := mutateInBuilder.Execute()
		if err == nil {
			return false, nil, uint64(docFragment.Cas())
		}

		shouldRetry = bucket.isRecoverableWriteError(err)
		if shouldRetry {
			return shouldRetry, err, 0
		}

		return false, err, 0
	}

	err, casOut := RetryLoopCas("SubdocWrite", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocWrite with key %s and subdocKey %s", UD(k).Redact(), UD(subdocKey).Redact())
	}

	return casOut, err
}

func (bucket *CouchbaseBucketGoCB) SubdocGetRaw(k string, subdocKey string) ([]byte, uint64, error) {

	var rawValue []byte
	worker := func() (shouldRetry bool, err error, casOut uint64) {
		res, lookupErr := bucket.Bucket.LookupInEx(k, gocb.SubdocDocFlagNone).
			GetEx(subdocKey, gocb.SubdocFlagNone).
			Execute()

		if lookupErr != nil {
			isRecoverable := bucket.isRecoverableReadError(lookupErr)
			if isRecoverable {
				return isRecoverable, lookupErr, 0
			}
			return false, lookupErr, 0
		}

		err = res.Content(subdocKey, &rawValue)
		if err != nil {
			return false, err, 0
		}

		return false, nil, uint64(res.Cas())
	}

	err, casOut := RetryLoopCas("SubdocGetRaw", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocGetRaw with key %s and subdocKey %s", UD(k).Redact(), UD(subdocKey).Redact())
	}

	return rawValue, casOut, err
}

// Retrieve a document and it's associated named xattr
func (bucket *CouchbaseBucketGoCB) SubdocGetBodyAndXattr(k string, xattrKey string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error) {

	worker := func() (shouldRetry bool, err error, value uint64) {

		// First, attempt to get the document and xattr in one shot. We can't set SubdocDocFlagAccessDeleted when attempting
		// to retrieve the full doc body, so need to retry that scenario below.
		res, lookupErr := bucket.Bucket.LookupInEx(k, gocb.SubdocDocFlagAccessDeleted).
			GetEx(xattrKey, gocb.SubdocFlagXattr). // Get the xattr
			GetEx("", gocb.SubdocFlagNone).        // Get the document body
			Execute()

		// There are two 'partial success' error codes:
		//   ErrSubDocBadMulti - one of the subdoc operations failed.  Occurs when doc exists but xattr does not
		//   ErrSubDocMultiPathFailureDeleted - one of the subdoc operations failed, and the doc is deleted.  Occurs when xattr exists but doc is deleted (tombstone)
		switch lookupErr {
		case nil, gocbcore.ErrSubDocBadMulti:
			// Attempt to retrieve the document body, if present
			docContentErr := res.Content("", rv)
			if docContentErr != nil {
				DebugfCtx(context.TODO(), KeyCRUD, "No document body found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), docContentErr)
			}
			// Attempt to retrieve the xattr, if present
			xattrContentErr := res.Content(xattrKey, xv)
			if xattrContentErr != nil {
				DebugfCtx(context.TODO(), KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
			}
			cas = uint64(res.Cas())

		case gocbcore.ErrSubDocMultiPathFailureDeleted:
			//   ErrSubDocMultiPathFailureDeleted - one of the subdoc operations failed, and the doc is deleted.  Occurs when xattr may exist but doc is deleted (tombstone)
			xattrContentErr := res.Content(xattrKey, xv)
			cas = uint64(res.Cas())
			if xattrContentErr != nil {
				// No doc, no xattr can be treated as NotFound from Sync Gateway's perspective, even if it is a server tombstone
				DebugfCtx(context.TODO(), KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
				return false, ErrNotFound, cas
			}
			return false, nil, cas
		case gocb.ErrKeyNotFound:
			return false, ErrNotFound, cas
		default:
			shouldRetry = bucket.isRecoverableReadError(lookupErr)
			return shouldRetry, lookupErr, uint64(0)
		}

		// TODO: We may be able to improve in the future by having this secondary op as part of the first. At present
		// there is no support to obtain more than one xattr in a single operation however MB-28041 is filed for this.
		if userXattrKey != "" {
			userXattrCas, err := bucket.SubdocGetXattr(k, userXattrKey, uxv)
			switch pkgerrors.Cause(err) {

			case gocb.ErrKeyNotFound:
				// If key not found it has been deleted in between the first op and this op.
				return false, ErrNotFound, userXattrCas
			case gocb.ErrSubDocBadMulti:
				// Xattr doesn't exist, can skip
			case ErrXattrNotFound:
				// Xattr doesn't exist, can skip
			case nil:
				if cas != userXattrCas {
					return true, errors.New("cas mismatch between user xattr and document body"), uint64(0)
				}
			default:
				// Unknown error occurred
				// Shouldn't retry as any recoverable error will have been retried already in SubdocGetXattr
				return false, err, uint64(0)
			}
		}

		return false, nil, cas

	}

	// Kick off retry loop
	err, cas = RetryLoopCas("SubdocGetBodyAndXattr", worker, bucket.Spec.RetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocGetBodyAndXattr %v", UD(k).Redact())
	}

	return cas, err

}

// SubdocDeleteXattr removes the specified xattr.  Used to remove xattr from Couchbase Server
// tombstones.
func (bucket *CouchbaseBucketGoCB) SubdocDeleteXattr(k string, xattrKey string, cas uint64) error {
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()
	// Cas-safe delete of just the XATTR.  Use SubdocDocFlagAccessDeleted since presumably the document body
	// has been deleted.
	_, mutateErrDeleteXattr := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagAccessDeleted, gocb.Cas(cas), uint32(0)).
		RemoveEx(xattrKey, gocb.SubdocFlagXattr). // Remove the xattr
		Execute()

	// If no error, or it was just a ErrSubDocSuccessDeleted error, we're done.
	// ErrSubDocSuccessDeleted is a "success error" that means "operation was on a tombstoned document"
	if mutateErrDeleteXattr == nil || mutateErrDeleteXattr == gocbcore.ErrSubDocSuccessDeleted {
		return nil
	}
	return mutateErrDeleteXattr
}

// SubdocDeleteXattrs will delete the supplied xattr keys from a document. Not a cas safe operation.
func (bucket *CouchbaseBucketGoCB) SubdocDeleteXattrs(k string, xattrKeys ...string) error {
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	builder := bucket.Bucket.MutateIn(k, 0, 0)

	for _, xattrKey := range xattrKeys {
		builder.RemoveEx(xattrKey, gocb.SubdocFlagXattr)
	}

	_, mutateErr := builder.Execute()

	return mutateErr
}

// SubdocDeleteBodyAndXattr removes the body and specified xattr for a document.  Used
// when purging a document
func (bucket *CouchbaseBucketGoCB) SubdocDeleteBodyAndXattr(k string, xattrKey string) error {
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	_, mutateErr := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagNone, gocb.Cas(0), uint32(0)).
		RemoveEx(xattrKey, gocb.SubdocFlagXattr). // Remove the xattr
		RemoveEx("", gocb.SubdocFlagNone).        // Delete the document body
		Execute()

	if mutateErr == nil || mutateErr == gocbcore.ErrSubDocSuccessDeleted {
		DebugfCtx(context.TODO(), KeyCRUD, "No error or ErrSubDocSuccessDeleted.  We're done.")
		return nil
	}

	if pkgerrors.Cause(mutateErr) == gocb.ErrKeyNotFound {
		return ErrNotFound
	}

	// If mutate fails due to missing xattr, return ErrXattrNotFound
	subdocMutateErr, ok := pkgerrors.Cause(mutateErr).(gocbcore.SubDocMutateError)
	if ok && subdocMutateErr.Err == gocb.ErrSubDocPathNotFound {
		return ErrXattrNotFound
	}

	return mutateErr
}

// SubdocRemoveBody removes the document body, and updates the cas and crc32c on the specified xattr
func (bucket *CouchbaseBucketGoCB) SubdocDeleteBody(k string, xattrKey string, exp uint32, cas uint64) (casOut uint64, err error) {
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	docFragment, mutateErr := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagNone, gocb.Cas(cas), exp).
		UpsertEx(xattrCasPath(xattrKey), "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros). // Stamp the cas on the xattr
		UpsertEx(xattrCrc32cPath(xattrKey), DeleteCrc32c, gocb.SubdocFlagXattr).                            // Stamp crc32c on the xattr
		RemoveEx("", gocb.SubdocFlagNone).                                                                  // Delete the document body
		Execute()

	if mutateErr == nil || mutateErr == gocbcore.ErrSubDocSuccessDeleted {
		DebugfCtx(context.TODO(), KeyCRUD, "No error or ErrSubDocSuccessDeleted.  We're done.")
		return uint64(docFragment.Cas()), nil
	}

	return uint64(0), mutateErr
}

// SubdocUpdateXattrRemoveBody upserts the xattr and removes the document body.  Used when tombstoning a
// document.
func (bucket *CouchbaseBucketGoCB) SubdocUpdateXattrDeleteBody(k, xattrKey string, exp uint32, cas uint64, xv interface{}) (casOut uint64, err error) {
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	docFragment, mutateErr := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagNone, gocb.Cas(cas), exp).
		UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                       // Update the xattr
		UpsertEx(xattrCasPath(xattrKey), "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros). // Stamp the cas on the xattr
		UpsertEx(xattrCrc32cPath(xattrKey), DeleteCrc32c, gocb.SubdocFlagXattr).                            // Stamp crc32c on the xattr
		RemoveEx("", gocb.SubdocFlagNone).                                                                  // Remove the body
		Execute()

	if mutateErr == nil || mutateErr == gocbcore.ErrSubDocSuccessDeleted {
		DebugfCtx(context.TODO(), KeyCRUD, "No error or ErrSubDocSuccessDeleted.  We're done.")
		return uint64(docFragment.Cas()), nil
	}

	return uint64(0), mutateErr
}

// Inserts a new server tombstone with xattr.  If tombstone creation is supported by server, body of created document
// will be nil.  If unsupported, document body will be {}
func (bucket *CouchbaseBucketGoCB) SubdocInsertXattr(k string, xattrKey string, exp uint32, cas uint64, xv interface{}) (casOut uint64, err error) {
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	// Check whether server supports creation of tombstone in a single operation
	supportsTombstoneCreation := bucket.IsSupported(sgbucket.DataStoreFeatureCreateDeletedWithXattr)

	var mutateFlag gocb.SubdocDocFlag
	if supportsTombstoneCreation {
		mutateFlag = SubdocDocFlagCreateAsDeleted | gocb.SubdocDocFlagAccessDeleted | gocb.SubdocDocFlagReplaceDoc
	} else {
		mutateFlag = gocb.SubdocDocFlagMkDoc
	}
	builder := bucket.Bucket.MutateInEx(k, mutateFlag, gocb.Cas(cas), exp).
		UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                       // Update the xattr
		UpsertEx(xattrCasPath(xattrKey), "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros). // Stamp the cas on the xattr
		UpsertEx(xattrCrc32cPath(xattrKey), DeleteCrc32c, gocb.SubdocFlagXattr)                             // Stamp the body hash on the xattr

	docFragment, err := builder.Execute()
	if err != nil {
		return uint64(0), err
	}
	return uint64(docFragment.Cas()), nil
}

// SubdocInsertBodyAndXattr creates a document with xattr.  Fails if document already exists
func (bucket *CouchbaseBucketGoCB) SubdocInsertBodyAndXattr(k string, xattrKey string, exp uint32, v interface{}, xv interface{}) (casOut uint64, err error) {

	mutateInBuilder := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagReplaceDoc, 0, exp).
		UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                      // Update the xattr
		UpsertEx(xattrCasPath(xattrKey), "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the cas on the xattr
	if bucket.IsSupported(sgbucket.DataStoreFeatureCrc32cMacroExpansion) {
		mutateInBuilder.UpsertEx(xattrCrc32cPath(xattrKey), "${Mutation.value_crc32c}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the body hash on the xattr
	}
	mutateInBuilder.UpsertEx("", v, gocb.SubdocFlagNone) // Update the document body
	docFragment, err := mutateInBuilder.Execute()
	if err != nil {
		return uint64(0), err
	}
	return uint64(docFragment.Cas()), nil
}

// SubdocUpdateithXattr updates the document body and specified xattr.
func (bucket *CouchbaseBucketGoCB) SubdocUpdateBodyAndXattr(k string, xattrKey string, exp uint32, cas uint64, _ *sgbucket.MutateInOptions, v interface{}, xv interface{}) (casOut uint64, err error) {

	// Have value and xattr value - update both
	mutateInBuilder := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagMkDoc, gocb.Cas(cas), exp).
		UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                      // Update the xattr
		UpsertEx(xattrCasPath(xattrKey), "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the cas on the xattr
	if bucket.IsSupported(sgbucket.DataStoreFeatureCrc32cMacroExpansion) {
		mutateInBuilder.UpsertEx(xattrCrc32cPath(xattrKey), "${Mutation.value_crc32c}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the body hash on the xattr
	}
	mutateInBuilder.UpsertEx("", v, gocb.SubdocFlagNone) // Update the document body
	docFragment, err := mutateInBuilder.Execute()
	if err != nil {
		return 0, err
	}
	return uint64(docFragment.Cas()), nil
}

// SubdocSetXattr performs a set of the given xattr. Does a straight set with no cas.
func (bucket *CouchbaseBucketGoCB) SubdocSetXattr(k string, xattrKey string, xv interface{}) (casOut uint64, err error) {
	mutateInBuilder := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagAccessDeleted, 0, 0).
		UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr)
	docFragment, mutateErr := mutateInBuilder.Execute()
	if mutateErr == nil || mutateErr == gocbcore.ErrSubDocSuccessDeleted {
		return uint64(docFragment.Cas()), nil
	}

	return uint64(0), mutateErr
}

// SubdocUpdateithXattrOnly upserts an xattr, does not modify body
func (bucket *CouchbaseBucketGoCB) SubdocUpdateXattr(k string, xattrKey string, exp uint32, cas uint64, xv interface{}) (casOut uint64, err error) {

	// Have value and xattr value - update both
	mutateInBuilder := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagAccessDeleted, gocb.Cas(cas), exp).
		UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                      // Update the xattr
		UpsertEx(xattrCasPath(xattrKey), "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the cas on the xattr
	if bucket.IsSupported(sgbucket.DataStoreFeatureCrc32cMacroExpansion) {
		mutateInBuilder.UpsertEx(xattrCrc32cPath(xattrKey), "${Mutation.value_crc32c}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros) // Stamp the body hash on the xattr
	}
	docFragment, mutateErr := mutateInBuilder.Execute()
	if mutateErr == nil || mutateErr == gocbcore.ErrSubDocSuccessDeleted {
		return uint64(docFragment.Cas()), nil
	}

	return uint64(0), mutateErr
}

func (bucket *CouchbaseBucketGoCB) GetSpec() BucketSpec {
	return bucket.Spec
}

func (bucket *CouchbaseBucketGoCB) WriteUserXattr(docKey string, xattrKey string, xattrVal interface{}) (uint64, error) {
	docFrag, err := bucket.Bucket.MutateIn(docKey, 0, 0).UpsertEx(xattrKey, xattrVal, gocb.SubdocFlagXattr|gocb.SubdocFlagCreatePath).Execute()
	if err != nil {
		return 0, err
	}

	return uint64(docFrag.Cas()), nil
}

func (bucket *CouchbaseBucketGoCB) DeleteUserXattr(docKey string, xattrKey string) (uint64, error) {
	docFrag, err := bucket.Bucket.MutateIn(docKey, 0, 0).RemoveEx(xattrKey, gocb.SubdocFlagXattr).Execute()
	if err != nil {
		return 0, err
	}

	return uint64(docFrag.Cas()), nil
}
