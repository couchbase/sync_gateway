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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/gocbcore/v10/memd"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

var GetSpecXattr = &gocb.GetSpecOptions{IsXattr: true}
var InsertSpecXattr = &gocb.InsertSpecOptions{IsXattr: true}
var UpsertSpecXattr = &gocb.UpsertSpecOptions{IsXattr: true}
var RemoveSpecXattr = &gocb.RemoveSpecOptions{IsXattr: true}
var LookupOptsAccessDeleted *gocb.LookupInOptions

// IsSupported is a shim that queries the parent bucket's feature
func (c *Collection) IsSupported(feature sgbucket.BucketStoreFeature) bool {
	return c.Bucket.IsSupported(feature)
}

var _ sgbucket.XattrStore = &Collection{}

func init() {
	LookupOptsAccessDeleted = &gocb.LookupInOptions{}
	LookupOptsAccessDeleted.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted
}

func (c *Collection) GetSpec() BucketSpec {
	return c.Bucket.Spec
}

// InsertTombstoneWithXattrs inserts a new server tombstone with the specified system xattrs
func (c *Collection) InsertTombstoneWithXattrs(ctx context.Context, k string, exp uint32, xattrValue map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {

	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	supportsTombstoneCreation := c.IsSupported(sgbucket.BucketStoreFeatureCreateDeletedWithXattr)

	var docFlags gocb.SubdocDocFlag
	if supportsTombstoneCreation {
		docFlags = gocb.SubdocDocFlagCreateAsDeleted | gocb.SubdocDocFlagAccessDeleted | gocb.SubdocDocFlagAddDoc
	} else {
		docFlags = gocb.SubdocDocFlagMkDoc
	}

	mutateOps := make([]gocb.MutateInSpec, 0, len(xattrValue))
	for xattrKey, value := range xattrValue {
		mutateOps = append(mutateOps, gocb.UpsertSpec(xattrKey, bytesToRawMessage(value), UpsertSpecXattr))
	}

	mutateOps = appendMacroExpansions(mutateOps, opts)
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace, // set replace here, as we're explicitly setting SubdocDocFlagMkDoc above if tombstone creation is not supported
		Expiry:        CbsExpiryToDuration(exp),
		Cas:           gocb.Cas(0),
	}
	options.Internal.DocFlags = docFlags
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

func (c *Collection) DeleteWithXattrs(ctx context.Context, k string, xattrKeys []string) error {
	return DeleteWithXattrs(ctx, c, k, xattrKeys)
}

func (c *Collection) GetXattrs(ctx context.Context, k string, xattrKeys []string) (xattrs map[string][]byte, casOut uint64, err error) {
	_, xattrs, casOut, err = c.subdocGetBodyAndXattrs(ctx, k, xattrKeys, false)
	return xattrs, casOut, err
}

func (c *Collection) GetXattr(ctx context.Context, k string, xattrKey string, xv interface{}) (casOut uint64, err error) {
	return c.SubdocGetXattr(ctx, k, xattrKey, xv)
}

func (c *Collection) GetSubDocRaw(ctx context.Context, k string, subdocKey string) ([]byte, uint64, error) {
	return c.SubdocGetRaw(ctx, k, subdocKey)
}

func (c *Collection) WriteSubDoc(ctx context.Context, k string, subdocKey string, cas uint64, value []byte) (uint64, error) {
	return c.SubdocWrite(ctx, k, subdocKey, cas, value)
}

func (c *Collection) GetWithXattrs(ctx context.Context, k string, xattrKeys []string) ([]byte, map[string][]byte, uint64, error) {
	return c.subdocGetBodyAndXattrs(ctx, k, xattrKeys, true)
}

func (c *Collection) GetWithXattr(ctx context.Context, k string, xattrKey string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error) {
	return c.SubdocGetBodyAndXattr(ctx, k, xattrKey, userXattrKey, rv, xv, uxv)
}

/*
func (c *Collection) WriteUpdateWithXattr(ctx context.Context, k string, xattrKey string, userXattrKey string, exp uint32, previous *sgbucket.BucketDocument, opts *sgbucket.MutateInOptions, callback sgbucket.WriteUpdateWithXattrFunc) (casOut uint64, err error) {
	return WriteUpdateWithXattr(ctx, c, k, xattrKey, userXattrKey, exp, previous, opts, callback)
}
*/

func (c *Collection) SetXattrs(ctx context.Context, k string, xattrs map[string][]byte) (casOut uint64, err error) {
	return c.SubdocSetXattrs(k, xattrs)
}

func (c *Collection) SetXattr(ctx context.Context, k string, xattrKey string, xv []byte) (casOut uint64, err error) {
	xvs := map[string][]byte{
		xattrKey: xv,
	}
	return SetXattrs(ctx, c, k, xvs)
}

func (c *Collection) RemoveXattrs(ctx context.Context, k string, xattrKeys []string, cas uint64) (err error) {
	return RemoveXattrs(ctx, c, k, xattrKeys, cas)
}

/*
func (c *Collection) RemoveXattr(ctx context.Context, k string, xattrKey string, cas uint64) (err error) {
	return RemoveXattr(ctx, c, k, xattrKey, cas)
}
*/

func (c *Collection) DeleteSubDocPaths(ctx context.Context, k string, paths ...string) (err error) {
	return removeSubdocPaths(ctx, c, k, paths...)
}

func (c *Collection) DeleteXattrs(ctx context.Context, k string, xattrKeys ...string) (err error) {
	return removeSubdocPaths(ctx, c, k, xattrKeys...)
}

// SubdocGetXattr retrieves the named xattr
// Notes on error handling
//   - gocb v2 returns subdoc errors at the op level, in the ContentAt response
//   - 'successful' error codes, like SucDocSuccessDeleted, aren't returned, and instead just set the internal.Deleted property on the response
func (c *Collection) SubdocGetXattr(ctx context.Context, k string, xattrKey string, xv interface{}) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	ops := []gocb.LookupInSpec{
		gocb.GetSpec(xattrKey, GetSpecXattr),
	}
	res, lookupErr := c.Collection.LookupIn(k, ops, LookupOptsAccessDeleted)
	if lookupErr == nil {
		xattrContErr := res.ContentAt(0, xv)
		// On error here, treat as the xattr wasn't found
		if xattrContErr != nil {
			DebugfCtx(ctx, KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContErr)
			return 0, ErrXattrNotFound
		}
		cas := uint64(res.Cas())
		return cas, nil
	} else if errors.Is(lookupErr, gocbcore.ErrDocumentNotFound) {
		TracefCtx(ctx, KeyCRUD, "No document found for key=%s", UD(k))
		return 0, ErrNotFound
	} else {
		return 0, lookupErr
	}
}

// SubdocGetXattr retrieves the requested xattrs
// Notes on error handling
//   - gocb v2 returns subdoc errors at the op level, in the ContentAt response
//   - 'successful' error codes, like SucDocSuccessDeleted, aren't returned, and instead just set the internal.Deleted property on the response
func (c *Collection) SubdocGetXattrs(ctx context.Context, k string, xvs map[string]interface{}) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	ops := make([]gocb.LookupInSpec, len(xvs))
	xattrKeys := make([]string, len(xvs))
	i := 0
	for xattrKey, _ := range xvs {
		ops[i] = gocb.GetSpec(xattrKey, GetSpecXattr)
		xattrKeys[i] = xattrKey
		i++
	}
	res, lookupErr := c.Collection.LookupIn(k, ops, LookupOptsAccessDeleted)
	if lookupErr == nil {
		errorCount := 0
		for i, xattrKey := range xattrKeys {
			xattrContErr := res.ContentAt(uint(i), xvs[xattrKey])
			if xattrContErr != nil {
				DebugfCtx(ctx, KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContErr)
				errorCount++
			}
		}
		cas := uint64(res.Cas())
		if errorCount == 0 {
			return cas, nil
		} else if errorCount == len(xattrKeys) {
			return cas, ErrXattrNotFound
		} else {
			return cas, ErrXattrPartialFound
		}
	} else if errors.Is(lookupErr, gocbcore.ErrDocumentNotFound) {
		DebugfCtx(ctx, KeyCRUD, "No document found for key=%s", UD(k))
		return 0, ErrNotFound
	} else {
		return 0, lookupErr
	}
}

func (c *Collection) SubdocGetRaw(ctx context.Context, k string, subdocKey string) ([]byte, uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	var rawValue []byte

	worker := func() (shouldRetry bool, err error, casOut uint64) {
		ops := []gocb.LookupInSpec{
			gocb.GetSpec(subdocKey, &gocb.GetSpecOptions{}),
		}

		res, lookupErr := c.Collection.LookupIn(k, ops, &gocb.LookupInOptions{})
		if lookupErr != nil {
			isRecoverable := c.isRecoverableReadError(lookupErr)
			if isRecoverable {
				return isRecoverable, lookupErr, 0
			}

			if isKVError(lookupErr, memd.StatusKeyNotFound) {
				return false, ErrNotFound, 0
			}

			return false, lookupErr, 0
		}

		err = res.ContentAt(0, &rawValue)
		if err != nil {
			return false, err, 0
		}

		return false, nil, uint64(res.Cas())
	}

	err, casOut := RetryLoopCas(ctx, "SubdocGetRaw", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocGetRaw with key %s and subdocKey %s", UD(k).Redact(), UD(subdocKey).Redact())
	}

	return rawValue, casOut, err
}

func (c *Collection) SubdocWrite(ctx context.Context, k string, subdocKey string, cas uint64, value []byte) (uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	worker := func() (shouldRetry bool, err error, casOut uint64) {
		mutateOps := []gocb.MutateInSpec{
			gocb.UpsertSpec(subdocKey, bytesToRawMessage(value), &gocb.UpsertSpecOptions{CreatePath: true}),
		}

		result, err := c.Collection.MutateIn(k, mutateOps, &gocb.MutateInOptions{
			Cas:           gocb.Cas(cas),
			StoreSemantic: gocb.StoreSemanticsUpsert,
		})
		if err == nil {
			return false, nil, uint64(result.Cas())
		}

		shouldRetry = c.isRecoverableWriteError(err)
		if shouldRetry {
			return shouldRetry, err, 0
		}

		return false, err, 0
	}

	err, casOut := RetryLoopCas(ctx, "SubdocWrite", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocWrite with key %s and subdocKey %s", UD(k).Redact(), UD(subdocKey).Redact())
	}

	return casOut, err
}

// subdocGetBodyAndXattr retrieves the document body and xattrs in a single LookupIn subdoc operation.  Does not require both to exist.
func (c *Collection) subdocGetBodyAndXattrs(ctx context.Context, k string, xattrKeys []string, fetchBody bool) (rawBody []byte, xattrs map[string][]byte, cas uint64, err error) {
	xattrs = make(map[string][]byte, len(xattrKeys))
	worker := func() (shouldRetry bool, err error, value uint64) {

		c.Bucket.waitForAvailKvOp()
		defer c.Bucket.releaseKvOp()
		// First, attempt to get the document and xattr in one shot.
		ops := make([]gocb.LookupInSpec, 0, len(xattrKeys)+1)
		for _, xattrKey := range xattrKeys {
			if xattrKey == "" {
				return false, fmt.Errorf("empty xattr key called"), uint64(0)
			}
			ops = append(ops, gocb.GetSpec(xattrKey, GetSpecXattr))
		}
		if fetchBody {
			ops = append(ops, gocb.GetSpec("", &gocb.GetSpecOptions{}))
		}
		res, lookupErr := c.Collection.LookupIn(k, ops, LookupOptsAccessDeleted)
		// There are two 'partial success' error codes:
		//   ErrMemdSubDocBadMulti - one of the subdoc operations failed.  Occurs when doc exists but xattr does not
		//   ErrMemdSubDocMultiPathFailureDeleted - one of the subdoc operations failed, and the doc is deleted.  Occurs when xattr exists but doc is deleted (tombstone)
		switch lookupErr {
		case nil, gocbcore.ErrMemdSubDocBadMulti:
			// Attempt to retrieve the document body, if present
			var docContentErr error
			if fetchBody {
				docContentErr = res.ContentAt(uint(len(xattrKeys)), &rawBody)
			}
			cas = uint64(res.Cas())
			var xattrErrors []error
			for i, xattrKey := range xattrKeys {
				var xattr []byte
				xattrContentErr := res.ContentAt(uint(i), &xattr)
				if xattrContentErr != nil {
					xattrErrors = append(xattrErrors, xattrContentErr)
					DebugfCtx(ctx, KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
					continue
				}
				xattrs[xattrKey] = xattr
			}
			cas = uint64(res.Cas())

			// If doc and all xattrs are not found, treat as ErrNotFound
			if isKVError(docContentErr, memd.StatusSubDocMultiPathFailureDeleted) && len(xattrErrors) == len(xattrKeys) {
				return false, ErrNotFound, cas
			}

			// If doc not requested and no xattrs are found, treat as ErrXattrNotFound
			if !fetchBody && len(xattrErrors) == len(xattrKeys) {
				return false, ErrXattrNotFound, cas
			}

			if docContentErr != nil {
				DebugfCtx(ctx, KeyCRUD, "No document body found for key=%s, xattrKeys=%s: %v", UD(k), UD(xattrKeys), docContentErr)
			}

		case gocbcore.ErrMemdSubDocMultiPathFailureDeleted:
			//   ErrSubDocMultiPathFailureDeleted - one of the subdoc operations failed, and the doc is deleted.  Occurs when xattr may exist but doc is deleted (tombstone)
			cas = uint64(res.Cas())
			var xattrErrors []error
			for i, xattrKey := range xattrKeys {
				var xattr []byte
				xattrContentErr := res.ContentAt(uint(i), xattr)
				if xattrContentErr != nil {
					xattrErrors = append(xattrErrors, xattrContentErr)
					DebugfCtx(ctx, KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
					continue
				}
				xattrs[xattrKey] = xattr
			}

			if len(xattrErrors) == len(xattrs) {
				// No doc, no xattrs means the doc isn't found
				DebugfCtx(ctx, KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKeys), xattrErrors)
				return false, ErrNotFound, cas
			}

			if len(xattrErrors) > 0 {
				DebugfCtx(ctx, KeyCRUD, "Partial xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKeys), xattrErrors)
				return false, ErrXattrPartialFound, cas
			}

			return false, nil, cas
		default:
			fmt.Println("err=", lookupErr)
			// KeyNotFound is returned as KVError
			if isKVError(lookupErr, memd.StatusKeyNotFound) {
				return false, ErrNotFound, cas
			}
			shouldRetry = c.isRecoverableReadError(lookupErr)
			return shouldRetry, lookupErr, uint64(0)
		}

		return false, nil, cas
	}

	// Kick off retry loop
	err, cas = RetryLoopCas(ctx, "subdocGetBodyAndXattrs", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "subdocGetBodyAndXattrs %v", UD(k).Redact())
	}

	return rawBody, xattrs, cas, err
}

// SubdocGetBodyAndXattr retrieves the document body and xattr in a single LookupIn subdoc operation.  Does not require both to exist.
func (c *Collection) SubdocGetBodyAndXattr(ctx context.Context, k string, xattrKey string, userXattrKey string, rv interface{}, xv interface{}, uxv interface{}) (cas uint64, err error) {
	worker := func() (shouldRetry bool, err error, value uint64) {

		c.Bucket.waitForAvailKvOp()
		defer c.Bucket.releaseKvOp()

		// First, attempt to get the document and xattr in one shot.
		ops := []gocb.LookupInSpec{
			gocb.GetSpec(xattrKey, GetSpecXattr),
			gocb.GetSpec("", &gocb.GetSpecOptions{}),
		}
		res, lookupErr := c.Collection.LookupIn(k, ops, LookupOptsAccessDeleted)

		// There are two 'partial success' error codes:
		//   ErrMemdSubDocBadMulti - one of the subdoc operations failed.  Occurs when doc exists but xattr does not
		//   ErrMemdSubDocMultiPathFailureDeleted - one of the subdoc operations failed, and the doc is deleted.  Occurs when xattr exists but doc is deleted (tombstone)
		switch lookupErr {
		case nil, gocbcore.ErrMemdSubDocBadMulti:
			// Attempt to retrieve the document body, if present
			docContentErr := res.ContentAt(1, rv)
			xattrContentErr := res.ContentAt(0, xv)
			cas = uint64(res.Cas())

			if isKVError(docContentErr, memd.StatusSubDocMultiPathFailureDeleted) && isKVError(xattrContentErr, memd.StatusSubDocMultiPathFailureDeleted) {
				// No doc, no xattr can be treated as NotFound from Sync Gateway's perspective, even if it is a server tombstone, but should return cas
				DebugfCtx(ctx, KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
				return false, ErrNotFound, cas
			}

			if docContentErr != nil {
				DebugfCtx(ctx, KeyCRUD, "No document body found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), docContentErr)
			}
			// Attempt to retrieve the xattr, if present
			if xattrContentErr != nil {
				DebugfCtx(ctx, KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
			}

		case gocbcore.ErrMemdSubDocMultiPathFailureDeleted:
			//   ErrSubDocMultiPathFailureDeleted - one of the subdoc operations failed, and the doc is deleted.  Occurs when xattr may exist but doc is deleted (tombstone)
			xattrContentErr := res.ContentAt(0, xv)
			cas = uint64(res.Cas())
			if xattrContentErr != nil {
				// No doc, no xattr means the doc isn't found
				DebugfCtx(ctx, KeyCRUD, "No xattr content found for key=%s, xattrKey=%s: %v", UD(k), UD(xattrKey), xattrContentErr)
				return false, ErrNotFound, cas
			}
			return false, nil, cas
		default:
			// KeyNotFound is returned as KVError
			if isKVError(lookupErr, memd.StatusKeyNotFound) {
				return false, ErrNotFound, cas
			}
			shouldRetry = c.isRecoverableReadError(lookupErr)
			return shouldRetry, lookupErr, uint64(0)
		}

		// TODO: We may be able to improve in the future by having this secondary op as part of the first. At present
		// there is no support to obtain more than one xattr in a single operation however MB-28041 is filed for this.
		if userXattrKey != "" {
			userXattrCas, userXattrErr := c.SubdocGetXattr(ctx, k, userXattrKey, uxv)
			switch pkgerrors.Cause(userXattrErr) {
			case gocb.ErrDocumentNotFound:
				// If key not found it has been deleted in between the first op and this op.
				return false, err, userXattrCas
			case ErrXattrNotFound:
				// Xattr doesn't exist, can skip
			case nil:
				if cas != userXattrCas {
					return true, errors.New("cas mismatch between user xattr and document body"), uint64(0)
				}
			default:
				// Unknown error occurred
				// Shouldn't retry as any recoverable error will have been retried already in SubdocGetXattr
				return false, userXattrErr, uint64(0)
			}
		}
		return false, nil, cas
	}

	// Kick off retry loop
	err, cas = RetryLoopCas(ctx, "SubdocGetBodyAndXattr", worker, DefaultRetrySleeper())
	if err != nil {
		err = pkgerrors.Wrapf(err, "SubdocGetBodyAndXattr %v", UD(k).Redact())
	}

	return cas, err
}

// createTombstone inserts a new server tombstone with associated xattrs.  Writes cas and crc32c to the xattr using macro expansion.
func (c *Collection) createTombstone(_ context.Context, k string, exp uint32, cas uint64, xattrs map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	supportsTombstoneCreation := c.IsSupported(sgbucket.BucketStoreFeatureCreateDeletedWithXattr)

	var docFlags gocb.SubdocDocFlag
	if supportsTombstoneCreation {
		docFlags = gocb.SubdocDocFlagCreateAsDeleted | gocb.SubdocDocFlagAccessDeleted | gocb.SubdocDocFlagAddDoc
	} else {
		docFlags = gocb.SubdocDocFlagMkDoc
	}

	mutateOps := make([]gocb.MutateInSpec, 0, len(xattrs))
	for xattrKey, xattrVal := range xattrs {
		mutateOps = append(mutateOps, gocb.UpsertSpec(xattrKey, bytesToRawMessage(xattrVal), UpsertSpecXattr))
	}
	mutateOps = appendMacroExpansions(mutateOps, opts)
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace, // set replace here, as we're explicitly setting SubdocDocFlagMkDoc above if tombstone creation is not supported
		Expiry:        CbsExpiryToDuration(exp),
		Cas:           gocb.Cas(cas),
	}
	options.Internal.DocFlags = docFlags
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// insertBodyAndXattrs inserts a document and associated xattrs in a single mutateIn operation.  Writes cas and crc32c to the xattr using macro expansion.
func (c *Collection) insertBodyAndXattrs(_ context.Context, k string, exp uint32, v interface{}, xattrs map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := make([]gocb.MutateInSpec, 0, len(xattrs)+1)
	for xattrKey, xv := range xattrs {
		mutateOps = append(mutateOps, gocb.UpsertSpec(xattrKey, bytesToRawMessage(xv), UpsertSpecXattr))
	}
	mutateOps = append(mutateOps, gocb.ReplaceSpec("", bytesToRawMessage(v), nil))
	mutateOps = appendMacroExpansions(mutateOps, opts)
	options := &gocb.MutateInOptions{
		Expiry:        CbsExpiryToDuration(exp),
		StoreSemantic: gocb.StoreSemanticsInsert,
	}
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil

}

// InsertBodyAndXattr inserts a document and associated mobile xattr in a single mutateIn operation.  Writes cas and crc32c to the xattr using
// macro expansion.
func (c *Collection) InsertBodyAndXattr(_ context.Context, k string, xattrKey string, exp uint32, v interface{}, xv interface{}, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(xattrKey, bytesToRawMessage(xv), UpsertSpecXattr),
		gocb.ReplaceSpec("", bytesToRawMessage(v), nil),
	}
	mutateOps = appendMacroExpansions(mutateOps, opts)
	options := &gocb.MutateInOptions{
		Expiry:        CbsExpiryToDuration(exp),
		StoreSemantic: gocb.StoreSemanticsInsert,
	}
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil

}

// SubdocInsert performs a subdoc insert operation to the specified path in the document body.
func (c *Collection) SubdocInsert(_ context.Context, k string, fieldPath string, cas uint64, value interface{}) error {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.InsertSpec(fieldPath, value, nil),
	}
	options := &gocb.MutateInOptions{
		Cas: gocb.Cas(cas),
	}
	_, mutateErr := c.Collection.MutateIn(k, mutateOps, options)

	if errors.Is(mutateErr, gocbcore.ErrDocumentNotFound) {
		return ErrNotFound
	}

	if errors.Is(mutateErr, gocbcore.ErrPathExists) {
		return ErrAlreadyExists
	}

	if errors.Is(mutateErr, gocbcore.ErrPathNotFound) {
		return ErrPathNotFound
	}

	return mutateErr

}

// SubdocSetXattr performs a set of the given xattr. Does a straight set with no cas.
func (c *Collection) SubdocSetXattrs(k string, xvs map[string][]byte) (casOut uint64, err error) {

	mutateOps := make([]gocb.MutateInSpec, 0, len(xvs))
	for xattrKey, xv := range xvs {
		mutateOps = append(mutateOps, gocb.UpsertSpec(xattrKey, bytesToRawMessage(xv), UpsertSpecXattr))
	}
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsUpsert,
	}
	options.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}

	return uint64(result.Cas()), nil
}

func (c *Collection) UpdateXattrs(ctx context.Context, k string, exp uint32, cas uint64, xv map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	return c.updateXattrs(ctx, k, exp, cas, xv, opts)
}

// updateXattrs updates the xattrs on an existing document. Writes cas and crc32c to the xattr using macro expansion.
func (c *Collection) updateXattrs(_ context.Context, k string, exp uint32, cas uint64, xattrs map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := make([]gocb.MutateInSpec, 0, len(xattrs))
	for xattrKey, xattrVal := range xattrs {
		mutateOps = append(mutateOps, gocb.UpsertSpec(xattrKey, bytesToRawMessage(xattrVal), UpsertSpecXattr))
	}
	fmt.Printf("mutateOps: %+v\n", mutateOps)
	mutateOps = appendMacroExpansions(mutateOps, opts)

	options := &gocb.MutateInOptions{
		Expiry:        CbsExpiryToDuration(exp),
		StoreSemantic: gocb.StoreSemanticsUpsert,
		Cas:           gocb.Cas(cas),
	}
	options.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// UpdateXattr updates the xattr on an existing document. Writes cas and crc32c to the xattr using
// macro expansion.
func (c *Collection) UpdateXattr(_ context.Context, k string, xattrKey string, exp uint32, cas uint64, xv interface{}, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(xattrKey, bytesToRawMessage(xv), UpsertSpecXattr),
	}
	mutateOps = appendMacroExpansions(mutateOps, opts)

	options := &gocb.MutateInOptions{
		Expiry:        CbsExpiryToDuration(exp),
		StoreSemantic: gocb.StoreSemanticsUpsert,
		Cas:           gocb.Cas(cas),
	}
	options.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// updateBodyAndXattr updates the document body and xattrs of an existing document. Writes cas and crc32c to the xattr using macro expansion.
func (c *Collection) updateBodyAndXattrs(ctx context.Context, k string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v interface{}, xattrs map[string][]byte) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := make([]gocb.MutateInSpec, 0, len(xattrs)+1)
	for xattrKey, xattrVal := range xattrs {
		mutateOps = append(mutateOps, gocb.UpsertSpec(xattrKey, bytesToRawMessage(xattrVal), UpsertSpecXattr))
	}
	mutateOps = append(mutateOps, gocb.ReplaceSpec("", bytesToRawMessage(v), nil))
	mutateOps = appendMacroExpansions(mutateOps, opts)

	options := &gocb.MutateInOptions{
		Expiry:        CbsExpiryToDuration(exp),
		StoreSemantic: gocb.StoreSemanticsUpsert,
		Cas:           gocb.Cas(cas),
	}
	fillMutateInOptions(ctx, options, opts)
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// UpdateBodyAndXattr updates the document body and xattr of an existing document. Writes cas and crc32c to the xattr using
// macro expansion.
func (c *Collection) UpdateBodyAndXattr(ctx context.Context, k string, xattrKey string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions, v interface{}, xv interface{}) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(xattrKey, bytesToRawMessage(xv), UpsertSpecXattr),
		gocb.ReplaceSpec("", bytesToRawMessage(v), nil),
	}
	mutateOps = appendMacroExpansions(mutateOps, opts)

	options := &gocb.MutateInOptions{
		Expiry:        CbsExpiryToDuration(exp),
		StoreSemantic: gocb.StoreSemanticsUpsert,
		Cas:           gocb.Cas(cas),
	}
	fillMutateInOptions(ctx, options, opts)
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// updateXattrDeleteBody deletes the document body and updates the xattrs of an existing document. Writes cas and crc32c to the xattr using macro expansion.
func (c *Collection) updateXattrsDeleteBody(_ context.Context, k string, exp uint32, cas uint64, xattrs map[string][]byte, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := make([]gocb.MutateInSpec, 0, len(xattrs)+1)
	for xattrKey, xattrVal := range xattrs {
		mutateOps = append(mutateOps, gocb.UpsertSpec(xattrKey, bytesToRawMessage(xattrVal), UpsertSpecXattr))
	}
	mutateOps = append(mutateOps, gocb.RemoveSpec("", nil))
	mutateOps = appendMacroExpansions(mutateOps, opts)
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace,
		Expiry:        CbsExpiryToDuration(exp),
		Cas:           gocb.Cas(cas),
	}
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// UpdateXattrDeleteBody deletes the document body and updates the xattr of an existing document. Writes cas and crc32c to the xattr using
// macro expansion.
func (c *Collection) UpdateXattrDeleteBody(_ context.Context, k, xattrKey string, exp uint32, cas uint64, xv interface{}, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(xattrKey, bytesToRawMessage(xv), UpsertSpecXattr),
		gocb.RemoveSpec("", nil),
	}
	mutateOps = appendMacroExpansions(mutateOps, opts)
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace,
		Expiry:        CbsExpiryToDuration(exp),
		Cas:           gocb.Cas(cas),
	}
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// subdocDeleteXattrs deletes xattrs of an existing document (or document tombstone)
func (c *Collection) subdocDeleteXattrs(k string, xattrKeys []string, cas uint64) (err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := make([]gocb.MutateInSpec, 0, len(xattrKeys))
	for _, xattrKey := range xattrKeys {
		mutateOps = append(mutateOps, gocb.RemoveSpec(xattrKey, RemoveSpecXattr))
	}

	options := &gocb.MutateInOptions{
		Cas: gocb.Cas(cas),
	}
	options.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	_, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	return mutateErr
}

// subdocRemovePaths will delete the supplied xattr keys from a document. Not a cas safe operation.
func (c *Collection) subdocRemovePaths(k string, xattrKeys ...string) error {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := make([]gocb.MutateInSpec, 0, len(xattrKeys))
	for _, xattrKey := range xattrKeys {
		mutateOps = append(mutateOps, gocb.RemoveSpec(xattrKey, RemoveSpecXattr))
	}

	_, mutateErr := c.Collection.MutateIn(k, mutateOps, &gocb.MutateInOptions{})

	return mutateErr
}

// SubdocDeleteXattr deletes the document body and associated xattr of an existing document.
func (c *Collection) deleteBodyAndXattrs(_ context.Context, k string, xattrKeys []string) (err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := make([]gocb.MutateInSpec, 0, len(xattrKeys)+1)

	for _, xattrKey := range xattrKeys {
		mutateOps = append(mutateOps, gocb.RemoveSpec(xattrKey, RemoveSpecXattr))
	}
	mutateOps = append(mutateOps, gocb.RemoveSpec("", nil))
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace,
	}
	_, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr == nil {
		return nil
	}

	// StatusKeyNotFound returned if document doesn't exist
	if errors.Is(mutateErr, gocbcore.ErrDocumentNotFound) {
		return ErrNotFound
	}

	// StatusSubDocBadMulti returned if xattr doesn't exist
	if isKVError(mutateErr, memd.StatusSubDocBadMulti) {
		return ErrXattrNotFound
	}
	return mutateErr
}

// DeleteBody deletes the document body of an existing document, and updates cas and crc32c in the associated xattr.
func (c *Collection) DeleteBody(_ context.Context, k string, xattrKeys []string, exp uint32, cas uint64, opts *sgbucket.MutateInOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	// FIXME xattrkeys
	mutateOps := []gocb.MutateInSpec{
		gocb.RemoveSpec("", nil),
	}
	mutateOps = appendMacroExpansions(mutateOps, opts)
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsReplace,
		Expiry:        CbsExpiryToDuration(exp),
		Cas:           gocb.Cas(cas),
	}
	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// isKVError compares the status code of a gocb KeyValueError to the provided code.  Used for nested subdoc errors
// where gocb doesn't return a typed error for the underlying error.
func isKVError(err error, code memd.StatusCode) bool {

	switch typedErr := err.(type) {
	case gocb.KeyValueError:
		if typedErr.StatusCode == code {
			return true
		}
	case *gocb.KeyValueError:
		if typedErr.StatusCode == code {
			return true
		}
	case gocbcore.KeyValueError:
		if typedErr.StatusCode == code {
			return true
		}
	case *gocbcore.KeyValueError:
		if typedErr.StatusCode == code {
			return true
		}
	case gocbcore.SubDocumentError:
		return isKVError(typedErr.InnerError, code)
	case *gocbcore.SubDocumentError:
		return isKVError(typedErr.InnerError, code)
	}

	return false
}

// If v is []byte or *[]byte, converts to json.RawMessage to avoid duplicate marshalling by gocb.
func bytesToRawMessage(v interface{}) interface{} {
	switch val := v.(type) {
	case []byte:
		return json.RawMessage(val)
	case *[]byte:
		return json.RawMessage(*val)
	default:
		return v
	}
}

func (c *Collection) WriteUserXattr(k string, xattrKey string, xattrVal interface{}) (uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.UpsertSpec(xattrKey, bytesToRawMessage(xattrVal), UpsertSpecXattr),
	}
	options := &gocb.MutateInOptions{
		StoreSemantic: gocb.StoreSemanticsUpsert,
	}

	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

func (c *Collection) DeleteUserXattr(k string, xattrKey string) (uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	mutateOps := []gocb.MutateInSpec{
		gocb.RemoveSpec(xattrKey, RemoveSpecXattr),
	}
	options := &gocb.MutateInOptions{
		Cas: gocb.Cas(0),
	}
	options.Internal.DocFlags = gocb.SubdocDocFlagAccessDeleted

	result, mutateErr := c.Collection.MutateIn(k, mutateOps, options)
	if mutateErr != nil {
		return 0, mutateErr
	}
	return uint64(result.Cas()), nil
}

// appendMacroExpansions will append macro expansions defined in MutateInOptions to the provided
// gocb MutateInSpec.
func appendMacroExpansions(mutateInSpec []gocb.MutateInSpec, opts *sgbucket.MutateInOptions) []gocb.MutateInSpec {

	if opts == nil {
		return mutateInSpec
	}
	for _, v := range opts.MacroExpansion {
		mutateInSpec = append(mutateInSpec, gocb.UpsertSpec(v.Path, gocbMutationMacro(v.Type), UpsertSpecXattr))
	}
	return mutateInSpec
}

func gocbMutationMacro(meType sgbucket.MacroExpansionType) gocb.MutationMacro {
	switch meType {
	case sgbucket.MacroCas:
		return gocb.MutationMacroCAS
	case sgbucket.MacroCrc32c:
		return gocb.MutationMacroValueCRC32c
	default:
		return gocb.MutationMacroCAS
	}
}
