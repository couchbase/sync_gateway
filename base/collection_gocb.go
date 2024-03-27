/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

// DefaultCollectionID represents _default._default collection
const DefaultCollectionID = uint32(0)

const (
	// SystemScope is the place for system collections to exist in
	SystemScope = "_system"
	// SystemCollectionMobile is the place to store Sync Gateway metadata on a system-collections-enabled Couchbase Server (7.6+)
	SystemCollectionMobile = "_mobile"

	// MetadataCollectionID is the KV collection ID for the SG Metadata store.
	// Subject to change when we move to system collections. Might not be possible to declare as const (need to retrieve from server?)
	MetadataCollectionID = DefaultCollectionID
)

type Collection struct {
	Bucket         *GocbV2Bucket
	Collection     *gocb.Collection
	kvCollectionID uint32 // cached copy of KV's collection ID for this collection
}

// Ensure that Collection implements sgbucket.DataStore/N1QLStore
var (
	_ DataStore          = &Collection{}
	_ N1QLStore          = &Collection{}
	_ sgbucket.ViewStore = &Collection{}
)

func AsCollection(dataStore DataStore) (*Collection, error) {
	collection, ok := dataStore.(*Collection)
	if !ok {
		return nil, fmt.Errorf("dataStore is not a *Collection (got %T)", dataStore)
	}
	return collection, nil
}

// NewCollection creates a new collection object, caching some kv ops.
func NewCollection(bucket *GocbV2Bucket, collection *gocb.Collection) (*Collection, error) {
	c := &Collection{
		Bucket:     bucket,
		Collection: collection}
	err := c.setCollectionID()
	if err != nil {
		return nil, err
	}
	return c, nil
}

// CollectionName returns the collection name
func (c *Collection) CollectionName() string {
	return c.Collection.Name()
}

// ScopeName returns the scope name
func (c *Collection) ScopeName() string {
	return c.Collection.ScopeName()
}

// GetName returns a qualified name
func (c *Collection) GetName() string {
	if c.IsDefaultScopeCollection() {
		return c.Collection.Bucket().Name()
	}
	// An unescaped variant of c.EscapedKeyspace()
	return FullyQualifiedCollectionName(c.BucketName(), c.ScopeName(), c.CollectionName())
}

// KV store

func (c *Collection) Get(k string, rv interface{}) (cas uint64, err error) {

	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	getOptions := &gocb.GetOptions{
		Transcoder: NewSGJSONTranscoder(),
	}
	getResult, err := c.Collection.Get(k, getOptions)
	if err != nil {
		return 0, err
	}
	err = getResult.Content(rv)
	return uint64(getResult.Cas()), err
}

func (c *Collection) GetRaw(k string) (rv []byte, cas uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	getOptions := &gocb.GetOptions{
		Transcoder: NewSGRawTranscoder(),
	}
	getRawResult, getErr := c.Collection.Get(k, getOptions)
	if getErr != nil {
		return nil, 0, getErr
	}

	err = getRawResult.Content(&rv)
	return rv, uint64(getRawResult.Cas()), err
}

func (c *Collection) GetAndTouchRaw(k string, exp uint32) (rv []byte, cas uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	getAndTouchOptions := &gocb.GetAndTouchOptions{
		Transcoder: NewSGRawTranscoder(),
	}
	getAndTouchRawResult, getErr := c.Collection.GetAndTouch(k, CbsExpiryToDuration(exp), getAndTouchOptions)
	if getErr != nil {
		return nil, 0, getErr
	}

	err = getAndTouchRawResult.Content(&rv)
	return rv, uint64(getAndTouchRawResult.Cas()), err
}

func (c *Collection) Touch(k string, exp uint32) (cas uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	result, err := c.Collection.Touch(k, CbsExpiryToDuration(exp), nil)
	if err != nil {
		return 0, err
	}
	return uint64(result.Cas()), nil
}

func (c *Collection) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	opts := &gocb.InsertOptions{
		Expiry:     CbsExpiryToDuration(exp),
		Transcoder: NewSGJSONTranscoder(),
	}
	_, gocbErr := c.Collection.Insert(k, v, opts)
	if gocbErr != nil {
		// Check key exists handling
		if errors.Is(gocbErr, gocb.ErrDocumentExists) {
			return false, nil
		}
		err = pkgerrors.WithStack(gocbErr)
	}
	return err == nil, err
}

func (c *Collection) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	opts := &gocb.InsertOptions{
		Expiry:     CbsExpiryToDuration(exp),
		Transcoder: NewSGRawTranscoder(),
	}
	_, gocbErr := c.Collection.Insert(k, v, opts)
	if gocbErr != nil {
		// Check key exists handling
		if errors.Is(gocbErr, gocb.ErrDocumentExists) {
			return false, nil
		}
		err = pkgerrors.WithStack(gocbErr)
	}
	return err == nil, err
}

func (c *Collection) Set(k string, exp uint32, opts *sgbucket.UpsertOptions, v interface{}) error {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	goCBUpsertOptions := &gocb.UpsertOptions{
		Expiry:     CbsExpiryToDuration(exp),
		Transcoder: NewSGJSONTranscoder(),
	}
	fillUpsertOptions(context.TODO(), goCBUpsertOptions, opts)

	if _, ok := v.([]byte); ok {
		goCBUpsertOptions.Transcoder = gocb.NewRawJSONTranscoder()
	}

	_, err := c.Collection.Upsert(k, v, goCBUpsertOptions)
	return err
}

func (c *Collection) SetRaw(k string, exp uint32, opts *sgbucket.UpsertOptions, v []byte) error {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	goCBUpsertOptions := &gocb.UpsertOptions{
		Expiry:     CbsExpiryToDuration(exp),
		Transcoder: NewSGRawTranscoder(),
	}
	fillUpsertOptions(context.TODO(), goCBUpsertOptions, opts)

	_, err := c.Collection.Upsert(k, v, goCBUpsertOptions)
	return err
}

func (c *Collection) WriteCas(k string, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	var result *gocb.MutationResult
	if cas == 0 {
		insertOpts := &gocb.InsertOptions{
			Expiry:     CbsExpiryToDuration(exp),
			Transcoder: NewSGJSONTranscoder(),
		}
		if opt == sgbucket.Raw {
			insertOpts.Transcoder = gocb.NewRawBinaryTranscoder()
		}
		result, err = c.Collection.Insert(k, v, insertOpts)
	} else {
		replaceOpts := &gocb.ReplaceOptions{
			Cas:        gocb.Cas(cas),
			Expiry:     CbsExpiryToDuration(exp),
			Transcoder: NewSGJSONTranscoder(),
		}
		if opt == sgbucket.Raw {
			replaceOpts.Transcoder = gocb.NewRawBinaryTranscoder()
		}
		result, err = c.Collection.Replace(k, v, replaceOpts)
	}
	if err != nil {
		return 0, err
	}
	return uint64(result.Cas()), nil
}

func (c *Collection) Delete(k string) error {
	_, err := c.Remove(k, 0)
	return err
}

func (c *Collection) Remove(k string, cas uint64) (casOut uint64, err error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	result, errRemove := c.Collection.Remove(k, &gocb.RemoveOptions{Cas: gocb.Cas(cas)})
	if errRemove == nil && result != nil {
		casOut = uint64(result.Cas())
	}
	return casOut, errRemove
}

func (c *Collection) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	for {
		var value []byte
		var err error
		var callbackExpiry *uint32

		// Load the existing value.
		getOptions := &gocb.GetOptions{
			Transcoder: gocb.NewRawJSONTranscoder(),
		}

		var cas uint64

		c.Bucket.waitForAvailKvOp()
		getResult, err := c.Collection.Get(k, getOptions)
		c.Bucket.releaseKvOp()

		if err != nil {
			if !errors.Is(err, gocb.ErrDocumentNotFound) {
				// Unexpected error, abort
				return cas, err
			}
			cas = 0 // Key not found error
		} else {
			cas = uint64(getResult.Cas())
			err = getResult.Content(&value)
			if err != nil {
				return 0, err
			}
		}

		// Invoke callback to get updated value
		var isDelete bool
		value, callbackExpiry, isDelete, err = callback(value)
		if err != nil {
			return cas, err
		}

		if callbackExpiry != nil {
			exp = *callbackExpiry
		}

		var casGoCB gocb.Cas
		var result *gocb.MutationResult
		casRetry := false

		c.Bucket.waitForAvailKvOp()
		if cas == 0 {
			// If the Get fails, the cas will be 0 and so call Insert().
			// If we get an error on the insert, due to a race, this will
			// go back through the cas loop
			insertOpts := &gocb.InsertOptions{
				Transcoder: gocb.NewRawJSONTranscoder(),
				Expiry:     CbsExpiryToDuration(exp),
			}
			result, err = c.Collection.Insert(k, value, insertOpts)
			if err == nil {
				casGoCB = result.Cas()
			} else if errors.Is(err, gocb.ErrDocumentExists) {
				casRetry = true
			}
		} else {
			if value == nil && isDelete {
				removeOptions := &gocb.RemoveOptions{
					Cas: gocb.Cas(cas),
				}
				result, err = c.Collection.Remove(k, removeOptions)
				if err == nil {
					casGoCB = result.Cas()
				} else if errors.Is(err, gocb.ErrCasMismatch) {
					casRetry = true
				}
			} else {
				// Otherwise, attempt to do a replace.  won't succeed if
				// updated underneath us
				replaceOptions := &gocb.ReplaceOptions{
					Transcoder: gocb.NewRawJSONTranscoder(),
					Cas:        gocb.Cas(cas),
					Expiry:     CbsExpiryToDuration(exp),
				}
				result, err = c.Collection.Replace(k, value, replaceOptions)
				if err == nil {
					casGoCB = result.Cas()
				} else if errors.Is(err, gocb.ErrCasMismatch) {
					casRetry = true
				}
			}
		}
		c.Bucket.releaseKvOp()

		if casRetry {
			// retry on cas failure
		} else {
			// err will be nil if successful
			return uint64(casGoCB), err
		}
	}
}

func (c *Collection) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()
	incrOptions := gocb.IncrementOptions{
		Initial: int64(def),
		Delta:   amt,
		Expiry:  CbsExpiryToDuration(exp),
	}
	incrResult, err := c.Collection.Binary().Increment(k, &incrOptions)
	if err != nil {
		return 0, err
	}

	return incrResult.Content(), nil
}

// CouchbaseBucketStore

// Recoverable errors or timeouts trigger retry for gocb v2 read operations
func (c *Collection) isRecoverableReadError(err error) bool {

	if err == nil {
		return false
	}

	if errors.Is(err, gocb.ErrTemporaryFailure) || errors.Is(err, gocb.ErrOverload) || errors.Is(err, gocb.ErrTimeout) {
		return true
	}
	return false
}

// Recoverable errors trigger retry for gocb v2 write operations
func (c *Collection) isRecoverableWriteError(err error) bool {

	if err == nil {
		return false
	}

	// TODO: CBG-1142 Handle SyncWriteInProgress errors --> Currently gocbv2 retries this internally and returns a
	//  timeout with KV_SYNC_WRITE_IN_PROGRESS as its reason. Decision on whether to handle inside gocb or retry here
	if errors.Is(err, gocb.ErrTemporaryFailure) || errors.Is(err, gocb.ErrOverload) {
		return true
	}
	return false
}

// GetExpiry requires a full document retrieval in order to obtain the expiry, which is reasonable for
// current use cases (on-demand import).  If there's a need for expiry as part of normal get, this shouldn't be
// used - an enhanced version of Get() should be implemented to avoid two ops
func (c *Collection) GetExpiry(ctx context.Context, k string) (expiry uint32, getMetaError error) {
	agent, err := c.Bucket.getGoCBAgent()
	if err != nil {
		WarnfCtx(ctx, "Unable to obtain gocbcore.Agent while retrieving expiry:%v", err)
		return 0, err
	}

	getMetaOptions := gocbcore.GetMetaOptions{
		Key:      []byte(k),
		Deadline: c.Bucket.getBucketOpDeadline(),
	}
	if c.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		getMetaOptions.CollectionID = c.GetCollectionID()
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	getMetaCallback := func(result *gocbcore.GetMetaResult, err error) {
		defer wg.Done()
		if err != nil {
			getMetaError = err
			return
		}
		expiry = result.Expiry
	}

	_, err = agent.GetMeta(getMetaOptions, getMetaCallback)
	if err != nil {
		wg.Done()
		return 0, err
	}
	wg.Wait()

	return expiry, getMetaError
}

func (c *Collection) Exists(k string) (exists bool, err error) {
	res, err := c.Collection.Exists(k, nil)
	if err != nil {
		return false, err
	}
	return res.Exists(), nil
}

func (c *Collection) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
	return c.Bucket.IsError(err, errorType)
}

// SGJsonTranscoder reads and writes JSON, with relaxed datatype restrictions on decode, and
// embedded support for writing raw JSON on encode
type SGJSONTranscoder struct {
}

func NewSGJSONTranscoder() *SGJSONTranscoder {
	return &SGJSONTranscoder{}
}

// SGJSONTranscoder supports reading BinaryType documents as JSON, for backward
// compatibility with legacy Sync Gateway data
func (t *SGJSONTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return errors.New("unexpected value compression")
	}
	// Type-based decoding
	if valueType == gocbcore.BinaryType {
		switch typedOut := out.(type) {
		case *[]byte:
			*typedOut = bytes
			return nil
		case *interface{}:
			*typedOut = bytes
			return nil
		case *string:
			*typedOut = string(bytes)
			return nil
		default:
			return errors.New("you must encode raw JSON data in a byte array or string")
		}
	} else if valueType == gocbcore.StringType {
		return gocb.NewRawStringTranscoder().Decode(bytes, flags, out)
	} else if valueType == gocbcore.JSONType {
		switch out.(type) {
		case []byte, *[]byte:
			return gocb.NewRawJSONTranscoder().Decode(bytes, flags, out)
		default:
			return gocb.NewJSONTranscoder().Decode(bytes, flags, out)
		}
	}

	return errors.New("unexpected expectedFlags value")
}

// SGJSONTranscoder.Encode supports writing JSON as either raw bytes or an unmarshalled interface
func (t *SGJSONTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	switch value.(type) {
	case []byte, *[]byte:
		return gocb.NewRawJSONTranscoder().Encode(value)
	default:
		return gocb.NewJSONTranscoder().Encode(value)
	}
}

// SGBinaryTranscoder uses the appropriate raw transcoder for the data type.  Provides backward compatibility
// for pre-3.0 documents intended to be binary but written with JSON datatype, and vice versa
type SGRawTranscoder struct {
}

// NewRawBinaryTranscoder returns a new RawBinaryTranscoder.
func NewSGRawTranscoder() *SGRawTranscoder {
	return &SGRawTranscoder{}
}

// Decode applies raw binary transcoding behaviour to decode into a Go type.
func (t *SGRawTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {
	valueType, compression := gocbcore.DecodeCommonFlags(flags)

	// Make sure compression is disabled
	if compression != gocbcore.NoCompression {
		return errors.New("unexpected value compression")
	}
	// Normal types of decoding
	switch valueType {
	case gocbcore.BinaryType:
		return gocb.NewRawBinaryTranscoder().Decode(bytes, flags, out)
	case gocbcore.StringType:
		return gocb.NewRawStringTranscoder().Decode(bytes, flags, out)
	case gocbcore.JSONType:
		return gocb.NewRawJSONTranscoder().Decode(bytes, flags, out)
	default:
		return errors.New("unexpected expectedFlags value")
	}
}

// Encode applies raw binary transcoding behaviour to encode a Go type.
func (t *SGRawTranscoder) Encode(value interface{}) ([]byte, uint32, error) {
	return gocb.NewRawBinaryTranscoder().Encode(value)

}

// GetCollectionID sets the kv CollectionID for the current collection. In the case of Couchbase Server not supporting collection, returns DefaultCollectionID.
func (c *Collection) GetCollectionID() uint32 {
	return c.kvCollectionID
}

// setCollectionID sets private property of kv CollectionID.
func (c *Collection) setCollectionID() error {
	if !c.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		c.kvCollectionID = DefaultCollectionID
		return nil
	}
	// default collection has a known ID
	if c.IsDefaultScopeCollection() {
		c.kvCollectionID = DefaultCollectionID
		return nil
	}
	agent, err := c.Bucket.getGoCBAgent()
	if err != nil {
		return err
	}
	scope := c.ScopeName()
	collection := c.CollectionName()
	wg := sync.WaitGroup{}
	wg.Add(1)
	var callbackErr error
	callbackFunc := func(res *gocbcore.GetCollectionIDResult, getCollectionErr error) {
		defer wg.Done()
		if getCollectionErr != nil {
			callbackErr = getCollectionErr
			return
		}
		if res == nil {
			callbackErr = fmt.Errorf("getCollectionID not retrieved for %s.%s", scope, collection)
			return
		}

		c.kvCollectionID = res.CollectionID
	}
	_, err = agent.GetCollectionID(scope,
		collection,
		gocbcore.GetCollectionIDOptions{
			Deadline: c.Bucket.getBucketOpDeadline(),
		},
		callbackFunc)

	if err != nil {
		wg.Done()
		return fmt.Errorf("GetCollectionID for %s.%s, err: %w", scope, collection, err)
	}
	wg.Wait()
	if callbackErr != nil {
		return fmt.Errorf("GetCollectionID for %s.%s, err: %w", scope, collection, callbackErr)
	}
	return nil
}
