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
	"errors"
	"expvar"
	"time"

	"github.com/couchbase/gocb"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

// Connect to the default collection for the specified bucket
func GetCouchbaseCollection(spec BucketSpec) (*Collection, error) {
	connString, err := spec.GetGoCBConnString()
	if err != nil {
		Warnf("Unable to parse server value: %s error: %v", SD(spec.Server), err)
		return nil, err
	}

	securityConfig, err := GoCBv2SecurityConfig(&spec.TLSSkipVerify, spec.CACertPath)
	if err != nil {
		return nil, err
	}

	username, password, _ := spec.Auth.GetCredentials()
	authenticatorConfig, isX509, err := GoCBv2AuthenticatorConfig(username, password, spec.Certpath, spec.Keypath)
	if err != nil {
		return nil, err
	} else if isX509 {
		Infof(KeyAuth, "Using cert authentication for bucket %s on %s", MD(spec.BucketName), MD(spec.Server))
	} else {
		Infof(KeyAuth, "Using credential authentication for bucket %s on %s", MD(spec.BucketName), MD(spec.Server))
	}

	timeoutsConfig := GoCBv2TimeoutsConfig(spec.BucketOpTimeout, StdlibDurationPtr(spec.GetViewQueryTimeout()))
	Infof(KeyAll, "Setting query timeouts for bucket %s to %v", spec.BucketName, timeoutsConfig.QueryTimeout)

	clusterOptions := gocb.ClusterOptions{
		Authenticator:  authenticatorConfig,
		SecurityConfig: securityConfig,
		TimeoutsConfig: timeoutsConfig,
	}

	if spec.KvPoolSize > 0 {
		// TODO: Equivalent of kvPoolSize in gocb v2?
	}

	cluster, err := gocb.Connect(connString, clusterOptions)
	if err != nil {
		Infof(KeyAuth, "Unable to connect to cluster: %v", err)
		return nil, err
	}

	// TODO: Cluster compatibility not exposed, need to make manual /pools/default/ call?

	// Connect to bucket
	bucket := cluster.Bucket(spec.BucketName)
	// TODO: identify required services and add to WaitUntilReadyOptions
	err = bucket.WaitUntilReady(30*time.Second, nil)
	if err != nil {
		Warnf("Error waiting for bucket to be ready: %v", err)
		return nil, err
	}

	nodeCount := 1
	router, routerErr := bucket.Internal().IORouter()
	if routerErr == nil {
		mgmtEps := router.MgmtEps()
		if mgmtEps != nil && len(mgmtEps) > 0 {
			nodeCount = len(mgmtEps)
		}
	}

	viewOpsQueue := make(chan struct{}, MaxConcurrentViewOps*nodeCount)
	collection := &Collection{
		Collection: bucket.DefaultCollection(),
		cluster:    cluster,
		viewOps:    viewOpsQueue,
	}

	return collection, nil
}

type Collection struct {
	*gocb.Collection               // underlying gocb Collection
	Spec             BucketSpec    // keep a copy of the BucketSpec for DCP usage
	cluster          *gocb.Cluster // Associated cluster - required for N1QL operations
	viewOps          chan struct{} // Manages max concurrent view ops (per kv node)
}

// DataStore
func (c *Collection) GetName() string {
	return c.Collection.Name()
}

func (c *Collection) UUID() (string, error) {
	return "", errors.New("Not implemented")
}
func (c *Collection) Close() {
	// No close handling for collection
	return
}

func (c *Collection) IsSupported(feature sgbucket.DataStoreFeature) bool {
	return true
}

// KV store

func (c *Collection) Get(k string, rv interface{}) (cas uint64, err error) {
	getResult, err := c.Collection.Get(k, nil)
	if err != nil {
		return 0, err
	}
	err = getResult.Content(rv)
	return uint64(getResult.Cas()), err
}

func (c *Collection) GetRaw(k string) (rv []byte, cas uint64, err error) {
	getOptions := &gocb.GetOptions{
		Transcoder: gocb.NewRawBinaryTranscoder(),
	}
	getRawResult, getErr := c.Collection.Get(k, getOptions)
	if getErr != nil {
		return nil, 0, getErr
	}

	err = getRawResult.Content(&rv)
	return rv, uint64(getRawResult.Cas()), err
}

func (c *Collection) GetAndTouchRaw(k string, exp uint32) (rv []byte, cas uint64, err error) {
	getAndTouchOptions := &gocb.GetAndTouchOptions{
		Transcoder: gocb.NewRawBinaryTranscoder(),
	}
	getAndTouchRawResult, getErr := c.Collection.GetAndTouch(k, expAsDuration(exp), getAndTouchOptions)
	if getErr != nil {
		return nil, 0, getErr
	}

	err = getAndTouchRawResult.Content(&rv)
	return rv, uint64(getAndTouchRawResult.Cas()), err
}

func (c *Collection) Touch(k string, exp uint32) (cas uint64, err error) {
	result, err := c.Collection.Touch(k, expAsDuration(exp), nil)
	if err != nil {
		return 0, err
	}
	return uint64(result.Cas()), nil
}

func (c *Collection) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	opts := &gocb.InsertOptions{
		Expiry: expAsDuration(exp),
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
	opts := &gocb.InsertOptions{
		Expiry:     expAsDuration(exp),
		Transcoder: gocb.NewRawBinaryTranscoder(),
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

func (c *Collection) Set(k string, exp uint32, v interface{}) error {
	upsertOptions := &gocb.UpsertOptions{
		Expiry: expAsDuration(exp),
	}
	_, err := c.Collection.Upsert(k, v, upsertOptions)
	return err
}

func (c *Collection) SetRaw(k string, exp uint32, v []byte) error {
	upsertOptions := &gocb.UpsertOptions{
		Expiry:     expAsDuration(exp),
		Transcoder: gocb.NewRawBinaryTranscoder(),
	}
	_, err := c.Collection.Upsert(k, v, upsertOptions)
	return err
}

func (c *Collection) WriteCas(k string, flags int, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (casOut uint64, err error) {
	var result *gocb.MutationResult
	if cas == 0 {
		insertOpts := &gocb.InsertOptions{
			Expiry: expAsDuration(exp),
		}
		if opt == sgbucket.Raw {
			insertOpts.Transcoder = gocb.NewRawBinaryTranscoder()
		}
		result, err = c.Collection.Insert(k, v, insertOpts)
	} else {
		replaceOpts := &gocb.ReplaceOptions{
			Cas:    gocb.Cas(cas),
			Expiry: expAsDuration(exp),
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
		getResult, err := c.Collection.Get(k, getOptions)
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
		if cas == 0 {
			// If the Get fails, the cas will be 0 and so call Insert().
			// If we get an error on the insert, due to a race, this will
			// go back through the cas loop
			insertOpts := &gocb.InsertOptions{
				Transcoder: gocb.NewRawJSONTranscoder(),
				Expiry:     expAsDuration(exp),
			}
			result, err = c.Collection.Insert(k, value, insertOpts)
			if err == nil {
				casGoCB = result.Cas()
			}
		} else {
			if value == nil && isDelete {
				removeOptions := &gocb.RemoveOptions{
					Cas: gocb.Cas(cas),
				}
				result, err = c.Collection.Remove(k, removeOptions)
				if err == nil {
					casGoCB = result.Cas()
				}
			} else {
				// Otherwise, attempt to do a replace.  won't succeed if
				// updated underneath us
				replaceOptions := &gocb.ReplaceOptions{
					Transcoder: gocb.NewRawJSONTranscoder(),
					Cas:        gocb.Cas(cas),
					Expiry:     expAsDuration(exp),
				}
				result, err = c.Collection.Replace(k, value, replaceOptions)
				if err == nil {
					casGoCB = result.Cas()
				}
			}
		}

		if errors.Is(err, gocb.ErrDocumentExists) {
			// retry on cas failure
		} else {
			// err will be nil if successful
			return uint64(casGoCB), err
		}
	}
}

func (c *Collection) Incr(k string, amt, def uint64, exp uint32) (uint64, error) {
	if amt == 0 {
		return 0, errors.New("amt passed to Incr must be non-zero")
	}
	incrOptions := gocb.IncrementOptions{
		Initial: int64(def),
		Delta:   amt,
		Expiry:  expAsDuration(exp),
	}
	incrResult, err := c.Collection.Binary().Increment(k, &incrOptions)
	if err != nil {
		return 0, err
	}

	return incrResult.Content(), nil
}

func (c *Collection) StartDCPFeed(args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	return errors.New("StartDCPFeed not implemented")
}
func (c *Collection) StartTapFeed(args sgbucket.FeedArguments, dbStats *expvar.Map) (sgbucket.MutationFeed, error) {
	return nil, errors.New("StartTapFeed not implemented")
}
func (c *Collection) Dump() {
	return
}

// CouchbaseStore

func (c *Collection) CouchbaseServerVersion() (major uint64, minor uint64, micro string) {
	return 0, 0, ""
}
func (c *Collection) GetStatsVbSeqno(maxVbno uint16, useAbsHighSeqNo bool) (uuids map[uint16]uint64, highSeqnos map[uint16]uint64, seqErr error) {
	return nil, nil, nil
}
func (c *Collection) GetMaxVbno() (uint16, error) {
	return 0, nil
}

func expAsDuration(exp uint32) time.Duration {
	return time.Duration(exp) * time.Second
}

func (c *Collection) IsError(err error, errorType sgbucket.DataStoreErrorType) bool {
	if err == nil {
		return false
	}
	switch errorType {
	case sgbucket.KeyNotFoundError:
		return errors.Is(err, gocb.ErrDocumentNotFound)
	default:
		return false
	}
}

// Recoverable errors or timeouts trigger retry for gocb v2 read operations
func (c *Collection) isRecoverableReadError(err error) bool {

	if err == nil {
		return false
	}

	if isGoCBTimeoutError(err) {
		return true
	}

	if errors.Is(err, gocb.ErrTemporaryFailure) || errors.Is(err, gocb.ErrOverload) {
		return true
	}
	return false
}

// Recoverable errors trigger retry for gocb v2 write operations
func (c *Collection) isRecoverableWriteError(err error) bool {

	if err == nil {
		return false
	}

	if errors.Is(err, gocb.ErrTemporaryFailure) || errors.Is(err, gocb.ErrOverload) {
		return true
	}
	return false
}

// This flushes the *entire* bucket associated with the collection (not just the collection).  Intended for test usage only.
func (c *Collection) Flush() error {

	bucketManager := c.cluster.Buckets()

	workerFlush := func() (shouldRetry bool, err error, value interface{}) {
		err = bucketManager.FlushBucket(c.Bucket().Name(), nil)
		if err != nil {
			Warnf("Error flushing bucket: %v  Will retry.", err)
			shouldRetry = true
		}
		return shouldRetry, err, nil
	}

	err, _ := RetryLoop("EmptyTestBucket", workerFlush, CreateDoublingSleeperFunc(12, 10))
	if err != nil {
		return err
	}

	// Wait until the bucket item count is 0, since flush is asynchronous
	worker := func() (shouldRetry bool, err error, value interface{}) {
		itemCount, err := c.BucketItemCount()
		if err != nil {
			return false, err, nil
		}

		if itemCount == 0 {
			// bucket flushed, we're done
			return false, nil, nil
		}

		// Retry
		return true, nil, nil

	}

	// Kick off retry loop
	err, _ = RetryLoop("Wait until bucket has 0 items after flush", worker, CreateMaxDoublingSleeperFunc(25, 100, 10000))
	if err != nil {
		return pkgerrors.Wrapf(err, "Error during Wait until bucket %s has 0 items after flush", MD(c.Bucket().Name()).Redact())
	}

	return nil

}

// BucketItemCount first tries to retrieve an accurate bucket count via N1QL,
// but falls back to the REST API if that cannot be done (when there's no index to count all items in a bucket)
func (c *Collection) BucketItemCount() (itemCount int, err error) {
	itemCount, err = QueryBucketItemCount(c)
	if err == nil {
		return itemCount, nil
	}

	// TODO: implement APIBucketItemCount for collections as part of CouchbaseStore refactoring.  Until then, give flush a moment to finish
	time.Sleep(1 * time.Second)
	//itemCount, err = bucket.APIBucketItemCount()
	return 0, err
}
