//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"encoding/json"
	"expvar"
	"fmt"
	"reflect"
	"strings"

	"github.com/couchbase/gocb"
	sgbucket "github.com/couchbase/sg-bucket"
	"gopkg.in/couchbase/gocbcore.v7"
)

var gocbExpvars *expvar.Map

const (
	MaxConcurrentSingleOps = 1000 // Max 1000 concurrent single bucket ops
	MaxConcurrentBulkOps   = 35   // Max 35 concurrent bulk ops
	MaxBulkBatchSize       = 100  // Maximum number of ops per bulk call

	// Causes the write op to block until the change has been replicated to numNodesReplicateTo many nodes.
	// In our case, we only want to block until it's durable on the node we're writing to, so this is set to 0.
	numNodesReplicateTo = uint(0)

	// Causes the write op to block until the change has been persisted (made durable -- written to disk) on
	// numNodesPersistTo.  In our case, we only want to block until it's durable on the node we're writing to,
	// so this is set to 1
	numNodesPersistTo = uint(1)
)

var recoverableGoCBErrors = map[string]struct{}{
	gocbcore.ErrTimeout.Error():  {},
	gocbcore.ErrOverload.Error(): {},
	gocbcore.ErrBusy.Error():     {},
	gocbcore.ErrTmpFail.Error():  {},
}

func init() {
	gocbExpvars = expvar.NewMap("syncGateway_gocb")
}

// Implementation of sgbucket.Bucket that talks to a Couchbase server and uses gocb
type CouchbaseBucketGoCB struct {
	*gocb.Bucket               // the underlying gocb bucket
	spec         BucketSpec    // keep a copy of the BucketSpec for DCP usage
	singleOps    chan struct{} // Manages max concurrent single ops
	bulkOps      chan struct{} // Manages max concurrent bulk ops
}

type GoCBLogger struct{}

func (l GoCBLogger) Log(level gocbcore.LogLevel, offset int, format string, v ...interface{}) error {
	switch level {
	case gocbcore.LogError:
		LogError(fmt.Errorf(format, v))
	case gocbcore.LogWarn:
		Warn(format, v)
	default:
		LogTo("gocb", format, v)
	}
	return nil
}

func EnableGoCBLogging() {
	gocbcore.SetLogger(GoCBLogger{})
}

// Creates a Bucket that talks to a real live Couchbase server.
func GetCouchbaseBucketGoCB(spec BucketSpec) (bucket *CouchbaseBucketGoCB, err error) {

	// Only wrap the gocb logging when the log key is set, to avoid the overhead of a log keys
	// map lookup for every gocb log call

	logKeys := GetLogKeys()
	if logKeys["gocb"] {
		EnableGoCBLogging()
	}

	cluster, err := gocb.Connect(spec.Server)
	if err != nil {
		return nil, err
	}

	password := ""
	if spec.Auth != nil {
		user, pass, _ := spec.Auth.GetCredentials()
		authErr := cluster.Authenticate(gocb.RbacAuthenticator{
			Username: user,
			Password: pass,
		})
		// If RBAC authentication fails, revert to non-RBAC authentication by including the password to OpenBucket
		if authErr != nil {
			Warn("RBAC authentication against bucket %s as user %s failed - will re-attempt w/ bucketname, password", spec.BucketName, user)
			password = pass
		}
	}
	goCBBucket, err := cluster.OpenBucket(spec.BucketName, password)
	if err != nil {
		return nil, err
	}

	// initially this was using SGTranscoder for all GoCB buckets, but due to
	// https://github.com/couchbase/sync_gateway/pull/2416#issuecomment-288882896
	// it's only being set for hybrid buckets
	// goCBBucket.SetTranscoder(SGTranscoder{})

	spec.MaxNumRetries = 10
	spec.InitialRetrySleepTimeMS = 5

	// Define channels to limit the number of concurrent single and bulk operations,
	// to avoid gocb queue overflow issues
	bucket = &CouchbaseBucketGoCB{
		goCBBucket,
		spec,
		make(chan struct{}, MaxConcurrentSingleOps),
		make(chan struct{}, MaxConcurrentBulkOps),
	}

	return bucket, err

}

func (bucket CouchbaseBucketGoCB) GetBucketCredentials() (username, password string) {

	if bucket.spec.Auth != nil {
		_, password, _ = bucket.spec.Auth.GetCredentials()
	}
	return bucket.spec.BucketName, password
}

func (bucket CouchbaseBucketGoCB) GetName() string {
	return bucket.spec.BucketName
}

func (bucket CouchbaseBucketGoCB) GetRaw(k string) (rv []byte, cas uint64, err error) {

	var returnVal []byte
	cas, err = bucket.Get(k, &returnVal)
	if returnVal == nil {
		return nil, cas, err
	}
	// Take a copy of the returned value until gocb issue is fixed http://review.couchbase.org/#/c/72059/
	rv = make([]byte, len(returnVal))
	copy(rv, returnVal)

	return rv, cas, err

}

func (bucket CouchbaseBucketGoCB) Get(k string, rv interface{}) (cas uint64, err error) {

	bucket.singleOps <- struct{}{}
	gocbExpvars.Add("SingleOps", 1)
	defer func() {
		<-bucket.singleOps
		gocbExpvars.Add("SingleOps", -1)
	}()
	worker := func() (shouldRetry bool, err error, value interface{}) {

		gocbExpvars.Add("Get", 1)
		casGoCB, err := bucket.Bucket.Get(k, rv)
		shouldRetry = isRecoverableGoCBError(err)

		return shouldRetry, err, uint64(casGoCB)

	}

	// Kick off retry loop
	description := fmt.Sprintf("Get %v", k)
	err, result := RetryLoop(description, worker, bucket.spec.RetrySleeper())

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint64(0)
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		return 0, fmt.Errorf("Get: Error doing type assertion of %v into a uint64,  Key: %v", result, k)
	}

	return cas, err

}

// Retry up to the retry limit, then return.  Does not retry items if they had CAS failures,
// and it's up to the caller to handle those.
func (bucket CouchbaseBucketGoCB) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {

	// Create the RetryWorker for BulkSet op
	worker := bucket.newSetBulkRetryWorker(entries)

	// Kick off retry loop
	description := fmt.Sprintf("SetBulk with %v entries", len(entries))
	err, _ = RetryLoop(description, worker, bucket.spec.RetrySleeper())

	return err

}

func (bucket CouchbaseBucketGoCB) newSetBulkRetryWorker(entries []*sgbucket.BulkSetEntry) RetryWorker {

	worker := func() (shouldRetry bool, err error, value interface{}) {

		retryEntries := []*sgbucket.BulkSetEntry{}

		// break up into batches
		entryBatches := createBatchesEntries(MaxBulkBatchSize, entries)

		for _, entryBatch := range entryBatches {
			err, retryEntriesForBatch := bucket.processBulkSetEntriesBatch(
				entryBatch,
			)
			retryEntries = append(retryEntries, retryEntriesForBatch...)
			if err != nil {
				return false, err, nil
			}
		}

		// if there are no keys to retry, then we're done.
		if len(retryEntries) == 0 {
			return false, nil, nil
		}

		// otherwise, retry the entries that need to be retried
		entries = retryEntries

		// return true to signal that this function needs to be retried
		return true, nil, nil

	}

	return worker

}

func (bucket CouchbaseBucketGoCB) processBulkSetEntriesBatch(entries []*sgbucket.BulkSetEntry) (error, []*sgbucket.BulkSetEntry) {

	retryEntries := []*sgbucket.BulkSetEntry{}
	var items []gocb.BulkOp

	for _, entry := range entries {

		switch entry.Cas {
		case 0:
			// if no CAS val, treat it as an insert (similar to WriteCas())
			item := &gocb.InsertOp{
				Key:   entry.Key,
				Value: entry.Value,
			}
			items = append(items, item)
		default:
			// otherwise, treat it as a replace
			item := &gocb.ReplaceOp{
				Key:   entry.Key,
				Value: entry.Value,
				Cas:   gocb.Cas(entry.Cas),
			}
			items = append(items, item)
		}

	}

	// Do the underlying bulk operation
	if err := bucket.Do(items); err != nil {
		return err, retryEntries
	}

	for index, item := range items {
		entry := entries[index]
		switch item := item.(type) {
		case *gocb.InsertOp:
			entry.Cas = uint64(item.Cas)
			entry.Error = item.Err
			if item.Err != nil && isRecoverableGoCBError(item.Err) {
				retryEntries = append(retryEntries, entry)
			}
		case *gocb.ReplaceOp:
			entry.Cas = uint64(item.Cas)
			entry.Error = item.Err
			if item.Err != nil && isRecoverableGoCBError(item.Err) {
				retryEntries = append(retryEntries, entry)
			}
		}
	}

	return nil, retryEntries

}

// Retrieve keys in bulk for increased efficiency.  If any keys are not found, they
// will not be returned, and so the size of the map may be less than the size of the
// keys slice, and no error will be returned in that case since it's an expected
// situation.
//
// If there is an "overall error" calling the underlying GoCB bulk operation, then
// that error will be returned.
//
// If there are errors on individual keys -- aside from "not found" errors -- such as
// QueueOverflow errors that can be retried successfully, they will be retried
// with a backoff loop.
func (bucket CouchbaseBucketGoCB) GetBulkRaw(keys []string) (map[string][]byte, error) {

	gocbExpvars.Add("GetBulkRaw", 1)
	gocbExpvars.Add("GetBulkRaw_totalKeys", int64(len(keys)))

	// Create a RetryWorker for the GetBulkRaw operation
	worker := bucket.newGetBulkRawRetryWorker(keys)

	// Kick off retry loop
	description := fmt.Sprintf("GetBulkRaw with %v keys", len(keys))
	err, result := RetryLoop(description, worker, bucket.spec.RetrySleeper())

	// If the RetryLoop returns a nil result, convert to an empty map.
	if result == nil {
		return map[string][]byte{}, err
	}

	// Type assertion of result into a map
	resultMap, ok := result.(map[string][]byte)
	if !ok {
		return nil, fmt.Errorf("Error doing type assertion of %v into a map", result)
	}

	return resultMap, err

}

// Retrieve keys in bulk for increased efficiency.  If any keys are not found, they
// will not be returned, and so the size of the map may be less than the size of the
// keys slice, and no error will be returned in that case since it's an expected
// situation.
//
// If there is an "overall error" calling the underlying GoCB bulk operation, then
// that error will be returned.
//
// If there are errors on individual keys -- aside from "not found" errors -- such as
// QueueOverflow errors that can be retried successfully, they will be retried
// with a backoff loop.
func (bucket CouchbaseBucketGoCB) GetBulkCounters(keys []string) (map[string]uint64, error) {

	gocbExpvars.Add("GetBulkRaw", 1)
	gocbExpvars.Add("GetBulkRaw_totalKeys", int64(len(keys)))

	// Create a RetryWorker for the GetBulkRaw operation
	worker := bucket.newGetBulkCountersRetryWorker(keys)

	// Kick off retry loop
	description := fmt.Sprintf("GetBulkRaw with %v keys", len(keys))
	err, result := RetryLoop(description, worker, bucket.spec.RetrySleeper())

	// If the RetryLoop returns a nil result, convert to an empty map.
	if result == nil {
		return map[string]uint64{}, err
	}

	// Type assertion of result into a map
	resultMap, ok := result.(map[string]uint64)
	if !ok {
		return nil, fmt.Errorf("Error doing type assertion of %v into a map", result)
	}

	return resultMap, err

}

func (bucket CouchbaseBucketGoCB) newGetBulkRawRetryWorker(keys []string) RetryWorker {

	// resultAccumulator scoped in closure, will accumulate results across multiple worker invocations
	resultAccumulator := make(map[string][]byte, len(keys))

	// pendingKeys scoped in closure, represents set of keys that still need to be attempted or re-attempted
	pendingKeys := keys

	worker := func() (shouldRetry bool, err error, value interface{}) {

		retryKeys := []string{}
		keyBatches := createBatchesKeys(MaxBulkBatchSize, pendingKeys)
		for _, keyBatch := range keyBatches {

			// process batch and add successful results to resultAccumulator
			// and recoverable (non "Not Found") errors to retryKeys
			err := bucket.processGetRawBatch(keyBatch, resultAccumulator, retryKeys)
			if err != nil {
				return false, err, nil
			}

		}

		// if there are no keys to retry, then we're done.
		if len(retryKeys) == 0 {
			return false, nil, resultAccumulator
		}

		// otherwise, retry the keys the need to be retried
		keys = retryKeys

		// return true to signal that this function needs to be retried
		return true, nil, nil

	}

	return worker

}

func (bucket CouchbaseBucketGoCB) newGetBulkCountersRetryWorker(keys []string) RetryWorker {

	// resultAccumulator scoped in closure, will accumulate results across multiple worker invocations
	resultAccumulator := make(map[string]uint64, len(keys))

	// pendingKeys scoped in closure, represents set of keys that still need to be attempted or re-attempted
	pendingKeys := keys

	worker := func() (shouldRetry bool, err error, value interface{}) {

		retryKeys := []string{}
		keyBatches := createBatchesKeys(MaxBulkBatchSize, pendingKeys)
		for _, keyBatch := range keyBatches {

			// process batch and add successful results to resultAccumulator
			// and recoverable (non "Not Found") errors to retryKeys
			err := bucket.processGetCountersBatch(keyBatch, resultAccumulator, retryKeys)
			if err != nil {
				return false, err, nil
			}

		}

		// if there are no keys to retry, then we're done.
		if len(retryKeys) == 0 {
			return false, nil, resultAccumulator
		}

		// otherwise, retry the keys the need to be retried
		keys = retryKeys

		// return true to signal that this function needs to be retried
		return true, nil, nil

	}

	return worker

}

func (bucket CouchbaseBucketGoCB) processGetRawBatch(keys []string, resultAccumulator map[string][]byte, retryKeys []string) error {

	var items []gocb.BulkOp
	for _, key := range keys {
		var value []byte
		item := &gocb.GetOp{Key: key, Value: &value}
		items = append(items, item)
	}
	err := bucket.Do(items)
	if err != nil {
		return err
	}

	for _, item := range items {
		getOp, ok := item.(*gocb.GetOp)
		if !ok {
			continue
		}
		// Ignore any ops with errors.
		// NOTE: some of the errors are misleading:
		// https://issues.couchbase.com/browse/GOCBC-64
		if getOp.Err == nil {
			byteValue, ok := getOp.Value.(*[]byte)
			if ok {
				resultAccumulator[getOp.Key] = *byteValue
			} else {
				Warn("Skipping GetBulkRaw result - unable to cast to []byte.  Type: %v", reflect.TypeOf(getOp.Value))
			}
		} else {
			// if it's a recoverable error, then throw it in retry collection.
			if isRecoverableGoCBError(getOp.Err) {
				retryKeys = append(retryKeys, getOp.Key)
			}
		}

	}

	return nil
}

func (bucket CouchbaseBucketGoCB) processGetCountersBatch(keys []string, resultAccumulator map[string]uint64, retryKeys []string) error {

	var items []gocb.BulkOp
	for _, key := range keys {
		var value uint64
		item := &gocb.GetOp{Key: key, Value: &value}
		items = append(items, item)
	}
	err := bucket.Do(items)
	if err != nil {
		return err
	}

	for _, item := range items {
		getOp, ok := item.(*gocb.GetOp)
		if !ok {
			continue
		}
		// Ignore any ops with errors.
		// NOTE: some of the errors are misleading:
		// https://issues.couchbase.com/browse/GOCBC-64
		if getOp.Err == nil {
			intValue, ok := getOp.Value.(*uint64)
			if ok {
				resultAccumulator[getOp.Key] = *intValue
			} else {
				Warn("Skipping GetBulkCounter result - unable to cast to []byte.  Type: %v", reflect.TypeOf(getOp.Value))
			}
		} else {
			// if it's a recoverable error, then throw it in retry collection.
			if isRecoverableGoCBError(getOp.Err) {
				retryKeys = append(retryKeys, getOp.Key)
			}
		}

	}

	return nil
}

func (bucket CouchbaseBucketGoCB) Close() {
	bucket.Bucket.Close()
}

func createBatchesEntries(batchSize uint, entries []*sgbucket.BulkSetEntry) [][]*sgbucket.BulkSetEntry {
	// boundary checking
	if len(entries) == 0 {
		Warn("createBatchesEnrties called with empty entries")
		return [][]*sgbucket.BulkSetEntry{}
	}
	if batchSize == 0 {
		Warn("createBatchesEntries called with invalid batchSize")
		result := [][]*sgbucket.BulkSetEntry{}
		return append(result, entries)
	}

	batches := [][]*sgbucket.BulkSetEntry{}
	batch := []*sgbucket.BulkSetEntry{}

	for idxEntry, entry := range entries {

		batch = append(batch, entry)

		isBatchFull := uint(len(batch)) == batchSize
		isLastEntry := idxEntry == (len(entries) - 1)

		if isBatchFull || isLastEntry {
			// this batch is full, add it to batches and start a new batch
			batches = append(batches, batch)
			batch = []*sgbucket.BulkSetEntry{}
		}

	}
	return batches

}

func createBatchesKeys(batchSize uint, keys []string) [][]string {

	// boundary checking
	if len(keys) == 0 {
		Warn("createBatchesKeys called with empty keys")
		return [][]string{}
	}
	if batchSize == 0 {
		Warn("createBatchesKeys called with invalid batchSize")
		result := [][]string{}
		return append(result, keys)
	}

	batches := [][]string{}
	batch := []string{}

	for idxKey, key := range keys {

		batch = append(batch, key)

		isBatchFull := uint(len(batch)) == batchSize
		isLastKey := idxKey == (len(keys) - 1)

		if isBatchFull || isLastKey {
			// this batch is full, add it to batches and start a new batch
			batches = append(batches, batch)
			batch = []string{}
		}

	}
	return batches

}

// Set this to true to cause isRecoverableGoCBError() to return true
// on the next call.  Only useful for unit tests.
var doSingleFakeRecoverableGOCBError = false

// There are several errors from GoCB that are known to happen when it becomes overloaded:
//
// 1) WARNING: WriteCasRaw got error when calling GetRaw:%!(EXTRA gocb.timeoutError=The operation has timed out.) -- db.writeCasRaw() at crud.go:958
// 2) WARNING: WriteCasRaw got error when calling GetRaw:%!(EXTRA gocbcore.overloadError=Queue overflow.) -- db.writeCasRaw() at crud.go:958
//
// Other errors, such as "key not found" errors, which happen on CAS update failures and other
// situations, should not be treated as recoverable
//
func isRecoverableGoCBError(err error) bool {

	if err == nil {
		return false
	}

	_, ok := recoverableGoCBErrors[err.Error()]

	return ok
}

func isCasFailure(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Key already exists")
}

func containsElement(items []string, itemToCheck string) bool {
	for _, item := range items {
		if item == itemToCheck {
			return true
		}
	}
	return false
}

func mapContains(mapInstance map[string]interface{}, key string) bool {
	_, ok := mapInstance[key]
	return ok
}

func (bucket CouchbaseBucketGoCB) GetAndTouchRaw(k string, exp int) (rv []byte, cas uint64, err error) {

	bucket.singleOps <- struct{}{}
	gocbExpvars.Add("SingleOps", 1)
	defer func() {
		<-bucket.singleOps
		gocbExpvars.Add("SingleOps", -1)
	}()

	var returnVal interface{}
	worker := func() (shouldRetry bool, err error, value interface{}) {

		gocbExpvars.Add("GetAndTouchRaw", 1)
		casGoCB, err := bucket.Bucket.GetAndTouch(k, uint32(exp), &returnVal)
		shouldRetry = isRecoverableGoCBError(err)
		return shouldRetry, err, uint64(casGoCB)

	}

	// Kick off retry loop
	description := fmt.Sprintf("GetAndTouchRaw with key %v", k)
	err, result := RetryLoop(description, worker, bucket.spec.RetrySleeper())

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint64(0)
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		return nil, 0, fmt.Errorf("GetAndTouchRaw: Error doing type assertion of %v into a uint64", result)
	}

	// If returnVal was never set to anything, return nil or else type assertion below will panic
	if returnVal == nil {
		return nil, cas, err
	}

	return returnVal.([]byte), cas, err

}

func (bucket CouchbaseBucketGoCB) Add(k string, exp int, v interface{}) (added bool, err error) {
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()
	gocbExpvars.Add("Add", 1)
	_, err = bucket.Bucket.Insert(k, v, uint32(exp))

	if err != nil && err == gocb.ErrKeyExists {
		return false, nil
	}
	return err == nil, err
}

// GoCB AddRaw writes as BinaryDocument, which results in the document having the
// binary doc common flag set.  Callers that want to write JSON documents as raw bytes should
// pass v as []byte to the stanard bucket.Add
func (bucket CouchbaseBucketGoCB) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()
	gocbExpvars.Add("AddRaw", 1)
	_, err = bucket.Bucket.Insert(k, BinaryDocument(v), uint32(exp))

	if err != nil && err == gocb.ErrKeyExists {
		return false, nil
	}
	return err == nil, err
}

func (bucket CouchbaseBucketGoCB) Append(k string, data []byte) error {
	LogPanic("Unimplemented method: Append()")
	return nil
}

func (bucket CouchbaseBucketGoCB) Set(k string, exp int, v interface{}) error {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	gocbExpvars.Add("Set", 1)
	_, err := bucket.Bucket.Upsert(k, v, uint32(exp))
	return err
}

func (bucket CouchbaseBucketGoCB) SetRaw(k string, exp int, v []byte) error {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()
	gocbExpvars.Add("SetRaw", 1)
	_, err := bucket.Bucket.Upsert(k, BinaryDocument(v), uint32(exp))
	return err
}

func (bucket CouchbaseBucketGoCB) Delete(k string) error {

	_, err := bucket.Remove(k, 0)
	return err
}

func (bucket CouchbaseBucketGoCB) Remove(k string, cas uint64) (casOut uint64, err error) {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()
	gocbExpvars.Add("Delete", 1)
	newCas, err := bucket.Bucket.Remove(k, gocb.Cas(cas))
	return uint64(newCas), err
}

func (bucket CouchbaseBucketGoCB) Write(k string, flags int, exp int, v interface{}, opt sgbucket.WriteOptions) error {
	LogPanic("Unimplemented method: Write()")
	return nil
}

func (bucket CouchbaseBucketGoCB) WriteCas(k string, flags int, exp int, cas uint64, v interface{}, opt sgbucket.WriteOptions) (casOut uint64, err error) {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	// we only support the sgbucket.Raw WriteOption at this point
	if opt != 0 && opt != sgbucket.Raw {
		LogPanic("WriteOption must be empty or sgbucket.Raw")
	}

	// also, flags must be 0, since that is not supported by gocb
	if flags != 0 {
		LogPanic("flags must be 0")
	}

	worker := func() (shouldRetry bool, err error, value interface{}) {

		if cas == 0 {
			// Try to insert the value into the bucket
			gocbExpvars.Add("WriteCas_Insert", 1)
			newCas, err := bucket.Bucket.Insert(k, v, uint32(exp))
			shouldRetry = isRecoverableGoCBError(err)
			return shouldRetry, err, uint64(newCas)
		}

		// Otherwise, replace existing value
		gocbExpvars.Add("WriteCas_Replace", 1)
		newCas, err := bucket.Bucket.Replace(k, v, gocb.Cas(cas), uint32(exp))
		shouldRetry = isRecoverableGoCBError(err)
		return shouldRetry, err, uint64(newCas)

	}

	// Kick off retry loop
	description := fmt.Sprintf("WriteCas with key %v", k)
	err, result := RetryLoop(description, worker, bucket.spec.RetrySleeper())

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint64(0)
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		return 0, fmt.Errorf("WriteCas: Error doing type assertion of %v into a uint64,  Key: %v", result, k)
	}

	return cas, err

}

// CAS-safe write of a document and it's associated named xattr
func (bucket CouchbaseBucketGoCB) WriteCasWithXattr(k string, xattrKey string, exp int, cas uint64, v interface{}, xv interface{}) (casOut uint64, err error) {

	// WriteCasWithXattr always stamps the xattr with the new cas using macro expansion, into a top-level property called 'cas'.
	// This is the only use case for macro expansion today - if more cases turn up, should change the bucket API to handle this more generically.
	xattrCasProperty := fmt.Sprintf("%s.cas", xattrKey)

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value interface{}) {

		// If cas=0, treat as insert
		if cas == 0 {
			// TODO: Once that's fixed, need to wrap w/ retry handling
			gocbExpvars.Add("WriteCasWithXattr_Insert", 1)
			docFragment, err := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagReplaceDoc, 0, uint32(exp)).
				UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                 // Update the xattr
				UpsertEx(xattrCasProperty, "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros). // Stamp the cas on the xattr
				UpsertEx("", v, gocb.SubdocFlagNone).                                                         // Update the document body
				Execute()
			if err != nil {
				shouldRetry = isRecoverableGoCBError(err)
				return shouldRetry, err, uint64(0)
			}
			return false, nil, uint64(docFragment.Cas())
		}

		casOut = 0
		// Otherwise, replace existing value
		gocbExpvars.Add("WriteCas_Replace", 1)
		if v != nil {
			// Have value and xattr value - update both
			docFragment, err := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagReplaceDoc&gocb.SubdocDocFlagAccessDeleted, gocb.Cas(cas), uint32(exp)).
				UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                 // Update the xattr
				UpsertEx(xattrCasProperty, "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros). // Stamp the cas on the xattr
				UpsertEx("", v, gocb.SubdocFlagNone).                                                         // Update the document body
				Execute()
			if err != nil {
				shouldRetry = isRecoverableGoCBError(err)
				return shouldRetry, err, uint64(0)
			}
			casOut = uint64(docFragment.Cas())
		} else {
			// Update xattr only
			docFragment, err := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagAccessDeleted, gocb.Cas(cas), uint32(exp)).
				UpsertEx(xattrKey, xv, gocb.SubdocFlagXattr).                                                 // Update the xattr
				UpsertEx(xattrCasProperty, "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros). // Stamp the cas on the xattr
				Execute()
			if err != nil {
				shouldRetry = isRecoverableGoCBError(err)
				return shouldRetry, err, uint64(0)
			}
			casOut = uint64(docFragment.Cas())
		}

		return false, nil, casOut
	}

	// Kick off retry loop
	description := fmt.Sprintf("WriteCasWithXattr with key %v", k)
	err, result := RetryLoop(description, worker, bucket.spec.RetrySleeper())

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint64(0)
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		return 0, fmt.Errorf("WriteCasWithXattr: Error doing type assertion of %v into a uint64,  Key: %v", result, k)
	}

	return cas, err
}

// Retrieve a document and it's associated named xattr
func (bucket CouchbaseBucketGoCB) GetWithXattr(k string, xattrKey string, rv interface{}, xv interface{}) (cas uint64, err error) {

	// Until we get a fix for https://issues.couchbase.com/browse/MB-23522, need to disable the singleOp handling because of the potential for a nested call to bucket.Get
	/*
		bucket.singleOps <- struct{}{}
		gocbExpvars.Add("SingleOps", 1)
		defer func() {
			<-bucket.singleOps
			gocbExpvars.Add("SingleOps", -1)
		}()
	*/
	worker := func() (shouldRetry bool, err error, value interface{}) {

		gocbExpvars.Add("Get", 1)
		// First, attempt to get the document and xattr in one shot. We can't set SubdocDocFlagAccessDeleted when attempting
		// to retrieve the full doc body, so need to retry that scenario below.
		res, lookupErr := bucket.Bucket.LookupIn(k).
			GetEx(xattrKey, gocb.SubdocFlagXattr). // Get the xattr
			GetEx("", gocb.SubdocFlagNone).        // Get the document body
			Execute()

		switch lookupErr {
		case nil:
			// Successfully retrieved doc and (optionally) xattr.  Copy the contents into rv, xv and return.
			contentErr := res.Content("", rv)
			if contentErr != nil {
				LogTo("CRUD", "Unable to retrieve document content for key=%s, xattrKey=%s: %v", k, xattrKey, contentErr)
				return false, contentErr, uint64(0)
			}
			contentErr = res.Content(xattrKey, xv)
			if contentErr != nil {
				LogTo("CRUD", "Unable to retrieve xattr content for key=%s, xattrKey=%s: %v", k, xattrKey, contentErr)
				return false, contentErr, uint64(0)
			}
			cas = uint64(res.Cas())
			return false, nil, cas

		case gocbcore.ErrSubDocBadMulti:
			// TODO: LookupIn should handle all cases with a single op:
			//     - doc and xattr
			//     - no doc, no xattr
			//     - doc, no xattr
			//     - no doc, xattr
			cas, docOnlyErr := bucket.Get(k, rv)
			if docOnlyErr != nil {
				shouldRetry = isRecoverableGoCBError(docOnlyErr)
				return shouldRetry, docOnlyErr, uint64(0)
			}
			return false, nil, cas

		case gocb.ErrKeyNotFound:
			// Doc not found - need to check whether this is a deleted doc with an xattr.
			res, xattrOnlyErr := bucket.Bucket.LookupInEx(k, gocb.SubdocDocFlagAccessDeleted).
				GetEx(xattrKey, gocb.SubdocFlagXattr).
				Execute()

			// SubDocBadMulti means there's no xattr.  Since there's also no doc, return KeyNotFound
			if xattrOnlyErr == gocbcore.ErrSubDocBadMulti {
				return false, gocb.ErrKeyNotFound, uint64(0)
			}

			if xattrOnlyErr != nil && xattrOnlyErr != gocbcore.ErrSubDocSuccessDeleted {
				shouldRetry = isRecoverableGoCBError(xattrOnlyErr)
				return shouldRetry, xattrOnlyErr, uint64(0)
			}

			// Successfully retrieved xattr only - return
			contentErr := res.Content(xattrKey, xv)
			if contentErr != nil {
				LogTo("CRUD", "Unable to retrieve xattr content for key=%s, xattrKey=%s: %v", k, xattrKey, contentErr)
				return false, contentErr, uint64(0)
			}
			cas = uint64(res.Cas())
			return false, nil, cas

		default:
			shouldRetry = isRecoverableGoCBError(err)
			return shouldRetry, err, uint64(0)
		}

	}

	// Kick off retry loop
	description := fmt.Sprintf("GetWithXattr %v", k)
	err, result := RetryLoop(description, worker, bucket.spec.RetrySleeper())

	if result == nil {
		return 0, err
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		return 0, fmt.Errorf("GetWithXattr: Error doing type assertion of %v (%T) into a uint64,  Key: %v", result, result, k)
	}

	return cas, err

}

// Delete a document and it's associated named xattr.  Couchbase server will preserve system xattrs as part of the (CBS) tombstone when a document is deleted.
// To remove the system xattr as well, an explicit subdoc delete operation is required.
func (bucket CouchbaseBucketGoCB) DeleteWithXattr(k string, xattrKey string) error {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()
	gocbExpvars.Add("Delete", 1)
	removeCas, err := bucket.Bucket.Remove(k, 0)
	if err != nil && err != gocb.ErrKeyNotFound {
		return err
	}

	_, deleteXattrErr := bucket.Bucket.MutateInEx(k, gocb.SubdocDocFlagAccessDeleted, removeCas, 0).
		RemoveEx(xattrKey, gocb.SubdocFlagXattr).
		Execute()

	if deleteXattrErr != nil && deleteXattrErr != gocbcore.ErrSubDocSuccessDeleted {
		return deleteXattrErr
	}

	return nil
}

func (bucket CouchbaseBucketGoCB) Update(k string, exp int, callback sgbucket.UpdateFunc) error {

	for {

		var value []byte
		var err error

		// Load the existing value.
		// NOTE: ignore error and assume it's a "key not found" error.  If it's a more
		// serious error, it will probably recur when calling other ops below

		gocbExpvars.Add("Update_Get", 1)
		cas, err := bucket.Get(k, &value)
		if err != nil {
			if !bucket.IsKeyNotFoundError(err) {
				// Unexpected error, abort
				return err
			}
			cas = 0 // Key not found error
		}

		// Invoke callback to get updated value
		value, err = callback(value)
		if err != nil {
			return err
		}

		if cas == 0 {
			// If the Get fails, the cas will be 0 and so call Insert().
			// If we get an error on the insert, due to a race, this will
			// go back through the cas loop

			gocbExpvars.Add("Update_Insert", 1)
			_, err = bucket.Bucket.Insert(k, value, uint32(exp))
		} else {
			if value == nil {
				// In order to match the go-couchbase bucket behavior, if the
				// callback returns nil, we delete the doc
				gocbExpvars.Add("Update_Remove", 1)
				_, err = bucket.Bucket.Remove(k, gocb.Cas(cas))
			} else {
				// Otherwise, attempt to do a replace.  won't succeed if
				// updated underneath us
				gocbExpvars.Add("Update_Replace", 1)
				_, err = bucket.Bucket.Replace(k, value, gocb.Cas(cas), uint32(exp))
			}
		}

		// If there was no error, we're done
		if err == nil {
			return nil
		}

	}

}

func (bucket CouchbaseBucketGoCB) WriteUpdate(k string, exp int, callback sgbucket.WriteUpdateFunc) error {

	for {
		var value []byte
		var err error
		var writeOpts sgbucket.WriteOptions
		var cas uint64

		// Load the existing value.
		gocbExpvars.Add("Update_Get", 1)
		value, cas, err = bucket.GetRaw(k)
		if err != nil {
			if !bucket.IsKeyNotFoundError(err) {
				// Unexpected error, abort
				return err
			}
			cas = 0 // Key not found error
		}

		// Invoke callback to get updated value
		value, writeOpts, err = callback(value)
		if err != nil {
			return err
		}

		if cas == 0 {
			// If the Get fails, the cas will be 0 and so call Insert().
			// If we get an error on the insert, due to a race, this will
			// go back through the cas loop

			gocbExpvars.Add("Update_Insert", 1)
			if writeOpts&(sgbucket.Persist|sgbucket.Indexable) != 0 {
				_, err = bucket.Bucket.InsertDura(k, value, uint32(exp), numNodesReplicateTo, numNodesPersistTo)
			} else {
				_, err = bucket.Bucket.Insert(k, value, uint32(exp))
			}

		} else {
			if value == nil {

				// This breaks the parity with the go-couchbase bucket behavior because it feels
				// dangerous to remove data based on the callback return value.  If there are any
				// errors in the callbacks to cause it to return a nil return value, and no error,
				// it could erroneously remove data.  At present, nothing in the Sync Gateway codebase
				// is known to use this functionality anyway.
				//
				// If this functionality is re-added, this method should probably take a flag called
				// allowDeletes (bool) so that callers must intentionally allow deletes
				return fmt.Errorf("The ability to remove items via WriteUpdate has been removed.  See code comments in bucket_gocb.go")

			} else {
				// Otherwise, attempt to do a replace.  won't succeed if
				// updated underneath us
				gocbExpvars.Add("Update_Replace", 1)
				if writeOpts&(sgbucket.Persist|sgbucket.Indexable) != 0 {
					_, err = bucket.Bucket.ReplaceDura(k, value, gocb.Cas(cas), uint32(exp), numNodesReplicateTo, numNodesPersistTo)
				} else {
					_, err = bucket.Bucket.Replace(k, value, gocb.Cas(cas), uint32(exp))
				}

			}
		}
		// If there was no error, we're done
		if err == nil {
			return nil
		}
	}
}

func (bucket CouchbaseBucketGoCB) WriteUpdateWithXattr(k string, xattrKey string, exp int, callback sgbucket.WriteUpdateWithXattrFunc) error {

	for {
		var value []byte
		var xattrValue []byte
		var err error
		var cas uint64

		// Load the existing value.
		gocbExpvars.Add("Update_GetWithXattr", 1)
		cas, err = bucket.GetWithXattr(k, xattrKey, &value, &xattrValue)
		if err != nil {
			if !bucket.IsKeyNotFoundError(err) {
				// Unexpected error, cancel writeupdate
				LogTo("CRUD", "Retrieval of existing doc failed during WriteUpdateWithXattr for key=%s, xattrKey=%s: %v", k, xattrKey, err)
				return err
			}
			// Key not found - initialize cas and values
			cas = 0
			value = nil
			xattrValue = nil
		}

		// Invoke callback to get updated value
		updatedValue, updatedXattrValue, deleteDoc, err := callback(value, xattrValue, cas)
		if err != nil {
			return err
		}

		// TODO: Avoid unmarshalling the raw xattr value here
		var xattrMap map[string]interface{}
		err = json.Unmarshal(updatedXattrValue, &xattrMap)
		if err != nil {
			return err
		}

		var writeErr error
		// If this is a tombstone, we want to delete the document and update the xattr
		if deleteDoc {
			// TODO: replace with a single op when https://issues.couchbase.com/browse/MB-24098 is ready
			removeCas, removeErr := bucket.Remove(k, cas)
			if removeErr == nil {
				// Successful removal - update the cas for the xattr operation
				cas = removeCas
			} else if removeErr == gocb.ErrKeyNotFound {
				// Document body has already been removed - continue to xattr processing w/ same cas
			} else if isRecoverableGoCBError(removeErr) {
				// Recoverable error - retry WriteUpdateWithXattr
				continue
			} else {
				// Non-recoverable error - return
				return removeErr
			}

			// update xattr only
			_, writeErr := bucket.WriteCasWithXattr(k, xattrKey, exp, cas, nil, xattrMap)
			if writeErr != nil && writeErr != gocb.ErrKeyExists && !isRecoverableGoCBError(writeErr) {
				LogTo("CRUD", "Update of new value during WriteUpdateWithXattr failed for key %s: %v", k, writeErr)
				return writeErr
			}

			// If there was no error, we're done
			if writeErr == nil {
				return nil
			}
		} else {

			// Not a delete - update the body and xattr
			_, writeErr = bucket.WriteCasWithXattr(k, xattrKey, exp, cas, updatedValue, xattrMap)

			// ErrKeyExists is CAS failure, which we want to retry.  Other non-recoverable errors should cancel the
			// WriteUpdate.
			if writeErr != nil && writeErr != gocb.ErrKeyExists && !isRecoverableGoCBError(writeErr) {
				LogTo("CRUD", "Update of new value during WriteUpdateWithXattr failed for key %s: %v", k, writeErr)
				return writeErr
			}

			// If there was no error, we're done
			if writeErr == nil {
				return nil
			}
		}
	}

}

func (bucket CouchbaseBucketGoCB) Incr(k string, amt, def uint64, exp int) (uint64, error) {

	// GoCB's Counter returns an error if amt=0 and the counter exists.  If amt=0, instead first
	// attempt a simple get, which gocb will transcode to uint64.  The call to Get includes its own
	// retry handling, so doesn't need redundant retry handling here.
	if amt == 0 {
		var result uint64
		_, err := bucket.Get(k, &result)
		if err != nil {
			return uint64(0), nil
		}
		return result, nil
	}

	// This is an actual incr, not just counter retrieval.
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value interface{}) {

		result, _, err := bucket.Counter(k, int64(amt), int64(def), uint32(exp))
		shouldRetry = isRecoverableGoCBError(err)
		return shouldRetry, err, result

	}

	// Kick off retry loop
	description := fmt.Sprintf("Incr with key: %v", k)
	err, result := RetryLoop(description, worker, bucket.spec.RetrySleeper())

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint64(0)
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		return 0, fmt.Errorf("Incr: Error doing type assertion of %v into a uint64,  Key: %v", result, k)
	}

	return cas, err

}

func (bucket CouchbaseBucketGoCB) GetDDoc(docname string, into interface{}) error {
	LogPanic("Unimplemented method: GetDDoc()")
	return nil
}

func (bucket CouchbaseBucketGoCB) PutDDoc(docname string, value interface{}) error {

	// NOTE: this doesn't seem to be identical in behavior with go-couchbase PutDDoc, since this returns an
	// error if the design doc already exists, whereas go-couchbase PutDDoc handles it more gracefully.
	// For this reason, the GoCBGoCouchbaseHybridBucket calls down into go-couchbase for it's DDoc operations.

	// Get bucket manager.  Relies on existing auth settings for bucket.
	username, password := bucket.GetBucketCredentials()
	manager := bucket.Bucket.Manager(username, password)
	if manager == nil {
		return fmt.Errorf("Unable to obtain manager for bucket %s - cannot PUT design doc %s", bucket.GetName(), docname)
	}

	sgDesignDoc, ok := value.(sgbucket.DesignDoc)
	if !ok {
		return fmt.Errorf("Unable to identify specified design document")
	}

	gocbDesignDoc := &gocb.DesignDocument{
		Name:  docname,
		Views: make(map[string]gocb.View),
	}

	for viewName, view := range sgDesignDoc.Views {
		gocbView := gocb.View{
			Map: view.Map,
		}
		gocbDesignDoc.Views[viewName] = gocbView
	}

	return manager.InsertDesignDocument(gocbDesignDoc)

}

func (bucket CouchbaseBucketGoCB) IsKeyNotFoundError(err error) bool {
	return err == gocb.ErrKeyNotFound
}

func (bucket CouchbaseBucketGoCB) DeleteDDoc(docname string) error {
	LogPanic("Unimplemented method: DeleteDDoc()")
	return nil
}

func (bucket CouchbaseBucketGoCB) View(ddoc, name string, params map[string]interface{}) (sgbucket.ViewResult, error) {
	LogPanic("Unimplemented method: View()")
	return sgbucket.ViewResult{}, nil
}

func (bucket CouchbaseBucketGoCB) ViewCustom(ddoc, name string, params map[string]interface{}, vres interface{}) error {
	LogPanic("Unimplemented method: ViewCustom()")
	return nil
}

func (bucket CouchbaseBucketGoCB) Refresh() error {
	LogPanic("Unimplemented method: Refresh()")
	return nil
}

func (bucket CouchbaseBucketGoCB) StartTapFeed(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {
	LogPanic("Unimplemented method: StartTapFeed()")
	return nil, nil
}

func (bucket CouchbaseBucketGoCB) Dump() {
	Warn("CouchbaseBucketGoCB: Unimplemented method: Dump()")
}

func (bucket CouchbaseBucketGoCB) VBHash(docID string) uint32 {
	numVbuckets := bucket.Bucket.IoRouter().NumVbuckets()
	return VBHash(docID, numVbuckets)
}

func (bucket CouchbaseBucketGoCB) GetMaxVbno() (uint16, error) {

	if bucket.Bucket.IoRouter() != nil {
		return uint16(bucket.Bucket.IoRouter().NumVbuckets()), nil
	}
	return 0, fmt.Errorf("Unable to determine vbucket count")
}

func (bucket CouchbaseBucketGoCB) CouchbaseServerVersion() (major uint64, minor uint64, micro string, err error) {

	// TODO: implement this using the ServerStats map + add unit test
	// https://github.com/couchbase/gocb/blob/master/bucket_crud.go#L90
	return 0, 0, "error", fmt.Errorf("GoCB bucket does not implement CouchbaseServerVersion yet")
}

func (bucket CouchbaseBucketGoCB) UUID() (string, error) {
	// See https://github.com/couchbase/sync_gateway/issues/2418#issuecomment-289941131
	return "error", fmt.Errorf("GoCB bucket does not expose UUID")
}
