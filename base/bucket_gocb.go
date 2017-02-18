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
	"expvar"
	"fmt"
	"reflect"
	"strings"

	"github.com/couchbase/gocb"
	"github.com/couchbase/gocb/gocbcore"
	sgbucket "github.com/couchbase/sg-bucket"
)

var gocbExpvars *expvar.Map

const (
	MaxConcurrentSingleOps = 1000 // Max 1000 concurrent single bucket ops
	MaxConcurrentBulkOps   = 35   // Max 35 concurrent bulk ops
	MaxBulkBatchSize       = 100  // Maximum number of ops per bulk call
)

// GoCB error types - workaround until gocb has public error type lookup support
type GoCBError uint8

const (
	GoCBErr_MemdStatusKeyNotFound GoCBError = iota
	GoCBErr_MemdStatusKeyExists
	GoCBErr_MemdStatusBusy
	GoCBErr_MemdStatusTmpFail
	GoCBErr_Timeout
	GoCBErr_QueueOverflow
	GoCBErr_Unknown
)

var recoverableGoCBErrors = map[GoCBError]struct{}{
	GoCBErr_Timeout:           {},
	GoCBErr_QueueOverflow:     {},
	GoCBErr_MemdStatusBusy:    {},
	GoCBErr_MemdStatusTmpFail: {},
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
func GetCouchbaseBucketGoCB(spec BucketSpec) (bucket Bucket, err error) {

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
		_, password, _ = spec.Auth.GetCredentials()
	}

	goCBBucket, err := cluster.OpenBucket(spec.BucketName, password)

	if err != nil {
		return nil, err
	}

	spec.MaxNumRetries = 10
	spec.InitialRetrySleepTimeMS = 5

	// Define channels to limit the number of concurrent single and bulk operations,
	// to avoid gocb queue overflow issues
	bucket = CouchbaseBucketGoCB{
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

	sleeper := CreateDoublingSleeperFunc(
		bucket.spec.MaxNumRetries,
		bucket.spec.InitialRetrySleepTimeMS,
	)

	// Kick off retry loop
	description := fmt.Sprintf("Get %v", k)
	err, result := RetryLoop(description, worker, sleeper)

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint64(0)
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		return 0, fmt.Errorf("Error doing type assertion of %v into a uint64,  Key: %v", result, k)
	}

	return cas, err

}

// Retry up to the retry limit, then return.  Does not retry items if they had CAS failures,
// and it's up to the caller to handle those.
func (bucket CouchbaseBucketGoCB) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {

	// Create the RetryWorker for BulkSet op
	worker := bucket.newSetBulkRetryWorker(entries)

	// this is the function that will be called back by the RetryLoop to determine
	// how long to sleep before retrying (uses backoff)
	sleeper := CreateDoublingSleeperFunc(
		bucket.spec.MaxNumRetries,
		bucket.spec.InitialRetrySleepTimeMS,
	)

	// Kick off retry loop
	description := fmt.Sprintf("SetBulk with %v entries", len(entries))
	err, _ = RetryLoop(description, worker, sleeper)

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

	// this is the function that will be called back by the RetryLoop to determine
	// how long to sleep before retrying (uses backoff)
	sleeper := CreateDoublingSleeperFunc(
		bucket.spec.MaxNumRetries,
		bucket.spec.InitialRetrySleepTimeMS,
	)

	// Kick off retry loop
	description := fmt.Sprintf("GetBulkRaw with %v keys", len(keys))
	err, result := RetryLoop(description, worker, sleeper)

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

	// this is the function that will be called back by the RetryLoop to determine
	// how long to sleep before retrying (uses backoff)
	sleeper := CreateDoublingSleeperFunc(
		bucket.spec.MaxNumRetries,
		bucket.spec.InitialRetrySleepTimeMS,
	)

	// Kick off retry loop
	description := fmt.Sprintf("GetBulkRaw with %v keys", len(keys))
	err, result := RetryLoop(description, worker, sleeper)

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

	// uncomment this to enable the retry code to be excercised via
	// certain unit tests
	// if doSingleFakeRecoverableGOCBError == true {
	// 	doSingleFakeRecoverableGOCBError = false
	// 	return true
	// }

	if err == nil {
		return false
	}

	goCBErr := GoCBErrorType(err)

	_, ok := recoverableGoCBErrors[goCBErr]

	return ok
}

// GoCB error types - workaround until gocb has public error type lookup support
func GoCBErrorType(err error) GoCBError {

	if err == nil {
		return GoCBErr_Unknown
	}

	// gocb internal errors
	if strings.Contains(err.Error(), "timed out") {
		return GoCBErr_Timeout
	}
	if strings.Contains(err.Error(), "Queue overflow") {
		return GoCBErr_QueueOverflow
	}

	// Transient Couchbase Server errors.  Copy-pasted from
	// https://github.com/couchbase/gocb/blob/master/gocbcore/error.go#L62
	// until gocb provides a way to validate.
	if strings.Contains(err.Error(), "The server is busy. Try again later.") {
		return GoCBErr_MemdStatusBusy
	}
	if strings.Contains(err.Error(), "A temporary failure occurred.  Try again later.") {
		return GoCBErr_MemdStatusTmpFail
	}

	if strings.Contains(err.Error(), "Key not found.") {
		return GoCBErr_MemdStatusKeyNotFound
	}

	if strings.Contains(err.Error(), "Key already exists.") {
		return GoCBErr_MemdStatusKeyExists
	}

	return GoCBErr_Unknown

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

	sleeper := CreateDoublingSleeperFunc(
		bucket.spec.MaxNumRetries,
		bucket.spec.InitialRetrySleepTimeMS,
	)

	// Kick off retry loop
	description := fmt.Sprintf("GetAndTouchRaw with key %v", k)
	err, result := RetryLoop(description, worker, sleeper)

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint64(0)
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		return nil, 0, fmt.Errorf("Error doing type assertion of %v into a uint64", result)
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

	if GoCBErrorType(err) == GoCBErr_MemdStatusKeyExists {
		return false, nil
	}
	return err == nil, err
}

func (bucket CouchbaseBucketGoCB) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()
	gocbExpvars.Add("AddRaw", 1)
	_, err = bucket.Bucket.Insert(k, v, uint32(exp))

	if GoCBErrorType(err) == GoCBErr_MemdStatusKeyExists {
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
	_, err := bucket.Bucket.Upsert(k, v, uint32(exp))
	return err
}

func (bucket CouchbaseBucketGoCB) Delete(k string) error {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()
	gocbExpvars.Add("Delete", 1)
	_, err := bucket.Bucket.Remove(k, 0)
	return err
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

	sleeper := CreateDoublingSleeperFunc(
		bucket.spec.MaxNumRetries,
		bucket.spec.InitialRetrySleepTimeMS,
	)

	// Kick off retry loop
	description := fmt.Sprintf("WriteCas with key %v", k)
	err, result := RetryLoop(description, worker, sleeper)

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint64(0)
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		return 0, fmt.Errorf("Error doing type assertion of %v into a uint64,  Key: %v", result, k)
	}

	return cas, err

}

func (bucket CouchbaseBucketGoCB) Update(k string, exp int, callback sgbucket.UpdateFunc) error {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	maxCasRetries := 100000 // prevent infinite loop
	for i := 0; i < maxCasRetries; i++ {

		var value interface{}
		var err error

		// Load the existing value.
		// NOTE: ignore error and assume it's a "key not found" error.  If it's a more
		// serious error, it will probably recur when calling other ops below

		gocbExpvars.Add("Update_Get", 1)
		cas, _ := bucket.Get(k, &value)

		var callbackParam []byte
		if value != nil {
			callbackParam = value.([]byte)
		}

		// Invoke callback to get updated value
		value, err = callback(callbackParam)
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

	return fmt.Errorf("Failed to update after %v CAS attempts", maxCasRetries)

}

func (bucket CouchbaseBucketGoCB) WriteUpdate(k string, exp int, callback sgbucket.WriteUpdateFunc) error {
	LogPanic("Unimplemented method: WriteUpdate()")
	return nil
}

func (bucket CouchbaseBucketGoCB) Incr(k string, amt, def uint64, exp int) (uint64, error) {

	bucket.singleOps <- struct{}{}
	defer func() {
		<-bucket.singleOps
	}()

	worker := func() (shouldRetry bool, err error, value interface{}) {

		// GoCB's Counter returns an error if amt=0 and the counter exists.  If amt=0, instead first
		// attempt a simple get, which gocb will transcode to uint64
		if amt == 0 {
			var result uint64
			_, err := bucket.Get(k, &result)
			shouldRetry = isRecoverableGoCBError(err)
			// don't return an error, since that will break calling code
			if err != nil {
				// but if there was an error, make sure to return a 0 result
				return shouldRetry, nil, uint64(0)
			}
			return shouldRetry, nil, result

		}

		result, _, err := bucket.Counter(k, int64(amt), int64(def), uint32(exp))
		shouldRetry = isRecoverableGoCBError(err)
		return shouldRetry, err, result

	}

	sleeper := CreateDoublingSleeperFunc(
		bucket.spec.MaxNumRetries,
		bucket.spec.InitialRetrySleepTimeMS,
	)

	// Kick off retry loop
	description := fmt.Sprintf("Incr with key: %v", k)
	err, result := RetryLoop(description, worker, sleeper)

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint64(0)
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		return 0, fmt.Errorf("Error doing type assertion of %v into a uint64,  Key: %v", result, k)
	}

	return cas, err

}

func (bucket CouchbaseBucketGoCB) GetDDoc(docname string, into interface{}) error {
	LogPanic("Unimplemented method: GetDDoc()")
	return nil
}

func (bucket CouchbaseBucketGoCB) PutDDoc(docname string, value interface{}) error {

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
