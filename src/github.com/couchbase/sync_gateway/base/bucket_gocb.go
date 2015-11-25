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
	"strings"

	"github.com/couchbase/gocb"
	"github.com/couchbase/gocb/gocbcore"
	"github.com/couchbase/sg-bucket"
)

var gocbExpvars *expvar.Map

func init() {
	gocbExpvars = expvar.NewMap("syncGateway_gocb")
}

// Implementation of sgbucket.Bucket that talks to a Couchbase server and uses gocb
type CouchbaseBucketGoCB struct {
	*gocb.Bucket            // the underlying gocb bucket
	spec         BucketSpec // keep a copy of the BucketSpec for DCP usage
}

type GoCBLogger struct{}

func (l GoCBLogger) Output(s string) error {
	LogTo("gocb", s)
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

	bucket = CouchbaseBucketGoCB{
		goCBBucket,
		spec,
	}

	return bucket, err

}

func (bucket CouchbaseBucketGoCB) GetName() string {
	return bucket.spec.BucketName
}

func (bucket CouchbaseBucketGoCB) Get(k string, rv interface{}) (cas uint64, err error) {

	worker := func() (finished bool, err error, value interface{}) {

		gocbExpvars.Add("Get", 1)
		casGoCB, err := bucket.Bucket.Get(k, rv)

		if err == nil {
			finished = true
		} else {
			finished = isNotFoundError(err)
		}

		return finished, err, uint64(casGoCB)

	}

	sleeper := CreateDoublingSleeperFunc(
		bucket.spec.MaxNumRetries,
		bucket.spec.InitialRetrySleepTimeMS,
	)

	// Kick off retry loop
	err, result := RetryLoop(worker, sleeper)

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint64(0)
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		LogPanic("Error doing type assertion of %v into a uint64", result)
	}

	return cas, err

}

func (bucket CouchbaseBucketGoCB) GetRaw(k string) (rv []byte, cas uint64, err error) {

	// TODO: rework this method to leverage bucket.Get() and reduce code duplication

	var returnVal interface{}
	worker := func() (finished bool, err error, value interface{}) {

		gocbExpvars.Add("GetRaw", 1)
		casGoCB, err := bucket.Bucket.Get(k, &returnVal)

		if err == nil {
			finished = true
		} else {
			finished = isNotFoundError(err)
		}

		return finished, err, uint64(casGoCB)

	}

	sleeper := CreateDoublingSleeperFunc(
		bucket.spec.MaxNumRetries,
		bucket.spec.InitialRetrySleepTimeMS,
	)

	// Kick off retry loop
	err, result := RetryLoop(worker, sleeper)

	// If the retry loop returned a nil result, set to 0 to prevent type assertion on nil error
	if result == nil {
		result = uint64(0)
	}

	// Type assertion of result
	cas, ok := result.(uint64)
	if !ok {
		LogPanic("Error doing type assertion of %v into a uint64", result)
	}

	// If returnVal was never set to anything, return nil or else type assertion below will panic
	if returnVal == nil {
		return nil, cas, err
	}

	return returnVal.([]byte), cas, err

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
	err, _ = RetryLoop(worker, sleeper)

	return err

}

func (bucket CouchbaseBucketGoCB) newSetBulkRetryWorker(entries []*sgbucket.BulkSetEntry) RetryWorker {

	// track the keys that either succeeded w/o error or had a CAS error that there's no point retrying
	succeededKeys := map[string]interface{}{}
	noRetryKeys := map[string]interface{}{}

	worker := func() (finished bool, err error, value interface{}) {

		var items []gocb.BulkOp
		for _, entry := range entries {

			// skip any entries that have already succeeded or had cas errors
			if mapContains(succeededKeys, entry.Key) || mapContains(noRetryKeys, entry.Key) {
				continue
			}

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

		// If there are no items to process because they were all skipped, that means we're done!
		if len(items) == 0 {
			return true, nil, nil
		}

		// Do the underlying bulk operation
		if err := bucket.Do(items); err != nil {
			return true, err, nil
		}

		for index, item := range items {
			entry := entries[index]
			switch item := item.(type) {
			case *gocb.InsertOp:
				entry.Cas = uint64(item.Cas)
				entry.Error = item.Err
				if item.Err == nil {
					succeededKeys[entry.Key] = struct{}{}
				}
				if isCasFailure(item.Err) {
					noRetryKeys[entry.Key] = struct{}{}
				}
			case *gocb.ReplaceOp:
				entry.Cas = uint64(item.Cas)
				entry.Error = item.Err
				if item.Err == nil {
					succeededKeys[entry.Key] = struct{}{}
				}
				if isCasFailure(item.Err) {
					noRetryKeys[entry.Key] = struct{}{}
				}
			}
		}

		// If every entry is either in the succeededKeys or the noRetryKeys, then we're done
		if len(succeededKeys)+len(noRetryKeys) == len(entries) {
			finished = true
		}

		return finished, nil, nil

	}

	return worker
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

	// Create a RetryWorker for the GetBulkRaw operation
	worker := bucket.newGetBulkRawRetryWorker(keys)

	// this is the function that will be called back by the RetryLoop to determine
	// how long to sleep before retrying (uses backoff)
	sleeper := CreateDoublingSleeperFunc(
		bucket.spec.MaxNumRetries,
		bucket.spec.InitialRetrySleepTimeMS,
	)

	// Kick off retry loop
	err, result := RetryLoop(worker, sleeper)

	// If the RetryLoop returns a nil result, convert to an empty map.
	if result == nil {
		return map[string][]byte{}, err
	}

	// Type assertion of result into a map
	resultMap, ok := result.(map[string][]byte)
	if !ok {
		LogPanic("Error doing type assertion of %v into a map", result)
	}

	return resultMap, err

}

func (bucket CouchbaseBucketGoCB) newGetBulkRawRetryWorker(keys []string) RetryWorker {

	resultAccumulator := make(map[string][]byte, len(keys))
	noRetryKeys := []string{}
	retryKeys := []string{}

	worker := func() (finished bool, err error, value interface{}) {

		var items []gocb.BulkOp
		for _, key := range keys {
			var value []byte
			item := &gocb.GetOp{Key: key, Value: &value}
			items = append(items, item)
		}
		err = bucket.Do(items)
		if err != nil {
			return true, err, nil
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
				}
			} else {
				// if it's a "not found" error, then throw it in failure collection
				// and don't retry.  Otherwise, throw it in retry collection.
				if isNotFoundError(getOp.Err) {
					noRetryKeys = append(noRetryKeys, getOp.Key)
				} else {
					retryKeys = append(retryKeys, getOp.Key)
				}
			}

		}

		// if there are no keys to retry, then we're done.
		if len(retryKeys) == 0 {
			return true, nil, resultAccumulator
		}

		// otherwise, retry the keys the need to be retried
		keys = retryKeys

		return false, nil, nil

	}

	return worker

}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "Key not found")
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
	var returnVal interface{}
	gocbExpvars.Add("GetAndTouchRaw", 1)
	casGoCB, err := bucket.Bucket.GetAndTouch(k, uint32(exp), &returnVal)
	if err != nil {
		return nil, 0, err
	}
	return returnVal.([]byte), uint64(casGoCB), err
}

func (bucket CouchbaseBucketGoCB) Add(k string, exp int, v interface{}) (added bool, err error) {
	LogPanic("Unimplemented method: Add()")
	return false, nil
}

func (bucket CouchbaseBucketGoCB) AddRaw(k string, exp int, v []byte) (added bool, err error) {
	LogPanic("Unimplemented method: AddRaw()")
	return false, nil
}

func (bucket CouchbaseBucketGoCB) Append(k string, data []byte) error {
	LogPanic("Unimplemented method: Append()")
	return nil
}

func (bucket CouchbaseBucketGoCB) Set(k string, exp int, v interface{}) error {

	gocbExpvars.Add("Set", 1)
	_, err := bucket.Bucket.Upsert(k, v, uint32(exp))
	return err
}

func (bucket CouchbaseBucketGoCB) SetRaw(k string, exp int, v []byte) error {
	gocbExpvars.Add("SetRaw", 1)
	_, err := bucket.Bucket.Upsert(k, v, uint32(exp))
	return err
}

func (bucket CouchbaseBucketGoCB) Delete(k string) error {
	gocbExpvars.Add("Delete", 1)
	_, err := bucket.Bucket.Remove(k, 0)
	return err
}

func (bucket CouchbaseBucketGoCB) Write(k string, flags int, exp int, v interface{}, opt sgbucket.WriteOptions) error {
	LogPanic("Unimplemented method: Write()")
	return nil
}

func (bucket CouchbaseBucketGoCB) WriteCas(k string, flags int, exp int, cas uint64, v interface{}, opt sgbucket.WriteOptions) (casOut uint64, err error) {

	// we only support the sgbucket.Raw WriteOption at this point
	if opt != sgbucket.Raw {
		LogPanic("WriteOption must be sgbucket.Raw")
	}

	// also, flags must be 0, since that is not supported by gocb
	if flags != 0 {
		LogPanic("flags must be 0")
	}

	if cas == 0 {
		// Try to insert the value into the bucket
		gocbExpvars.Add("WriteCas_Insert", 1)
		newCas, err := bucket.Bucket.Insert(k, v, uint32(exp))
		return uint64(newCas), err
	}

	// Otherwise, replace existing value
	gocbExpvars.Add("WriteCas_Replace", 1)
	newCas, err := bucket.Bucket.Replace(k, v, gocb.Cas(cas), uint32(exp))

	return uint64(newCas), err

}

func (bucket CouchbaseBucketGoCB) Update(k string, exp int, callback sgbucket.UpdateFunc) error {

	maxCasRetries := 100000 // prevent infinite loop
	for i := 0; i < maxCasRetries; i++ {

		var value interface{}
		var err error

		// Load the existing value.
		// NOTE: ignore error and assume it's a "key not found" error.  If it's a more
		// serious error, it will probably recur when calling other ops below

		gocbExpvars.Add("Update_Get", 1)
		cas, _ := bucket.Bucket.Get(k, &value)

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
				_, err = bucket.Bucket.Remove(k, cas)
			} else {
				// Otherwise, attempt to do a replace.  won't succeed if
				// updated underneath us
				gocbExpvars.Add("Update_Replace", 1)
				_, err = bucket.Bucket.Replace(k, value, cas, uint32(exp))
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

	var result uint64
	// GoCB's Counter returns an error if amt=0 and the counter exists.  If amt=0, instead first
	// attempt a simple get, which gocb will transcode to uint64
	if amt == 0 {
		_, err := bucket.Get(k, &result)
		// If successful, return.  Otherwise fall through to Counter attempt (handles non-existent counter)
		if err == nil {
			return result, nil
		} else {
			Warn("Error during Get during Incr for key %s:%v", k, err)
			return 0, nil
		}
	}

	// Otherwise, attempt to use counter to increment/create if non-existing
	result, _, err := bucket.Counter(k, int64(amt), int64(def), uint32(exp))
	return result, err
}

func (bucket CouchbaseBucketGoCB) GetDDoc(docname string, into interface{}) error {
	LogPanic("Unimplemented method: GetDDoc()")
	return nil
}

func (bucket CouchbaseBucketGoCB) PutDDoc(docname string, value interface{}) error {
	LogPanic("Unimplemented method: PutDDoc()")
	return nil
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

func (bucket CouchbaseBucketGoCB) StartTapFeed(args sgbucket.TapArguments) (sgbucket.TapFeed, error) {
	LogPanic("Unimplemented method: StartTapFeed()")
	return nil, nil
}

func (bucket CouchbaseBucketGoCB) Close() {
	LogPanic("Unimplemented method: Close()")
}

func (bucket CouchbaseBucketGoCB) Dump() {
	LogPanic("Unimplemented method: Dump()")
}

func (bucket CouchbaseBucketGoCB) VBHash(docID string) uint32 {
	LogPanic("Unimplemented method: VBHash()")
	return 0
}
