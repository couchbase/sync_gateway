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
	"log"
	"time"

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
	spec.InitialRetrySleepTimeMS = 500

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
	gocbExpvars.Add("Get", 1)
	casGoCB, err := bucket.Bucket.Get(k, rv)
	return uint64(casGoCB), err
}

func (bucket CouchbaseBucketGoCB) GetRaw(k string) (rv []byte, cas uint64, err error) {
	var returnVal interface{}

	gocbExpvars.Add("GetRaw", 1)
	casGoCB, err := bucket.Bucket.Get(k, &returnVal)
	if err != nil {
		return nil, 0, err
	}
	return returnVal.([]byte), uint64(casGoCB), err
}

func (bucket CouchbaseBucketGoCB) SetBulk(entries []*sgbucket.BulkSetEntry) (err error) {

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
	if err := bucket.Do(items); err != nil {
		return err
	}
	for index, item := range items {
		entry := entries[index]
		switch item := item.(type) {
		case *gocb.InsertOp:
			entry.Cas = uint64(item.Cas)
			entry.Error = item.Err
		case *gocb.ReplaceOp:
			entry.Cas = uint64(item.Cas)
			entry.Error = item.Err
		}
	}
	return nil

}

func (bucket CouchbaseBucketGoCB) GetBulkRaw(keys []string) (map[string][]byte, error) {

	numRetries := 0
	timeToSleepMs := bucket.spec.InitialRetrySleepTimeMS

	totalNumKeys := len(keys)
	resultsAccumulator := make(map[string][]byte, len(keys))
	keysRemaining := make([]string, totalNumKeys)
	copy(keysRemaining, keys)

	log.Printf("GetBulkRaw called with %v keys", totalNumKeys)
	defer log.Printf("/GetBulkRaw finished with %v keys", totalNumKeys)

	for {

		log.Printf("GetBulkRaw calloing getBulkRawOneOff with %v / %v keys", len(keysRemaining), totalNumKeys)
		result, err := getBulkRawOneOff(bucket, keysRemaining)

		// if we get an "overall" error, as opposed to errors on some of the keys
		// treat as fatal and just abort
		if err != nil {
			log.Printf("GetBulkRaw got an overall error: %v.  Aborting", err)
			return nil, err
		}

		// if we didn't get an error, and no keys are missing from the result,
		// then we're done
		log.Printf("GetBulkRaw len(result): %v / %v", len(result), totalNumKeys)
		if len(result) == totalNumKeys {
			return result, nil
		}

		// otherwise, if we didn't get an overall error but the result has less
		// than the total number of keys, add the result to the accumulated result
		log.Printf("GetBulkRaw merge %v keys into resultsAccumulator, which currently has %v / %v keys", len(result), len(resultsAccumulator), totalNumKeys)

		for key, val := range result {
			resultsAccumulator[key] = val
		}

		log.Printf("GetBulkRaw merged %v keys into resultsAccumulator, which currently has %v / %v keys", len(result), len(resultsAccumulator), totalNumKeys)

		// does the accumulated result have the total number of keys we want?
		// if so, we're done
		if len(resultsAccumulator) == totalNumKeys {
			return resultsAccumulator, nil
		}

		// figure out the set of keys that we are still missing, and set keysRemaining to this
		missingKeys := []string{}
		for _, key := range keys {
			_, ok := resultsAccumulator[key]
			if !ok {
				missingKeys = append(missingKeys, key)
			}
		}
		keysRemaining = missingKeys

		// if retry attempts exhausted, return the error
		if numRetries >= bucket.spec.MaxNumRetries {
			return nil, fmt.Errorf("Giving up GoCB request for key: %v after %v retries", keys, numRetries)
		}

		// sleep before retrying
		Warn("Sleeping %v ms before retrying GoCB request for %v / %v keys remaining", timeToSleepMs, len(keysRemaining), totalNumKeys)

		<-time.After(time.Duration(timeToSleepMs) * time.Millisecond)

		// increase sleep time for next retry
		timeToSleepMs *= 2

		// increment retry counter
		numRetries += 1

	}

	// call getBulkRawOneOff(bucket, keys)

	/*
		resultsAccumulator := make(map[string][]byte, len(keys))

		worker := func() (finished bool, err error, value interface{}) {

			// do the underlying request
			result, err := getBulkRawOneOff(bucket, keys)

			if err != nil {

				// got an "overall error" -- eg, an error with the bulk operation,
				// as opposed to some keys missing from the result due to partial errors

				// if it's unrecoverable and not worth retrying, just return an error
				if !shouldRetryError(err) {
					return true, nil, err
				}

				// if we've already accumulated results from partial successes,
				// and now we're getting an overall error, abort, since this is not
				// taken into account in this code.
				if len(resultsAccumulator) > 0 {
					return true, nil, err
				}

				// otherwise, we should retry all of the keys
				return true, nil, nil

			} else {
				// no "overall error", but there may be partial errors

				// if no overall error and result contains all keys, then return result
				if len(result) == len(keys) {
					return true, nil, result
				}

				// save all the results we have so far, and retry with only the failed keys
				for key, val := range result {
					resultsAccumulator[key] = val
				}

				// find the failed keys by looping over all the keys, and only including
				// the keys *not* present in the result
				newKeys := []string{}
				for _, key := range keys {
					_, ok := result[key]
					if !ok {
						newKeys = append(newKeys, key)
					}
				}

			}

		}

		timeToSleepMs := bucket.spec.InitialRetrySleepTimeMS

		sleeper := func(numAttempts int) (bool, int) {
			if numAttempts > bucket.spec.MaxNumRetries {
				return false, -1
			}
			timeToSleepMs *= 2
			return true, timeToSleepMs
		}

		err, result := RetryLoop(worker, sleeper)
		return result.(map[string][]byte), err
	*/

	/*
		numRetries := 0
		timeToSleepMs := bucket.spec.InitialRetrySleepTimeMS

		for i := 0; i < bucket.spec.MaxNumRetries; i++ {

			// do the underlying request
			result, err := getBulkRawOneOff(bucket, keys)

			// if no error, return result
			if err == nil || !shouldRetryError(err) {
				return result, err
			}

			// if retry attempts exhausted, return the error
			if numRetries >= bucket.spec.MaxNumRetries {
				Warn("Giving up GoCB request after %v retries", numRetries)
				return nil, err
			}

			// sleep before retrying
			LogTo("gocb", "Sleeping %v ms before retrying GoCB request", timeToSleepMs)
			<-time.After(timeToSleepMs * time.Millisecond)

			// increase sleep time for next retry
			timeToSleepMs *= 2

			// increment retry counter
			numRetries += 1

		}
	*/

}

func shouldRetryError(err error) bool {
	// TODO!! Do a string comparison to see if it's "not found" and therefore
	// non-recoverable
	return true
}

func getBulkRawOneOff(bucket CouchbaseBucketGoCB, keys []string) (map[string][]byte, error) {

	gocbExpvars.Add("GetBulkRaw", 1)
	result := make(map[string][]byte)
	var items []gocb.BulkOp
	for _, key := range keys {
		var value []byte
		item := &gocb.GetOp{Key: key, Value: &value}
		items = append(items, item)
	}
	err := bucket.Do(items)
	if err != nil {
		return nil, err
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
				result[getOp.Key] = *byteValue
			}
		}
	}

	return result, nil

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
