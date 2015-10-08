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
	"fmt"
	"time"

	"github.com/couchbase/gocb"
	"github.com/couchbase/sg-bucket"
)

// Implementation of sgbucket.Bucket that talks to a Couchbase server and uses gocb
type CouchbaseBucketGoCB struct {
	*gocb.Bucket            // the underlying gocb bucket
	spec         BucketSpec // keep a copy of the BucketSpec for DCP usage
}

// Creates a Bucket that talks to a real live Couchbase server.
func GetCouchbaseBucketGoCB(spec BucketSpec) (bucket Bucket, err error) {

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

	// increase timeout, as I was seeing timeouts in the logs during
	// performance testing: https://gist.github.com/tleyden/7d2e2fd12927b9cee941
	goCBBucket.SetOperationTimeout(30000 * time.Millisecond)

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
	casGoCB, err := bucket.Bucket.Get(k, rv)
	return uint64(casGoCB), err
}

func (bucket CouchbaseBucketGoCB) GetRaw(k string) (rv []byte, cas uint64, err error) {
	var returnVal []byte
	casGoCB, err := bucket.Bucket.Get(k, &returnVal)
	return returnVal, uint64(casGoCB), err
}

func (bucket CouchbaseBucketGoCB) GetBulkRaw(keys []string) (map[string][]byte, error) {

	result := make(map[string][]byte)
	for _, key := range keys {
		getResult, _, err := bucket.GetRaw(key)
		if err == nil { // Ignore any docs with errors
			result[key] = getResult
		}
	}
	return result, nil

	// The code below is not working -- the gateload test does not pull any docs
	// when it is enabled

	/*

		var items []gocb.BulkOp
		for _, key := range keys {
			item := &gocb.GetOp{Key: key}
			items = append(items, item)
		}
		err := bucket.Do(items)
		if err != nil {
			return nil, err
		}

		result := make(map[string][]byte)
		for _, item := range items {
			getOp := item.(*gocb.GetOp)

			// Ignore any ops with errors.
			// NOTE: some of the errors are misleading:
			// https://issues.couchbase.com/browse/GOCBC-64
			if getOp.Err == nil {
				result[getOp.Key] = getOp.Value.([]byte)

			}
			result[getOp.Key] = getOp.Value.([]byte)
		}

		return result, nil
	*/

}

func (bucket CouchbaseBucketGoCB) GetAndTouchRaw(k string, exp int) (rv []byte, cas uint64, err error) {
	var returnVal []byte
	casGoCB, err := bucket.Bucket.GetAndTouch(k, uint32(exp), &returnVal)
	return returnVal, uint64(casGoCB), nil
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
	_, err := bucket.Bucket.Upsert(k, v, uint32(exp))
	return err
}

func (bucket CouchbaseBucketGoCB) SetRaw(k string, exp int, v []byte) error {
	_, err := bucket.Bucket.Upsert(k, v, uint32(exp))
	return err
}

func (bucket CouchbaseBucketGoCB) Delete(k string) error {
	LogPanic("Unimplemented method: Delete()")
	return nil
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
		newCas, err := bucket.Bucket.Insert(k, v, uint32(exp))
		return uint64(newCas), err
	}

	// Otherwise, replace existing value
	newCas, err := bucket.Bucket.Replace(k, v, gocb.Cas(cas), uint32(exp))

	return uint64(newCas), err

}

func (bucket CouchbaseBucketGoCB) Update(k string, exp int, callback sgbucket.UpdateFunc) error {

	maxCasRetries := 100000 // prevent infinite loop
	for i := 0; i < maxCasRetries; i++ {

		var bytes []byte
		var err error

		// Load the existing value.
		// NOTE: ignore error and assume it's a "key not found" error.  If it's a more
		// serious error, it will probably recur when calling other ops below
		cas, _ := bucket.Bucket.Get(k, &bytes)

		// Invoke callback to get updated value
		valueToInsert, err := callback(bytes)
		if err != nil {
			return err
		}

		if cas == 0 {
			// If the Get fails, the cas will be 0 and so call Insert().
			// If we get an error on the insert, due to a race, this will
			// go back through the cas loop
			_, err = bucket.Bucket.Insert(k, valueToInsert, uint32(exp))
		} else {
			if valueToInsert == nil {
				// In order to match the go-couchbase bucket behavior, if the
				// callback returns nil, we delete the doc
				_, err = bucket.Bucket.Remove(k, cas)
			} else {
				// Otherwise, attempt to do a replace.  won't succeed if
				// updated underneath us
				_, err = bucket.Bucket.Replace(k, valueToInsert, cas, uint32(exp))
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
	LogPanic("Unimplemented method: Incr()")
	return 0, nil
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
