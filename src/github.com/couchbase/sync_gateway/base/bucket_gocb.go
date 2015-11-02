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

func (bucket CouchbaseBucketGoCB) GetBulkRaw(keys []string) (map[string][]byte, error) {

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
