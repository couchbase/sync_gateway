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
	"fmt"
	"log"

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
	goCBBucket, err := cluster.OpenBucket(spec.BucketName, "")
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
	LogPanic("Unimplemented method: GetName()")
	return ""
}

func (bucket CouchbaseBucketGoCB) Get(k string, rv interface{}) (cas uint64, err error) {
	casGoCB, err := bucket.Bucket.Get(k, rv)
	return uint64(casGoCB), err
}

func (bucket CouchbaseBucketGoCB) GetRaw(k string) (rv []byte, cas uint64, err error) {
	var returnVal interface{}

	log.Printf("GetRaw() called with: %v", k)

	casGoCB, err := bucket.Bucket.Get(k, &returnVal)
	if err != nil {
		log.Printf("GetRaw error calling Get: %v", err)
		return nil, 0, err
	}
	return returnVal.([]byte), uint64(casGoCB), nil

}

func (bucket CouchbaseBucketGoCB) GetBulkRaw(keys []string) (map[string][]byte, error) {

	result := make(map[string][]byte)
	for _, key := range keys {
		getResult, _, err := bucket.GetRaw(key)
		if err != nil {
			return nil, err
		}
		result[key] = getResult
	}
	return result, nil

	/*
	   The code below is not working, I'm getting this error:

	   2015/10/05 16:12:40 GetBulkRaw getOp item: &{bulkOp:{pendop:<nil>} Key:_idx:channel-0:block0:1jwu Value:<nil> Cas:0 Err:You must encode binary in a byte array or interface}

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
	   		log.Printf("GetBulkRaw item: %+v", item)
	   		getOp := item.(*gocb.GetOp)
	   		log.Printf("GetBulkRaw getOp item: %+v", getOp)
	   		if getOp.Err != nil {
	   			return nil, getOp.Err
	   		}
	   		result[getOp.Key] = getOp.Value.([]byte)
	   	}

	   	return result, nil
	*/

}

func (bucket CouchbaseBucketGoCB) GetAndTouchRaw(k string, exp int) (rv []byte, cas uint64, err error) {
	LogPanic("Unimplemented method: GetAndTouchRaw()")
	return nil, 0, nil
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

	// Try to insert the value into the bucket
	_, err = bucket.Bucket.Insert(k, v, uint32(exp))

	// If there was no error, we've successfully inserted so we're done
	if err == nil {
		return 0, nil
	}

	// Otherwise, replace existing value
	newCas, err := bucket.Bucket.Replace(k, v, gocb.Cas(cas), uint32(exp))

	return uint64(newCas), err

}

func (bucket CouchbaseBucketGoCB) Update(k string, exp int, callback sgbucket.UpdateFunc) error {

	// TODO: a bunch of the json marshalling/unmarshalling stuff is probably not needed

	// Call the callback() with an empty value, since we are first going to try to Insert()
	valueToInsert, err := callback(nil)
	if err != nil {
		return err
	}

	// Since callback() returns a []byte but Insert() expects an interface,
	// it's necessary to unmarshal the []byte into an interface
	var valueToInsertIface interface{}
	err = json.Unmarshal(valueToInsert, &valueToInsertIface)
	if err != nil {
		return err
	}

	// Try to insert the value into the bucket
	_, err = bucket.Bucket.Insert(k, valueToInsertIface, uint32(exp))

	// If there was no error, we've successfully inserted so we're done
	if err == nil {
		return nil
	}

	// Unable to insert, do a Replace CAS loop
	maxCasRetries := 100000 // prevent infinite loop
	for i := 0; i < maxCasRetries; i++ {

		// get the latest value and cas value
		cas, err := bucket.Bucket.Get(k, &valueToInsertIface)
		if err != nil {
			return err
		}

		// interface{} -> []byte
		val, err := json.Marshal(valueToInsertIface)
		if err != nil {
			return err
		}

		// invoke callback to get updated value
		valueToInsert, err = callback(val)
		if err != nil {
			return err
		}

		// []byte -> interface{}
		err = json.Unmarshal(valueToInsert, &valueToInsertIface)
		if err != nil {
			return err
		}

		// attempt to do a replace
		_, err = bucket.Bucket.Replace(k, valueToInsertIface, cas, uint32(exp))

		// if there was no error, we're done
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
