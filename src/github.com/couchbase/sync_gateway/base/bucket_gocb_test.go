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
	"bytes"
	"fmt"
	"testing"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/go.assert"
)

func GetBucketOrPanic() Bucket {
	spec := BucketSpec{
		Server:     "http://localhost:8091",
		BucketName: "bucket-1",
	}
	bucket, err := GetCouchbaseBucketGoCB(spec)
	if err != nil {
		panic("Could not open bucket")
	}
	return bucket
}

func CouchbaseTestSetGetRaw(t *testing.T) {

	bucket := GetBucketOrPanic()

	key := "TestSetGetRaw"
	val := []byte("bar")

	_, _, err := bucket.GetRaw(key)
	if err == nil {
		t.Errorf("Key should not exist yet, expected error but got nil")
	}

	if err := bucket.SetRaw(key, 0, val); err != nil {
		t.Errorf("Error calling SetRaw(): %v", err)
	}

	rv, _, err := bucket.GetRaw(key)
	if string(rv) != string(val) {
		t.Errorf("%v != %v", string(rv), string(val))
	}

	err = bucket.Delete(key)
	if err != nil {
		t.Errorf("Error removing key from bucket")
	}

}

func CouchbaseTestBulkGetRaw(t *testing.T) {

	bucket := GetBucketOrPanic()

	keyPrefix := "TestBulkGetRaw"
	keySet := make([]string, 10)
	valueSet := make(map[string][]byte, 10)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%s%d", keyPrefix, i)
		val := []byte(fmt.Sprintf("bar%d", i))
		keySet[i] = key
		valueSet[key] = val

		_, _, err := bucket.GetRaw(key)
		if err == nil {
			t.Errorf("Key [%s] should not exist yet, expected error but got nil", key)
		}

		if err := bucket.SetRaw(key, 0, val); err != nil {
			t.Errorf("Error calling SetRaw(): %v", err)
		}
	}

	results, err := bucket.GetBulkRaw(keySet)
	assertNoError(t, err, fmt.Sprintf("Error calling GetBulkRaw(): %v", err))
	assert.True(t, len(results) == 10)

	// validate results, and prepare new keySet with non-existent keys
	mixedKeySet := make([]string, 20)
	for index, key := range keySet {
		// Verify value
		assert.True(t, bytes.Equal(results[key], valueSet[key]))
		mixedKeySet[2*index] = key
		mixedKeySet[2*index+1] = fmt.Sprintf("%s_invalid", key)
	}

	// Validate bulkGet that include non-existent keys work as expected
	mixedResults, err := bucket.GetBulkRaw(mixedKeySet)
	assertNoError(t, err, fmt.Sprintf("Error calling GetBulkRaw(): %v", err))
	assert.True(t, len(results) == 10)

	// Clean up
	for _, key := range keySet {
		// validate mixed results
		assert.True(t, bytes.Equal(mixedResults[key], valueSet[key]))
		// Delete key
		err = bucket.Delete(key)
		if err != nil {
			t.Errorf("Error removing key from bucket")
		}
	}

}

func CouchbaseTestWriteCas(t *testing.T) {

	bucket := GetBucketOrPanic()

	key := "TestWriteCas"
	val := []byte("bar2")

	_, _, err := bucket.GetRaw(key)
	if err == nil {
		t.Errorf("Key should not exist yet, expected error but got nil")
	}

	cas := uint64(0)

	cas, err = bucket.WriteCas(key, 0, 0, cas, []byte("bar"), sgbucket.Raw)
	if err != nil {
		t.Errorf("Error doing WriteCas: %v", err)
	}

	casOut, err := bucket.WriteCas(key, 0, 0, cas, val, sgbucket.Raw)
	if err != nil {
		t.Errorf("Error doing WriteCas: %v", err)
	}

	if casOut == cas {
		t.Errorf("Expected different casOut value")
	}

	rv, _, err := bucket.GetRaw(key)
	if string(rv) != string(val) {
		t.Errorf("%v != %v", string(rv), string(val))
	}

	err = bucket.Delete(key)
	if err != nil {
		t.Errorf("Error removing key from bucket")
	}

}

func CouchbaseTestSetBulk(t *testing.T) {

	bucket := GetBucketOrPanic()

	key := "TestSetBulk1"
	key2 := "TestSetBulk2"
	key3 := "TestSetBulk3"
	var returnVal interface{}

	// Cleanup
	defer func() {
		keys2del := []string{key, key2, key3}
		for _, key2del := range keys2del {
			err := bucket.Delete(key2del)
			if err != nil {
				t.Errorf("Error removing key from bucket")
			}

		}
	}()

	_, err := bucket.Get(key, &returnVal)
	if err == nil {
		t.Errorf("Key should not exist yet, expected error but got nil")
	}

	// Write a single key, get cas val: casStale
	casZero := uint64(0)
	casStale, err := bucket.WriteCas(key, 0, 0, casZero, "key-initial", sgbucket.Raw)
	if err != nil {
		t.Errorf("Error doing WriteCas: %v", err)
	}

	// Update that key so that casStale is now stale, get casFresh
	casUpdated, err := bucket.WriteCas(key, 0, 0, casStale, "key-updated", sgbucket.Raw)
	if err != nil {
		t.Errorf("Error doing WriteCas: %v", err)
	}

	// Do bulk set with a new key and the prev key with casStale
	entries := []*sgbucket.BulkSetEntry{}
	entries = append(entries, &sgbucket.BulkSetEntry{
		Key:   key,
		Value: "key-updated2",
		Cas:   casStale,
	})
	entries = append(entries, &sgbucket.BulkSetEntry{
		Key:   key2,
		Value: "key2-initial",
		Cas:   casZero,
	})

	err = bucket.SetBulk(entries)
	assert.True(t, err == nil)

	// Expect one error for the casStale key
	assert.Equals(t, numNonNilErrors(entries), 1)

	// Expect that the other key was correctly written
	_, err = bucket.Get(key2, &returnVal)
	assert.True(t, err == nil)
	assert.Equals(t, returnVal, "key2-initial")

	// Retry with bulk set with another new key and casFresh key
	entries = []*sgbucket.BulkSetEntry{}
	entries = append(entries, &sgbucket.BulkSetEntry{
		Key:   key,
		Value: "key-updated3",
		Cas:   casUpdated,
	})
	entries = append(entries, &sgbucket.BulkSetEntry{
		Key:   key3,
		Value: "key3-initial",
		Cas:   casZero,
	})

	err = bucket.SetBulk(entries)

	// Expect no errors
	assert.Equals(t, numNonNilErrors(entries), 0)

	// Make sure the original key that previously failed now works
	_, err = bucket.Get(key, &returnVal)
	assert.True(t, err == nil)
	assert.Equals(t, returnVal, "key-updated3")

}

func numNonNilErrors(entries []*sgbucket.BulkSetEntry) int {
	errorCount := 0
	for _, entry := range entries {
		if entry.Error != nil {
			errorCount += 1
		}
	}
	return errorCount
}

func CouchbaseTestUpdate(t *testing.T) {

	bucket := GetBucketOrPanic()

	key := "TestUpdate"
	valInitial := []byte("initial")
	valUpdated := []byte("updated")

	_, _, err := bucket.GetRaw(key)
	if err == nil {
		t.Errorf("Key should not exist yet, expected error but got nil")
	}

	updateFunc := func(current []byte) (updated []byte, err error) {
		if len(current) == 0 {
			return valInitial, nil
		} else {
			return valUpdated, nil
		}
	}

	err = bucket.Update(key, 0, updateFunc)
	if err != nil {
		t.Errorf("Error calling Update: %v", err)
	}

	rv, _, err := bucket.GetRaw(key)
	if string(rv) != string(valInitial) {
		t.Errorf("%v != %v", string(rv), string(valInitial))
	}

	err = bucket.Update(key, 0, updateFunc)
	if err != nil {
		t.Errorf("Error calling Update: %v", err)
	}

	rv, _, err = bucket.GetRaw(key)
	if string(rv) != string(valUpdated) {
		t.Errorf("%v != %v", string(rv), string(valUpdated))
	}

	err = bucket.Delete(key)
	if err != nil {
		t.Errorf("Error removing key from bucket")
	}

}

func CouchbaseTestIncrCounter(t *testing.T) {

	bucket := GetBucketOrPanic()
	key := "TestIncr"

	defer func() {
		err := bucket.Delete(key)
		if err != nil {
			t.Errorf("Error removing counter from bucket")
		}
	}()

	// New Counter - incr 1, default 1
	value, err := bucket.Incr(key, 1, 1, 0)
	if err != nil {
		t.Errorf("Error incrementing non-existent counter")
	}
	assert.Equals(t, value, uint64(1))

	// Retrieve an existing counter using delta=0
	retrieval, err := bucket.Incr(key, 0, 0, 0)
	if err != nil {
		t.Errorf("Error retrieving value for existing counter")
	}
	assert.Equals(t, retrieval, uint64(1))

	// Increment existing counter
	retrieval, err = bucket.Incr(key, 1, 1, 0)
	if err != nil {
		t.Errorf("Error incrementing value for existing counter")
	}
	assert.Equals(t, retrieval, uint64(2))

	// Attempt retrieval of a non-existent counter using delta=0
	retrieval, err = bucket.Incr("badkey", 0, 0, 0)
	if err == nil {
		t.Errorf("Attempt to retrieve non-existent counter should return error")
	}

}
