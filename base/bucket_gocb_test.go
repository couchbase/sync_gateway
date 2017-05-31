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
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"reflect"
	"time"

	"github.com/couchbase/gocb"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/go.assert"
	"gopkg.in/couchbase/gocbcore.v7"
)

// NOTE: most of these tests are disabled by default and have been renamed to Couchbase*
// because they depend on a running Couchbase server.  To run these tests, manually rename
// them to remove the Couchbase* prefix, and then rename them back before checking into
// Git.


func TestTranscoder(t *testing.T) {
	transcoder := SGTranscoder{}

	jsonBody := `{"a":1}`
	jsonBytes := []byte(jsonBody)
	binaryFlags := gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression)
	jsonFlags := gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)

	resultBytes, flags, err := transcoder.Encode([]byte(jsonBody))
	assert.Equals(t, bytes.Compare(resultBytes, jsonBytes), 0)
	assert.Equals(t, flags, jsonFlags)
	assert.Equals(t, err, nil)

	resultBytes, flags, err = transcoder.Encode(BinaryDocument(jsonBody))
	assert.Equals(t, bytes.Compare(resultBytes, jsonBytes), 0)
	assert.Equals(t, flags, binaryFlags)
	assert.Equals(t, err, nil)
}

func TestSetGetRaw(t *testing.T) {

	bucket := GetBucketOrPanic()

	key := "TestSetGetRaw2"
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

func TestAddRaw(t *testing.T) {

	bucket := GetBucketOrPanic()

	key := "TestAddRaw"
	val := []byte("bar")

	_, _, err := bucket.GetRaw(key)
	if err == nil {
		t.Errorf("Key should not exist yet, expected error but got nil")
	}

	added, err := bucket.AddRaw(key, 0, val)
	if err != nil {
		t.Errorf("Error calling AddRaw(): %v", err)
	}
	assertTrue(t, added, "AddRaw returned added=false, expected true")

	rv, _, err := bucket.GetRaw(key)
	if string(rv) != string(val) {
		t.Errorf("%v != %v", string(rv), string(val))
	}

	// Calling AddRaw for existing value should return added=false, no error
	added, err = bucket.AddRaw(key, 0, val)
	if err != nil {
		t.Errorf("Error calling AddRaw(): %v", err)
	}
	assertTrue(t, added == false, "AddRaw returned added=true for duplicate, expected false")

	err = bucket.Delete(key)
	if err != nil {
		t.Errorf("Error removing key from bucket")
	}

}

func TestBulkGetRaw(t *testing.T) {

	bucket := GetBucketOrPanic()

	keyPrefix := "TestBulkGetRaw"
	keySet := make([]string, 1000)
	valueSet := make(map[string][]byte, 1000)

	defer func() {
		// Clean up
		for _, key := range keySet {
			// Delete key
			err := bucket.Delete(key)
			if err != nil {
				t.Errorf("Error removing key from bucket")
			}
		}

	}()

	for i := 0; i < 1000; i++ {

		key := fmt.Sprintf("%s%d", keyPrefix, i)
		val := []byte(fmt.Sprintf("bar%d", i))
		keySet[i] = key
		valueSet[key] = val

		_, _, err := bucket.GetRaw(key)
		if err == nil {
			t.Errorf("Key [%s] should not exist yet, expected error but didn't get one.", key)
		}

		if err := bucket.SetRaw(key, 0, val); err != nil {
			t.Errorf("Error calling SetRaw(): %v", err)
		}
	}

	results, err := bucket.GetBulkRaw(keySet)
	assertNoError(t, err, fmt.Sprintf("Error calling GetBulkRaw(): %v", err))
	assert.True(t, len(results) == 1000)

	// validate results, and prepare new keySet with non-existent keys
	mixedKeySet := make([]string, 2000)
	for index, key := range keySet {
		// Verify value
		assert.True(t, bytes.Equal(results[key], valueSet[key]))
		mixedKeySet[2*index] = key
		mixedKeySet[2*index+1] = fmt.Sprintf("%s_invalid", key)
	}

	// Validate bulkGet that include non-existent keys work as expected
	mixedResults, err := bucket.GetBulkRaw(mixedKeySet)
	assertNoError(t, err, fmt.Sprintf("Error calling GetBulkRaw(): %v", err))
	assert.True(t, len(results) == 1000)

	for _, key := range keySet {
		// validate mixed results
		assert.True(t, bytes.Equal(mixedResults[key], valueSet[key]))
	}

	// if passed all non-existent keys, should return an empty map
	nonExistentKeySet := make([]string, 1000)
	for index, key := range keySet {
		nonExistentKeySet[index] = fmt.Sprintf("%s_invalid", key)
	}
	emptyResults, err := bucket.GetBulkRaw(nonExistentKeySet)
	assertNoError(t, err, fmt.Sprintf("Unexpected error calling GetBulkRaw(): %v", err))
	assert.False(t, emptyResults == nil)
	assert.True(t, len(emptyResults) == 0)

}

func TestWriteCasBasic(t *testing.T) {

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

func TestWriteCasAdvanced(t *testing.T) {

	bucket := GetBucketOrPanic()

	key := "TestWriteCas"

	_, _, err := bucket.GetRaw(key)
	if err == nil {
		t.Errorf("Key should not exist yet, expected error but got nil")
	}

	casZero := uint64(0)

	// write doc to bucket, giving cas value of 0

	_, err = bucket.WriteCas(key, 0, 0, casZero, []byte("bar"), sgbucket.Raw)
	if err != nil {
		t.Errorf("Error doing WriteCas: %v", err)
	}

	// try to write doc to bucket, giving cas value of 0 again -- exepct a failure
	secondWriteCas, err := bucket.WriteCas(key, 0, 0, casZero, []byte("bar"), sgbucket.Raw)
	assert.True(t, err != nil)

	// try to write doc to bucket again, giving invalid cas value -- expect a failure
	// also, expect no retries, however there is currently no easy way to detect that.
	_, err = bucket.WriteCas(key, 0, 0, secondWriteCas-1, []byte("bar"), sgbucket.Raw)
	assert.True(t, err != nil)

	err = bucket.Delete(key)
	if err != nil {
		t.Errorf("Error removing key from bucket")
	}

}

// When enabling this test, you should also uncomment the code in isRecoverableGoCBError()
func TestSetBulk(t *testing.T) {


	// Might be failing due to something related to this comment:
	//    When enabling this test, you should also uncomment the code in isRecoverableGoCBError()
	// However, there's no commented code in isRecoverableGoCBError()
	t.Skip("TestSetBulk is currently failing against both walrus and couchbase server.  Error logs: https://gist.github.com/tleyden/22d69ff9e627d7ad37043200614a3cc5")

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

	doSingleFakeRecoverableGOCBError = true
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

	doSingleFakeRecoverableGOCBError = true
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

func TestUpdate(t *testing.T) {

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

func TestIncrCounter(t *testing.T) {

	t.Skip("Currently failing: under walrus, go-couchbase and gocb, this test fails with: Attempt to retrieve non-existent counter should return error")

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

func TestGetAndTouchRaw(t *testing.T) {

	// There's no easy way to validate the expiry time of a doc (that I know of)
	// so this is just a smoke test

	key := "TestGetAndTouchRaw"
	val := []byte("bar")

	bucket := GetBucketOrPanic()

	defer func() {
		err := bucket.Delete(key)
		if err != nil {
			t.Errorf("Error removing key from bucket")
		}

	}()

	_, _, err := bucket.GetRaw(key)
	if err == nil {
		t.Errorf("Key should not exist yet, expected error but got nil")
	}

	if err := bucket.SetRaw(key, 0, val); err != nil {
		t.Errorf("Error calling SetRaw(): %v", err)
	}

	rv, _, err := bucket.GetRaw(key)
	assert.True(t, err == nil)
	if string(rv) != string(val) {
		t.Errorf("%v != %v", string(rv), string(val))
	}

	rv, _, err = bucket.GetAndTouchRaw(key, 1)
	if err != nil {
		log.Printf("Error calling GetAndTouchRaw: %v", err)
	}

	assert.True(t, err == nil)
	assert.Equals(t, len(rv), len(val))

}

func TestCreateBatchesEntries(t *testing.T) {

	entries := []*sgbucket.BulkSetEntry{
		{
			Key: "one",
		},
		{
			Key: "two",
		},
		{
			Key: "three",
		},
		{
			Key: "four",
		},
		{
			Key: "five",
		},
		{
			Key: "six",
		},
		{
			Key: "seven",
		},
	}

	batchSize := uint(2)
	batches := createBatchesEntries(batchSize, entries)
	log.Printf("batches: %+v", batches)
	assert.Equals(t, len(batches), 4)
	assert.Equals(t, batches[0][0].Key, "one")
	assert.Equals(t, batches[0][1].Key, "two")
	assert.Equals(t, batches[1][0].Key, "three")
	assert.Equals(t, batches[1][1].Key, "four")
	assert.Equals(t, batches[2][0].Key, "five")
	assert.Equals(t, batches[2][1].Key, "six")
	assert.Equals(t, batches[3][0].Key, "seven")
}

func TestCreateBatchesKeys(t *testing.T) {
	keys := []string{"one", "two", "three", "four", "five", "six", "seven"}
	batchSize := uint(2)
	batches := createBatchesKeys(batchSize, keys)
	log.Printf("batches: %+v", batches)
	assert.Equals(t, len(batches), 4)
	assert.Equals(t, batches[0][0], "one")
	assert.Equals(t, batches[0][1], "two")
	assert.Equals(t, batches[1][0], "three")
	assert.Equals(t, batches[1][1], "four")
	assert.Equals(t, batches[2][0], "five")
	assert.Equals(t, batches[2][1], "six")
	assert.Equals(t, batches[3][0], "seven")
}

func SkipXattrTestsIfNotEnabled(t *testing.T) {

	if !TestUseXattrs() {
		t.Skip("XATTR based tests not enabled.  Enable via SG_TEST_USE_XATTRS=true environment variable")
	}

	if UnitTestUrlIsWalrus() {
		t.Skip("This test won't work under walrus until https://github.com/couchbase/sync_gateway/issues/2390")
	}
}

// TestWriteCasXATTR.  Validates basic write of document with xattr, and retrieval of the same doc w/ xattr.
func TestWriteCasXattrSimple(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	bucket := GetBucketOrPanic()

	key := "TestWriteCasXATTRSimple"
	xattrName := "_sync"
	val := make(map[string]interface{})
	val["body_field"] = "1234"

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = float64(123)
	xattrVal["rev"] = "1-1234"

	var existsVal map[string]interface{}
	_, err := bucket.Get(key, existsVal)
	if err == nil {
		log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
		err = bucket.DeleteWithXattr(key, xattrName)
	}

	cas := uint64(0)
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, val, xattrVal)
	assertNoError(t, err, "WriteCasWithXattr error")
	log.Printf("Post-write, cas is %d", cas)

	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	getCas, err := bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	// TODO: Cas check fails, pending xattr code to make it to gocb master
	log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
	assert.Equals(t, getCas, cas)
	assert.Equals(t, retrievedVal["body_field"], val["body_field"])
	assert.Equals(t, retrievedXattr["seq"], xattrVal["seq"])
	assert.Equals(t, retrievedXattr["rev"], xattrVal["rev"])
}

// TestWriteCasXATTR.  Validates basic write of document with xattr,  retrieval of the same doc w/ xattr, update of the doc w/ xattr, retrieval of the doc w/ xattr.
func TestWriteCasXattrUpsert(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	b := GetBucketOrPanic()
	bucket, ok := b.(*CouchbaseBucketGoCB)
	if !ok {
		log.Printf("Can't cast to bucket")
		return
	}
	bucket.SetTranscoder(SGTranscoder{})

	key := "TestWriteCasXATTRUpsert"
	xattrName := "_sync"
	val := make(map[string]interface{})
	val["body_field"] = "1234"

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = float64(123)
	xattrVal["rev"] = "1-1234"

	var existsVal map[string]interface{}
	_, err := bucket.Get(key, existsVal)
	if err == nil {
		log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
		err = bucket.DeleteWithXattr(key, xattrName)
	}

	cas := uint64(0)
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, val, xattrVal)
	assertNoError(t, err, "WriteCasWithXattr error")
	log.Printf("Post-write, cas is %d", cas)

	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	getCas, err := bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	// TODO: Cas check fails, pending xattr code to make it to gocb master
	log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
	assert.Equals(t, getCas, cas)
	assert.Equals(t, retrievedVal["body_field"], val["body_field"])
	assert.Equals(t, retrievedXattr["seq"], xattrVal["seq"])
	assert.Equals(t, retrievedXattr["rev"], xattrVal["rev"])

	val2 := make(map[string]interface{})
	val2["body_field"] = "5678"
	xattrVal2 := make(map[string]interface{})
	xattrVal2["seq"] = float64(124)
	xattrVal2["rev"] = "2-5678"
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, getCas, val2, xattrVal2)
	assertNoError(t, err, "WriteCasWithXattr error")
	log.Printf("Post-write, cas is %d", cas)

	var retrievedVal2 map[string]interface{}
	var retrievedXattr2 map[string]interface{}
	getCas, err = bucket.GetWithXattr(key, xattrName, &retrievedVal2, &retrievedXattr2)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	// TODO: Cas check fails, pending xattr code to make it to gocb master
	log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal2, retrievedXattr2)
	assert.Equals(t, getCas, cas)
	assert.Equals(t, retrievedVal2["body_field"], val2["body_field"])
	assert.Equals(t, retrievedXattr2["seq"], xattrVal2["seq"])
	assert.Equals(t, retrievedXattr2["rev"], xattrVal2["rev"])

}

// TestWriteCasXATTRRaw.  Validates basic write of document and xattr as raw bytes.
func TestWriteCasXattrRaw(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	b := GetBucketOrPanic()
	bucket, ok := b.(*CouchbaseBucketGoCB)
	if !ok {
		log.Printf("Can't cast to bucket")
		return
	}
	bucket.SetTranscoder(SGTranscoder{})

	key := "TestWriteCasXattrRaw"
	xattrName := "_sync"
	val := make(map[string]interface{})
	val["body_field"] = "1234"
	valRaw, _ := json.Marshal(val)

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = float64(123)
	xattrVal["rev"] = "1-1234"
	xattrValRaw, _ := json.Marshal(xattrVal)

	var existsVal map[string]interface{}
	_, err := bucket.Get(key, existsVal)
	if err == nil {
		log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
		err = bucket.DeleteWithXattr(key, xattrName)
	}

	cas := uint64(0)
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, valRaw, xattrValRaw)
	if err != nil {
		t.Errorf("Error doing WriteCasWithXattr: %+v", err)
	}

	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	getCas, err := bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	// TODO: Fails until https://issues.couchbase.com/browse/GOCBC-183 is available
	log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
	assert.Equals(t, getCas, cas)
	assert.Equals(t, retrievedVal["body_field"], val["body_field"])
	assert.Equals(t, retrievedXattr["seq"], xattrVal["seq"])
	assert.Equals(t, retrievedXattr["rev"], xattrVal["rev"])
}

// TestWriteCasTombstoneResurrect.  Verifies writing a new document body and xattr to a logically deleted document (xattr still exists)
// TODO: This fails with key not found trying to do a CAS-safe rewrite of the doc.  Updating the doc via subdoc (with access_deleted) is
// expected to work - need to retry when GOCBC-181 is available.
func TestWriteCasXattrTombstoneResurrect(t *testing.T) {

	t.Skip("Test fails with errors: https://gist.github.com/tleyden/c64bc7c473c74e241a2a05f138c8be6e.  Needs investigation")

	SkipXattrTestsIfNotEnabled(t)

	b := GetBucketOrPanic()
	bucket, ok := b.(*CouchbaseBucketGoCB)
	if !ok {
		log.Printf("Can't cast to bucket")
		return
	}
	bucket.SetTranscoder(SGTranscoder{})

	key := "TestWriteCasXattrTombstoneResurrect"
	xattrName := "_sync"
	val := make(map[string]interface{})
	val["body_field"] = "1234"

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = float64(123)
	xattrVal["rev"] = "1-1234"

	var existsVal map[string]interface{}
	_, err := bucket.Get(key, existsVal)
	if err == nil {
		log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
		err = bucket.DeleteWithXattr(key, xattrName)
	}

	// Write document with xattr
	cas := uint64(0)
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, val, xattrVal)
	if err != nil {
		t.Errorf("Error doing WriteCasWithXattr: %+v", err)
	}
	log.Printf("Post-write, cas is %d", cas)

	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	getCas, err := bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	// TODO: Cas check fails, pending xattr code to make it to gocb master
	log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
	assert.Equals(t, getCas, cas)
	assert.Equals(t, retrievedVal["body_field"], val["body_field"])
	assert.Equals(t, retrievedXattr["seq"], xattrVal["seq"])
	assert.Equals(t, retrievedXattr["rev"], xattrVal["rev"])

	// Delete the body (retains xattr)
	err = bucket.Delete(key)
	if err != nil {
		t.Errorf("Error doing Delete: %+v", err)
	}

	// Update the doc and xattr
	val = make(map[string]interface{})
	val["body_field"] = "5678"
	xattrVal = make(map[string]interface{})
	xattrVal["seq"] = float64(456)
	xattrVal["rev"] = "2-2345"
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, val, xattrVal)
	if err != nil {
		t.Errorf("Error doing WriteCasWithXattr: %+v", err)
	}

	// Verify retrieval
	getCas, err = bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	// TODO: Cas check fails, pending xattr code to make it to gocb master
	log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
	assert.Equals(t, retrievedVal["body_field"], val["body_field"])
	assert.Equals(t, retrievedXattr["seq"], xattrVal["seq"])
	assert.Equals(t, retrievedXattr["rev"], xattrVal["rev"])

}

// TestWriteCasXATTRDeleted.  Validates update of xattr on logically deleted document.
func TestWriteCasXattrTombstoneXattrUpdate(t *testing.T) {

	t.Skip("Test fails with errors: https://gist.github.com/tleyden/d261fe2b92bdaaa6e78f9f1c00fdfd58.  Needs investigation")


	SkipXattrTestsIfNotEnabled(t)

	b := GetBucketOrPanic()
	bucket, ok := b.(*CouchbaseBucketGoCB)
	if !ok {
		log.Printf("Can't cast to bucket")
		return
	}
	bucket.SetTranscoder(SGTranscoder{})

	key := "TestWriteCasXattrTombstoneXattrUpdate"
	xattrName := "_sync"
	val := make(map[string]interface{})
	val["body_field"] = "1234"

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = float64(123)
	xattrVal["rev"] = "1-1234"

	var existsVal map[string]interface{}
	_, err := bucket.Get(key, existsVal)
	if err == nil {
		log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
		err = bucket.DeleteWithXattr(key, xattrName)
	}

	// Write document with xattr
	cas := uint64(0)
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, val, xattrVal)
	if err != nil {
		t.Errorf("Error doing WriteCasWithXattr: %+v", err)
	}
	log.Printf("Wrote document")
	log.Printf("Post-write, cas is %d", cas)

	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	getCas, err := bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	log.Printf("Retrieved document")
	// TODO: Cas check fails, pending xattr code to make it to gocb master
	log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
	assert.Equals(t, getCas, cas)
	assert.Equals(t, retrievedVal["body_field"], val["body_field"])
	assert.Equals(t, retrievedXattr["seq"], xattrVal["seq"])
	assert.Equals(t, retrievedXattr["rev"], xattrVal["rev"])

	err = bucket.Delete(key)
	if err != nil {
		t.Errorf("Error doing Delete: %+v", err)
	}

	log.Printf("Deleted document")
	// Update the xattr
	xattrVal = make(map[string]interface{})
	xattrVal["seq"] = float64(456)
	xattrVal["rev"] = "2-2345"
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, xattrVal)
	if err != nil {
		t.Errorf("Error doing WriteCasWithXattr: %+v", err)
	}

	log.Printf("Updated tombstoned document")
	// Verify retrieval
	var modifiedVal map[string]interface{}
	var modifiedXattr map[string]interface{}
	getCas, err = bucket.GetWithXattr(key, xattrName, &modifiedVal, &modifiedXattr)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	log.Printf("Retrieved tombstoned document")
	// TODO: Cas check fails, pending xattr code to make it to gocb master
	log.Printf("TestWriteCasXATTR retrieved modified: %s, %s", modifiedVal, modifiedXattr)
	assert.Equals(t, modifiedXattr["seq"], xattrVal["seq"])
	assert.Equals(t, modifiedXattr["rev"], xattrVal["rev"])

}

// TestWriteUpdateXATTR.  Validates basic write of document with xattr, and retrieval of the same doc w/ xattr.
func TestWriteUpdateXattr(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	b := GetBucketOrPanic()
	bucket, ok := b.(*CouchbaseBucketGoCB)
	if !ok {
		log.Printf("Can't cast to bucket")
		return
	}
	bucket.SetTranscoder(SGTranscoder{})

	key := "TestWriteUpdateXATTR"
	xattrName := "_sync"
	val := make(map[string]interface{})
	val["counter"] = float64(1)

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = float64(1)
	xattrVal["rev"] = "1-1234"

	var existsVal map[string]interface{}
	var existsXattr map[string]interface{}
	_, err := bucket.GetWithXattr(key, xattrName, &existsVal, &existsXattr)
	if err == nil {
		log.Printf("Key should not exist yet, but get succeeded.  Doing cleanup, assuming couchbase bucket testing")
		err := bucket.DeleteWithXattr(key, xattrName)
		if err != nil {
			log.Printf("Got error trying to do pre-test cleanup:%v", err)
		}
	}

	// Dummy write update function that increments 'counter' in the doc and 'seq' in the xattr
	writeUpdateFunc := func(doc []byte, xattr []byte, cas uint64) (updatedDoc []byte, updatedXattr []byte, isDelete bool, err error) {

		var docMap map[string]interface{}
		var xattrMap map[string]interface{}
		// Marshal the doc
		if len(doc) > 0 {
			err = json.Unmarshal(doc, &docMap)
			if err != nil {
				return nil, nil, false, fmt.Errorf("Unable to unmarshal incoming doc: %v", err)
			}
		} else {
			// No incoming doc, treat as insert.
			docMap = make(map[string]interface{})
		}

		// Marshal the xattr
		if len(xattr) > 0 {
			err = json.Unmarshal(xattr, &xattrMap)
			if err != nil {
				return nil, nil, false, fmt.Errorf("Unable to unmarshal incoming xattr: %v", err)
			}
		} else {
			// No incoming xattr, treat as insert.
			xattrMap = make(map[string]interface{})
		}

		// Update the doc
		existingCounter, ok := docMap["counter"].(float64)
		if ok {
			docMap["counter"] = existingCounter + float64(1)
		} else {
			docMap["counter"] = float64(1)
		}

		// Update the xattr
		existingSeq, ok := xattrMap["seq"].(float64)
		if ok {
			xattrMap["seq"] = existingSeq + float64(1)
		} else {
			xattrMap["seq"] = float64(1)
		}

		updatedDoc, _ = json.Marshal(docMap)
		updatedXattr, _ = json.Marshal(xattrMap)
		return updatedDoc, updatedXattr, false, nil
	}

	// Insert
	_, err = bucket.WriteUpdateWithXattr(key, xattrName, 0, writeUpdateFunc)
	if err != nil {
		t.Errorf("Error doing WriteUpdateWithXattr: %+v", err)
	}

	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	_, err = bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	log.Printf("Retrieval after WriteUpdate insert: doc: %v, xattr: %v", retrievedVal, retrievedXattr)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	assert.Equals(t, retrievedVal["counter"], float64(1))
	assert.Equals(t, retrievedXattr["seq"], float64(1))

	// Update
	_, err = bucket.WriteUpdateWithXattr(key, xattrName, 0, writeUpdateFunc)
	if err != nil {
		t.Errorf("Error doing WriteUpdateWithXattr: %+v", err)
	}
	_, err = bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	log.Printf("Retrieval after WriteUpdate update: doc: %v, xattr: %v", retrievedVal, retrievedXattr)

	assert.Equals(t, retrievedVal["counter"], float64(2))
	assert.Equals(t, retrievedXattr["seq"], float64(2))

}

// TestDeleteDocumentHavingXATTR.  Delete document that has a system xattr.  System XATTR should be retained and retrievable.
func TestDeleteDocumentHavingXattr(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	b := GetBucketOrPanic()
	bucket, ok := b.(*CouchbaseBucketGoCB)
	if !ok {
		log.Printf("Can't cast to bucket")
		return
	}

	// Create document with XATTR
	xattrName := "_sync"
	val := make(map[string]interface{})
	val["body_field"] = "1234"

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 123
	xattrVal["rev"] = "1-1234"

	key := "TestDeleteDocumentHavingXATTR"
	_, _, err := bucket.GetRaw(key)
	if err == nil {
		log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
		bucket.Delete(key)
	}

	// Create w/ XATTR, delete doc and XATTR, retrieve doc (expect fail), retrieve XATTR (expect success)
	cas := uint64(0)
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, val, xattrVal)
	if err != nil {
		t.Errorf("Error doing WriteCasWithXattr: %+v", err)
	}

	// Delete the document.
	err = bucket.Delete(key)
	if err != nil {
		t.Errorf("Error doing Delete: %+v", err)
	}

	// Verify delete of body was successful, retrieve XATTR
	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	_, err = bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	assert.Equals(t, len(retrievedVal), 0)
	assert.Equals(t, retrievedXattr["seq"], float64(123))

}

// TestDeleteDocumentUpdateXATTR.  Delete document that has a system xattr along with an xattr update.
func TestDeleteDocumentUpdateXattr(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	b := GetBucketOrPanic()
	bucket, ok := b.(*CouchbaseBucketGoCB)
	if !ok {
		log.Printf("Can't cast to bucket")
		return
	}

	// Create document with XATTR
	xattrName := "_sync"
	val := make(map[string]interface{})
	val["body_field"] = "1234"

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 1
	xattrVal["rev"] = "1-1234"

	key := "TestDeleteDocumentHavingXATTR"
	_, _, err := bucket.GetRaw(key)
	if err == nil {
		log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
		bucket.Delete(key)
	}

	// Create w/ XATTR, delete doc and XATTR, retrieve doc (expect fail), retrieve XATTR (expect success)
	cas := uint64(0)
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, val, xattrVal)
	if err != nil {
		t.Errorf("Error doing WriteCasWithXattr: %+v", err)
	}

	// Delete the document.
	err = bucket.Delete(key)
	if err != nil {
		t.Errorf("Error doing Delete: %+v", err)
	}

	// Verify delete of body was successful, retrieve XATTR
	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	getCas, err := bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	if err != nil {
		t.Errorf("Error doing GetWithXattr: %+v", err)
	}
	assert.Equals(t, len(retrievedVal), 0)
	assert.Equals(t, retrievedXattr["seq"], float64(1))
	log.Printf("Post-delete xattr (1): %s", retrievedXattr)
	log.Printf("Post-delete cas (1): %x", getCas)

	// Update the xattr only
	xattrVal["seq"] = 2
	xattrVal["rev"] = "1-1234"
	casOut, writeErr := bucket.WriteCasWithXattr(key, xattrName, 0, getCas, nil, xattrVal)
	assertNoError(t, writeErr, "Error updating xattr post-delete")
	log.Printf("WriteCasWithXattr cas: %d", casOut)

	// Retrieve the document, validate cas values
	var postDeleteVal map[string]interface{}
	var postDeleteXattr map[string]interface{}
	getCas2, err := bucket.GetWithXattr(key, xattrName, &postDeleteVal, &postDeleteXattr)
	assertNoError(t, err, "Error getting document post-delete")
	assert.Equals(t, postDeleteXattr["seq"], float64(2))
	assert.Equals(t, len(postDeleteVal), 0)
	log.Printf("Post-delete xattr (2): %s", postDeleteXattr)
	log.Printf("Post-delete cas (2): %x", getCas2)

}

// TestDeleteDocumentAndXATTR.  Delete document and XATTR, ensure it's not available
func TestDeleteDocumentAndXATTR(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	b := GetBucketOrPanic()
	bucket, ok := b.(*CouchbaseBucketGoCB)
	if !ok {
		log.Printf("Can't cast to bucket")
		return
	}

	// Create document with XATTR
	xattrName := "_sync"
	val := make(map[string]interface{})
	val["body_field"] = "1234"

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 123
	xattrVal["rev"] = "1-1234"

	key := "TestDeleteDocumentAndXATTR"
	_, _, err := bucket.GetRaw(key)
	if err == nil {
		log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
		bucket.Delete(key)
	}

	// Create w/ XATTR, delete doc and XATTR, retrieve doc (expect fail), retrieve XATTR (expect fail)

	cas := uint64(0)
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, val, xattrVal)
	if err != nil {
		t.Errorf("Error doing WriteCasWithXattr: %+v", err)
	}

	// Delete the document and XATTR.
	err = bucket.DeleteWithXattr(key, xattrName)
	if err != nil {
		t.Errorf("Error doing DeleteWithXATTR: %+v", err)
	}

	// Verify delete of body and XATTR
	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	_, err = bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	assert.Equals(t, err, gocbcore.ErrKeyNotFound)

}

// TestDeleteDocumentAndUpdateXATTR.  Delete the document body and update the xattr.  Pending https://issues.couchbase.com/browse/MB-24098
func TestDeleteDocumentAndUpdateXATTR(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	b := GetBucketOrPanic()
	bucket, ok := b.(*CouchbaseBucketGoCB)
	if !ok {
		log.Printf("Can't cast to bucket")
		return
	}

	// Create document with XATTR
	xattrName := "_sync"
	val := make(map[string]interface{})
	val["body_field"] = "1234"

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 123
	xattrVal["rev"] = "1-1234"

	key := "TestDeleteDocumentAndUpdateXATTR_2"
	_, _, err := bucket.GetRaw(key)
	if err == nil {
		log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
		bucket.DeleteWithXattr(key, xattrName)
	}

	// Create w/ XATTR, delete doc and XATTR, retrieve doc (expect fail), retrieve XATTR (expect fail)
	cas := uint64(0)
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, val, xattrVal)
	if err != nil {
		t.Errorf("Error doing WriteCasWithXattr: %+v", err)
	}

	_, mutateErr := bucket.Bucket.MutateInEx(key, gocb.SubdocDocFlagReplaceDoc&gocb.SubdocDocFlagAccessDeleted, gocb.Cas(cas), uint32(0)).
		UpsertEx(xattrName, xattrVal, gocb.SubdocFlagXattr).                                     // Update the xattr
		UpsertEx("_sync.cas", "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros). // Stamp the cas on the xattr
		RemoveEx("", gocb.SubdocFlagNone).                                                       // Delete the document body
		Execute()

	log.Printf("MutateInEx error: %v", mutateErr)
	// Verify delete of body and XATTR
	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	mutateCas, err := bucket.GetWithXattr(key, xattrName, &retrievedVal, &retrievedXattr)
	assert.Equals(t, len(retrievedVal), 0)
	assert.Equals(t, retrievedXattr["seq"], float64(123))
	log.Printf("value: %v, xattr: %v", retrievedVal, retrievedXattr)
	log.Printf("MutateInEx cas: %v", mutateCas)

}

// CouchbaseTestRetrieveDocumentAndXattr.  Pending https://issues.couchbase.com/browse/MB-24152
func TestRetrieveDocumentAndXattr(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	b := GetBucketOrPanic()
	bucket, ok := b.(*CouchbaseBucketGoCB)
	if !ok {
		log.Printf("Can't cast to bucket")
		return
	}

	// 1. Create document with XATTR
	val := make(map[string]interface{})
	val["type"] = "docExistsXattrExists"

	xattrName := "_sync"
	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 123
	xattrVal["rev"] = "1-1234"

	key1 := "DocExistsXattrExists"
	key2 := "DocExistsNoXattr"
	key3 := "XattrExistsNoDoc"
	key4 := "NoDocNoXattr"
	var err error

	// Create w/ XATTR
	cas := uint64(0)
	cas, err = bucket.WriteCasWithXattr(key1, xattrName, 0, cas, val, xattrVal)
	if err != nil {
		t.Errorf("Error doing WriteCasWithXattr: %+v", err)
	}

	// 2. Create document with no XATTR
	val = make(map[string]interface{})
	val["type"] = "DocExistsNoXattr"
	_, err = bucket.Add(key2, 0, val)

	// 3. Xattr, no document
	val = make(map[string]interface{})
	val["type"] = "xattrExistsNoDoc"

	xattrVal = make(map[string]interface{})
	xattrVal["seq"] = 456
	xattrVal["rev"] = "1-1234"

	// Create w/ XATTR
	cas = uint64(0)
	cas, err = bucket.WriteCasWithXattr(key3, xattrName, 0, cas, val, xattrVal)
	if err != nil {
		t.Errorf("Error doing WriteCasWithXattr: %+v", err)
	}
	// Delete the doc
	bucket.Delete(key3)

	// 4. No xattr, no document

	// Attempt to retrieve all 4 docs
	res1, key1err := bucket.Bucket.LookupInEx(key1, gocb.SubdocDocFlagAccessDeleted).
		GetEx(xattrName, gocb.SubdocFlagXattr). // Get the xattr
		GetEx("", gocb.SubdocFlagNone).         // Get the document body
		Execute()
	log.Printf("key1err: %v", key1err)
	log.Printf("key1res: %+v", res1)

	time.Sleep(100 * time.Millisecond)
	res2, key2err := bucket.Bucket.LookupInEx(key2, gocb.SubdocDocFlagAccessDeleted).
		GetEx(xattrName, gocb.SubdocFlagXattr). // Get the xattr
		GetEx("", gocb.SubdocFlagNone).         // Get the document body
		Execute()
	log.Printf("key2err: %v", key2err)
	log.Printf("key2res: %+v", res2)

	time.Sleep(100 * time.Millisecond)
	res3, key3err := bucket.Bucket.LookupInEx(key3, gocb.SubdocDocFlagAccessDeleted).
		GetEx(xattrName, gocb.SubdocFlagXattr). // Get the xattr
		GetEx("", gocb.SubdocFlagNone).         // Get the document body
		Execute()
	log.Printf("key3err: %v", key3err)
	log.Printf("key3res: %+v", res3)

	time.Sleep(100 * time.Millisecond)
	res4, key4err := bucket.Bucket.LookupInEx(key4, gocb.SubdocDocFlagAccessDeleted).
		GetEx(xattrName, gocb.SubdocFlagXattr). // Get the xattr
		GetEx("", gocb.SubdocFlagNone).         // Get the document body
		Execute()
	log.Printf("key4err: %v", key4err)
	log.Printf("key4res: %+v", res4)

}
func TestApplyViewQueryOptions(t *testing.T) {

	// ------------------- Inline Helper functions ---------------------------

	// Given a string "foo", return ""foo"" with an extra set of double quotes added
	// This is to be in line with gocb's behavior of wrapping these startkey, endkey in an extra set of double quotes
	wrapInDoubleQuotes := func(original string) string {
		return fmt.Sprintf("\"%v\"", original)
	}

	// The gocb viewquery options map is a url.Values map where each key points to a slice of values.
	// This helper function makes it easier to get the first value out of that array for a given key
	getStringFromReflectUrlValuesMap := func(key reflect.Value, reflectMap reflect.Value) string {
		mapVal := reflectMap.MapIndex(key)
		return mapVal.Index(0).String()

	}

	// Helper function to extract a particular reflection map key from a list of reflection map keys
	findStringKeyValue := func(values []reflect.Value, valueToFind string) (foundValue reflect.Value, found bool) {
		for _, value := range values {
			if value.Kind() == reflect.String {
				if value.String() == valueToFind {
					return value, true
				}
			}
		}
		return reflect.Value{}, false
	}

	findStringValue := func(mapKeys []reflect.Value, reflectMap reflect.Value, key string) string {
		mapKey, found := findStringKeyValue(mapKeys, key)
		assert.True(t, found)
		return getStringFromReflectUrlValuesMap(mapKey, reflectMap)
	}

	// ------------------- Test Code ---------------------------

	// View query params map (go-couchbase/walrus style)
	params := map[string]interface{}{
		ViewQueryParamStale:         false,
		ViewQueryParamReduce:        true,
		ViewQueryParamStartKey:      "foo",
		ViewQueryParamEndKey:        "bar",
		ViewQueryParamInclusiveEnd:  true,
		ViewQueryParamLimit:         uint64(1),
		ViewQueryParamIncludeDocs:   true, // Ignored -- see https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		ViewQueryParamDescending:    true,
		ViewQueryParamGroup:         true,
		ViewQueryParamSkip:          uint64(2),
		ViewQueryParamGroupLevel:    uint64(3),
		ViewQueryParamStartKeyDocId: "baz",
		ViewQueryParamEndKeyDocId:   "blah",
		ViewQueryParamKey:           "hello",
		ViewQueryParamKeys:          []string{"a", "b"},
	}

	// add ViewQueryParamKeys param, which is a json array
	params[ViewQueryParamKeys] = []string{"a", "b"}

	// Create a new viewquery
	viewQuery := gocb.NewViewQuery("ddoc", "viewname")

	// Call applyViewQueryOptions (method being tested) which modifies viewQuery according to params
	if err := applyViewQueryOptions(viewQuery, params); err != nil {
		t.Fatalf("Error calling applyViewQueryOptions: %v", err)
	}

	// Use reflection to get a handle on the viewquery options (unexported)
	viewQueryReflectedVal := reflect.ValueOf(*viewQuery)
	optionsReflectedVal := viewQueryReflectedVal.FieldByName("options")

	// Get all the view query option keys
	mapKeys := optionsReflectedVal.MapKeys()

	// "stale"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamStale), "false")

	// "reduce"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamReduce), "true")

	// "startkey"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamStartKey), wrapInDoubleQuotes("foo"))

	// "endkey"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamEndKey), wrapInDoubleQuotes("bar"))

	// "inclusive_end"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamInclusiveEnd), "true")

	// "limit"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamLimit), "1")

	// "descending"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamDescending), "true")

	// "group"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamGroup), "true")

	// "skip"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamSkip), "2")

	// "group_level"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamGroupLevel), "3")

	// "startkey_docid"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamStartKeyDocId), "baz")

	// "endkey_docid"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamEndKeyDocId), "blah")

	// "key"
	assert.Equals(t, findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamKey), wrapInDoubleQuotes("hello"))

	// "keys"
	assert.Equals(t,
		findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamKeys),
		fmt.Sprintf("[%v,%v]", wrapInDoubleQuotes("a"), wrapInDoubleQuotes("b")))

}

// In certain cases, the params will have strings instead of bools
// https://github.com/couchbase/sync_gateway/issues/2423#issuecomment-296245658
func TestApplyViewQueryOptionsWithStrings(t *testing.T) {

	// View query params map (go-couchbase/walrus style)
	params := map[string]interface{}{
		ViewQueryParamStale:         "false",
		ViewQueryParamReduce:        "true",
		ViewQueryParamStartKey:      "foo",
		ViewQueryParamEndKey:        "bar",
		ViewQueryParamInclusiveEnd:  "true",
		ViewQueryParamLimit:         "1",
		ViewQueryParamIncludeDocs:   "true", // Ignored -- see https://forums.couchbase.com/t/do-the-viewquery-options-omit-include-docs-on-purpose/12399
		ViewQueryParamDescending:    "true",
		ViewQueryParamGroup:         "true",
		ViewQueryParamSkip:          "2",
		ViewQueryParamGroupLevel:    "3",
		ViewQueryParamStartKeyDocId: "baz",
		ViewQueryParamEndKeyDocId:   "blah",
		ViewQueryParamKey:           "hello",
		ViewQueryParamKeys:          []string{"a", "b"},
	}

	// Create a new viewquery
	viewQuery := gocb.NewViewQuery("ddoc", "viewname")

	if err := applyViewQueryOptions(viewQuery, params); err != nil {
		t.Fatalf("Error calling applyViewQueryOptions: %v", err)
	}

	// if it doesn't blow up, test passes

}
