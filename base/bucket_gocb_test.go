//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/couchbase/gocb.v1"
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
	assert.Equal(t, 0, bytes.Compare(resultBytes, jsonBytes))
	assert.Equal(t, jsonFlags, flags)
	assert.NoError(t, err)

	resultBytes, flags, err = transcoder.Encode(BinaryDocument(jsonBody))
	assert.Equal(t, 0, bytes.Compare(resultBytes, jsonBytes))
	assert.Equal(t, binaryFlags, flags)
	assert.NoError(t, err)
}

func TestSetGet(t *testing.T) {
	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key := t.Name()
		val := make(map[string]interface{}, 0)
		val["foo"] = "bar"

		var rVal map[string]interface{}
		_, err := bucket.Get(key, &rVal)
		assert.Error(t, err, "Key should not exist yet, expected error but got nil")

		err = bucket.Set(key, 0, nil, val)
		assert.NoError(t, err, "Error calling Set()")

		_, err = bucket.Get(key, &rVal)
		require.NoError(t, err, "Error calling Get()")
		fooVal, ok := rVal["foo"]
		require.True(t, ok, "expected property 'foo' not found")
		assert.Equal(t, "bar", fooVal)

		err = bucket.Delete(key)
		if err != nil {
			t.Errorf("Error removing key from bucket")
		}
	})
}

func TestSetGetRaw(t *testing.T) {
	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key := t.Name()
		val := []byte("bar")

		_, _, err := bucket.GetRaw(key)
		if err == nil {
			t.Errorf("Key should not exist yet, expected error but got nil")
		}

		if err := bucket.SetRaw(key, 0, nil, val); err != nil {
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
	})
}

func TestAddRaw(t *testing.T) {
	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		key := t.Name()
		val := []byte("bar")

		_, _, err := bucket.GetRaw(key)
		if err == nil {
			t.Errorf("Key should not exist yet, expected error but got nil")
		}

		added, err := bucket.AddRaw(key, 0, val)
		if err != nil {
			t.Errorf("Error calling AddRaw(): %v", err)
		}
		assert.True(t, added, "AddRaw returned added=false, expected true")

		rv, _, err := bucket.GetRaw(key)
		if string(rv) != string(val) {
			t.Errorf("%v != %v", string(rv), string(val))
		}

		// Calling AddRaw for existing value should return added=false, no error
		added, err = bucket.AddRaw(key, 0, val)
		if err != nil {
			t.Errorf("Error calling AddRaw(): %v", err)
		}
		assert.True(t, added == false, "AddRaw returned added=true for duplicate, expected false")

		err = bucket.Delete(key)
		if err != nil {
			t.Errorf("Error removing key from bucket: %v", err)
		}
	})
}

// TestAddRawTimeout attempts to fill up the gocbpipeline by writing large documents concurrently with a small timeout,
// to verify that timeout errors are returned, and the operation isn't retried (which would return a cas error).
//   (see CBG-463)
func TestAddRawTimeoutRetry(t *testing.T) {
	bucket := GetTestBucket(t)
	defer bucket.Close()

	gocbBucket, ok := bucket.Bucket.(*CouchbaseBucketGoCB)
	if ok {
		gocbBucket.Bucket.SetOperationTimeout(250 * time.Millisecond)
	}

	largeDoc := make([]byte, 1000000)
	rand.Read(largeDoc)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("%s_%d", t.Name(), i)
			added, err := bucket.AddRaw(key, 0, largeDoc)
			if err != nil {
				if pkgerrors.Cause(err) != gocb.ErrTimeout {
					t.Errorf("Unexpected error calling AddRaw(): %v", err)
				}
			} else {
				assert.True(t, added, fmt.Sprintf("AddRaw returned added=false, expected true for key %v", key))
			}
		}(i)

	}
	wg.Wait()
}

func TestWriteCasBasic(t *testing.T) {
	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		key := t.Name()
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
	})
}

func TestWriteCasAdvanced(t *testing.T) {
	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		key := t.Name()

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
	})
}

func TestUpdate(t *testing.T) {
	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		key := t.Name()
		valInitial := []byte(`{"state":"initial"}`)
		valUpdated := []byte(`{"state":"updated"}`)

		var rv map[string]interface{}
		_, err := bucket.Get(key, &rv)
		if err == nil {
			t.Errorf("Key should not exist yet, expected error but got nil")
		}

		updateFunc := func(current []byte) (updated []byte, expiry *uint32, isDelete bool, err error) {
			if len(current) == 0 {
				return valInitial, nil, false, nil
			} else {
				return valUpdated, nil, false, nil
			}
		}
		var cas uint64
		cas, err = bucket.Update(key, 0, updateFunc)
		if err != nil {
			t.Errorf("Error calling Update: %v", err)
		}
		if cas == 0 {
			t.Errorf("Unexpected cas returned by bucket.Update")
		}

		_, err = bucket.Get(key, &rv)
		assert.NoError(t, err, "error retrieving initial value")
		state, ok := rv["state"]
		assert.True(t, ok, "expected state property not present")
		assert.Equal(t, "initial", state)

		cas, err = bucket.Update(key, 0, updateFunc)
		if err != nil {
			t.Errorf("Error calling Update: %v", err)
		}
		if cas == 0 {
			t.Errorf("Unexpected cas returned by bucket.Update")
		}

		_, err = bucket.Get(key, &rv)
		assert.NoError(t, err, "error retrieving updated value")
		state, ok = rv["state"]
		assert.True(t, ok, "expected state property not present")
		assert.Equal(t, "updated", state)

		err = bucket.Delete(key)
		if err != nil {
			t.Errorf("Error removing key from bucket")
		}
	})
}

func TestUpdateCASFailure(t *testing.T) {
	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		key := t.Name()
		valInitial := []byte(`{"state":"initial"}`)
		valCasMismatch := []byte(`{"state":"casMismatch"}`)
		valUpdated := []byte(`{"state":"updated"}`)

		var rv map[string]interface{}
		_, err := bucket.Get(key, &rv)
		if err == nil {
			t.Errorf("Key should not exist yet, expected error but got nil")
		}

		// Initialize document
		setErr := bucket.Set(key, 0, nil, valInitial)
		assert.NoError(t, setErr)

		triggerCasFail := true
		updateFunc := func(current []byte) (updated []byte, expiry *uint32, isDelete bool, err error) {
			if triggerCasFail == true {
				// mutate the document to trigger cas failure
				setErr := bucket.Set(key, 0, nil, valCasMismatch)
				assert.NoError(t, setErr)
				triggerCasFail = false
			}
			return valUpdated, nil, false, nil
		}

		_, err = bucket.Update(key, 0, updateFunc)
		if err != nil {
			t.Errorf("Error calling Update: %v", err)
		}

		// verify update succeeded
		_, err = bucket.Get(key, &rv)
		assert.NoError(t, err, "error retrieving updated value")
		state, ok := rv["state"]
		assert.True(t, ok, "expected state property not present")
		assert.Equal(t, "updated", state)

		err = bucket.Delete(key)
		if err != nil {
			t.Errorf("Error removing key from bucket")
		}
	})
}

func TestUpdateCASFailureOnInsert(t *testing.T) {
	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		key := t.Name()
		valCasMismatch := []byte(`{"state":"casMismatch"}`)
		valInitial := []byte(`{"state":"initial"}`)

		var rv map[string]interface{}
		_, err := bucket.Get(key, &rv)
		if err == nil {
			t.Errorf("Key should not exist yet, expected error but got nil")
		}

		// Attempt to create the doc via update
		triggerCasFail := true
		updateFunc := func(current []byte) (updated []byte, expiry *uint32, isDelete bool, err error) {
			if triggerCasFail == true {
				// mutate the document to trigger cas failure
				setErr := bucket.Set(key, 0, nil, valCasMismatch)
				assert.NoError(t, setErr)
				triggerCasFail = false
			}
			return valInitial, nil, false, nil
		}

		_, err = bucket.Update(key, 0, updateFunc)
		if err != nil {
			t.Errorf("Error calling Update: %v", err)
		}

		// verify update succeeded
		_, err = bucket.Get(key, &rv)
		assert.NoError(t, err, "error retrieving updated value")
		state, ok := rv["state"]
		assert.True(t, ok, "expected state property not present")
		assert.Equal(t, "initial", state)

		err = bucket.Delete(key)
		if err != nil {
			t.Errorf("Error removing key from bucket")
		}
	})
}

func TestIncrCounter(t *testing.T) {
	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		key := t.Name()

		defer func() {
			err := bucket.Delete(key)
			if err != nil {
				t.Errorf("Error removing counter from bucket")
			}
		}()

		// New Counter - incr 1, default 1
		value, err := bucket.Incr(key, 1, 1, 0)
		assert.NoError(t, err, "Error incrementing non-existent counter")

		// key did not exist - so expect the "initial" value of 1
		assert.Equal(t, uint64(1), value)

		// Retrieve existing counter value using GetCounter
		retrieval, err := GetCounter(bucket, key)
		assert.NoError(t, err, "Error retrieving value for existing counter")
		assert.Equal(t, uint64(1), retrieval)

		// Increment existing counter
		retrieval, err = bucket.Incr(key, 1, 1, 0)
		assert.NoError(t, err, "Error incrementing value for existing counter")
		assert.Equal(t, uint64(2), retrieval)
	})
}

func TestGetAndTouchRaw(t *testing.T) {

	// There's no easy way to validate the expiry time of a doc (that I know of)
	// so this is just a smoke test

	key := t.Name()
	val := []byte("bar")
	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		defer func() {
			err := bucket.Delete(key)
			if err != nil {
				t.Errorf("Error removing key from bucket")
			}

		}()

		_, _, err := bucket.GetRaw(key)
		assert.Error(t, err, "Key should not exist yet, expected error but got nil")

		err = bucket.SetRaw(key, 0, nil, val)
		assert.NoError(t, err, "Error calling SetRaw()")

		rv, _, err := bucket.GetRaw(key)
		assert.True(t, err == nil)
		if string(rv) != string(val) {
			t.Errorf("%v != %v", string(rv), string(val))
		}

		rv, _, err = bucket.GetAndTouchRaw(key, 1)
		assert.NoError(t, err, "Error calling GetAndTouchRaw")

		assert.True(t, err == nil)
		assert.Equal(t, len(val), len(rv))

		_, err = bucket.Touch(key, 1)
		assert.NoError(t, err, "Error calling Touch")
	})

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
	assert.Equal(t, 4, len(batches))
	assert.Equal(t, "one", batches[0][0].Key)
	assert.Equal(t, "two", batches[0][1].Key)
	assert.Equal(t, "three", batches[1][0].Key)
	assert.Equal(t, "four", batches[1][1].Key)
	assert.Equal(t, "five", batches[2][0].Key)
	assert.Equal(t, "six", batches[2][1].Key)
	assert.Equal(t, "seven", batches[3][0].Key)
}

func TestCreateBatchesKeys(t *testing.T) {
	keys := []string{"one", "two", "three", "four", "five", "six", "seven"}
	batchSize := uint(2)
	batches := createBatchesKeys(batchSize, keys)
	log.Printf("batches: %+v", batches)
	assert.Equal(t, 4, len(batches))
	assert.Equal(t, "one", batches[0][0])
	assert.Equal(t, "two", batches[0][1])
	assert.Equal(t, "three", batches[1][0])
	assert.Equal(t, "four", batches[1][1])
	assert.Equal(t, "five", batches[2][0])
	assert.Equal(t, "six", batches[2][1])
	assert.Equal(t, "seven", batches[3][0])
}

func SkipXattrTestsIfNotEnabled(t *testing.T) {

	if !TestUseXattrs() {
		t.Skip("XATTR based tests not enabled.  Enable via SG_TEST_USE_XATTRS=true environment variable")
	}

	if UnitTestUrlIsWalrus() {
		t.Skip("This test won't work under walrus until https://github.com/couchbase/sync_gateway/issues/2390")
	}
}

// TestXattrWriteCasSimple.  Validates basic write of document with xattr, and retrieval of the same doc w/ xattr.
func TestXattrWriteCasSimple(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		key := t.Name()
		xattrName := SyncXattrName
		val := make(map[string]interface{})
		val["body_field"] = "1234"

		valBytes, marshalErr := JSONMarshal(val)
		assert.NoError(t, marshalErr, "Error marshalling document body")

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
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, val, xattrVal)
		assert.NoError(t, err, "WriteCasWithXattr error")
		log.Printf("Post-write, cas is %d", cas)

		var retrievedVal map[string]interface{}
		var retrievedXattr map[string]interface{}
		getCas, err := bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}

		assert.Equal(t, cas, getCas)
		assert.Equal(t, val["body_field"], retrievedVal["body_field"])
		assert.Equal(t, xattrVal["seq"], retrievedXattr["seq"])
		assert.Equal(t, xattrVal["rev"], retrievedXattr["rev"])
		macroCasString, ok := retrievedXattr[xattrMacroCas].(string)
		assert.True(t, ok, "Unable to retrieve xattrMacroCas as string")
		assert.Equal(t, cas, HexCasToUint64(macroCasString))
		macroBodyHashString, ok := retrievedXattr[xattrMacroValueCrc32c].(string)
		assert.True(t, ok, "Unable to retrieve xattrMacroValueCrc32c as string")
		assert.Equal(t, Crc32cHashString(valBytes), macroBodyHashString)

		// Validate against $document.value_crc32c
		var retrievedVxattr map[string]interface{}
		_, err = bucket.GetWithXattr(key, "$document", "", retrievedVal, &retrievedVxattr, nil)
		vxattrCrc32c, ok := retrievedVxattr["value_crc32c"].(string)
		assert.True(t, ok, "Unable to retrieve virtual xattr crc32c as string")

		assert.Equal(t, Crc32cHashString(valBytes), vxattrCrc32c)
		assert.Equal(t, macroBodyHashString, vxattrCrc32c)
	})

}

// TestXattrWriteCasUpsert.  Validates basic write of document with xattr,  retrieval of the same doc w/ xattr, update of the doc w/ xattr, retrieval of the doc w/ xattr.
func TestXattrWriteCasUpsert(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key := t.Name()
		xattrName := SyncXattrName
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
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, val, xattrVal)
		assert.NoError(t, err, "WriteCasWithXattr error")
		log.Printf("Post-write, cas is %d", cas)

		var retrievedVal map[string]interface{}
		var retrievedXattr map[string]interface{}
		getCas, err := bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		// TODO: Cas check fails, pending xattr code to make it to gocb master
		log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
		assert.Equal(t, cas, getCas)
		assert.Equal(t, val["body_field"], retrievedVal["body_field"])
		assert.Equal(t, xattrVal["seq"], retrievedXattr["seq"])
		assert.Equal(t, xattrVal["rev"], retrievedXattr["rev"])

		val2 := make(map[string]interface{})
		val2["body_field"] = "5678"
		xattrVal2 := make(map[string]interface{})
		xattrVal2["seq"] = float64(124)
		xattrVal2["rev"] = "2-5678"
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, getCas, nil, val2, xattrVal2)
		assert.NoError(t, err, "WriteCasWithXattr error")
		log.Printf("Post-write, cas is %d", cas)

		var retrievedVal2 map[string]interface{}
		var retrievedXattr2 map[string]interface{}
		getCas, err = bucket.GetWithXattr(key, xattrName, "", &retrievedVal2, &retrievedXattr2, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal2, retrievedXattr2)
		assert.Equal(t, cas, getCas)
		assert.Equal(t, val2["body_field"], retrievedVal2["body_field"])
		assert.Equal(t, xattrVal2["seq"], retrievedXattr2["seq"])
		assert.Equal(t, xattrVal2["rev"], retrievedXattr2["rev"])
	})

}

// TestXattrWriteCasWithXattrCasCheck.  Validates cas check when using WriteCasWithXattr
func TestXattrWriteCasWithXattrCasCheck(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key := t.Name()
		xattrName := SyncXattrName
		val := make(map[string]interface{})
		val["sg_field"] = "sg_value"

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
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, val, xattrVal)
		assert.NoError(t, err, "WriteCasWithXattr error")
		log.Printf("Post-write, cas is %d", cas)

		var retrievedVal map[string]interface{}
		var retrievedXattr map[string]interface{}
		getCas, err := bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, "")
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
		assert.Equal(t, cas, getCas)
		assert.Equal(t, val["sg_field"], retrievedVal["sg_field"])
		assert.Equal(t, xattrVal["seq"], retrievedXattr["seq"])
		assert.Equal(t, xattrVal["rev"], retrievedXattr["rev"])

		// Simulate an SDK update
		updatedVal := make(map[string]interface{})
		updatedVal["sdk_field"] = "abc"
		require.NoError(t, bucket.Set(key, 0, nil, updatedVal))

		// Attempt to update with the previous CAS
		val["sg_field"] = "sg_value_mod"
		xattrVal["rev"] = "2-1234"
		_, err = bucket.WriteCasWithXattr(key, xattrName, 0, getCas, nil, val, xattrVal)
		assert.True(t, IsCasMismatch(err))

		// Retrieve again, ensure we get the SDK value, SG xattr
		retrievedVal = nil
		retrievedXattr = nil
		_, err = bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
		assert.Equal(t, nil, retrievedVal["sg_field"])
		assert.Equal(t, updatedVal["sdk_field"], retrievedVal["sdk_field"])
		assert.Equal(t, xattrVal["seq"], retrievedXattr["seq"])
		assert.Equal(t, "1-1234", retrievedXattr["rev"])
	})

}

// TestWriteCasXATTRRaw.  Validates basic write of document and xattr as raw bytes.
func TestXattrWriteCasRaw(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key := t.Name()
		xattrName := SyncXattrName
		val := make(map[string]interface{})
		val["body_field"] = "1234"
		valRaw, _ := JSONMarshal(val)

		xattrVal := make(map[string]interface{})
		xattrVal["seq"] = float64(123)
		xattrVal["rev"] = "1-1234"
		xattrValRaw, _ := JSONMarshal(xattrVal)

		var existsVal map[string]interface{}
		_, err := bucket.Get(key, existsVal)
		if err == nil {
			log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
			err = bucket.DeleteWithXattr(key, xattrName)
		}

		cas := uint64(0)
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, valRaw, xattrValRaw)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}

		var retrievedValByte []byte
		var retrievedXattrByte []byte
		getCas, err := bucket.GetWithXattr(key, xattrName, "", &retrievedValByte, &retrievedXattrByte, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}

		var retrievedVal map[string]interface{}
		var retrievedXattr map[string]interface{}
		_ = json.Unmarshal(retrievedValByte, &retrievedVal)
		_ = json.Unmarshal(retrievedXattrByte, &retrievedXattr)
		log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
		assert.Equal(t, cas, getCas)
		assert.Equal(t, val["body_field"], retrievedVal["body_field"])
		assert.Equal(t, xattrVal["seq"], retrievedXattr["seq"])
		assert.Equal(t, xattrVal["rev"], retrievedXattr["rev"])
	})
}

// TestWriteCasTombstoneResurrect.  Verifies writing a new document body and xattr to a logically deleted document (xattr still exists)
func TestXattrWriteCasTombstoneResurrect(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key := t.Name()
		xattrName := SyncXattrName
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
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}
		log.Printf("Post-write, cas is %d", cas)

		var retrievedVal map[string]interface{}
		var retrievedXattr map[string]interface{}
		getCas, err := bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		// TODO: Cas check fails, pending xattr code to make it to gocb master
		log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
		assert.Equal(t, cas, getCas)
		assert.Equal(t, val["body_field"], retrievedVal["body_field"])
		assert.Equal(t, xattrVal["seq"], retrievedXattr["seq"])
		assert.Equal(t, xattrVal["rev"], retrievedXattr["rev"])

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
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}

		// Verify retrieval
		getCas, err = bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		// TODO: Cas check fails, pending xattr code to make it to gocb master
		log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
		assert.Equal(t, val["body_field"], retrievedVal["body_field"])
		assert.Equal(t, xattrVal["seq"], retrievedXattr["seq"])
		assert.Equal(t, xattrVal["rev"], retrievedXattr["rev"])
	})

}

// TestXattrWriteCasTombstoneUpdate.  Validates update of xattr on logically deleted document.
func TestXattrWriteCasTombstoneUpdate(t *testing.T) {

	t.Skip("Test does not pass with errors: https://gist.github.com/tleyden/d261fe2b92bdaaa6e78f9f1c00fdfd58.  Needs investigation")

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		key := t.Name()
		xattrName := SyncXattrName
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
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}
		log.Printf("Wrote document")
		log.Printf("Post-write, cas is %d", cas)

		var retrievedVal map[string]interface{}
		var retrievedXattr map[string]interface{}
		getCas, err := bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		log.Printf("Retrieved document")
		// TODO: Cas check fails, pending xattr code to make it to gocb master
		log.Printf("TestWriteCasXATTR retrieved: %s, %s", retrievedVal, retrievedXattr)
		assert.Equal(t, cas, getCas)
		assert.Equal(t, val["body_field"], retrievedVal["body_field"])
		assert.Equal(t, xattrVal["seq"], retrievedXattr["seq"])
		assert.Equal(t, xattrVal["rev"], retrievedXattr["rev"])

		err = bucket.Delete(key)
		if err != nil {
			t.Errorf("Error doing Delete: %+v", err)
		}

		log.Printf("Deleted document")
		// Update the xattr
		xattrVal = make(map[string]interface{})
		xattrVal["seq"] = float64(456)
		xattrVal["rev"] = "2-2345"
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, nil, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}

		log.Printf("Updated tombstoned document")
		// Verify retrieval
		var modifiedVal map[string]interface{}
		var modifiedXattr map[string]interface{}
		getCas, err = bucket.GetWithXattr(key, xattrName, "", &modifiedVal, &modifiedXattr, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		log.Printf("Retrieved tombstoned document")
		// TODO: Cas check fails, pending xattr code to make it to gocb master
		log.Printf("TestWriteCasXATTR retrieved modified: %s, %s", modifiedVal, modifiedXattr)
		assert.Equal(t, xattrVal["seq"], modifiedXattr["seq"])
		assert.Equal(t, xattrVal["rev"], modifiedXattr["rev"])
	})

}

// TestXattrWriteUpdateXattr.  Validates basic write of document with xattr, and retrieval of the same doc w/ xattr.
func TestXattrWriteUpdateXattr(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key := t.Name()
		xattrName := SyncXattrName
		val := make(map[string]interface{})
		val["counter"] = float64(1)

		xattrVal := make(map[string]interface{})
		xattrVal["seq"] = float64(1)
		xattrVal["rev"] = "1-1234"

		var existsVal map[string]interface{}
		var existsXattr map[string]interface{}
		_, err := bucket.GetWithXattr(key, xattrName, "", &existsVal, &existsXattr, nil)
		if err == nil {
			log.Printf("Key should not exist yet, but get succeeded.  Doing cleanup, assuming couchbase bucket testing")
			err := bucket.DeleteWithXattr(key, xattrName)
			if err != nil {
				log.Printf("Got error trying to do pre-test cleanup:%v", err)
			}
		}

		// Dummy write update function that increments 'counter' in the doc and 'seq' in the xattr
		writeUpdateFunc := func(doc []byte, xattr []byte, userXattr []byte, cas uint64) (
			updatedDoc []byte, updatedXattr []byte, isDelete bool, updatedExpiry *uint32, err error) {

			var docMap map[string]interface{}
			var xattrMap map[string]interface{}
			// Marshal the doc
			if len(doc) > 0 {
				err = JSONUnmarshal(doc, &docMap)
				if err != nil {
					return nil, nil, false, nil, pkgerrors.Wrapf(err, "Unable to unmarshal incoming doc")
				}
			} else {
				// No incoming doc, treat as insert.
				docMap = make(map[string]interface{})
			}

			// Marshal the xattr
			if len(xattr) > 0 {
				err = JSONUnmarshal(xattr, &xattrMap)
				if err != nil {
					return nil, nil, false, nil, pkgerrors.Wrapf(err, "Unable to unmarshal incoming xattr")
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

			updatedDoc, _ = JSONMarshal(docMap)
			updatedXattr, _ = JSONMarshal(xattrMap)
			return updatedDoc, updatedXattr, false, nil, nil
		}

		// Insert
		_, err = bucket.WriteUpdateWithXattr(key, xattrName, "", 0, nil, nil, writeUpdateFunc)
		if err != nil {
			t.Errorf("Error doing WriteUpdateWithXattr: %+v", err)
		}

		var retrievedVal map[string]interface{}
		var retrievedXattr map[string]interface{}
		_, err = bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
		log.Printf("Retrieval after WriteUpdate insert: doc: %v, xattr: %v", retrievedVal, retrievedXattr)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		assert.Equal(t, float64(1), retrievedVal["counter"])
		assert.Equal(t, float64(1), retrievedXattr["seq"])

		// Update
		_, err = bucket.WriteUpdateWithXattr(key, xattrName, "", 0, nil, nil, writeUpdateFunc)
		if err != nil {
			t.Errorf("Error doing WriteUpdateWithXattr: %+v", err)
		}
		_, err = bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		log.Printf("Retrieval after WriteUpdate update: doc: %v, xattr: %v", retrievedVal, retrievedXattr)

		assert.Equal(t, float64(2), retrievedVal["counter"])
		assert.Equal(t, float64(2), retrievedXattr["seq"])
	})

}

func TestWriteUpdateWithXattrUserXattr(t *testing.T) {
	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key := t.Name()
		xattrKey := SyncXattrName
		userXattrKey := "UserXattr"

		writeUpdateFunc := func(doc []byte, xattr []byte, userXattr []byte, cas uint64) (updatedDoc []byte, updatedXattr []byte, isDelete bool, updatedExpiry *uint32, err error) {

			var docMap map[string]interface{}
			var xattrMap map[string]interface{}

			if len(doc) > 0 {
				err = JSONUnmarshal(xattr, &docMap)
				if err != nil {
					return nil, nil, false, nil, err
				}
			} else {
				docMap = make(map[string]interface{})
			}

			if len(xattr) > 0 {
				err = JSONUnmarshal(xattr, &xattrMap)
				if err != nil {
					return nil, nil, false, nil, err
				}
			} else {
				xattrMap = make(map[string]interface{})
			}

			var userXattrMap map[string]interface{}
			if len(userXattr) > 0 {
				err = JSONUnmarshal(userXattr, &userXattrMap)
				if err != nil {
					return nil, nil, false, nil, err
				}
			} else {
				userXattrMap = nil
			}

			docMap["userXattrVal"] = userXattrMap

			updatedDoc, _ = JSONMarshal(docMap)
			updatedXattr, _ = JSONMarshal(xattrMap)

			return updatedDoc, updatedXattr, false, nil, nil
		}

		_, err := bucket.WriteUpdateWithXattr(key, xattrKey, userXattrKey, 0, nil, nil, writeUpdateFunc)
		assert.NoError(t, err)

		var gotBody map[string]interface{}
		_, err = bucket.Get(key, &gotBody)
		assert.NoError(t, err)
		assert.Equal(t, nil, gotBody["userXattrVal"])

		userXattrVal := map[string]interface{}{"val": "val"}

		userXattrStore, ok := AsUserXattrStore(bucket)
		require.True(t, ok)
		_, err = userXattrStore.WriteUserXattr(key, userXattrKey, userXattrVal)
		assert.NoError(t, err)

		_, err = bucket.WriteUpdateWithXattr(key, xattrKey, userXattrKey, 0, nil, nil, writeUpdateFunc)
		assert.NoError(t, err)

		_, err = bucket.Get(key, &gotBody)
		assert.NoError(t, err)

		assert.Equal(t, userXattrVal, gotBody["userXattrVal"])
	})

}

// TestXattrDeleteDocument.  Delete document that has a system xattr.  System XATTR should be retained and retrievable.
func TestXattrDeleteDocument(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		// Create document with XATTR
		xattrName := SyncXattrName
		val := make(map[string]interface{})
		val["body_field"] = "1234"

		xattrVal := make(map[string]interface{})
		xattrVal["seq"] = 123
		xattrVal["rev"] = "1-1234"

		key := t.Name()
		_, _, err := bucket.GetRaw(key)
		if err == nil {
			log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
			require.NoError(t, bucket.Delete(key))
		}

		// Create w/ XATTR, delete doc and XATTR, retrieve doc (expect fail), retrieve XATTR (expect success)
		cas := uint64(0)
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, val, xattrVal)
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
		_, err = bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		assert.Equal(t, 0, len(retrievedVal))
		assert.Equal(t, float64(123), retrievedXattr["seq"])
	})

}

// TestXattrDeleteDocumentUpdate.  Delete a document that has a system xattr along with an xattr update.
func TestXattrDeleteDocumentUpdate(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		// Create document with XATTR
		xattrName := SyncXattrName
		val := make(map[string]interface{})
		val["body_field"] = "1234"

		xattrVal := make(map[string]interface{})
		xattrVal["seq"] = 1
		xattrVal["rev"] = "1-1234"

		key := t.Name()
		_, _, err := bucket.GetRaw(key)
		if err == nil {
			log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
			require.NoError(t, bucket.Delete(key))
		}

		// Create w/ XATTR, delete doc and XATTR, retrieve doc (expect fail), retrieve XATTR (expect success)
		cas := uint64(0)
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, val, xattrVal)
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
		getCas, err := bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
		if err != nil {
			t.Errorf("Error doing GetWithXattr: %+v", err)
		}
		assert.Equal(t, 0, len(retrievedVal))
		assert.Equal(t, float64(1), retrievedXattr["seq"])
		log.Printf("Post-delete xattr (1): %s", retrievedXattr)
		log.Printf("Post-delete cas (1): %x", getCas)

		// Update the xattr only
		xattrVal["seq"] = 2
		xattrVal["rev"] = "1-1234"
		casOut, writeErr := bucket.WriteCasWithXattr(key, xattrName, 0, getCas, nil, nil, xattrVal)
		assert.NoError(t, writeErr, "Error updating xattr post-delete")
		log.Printf("WriteCasWithXattr cas: %d", casOut)

		// Retrieve the document, validate cas values
		var postDeleteVal map[string]interface{}
		var postDeleteXattr map[string]interface{}
		getCas2, err := bucket.GetWithXattr(key, xattrName, "", &postDeleteVal, &postDeleteXattr, nil)
		assert.NoError(t, err, "Error getting document post-delete")
		assert.Equal(t, float64(2), postDeleteXattr["seq"])
		assert.Equal(t, 0, len(postDeleteVal))
		log.Printf("Post-delete xattr (2): %s", postDeleteXattr)
		log.Printf("Post-delete cas (2): %x", getCas2)
	})

}

// TestXattrDeleteDocumentAndUpdateXATTR.  Delete the document body and update the xattr.
func TestXattrDeleteDocumentAndUpdateXattr(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		// Create document with XATTR
		xattrName := SyncXattrName
		val := make(map[string]interface{})
		val["body_field"] = "1234"

		xattrVal := make(map[string]interface{})
		xattrVal["seq"] = 123
		xattrVal["rev"] = "1-1234"

		key := t.Name()
		_, _, err := bucket.GetRaw(key)
		if err == nil {
			log.Printf("Key should not exist yet, expected error but got nil.  Doing cleanup, assuming couchbase bucket testing")
			require.NoError(t, bucket.DeleteWithXattr(key, xattrName))
		}

		// Create w/ XATTR, delete doc and XATTR, retrieve doc (expect fail), retrieve XATTR (expect fail)
		cas := uint64(0)
		cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}

		subdocXattrStore, ok := AsSubdocXattrStore(bucket)
		require.True(t, ok)

		_, mutateErr := subdocXattrStore.SubdocUpdateXattrDeleteBody(key, xattrName, 0, cas, xattrVal)
		assert.NoError(t, mutateErr)

		// Verify delete of body and update of XATTR
		var retrievedVal map[string]interface{}
		var retrievedXattr map[string]interface{}
		mutateCas, err := bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
		assert.Equal(t, 0, len(retrievedVal))
		assert.Equal(t, float64(123), retrievedXattr["seq"])
		log.Printf("value: %v, xattr: %v", retrievedVal, retrievedXattr)
		log.Printf("MutateInEx cas: %v", mutateCas)
	})

}

// Validates tombstone of doc + xattr in a matrix of various possible previous states of the document.
func TestXattrTombstoneDocAndUpdateXattr(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	SetUpTestLogging(t, LevelDebug, KeyCRUD)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key1 := t.Name() + "DocExistsXattrExists"
		key2 := t.Name() + "DocExistsNoXattr"
		key3 := t.Name() + "XattrExistsNoDoc"
		key4 := t.Name() + "NoDocNoXattr"

		// 1. Create document with XATTR
		val := make(map[string]interface{})
		val["type"] = key1

		xattrName := SyncXattrName
		xattrVal := make(map[string]interface{})
		xattrVal["seq"] = 123
		xattrVal["rev"] = "1-1234"

		var err error

		// Create w/ XATTR
		cas1 := uint64(0)
		cas1, err = bucket.WriteCasWithXattr(key1, xattrName, 0, cas1, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}

		// 2. Create document with no XATTR
		val = make(map[string]interface{})
		val["type"] = key2
		cas2, writeErr := bucket.WriteCas(key2, 0, 0, 0, val, 0)
		assert.NoError(t, writeErr)

		// 3. Xattr, no document
		val = make(map[string]interface{})
		val["type"] = key3

		xattrVal = make(map[string]interface{})
		xattrVal["seq"] = 456
		xattrVal["rev"] = "1-1234"

		// Create w/ XATTR
		cas3int := uint64(0)
		cas3int, err = bucket.WriteCasWithXattr(key3, xattrName, 0, cas3int, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}
		// Delete the doc body
		cas3, removeErr := bucket.Remove(key3, cas3int)
		if removeErr != nil {
			t.Errorf("Error removing doc body: %+v.  Cas: %v", removeErr, cas3)
		}

		// 4. No xattr, no document
		updatedVal := make(map[string]interface{})
		updatedVal["type"] = "updated"

		updatedXattrVal := make(map[string]interface{})
		updatedXattrVal["seq"] = 123
		updatedXattrVal["rev"] = "2-1234"

		// Attempt to delete DocExistsXattrExists, DocExistsNoXattr, and XattrExistsNoDoc
		// No errors should be returned when deleting these.
		keys := []string{key1, key2, key3}
		casValues := []uint64{cas1, cas2, cas3}
		shouldDeleteBody := []bool{true, true, false}
		subdocStore, ok := AsSubdocXattrStore(bucket)
		require.True(t, ok)
		for i, key := range keys {

			log.Printf("Delete testing for key: %v", key)
			// First attempt to update with a bad cas value, and ensure we're getting the expected error
			_, errCasMismatch := UpdateTombstoneXattr(subdocStore, key, xattrName, 0, uint64(1234), &updatedXattrVal, shouldDeleteBody[i])
			assert.True(t, IsCasMismatch(errCasMismatch), fmt.Sprintf("Expected cas mismatch for %s", key))

			_, errDelete := UpdateTombstoneXattr(subdocStore, key, xattrName, 0, uint64(casValues[i]), &updatedXattrVal, shouldDeleteBody[i])
			log.Printf("Delete error: %v", errDelete)

			assert.NoError(t, errDelete, fmt.Sprintf("Unexpected error deleting %s", key))
			assert.True(t, verifyDocDeletedXattrExists(bucket, key, xattrName), fmt.Sprintf("Expected doc %s to be deleted", key))
		}

		// Now attempt to tombstone key4 (NoDocNoXattr), should not return an error (per SG #3307).  Should save xattr metadata.
		log.Printf("Deleting key: %v", key4)
		_, errDelete := UpdateTombstoneXattr(subdocStore, key4, xattrName, 0, uint64(0), &updatedXattrVal, false)
		assert.NoError(t, errDelete, "Unexpected error tombstoning non-existent doc")
		assert.True(t, verifyDocDeletedXattrExists(bucket, key4, xattrName), "Expected doc to be deleted, but xattrs to exist")
	})

}

// Validates deletion of doc + xattr in a matrix of various possible previous states of the document.
func TestXattrDeleteDocAndXattr(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	SetUpTestLogging(t, LevelDebug, KeyCRUD)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key1 := t.Name() + "DocExistsXattrExists"
		key2 := t.Name() + "DocExistsNoXattr"
		key3 := t.Name() + "XattrExistsNoDoc"
		key4 := t.Name() + "NoDocNoXattr"

		// 1. Create document with XATTR
		val := make(map[string]interface{})
		val["type"] = key1

		xattrName := SyncXattrName
		xattrVal := make(map[string]interface{})
		xattrVal["seq"] = 123
		xattrVal["rev"] = "1-1234"

		var err error

		// Create w/ XATTR
		cas1 := uint64(0)
		cas1, err = bucket.WriteCasWithXattr(key1, xattrName, 0, cas1, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}

		// 2. Create document with no XATTR
		val = make(map[string]interface{})
		val["type"] = key2
		err = bucket.Set(key2, uint32(0), nil, val)
		assert.NoError(t, err)

		// 3. Xattr, no document
		val = make(map[string]interface{})
		val["type"] = key3

		xattrVal = make(map[string]interface{})
		xattrVal["seq"] = 456
		xattrVal["rev"] = "1-1234"

		// Create w/ XATTR
		cas3int := uint64(0)
		cas3int, err = bucket.WriteCasWithXattr(key3, xattrName, 0, cas3int, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}
		// Delete the doc body
		var cas3 gocb.Cas
		err = bucket.Delete(key3)
		if err != nil {
			t.Errorf("Error removing doc body: %+v.  Cas: %v", err, cas3)
		}

		// 4. No xattr, no document

		// Attempt to delete DocExistsXattrExists, DocExistsNoXattr, and XattrExistsNoDoc
		// No errors should be returned when deleting these.
		keys := []string{key1, key2, key3}
		for _, key := range keys {
			log.Printf("Deleting key: %v", key)
			errDelete := bucket.DeleteWithXattr(key, xattrName)
			assert.NoError(t, errDelete, fmt.Sprintf("Unexpected error deleting %s", key))
			assert.True(t, verifyDocAndXattrDeleted(bucket, key, xattrName), "Expected doc to be deleted")
		}

		// Now attempt to delete key4 (NoDocNoXattr), which is expected to return a Key Not Found error
		log.Printf("Deleting key: %v", key4)
		errDelete := bucket.DeleteWithXattr(key4, xattrName)
		assert.Error(t, errDelete, "Expected error when calling bucket.DeleteWithXattr")
		assert.Truef(t, pkgerrors.Cause(errDelete) == ErrNotFound, "Exepcted keynotfound error but got %v", errDelete)
		assert.True(t, verifyDocAndXattrDeleted(bucket, key4, xattrName), "Expected doc to be deleted")
	})
}

// This simulates a race condition by calling deleteWithXattrInternal() and passing a custom
// callback function
func TestDeleteWithXattrWithSimulatedRaceResurrect(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key := t.Name()
		xattrName := SyncXattrName
		createTombstonedDoc(bucket, key, xattrName)

		numTimesCalledBack := 0
		callback := func(k string, xattrKey string) {

			// Only want the callback to execute once.  Should be called multiple times (twice) due to expected
			// cas failure due to using stale cas
			if numTimesCalledBack >= 1 {
				return
			}
			numTimesCalledBack += 1

			// Resurrect the doc by updating the doc body
			updatedVal := map[string]interface{}{}
			updatedVal["foo"] = "bar"
			xattrVal := make(map[string]interface{})
			xattrVal["seq"] = float64(456)
			xattrVal["rev"] = "2-2345"
			_, writeErr := bucket.WriteCasWithXattr(k, xattrKey, 0, 0, nil, updatedVal, xattrVal)
			if writeErr != nil {
				panic(fmt.Sprintf("Unexpected error in WriteCasWithXattr: %v", writeErr))

			}

		}

		// Use AsSubdocXattrStore to do the underlying bucket lookup, then switch to KvXattrStore to pass to
		// deleteWithXattrInternal
		subdocStore, ok := AsSubdocXattrStore(bucket)
		require.True(t, ok)
		kvXattrStore, ok := subdocStore.(KvXattrStore)
		require.True(t, ok)
		deleteErr := deleteWithXattrInternal(kvXattrStore, key, xattrName, callback)

		assert.True(t, deleteErr != nil, "We expected an error here, because deleteWithXattrInternal should have "+
			" detected that the doc was resurrected during its execution")
	})

}

// TestXattrRetrieveDocumentAndXattr.
func TestXattrRetrieveDocumentAndXattr(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key1 := t.Name() + "DocExistsXattrExists"
		key2 := t.Name() + "DocExistsNoXattr"
		key3 := t.Name() + "XattrExistsNoDoc"
		key4 := t.Name() + "NoDocNoXattr"

		// 1. Create document with XATTR
		val := make(map[string]interface{})
		val["type"] = key1

		xattrName := SyncXattrName
		xattrVal := make(map[string]interface{})
		xattrVal["seq"] = 123
		xattrVal["rev"] = "1-1234"

		var err error

		// Create w/ XATTR
		cas := uint64(0)
		cas, err = bucket.WriteCasWithXattr(key1, xattrName, 0, cas, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}

		// 2. Create document with no XATTR
		val = make(map[string]interface{})
		val["type"] = key2
		_, err = bucket.Add(key2, 0, val)

		// 3. Xattr, no document
		val = make(map[string]interface{})
		val["type"] = key3

		xattrVal = make(map[string]interface{})
		xattrVal["seq"] = 456
		xattrVal["rev"] = "1-1234"

		// Create w/ XATTR
		cas = uint64(0)
		cas, err = bucket.WriteCasWithXattr(key3, xattrName, 0, cas, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}
		// Delete the doc
		require.NoError(t, bucket.Delete(key3))

		// 4. No xattr, no document

		// Attempt to retrieve all 4 docs
		var key1DocResult map[string]interface{}
		var key1XattrResult map[string]interface{}
		_, key1err := bucket.GetWithXattr(key1, xattrName, "", &key1DocResult, &key1XattrResult, nil)
		assert.NoError(t, key1err, "Unexpected error retrieving doc w/ xattr")
		assert.Equal(t, key1, key1DocResult["type"])
		assert.Equal(t, "1-1234", key1XattrResult["rev"])

		var key2DocResult map[string]interface{}
		var key2XattrResult map[string]interface{}
		_, key2err := bucket.GetWithXattr(key2, xattrName, "", &key2DocResult, &key2XattrResult, nil)
		assert.NoError(t, key2err, "Unexpected error retrieving doc w/out xattr")
		assert.Equal(t, key2, key2DocResult["type"])
		assert.Nil(t, key2XattrResult)

		var key3DocResult map[string]interface{}
		var key3XattrResult map[string]interface{}
		_, key3err := bucket.GetWithXattr(key3, xattrName, "", &key3DocResult, &key3XattrResult, nil)
		assert.NoError(t, key3err, "Unexpected error retrieving doc w/out xattr")
		assert.Nil(t, key3DocResult)
		assert.Equal(t, "1-1234", key3XattrResult["rev"])

		var key4DocResult map[string]interface{}
		var key4XattrResult map[string]interface{}
		_, key4err := bucket.GetWithXattr(key4, xattrName, "", &key4DocResult, &key4XattrResult, nil)
		assert.Equal(t, ErrNotFound, pkgerrors.Cause(key4err))
		assert.Nil(t, key4DocResult)
		assert.Nil(t, key4XattrResult)
	})

}

// TestXattrMutateDocAndXattr.  Validates mutation of doc + xattr in various possible previous states of the document.
func TestXattrMutateDocAndXattr(t *testing.T) {

	SkipXattrTestsIfNotEnabled(t)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key1 := t.Name() + "DocExistsXattrExists"
		key2 := t.Name() + "DocExistsNoXattr"
		key3 := t.Name() + "XattrExistsNoDoc"
		key4 := t.Name() + "NoDocNoXattr"

		// 1. Create document with XATTR
		val := make(map[string]interface{})
		val["type"] = key1

		xattrName := SyncXattrName
		xattrVal := make(map[string]interface{})
		xattrVal["seq"] = 123
		xattrVal["rev"] = "1-1234"

		var err error

		// Create w/ XATTR
		cas1 := uint64(0)
		cas1, err = bucket.WriteCasWithXattr(key1, xattrName, 0, cas1, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}

		// 2. Create document with no XATTR
		val = make(map[string]interface{})
		val["type"] = key2
		cas2, err := bucket.WriteCas(key2, 0, 0, 0, val, 0)
		require.NoError(t, err)

		// 3. Xattr, no document
		val = make(map[string]interface{})
		val["type"] = key3

		xattrVal = make(map[string]interface{})
		xattrVal["seq"] = 456
		xattrVal["rev"] = "1-1234"

		// Create w/ XATTR
		cas3int := uint64(0)
		cas3int, err = bucket.WriteCasWithXattr(key3, xattrName, 0, cas3int, nil, val, xattrVal)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}
		// Delete the doc body
		var cas3 gocb.Cas
		err = bucket.Delete(key3)
		if err != nil {
			t.Errorf("Error removing doc body: %+v", err)
		}

		// 4. No xattr, no document
		cas4 := 0
		updatedVal := make(map[string]interface{})
		updatedVal["type"] = "updated"

		updatedXattrVal := make(map[string]interface{})
		updatedXattrVal["seq"] = 123
		updatedXattrVal["rev"] = "2-1234"

		// Attempt to mutate all 4 docs
		exp := uint32(0)
		updatedVal["type"] = fmt.Sprintf("updated_%s", key1)
		_, key1err := bucket.WriteCasWithXattr(key1, xattrName, exp, cas1, nil, &updatedVal, &updatedXattrVal)
		assert.NoError(t, key1err, fmt.Sprintf("Unexpected error mutating %s", key1))
		var key1DocResult map[string]interface{}
		var key1XattrResult map[string]interface{}
		_, key1err = bucket.GetWithXattr(key1, xattrName, "", &key1DocResult, &key1XattrResult, nil)
		assert.Equal(t, fmt.Sprintf("updated_%s", key1), key1DocResult["type"])
		assert.Equal(t, "2-1234", key1XattrResult["rev"])

		updatedVal["type"] = fmt.Sprintf("updated_%s", key2)
		_, key2err := bucket.WriteCasWithXattr(key2, xattrName, exp, uint64(cas2), nil, &updatedVal, &updatedXattrVal)
		assert.NoError(t, key2err, fmt.Sprintf("Unexpected error mutating %s", key2))
		var key2DocResult map[string]interface{}
		var key2XattrResult map[string]interface{}
		_, key2err = bucket.GetWithXattr(key2, xattrName, "", &key2DocResult, &key2XattrResult, nil)
		assert.Equal(t, fmt.Sprintf("updated_%s", key2), key2DocResult["type"])
		assert.Equal(t, "2-1234", key2XattrResult["rev"])

		updatedVal["type"] = fmt.Sprintf("updated_%s", key3)
		_, key3err := bucket.WriteCasWithXattr(key3, xattrName, exp, uint64(cas3), nil, &updatedVal, &updatedXattrVal)
		assert.NoError(t, key3err, fmt.Sprintf("Unexpected error mutating %s", key3))
		var key3DocResult map[string]interface{}
		var key3XattrResult map[string]interface{}
		_, key3err = bucket.GetWithXattr(key3, xattrName, "", &key3DocResult, &key3XattrResult, nil)
		assert.Equal(t, fmt.Sprintf("updated_%s", key3), key3DocResult["type"])
		assert.Equal(t, "2-1234", key3XattrResult["rev"])

		updatedVal["type"] = fmt.Sprintf("updated_%s", key4)
		_, key4err := bucket.WriteCasWithXattr(key4, xattrName, exp, uint64(cas4), nil, &updatedVal, &updatedXattrVal)
		assert.NoError(t, key4err, fmt.Sprintf("Unexpected error mutating %s", key4))
		var key4DocResult map[string]interface{}
		var key4XattrResult map[string]interface{}
		_, key4err = bucket.GetWithXattr(key4, xattrName, "", &key4DocResult, &key4XattrResult, nil)
		assert.Equal(t, fmt.Sprintf("updated_%s", key4), key4DocResult["type"])
		assert.Equal(t, "2-1234", key4XattrResult["rev"])
	})

}

func TestGetXattr(t *testing.T) {
	SkipXattrTestsIfNotEnabled(t)

	SetUpTestLogging(t, LevelDebug, KeyAll)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		//Doc 1
		key1 := t.Name() + "DocExistsXattrExists"
		val1 := make(map[string]interface{})
		val1["type"] = key1
		xattrName1 := "sync"
		xattrVal1 := make(map[string]interface{})
		xattrVal1["seq"] = float64(1)
		xattrVal1["rev"] = "1-foo"

		//Doc 2 - Tombstone
		key2 := t.Name() + "TombstonedDocXattrExists"
		val2 := make(map[string]interface{})
		val2["type"] = key2
		xattrVal2 := make(map[string]interface{})
		xattrVal2["seq"] = float64(1)
		xattrVal2["rev"] = "1-foo"

		//Doc 3 - To Delete
		key3 := t.Name() + "DeletedDocXattrExists"
		val3 := make(map[string]interface{})
		val3["type"] = key3
		xattrName3 := "sync"
		xattrVal3 := make(map[string]interface{})
		xattrVal3["seq"] = float64(1)
		xattrVal3["rev"] = "1-foo"

		var err error

		//Create w/ XATTR
		cas := uint64(0)
		cas, err = bucket.WriteCasWithXattr(key1, xattrName1, 0, cas, nil, val1, xattrVal1)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}

		var response map[string]interface{}

		subdocStore, ok := AsSubdocXattrStore(bucket)

		//Get Xattr From Existing Doc with Existing Xattr
		_, err = bucket.GetXattr(key1, xattrName1, &response)
		assert.NoError(t, err)

		assert.Equal(t, xattrVal1["seq"], response["seq"])
		assert.Equal(t, xattrVal1["rev"], response["rev"])

		//Get Xattr From Existing Doc With Non-Existent Xattr -> ErrSubDocBadMulti
		_, err = bucket.GetXattr(key1, "non-exist", &response)
		assert.Error(t, err)
		assert.Equal(t, ErrXattrNotFound, pkgerrors.Cause(err))

		//Get Xattr From Non-Existent Doc With Non-Existent Xattr
		_, err = bucket.GetXattr("non-exist", "non-exist", &response)
		assert.Error(t, err)
		assert.Equal(t, ErrNotFound, pkgerrors.Cause(err))

		//Get Xattr From Tombstoned Doc With Existing System Xattr (ErrSubDocSuccessDeleted)
		cas, err = bucket.WriteCasWithXattr(key2, SyncXattrName, 0, uint64(0), nil, val2, xattrVal2)
		_, err = bucket.Remove(key2, cas)
		require.NoError(t, err)
		_, err = bucket.GetXattr(key2, SyncXattrName, &response)
		assert.NoError(t, err)

		//Get Xattr From Tombstoned Doc With Non-Existent System Xattr -> SubDocMultiPathFailureDeleted
		_, err = bucket.GetXattr(key2, "_non-exist", &response)
		assert.Error(t, err)
		assert.Equal(t, ErrXattrNotFound, pkgerrors.Cause(err))

		//Get Xattr and Body From Tombstoned Doc With Non-Existent System Xattr -> SubDocMultiPathFailureDeleted
		var v, xv, userXv map[string]interface{}
		require.True(t, ok)
		_, err = subdocStore.SubdocGetBodyAndXattr(key2, "_non-exist", "", &v, &xv, &userXv)
		assert.Error(t, err)
		assert.Equal(t, ErrNotFound, pkgerrors.Cause(err))

		////Get Xattr From Tombstoned Doc With Deleted User Xattr
		cas, err = bucket.WriteCasWithXattr(key3, xattrName3, 0, uint64(0), nil, val3, xattrVal3)
		_, err = bucket.Remove(key3, cas)
		require.NoError(t, err)
		_, err = bucket.GetXattr(key3, xattrName3, &response)
		assert.Error(t, err)
		assert.Equal(t, ErrXattrNotFound, pkgerrors.Cause(err))
	})
}

func TestGetXattrAndBody(t *testing.T) {
	SkipXattrTestsIfNotEnabled(t)

	SetUpTestLogging(t, LevelDebug, KeyAll)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		//Doc 1
		key1 := t.Name() + "DocExistsXattrExists"
		val1 := make(map[string]interface{})
		val1["type"] = key1
		xattrName1 := "sync"
		xattrVal1 := make(map[string]interface{})
		xattrVal1["seq"] = float64(1)
		xattrVal1["rev"] = "1-foo"

		//Doc 2 - Tombstone
		key2 := t.Name() + "TombstonedDocXattrExists"
		val2 := make(map[string]interface{})
		val2["type"] = key2
		xattrVal2 := make(map[string]interface{})
		xattrVal2["seq"] = float64(1)
		xattrVal2["rev"] = "1-foo"

		//Doc 3 - To Delete
		key3 := t.Name() + "DeletedDocXattrExists"
		val3 := make(map[string]interface{})
		val3["type"] = key3
		xattrName3 := "sync"
		xattrVal3 := make(map[string]interface{})
		xattrVal3["seq"] = float64(1)
		xattrVal3["rev"] = "1-foo"

		var err error

		//Create w/ XATTR
		cas := uint64(0)
		cas, err = bucket.WriteCasWithXattr(key1, xattrName1, 0, cas, nil, val1, xattrVal1)
		if err != nil {
			t.Errorf("Error doing WriteCasWithXattr: %+v", err)
		}

		subdocStore, ok := AsSubdocXattrStore(bucket)
		require.True(t, ok)

		//Get Xattr From Existing Doc with Existing Xattr
		var v, xv, userXv map[string]interface{}
		_, err = subdocStore.SubdocGetBodyAndXattr(key1, xattrName1, "", &v, &xv, &userXv)
		assert.NoError(t, err)

		assert.Equal(t, xattrVal1["seq"], xv["seq"])
		assert.Equal(t, xattrVal1["rev"], xv["rev"])

		//Get body and Xattr From Existing Doc With Non-Existent Xattr -> returns body only
		_, err = subdocStore.SubdocGetBodyAndXattr(key1, "non-exist", "", &v, &xv, &userXv)
		assert.NoError(t, err)
		assert.Equal(t, val1["type"], v["type"])

		//Get Xattr From Non-Existent Doc With Non-Existent Xattr
		_, err = subdocStore.SubdocGetBodyAndXattr("non-exist", "non-exist", "", &v, &xv, &userXv)
		assert.Error(t, err)
		assert.Equal(t, ErrNotFound, pkgerrors.Cause(err))

		//Get Xattr From Tombstoned Doc With Existing System Xattr (ErrSubDocSuccessDeleted)
		cas, err = bucket.WriteCasWithXattr(key2, SyncXattrName, 0, uint64(0), nil, val2, xattrVal2)
		_, err = bucket.Remove(key2, cas)
		require.NoError(t, err)
		_, err = subdocStore.SubdocGetBodyAndXattr(key2, SyncXattrName, "", &v, &xv, &userXv)
		assert.NoError(t, err)

		//Get Xattr From Tombstoned Doc With Non-Existent System Xattr -> returns not found
		_, err = subdocStore.SubdocGetBodyAndXattr(key2, "_non-exist", "", &v, &xv, &userXv)
		assert.Error(t, err)
		assert.Equal(t, ErrNotFound, pkgerrors.Cause(err))

		////Get Xattr From Tombstoned Doc With Deleted User Xattr -> returns not found
		cas, err = bucket.WriteCasWithXattr(key3, xattrName3, 0, uint64(0), nil, val3, xattrVal3)
		_, err = bucket.Remove(key3, cas)
		require.NoError(t, err)
		_, err = subdocStore.SubdocGetBodyAndXattr(key3, xattrName3, "", &v, &xv, &userXv)
		assert.Error(t, err)
		assert.Equal(t, ErrNotFound, pkgerrors.Cause(err))
	})
}

func TestApplyViewQueryOptions(t *testing.T) {

	// ------------------- Inline Helper functions ---------------------------

	// Given a string "foo", return ""foo"" with an extra set of double quotes added
	// This is to be in line with gocb's behavior of wrapping these startkey, endkey in an extra set of double quotes
	wrapInDoubleQuotes := func(original string) string {
		return fmt.Sprintf("\"%v\"\n", original)
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
		ViewQueryParamKeys:          []interface{}{"a", "b"},
	}

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
	assert.Equal(t, "false", findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamStale))

	// "reduce"
	assert.Equal(t, "true", findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamReduce))

	// "startkey"
	assert.Equal(t, wrapInDoubleQuotes("foo"), findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamStartKey))

	// "endkey"
	assert.Equal(t, wrapInDoubleQuotes("bar"), findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamEndKey))

	// "inclusive_end"
	assert.Equal(t, "true", findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamInclusiveEnd))

	// "limit"
	assert.Equal(t, "1", findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamLimit))

	// "descending"
	assert.Equal(t, "true", findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamDescending))

	// "group"
	assert.Equal(t, "true", findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamGroup))

	// "skip"
	assert.Equal(t, "2", findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamSkip))

	// "group_level"
	assert.Equal(t, "3", findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamGroupLevel))

	// "startkey_docid"
	assert.Equal(t, "baz", findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamStartKeyDocId))

	// "endkey_docid"
	assert.Equal(t, "blah", findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamEndKeyDocId))

	// "key"
	assert.Equal(t, wrapInDoubleQuotes("hello"), findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamKey))

	// "keys"
	assert.Equal(t,

		"[\"a\",\"b\"]\n", findStringValue(mapKeys, optionsReflectedVal, ViewQueryParamKeys))

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

// Validate non-bool stale handling
func TestApplyViewQueryStaleOptions(t *testing.T) {

	// View query params map (go-couchbase/walrus style)
	params := map[string]interface{}{
		ViewQueryParamStale: "false",
	}

	// Create a new viewquery
	viewQuery := gocb.NewViewQuery("ddoc", "viewname")

	// if it doesn't blow up, test passes
	if err := applyViewQueryOptions(viewQuery, params); err != nil {
		t.Fatalf("Error calling applyViewQueryOptions: %v", err)
	}

	params = map[string]interface{}{
		ViewQueryParamStale: "ok",
	}

	// Create a new viewquery
	viewQuery = gocb.NewViewQuery("ddoc", "viewname")

	if err := applyViewQueryOptions(viewQuery, params); err != nil {
		t.Fatalf("Error calling applyViewQueryOptions: %v", err)
	}

}

func TestCouchbaseServerMaxTTL(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	bucket := GetTestBucket(t)
	defer bucket.Close()

	cbStore, ok := AsCouchbaseStore(bucket)
	require.True(t, ok)
	maxTTL, err := cbStore.MaxTTL()
	assert.NoError(t, err, "Unexpected error")
	assert.Equal(t, 0, maxTTL)

}

func TestCouchbaseServerIncorrectLogin(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	testBucket := GetTestBucket(t)
	defer testBucket.Close()

	// Override test bucket spec with invalid creds
	testBucket.BucketSpec.Auth = TestAuthenticator{
		Username:   "invalid_username",
		Password:   "invalid_password",
		BucketName: testBucket.BucketSpec.BucketName,
	}

	// Attempt to open the bucket again using invalid creds. We should expect an error.
	bucket, err := GetBucket(testBucket.BucketSpec)
	assert.Equal(t, ErrAuthError, err)
	assert.Nil(t, bucket)
}

// TestCouchbaseServerIncorrectX509Login tries to open a bucket using an example X509 Cert/Key
// to make sure that we get back a "no access" error.
func TestCouchbaseServerIncorrectX509Login(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	if TestClusterDriver() == GoCBv2 {
		t.Skip("This test doesn't work using GoCBv2")
	}

	testBucket := GetTestBucket(t)
	defer testBucket.Close()

	// Remove existing password-based authentication
	testBucket.BucketSpec.Auth = nil

	// Force use of TLS so we are able to use X509 by stripping protocol and using couchbases://
	if !strings.HasPrefix(testBucket.BucketSpec.Server, "couchbases://") {
		for _, proto := range []string{"http://", "couchbase://", ""} {
			if !strings.HasPrefix(testBucket.BucketSpec.Server, proto) {
				continue
			}
			testBucket.BucketSpec.Server = "couchbases://" + testBucket.BucketSpec.Server[len(proto):]
			testBucket.BucketSpec.Server = strings.TrimSuffix(testBucket.BucketSpec.Server, ":8091")
			break
		}
	}

	// Set CertPath/KeyPath for X509 auth
	certPath, keyPath, x509CleanupFn := tempX509Certs(t)
	testBucket.BucketSpec.Certpath = certPath
	testBucket.BucketSpec.Keypath = keyPath

	// Attempt to open a test bucket with invalid certs
	bucket, err := GetBucket(testBucket.BucketSpec)

	// We no longer need the cert files, so go ahead and clean those up now before any assertions stop the test.
	x509CleanupFn()

	// Make sure we get an error when opening the bucket
	require.Error(t, err)

	// And also check it's an access error
	errCause := pkgerrors.Cause(err)
	kvErr, ok := errCause.(*gocbcore.KvError)
	require.Truef(t, ok, "Expected error type gocbcore.KvError, but got %#v", errCause)
	assert.Equal(t, gocbcore.StatusAccessError, kvErr.Code)

	assert.Nil(t, bucket)
}

// tempX509Certs creates temporary files for an example X509 cert and key
// which returns the path to those files, and a cleanupFn to remove the files.
func tempX509Certs(t *testing.T) (certPath, keyPath string, cleanupFn func()) {
	tmpDir := os.TempDir()

	// The contents of these certificates was taken directly from Go's tls package examples.
	certFile, err := ioutil.TempFile(tmpDir, "x509-cert")
	require.NoError(t, err)
	_, err = certFile.Write([]byte(`-----BEGIN CERTIFICATE-----
MIIBhTCCASugAwIBAgIQIRi6zePL6mKjOipn+dNuaTAKBggqhkjOPQQDAjASMRAw
DgYDVQQKEwdBY21lIENvMB4XDTE3MTAyMDE5NDMwNloXDTE4MTAyMDE5NDMwNlow
EjEQMA4GA1UEChMHQWNtZSBDbzBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABD0d
7VNhbWvZLWPuj/RtHFjvtJBEwOkhbN/BnnE8rnZR8+sbwnc/KhCk3FhnpHZnQz7B
5aETbbIgmuvewdjvSBSjYzBhMA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggr
BgEFBQcDATAPBgNVHRMBAf8EBTADAQH/MCkGA1UdEQQiMCCCDmxvY2FsaG9zdDo1
NDUzgg4xMjcuMC4wLjE6NTQ1MzAKBggqhkjOPQQDAgNIADBFAiEA2zpJEPQyz6/l
Wf86aX6PepsntZv2GYlA5UpabfT2EZICICpJ5h/iI+i341gBmLiAFQOyTDT+/wQc
6MF9+Yw1Yy0t
-----END CERTIFICATE-----`))
	require.NoError(t, err)
	_ = certFile.Close()

	keyFile, err := ioutil.TempFile(tmpDir, "x509-key")
	require.NoError(t, err)
	_, err = keyFile.Write([]byte(`-----BEGIN EC PRIVATE KEY-----
MHcCAQEEIIrYSSNQFaA2Hwf1duRSxKtLYX5CB04fSeQ6tF1aY/PuoAoGCCqGSM49
AwEHoUQDQgAEPR3tU2Fta9ktY+6P9G0cWO+0kETA6SFs38GecTyudlHz6xvCdz8q
EKTcWGekdmdDPsHloRNtsiCa697B2O9IFA==
-----END EC PRIVATE KEY-----`))
	require.NoError(t, err)
	_ = keyFile.Close()

	certPath = certFile.Name()
	keyPath = keyFile.Name()
	cleanupFn = func() {
		_ = os.Remove(certPath)
		_ = os.Remove(keyPath)
	}

	return certPath, keyPath, cleanupFn
}

func createTombstonedDoc(bucket sgbucket.DataStore, key, xattrName string) {

	// Create document with XATTR

	val := make(map[string]interface{})
	val["body_field"] = "1234"

	xattrVal := make(map[string]interface{})
	xattrVal["seq"] = 123
	xattrVal["rev"] = "1-1234"

	_, _, err := bucket.GetRaw(key)
	if err == nil {
		panic(fmt.Sprintf("Expected empty bucket"))
	}

	// Create w/ doc and XATTR
	cas := uint64(0)
	cas, err = bucket.WriteCasWithXattr(key, xattrName, 0, cas, nil, val, xattrVal)
	if err != nil {
		panic(fmt.Sprintf("Error doing WriteCasWithXattr: %+v", err))
	}

	subdocStore, _ := AsSubdocXattrStore(bucket)
	// Create tombstone revision which deletes doc body but preserves XATTR
	_, mutateErr := subdocStore.SubdocDeleteBody(key, xattrName, 0, cas)
	/*
		flags := gocb.SubdocDocFlagAccessDeleted
		_, mutateErr := bucket.Bucket.MutateInEx(key, flags, gocb.Cas(cas), uint32(0)).
			UpsertEx(xattrName, xattrVal, gocb.SubdocFlagXattr).                                              // Update the xattr
			UpsertEx(SyncXattrName+".cas", "${Mutation.CAS}", gocb.SubdocFlagXattr|gocb.SubdocFlagUseMacros). // Stamp the cas on the xattr
			RemoveEx("", gocb.SubdocFlagNone).                                                                // Delete the document body
			Execute()
	*/
	if mutateErr != nil {
		panic(fmt.Sprintf("Unexpected mutateErr: %v", mutateErr))
	}

	// Verify delete of body and XATTR
	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	_, err = bucket.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)

	if len(retrievedVal) != 0 {
		panic(fmt.Sprintf("len(retrievedVal) should be 0"))
	}

	if retrievedXattr["seq"] != float64(123) {
		panic(fmt.Sprintf("retrievedXattr[seq] should be 123"))
	}

}

func verifyDocAndXattrDeleted(store sgbucket.XattrStore, key, xattrName string) bool {
	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	_, err := store.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)
	notFound := pkgerrors.Cause(err) == ErrNotFound

	return notFound
}

func verifyDocDeletedXattrExists(store sgbucket.XattrStore, key, xattrName string) bool {
	var retrievedVal map[string]interface{}
	var retrievedXattr map[string]interface{}
	_, err := store.GetWithXattr(key, xattrName, "", &retrievedVal, &retrievedXattr, nil)

	log.Printf("verification for key: %s   body: %s  xattr: %s", key, retrievedVal, retrievedXattr)
	if err != nil || len(retrievedVal) > 0 || len(retrievedXattr) == 0 {
		return false
	}
	return true
}

func TestUpdateXattrWithDeleteBodyAndIsDelete(t *testing.T) {
	SkipXattrTestsIfNotEnabled(t)
	SetUpTestLogging(t, LevelDebug, KeyCRUD)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		subdocXattrStore, ok := AsSubdocXattrStore(bucket)
		require.True(t, ok)

		// Create a document with extended attributes
		key := "DocWithXattrAndIsDelete"
		val := make(map[string]interface{})
		val["type"] = key

		xattrKey := SyncXattrName
		xattrVal := make(map[string]interface{})
		xattrVal["seq"] = 123
		xattrVal["rev"] = "1-EmDC"

		cas := uint64(0)
		// CAS-safe write of the document and it's associated named extended attributes
		cas, err := bucket.WriteCasWithXattr(key, xattrKey, 0, cas, nil, val, xattrVal)
		require.NoError(t, err, "Error doing WriteCasWithXattr")

		updatedXattrVal := make(map[string]interface{})
		updatedXattrVal["seq"] = 123
		updatedXattrVal["rev"] = "2-EmDC"

		// Attempt to delete the document body (deleteBody = true); isDelete is true to mark this doc as a tombstone.
		_, errDelete := UpdateTombstoneXattr(subdocXattrStore, key, xattrKey, 0, cas, &updatedXattrVal, true)
		assert.NoError(t, errDelete, fmt.Sprintf("Unexpected error deleting %s", key))
		assert.True(t, verifyDocDeletedXattrExists(bucket, key, xattrKey), fmt.Sprintf("Expected doc %s to be deleted", key))

		var docResult map[string]interface{}
		var xattrResult map[string]interface{}
		_, err = bucket.GetWithXattr(key, xattrKey, "", &docResult, &xattrResult, nil)
		assert.NoError(t, err)
		assert.Len(t, docResult, 0)
		assert.Equal(t, "2-EmDC", xattrResult["rev"])
		assert.Equal(t, "0x00000000", xattrResult[xattrMacroValueCrc32c])
	})
}

func TestUserXattrGetWithXattr(t *testing.T) {
	SkipXattrTestsIfNotEnabled(t)
	SetUpTestLogging(t, LevelDebug, KeyCRUD)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		userXattrStore, ok := AsUserXattrStore(bucket)
		require.True(t, ok)

		docKey := t.Name()

		docVal := map[string]interface{}{"val": "docVal"}
		syncXattrVal := map[string]interface{}{"val": "syncVal"}
		userXattrVal := map[string]interface{}{"val": "userXattrVal"}

		err := bucket.Set(docKey, 0, nil, docVal)
		assert.NoError(t, err)

		_, err = userXattrStore.WriteUserXattr(docKey, "_sync", syncXattrVal)
		assert.NoError(t, err)

		_, err = userXattrStore.WriteUserXattr(docKey, "test", userXattrVal)
		assert.NoError(t, err)

		var docValRet, syncXattrValRet, userXattrValRet map[string]interface{}
		_, err = bucket.GetWithXattr(docKey, SyncXattrName, "test", &docValRet, &syncXattrValRet, &userXattrValRet)
		assert.NoError(t, err)
		assert.Equal(t, docVal, docValRet)
		assert.Equal(t, syncXattrVal, syncXattrValRet)
		assert.Equal(t, userXattrVal, userXattrValRet)
	})
}

func TestUserXattrGetWithXattrNil(t *testing.T) {
	SkipXattrTestsIfNotEnabled(t)
	SetUpTestLogging(t, LevelDebug, KeyCRUD)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		docKey := t.Name()

		docVal := map[string]interface{}{"val": "docVal"}
		syncXattrVal := map[string]interface{}{"val": "syncVal"}

		err := bucket.Set(docKey, 0, nil, docVal)
		assert.NoError(t, err)

		userXattrStore, ok := AsUserXattrStore(bucket)
		require.True(t, ok)
		_, err = userXattrStore.WriteUserXattr(docKey, "_sync", syncXattrVal)
		assert.NoError(t, err)

		var docValRet, syncXattrValRet, userXattrValRet map[string]interface{}
		_, err = bucket.GetWithXattr(docKey, SyncXattrName, "test", &docValRet, &syncXattrValRet, &userXattrValRet)
		assert.NoError(t, err)
		assert.Equal(t, docVal, docValRet)
		assert.Equal(t, syncXattrVal, syncXattrValRet)
	})
}

func TestInsertTombstoneWithXattr(t *testing.T) {
	SkipXattrTestsIfNotEnabled(t)
	SetUpTestLogging(t, LevelDebug, KeyCRUD)

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		subdocXattrStore, ok := AsSubdocXattrStore(bucket)
		require.True(t, ok)

		// Create a document with extended attributes
		key := "InsertedTombstoneDoc"
		val := make(map[string]interface{})
		val["type"] = key

		xattrKey := SyncXattrName
		xattrVal := make(map[string]interface{})
		xattrVal["seq"] = 123
		xattrVal["rev"] = "1-EmDC"

		cas := uint64(0)
		// Attempt to delete the document body (deleteBody = true); isDelete is true to mark this doc as a tombstone.
		_, errDelete := UpdateTombstoneXattr(subdocXattrStore, key, xattrKey, 0, cas, &xattrVal, false)
		assert.NoError(t, errDelete, fmt.Sprintf("Unexpected error deleting %s", key))
		assert.True(t, verifyDocDeletedXattrExists(bucket, key, xattrKey), fmt.Sprintf("Expected doc %s to be deleted", key))

		var docResult map[string]interface{}
		var xattrResult map[string]interface{}
		_, err := bucket.GetWithXattr(key, xattrKey, "", &docResult, &xattrResult, nil)
		assert.NoError(t, err)
		assert.Len(t, docResult, 0)
		assert.Equal(t, "1-EmDC", xattrResult["rev"])
		assert.Equal(t, "0x00000000", xattrResult[xattrMacroValueCrc32c])
	})
}

// TestRawBackwardCompatibilityFromJSON ensures that bucket implementation handles the case
// where legacy SG versions set incorrect data types:
//    - write as JSON, read as binary, (re-)write as binary
func TestRawBackwardCompatibilityFromJSON(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("RawBackwardCompatibility tests depend on couchbase transcoding")
	}

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key := t.Name()
		val := []byte(`{"foo":"bar"}`)
		updatedVal := []byte(`{"foo":"bars"}`)

		var body []byte
		_, err := bucket.Get(key, &body)
		if err == nil {
			t.Errorf("Key should not exist yet, expected error but got nil")
		}

		// Write as JSON
		setErr := bucket.Set(key, 0, nil, val)
		assert.NoError(t, setErr)

		// Read as binary
		rv, _, getRawErr := bucket.GetRaw(key)
		assert.NoError(t, getRawErr)
		if string(rv) != string(val) {
			t.Errorf("%v != %v", string(rv), string(val))
		}

		// Write as binary
		setRawErr := bucket.SetRaw(key, 0, nil, updatedVal)
		assert.NoError(t, setRawErr)

	})
}

// TestRawBackwardCompatibilityFromBinary ensures that bucket implementation handles the case
// where legacy SG versions set incorrect data types:
//    - write as binary, read as raw JSON, rewrite as raw JSON
func TestRawBackwardCompatibilityFromBinary(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("RawBackwardCompatibility tests depend on couchbase transcoding")
	}

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		key := t.Name()
		val := []byte(`{{"foo":"bar"}`)
		updatedVal := []byte(`{"foo":"bars"}`)

		var body []byte
		_, err := bucket.Get(key, &body)
		if err == nil {
			t.Errorf("Key should not exist yet, expected error but got nil")
		}

		// Write as binary
		err = bucket.SetRaw(key, 0, nil, val)
		assert.NoError(t, err)

		// Read as raw JSON
		var rv []byte
		_, getErr := bucket.Get(key, &rv)
		assert.NoError(t, getErr)
		if string(rv) != string(val) {
			t.Errorf("%v != %v", string(rv), string(val))
		}

		// Write as raw JSON
		setErr := bucket.Set(key, 0, nil, updatedVal)
		assert.NoError(t, setErr)

	})
}

func TestGetExpiry(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("Walrus doesn't support expiry")
	}

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		store, ok := AsCouchbaseStore(bucket)
		assert.True(t, ok)

		key := t.Name()
		val := make(map[string]interface{}, 0)
		val["foo"] = "bar"

		expiryValue := uint32(time.Now().Add(1 * time.Minute).Unix())
		err := bucket.Set(key, expiryValue, nil, val)
		assert.NoError(t, err, "Error calling Set()")

		expiry, expiryErr := store.GetExpiry(key)
		assert.NoError(t, expiryErr)

		// gocb v2 expiry does an expiry-to-duration conversion which results in non-exact equality,
		// so check whether it's within 30s
		assert.True(t, DiffUint32(expiryValue, expiry) < 30)
		log.Printf("expiryValue: %d", expiryValue)
		log.Printf("expiry: %d", expiry)

		err = bucket.Delete(key)
		if err != nil {
			t.Errorf("Error removing key from bucket")
		}

		// ensure expiry retrieval on tombstone doesn't return error
		tombstoneExpiry, tombstoneExpiryErr := store.GetExpiry(key)
		assert.NoError(t, tombstoneExpiryErr)
		log.Printf("tombstoneExpiry: %d", tombstoneExpiry)

		// ensure expiry retrieval on non-existent doc returns key not found
		_, nonExistentExpiryErr := store.GetExpiry("nonExistentKey")
		assert.Error(t, nonExistentExpiryErr)
		assert.True(t, IsKeyNotFoundError(bucket, nonExistentExpiryErr))

	})
}

func TestGetStatsVbSeqNo(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("Walrus doesn't support stats-vbseqno")
	}

	ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		maxVbNo, err := bucket.GetMaxVbno()
		assert.NoError(t, err)

		store, ok := AsCouchbaseStore(bucket)
		assert.True(t, ok)

		// Write docs to increment vbseq in at least one vbucket
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("doc%d", i)
			value := map[string]interface{}{"k": "v"}
			ok, err := bucket.Add(key, 0, value)
			require.NoError(t, err)
			assert.True(t, ok)
		}

		uuids, highSeqNos, statsErr := store.GetStatsVbSeqno(maxVbNo, false)
		assert.NoError(t, statsErr)

		assert.NotNil(t, uuids)
		assert.NotNil(t, highSeqNos)
		assert.True(t, len(uuids) > 0)
		assert.True(t, len(highSeqNos) > 0)
	})
}

// Confirm that GoCBv2 preserveExpiry option works correctly for bucket Set function
func TestUpsertOptionPreserveExpiry(t *testing.T) {
	if !TestUseCouchbaseServer() {
		t.Skip("Test can only be ran against CBS due to GoCB v2 use")
	}
	bucket := GetTestBucketForDriver(t, GoCBv2)
	defer bucket.Close()
	if bucket.IsSupported(sgbucket.DataStoreFeaturePreserveExpiry) {
		t.Skip("Preserve expiry is not supported with this CBS version. Skipping test...")
	}
	SetUpTestLogging(t, LevelInfo, KeyAll)

	testCases := []struct {
		name          string
		upsertOptions *sgbucket.UpsertOptions
		expectMatch   bool
	}{
		{
			name:          "Expect matching expiry - preserveExpiry",
			upsertOptions: &sgbucket.UpsertOptions{PreserveExpiry: true},
			expectMatch:   true,
		},
		{
			name:          "Expect updated expiry - false preserveExpiry",
			upsertOptions: &sgbucket.UpsertOptions{PreserveExpiry: false},
			expectMatch:   false,
		},
		{
			name:          "Expect updated expiry - nil upsert options",
			upsertOptions: nil,
			expectMatch:   false,
		},
	}

	for i, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			cbStore, _ := AsCouchbaseStore(bucket)
			key := fmt.Sprintf("test%d", i)
			val := make(map[string]interface{}, 0)
			val["foo"] = "bar"

			var rVal map[string]interface{}
			_, err := bucket.Get(key, &rVal)
			assert.Error(t, err, "Key should not exist yet, expected error but got nil")

			err = bucket.Set(key, DurationToCbsExpiry(time.Hour*24), nil, val)
			assert.NoError(t, err, "Error calling Set()")

			beforeExp, err := cbStore.GetExpiry(key)
			require.NoError(t, err)
			require.NotEqual(t, 0, beforeExp)

			val["foo"] = "baz"
			err = bucket.Set(key, 0, test.upsertOptions, val)
			assert.NoError(t, err, "Error calling Set()")

			afterExp, err := cbStore.GetExpiry(key)
			assert.NoError(t, err)
			if test.expectMatch {
				assert.Equal(t, beforeExp, afterExp) // Make sure both expiry timestamps match
			} else {
				assert.NotEqual(t, beforeExp, afterExp) // Make sure both expiry timestamps do not match
			}

			err = bucket.Delete(key)
			require.NoError(t, err)
		})
	}
}
