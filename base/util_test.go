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
	"log"
	"testing"

	"github.com/couchbaselabs/go.assert"
	"net/url"
)

func TestFixJSONNumbers(t *testing.T) {
	assert.DeepEquals(t, FixJSONNumbers(1), 1)
	assert.DeepEquals(t, FixJSONNumbers(float64(1.23)), float64(1.23))
	assert.DeepEquals(t, FixJSONNumbers(float64(123456)), int64(123456))
	assert.DeepEquals(t, FixJSONNumbers(float64(123456789)), int64(123456789))
	assert.DeepEquals(t, FixJSONNumbers(float64(12345678901234567890)),
		float64(12345678901234567890))
	assert.DeepEquals(t, FixJSONNumbers("foo"), "foo")
	assert.DeepEquals(t, FixJSONNumbers([]interface{}{1, float64(123456)}),
		[]interface{}{1, int64(123456)})
	assert.DeepEquals(t, FixJSONNumbers(map[string]interface{}{"foo": float64(123456)}),
		map[string]interface{}{"foo": int64(123456)})
}

func TestBackQuotedStrings(t *testing.T) {
	input := `{"foo": "bar"}`
	output := ConvertBackQuotedStrings([]byte(input))
	assert.Equals(t, string(output), input)

	input = "{\"foo\": `bar`}"
	output = ConvertBackQuotedStrings([]byte(input))
	assert.Equals(t, string(output), `{"foo": "bar"}`)

	input = "{\"foo\": `bar\nbaz\nboo`}"
	output = ConvertBackQuotedStrings([]byte(input))
	assert.Equals(t, string(output), `{"foo": "bar\nbaz\nboo"}`)

	input = "{\"foo\": `bar\n\"baz\n\tboo`}"
	output = ConvertBackQuotedStrings([]byte(input))
	assert.Equals(t, string(output), `{"foo": "bar\n\"baz\n\tboo"}`)

	input = "{\"foo\": `bar\n`, \"baz\": `howdy`}"
	output = ConvertBackQuotedStrings([]byte(input))
	assert.Equals(t, string(output), `{"foo": "bar\n", "baz": "howdy"}`)

	input = "{\"foo\": `bar\r\n`, \"baz\": `\r\nhowdy`}"
	output = ConvertBackQuotedStrings([]byte(input))
	assert.Equals(t, string(output), `{"foo": "bar\n", "baz": "\nhowdy"}`)

	input = "{\"foo\": `bar\\baz`, \"something\": `else\\is\\here`}"
	output = ConvertBackQuotedStrings([]byte(input))
	assert.Equals(t, string(output), `{"foo": "bar\\baz", "something": "else\\is\\here"}`)
}

func TestCouchbaseUrlWithAuth(t *testing.T) {

	// normal bucket
	result, err := CouchbaseUrlWithAuth(
		"http://127.0.0.1:8091",
		"username",
		"password",
		"bucket",
	)
	assert.True(t, err == nil)
	assert.Equals(t, result, "http://username:password@127.0.0.1:8091")

	// default bucket
	result, err = CouchbaseUrlWithAuth(
		"http://127.0.0.1:8091",
		"",
		"",
		"default",
	)
	assert.True(t, err == nil)
	assert.Equals(t, result, "http://127.0.0.1:8091")

}

func TestCreateDoublingSleeperFunc(t *testing.T) {

	maxNumAttempts := 2
	initialTimeToSleepMs := 1
	sleeper := CreateDoublingSleeperFunc(maxNumAttempts, initialTimeToSleepMs)

	shouldContinue, timeTosleepMs := sleeper(1)
	assert.True(t, shouldContinue)
	assert.Equals(t, timeTosleepMs, initialTimeToSleepMs)

	shouldContinue, timeTosleepMs = sleeper(2)
	assert.True(t, shouldContinue)
	assert.Equals(t, timeTosleepMs, initialTimeToSleepMs*2)

	shouldContinue, _ = sleeper(3)
	assert.False(t, shouldContinue)

}

func TestRetryLoop(t *testing.T) {

	// Make sure that the worker retries if an error is returned and shouldRetry == true

	numTimesInvoked := 0
	worker := func() (shouldRetry bool, err error, value interface{}) {
		log.Printf("Worker invoked")
		numTimesInvoked += 1
		if numTimesInvoked <= 3 {
			log.Printf("Worker returning shouldRetry true, fake error")
			return true, fmt.Errorf("Fake error"), nil
		}
		return false, nil, "result"
	}

	sleeper := func(numAttempts int) (bool, int) {
		if numAttempts > 10 {
			return false, -1
		}
		return true, 0
	}

	// Kick off retry loop
	description := fmt.Sprintf("TestRetryLoop")
	err, result := RetryLoop(description, worker, sleeper)

	// We shouldn't get an error, because it will retry a few times and then succeed
	assert.True(t, err == nil)
	assert.Equals(t, result, "result")
	assert.True(t, numTimesInvoked == 4)

}

func TestSyncSourceFromURL(t *testing.T) {
	u, err := url.Parse("http://www.test.com:4985/mydb")
	assert.True(t, err == nil)
	result := SyncSourceFromURL(u)
	assert.Equals(t, result, "http://www.test.com:4985")

	u, err = url.Parse("http://www.test.com:4984/mydb/some otherinvalidpath?query=yes#fragment")
	assert.True(t, err == nil)
	result = SyncSourceFromURL(u)
	assert.Equals(t, result, "http://www.test.com:4984")

	u, err = url.Parse("MyDB")
	assert.True(t, err == nil)
	result = SyncSourceFromURL(u)
	assert.Equals(t, result, "")
}

func TestValueToStringArray(t *testing.T) {
	result := ValueToStringArray("foobar")
	assert.DeepEquals(t, result, []string{"foobar"})

	result = ValueToStringArray([]string{"foobar", "moocar"})
	assert.DeepEquals(t, result, []string{"foobar", "moocar"})

	result = ValueToStringArray([]interface{}{"foobar", 1, true})
	assert.DeepEquals(t, result, []string{"foobar"})
}

func TestHighSeqNosToSequenceClock(t *testing.T) {

	highSeqs := map[uint16]uint64{}
	highSeqs[0] = 568
	highSeqs[1] = 98798
	highSeqs[2] = 100
	highSeqs[3] = 2
	// leave a gap and don't specify a high seq for vbno 4
	highSeqs[5] = 250


	var seqClock SequenceClock
	var err error

	seqClock, err = HighSeqNosToSequenceClock(highSeqs)

	assertNoError(t, err, "Unexpected error")

	assert.True(t, seqClock.GetSequence(0) == 568)
	assert.True(t, seqClock.GetSequence(1) == 98798)
	assert.True(t, seqClock.GetSequence(2) == 100)
	assert.True(t, seqClock.GetSequence(3) == 2)
	assert.True(t, seqClock.GetSequence(5) == 250)

}



