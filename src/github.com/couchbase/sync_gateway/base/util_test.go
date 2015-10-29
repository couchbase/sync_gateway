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
