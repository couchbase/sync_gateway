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
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

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

func TestConvertJSONString(t *testing.T) {
	assert.Equals(t, ConvertJSONString(`"blah"`), "blah")
	assert.Equals(t, ConvertJSONString("blah"), "blah")
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

// Make sure that the RetryLoopTimeout doesn't break existing RetryLoop functionality
func TestRetryLoopTimeoutSafe(t *testing.T) {

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
	err, result := RetryLoopTimeout(description, worker, sleeper, time.Hour)

	// We shouldn't get an error, because it will retry a few times and then succeed
	assert.True(t, err == nil)
	assert.Equals(t, result, "result")
	assert.True(t, numTimesInvoked == 4)

}

// Make sure that the RetryLoopTimeout enforces timeout on worker functions that block for too long
func TestRetryLoopTimeoutEffective(t *testing.T) {

	worker := func() (shouldRetry bool, err error, value interface{}) {
		// The laziest worker ever .. sleeps for a week before returning a value
		time.Sleep(time.Hour * 24 * 7)
		return false, nil, "result"
	}

	sleeper := CreateDoublingSleeperFunc(10, 100)

	// Kick off timeout loop that expects lazy worker to return in 100 ms, even though it takes a week
	description := fmt.Sprintf("TestRetryLoop")
	err, _ := RetryLoopTimeout(description, worker, sleeper, time.Millisecond*100)

	// We should get a timeout error
	assert.True(t, err != nil)
	assert.True(t, strings.Contains(err.Error(), "timeout"))

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

func TestCouchbaseURIToHttpURL(t *testing.T) {

	inputsAndExpected := []struct {
		input    string
		expected []string
	}{
		{
			input: "http://host1:8091",
			expected: []string{
				"http://host1:8091",
			},
		},
		{
			input: "http://host1,host2:8091",
			expected: []string{
				"http://host1:8091",
				"http://host2:8091",
			},
		},
		{
			input: "http://foo:bar@host1:8091",
			expected: []string{
				"http://foo:bar@host1:8091",
			},
		},
	}

	for _, inputAndExpected := range inputsAndExpected {
		actual, err := CouchbaseURIToHttpURL(nil, inputAndExpected.input)
		assertNoError(t, err, "Unexpected error")
		assert.DeepEquals(t, actual, inputAndExpected.expected)
	}

	// With a nil (or walrus bucket) and a couchbase or couchbases url, expect errors
	_, err := CouchbaseURIToHttpURL(nil, "couchbases://host1:18191,host2:18191")
	assert.True(t, err != nil)
	_, err = CouchbaseURIToHttpURL(nil, "couchbase://host1")
	assert.True(t, err != nil)

}

func TestReflectExpiry(t *testing.T) {
	exp := time.Now().Add(time.Hour)

	expiry, err := ReflectExpiry(uint(1234))
	assert.Equals(t, err.Error(), "Unrecognized expiry format")
	assert.Equals(t, expiry, (*uint32)(nil))

	expiry, err = ReflectExpiry(true)
	assert.Equals(t, err.Error(), "Unrecognized expiry format")
	assert.Equals(t, expiry, (*uint32)(nil))

	expiry, err = ReflectExpiry(int64(1234))
	assert.Equals(t, err, nil)
	assert.Equals(t, *expiry, uint32(1234))

	expiry, err = ReflectExpiry(float64(1234))
	assert.Equals(t, err, nil)
	assert.Equals(t, *expiry, uint32(1234))

	expiry, err = ReflectExpiry("1234")
	assert.Equals(t, err, nil)
	assert.Equals(t, *expiry, uint32(1234))

	expiry, err = ReflectExpiry(exp.Format(time.RFC3339))
	assert.Equals(t, err, nil)
	assert.Equals(t, *expiry, uint32(exp.Unix()))

	expiry, err = ReflectExpiry("invalid")
	assert.Equals(t, err.Error(), `Unable to parse expiry invalid as either numeric or date expiry: parsing time "invalid" as "2006-01-02T15:04:05Z07:00": cannot parse "invalid" as "2006"`)
	assert.Equals(t, expiry, (*uint32)(nil))

	expiry, err = ReflectExpiry(nil)
	assert.Equals(t, err, nil)
	assert.Equals(t, expiry, (*uint32)(nil))
}

// IsMinimumVersion takes (major, minor, minimumMajor, minimumMinor)
func TestIsMinimumVersion(t *testing.T) {

	// Expected true
	assertTrue(t, isMinimumVersion(1, 0, 0, 0), "Invalid minimum version check - expected true")
	assertTrue(t, isMinimumVersion(1, 0, 1, 0), "Invalid minimum version check - expected true")
	assertTrue(t, isMinimumVersion(2, 5, 2, 5), "Invalid minimum version check - expected true")
	assertTrue(t, isMinimumVersion(3, 0, 2, 5), "Invalid minimum version check - expected true")
	assertTrue(t, isMinimumVersion(3, 5, 3, 4), "Invalid minimum version check - expected true")
	assertTrue(t, isMinimumVersion(5, 5, 4, 4), "Invalid minimum version check - expected true")
	assertTrue(t, isMinimumVersion(0, 0, 0, 0), "Invalid minimum version check - expected true")

	// Expected false
	assertTrue(t, !isMinimumVersion(0, 0, 1, 0), "Invalid minimum version check - expected false")
	assertTrue(t, !isMinimumVersion(5, 0, 6, 0), "Invalid minimum version check - expected false")
	assertTrue(t, !isMinimumVersion(4, 5, 5, 0), "Invalid minimum version check - expected false")
	assertTrue(t, !isMinimumVersion(5, 0, 5, 1), "Invalid minimum version check - expected false")
	assertTrue(t, !isMinimumVersion(0, 0, 1, 0), "Invalid minimum version check - expected false")
}

func TestSanitizeRequestURL(t *testing.T) {

	tests := []struct {
		input, output string
	}{
		{
			// Test zero values
			"", "",
		},
		{
			"http://localhost:4985/default/_oidc_callback?code=4/1zaCA0RXtFqw93PmcP9fqOMMHfyBDhI0fS2AzeQw-5E",
			"http://localhost:4985/default/_oidc_callback?code=******",
		},
		{
			"http://localhost:4985/default/_oidc_refresh?refresh_token==1/KPuhjLJrTZO9OExSypWtqiDioXf3nzAUJnewmyhK94s",
			"http://localhost:4985/default/_oidc_refresh?refresh_token=******",
		},
		{
			// Ensure non-matching parameters aren't getting sanitized
			"http://localhost:4985/default/_oidc_callback?code=4/1zaCA0RXtFqw93PmcP9fqOMMHfyBDhI0fS2AzeQw-5E&state=123456",
			"http://localhost:4985/default/_oidc_callback?code=******&state=123456",
		},
		{
			"http://localhost:4985/default/_changes?since=5&feed=longpoll",
			"http://localhost:4985/default/_changes?since=5&feed=longpoll",
		},
		{
			// Ensure matching non-parameters aren't getting sanitized
			"http://localhost:4985/default/doctokencode",
			"http://localhost:4985/default/doctokencode",
		},
		{
			"http://localhost:4985/default/doctoken=code=",
			"http://localhost:4985/default/doctoken=code=",
		},
	}

	for _, test := range tests {
		req, err := http.NewRequest(http.MethodGet, test.input, nil)
		assertNoError(t, err, "Unable to create request")
		sanitizedURL := SanitizeRequestURL(req, nil)
		assert.Equals(t, sanitizedURL, test.output)
	}

}

func TestSanitizeRequestURLRedaction(t *testing.T) {

	tests := []struct {
		input,
		output,
		outputRedacted string
	}{
		{
			// channels should be tagged as UserData
			"http://localhost:4985/default/_changes?channels=A",
			"http://localhost:4985/default/_changes?channels=A",
			"http://localhost:4985/default/_changes?channels=<ud>A</ud>",
		},
		{
			// Multiple tagged params
			"http://localhost:4985/default/_changes?channels=A&startkey=B",
			"http://localhost:4985/default/_changes?channels=A&startkey=B",
			"http://localhost:4985/default/_changes?channels=<ud>A</ud>&startkey=<ud>B</ud>",
		},
		{
			// What about multiple channels?
			"http://localhost:4985/default/_changes?channels=A&channels=B",
			"http://localhost:4985/default/_changes?channels=A&channels=B",
			"http://localhost:4985/default/_changes?channels=<ud>A</ud>&channels=<ud>B</ud>",
		},
		{
			// Non-matching params?
			"http://localhost:4985/default/_changes?channels=A&other=B",
			"http://localhost:4985/default/_changes?channels=A&other=B",
			"http://localhost:4985/default/_changes?channels=<ud>A</ud>&other=B",
		},
		{
			// Conflicting values
			"http://localhost:4985/A/_changes?channels=A&other=A",
			"http://localhost:4985/A/_changes?channels=A&other=A",
			"http://localhost:4985/A/_changes?channels=<ud>A</ud>&other=A",
		},
		{
			// More conflicting values
			"http://localhost:4985/A/_changes?channels=A&other=A",
			"http://localhost:4985/A/_changes?channels=A&other=A",
			"http://localhost:4985/A/_changes?channels=<ud>A</ud>&other=A",
		},
		{
			"http://localhost:4985/A/_changes?channels=🔥",
			"http://localhost:4985/A/_changes?channels=🔥",
			"http://localhost:4985/A/_changes?channels=<ud>🔥</ud>",
		},
	}

	for _, test := range tests {
		req, err := http.NewRequest(http.MethodGet, test.input, nil)
		assertNoError(t, err, "Unable to create request")

		SetRedaction(RedactNone)
		sanitizedURL := SanitizeRequestURL(req, nil)
		assert.Equals(t, sanitizedURL, test.output)

		SetRedaction(RedactPartial)
		sanitizedURL = SanitizeRequestURL(req, nil)
		assert.Equals(t, sanitizedURL, test.outputRedacted)
	}

}

func TestFindPrimaryAddr(t *testing.T) {
	ip, err := FindPrimaryAddr()
	if err != nil && strings.Contains(err.Error(), "network is unreachable") {
		// Skip test if dial fails.
		// This is to allow tests to be run offline/without third-party dependencies.
		t.Skipf("WARNING: network is unreachable: %s", err)
	}

	assert.NotEquals(t, ip, nil)
	assert.NotEquals(t, ip.String(), "")
	assert.NotEquals(t, ip.String(), "<nil>")
}

func TestReplaceAll(t *testing.T) {
	tests := []struct {
		input,
		chars,
		new,
		expected string
	}{
		{"", "", "", ""},
		{"safe", ":", "", "safe"},
		{"unsafe?", "?", "", "unsafe"},
		{"123:456:789", ":", "-", "123-456-789"},
	}

	for _, test := range tests {
		t.Run(test.chars, func(ts *testing.T) {
			output := ReplaceAll(test.input, test.chars, test.new)
			assert.Equals(ts, output, test.expected)
		})
	}
}
