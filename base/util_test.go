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
	"math"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
)

func TestFixJSONNumbers(t *testing.T) {
	goassert.DeepEquals(t, FixJSONNumbers(1), 1)
	goassert.DeepEquals(t, FixJSONNumbers(float64(1.23)), float64(1.23))
	goassert.DeepEquals(t, FixJSONNumbers(float64(123456)), int64(123456))
	goassert.DeepEquals(t, FixJSONNumbers(float64(123456789)), int64(123456789))
	goassert.DeepEquals(t, FixJSONNumbers(float64(12345678901234567890)),
		float64(12345678901234567890))
	goassert.DeepEquals(t, FixJSONNumbers("foo"), "foo")
	goassert.DeepEquals(t, FixJSONNumbers([]interface{}{1, float64(123456)}),
		[]interface{}{1, int64(123456)})
	goassert.DeepEquals(t, FixJSONNumbers(map[string]interface{}{"foo": float64(123456)}),
		map[string]interface{}{"foo": int64(123456)})
}

func TestConvertJSONString(t *testing.T) {
	assert.Equal(t, "blah", ConvertJSONString(`"blah"`))
	assert.Equal(t, "blah", ConvertJSONString("blah"))
}

func TestJSONStringUtils(t *testing.T) {
	tests := []struct {
		input  string
		output string
	}{
		{`test`, `"test"`},
		{`"test"`, `"\"test\""`},
		{"\x00", `"\u0000"`},
	}

	for _, test := range tests {
		t.Run("ConvertToJSONString "+test.input, func(t *testing.T) {
			out := ConvertToJSONString(test.input)
			assert.Equal(t, test.output, out)
		})
		t.Run("ConvertJSONString "+test.input, func(t *testing.T) {
			out := ConvertJSONString(test.output)
			assert.Equal(t, test.input, out)
		})
	}
}

func BenchmarkJSONStringUtils(b *testing.B) {
	tests := []struct {
		input  string
		output string
	}{
		{`test`, `"test"`},
		{`"test"`, `"\"test\""`},
		{"\x00", `"\u0000"`},
	}

	for _, test := range tests {
		b.Run("ConvertToJSONString "+test.input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = ConvertToJSONString(test.input)
			}
		})
		b.Run("ConvertJSONString "+test.input, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = ConvertJSONString(test.output)
			}
		})
	}
}

func TestConvertBackQuotedStrings(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{
			input:    `{"foo": "bar"}`,
			expected: `{"foo": "bar"}`,
		},
		{
			input:    "{\"foo\": `bar`}",
			expected: `{"foo": "bar"}`,
		},
		{
			input:    "{\"foo\": `bar\nbaz\nboo`}",
			expected: `{"foo": "bar\nbaz\nboo"}`,
		},
		{
			input:    "{\"foo\": `bar\n\"baz\n\tboo`}",
			expected: `{"foo": "bar\n\"baz\n\tboo"}`,
		},
		{
			input:    "{\"foo\": `bar\n`, \"baz\": `howdy`}",
			expected: `{"foo": "bar\n", "baz": "howdy"}`,
		},
		{
			input:    "{\"foo\": `bar\r\n`, \"baz\": `\r\nhowdy`}",
			expected: `{"foo": "bar\n", "baz": "\nhowdy"}`,
		},
		{
			input:    "{\"foo\": `bar\\baz`, \"something\": `else\\is\\here`}",
			expected: `{"foo": "bar\\baz", "something": "else\\is\\here"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(tt *testing.T) {
			output := ConvertBackQuotedStrings([]byte(test.input))
			assert.Equal(t, test.expected, string(output))
		})
	}
}

func TestCouchbaseUrlWithAuth(t *testing.T) {

	// normal bucket
	result, err := CouchbaseUrlWithAuth(
		"http://127.0.0.1:8091",
		"username",
		"password",
		"bucket",
	)
	goassert.True(t, err == nil)
	goassert.Equals(t, result, "http://username:password@127.0.0.1:8091")

	// default bucket
	result, err = CouchbaseUrlWithAuth(
		"http://127.0.0.1:8091",
		"",
		"",
		"default",
	)
	goassert.True(t, err == nil)
	goassert.Equals(t, result, "http://127.0.0.1:8091")

}

func TestCreateDoublingSleeperFunc(t *testing.T) {

	maxNumAttempts := 2
	initialTimeToSleepMs := 1
	sleeper := CreateDoublingSleeperFunc(maxNumAttempts, initialTimeToSleepMs)

	shouldContinue, timeTosleepMs := sleeper(1)
	goassert.True(t, shouldContinue)
	goassert.Equals(t, timeTosleepMs, initialTimeToSleepMs)

	shouldContinue, timeTosleepMs = sleeper(2)
	goassert.True(t, shouldContinue)
	goassert.Equals(t, timeTosleepMs, initialTimeToSleepMs*2)

	shouldContinue, _ = sleeper(3)
	goassert.False(t, shouldContinue)

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
	goassert.True(t, err == nil)
	goassert.Equals(t, result, "result")
	goassert.True(t, numTimesInvoked == 4)

}

func TestSyncSourceFromURL(t *testing.T) {
	u, err := url.Parse("http://www.test.com:4985/mydb")
	goassert.True(t, err == nil)
	result := SyncSourceFromURL(u)
	goassert.Equals(t, result, "http://www.test.com:4985")

	u, err = url.Parse("http://www.test.com:4984/mydb/some otherinvalidpath?query=yes#fragment")
	goassert.True(t, err == nil)
	result = SyncSourceFromURL(u)
	goassert.Equals(t, result, "http://www.test.com:4984")

	u, err = url.Parse("MyDB")
	goassert.True(t, err == nil)
	result = SyncSourceFromURL(u)
	goassert.Equals(t, result, "")
}

func TestValueToStringArray(t *testing.T) {
	result, nonStrings := ValueToStringArray("foobar")
	assert.Equal(t, []string{"foobar"}, result)
	assert.Nil(t, nonStrings)

	result, nonStrings = ValueToStringArray([]string{"foobar", "moocar"})
	assert.Equal(t, []string{"foobar", "moocar"}, result)
	assert.Nil(t, nonStrings)

	result, nonStrings = ValueToStringArray([]interface{}{"foobar", 1, true})
	assert.Equal(t, []string{"foobar"}, result)
	assert.Equal(t, []interface{}{1, true}, nonStrings)

	result, nonStrings = ValueToStringArray([]interface{}{"a", []interface{}{"b", "g"}, "c", 4})
	assert.Equal(t, []string{"a", "c"}, result)
	assert.Equal(t, []interface{}{[]interface{}{"b", "g"}, 4}, nonStrings)

	result, nonStrings = ValueToStringArray(4)
	assert.Nil(t, result)
	assert.Equal(t, []interface{}{4}, nonStrings)

	result, nonStrings = ValueToStringArray([]interface{}{1, true})
	assert.Equal(t, result, []string{})
	assert.Equal(t, []interface{}{1, true}, nonStrings)
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
		actual, err := CouchbaseURIToHttpURL(nil, inputAndExpected.input, nil)
		assert.NoError(t, err, "Unexpected error")
		goassert.DeepEquals(t, actual, inputAndExpected.expected)
	}

	// With a nil (or walrus bucket) and a couchbase or couchbases url, expect errors
	_, err := CouchbaseURIToHttpURL(nil, "couchbases://host1:18191,host2:18191", nil)
	goassert.True(t, err != nil)
	_, err = CouchbaseURIToHttpURL(nil, "couchbase://host1", nil)
	goassert.True(t, err != nil)

}

func TestReflectExpiry(t *testing.T) {
	exp := time.Now().Add(time.Hour)

	expiry, err := ReflectExpiry(uint(1234))
	goassert.Equals(t, err.Error(), "Unrecognized expiry format")
	goassert.Equals(t, expiry, (*uint32)(nil))

	expiry, err = ReflectExpiry(true)
	goassert.Equals(t, err.Error(), "Unrecognized expiry format")
	goassert.Equals(t, expiry, (*uint32)(nil))

	expiry, err = ReflectExpiry(int64(1234))
	assert.NoError(t, err)
	goassert.Equals(t, *expiry, uint32(1234))

	expiry, err = ReflectExpiry(float64(1234))
	assert.NoError(t, err)
	goassert.Equals(t, *expiry, uint32(1234))

	expiry, err = ReflectExpiry("1234")
	assert.NoError(t, err)
	goassert.Equals(t, *expiry, uint32(1234))

	expiry, err = ReflectExpiry(exp.Format(time.RFC3339))
	assert.NoError(t, err)
	goassert.Equals(t, *expiry, uint32(exp.Unix()))

	expiry, err = ReflectExpiry("invalid")
	goassert.Equals(t, err.Error(), `Unable to parse expiry invalid as either numeric or date expiry: parsing time "invalid" as "2006-01-02T15:04:05Z07:00": cannot parse "invalid" as "2006"`)
	goassert.Equals(t, expiry, (*uint32)(nil))

	expiry, err = ReflectExpiry(nil)
	assert.NoError(t, err)
	goassert.Equals(t, expiry, (*uint32)(nil))
}

// IsMinimumVersion takes (major, minor, minimumMajor, minimumMinor)
func TestIsMinimumVersion(t *testing.T) {

	// Expected true
	assert.True(t, isMinimumVersion(1, 0, 0, 0), "Invalid minimum version check - expected true")
	assert.True(t, isMinimumVersion(1, 0, 1, 0), "Invalid minimum version check - expected true")
	assert.True(t, isMinimumVersion(2, 5, 2, 5), "Invalid minimum version check - expected true")
	assert.True(t, isMinimumVersion(3, 0, 2, 5), "Invalid minimum version check - expected true")
	assert.True(t, isMinimumVersion(3, 5, 3, 4), "Invalid minimum version check - expected true")
	assert.True(t, isMinimumVersion(5, 5, 4, 4), "Invalid minimum version check - expected true")
	assert.True(t, isMinimumVersion(0, 0, 0, 0), "Invalid minimum version check - expected true")

	// Expected false
	assert.True(t, !isMinimumVersion(0, 0, 1, 0), "Invalid minimum version check - expected false")
	assert.True(t, !isMinimumVersion(5, 0, 6, 0), "Invalid minimum version check - expected false")
	assert.True(t, !isMinimumVersion(4, 5, 5, 0), "Invalid minimum version check - expected false")
	assert.True(t, !isMinimumVersion(5, 0, 5, 1), "Invalid minimum version check - expected false")
	assert.True(t, !isMinimumVersion(0, 0, 1, 0), "Invalid minimum version check - expected false")
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
		assert.NoError(t, err, "Unable to create request")
		sanitizedURL := SanitizeRequestURL(req, nil)
		goassert.Equals(t, sanitizedURL, test.output)
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
		assert.NoError(t, err, "Unable to create request")

		SetRedaction(RedactNone)
		sanitizedURL := SanitizeRequestURL(req, nil)
		goassert.Equals(t, sanitizedURL, test.output)

		SetRedaction(RedactPartial)
		sanitizedURL = SanitizeRequestURL(req, nil)
		goassert.Equals(t, sanitizedURL, test.outputRedacted)
	}

}

func TestFindPrimaryAddr(t *testing.T) {
	ip, err := FindPrimaryAddr()
	if err != nil && strings.Contains(err.Error(), "network is unreachable") {
		// Skip test if dial fails.
		// This is to allow tests to be run offline/without third-party dependencies.
		t.Skipf("WARNING: network is unreachable: %s", err)
	}

	goassert.NotEquals(t, ip, nil)
	goassert.NotEquals(t, ip.String(), "")
	goassert.NotEquals(t, ip.String(), "<nil>")
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
			goassert.Equals(ts, output, test.expected)
		})
	}
}

type A struct {
	String  string
	Int     int
	Strings []string
	Ints    map[string]int
	As      map[string]*A
}

// Copied from https://github.com/getlantern/deepcopy, commit 7f45deb8130a0acc553242eb0e009e3f6f3d9ce3 (Apache 2 licensed)
func TestDeepCopyInefficient(t *testing.T) {
	src := map[string]interface{}{
		"String":  "Hello World",
		"Int":     5,
		"Strings": []string{"A", "B"},
		"Ints":    map[string]int{"A": 1, "B": 2},
		"As": map[string]map[string]interface{}{
			"One": map[string]interface{}{
				"String": "2",
			},
			"Two": map[string]interface{}{
				"String": "3",
			},
		},
	}
	dst := &A{
		Strings: []string{"C"},
		Ints:    map[string]int{"B": 3, "C": 4},
		As:      map[string]*A{"One": &A{String: "1", Int: 5}}}
	expected := &A{
		String:  "Hello World",
		Int:     5,
		Strings: []string{"A", "B"},
		Ints:    map[string]int{"A": 1, "B": 2, "C": 4},
		As: map[string]*A{
			"One": &A{String: "2"},
			"Two": &A{String: "3"},
		},
	}
	err := DeepCopyInefficient(dst, src)
	if err != nil {
		t.Errorf("Unable to copy!")
	}
	if !reflect.DeepEqual(expected, dst) {
		t.Errorf("expected and dst differed")
	}
}

func TestRedactBasicAuthURL(t *testing.T) {
	tests := []struct {
		input,
		expected string
	}{
		{
			input:    "http://hostname",
			expected: "http://hostname",
		},
		{
			input:    "http://username:password@hostname",
			expected: "http://****:****@hostname",
		},
		{
			input:    "https://username:password@example.org:8123",
			expected: "https://****:****@example.org:8123",
		},
		{
			input:    "https://username:password@example.org/path",
			expected: "https://****:****@example.org/path",
		},
		{
			input:    "https://username:password@example.org:8123/path?key=val&email=me@example.org",
			expected: "https://****:****@example.org:8123/path?key=val&email=me@example.org",
		},
		{
			input:    "https://foo%40bar.baz:my-%24ecret-p%40%25%24w0rd@example.com:8888/bar",
			expected: "https://****:****@example.com:8888/bar",
		},
		{
			input:    "https://example.com/does-not-count-as-url-embedded:basic-auth-credentials@qux",
			expected: "https://example.com/does-not-count-as-url-embedded:basic-auth-credentials@qux",
		},
	}

	for _, test := range tests {
		goassert.Equals(t, RedactBasicAuthURL(test.input), test.expected)
	}
}

func TestSetUpTestLogging(t *testing.T) {
	// Check default state of logging is as expected.
	require.Equal(t, LevelInfo, *consoleLogger.LogLevel)
	require.Equal(t, *logKeyMask(KeyHTTP), *consoleLogger.LogKeyMask)

	teardownFn := SetUpTestLogging(LevelDebug, KeyDCP, KeySync)
	assert.Equal(t, LevelDebug, *consoleLogger.LogLevel)
	assert.Equal(t, *logKeyMask(KeyDCP, KeySync), *consoleLogger.LogKeyMask)

	teardownFn()
	assert.Equal(t, LevelInfo, *consoleLogger.LogLevel)
	assert.Equal(t, *logKeyMask(KeyHTTP), *consoleLogger.LogKeyMask)

	teardownFn = DisableTestLogging()
	assert.Equal(t, LevelNone, *consoleLogger.LogLevel)
	assert.Equal(t, *logKeyMask(KeyNone), *consoleLogger.LogKeyMask)

	teardownFn()
	assert.Equal(t, LevelInfo, *consoleLogger.LogLevel)
	assert.Equal(t, *logKeyMask(KeyHTTP), *consoleLogger.LogKeyMask)

	SetUpTestLogging(LevelDebug, KeyDCP, KeySync)
	assert.Equal(t, LevelDebug, *consoleLogger.LogLevel)
	assert.Equal(t, *logKeyMask(KeyDCP, KeySync), *consoleLogger.LogKeyMask)

	// Now we should panic because we forgot to call teardown!
	assert.Panics(t, func() {
		SetUpTestLogging(LevelError, KeyAuth, KeyCRUD)
	}, "Expected panic from multiple SetUpTestLogging calls")
	teardownFn()
}

func TestEncodeDecodeCompatVersion(t *testing.T) {
	tests := []struct {
		major,
		minor int
	}{
		{
			major: 2,
			minor: 5,
		},
		{
			major: 3,
			minor: 0,
		},
		{
			major: 4,
			minor: 0,
		},
		{
			major: 4,
			minor: 5,
		},
		{
			major: 4,
			minor: 6,
		},
		{
			major: 5,
			minor: 0,
		},
		{
			major: 5,
			minor: 5,
		},
		{
			major: 0,
			minor: 0,
		},
		{
			major: 10,
			minor: 65535,
		},
		{
			major: 32767, //Max size 15 bit integer
			minor: 65535, //Max size 16 bit integer
		},
	}

	for _, test := range tests {
		major, minor := decodeClusterVersion(encodeClusterVersion(test.major, test.minor))
		assert.Equal(t, test.major, major, "Major")
		assert.Equal(t, test.minor, minor, "Minor")
	}
}

func TestDefaultHTTPTransport(t *testing.T) {
	assert.NotPanics(t, func() {
		transport := DefaultHTTPTransport()
		assert.NotNil(t, transport, "Returned DefaultHTTPTransport was unexpectedly nil")
	})
}

// Test to ensure that InjectJSONProperties does not mutate the given byte slice, and instead only returns a modified copy.
func TestInjectJSONPropertiesMutable(t *testing.T) {
	origBytes := []byte(`{"orig":true}`)

	newBytes, err := InjectJSONProperties(origBytes, KVPair{Key: "updated", Val: true})
	require.NoError(t, err)
	assert.NotEqual(t, origBytes, newBytes)

	assert.Contains(t, string(newBytes), `"updated":true`)
	assert.NotContains(t, string(origBytes), `"updated":true`)

}

func TestInjectJSONProperties(t *testing.T) {
	newKV := KVPair{
		Key: "newval",
		Val: 123,
	}

	tests := []struct {
		input          string
		expectedOutput string
		expectedErr    string
	}{
		{
			input:       ``,
			expectedErr: `not a JSON object`,
		},
		{
			input:       `null`,
			expectedErr: `not a JSON object`,
		},
		{
			input:       "123",
			expectedErr: `not a JSON object`,
		},
		{
			input:          "{}",
			expectedOutput: `{"newval":123}`,
		},
		{
			input:          `{"key":"val"}`,
			expectedOutput: `{"key":"val","newval":123}`,
		},
		{
			input:          `{"newval":"old"}`,
			expectedOutput: `{"newval":"old","newval":123}`,
		},
		{
			input:          `    {"key":"val"}  `,
			expectedOutput: `{"key":"val","newval":123}`,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(tt *testing.T) {
			output, err := InjectJSONProperties([]byte(test.input), newKV)
			if test.expectedErr != "" {
				require.Errorf(tt, err, test.expectedErr, "expected error did not match")
				return
			} else {
				require.NoError(tt, err, "unexpected error")
			}

			assert.Equal(tt, test.expectedOutput, string(output))

			var m map[string]interface{}
			err = JSONUnmarshal(output, &m)
			assert.NoError(tt, err, "produced invalid JSON")
		})
	}
}

func TestInjectJSONPropertiesDiffTypes(t *testing.T) {

	tests := []struct {
		input  string
		output string
		pair   KVPair
	}{
		{
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar","maxuint64":18446744073709551615}`,
			pair: KVPair{
				"maxuint64",
				uint64(math.MaxUint64),
			},
		},
		{
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar","minuint64":0}`,
			pair: KVPair{
				"minuint64",
				0,
			},
		},
		{
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar","int":0}`,
			pair: KVPair{
				"int",
				int(0),
			},
		},
		{
			input:  `{"foo": "bar"}`,
			output: fmt.Sprintf(`{"foo": "bar","maxint64":%d}`, math.MaxInt64),
			pair: KVPair{
				"maxint64",
				math.MaxInt64,
			},
		},
		{
			input:  `{"foo": "bar"}`,
			output: fmt.Sprintf(`{"foo": "bar","minint64":%d}`, math.MinInt64),
			pair: KVPair{
				"minint64",
				math.MinInt64,
			},
		},
		{
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar","float32":0}`,
			pair: KVPair{
				"float32",
				float32(0),
			},
		},
		{
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar","float64":0}`,
			pair: KVPair{
				"float64",
				float64(0),
			},
		},
		{
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar","float32-2":123.45}`,
			pair: KVPair{
				"float32-2",
				float32(123.45),
			},
		},
		{
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar","float64-2":123.45}`,
			pair: KVPair{
				"float64-2",
				float64(123.45),
			},
		},
		{
			input:  `{"foo": "bar"}`,
			output: fmt.Sprintf(`{"foo": "bar","maxfloat64":%v}`, math.MaxFloat64),
			pair: KVPair{
				"maxfloat64",
				float64(math.MaxFloat64),
			},
		},
		{
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar","bool":true}`,
			pair: KVPair{
				"bool",
				true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.output, func(t *testing.T) {
			output, err := InjectJSONProperties([]byte(test.input), test.pair)
			assert.NoError(t, err)
			assert.Equal(t, test.output, string(output))
		})
	}
}

func TestInjectJSONProperties_Multiple(t *testing.T) {
	newKVs := []KVPair{
		{
			Key: "newval",
			Val: 123,
		},
		{
			Key: "test",
			Val: true,
		},
		{
			Key: "asdf",
			Val: "qwerty",
		},
	}

	tests := []struct {
		input          string
		expectedOutput string
		expectedErr    string
	}{
		{
			input:       ``,
			expectedErr: `not a JSON object`,
		},
		{
			input:       `null`,
			expectedErr: `not a JSON object`,
		},
		{
			input:       "123",
			expectedErr: `not a JSON object`,
		},
		{
			input:          "{}",
			expectedOutput: `{"newval":123,"test":true,"asdf":"qwerty"}`,
		},
		{
			input:          `{"key":"val"}`,
			expectedOutput: `{"key":"val","newval":123,"test":true,"asdf":"qwerty"}`,
		},
		{
			input:          `{"newval":"old"}`,
			expectedOutput: `{"newval":"old","newval":123,"test":true,"asdf":"qwerty"}`,
		},
		{
			input:          `    {"key":"val"}  `,
			expectedOutput: `{"key":"val","newval":123,"test":true,"asdf":"qwerty"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(tt *testing.T) {
			output, err := InjectJSONProperties([]byte(test.input), newKVs...)
			if test.expectedErr != "" {
				require.Errorf(tt, err, test.expectedErr, "expected error did not match")
				return
			} else {
				require.NoError(tt, err, "unexpected error")
			}

			assert.Equal(tt, test.expectedOutput, string(output))

			var m map[string]interface{}
			err = JSONUnmarshal(output, &m)
			assert.NoError(tt, err, "produced invalid JSON")
		})
	}
}

func TestInjectJSONPropertiesFromBytes(t *testing.T) {
	newKVBytes := KVPairBytes{
		Key: "newval",
		Val: []byte(`{"abc":123,"nums":["one","two","three"],"test":true}`),
	}

	tests := []struct {
		input          string
		expectedOutput string
		expectedErr    string
	}{
		{
			input:       ``,
			expectedErr: `not a JSON object`,
		},
		{
			input:       `null`,
			expectedErr: `not a JSON object`,
		},
		{
			input:       "123",
			expectedErr: `not a JSON object`,
		},
		{
			input:          "{}",
			expectedOutput: `{"newval":{"abc":123,"nums":["one","two","three"],"test":true}}`,
		},
		{
			input:          `{"key":"val"}`,
			expectedOutput: `{"key":"val","newval":{"abc":123,"nums":["one","two","three"],"test":true}}`,
		},
		{
			input:          `{"newval":"old"}`,
			expectedOutput: `{"newval":"old","newval":{"abc":123,"nums":["one","two","three"],"test":true}}`,
		},
		{
			input:          `    {"key":"val"}  `,
			expectedOutput: `{"key":"val","newval":{"abc":123,"nums":["one","two","three"],"test":true}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(tt *testing.T) {
			output, err := InjectJSONPropertiesFromBytes([]byte(test.input), newKVBytes)
			if test.expectedErr != "" {
				require.Errorf(tt, err, test.expectedErr, "expected error did not match")
				return
			} else {
				require.NoError(tt, err, "unexpected error")
			}

			assert.Equal(tt, test.expectedOutput, string(output))

			var m map[string]interface{}
			err = JSONUnmarshal(output, &m)
			assert.NoError(tt, err, "produced invalid JSON")
		})
	}
}

func TestInjectJSONPropertiesFromBytes_Multiple(t *testing.T) {
	newKVBytes := []KVPairBytes{
		{
			Key: "newval",
			Val: []byte(`{"abc":123,"nums":["one","two","three"],"test":true}`),
		},
		{
			Key: "test",
			Val: []byte(`true`),
		},
		{
			Key: "asdf",
			Val: []byte(`"qwerty"`),
		},
	}

	tests := []struct {
		input          string
		expectedOutput string
		expectedErr    string
	}{
		{
			input:       ``,
			expectedErr: `not a JSON object`,
		},
		{
			input:       `null`,
			expectedErr: `not a JSON object`,
		},
		{
			input:       "123",
			expectedErr: `not a JSON object`,
		},
		{
			input:          "{}",
			expectedOutput: `{"newval":{"abc":123,"nums":["one","two","three"],"test":true},"test":true,"asdf":"qwerty"}`,
		},
		{
			input:          `{"key":"val"}`,
			expectedOutput: `{"key":"val","newval":{"abc":123,"nums":["one","two","three"],"test":true},"test":true,"asdf":"qwerty"}`,
		},
		{
			input:          `{"newval":"old"}`,
			expectedOutput: `{"newval":"old","newval":{"abc":123,"nums":["one","two","three"],"test":true},"test":true,"asdf":"qwerty"}`,
		},
		{
			input:          `    {"key":"val"}  `,
			expectedOutput: `{"key":"val","newval":{"abc":123,"nums":["one","two","three"],"test":true},"test":true,"asdf":"qwerty"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(tt *testing.T) {
			output, err := InjectJSONPropertiesFromBytes([]byte(test.input), newKVBytes...)
			if test.expectedErr != "" {
				require.Errorf(tt, err, test.expectedErr, "expected error did not match")
				return
			} else {
				require.NoError(tt, err, "unexpected error")
			}

			assert.Equal(tt, test.expectedOutput, string(output))

			var m map[string]interface{}
			err = JSONUnmarshal(output, &m)
			assert.NoError(tt, err, "produced invalid JSON")
		})
	}
}

func BenchmarkInjectJSONPropertiesFromBytes(b *testing.B) {
	newKVBytes := []KVPairBytes{
		{
			Key: "newval",
			Val: []byte(`{"abc":123,"nums":["one","two","three"],"test":true}`),
		},
		{
			Key: "test",
			Val: []byte(`true`),
		},
		{
			Key: "asdf",
			Val: []byte(`"qwerty"`),
		},
	}

	tests := []struct {
		input string
	}{
		{
			input: `null`,
		},
		{
			input: "{}",
		},
		{
			input: `{"key":"val"}`,
		},
	}

	for _, test := range tests {
		b.Run(test.input, func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				_, _ = InjectJSONPropertiesFromBytes([]byte(test.input), newKVBytes...)
			}
		})
	}
}

func BenchmarkInjectJSONPropertiesFromBytes_Multiple(b *testing.B) {
	newKVBytes := KVPairBytes{
		Key: "newval",
		Val: []byte(`{"abc":123,"nums":["one","two","three"],"test":true}`),
	}

	tests := []struct {
		input string
	}{
		{
			input: `null`,
		},
		{
			input: "{}",
		},
		{
			input: `{"key":"val"}`,
		},
	}

	for _, test := range tests {
		b.Run(test.input, func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				_, _ = InjectJSONPropertiesFromBytes([]byte(test.input), newKVBytes)
			}
		})
	}
}

func BenchmarkInjectJSONProperties(b *testing.B) {
	newKV := KVPair{
		Key: "newval",
		Val: 123,
	}

	tests := []struct {
		input string
	}{
		{
			input: `null`,
		},
		{
			input: "{}",
		},
		{
			input: `{"key":"val"}`,
		},
	}

	for _, test := range tests {
		b.Run(test.input, func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				_, _ = InjectJSONProperties([]byte(test.input), newKV)
			}
		})
	}
}

func BenchmarkInjectJSONProperties_Multiple(b *testing.B) {
	newKVs := []KVPair{
		{
			Key: "newval",
			Val: 123,
		},
		{
			Key: "test",
			Val: true,
		},
		{
			Key: "asdf",
			Val: "qwerty",
		},
	}

	tests := []struct {
		input string
	}{
		{
			input: `null`,
		},
		{
			input: "{}",
		},
		{
			input: `{"key":"val"}`,
		},
	}

	for _, test := range tests {
		b.Run(test.input, func(bb *testing.B) {
			for i := 0; i < bb.N; i++ {
				_, _ = InjectJSONProperties([]byte(test.input), newKVs...)
			}
		})
	}
}

func BenchmarkPanicRecover(b *testing.B) {
	b.Run("recover panic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			func() {
				defer func() {
					_ = recover()
				}()
				panic("test")
			}()
		}
	})

	b.Run("recover no panic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			func() {
				defer func() {
					_ = recover()
				}()
			}()
		}
	})

	b.Run("noop no panic", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			func() {
				defer func() {}()
			}()
		}
	})
}

func TestGetRestrictedIntQuery(t *testing.T) {

	defaultValue := uint64(42)
	minValue := uint64(20)
	maxValue := uint64(100)

	// make sure it returns default value when passed empty Values
	values := make(url.Values)
	restricted := GetRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	assert.Equal(t, defaultValue, restricted)

	// make sure it returns default value when passed Values that doesn't contain key
	values.Set("bar", "99")
	restricted = GetRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	assert.Equal(t, defaultValue, restricted)

	// make sure it returns appropriate value from Values
	values.Set("foo", "99")
	restricted = GetRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	assert.Equal(t, uint64(99), restricted)

	// make sure it is limited to max when value value is over max
	values.Set("foo", "200")
	restricted = GetRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	assert.Equal(t, maxValue, restricted)

	// make sure it is limited to min when value value is under min
	values.Set("foo", "1")
	restricted = GetRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	assert.Equal(t, minValue, restricted)

	// Return zero when allowZero=true
	values.Set("foo", "0")
	restricted = GetRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		true,
	)
	assert.Equal(t, uint64(0), restricted)

	// Return minValue when allowZero=false
	values.Set("foo", "0")
	restricted = GetRestrictedIntQuery(
		values,
		"foo",
		defaultValue,
		minValue,
		maxValue,
		false,
	)
	assert.Equal(t, minValue, restricted)
}
