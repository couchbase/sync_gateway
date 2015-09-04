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
