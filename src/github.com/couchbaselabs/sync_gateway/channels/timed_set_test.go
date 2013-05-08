//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package channels

import (
	"encoding/json"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestTimedSetMarshal(t *testing.T) {
	var str struct {
		Channels TimedSet
	}
	bytes, err := json.Marshal(str)
	assertNoError(t, err, "Marshal")
	assert.Equals(t, string(bytes), `{"Channels":null}`)

	str.Channels = TimedSet{}
	bytes, err = json.Marshal(str)
	assertNoError(t, err, "Marshal")
	assert.Equals(t, string(bytes), `{"Channels":{}}`)

	str.Channels = SetOf("a", "b").AtSequence(17)
	bytes, err = json.Marshal(str)
	assertNoError(t, err, "Marshal")
	assert.Equals(t, string(bytes), `{"Channels":{"a":17,"b":17}}`)
}

func TestTimedSetUnmarshal(t *testing.T) {
	var str struct {
		Channels TimedSet
	}
	err := json.Unmarshal([]byte(`{"channels":null}`), &str)
	assertNoError(t, err, "Unmarshal")
	assert.DeepEquals(t, str.Channels, TimedSet(nil))

	err = json.Unmarshal([]byte(`{"channels":{}}`), &str)
	assertNoError(t, err, "Unmarshal")
	assert.DeepEquals(t, str.Channels, TimedSet{})

	err = json.Unmarshal([]byte(`{"channels":{"a":17,"b":17}}`), &str)
	assertNoError(t, err, "Unmarshal")
	assert.DeepEquals(t, str.Channels, TimedSet{"a": 17, "b": 17})

	// Now try unmarshaling the alternative array form:
	err = json.Unmarshal([]byte(`{"channels":[]}`), &str)
	assertNoError(t, err, "Unmarshal")
	assert.DeepEquals(t, str.Channels, TimedSet{})

	err = json.Unmarshal([]byte(`{"channels":["a","b"]}`), &str)
	assertNoError(t, err, "Unmarshal")
	assert.DeepEquals(t, str.Channels, TimedSet{"a": 0, "b": 0})
}
