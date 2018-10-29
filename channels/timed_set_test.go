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
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
)

func TestTimedSetMarshal(t *testing.T) {
	var str struct {
		Channels TimedSet
	}
	bytes, err := json.Marshal(str)
	assertNoError(t, err, "Marshal")
	goassert.Equals(t, string(bytes), `{"Channels":null}`)

	str.Channels = TimedSet{}
	bytes, err = json.Marshal(str)
	assertNoError(t, err, "Marshal")
	goassert.Equals(t, string(bytes), `{"Channels":{}}`)

	str.Channels = AtSequence(SetOf("a", "b"), 17)
	bytes, err = json.Marshal(str)
	assertNoError(t, err, "Marshal")
	goassert.Equals(t, string(bytes), `{"Channels":{"a":17,"b":17}}`)
}

func TestTimedSetUnmarshal(t *testing.T) {
	var str struct {
		Channels TimedSet
	}
	err := json.Unmarshal([]byte(`{"channels":null}`), &str)
	assertNoError(t, err, "Unmarshal")
	goassert.DeepEquals(t, str.Channels, TimedSet(nil))

	err = json.Unmarshal([]byte(`{"channels":{}}`), &str)
	assertNoError(t, err, "Unmarshal empty")
	goassert.DeepEquals(t, str.Channels, TimedSet{})

	err = json.Unmarshal([]byte(`{"channels":{"a":17,"b":17}}`), &str)
	assertNoError(t, err, "Unmarshal sequence only")
	goassert.DeepEquals(t, str.Channels, TimedSet{"a": NewVbSimpleSequence(17), "b": NewVbSimpleSequence(17)})

	// Now try unmarshaling the alternative array form:
	err = json.Unmarshal([]byte(`{"channels":[]}`), &str)
	assertNoError(t, err, "Unmarshal empty array")
	goassert.DeepEquals(t, str.Channels, TimedSet{})

	err = json.Unmarshal([]byte(`{"channels":["a","b"]}`), &str)
	assertNoError(t, err, "Unmarshal populated array")
	goassert.DeepEquals(t, str.Channels, TimedSet{"a": NewVbSimpleSequence(0), "b": NewVbSimpleSequence(0)})

	err = json.Unmarshal([]byte(`{"channels":{"a":{"seq":17, "vb":21},"b":{"seq":23, "vb":25}}}`), &str)
	assertNoError(t, err, "Unmarshal sequence and vbucket only")
	goassert.Equals(t, fmt.Sprintf("%s", str.Channels), fmt.Sprintf("%s", TimedSet{"a": NewVbSequence(21, 17), "b": NewVbSequence(25, 23)}))
}

func TestEncodeSequenceID(t *testing.T) {
	set := TimedSet{"ABC": NewVbSimpleSequence(17), "CBS": NewVbSimpleSequence(23), "BBC": NewVbSimpleSequence(1)}
	encoded := set.String()
	goassert.Equals(t, encoded, "ABC:17,BBC:1,CBS:23")
	decoded := TimedSetFromString(encoded)
	goassert.DeepEquals(t, decoded, set)

	goassert.Equals(t, TimedSet{"ABC": NewVbSimpleSequence(17), "CBS": NewVbSimpleSequence(0)}.String(), "ABC:17")

	goassert.DeepEquals(t, TimedSetFromString(""), TimedSet{})
	goassert.DeepEquals(t, TimedSetFromString("ABC:17"), TimedSet{"ABC": NewVbSimpleSequence(17)})

	goassert.DeepEquals(t, TimedSetFromString(":17"), TimedSet(nil))
	goassert.DeepEquals(t, TimedSetFromString("ABC:"), TimedSet(nil))
	goassert.DeepEquals(t, TimedSetFromString("ABC:0"), TimedSet(nil))
	goassert.DeepEquals(t, TimedSetFromString("ABC:-1"), TimedSet(nil))
	goassert.DeepEquals(t, TimedSetFromString("ABC:17,"), TimedSet(nil))
	goassert.DeepEquals(t, TimedSetFromString(",ABC:17"), TimedSet(nil))
	goassert.DeepEquals(t, TimedSetFromString("ABC:17,,NBC:12"), TimedSet(nil))
	goassert.DeepEquals(t, TimedSetFromString("ABC:17,ABC:12"), TimedSet(nil))
}

func TestEqualsWithEqualSet(t *testing.T) {
	set1 := TimedSet{"ABC": NewVbSimpleSequence(17), "CBS": NewVbSimpleSequence(23), "BBC": NewVbSimpleSequence(1)}
	set2 := base.SetFromArray([]string{"ABC", "CBS", "BBC"})
	goassert.True(t, set1.Equals(set2))

}

func TestEqualsWithUnequalSet(t *testing.T) {
	set1 := TimedSet{"ABC": NewVbSimpleSequence(17), "CBS": NewVbSimpleSequence(23), "BBC": NewVbSimpleSequence(1)}
	set2 := base.SetFromArray([]string{"ABC", "BBC"})
	goassert.True(t, !set1.Equals(set2))
	set3 := base.SetFromArray([]string{"ABC", "BBC", "CBS", "FOO"})
	goassert.True(t, !set1.Equals(set3))

}
