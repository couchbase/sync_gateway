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
	"encoding/json"
	"sort"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestSetFromArray(t *testing.T) {
	cases := [][][]string{
		{{}, {}},
		{{"a"}, {"a"}},
		{{"a", "b"}, {"a", "b"}},
		{{"a", "a"}, {"a"}},
		{{"a", "b", "a"}, {"a", "b"}},
	}
	for _, cas := range cases {
		channels := SetFromArray(cas[0])
		assert.DeepEquals(t, channels, SetOf(cas[1]...))
	}
}

func TestSet(t *testing.T) {
	set := SetFromArray(nil)
	assert.Equals(t, len(set), 0)
	assert.DeepEquals(t, set.ToArray(), []string{})

	set = SetFromArray([]string{})
	assert.Equals(t, len(set), 0)

	set = SetFromArray([]string{"foo"})
	assert.Equals(t, len(set), 1)
	assert.True(t, set.Contains("foo"))
	assert.False(t, set.Contains("bar"))

	values := []string{"bar", "foo", "zog"}
	set = SetFromArray(values)
	assert.Equals(t, len(set), 3)
	asArray := set.ToArray()
	sort.Strings(asArray)
	assert.DeepEquals(t, asArray, values)

	set2 := set.copy()
	assert.DeepEquals(t, set2, set)
}

func TestUnion(t *testing.T) {
	var nilSet Set
	empty := Set{}
	set1 := SetOf("foo", "bar", "baz")
	set2 := SetOf("bar", "block", "deny")
	assert.DeepEquals(t, set1.Union(empty), set1)
	assert.DeepEquals(t, empty.Union(set1), set1)
	assert.DeepEquals(t, set1.Union(nilSet), set1)
	assert.DeepEquals(t, nilSet.Union(set1), set1)
	assert.DeepEquals(t, nilSet.Union(nilSet), nilSet)
	assert.Equals(t, set1.Union(set2).String(), "{bar, baz, block, deny, foo}")
}

func TestSetMarshal(t *testing.T) {
	var str struct {
		Channels Set
	}
	bytes, err := json.Marshal(str)
	assertNoError(t, err, "Marshal")
	assert.Equals(t, string(bytes), `{"Channels":null}`)

	str.Channels = SetOf()
	bytes, err = json.Marshal(str)
	assertNoError(t, err, "Marshal")
	assert.Equals(t, string(bytes), `{"Channels":[]}`)

	str.Channels = SetOf("a", "b")
	bytes, err = json.Marshal(str)
	assertNoError(t, err, "Marshal")
	assert.Equals(t, string(bytes), `{"Channels":["a","b"]}`)
}

func TestSetUnmarshal(t *testing.T) {
	var str struct {
		Channels Set
	}
	err := json.Unmarshal([]byte(`{"channels":null}`), &str)
	assertNoError(t, err, "Unmarshal")
	assert.DeepEquals(t, str.Channels, Set(nil))

	err = json.Unmarshal([]byte(`{"channels":[]}`), &str)
	assertNoError(t, err, "Unmarshal")
	assert.DeepEquals(t, str.Channels, SetOf())

	err = json.Unmarshal([]byte(`{"channels":["foo"]}`), &str)
	assertNoError(t, err, "Unmarshal")
	assert.DeepEquals(t, str.Channels.ToArray(), []string{"foo"})

}

//////// HELPERS:

func assertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
}

func assertTrue(t *testing.T, success bool, message string) {
	if !success {
		t.Fatalf("%s", message)
	}
}
