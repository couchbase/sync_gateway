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
	"sort"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestIsValidChannel(t *testing.T) {
	valid := []string{"*", "a", "FOO", "123", "-z", "foo_bar", "Éclær", "z7_"}
	for _, ch := range valid {
		if !IsValidChannel(ch) {
			t.Errorf("IsValidChannel(%q) should be true", ch)
		}
	}
	invalid := []string{"", "**", "a*", "a ", "b?", ",", "Z∫•"}
	for _, ch := range invalid {
		if IsValidChannel(ch) {
			t.Errorf("IsValidChannel(%q) should be false", ch)
		}
	}
}

func TestSetFromArray(t *testing.T) {
	cases := [][][]string{
		{{}, {}},
		{{"*"}, {}},
		{{"a"}, {"a"}},
		{{"a", "b"}, {"a", "b"}},
		{{"a", "a"}, {"a"}},
		{{"a", "b", "a"}, {"a", "b"}},
		{{"a", "*", "b"}, {"a", "b"}},
	}
	for _, cas := range cases {
		channels, err := SetFromArray(cas[0], RemoveStar)
		assertNoError(t, err, "SetFromArray failed")
		assert.DeepEquals(t, channels, SetOf(cas[1]...))
	}
}

func TestSetFromArrayWithStar(t *testing.T) {
	cases := [][][]string{
		{{}, {}},
		{{"*"}, {"*"}},
		{{"a"}, {"a"}},
		{{"a", "b"}, {"a", "b"}},
		{{"a", "a"}, {"a"}},
		{{"a", "b", "a"}, {"a", "b"}},
		{{"a", "*", "b"}, {"*"}},
	}
	for _, cas := range cases {
		channels, err := SetFromArray(cas[0], ExpandStar)
		assertNoError(t, err, "SetFromArray failed")
		assert.DeepEquals(t, channels, SetOf(cas[1]...))
	}
}

func TestSetFromArrayError(t *testing.T) {
	_, err := SetFromArray([]string{""}, RemoveStar)
	assertTrue(t, err != nil, "SetFromArray didn't return an error")
	_, err = SetFromArray([]string{"chan1", "chan2", "bogus name", "chan3"}, RemoveStar)
	assertTrue(t, err != nil, "SetFromArray didn't return an error")
}

func TestSet(t *testing.T) {
	set, err := SetFromArray(nil, KeepStar)
	assertNoError(t, err, "SetFromArray")
	assert.Equals(t, len(set), 0)
	assert.DeepEquals(t, set.ToArray(), []string{})

	set, err = SetFromArray([]string{}, KeepStar)
	assertNoError(t, err, "SetFromArray")
	assert.Equals(t, len(set), 0)

	set, err = SetFromArray([]string{"foo"}, KeepStar)
	assertNoError(t, err, "SetFromArray")
	assert.Equals(t, len(set), 1)
	assert.True(t, set.Contains("foo"))
	assert.False(t, set.Contains("bar"))

	values := []string{"bar", "foo", "zog"}
	set, err = SetFromArray(values, KeepStar)
	assertNoError(t, err, "SetFromArray")
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
