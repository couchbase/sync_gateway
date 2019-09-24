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
	"sort"
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
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
		goassert.DeepEquals(t, channels, SetOf(cas[1]...))
	}
}

func TestSet(t *testing.T) {
	set := SetFromArray(nil)
	goassert.Equals(t, len(set), 0)
	goassert.DeepEquals(t, set.ToArray(), []string{})

	set = SetFromArray([]string{})
	goassert.Equals(t, len(set), 0)

	set = SetFromArray([]string{"foo"})
	goassert.Equals(t, len(set), 1)
	goassert.True(t, set.Contains("foo"))
	goassert.False(t, set.Contains("bar"))

	values := []string{"bar", "foo", "zog"}
	set = SetFromArray(values)
	goassert.Equals(t, len(set), 3)
	asArray := set.ToArray()
	sort.Strings(asArray)
	goassert.DeepEquals(t, asArray, values)

	set2 := set.copy()
	goassert.DeepEquals(t, set2, set)
}

func TestUnion(t *testing.T) {
	var nilSet Set
	empty := Set{}
	set1 := SetOf("foo", "bar", "baz")
	set2 := SetOf("bar", "block", "deny")
	goassert.DeepEquals(t, set1.Union(empty), set1)
	goassert.DeepEquals(t, empty.Union(set1), set1)
	goassert.DeepEquals(t, set1.Union(nilSet), set1)
	goassert.DeepEquals(t, nilSet.Union(set1), set1)
	goassert.DeepEquals(t, nilSet.Union(nilSet), nilSet)
	goassert.Equals(t, set1.Union(set2).String(), "{bar, baz, block, deny, foo}")
}

func TestUpdateSet(t *testing.T) {
	var nilSet Set
	empty := Set{}
	set1 := SetOf("foo", "bar", "baz")
	set2 := SetOf("bar", "block", "deny")
	goassert.DeepEquals(t, set1.Update(empty), set1)
	goassert.DeepEquals(t, empty.Update(set1), set1)
	goassert.DeepEquals(t, set1.Update(nilSet), set1)
	goassert.DeepEquals(t, nilSet.Update(set1), set1)
	goassert.DeepEquals(t, nilSet.Update(nilSet), nilSet)
	goassert.Equals(t, set1.Update(set2).String(), "{bar, baz, block, deny, foo}")
}

func TestSetMarshal(t *testing.T) {
	var str struct {
		Channels Set
	}
	bytes, err := JSONMarshal(str)
	assert.NoError(t, err, "Marshal")
	goassert.Equals(t, string(bytes), `{"Channels":null}`)

	str.Channels = SetOf()
	bytes, err = JSONMarshal(str)
	assert.NoError(t, err, "Marshal")
	goassert.Equals(t, string(bytes), `{"Channels":[]}`)

	str.Channels = SetOf("a", "b")
	bytes, err = JSONMarshal(str)
	assert.NoError(t, err, "Marshal")
	goassert.Equals(t, string(bytes), `{"Channels":["a","b"]}`)
}

func BenchmarkSet_Union(b *testing.B) {
	set1 := SetOf("2", "3", "5", "8", "13", "21", "34")
	set2 := SetOf("2", "3", "5", "7", "11", "13", "17")

	for i := 0; i < b.N; i++ {
		set1 = set1.Union(set2)
	}
}

func BenchmarkSet_Update(b *testing.B) {
	set1 := SetOf("2", "3", "5", "8", "13", "21", "34")
	set2 := SetOf("2", "3", "5", "7", "11", "13", "17")

	for i := 0; i < b.N; i++ {
		set1 = set1.Update(set2)
	}
}

func TestSetUnmarshal(t *testing.T) {
	var str struct {
		Channels Set
	}
	err := JSONUnmarshal([]byte(`{"channels":null}`), &str)
	assert.NoError(t, err, "Unmarshal")
	goassert.DeepEquals(t, str.Channels, Set(nil))

	err = JSONUnmarshal([]byte(`{"channels":[]}`), &str)
	assert.NoError(t, err, "Unmarshal")
	goassert.DeepEquals(t, str.Channels, SetOf())

	err = JSONUnmarshal([]byte(`{"channels":["foo"]}`), &str)
	assert.NoError(t, err, "Unmarshal")
	goassert.DeepEquals(t, str.Channels.ToArray(), []string{"foo"})

}
