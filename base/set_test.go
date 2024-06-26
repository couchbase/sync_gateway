//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"fmt"
	"sort"
	"testing"

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
		assert.Equal(t, SetOf(cas[1]...), channels)
	}
}

func TestSet(t *testing.T) {
	set := SetFromArray(nil)
	assert.Len(t, set, 0)
	assert.Equal(t, []string{}, set.ToArray())

	set = SetFromArray([]string{})
	assert.Len(t, set, 0)

	set = SetFromArray([]string{"foo"})
	assert.Len(t, set, 1)
	assert.True(t, set.Contains("foo"))
	assert.False(t, set.Contains("bar"))

	values := []string{"bar", "foo", "zog"}
	set = SetFromArray(values)
	assert.Len(t, set, 3)
	asArray := set.ToArray()
	sort.Strings(asArray)
	assert.Equal(t, values, asArray)

	set2 := set.copy()
	assert.Equal(t, set, set2)
}

func TestSetContains(t *testing.T) {
	set := SetFromArray([]string{"foo", "bar"})

	assert.True(t, set.Contains("foo"))
	assert.True(t, set.Contains("bar"))
	assert.False(t, set.Contains("baz"))

	// variadic params
	assert.True(t, set.Contains("foo", "bar"))
	assert.True(t, set.Contains("baz", "foo"))
	assert.False(t, set.Contains("baz", "buzz"))
}

func TestUnion(t *testing.T) {
	var nilSet Set
	empty := Set{}
	set1 := SetOf("foo", "bar", "baz")
	set2 := SetOf("bar", "block", "deny")
	assert.Equal(t, set1, set1.Union(empty))
	assert.Equal(t, set1, empty.Union(set1))
	assert.Equal(t, set1, set1.Union(nilSet))
	assert.Equal(t, set1, nilSet.Union(set1))
	assert.Equal(t, nilSet, nilSet.Union(nilSet))
	assert.Equal(t, "{bar, baz, block, deny, foo}", set1.Union(set2).String())
}

func TestUpdateSet(t *testing.T) {
	var nilSet Set
	empty := Set{}
	set1 := SetOf("foo", "bar", "baz")
	set2 := SetOf("bar", "block", "deny")
	assert.Equal(t, set1, set1.Update(empty))
	assert.Equal(t, set1, empty.Update(set1))
	assert.Equal(t, set1, set1.Update(nilSet))
	assert.Equal(t, set1, nilSet.Update(set1))
	assert.Equal(t, nilSet, nilSet.Update(nilSet))
	assert.Equal(t, "{bar, baz, block, deny, foo}", set1.Update(set2).String())
}

func TestSetMarshal(t *testing.T) {
	var str struct {
		Channels Set
	}
	bytes, err := JSONMarshal(str)
	assert.NoError(t, err, "Marshal")
	assert.Equal(t, `{"Channels":null}`, string(bytes))

	str.Channels = SetOf()
	bytes, err = JSONMarshal(str)
	assert.NoError(t, err, "Marshal")
	assert.Equal(t, `{"Channels":[]}`, string(bytes))

	str.Channels = SetOf("a", "b")
	bytes, err = JSONMarshal(str)
	assert.NoError(t, err, "Marshal")
	assert.Equal(t, `{"Channels":["a","b"]}`, string(bytes))
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
	assert.Equal(t, Set(nil), str.Channels)

	err = JSONUnmarshal([]byte(`{"channels":[]}`), &str)
	assert.NoError(t, err, "Unmarshal")
	assert.Equal(t, SetOf(), str.Channels)

	err = JSONUnmarshal([]byte(`{"channels":["foo"]}`), &str)
	assert.NoError(t, err, "Unmarshal")
	assert.Equal(t, []string{"foo"}, str.Channels.ToArray())

}

func TestSetNumMatches(t *testing.T) {
	tests := []struct {
		a, b     Set
		expected int
	}{
		{
			a:        nil,
			b:        nil,
			expected: 0,
		},
		{
			a:        SetOf("a", "b", "c"),
			b:        nil,
			expected: 0,
		},
		{
			a:        nil,
			b:        SetOf("a", "b", "c"),
			expected: 0,
		},
		{
			a:        SetOf("a", "b", "c"),
			b:        SetOf("a", "b", "c"),
			expected: 3,
		},
		{
			a:        SetOf("a", "b", "c"),
			b:        SetOf("a", "c"),
			expected: 2,
		},
		{
			a:        SetOf("a", "c"),
			b:        SetOf("a", "b", "c"),
			expected: 2,
		},
		{
			a:        SetOf("a", "b", "c"),
			b:        SetOf("a", "y", "c"),
			expected: 2,
		},
		{
			a:        SetOf("a", "b", "c"),
			b:        SetOf("x", "y", "z"),
			expected: 0,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%v %v=%v", test.a, test.b, test.expected), func(t *testing.T) {
			assert.Equal(t, test.expected, test.a.NumMatches(test.b))
		})
	}
}
