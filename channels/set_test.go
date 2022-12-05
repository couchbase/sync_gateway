//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package channels

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsValidChannel(t *testing.T) {
	valid := []string{"*", "**", "a", "a ", "a b", "a*b", "FOO", "123", "-z", "foo_bar", "Éclær", "z7_", "!", "Z∫•", "*!"}
	for _, ch := range valid {
		if !IsValidChannel(ch) {
			t.Errorf("IsValidChannel(%q) should be true", ch)
		}
	}
	invalid := []string{"", "*,*", "a,*", "a, ", "b,?", ",", "Z,∫•", "*,!"}
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
		assert.NoError(t, err, "SetFromArray failed")
		assert.Equal(t, BaseSetOf(t, cas[1]...), channels)
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
		assert.NoError(t, err, "SetFromArray failed")
		assert.Equal(t, BaseSetOf(t, cas[1]...), channels)
	}
}

func TestSetFromArrayError(t *testing.T) {
	_, err := SetFromArray([]string{""}, RemoveStar)
	assert.True(t, err != nil, "SetFromArray didn't return an error")
	_, err = SetFromArray([]string{"chan1", "chan2", "bogus,name", "chan3"}, RemoveStar)
	assert.True(t, err != nil, "SetFromArray didn't return an error")
}

func TestSetFromArrayNoValidate(t *testing.T) {
	testCases := []struct {
		name   string
		input  []ID
		output Set
	}{
		{
			name:  "singleID",
			input: []ID{NewID("A", 1)},
			output: Set{
				NewID("A", 1): present{},
			},
		},
		{
			name: "twoIDs",
			input: []ID{
				NewID("A", 1),
				NewID("A", 2),
			},
			output: Set{
				NewID("A", 1): present{},
				NewID("A", 2): present{},
			},
		},
		{
			name:  "illegalChannel",
			input: []ID{NewID(",", 1)},
			output: Set{
				NewID(",", 1): present{},
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.output, SetFromArrayNoValidate(test.input))
		})
	}
}

func TestSetFromSingleCollection(t *testing.T) {
	testCases := []struct {
		name         string
		input        []string
		collectionID uint32
		output       Set
	}{
		{
			name:         "singleChannel0",
			input:        []string{"A"},
			collectionID: 0,
			output: Set{
				NewID("A", 0): present{},
			},
		},

		{
			name:         "singleChannel1",
			input:        []string{"A"},
			collectionID: 1,
			output: Set{
				NewID("A", 1): present{},
			},
		},
		{
			name:         "multiChannel0",
			input:        []string{"A", "B"},
			collectionID: 0,
			output: Set{
				NewID("A", 0): present{},
				NewID("B", 0): present{},
			},
		},

		{
			name:         "multiChannel1",
			input:        []string{"A", "B"},
			collectionID: 1,
			output: Set{
				NewID("A", 1): present{},
				NewID("B", 1): present{},
			},
		},
		{
			name:         "illegalChannel",
			input:        []string{","},
			collectionID: 1,
			output: Set{
				NewID(",", 1): present{},
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.output, SetOfFromSingleCollection(test.input, test.collectionID))
		})
	}
}

func TestSetUpdate(t *testing.T) {
	testCases := []struct {
		name        string
		set1        Set
		set2        Set
		combinedSet Set
	}{
		{
			name:        "emptysets",
			set1:        Set{},
			set2:        Set{},
			combinedSet: Set{},
		},
		{
			name: "set1empty",
			set1: Set{},
			set2: Set{
				NewID("A", 1): present{},
			},
			combinedSet: Set{
				NewID("A", 1): present{},
			},
		},
		{
			name: "set2empty",
			set1: Set{
				NewID("A", 1): present{},
			},
			set2: Set{},
			combinedSet: Set{
				NewID("A", 1): present{},
			},
		},
		{
			name: "samedata",
			set1: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
			},
			set2: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
			},
			combinedSet: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
			},
		},
		{
			name: "somesamedata",
			set1: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
			},
			set2: Set{
				NewID("A", 1): present{},
				NewID("C", 1): present{},
			},
			combinedSet: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
				NewID("C", 1): present{},
			},
		},
		{
			name: "alldifferent",
			set1: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
			},
			set2: Set{
				NewID("C", 1): present{},
				NewID("D", 1): present{},
			},
			combinedSet: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
				NewID("C", 1): present{},
				NewID("D", 1): present{},
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.combinedSet, test.set1.Update(test.set2))
			chans := []ID{}
			for ch := range test.set2 {
				chans = append(chans, ch)
			}
			require.Equal(t, test.combinedSet, test.set1.UpdateWithSlice(chans))
		})
	}
}

func TestSetAdd(t *testing.T) {
	testCases := []struct {
		name     string
		inputSet Set
		inputID  ID
		result   Set
	}{
		{
			name:     "empty",
			inputSet: Set{},
			inputID:  ID{},
			result:   Set{ID{}: present{}},
		},
		{
			name:     "inputSetempty",
			inputSet: Set{},
			inputID:  NewID("A", 1),
			result: Set{
				NewID("A", 1): present{},
			},
		},
		{
			name: "IDempty",
			inputSet: Set{
				NewID("A", 1): present{},
			},
			inputID: ID{},
			result: Set{
				ID{}:          present{},
				NewID("A", 1): present{},
			},
		},
		{
			name: "samedata",
			inputSet: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
			},
			inputID: NewID("A", 1),
			result: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
			},
		},
		{
			name: "differenta",
			inputSet: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
			},
			inputID: NewID("C", 1),
			result: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
				NewID("C", 1): present{},
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.result, test.inputSet.Add(test.inputID))
		})
	}
}

func TestSetContains(t *testing.T) {
	testCases := []struct {
		name     string
		inputSet Set
		inputID  ID
		contains bool
	}{
		{
			name:     "empty,emptyID",
			inputSet: Set{},
			inputID:  ID{},
			contains: false,
		},
		{
			name:     "inputSetempty,realID",
			inputSet: Set{},
			inputID:  NewID("A", 1),
			contains: false,
		},
		{
			name: "nonemptyset,emptyinput",
			inputSet: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
				NewID("C", 1): present{},
			},
			inputID:  ID{},
			contains: false,
		},
		{
			name: "somedatapresent",
			inputSet: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
				NewID("C", 1): present{},
			},
			inputID:  NewID("A", 1),
			contains: true,
		},
		{
			name: "somedatanotpresent",
			inputSet: Set{
				NewID("A", 1): present{},
				NewID("B", 2): present{},
				NewID("C", 1): present{},
			},
			inputID:  NewID("D", 1),
			contains: false,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.contains, test.inputSet.Contains(test.inputID))
		})
	}
}
