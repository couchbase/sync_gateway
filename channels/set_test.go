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
		assert.Equal(t, SetOf(t, cas[1]...), channels)
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
		assert.Equal(t, SetOf(t, cas[1]...), channels)
	}
}

func TestSetFromArrayError(t *testing.T) {
	_, err := SetFromArray([]string{""}, RemoveStar)
	assert.True(t, err != nil, "SetFromArray didn't return an error")
	_, err = SetFromArray([]string{"chan1", "chan2", "bogus,name", "chan3"}, RemoveStar)
	assert.True(t, err != nil, "SetFromArray didn't return an error")
}
