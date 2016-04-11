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
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestIsValidChannel(t *testing.T) {
	valid := []string{"*", "**", "a", "a ", "a b", "a*b", "FOO", "123", "-z", "foo_bar", "Éclær", "z7_", "!",  "Z∫•", "*!"}
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
	_, err = SetFromArray([]string{"chan1", "chan2", "bogus,name", "chan3"}, RemoveStar)
	assertTrue(t, err != nil, "SetFromArray didn't return an error")
}
