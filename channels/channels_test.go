//  Copyright (c) 2012-2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package channels

import (
	"github.com/sdegutis/go.assert"
	"testing"
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

func TestSimplifyChannels(t *testing.T) {
	cases := [][][]string{
		{{}, {}},
		{{""}, {}},
		{{"*"}, {}},
		{{"a"}, {"a"}},
		{{"a", "b"}, {"a", "b"}},
		{{"a", "a"}, {"a"}},
		{{"a", "bad "}, {"a"}},
		{{"a", "b", "a"}, {"a", "b"}},
		{{"a", "*", "b"}, {"a", "b"}},
	}
	for _, cas := range cases {
		assert.DeepEquals(t, SimplifyChannels(cas[0], false), cas[1])
	}
}

func TestSimplifyChannelsWithStar(t *testing.T) {
	cases := [][][]string{
		{{}, {}},
		{{""}, {}},
		{{"*"}, {"*"}},
		{{"a"}, {"a"}},
		{{"a", "b"}, {"a", "b"}},
		{{"a", "a"}, {"a"}},
		{{"a", "bad "}, {"a"}},
		{{"a", "b", "a"}, {"a", "b"}},
		{{"a", "*", "b"}, {"*"}},
	}
	for _, cas := range cases {
		assert.DeepEquals(t, SimplifyChannels(cas[0], true), cas[1])
	}
}
