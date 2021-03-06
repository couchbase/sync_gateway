//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package channels

import (
	"fmt"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
)

func TestTimedSetMarshal(t *testing.T) {
	var str struct {
		Channels TimedSet
	}
	bytes, err := base.JSONMarshal(str)
	assert.NoError(t, err, "Marshal")
	assert.Equal(t, string(bytes), `{"Channels":null}`)

	str.Channels = TimedSet{}
	bytes, err = base.JSONMarshal(str)
	assert.NoError(t, err, "Marshal")
	goassert.Equals(t, string(bytes), `{"Channels":{}}`)

	str.Channels = AtSequence(SetOf(t, "a"), 17)
	bytes, err = base.JSONMarshal(str)
	assert.NoError(t, err, "Marshal")
	goassert.Equals(t, string(bytes), `{"Channels":{"a":17}}`)

	str.Channels = AtSequence(SetOf(t, "a", "b"), 17)
	bytes, err = base.JSONMarshal(str)
	assert.NoError(t, err, "Marshal")
	// Ordering of JSON keys can vary - so just check each channel is present with the correct sequence
	assert.True(t, strings.Contains(string(bytes), `"a":17`))
	assert.True(t, strings.Contains(string(bytes), `"b":17`))
}

func TestTimedSetUnmarshal(t *testing.T) {
	var str struct {
		Channels TimedSet
	}
	err := base.JSONUnmarshal([]byte(`{"channels":null}`), &str)
	assert.NoError(t, err, "Unmarshal")
	goassert.DeepEquals(t, str.Channels, TimedSet(nil))

	err = base.JSONUnmarshal([]byte(`{"channels":{}}`), &str)
	assert.NoError(t, err, "Unmarshal empty")
	goassert.DeepEquals(t, str.Channels, TimedSet{})

	err = base.JSONUnmarshal([]byte(`{"channels":{"a":17,"b":17}}`), &str)
	assert.NoError(t, err, "Unmarshal sequence only")
	goassert.DeepEquals(t, str.Channels, TimedSet{"a": NewVbSimpleSequence(17), "b": NewVbSimpleSequence(17)})

	// Now try unmarshaling the alternative array form:
	err = base.JSONUnmarshal([]byte(`{"channels":[]}`), &str)
	assert.NoError(t, err, "Unmarshal empty array")
	goassert.DeepEquals(t, str.Channels, TimedSet{})

	err = base.JSONUnmarshal([]byte(`{"channels":["a","b"]}`), &str)
	assert.NoError(t, err, "Unmarshal populated array")
	goassert.DeepEquals(t, str.Channels, TimedSet{"a": NewVbSimpleSequence(0), "b": NewVbSimpleSequence(0)})

	err = base.JSONUnmarshal([]byte(`{"channels":{"a":{"seq":17, "vb":21},"b":{"seq":23, "vb":25}}}`), &str)
	assert.NoError(t, err, "Unmarshal sequence and vbucket only")
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

func TestTimedSetCompareKeys(t *testing.T) {
	tests := []struct {
		name     string
		fromSet  TimedSet
		toSet    TimedSet
		expected ChangedKeys
	}{
		{
			name:     "NoChange",
			fromSet:  TimedSetFromString("ABC:1,BBC:2"),
			toSet:    TimedSetFromString("ABC:1,BBC:2"),
			expected: ChangedKeys{},
		},
		{
			name:     "AddedKey",
			fromSet:  TimedSetFromString("ABC:1"),
			toSet:    TimedSetFromString("ABC:1,BBC:2"),
			expected: ChangedKeys{"BBC": true},
		},
		{
			name:     "RemovedKey",
			fromSet:  TimedSetFromString("ABC:1,BBC:2"),
			toSet:    TimedSetFromString("BBC:2"),
			expected: ChangedKeys{"ABC": false},
		},
		{
			name:     "AddedAll",
			fromSet:  TimedSet{},
			toSet:    TimedSetFromString("ABC:1,BBC:2"),
			expected: ChangedKeys{"ABC": true, "BBC": true},
		},
		{
			name:     "RemovedAll",
			fromSet:  TimedSetFromString("ABC:1,BBC:2"),
			toSet:    TimedSet{},
			expected: ChangedKeys{"ABC": false, "BBC": false},
		},
		{
			name:     "AddAndRemove",
			fromSet:  TimedSetFromString("ABC:1,BBC:2,NBC:1"),
			toSet:    TimedSetFromString("ABC:1,HBO:3,PBS:5"),
			expected: ChangedKeys{"BBC": false, "NBC": false, "HBO": true, "PBS": true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			result := test.toSet.CompareKeys(test.fromSet)
			assert.Equal(t, len(test.expected), len(result))
			for expectedChannel, expectedValue := range test.expected {
				actualValue, found := result[expectedChannel]
				assert.True(t, found)
				assert.Equal(t, expectedValue, actualValue)
			}
		})
	}
}
