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
	assert.Equal(t, `{"Channels":{}}`, string(bytes))

	str.Channels = AtSequence(SetOf(t, "a"), 17)
	bytes, err = base.JSONMarshal(str)
	assert.NoError(t, err, "Marshal")
	assert.Equal(t, `{"Channels":{"a":17}}`, string(bytes))

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
	assert.Equal(t, TimedSet(nil), str.Channels)

	err = base.JSONUnmarshal([]byte(`{"channels":{}}`), &str)
	assert.NoError(t, err, "Unmarshal empty")
	assert.Equal(t, TimedSet{}, str.Channels)

	err = base.JSONUnmarshal([]byte(`{"channels":{"a":17,"b":17}}`), &str)
	assert.NoError(t, err, "Unmarshal sequence only")
	assert.Equal(t, TimedSet{"a": NewVbSimpleSequence(17), "b": NewVbSimpleSequence(17)}, str.Channels)

	// Now try unmarshaling the alternative array form:
	err = base.JSONUnmarshal([]byte(`{"channels":[]}`), &str)
	assert.NoError(t, err, "Unmarshal empty array")
	assert.Equal(t, TimedSet{}, str.Channels)

	err = base.JSONUnmarshal([]byte(`{"channels":["a","b"]}`), &str)
	assert.NoError(t, err, "Unmarshal populated array")
	assert.Equal(t, TimedSet{"a": NewVbSimpleSequence(0), "b": NewVbSimpleSequence(0)}, str.Channels)

	err = base.JSONUnmarshal([]byte(`{"channels":{"a":{"seq":17, "vb":21},"b":{"seq":23, "vb":25}}}`), &str)
	assert.NoError(t, err, "Unmarshal sequence and vbucket only")
	assert.Equal(t, fmt.Sprintf("%s", TimedSet{"a": NewVbSequence(21, 17), "b": NewVbSequence(25, 23)}), fmt.Sprintf("%s", str.Channels))
}

func TestEncodeSequenceID(t *testing.T) {
	set := TimedSet{"ABC": NewVbSimpleSequence(17), "CBS": NewVbSimpleSequence(23), "BBC": NewVbSimpleSequence(1)}
	encoded := set.String()
	assert.Equal(t, "ABC:17,BBC:1,CBS:23", encoded)
	decoded := TimedSetFromString(encoded)
	assert.Equal(t, set, decoded)

	assert.Equal(t, "ABC:17", TimedSet{"ABC": NewVbSimpleSequence(17), "CBS": NewVbSimpleSequence(0)}.String())

	assert.Equal(t, TimedSet{}, TimedSetFromString(""))
	assert.Equal(t, TimedSet{"ABC": NewVbSimpleSequence(17)}, TimedSetFromString("ABC:17"))

	assert.Equal(t, TimedSet(nil), TimedSetFromString(":17"))
	assert.Equal(t, TimedSet(nil), TimedSetFromString("ABC:"))
	assert.Equal(t, TimedSet(nil), TimedSetFromString("ABC:0"))
	assert.Equal(t, TimedSet(nil), TimedSetFromString("ABC:-1"))
	assert.Equal(t, TimedSet(nil), TimedSetFromString("ABC:17,"))
	assert.Equal(t, TimedSet(nil), TimedSetFromString(",ABC:17"))
	assert.Equal(t, TimedSet(nil), TimedSetFromString("ABC:17,,NBC:12"))
	assert.Equal(t, TimedSet(nil), TimedSetFromString("ABC:17,ABC:12"))
}

func TestEqualsWithEqualSet(t *testing.T) {
	set1 := TimedSet{"ABC": NewVbSimpleSequence(17), "CBS": NewVbSimpleSequence(23), "BBC": NewVbSimpleSequence(1)}
	set2 := base.SetFromArray([]string{"ABC", "CBS", "BBC"})
	assert.True(t, set1.Equals(set2))

}

func TestEqualsWithUnequalSet(t *testing.T) {
	set1 := TimedSet{"ABC": NewVbSimpleSequence(17), "CBS": NewVbSimpleSequence(23), "BBC": NewVbSimpleSequence(1)}
	set2 := base.SetFromArray([]string{"ABC", "BBC"})
	assert.True(t, !set1.Equals(set2))
	set3 := base.SetFromArray([]string{"ABC", "BBC", "CBS", "FOO"})
	assert.True(t, !set1.Equals(set3))

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
