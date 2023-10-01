// Copyright 2022-Present Couchbase, Inc.
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComparableVersion(t *testing.T) {
	// An *ascending* list of valid versions (order is required for comparison testing)
	testDataComparableVersions := []struct {
		str string
	}{
		{"0.0.0"}, // min
		{"0.0.0.1"},
		{"0.0.1"},
		{"0.0.1.1"},
		{"0.1.0"},
		{"0.1.0.1"},
		{"0.1.0.1@2"}, // build ordering
		{"0.1.0.1@11"},
		{"0.1.0.2"},
		{"0.1.1"},
		{"0.1.1.1"},
		{"0.1.1.1@1-CE"}, // edition ordering
		{"0.1.1.1@1-EE"},
		{"1.0.0"},
		{"1.0.0.1"},
		{"1.1.1.1"},
		{"2.3.4"},
		{"2.3.4.5"},
		{"2.3.5"},
		{"11.0.0"}, // check for lexicographic ordering
		{"31.3.3.7"},
		{"32.1.0"},
		{"1:22.3.25"}, // maintain ordering with new epoch when defining new versioning scheme (yy.m.d)
		{"2:1.0.0"},
		{"8:7.6.5.4@3-EE"},
		{"255:255.255.255.255@65535-EE"}, // max
	}

	for i, test := range testDataComparableVersions {
		t.Run(test.str, func(t *testing.T) {
			current, err := NewComparableVersionFromString(test.str)
			require.NoError(t, err)

			// string->version->string round-trip
			assert.Equal(t, test.str, current.String())

			// comparisons (Less/Equal)
			if i > 1 {
				prevStr := testDataComparableVersions[i-1].str
				previous, err := NewComparableVersionFromString(prevStr)
				require.NoError(t, err)

				assert.Truef(t, previous.Less(current), "incorrect comparison: expected %q < %q", prevStr, test.str)
				assert.Falsef(t, current.Equal(previous), "incorrect comparison: expected %q != %q", prevStr, test.str)
				assert.Falsef(t, current.Less(previous), "incorrect comparison: expected %q > %q", test.str, prevStr)
			}
		})
	}
}

func TestInvalidComparableVersion(t *testing.T) {
	// A list of invalid ComparableVersion
	tests := []struct {
		ver string
	}{
		{""},
		{":..@-"},
		{":...@-"},
		{"."},
		{".."},
		{"..."},
		{"...."},
		{"1:"},
		{"@1"},
		{"-EE"},
		{"-1"},
		{"0"},
		{"0.1"},
		{"0.0."},
		{"0.0.0.a"},
		{"0.0.a.0"},
		{"0.a.0.0"},
		{"a.0.0.0"},
		{"1.1.1-3"},                 // invalid edition
		{"1.1.1-ZZ"},                // invalid edition
		{"a:1.1.1-EE"},              // invalid epoch
		{"1.1.1@a-EE"},              // invalid build
		{"256.1.1"},                 // overflowing major
		{"1.256.1"},                 // overflowing minor
		{"1.1.256"},                 // overflowing patch
		{"1.1.1.256"},               // overflowing other
		{"1.1.1@65536"},             // overflowing build
		{"256:1.1.1-EE"},            // overflowing epoch
		{"256.256.256.256@65536-3"}, // overflowing all
	}

	for _, test := range tests {
		t.Run(test.ver, func(t *testing.T) {
			ver, err := NewComparableVersionFromString(test.ver)
			assert.Error(t, err)
			assert.Nil(t, ver)
		})
	}
}

func TestJSONRoundTrip(t *testing.T) {
	json, err := JSONMarshal(ProductVersion)
	require.NoError(t, err)
	var version ComparableVersion
	err = JSONUnmarshal(json, &version)
	require.NoError(t, err)
	require.True(t, ProductVersion.Equal(&version))
	require.Equal(t, ProductVersion.String(), version.String())
}

func TestAtLeastMinorDowngradeVersion(t *testing.T) {
	testCases := []struct {
		versionA       string
		versionB       string
		minorDowngrade bool
	}{
		{
			versionA:       "1.0.0",
			versionB:       "1.0.0",
			minorDowngrade: false,
		},
		{
			versionA:       "1.0.0",
			versionB:       "2.0.0",
			minorDowngrade: false,
		},
		{
			versionA:       "2.0.0",
			versionB:       "1.0.0",
			minorDowngrade: true,
		},
		{
			versionA:       "1.0.0",
			versionB:       "1.0.1",
			minorDowngrade: false,
		},
		{
			versionA:       "1.0.1",
			versionB:       "1.0.0",
			minorDowngrade: false,
		},
		{
			versionA:       "1.1.0",
			versionB:       "1.0.0",
			minorDowngrade: true,
		},
		{
			versionA:       "1.0.0",
			versionB:       "1.1.0",
			minorDowngrade: false,
		},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s->%s", test.versionA, test.versionB), func(t *testing.T) {
			versionA, err := NewComparableVersionFromString(test.versionA)
			require.NoError(t, err)

			versionB, err := NewComparableVersionFromString(test.versionB)
			require.NoError(t, err)
			require.Equal(t, test.minorDowngrade, versionA.AtLeastMinorDowngrade(versionB))
		})
	}
}

func BenchmarkComparableVersion(b *testing.B) {
	const str = "8:7.6.5.4@3-EE"

	current, err := NewComparableVersionFromString(str)
	require.NoError(b, err)

	b.Run("parseComparableVersion", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, _, _, _, _, _, _, _ = parseComparableVersion(str)
		}
	})
	b.Run("formatComparableVersion", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = current.formatComparableVersion()
		}
	})
}
