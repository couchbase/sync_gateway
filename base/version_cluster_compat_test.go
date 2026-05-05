// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClusterCompatVersionAtLeast(t *testing.T) {
	tests := []struct {
		name     string
		version  ClusterCompatVersion
		major    uint8
		minor    uint8
		expected bool
	}{
		{"exact match", NewClusterCompatVersion(4, 0), 4, 0, true},
		{"higher minor", NewClusterCompatVersion(4, 1), 4, 0, true},
		{"higher major", NewClusterCompatVersion(5, 0), 4, 1, true},
		{"lower minor", NewClusterCompatVersion(4, 0), 4, 1, false},
		{"lower major", NewClusterCompatVersion(3, 9), 4, 0, false},
		{"zero version", NewClusterCompatVersion(0, 0), 0, 0, true},
		{"zero below", NewClusterCompatVersion(0, 0), 0, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.version.AtLeast(tt.major, tt.minor))
		})
	}
}

func TestMinClusterCompatVersion(t *testing.T) {
	tests := []struct {
		name     string
		versions []ClusterCompatVersion
		expected ClusterCompatVersion
	}{
		{"single version", []ClusterCompatVersion{NewClusterCompatVersion(4, 1)}, NewClusterCompatVersion(4, 1)},
		{"min by minor", []ClusterCompatVersion{NewClusterCompatVersion(4, 1), NewClusterCompatVersion(4, 0)}, NewClusterCompatVersion(4, 0)},
		{"min by major", []ClusterCompatVersion{NewClusterCompatVersion(5, 0), NewClusterCompatVersion(4, 9)}, NewClusterCompatVersion(4, 9)},
		{"all equal", []ClusterCompatVersion{NewClusterCompatVersion(4, 0), NewClusterCompatVersion(4, 0)}, NewClusterCompatVersion(4, 0)},
		{"three versions", []ClusterCompatVersion{NewClusterCompatVersion(4, 1), NewClusterCompatVersion(3, 5), NewClusterCompatVersion(4, 0)}, NewClusterCompatVersion(3, 5)},
		{"no versions", nil, ClusterCompatVersion{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, MinClusterCompatVersion(tt.versions...))
		})
	}
}

func TestClusterCompatVersionString(t *testing.T) {
	assert.Equal(t, "4.0", NewClusterCompatVersion(4, 0).String())
	assert.Equal(t, "0.0", NewClusterCompatVersion(0, 0).String())
	assert.Equal(t, "12.34", NewClusterCompatVersion(12, 34).String())
}

func TestClusterCompatVersionJSON(t *testing.T) {
	v := NewClusterCompatVersion(4, 1)

	data, err := JSONMarshal(v)
	require.NoError(t, err)
	assert.Equal(t, `"4.1"`, string(data))

	var parsed ClusterCompatVersion
	err = JSONUnmarshal(data, &parsed)
	require.NoError(t, err)
	assert.Equal(t, v, parsed)
}

func TestParseClusterCompatVersion(t *testing.T) {
	tests := []struct {
		input   string
		want    ClusterCompatVersion
		wantErr bool
	}{
		{"4.0", NewClusterCompatVersion(4, 0), false},
		{"0.0", NewClusterCompatVersion(0, 0), false},
		{"255.255", NewClusterCompatVersion(255, 255), false},
		{"4", ClusterCompatVersion{}, true},
		{"", ClusterCompatVersion{}, true},
		{"4.0.1", ClusterCompatVersion{}, true},
		{"abc.0", ClusterCompatVersion{}, true},
		{"4.abc", ClusterCompatVersion{}, true},
		{"256.0", ClusterCompatVersion{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseClusterCompatVersion(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
