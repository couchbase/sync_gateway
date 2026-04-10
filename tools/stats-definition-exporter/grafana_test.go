// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included
// in the file licenses/APL2.txt.

package main

import (
	"bytes"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapUnit(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "none"},
		{"bytes", "bytes"},
		{"nanoseconds", "ns"},
		{"percent", "percent"},
		{"seconds", "s"},
		{"unix timestamp", "dateTimeAsIso"},
		{"unknown", "none"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := mapUnit(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestHasLabel(t *testing.T) {
	tests := []struct {
		name     string
		labels   []string
		label    string
		expected bool
	}{
		{"no labels", []string{}, "database", false},
		{"only database", []string{"database"}, "database", true},
		{"database and collection", []string{"collection", "database"}, "collection", true},
		{"only collection", []string{"collection"}, "database", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasLabel(tt.labels, tt.label)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSubsystemGrouping(t *testing.T) {
	stats := getTestStats(t)

	subsystems, grouped := statsBySubsystem(stats)

	// All stats should have a subsystem
	totalGrouped := 0
	for _, names := range grouped {
		totalGrouped += len(names)
	}
	assert.Equal(t, len(stats), totalGrouped)

	// Subsystems should be sorted
	for i := 1; i < len(subsystems); i++ {
		assert.LessOrEqual(t, subsystems[i-1], subsystems[i])
	}

	// Stats within each subsystem should be sorted
	for _, names := range grouped {
		for i := 1; i < len(names); i++ {
			assert.LessOrEqual(t, names[i-1], names[i])
		}
	}
}

// findChildPanel searches all row panels for a child panel with the given title
func findChildPanel(d dashboard, title string) *panel {
	for i := range d.Panels {
		for j := range d.Panels[i].Panels {
			if d.Panels[i].Panels[j].Title == title {
				return &d.Panels[i].Panels[j]
			}
		}
	}
	return nil
}

// getTestStats retrieves stats for testing
func getTestStats(t *testing.T) statDefinitions {
	t.Helper()

	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	stats, err := getStats(logger)
	require.NoError(t, err)
	require.NotEmpty(t, stats)

	return stats
}

// readFile reads a file and returns its contents (helper for testing)
func readFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}
