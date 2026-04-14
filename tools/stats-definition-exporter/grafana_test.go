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
	"testing"

	sdkdashboard "github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapUnit(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"", "locale"},
		{"bytes", "bytes"},
		{"nanoseconds", "ns"},
		{"percent", "percent"},
		{"seconds", "s"},
		{"unix timestamp", "dateTimeAsIso"},
		{"unknown", "locale"},
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

func TestBuildExprReplicationLabelOnly(t *testing.T) {
	// A stat with a "replication" label (which has no template variable on
	// either dashboard) should only surface the replication id in the legend
	// and must not inject a selector clause into the PromQL query.
	stat := statDefinition{Labels: []string{"replication"}}
	expr := buildExpr("sgw_test", stat, supportalConfig)
	assert.Equal(t, `sgw_test{databaseUuid="$databaseUuid",nodeHostname=~"$nodeHostname"}`, expr)

	legend := legendForLabels(stat.Labels, supportalConfig)
	assert.Equal(t, "{{nodeHostname}} {{replication}}", legend)
}

func TestBuildExprDatabaseAndReplicationLabels(t *testing.T) {
	// A stat carrying both database and replication labels must filter by
	// database ($endpoint) but still only surface replication in the legend.
	stat := statDefinition{Labels: []string{"database", "replication"}}
	expr := buildExpr("sgw_test", stat, capellaConfig)
	assert.Contains(t, expr, `database=~"$endpoint"`)
	assert.NotContains(t, expr, "$replication")

	legend := legendForLabels(stat.Labels, capellaConfig)
	assert.Equal(t, "{{node}} {{database}} {{replication}}", legend)
}

func TestBuildExprCapellaLabelReplace(t *testing.T) {
	// Exact-match the label_replace() wrapping Capella applies so a regression
	// that changes argument order / regex is caught by tests, not by a human
	// eyeballing the dashboard.
	stat := statDefinition{}
	expr := buildExpr("sgw_test", stat, capellaConfig)
	assert.Equal(t,
		`label_replace(sgw_test{databaseId="$databaseId",couchbaseNode=~"$couchbaseNode"}, "node", "$1", "couchbaseNode", "([^.]+).*")`,
		expr,
	)
}

func TestGenerateDashboardEmptyStats(t *testing.T) {
	// Empty stats should still produce a valid dashboard shell — all
	// annotations / template vars intact — just with zero row panels.
	d, err := generateGrafanaDashboard(statDefinitions{}, supportalConfig)
	require.NoError(t, err)
	assert.Empty(t, d.Panels)
	assert.NotEmpty(t, d.Templating.List)
	require.NotNil(t, d.Uid)
	assert.Equal(t, "sync-gateway-all", *d.Uid)
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
func findChildPanel(d sdkdashboard.Dashboard, title string) *sdkdashboard.Panel {
	for i := range d.Panels {
		rp := d.Panels[i].RowPanel
		if rp == nil {
			continue
		}
		for j := range rp.Panels {
			p := &rp.Panels[j]
			if p.Title != nil && *p.Title == title {
				return p
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
