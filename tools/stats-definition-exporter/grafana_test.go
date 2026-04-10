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
	"encoding/json"
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

func TestHasDatabaseLabel(t *testing.T) {
	tests := []struct {
		name     string
		labels   []string
		expected bool
	}{
		{"no labels", []string{}, false},
		{"only database", []string{"database"}, true},
		{"database and collection", []string{"collection", "database"}, true},
		{"only collection", []string{"collection"}, false},
		{"database and replication", []string{"database", "replication"}, true},
		{"only replication", []string{"replication"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hasDatabaseLabel(tt.labels)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSupportalGrafanaDashboardGeneration(t *testing.T) {
	stats := getTestStats(t)
	config := supportalConfig

	dashboard := generateGrafanaDashboard(stats, config)

	// Verify dashboard metadata
	assert.Equal(t, "sync-gateway-all", dashboard.UID)
	assert.Equal(t, "Sync Gateway All", dashboard.Title)
	assert.Equal(t, 42, dashboard.SchemaVersion)
	assert.Equal(t, []string{"Sync Gateway"}, dashboard.Tags)
	assert.True(t, dashboard.Editable)

	// Top-level panels should all be row panels
	for _, panel := range dashboard.Panels {
		assert.Equal(t, "row", panel.Type)
		require.NotNil(t, panel.Collapsed)
		assert.True(t, *panel.Collapsed)
	}

	// Count total child panels across all rows -- should match stat count
	totalChildPanels := 0
	for _, row := range dashboard.Panels {
		totalChildPanels += len(row.Panels)
	}
	assert.Equal(t, len(stats), totalChildPanels)

	// Row titles should be sorted alphabetically
	for i := 1; i < len(dashboard.Panels); i++ {
		assert.LessOrEqual(t, dashboard.Panels[i-1].Title, dashboard.Panels[i].Title)
	}

	// Verify child panels within each row are alphabetically ordered and are timeseries
	for _, row := range dashboard.Panels {
		for j := 1; j < len(row.Panels); j++ {
			assert.LessOrEqual(t, row.Panels[j-1].Title, row.Panels[j].Title)
		}
		for _, panel := range row.Panels {
			assert.Equal(t, "timeseries", panel.Type)
			require.NotNil(t, panel.Datasource)
			assert.Equal(t, "prometheus", panel.Datasource.Type)
			assert.Equal(t, "mimir", panel.Datasource.UID)
		}
	}

	// Verify template variables exist
	varNames := make(map[string]bool)
	for _, v := range dashboard.templating.List {
		varNames[v.Name] = true
	}
	assert.True(t, varNames["databaseUuid"], "databaseUuid variable should exist")
	assert.True(t, varNames["nodeHostname"], "nodeHostname variable should exist")
	assert.True(t, varNames["endpoint"], "endpoint variable should exist")
}

func TestCapellaGrafanaDashboardGeneration(t *testing.T) {
	stats := getTestStats(t)
	config := capellaConfig

	dashboard := generateGrafanaDashboard(stats, config)

	// Verify dashboard metadata
	assert.Equal(t, "sync-gateway-all-stats", dashboard.UID)
	assert.Equal(t, "Sync Gateway All Stats", dashboard.Title)
	assert.Equal(t, 38, dashboard.SchemaVersion)

	// Top-level panels should all be row panels
	for _, panel := range dashboard.Panels {
		assert.Equal(t, "row", panel.Type)
	}

	// Count total child panels
	totalChildPanels := 0
	for _, row := range dashboard.Panels {
		totalChildPanels += len(row.Panels)
	}
	assert.Equal(t, len(stats), totalChildPanels)

	// Verify child panels use correct datasource
	for _, row := range dashboard.Panels {
		for _, panel := range row.Panels {
			assert.Equal(t, "timeseries", panel.Type)
			require.NotNil(t, panel.Datasource)
			assert.Equal(t, "prometheus", panel.Datasource.Type)
			assert.Equal(t, "${DataSource}", panel.Datasource.UID)
		}
	}

	// Verify template variables exist
	varNames := make(map[string]bool)
	for _, v := range dashboard.templating.List {
		varNames[v.Name] = true
	}
	assert.True(t, varNames["databaseId"], "databaseId variable should exist")
	assert.True(t, varNames["endpoint"], "endpoint variable should exist")
	assert.True(t, varNames["DataSource"], "DataSource variable should exist")
	assert.True(t, varNames["syncgatewayId"], "syncgatewayId hidden variable should exist")
}

// findChildPanel searches all row panels for a child panel with the given title
func findChildPanel(dashboard dashboard, title string) *panel {
	for i := range dashboard.Panels {
		for j := range dashboard.Panels[i].Panels {
			if dashboard.Panels[i].Panels[j].Title == title {
				return &dashboard.Panels[i].Panels[j]
			}
		}
	}
	return nil
}

func TestSupportalExprGeneration(t *testing.T) {
	stats := getTestStats(t)
	config := supportalConfig

	// Find a global stat (no database label)
	var globalStatName string
	var dbScopedStatName string
	for name, stat := range stats {
		if len(stat.Labels) == 0 && globalStatName == "" {
			globalStatName = name
		}
		if hasDatabaseLabel(stat.Labels) && dbScopedStatName == "" {
			dbScopedStatName = name
		}
		if globalStatName != "" && dbScopedStatName != "" {
			break
		}
	}

	require.NotEmpty(t, globalStatName, "should find a global stat")
	require.NotEmpty(t, dbScopedStatName, "should find a database-scoped stat")

	dashboard := generateGrafanaDashboard(stats, config)

	globalPanel := findChildPanel(dashboard, config.MetricPrefix+globalStatName)
	dbScopedPanel := findChildPanel(dashboard, config.MetricPrefix+dbScopedStatName)

	require.NotNil(t, globalPanel, "should find global stat panel")
	require.NotNil(t, dbScopedPanel, "should find database-scoped stat panel")

	// Verify global stat expression
	require.Len(t, globalPanel.Targets, 1)
	assert.Contains(t, globalPanel.Targets[0].Expr, `databaseUuid="$databaseUuid"`)
	assert.Contains(t, globalPanel.Targets[0].Expr, `nodeHostname=~"$nodeHostname"`)
	assert.NotContains(t, globalPanel.Targets[0].Expr, "endpoint")

	// Verify database-scoped stat expression
	require.Len(t, dbScopedPanel.Targets, 1)
	assert.Contains(t, dbScopedPanel.Targets[0].Expr, `databaseUuid="$databaseUuid"`)
	assert.Contains(t, dbScopedPanel.Targets[0].Expr, `nodeHostname=~"$nodeHostname"`)
	assert.Contains(t, dbScopedPanel.Targets[0].Expr, `database=~"$endpoint"`)

	// Verify legend formats
	assert.Equal(t, "{{nodeHostname}}", globalPanel.Targets[0].LegendFormat)
	assert.Equal(t, "{{nodeHostname}} {{database}}", dbScopedPanel.Targets[0].LegendFormat)
}

func TestCapellaExprGeneration(t *testing.T) {
	stats := getTestStats(t)
	config := capellaConfig

	// Find a global stat (no database label)
	var globalStatName string
	var dbScopedStatName string
	for name, stat := range stats {
		if len(stat.Labels) == 0 && globalStatName == "" {
			globalStatName = name
		}
		if hasDatabaseLabel(stat.Labels) && dbScopedStatName == "" {
			dbScopedStatName = name
		}
		if globalStatName != "" && dbScopedStatName != "" {
			break
		}
	}

	require.NotEmpty(t, globalStatName, "should find a global stat")
	require.NotEmpty(t, dbScopedStatName, "should find a database-scoped stat")

	dashboard := generateGrafanaDashboard(stats, config)

	// Capella has no prefix
	globalPanel := findChildPanel(dashboard, globalStatName)
	dbScopedPanel := findChildPanel(dashboard, dbScopedStatName)

	require.NotNil(t, globalPanel, "should find global stat panel")
	require.NotNil(t, dbScopedPanel, "should find database-scoped stat panel")

	// Verify global stat expression (no nodeHostname in Capella)
	require.Len(t, globalPanel.Targets, 1)
	assert.Contains(t, globalPanel.Targets[0].Expr, `databaseId="$databaseId"`)
	assert.NotContains(t, globalPanel.Targets[0].Expr, "nodeHostname")
	assert.NotContains(t, globalPanel.Targets[0].Expr, "endpoint")

	// Verify database-scoped stat expression
	require.Len(t, dbScopedPanel.Targets, 1)
	assert.Contains(t, dbScopedPanel.Targets[0].Expr, `databaseId="$databaseId"`)
	assert.Contains(t, dbScopedPanel.Targets[0].Expr, `database=~"$endpoint"`)

	// Verify legend formats (different from supportal)
	assert.Equal(t, "{{databaseId}}", globalPanel.Targets[0].LegendFormat)
	assert.Equal(t, "{{database}}", dbScopedPanel.Targets[0].LegendFormat)
}

func TestWriteGrafanaDashboardJSON(t *testing.T) {
	stats := getTestStats(t)

	var buf bytes.Buffer
	err := writeGrafanaDashboard(stats, supportalConfig, &buf)
	require.NoError(t, err)

	// Verify it produces valid JSON
	var raw map[string]interface{}
	err = json.Unmarshal(buf.Bytes(), &raw)
	require.NoError(t, err)

	assert.Equal(t, "sync-gateway-all", raw["uid"])
}

func TestGrafanaFormatFileOutput(t *testing.T) {
	outputFile := t.TempDir() + "/" + t.Name() + ".json"

	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	err := statsToFile(logger, &outputFile, formatSupportalGrafana)
	assert.NoError(t, err)
	assert.Empty(t, buf)
	assert.FileExists(t, outputFile)

	data, err := readFile(outputFile)
	require.NoError(t, err)

	var raw map[string]interface{}
	err = json.Unmarshal(data, &raw)
	require.NoError(t, err)
	assert.Equal(t, "sync-gateway-all", raw["uid"])
}

func TestGrafanaFormatStdOutput(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	err := statsToFile(logger, nil, formatCapellaGrafana)
	assert.NoError(t, err)
	assert.Empty(t, buf)
}

func TestUnitMappingInPanels(t *testing.T) {
	stats := getTestStats(t)

	// Find a stat with bytes unit
	var bytesStatName string
	for name, stat := range stats {
		if stat.Unit == "bytes" {
			bytesStatName = name
			break
		}
	}

	require.NotEmpty(t, bytesStatName, "should find a stat with bytes unit")

	dashboard := generateGrafanaDashboard(stats, supportalConfig)

	panel := findChildPanel(dashboard, supportalConfig.MetricPrefix+bytesStatName)
	require.NotNil(t, panel, "should find panel for bytes stat")
	require.NotNil(t, panel.fieldConfig)
	assert.Equal(t, "bytes", panel.fieldConfig.Defaults.Unit)
}

func TestDescriptionFromHelp(t *testing.T) {
	stats := getTestStats(t)

	// Find a stat with help text
	var statWithHelp string
	var helpText string
	for name, stat := range stats {
		if stat.Help != "" {
			statWithHelp = name
			helpText = stat.Help
			break
		}
	}

	require.NotEmpty(t, statWithHelp, "should find a stat with help text")

	dashboard := generateGrafanaDashboard(stats, supportalConfig)

	panel := findChildPanel(dashboard, supportalConfig.MetricPrefix+statWithHelp)
	require.NotNil(t, panel, "should find panel with help text")
	assert.Equal(t, helpText, panel.Description)
}

func TestRowPanelStructure(t *testing.T) {
	stats := getTestStats(t)
	dashboard := generateGrafanaDashboard(stats, supportalConfig)

	// Verify all top-level panels are collapsed rows
	require.NotEmpty(t, dashboard.Panels)
	for _, row := range dashboard.Panels {
		assert.Equal(t, "row", row.Type, "top-level panel should be a row")
		require.NotNil(t, row.Collapsed, "row should have collapsed field")
		assert.True(t, *row.Collapsed, "row should be collapsed")
		assert.NotEmpty(t, row.Panels, "row should contain child panels for %s", row.Title)
		assert.Equal(t, 24, row.gridPos.W, "row should be full width")
		assert.Equal(t, 1, row.gridPos.H, "row should have height 1")
	}

	// Verify child panels have correct dimensions
	for _, row := range dashboard.Panels {
		for _, panel := range row.Panels {
			assert.Equal(t, 24, panel.gridPos.W, "child panel should be full width")
			assert.Equal(t, 8, panel.gridPos.H, "child panel should have height 8")
		}
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

func TestSubsystemDisplayNames(t *testing.T) {
	assert.Equal(t, "Cache", subsystemDisplayName("cache"))
	assert.Equal(t, "Database", subsystemDisplayName("database"))
	assert.Equal(t, "Replication (ISGR)", subsystemDisplayName("replication"))
	assert.Equal(t, "Resource Utilization", subsystemDisplayName("resource_utilization"))
	// Unknown subsystem should return the raw value
	assert.Equal(t, "unknown_subsystem", subsystemDisplayName("unknown_subsystem"))
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
