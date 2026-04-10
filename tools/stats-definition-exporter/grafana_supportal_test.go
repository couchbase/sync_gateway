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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	for _, v := range dashboard.Templating.List {
		varNames[v.Name] = true
	}
	assert.True(t, varNames["databaseUuid"], "databaseUuid variable should exist")
	assert.True(t, varNames["nodeHostname"], "nodeHostname variable should exist")
	assert.True(t, varNames["endpoint"], "endpoint variable should exist")
	assert.True(t, varNames["collection"], "collection variable should exist")
}

func TestSupportalExprGeneration(t *testing.T) {
	stats := getTestStats(t)
	config := supportalConfig

	var globalStatName, dbScopedStatName, collectionStatName string
	for name, stat := range stats {
		if len(stat.Labels) == 0 && globalStatName == "" {
			globalStatName = name
		}
		if hasLabel(stat.Labels, "database") && !hasLabel(stat.Labels, "collection") && !hasLabel(stat.Labels, "replication") && dbScopedStatName == "" {
			dbScopedStatName = name
		}
		if hasLabel(stat.Labels, "collection") && collectionStatName == "" {
			collectionStatName = name
		}
		if globalStatName != "" && dbScopedStatName != "" && collectionStatName != "" {
			break
		}
	}

	require.NotEmpty(t, globalStatName, "should find a global stat")
	require.NotEmpty(t, dbScopedStatName, "should find a database-scoped stat")
	require.NotEmpty(t, collectionStatName, "should find a collection-scoped stat")

	dashboard := generateGrafanaDashboard(stats, config)

	globalPanel := findChildPanel(dashboard, config.MetricPrefix+globalStatName)
	dbScopedPanel := findChildPanel(dashboard, config.MetricPrefix+dbScopedStatName)
	collectionPanel := findChildPanel(dashboard, config.MetricPrefix+collectionStatName)

	require.NotNil(t, globalPanel, "should find global stat panel")
	require.NotNil(t, dbScopedPanel, "should find database-scoped stat panel")
	require.NotNil(t, collectionPanel, "should find collection-scoped stat panel")

	// Verify global stat expression
	require.Len(t, globalPanel.Targets, 1)
	assert.Contains(t, globalPanel.Targets[0].Expr, `databaseUuid="$databaseUuid"`)
	assert.Contains(t, globalPanel.Targets[0].Expr, `nodeHostname=~"$nodeHostname"`)
	assert.NotContains(t, globalPanel.Targets[0].Expr, "endpoint")
	assert.NotContains(t, globalPanel.Targets[0].Expr, "collection")
	assert.Equal(t, "{{nodeHostname}}", globalPanel.Targets[0].LegendFormat)

	// Verify database-scoped stat expression
	require.Len(t, dbScopedPanel.Targets, 1)
	assert.Contains(t, dbScopedPanel.Targets[0].Expr, `databaseUuid="$databaseUuid"`)
	assert.Contains(t, dbScopedPanel.Targets[0].Expr, `database=~"$endpoint"`)
	assert.NotContains(t, dbScopedPanel.Targets[0].Expr, "collection")
	assert.Equal(t, "{{nodeHostname}} {{database}}", dbScopedPanel.Targets[0].LegendFormat)

	// Verify collection-scoped stat expression and legend
	require.Len(t, collectionPanel.Targets, 1)
	assert.Contains(t, collectionPanel.Targets[0].Expr, `database=~"$endpoint"`)
	assert.Contains(t, collectionPanel.Targets[0].Expr, `collection=~"$collection"`)
	assert.Contains(t, collectionPanel.Targets[0].LegendFormat, "{{collection}}")
	assert.Contains(t, collectionPanel.Targets[0].LegendFormat, "{{database}}")

	// Supportal should not use label_replace
	assert.NotContains(t, globalPanel.Targets[0].Expr, "label_replace")
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
	require.NotNil(t, panel.FieldConfig)
	assert.Equal(t, "bytes", panel.FieldConfig.Defaults.Unit)
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
		assert.Equal(t, 24, row.GridPos.W, "row should be full width")
		assert.Equal(t, 1, row.GridPos.H, "row should have height 1")
	}

	// Verify child panels have correct dimensions
	for _, row := range dashboard.Panels {
		for _, panel := range row.Panels {
			assert.Equal(t, 24, panel.GridPos.W, "child panel should be full width")
			assert.Equal(t, 8, panel.GridPos.H, "child panel should have height 8")
		}
	}
}
