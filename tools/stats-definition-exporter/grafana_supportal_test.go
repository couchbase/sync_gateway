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

func TestSupportalGrafanaDashboardGeneration(t *testing.T) {
	stats := getTestStats(t)
	config := supportalConfig

	dashboard := generateGrafanaDashboard(stats, config)

	// Verify dashboard metadata
	require.NotNil(t, dashboard.Uid)
	require.NotNil(t, dashboard.Title)
	require.NotNil(t, dashboard.Editable)
	assert.Equal(t, "sync-gateway-all", *dashboard.Uid)
	assert.Equal(t, "Sync Gateway All", *dashboard.Title)
	assert.Equal(t, uint16(42), dashboard.SchemaVersion)
	assert.Equal(t, []string{"Sync Gateway"}, dashboard.Tags)
	assert.True(t, *dashboard.Editable)

	// Top-level panels should all be row panels
	for _, p := range dashboard.Panels {
		require.NotNil(t, p.RowPanel, "top-level panel should be a row panel")
		assert.Equal(t, "row", p.RowPanel.Type)
		assert.True(t, p.RowPanel.Collapsed)
	}

	// Count total child panels across all rows -- should match stat count
	totalChildPanels := 0
	for _, p := range dashboard.Panels {
		totalChildPanels += len(p.RowPanel.Panels)
	}
	assert.Equal(t, len(stats), totalChildPanels)

	// Row titles should be sorted alphabetically
	for i := 1; i < len(dashboard.Panels); i++ {
		prev := dashboard.Panels[i-1].RowPanel.Title
		curr := dashboard.Panels[i].RowPanel.Title
		require.NotNil(t, prev)
		require.NotNil(t, curr)
		assert.LessOrEqual(t, *prev, *curr)
	}

	// Verify child panels within each row are alphabetically ordered and are timeseries
	for _, row := range dashboard.Panels {
		children := row.RowPanel.Panels
		for j := 1; j < len(children); j++ {
			prev := children[j-1].Title
			curr := children[j].Title
			require.NotNil(t, prev)
			require.NotNil(t, curr)
			assert.LessOrEqual(t, *prev, *curr)
		}
		for _, panel := range children {
			assert.Equal(t, "timeseries", panel.Type)
			require.NotNil(t, panel.Datasource)
			require.NotNil(t, panel.Datasource.Type)
			require.NotNil(t, panel.Datasource.Uid)
			assert.Equal(t, "prometheus", *panel.Datasource.Type)
			assert.Equal(t, "mimir", *panel.Datasource.Uid)
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

	globalPanel := findChildPanel(dashboard, config.metricPrefix+globalStatName)
	dbScopedPanel := findChildPanel(dashboard, config.metricPrefix+dbScopedStatName)
	collectionPanel := findChildPanel(dashboard, config.metricPrefix+collectionStatName)

	require.NotNil(t, globalPanel, "should find global stat panel")
	require.NotNil(t, dbScopedPanel, "should find database-scoped stat panel")
	require.NotNil(t, collectionPanel, "should find collection-scoped stat panel")

	// Verify global stat expression. Check for `$endpoint`/`$collection` rather than
	// the bare label names, since a metric name itself can contain those words
	// (e.g. `database_config_collection_conflicts`).
	require.Len(t, globalPanel.Targets, 1)
	globalQ := promQuery(t, globalPanel.Targets[0])
	assert.Contains(t, globalQ.Expr, `databaseUuid="$databaseUuid"`)
	assert.Contains(t, globalQ.Expr, `nodeHostname=~"$nodeHostname"`)
	assert.NotContains(t, globalQ.Expr, "$endpoint")
	assert.NotContains(t, globalQ.Expr, "$collection")
	require.NotNil(t, globalQ.LegendFormat)
	assert.Equal(t, "{{nodeHostname}}", *globalQ.LegendFormat)

	// Verify database-scoped stat expression
	require.Len(t, dbScopedPanel.Targets, 1)
	dbQ := promQuery(t, dbScopedPanel.Targets[0])
	assert.Contains(t, dbQ.Expr, `databaseUuid="$databaseUuid"`)
	assert.Contains(t, dbQ.Expr, `database=~"$endpoint"`)
	assert.NotContains(t, dbQ.Expr, "$collection")
	require.NotNil(t, dbQ.LegendFormat)
	assert.Equal(t, "{{nodeHostname}} {{database}}", *dbQ.LegendFormat)

	// Verify collection-scoped stat expression and legend
	require.Len(t, collectionPanel.Targets, 1)
	collQ := promQuery(t, collectionPanel.Targets[0])
	assert.Contains(t, collQ.Expr, `database=~"$endpoint"`)
	assert.Contains(t, collQ.Expr, `collection=~"$collection"`)
	require.NotNil(t, collQ.LegendFormat)
	assert.Contains(t, *collQ.LegendFormat, "{{collection}}")
	assert.Contains(t, *collQ.LegendFormat, "{{database}}")

	// Supportal should not use label_replace
	assert.NotContains(t, globalQ.Expr, "label_replace")
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

	data, err := os.ReadFile(outputFile)
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

	panel := findChildPanel(dashboard, supportalConfig.metricPrefix+bytesStatName)
	require.NotNil(t, panel, "should find panel for bytes stat")
	require.NotNil(t, panel.FieldConfig)
	require.NotNil(t, panel.FieldConfig.Defaults.Unit)
	assert.Equal(t, "bytes", *panel.FieldConfig.Defaults.Unit)
}

func TestDescriptionFromHelp(t *testing.T) {
	stats := getTestStats(t)

	// Find a stat with help text and verify the panel description starts with it.
	// The exporter appends a "\n---\nSGW X.Y.Z+" version suffix to panel
	// descriptions, so exact equality no longer holds.
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

	panel := findChildPanel(dashboard, supportalConfig.metricPrefix+statWithHelp)
	require.NotNil(t, panel, "should find panel with help text")
	require.NotNil(t, panel.Description)
	assert.Contains(t, *panel.Description, helpText)
	assert.Contains(t, *panel.Description, "\n---\nSGW ")
}

func TestRowPanelStructure(t *testing.T) {
	stats := getTestStats(t)
	dashboard := generateGrafanaDashboard(stats, supportalConfig)

	// Verify all top-level panels are collapsed rows
	require.NotEmpty(t, dashboard.Panels)
	for _, p := range dashboard.Panels {
		require.NotNil(t, p.RowPanel, "top-level panel should be a row")
		row := p.RowPanel
		assert.Equal(t, "row", row.Type, "top-level panel should be a row")
		assert.True(t, row.Collapsed, "row should be collapsed")
		rowTitle := ""
		if row.Title != nil {
			rowTitle = *row.Title
		}
		assert.NotEmpty(t, row.Panels, "row should contain child panels for %s", rowTitle)
		require.NotNil(t, row.GridPos)
		assert.Equal(t, uint32(24), row.GridPos.W, "row should be full width")
		assert.Equal(t, uint32(1), row.GridPos.H, "row should have height 1")
	}

	// Verify child panels have correct dimensions
	for _, p := range dashboard.Panels {
		for _, panel := range p.RowPanel.Panels {
			require.NotNil(t, panel.GridPos)
			assert.Equal(t, uint32(24), panel.GridPos.W, "child panel should be full width")
			assert.Equal(t, uint32(8), panel.GridPos.H, "child panel should have height 8")
		}
	}
}
