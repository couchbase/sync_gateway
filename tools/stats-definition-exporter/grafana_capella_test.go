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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	for _, v := range dashboard.Templating.List {
		varNames[v.Name] = true
	}
	assert.True(t, varNames["databaseId"], "databaseId variable should exist")
	assert.True(t, varNames["endpoint"], "endpoint variable should exist")
	assert.True(t, varNames["collection"], "collection variable should exist")
	assert.True(t, varNames["DataSource"], "DataSource variable should exist")
	assert.True(t, varNames["syncgatewayId"], "syncgatewayId hidden variable should exist")
}

func TestCapellaExprGeneration(t *testing.T) {
	stats := getTestStats(t)
	config := capellaConfig

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

	globalPanel := findChildPanel(dashboard, globalStatName)
	dbScopedPanel := findChildPanel(dashboard, dbScopedStatName)
	collectionPanel := findChildPanel(dashboard, collectionStatName)

	require.NotNil(t, globalPanel, "should find global stat panel")
	require.NotNil(t, dbScopedPanel, "should find database-scoped stat panel")
	require.NotNil(t, collectionPanel, "should find collection-scoped stat panel")

	// All Capella expressions should be wrapped in label_replace for node short hostname
	require.Len(t, globalPanel.Targets, 1)
	assert.Contains(t, globalPanel.Targets[0].Expr, "label_replace(")
	assert.Contains(t, globalPanel.Targets[0].Expr, "couchbaseNode")
	assert.Contains(t, globalPanel.Targets[0].Expr, `databaseId="$databaseId"`)
	assert.NotContains(t, globalPanel.Targets[0].Expr, "nodeHostname")

	// Verify global stat legend uses short node name
	assert.Equal(t, "{{node}}", globalPanel.Targets[0].LegendFormat)

	// Verify database-scoped stat
	require.Len(t, dbScopedPanel.Targets, 1)
	assert.Contains(t, dbScopedPanel.Targets[0].Expr, `database=~"$endpoint"`)
	assert.Equal(t, "{{node}} {{database}}", dbScopedPanel.Targets[0].LegendFormat)

	// Verify collection-scoped stat
	require.Len(t, collectionPanel.Targets, 1)
	assert.Contains(t, collectionPanel.Targets[0].Expr, `collection=~"$collection"`)
	assert.Contains(t, collectionPanel.Targets[0].LegendFormat, "{{collection}}")
	assert.Contains(t, collectionPanel.Targets[0].LegendFormat, "{{database}}")
}

func TestGrafanaFormatStdOutput(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	err := statsToFile(logger, nil, formatCapellaGrafana)
	assert.NoError(t, err)
	assert.Empty(t, buf)
}
