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

	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapellaGrafanaDashboardGeneration(t *testing.T) {
	stats := getTestStats(t)
	config := capellaConfig

	dashboard := generateGrafanaDashboard(stats, config)

	// Verify dashboard metadata
	require.NotNil(t, dashboard.Uid)
	require.NotNil(t, dashboard.Title)
	assert.Equal(t, "sync-gateway-all-stats", *dashboard.Uid)
	assert.Equal(t, "Sync Gateway All Stats", *dashboard.Title)
	assert.Equal(t, uint16(38), dashboard.SchemaVersion)

	// Top-level panels should all be row panels
	for _, p := range dashboard.Panels {
		require.NotNil(t, p.RowPanel)
		assert.Equal(t, "row", p.RowPanel.Type)
	}

	// Count total child panels
	totalChildPanels := 0
	for _, p := range dashboard.Panels {
		totalChildPanels += len(p.RowPanel.Panels)
	}
	assert.Equal(t, len(stats), totalChildPanels)

	// Verify child panels use correct datasource
	for _, row := range dashboard.Panels {
		for _, panel := range row.RowPanel.Panels {
			assert.Equal(t, "timeseries", panel.Type)
			require.NotNil(t, panel.Datasource)
			require.NotNil(t, panel.Datasource.Type)
			require.NotNil(t, panel.Datasource.Uid)
			assert.Equal(t, "prometheus", *panel.Datasource.Type)
			assert.Equal(t, "${DataSource}", *panel.Datasource.Uid)
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
	globalQ := promQuery(t, globalPanel.Targets[0])
	assert.Contains(t, globalQ.Expr, "label_replace(")
	assert.Contains(t, globalQ.Expr, "couchbaseNode")
	assert.Contains(t, globalQ.Expr, `databaseId="$databaseId"`)
	assert.NotContains(t, globalQ.Expr, "nodeHostname")

	// Verify global stat legend uses short node name
	require.NotNil(t, globalQ.LegendFormat)
	assert.Equal(t, "{{node}}", *globalQ.LegendFormat)

	// Verify database-scoped stat
	require.Len(t, dbScopedPanel.Targets, 1)
	dbQ := promQuery(t, dbScopedPanel.Targets[0])
	assert.Contains(t, dbQ.Expr, `database=~"$endpoint"`)
	require.NotNil(t, dbQ.LegendFormat)
	assert.Equal(t, "{{node}} {{database}}", *dbQ.LegendFormat)

	// Verify collection-scoped stat
	require.Len(t, collectionPanel.Targets, 1)
	collQ := promQuery(t, collectionPanel.Targets[0])
	assert.Contains(t, collQ.Expr, `collection=~"$collection"`)
	require.NotNil(t, collQ.LegendFormat)
	assert.Contains(t, *collQ.LegendFormat, "{{collection}}")
	assert.Contains(t, *collQ.LegendFormat, "{{database}}")
}

// promQuery extracts the prometheus.Dataquery from a panel target for assertion.
func promQuery(t *testing.T, target interface{}) prometheus.Dataquery {
	t.Helper()
	q, ok := target.(prometheus.Dataquery)
	require.True(t, ok, "panel target is not a prometheus.Dataquery: %T", target)
	return q
}

func TestGrafanaFormatStdOutput(t *testing.T) {
	var buf bytes.Buffer
	logger := log.New(&buf, "", 0)

	err := statsToFile(logger, nil, formatCapellaGrafana)
	assert.NoError(t, err)
	assert.Empty(t, buf)
}
