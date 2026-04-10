// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included
// in the file licenses/APL2.txt.

package main

var supportalConfig = grafanaFormatConfig{
	MetricPrefix:   "parsed_",
	DashboardUID:   "sync-gateway-all",
	DashboardTitle: "Sync Gateway All",
	SchemaVersion:  42,
	PluginVersion:  "12.4.2",
	DatasourceType: "prometheus",
	DatasourceUID:  "mimir",
	BaseLegend:     "{{nodeHostname}}",
	BaseSelector:   `databaseUuid="$databaseUuid",nodeHostname=~"$nodeHostname"`,
	LabelSelectors: []labelSelector{
		{Label: "database", Selector: `,database=~"$endpoint"`, Legend: " {{database}}"},
		{Label: "collection", Selector: `,collection=~"$collection"`, Legend: " {{collection}}"},
		{Label: "replication", Legend: " {{replication}}"},
	},
	annotations: []annotation{
		{
			BuiltIn:    1,
			Datasource: datasourceRef{Type: "grafana", UID: "-- Grafana --"},
			Enable:     true,
			Hide:       true,
			IconColor:  "rgba(0, 211, 255, 1)",
			Name:       "annotations & Alerts",
			Type:       "dashboard",
		},
		{
			Datasource:  datasourceRef{Type: "prometheus", UID: "mimir"},
			Enable:      false,
			Hide:        false,
			IconColor:   "#5794F2",
			Name:        "Show Restarts",
			Expr:        `parsed_sgw_resource_utilization_uptime{databaseUuid="$databaseUuid",nodeHostname=~"$nodeHostname"} <= 1200000000000`,
			TextFormat:  "SG Restart: {{nodeHostname}}",
			TitleFormat: "SG Restart: {{nodeHostname}}",
		},
	},
	templateVars: []templateVariable{
		{
			Datasource:  datasourceRef{Type: "prometheus", UID: "mimir"},
			Definition:  "label_values(databaseUuid)",
			Description: "UUID of the cluster",
			Label:       "Cluster",
			Name:        "databaseUuid",
			Options:     []variableOption{},
			Query:       variableQuery{Query: "label_values(databaseUuid)", RefID: "PrometheusVariableQueryEditor-variableQuery"},
			Refresh:     1,
			Type:        "query",
		},
		{
			Current:     currentValue{Text: "All", Value: "$__all"},
			Datasource:  datasourceRef{Type: "prometheus", UID: "mimir"},
			Definition:  `label_values(parsed_sgw_resource_utilization_uptime{databaseUuid="$databaseUuid"},nodeHostname)`,
			Description: "SG node by hostname",
			IncludeAll:  true,
			Label:       "SG Node",
			Multi:       true,
			Name:        "nodeHostname",
			Options:     []variableOption{},
			Query:       variableQuery{Query: `label_values(parsed_sgw_resource_utilization_uptime{databaseUuid="$databaseUuid"},nodeHostname)`, RefID: "PrometheusVariableQueryEditor-variableQuery"},
			Refresh:     1,
			Type:        "query",
		},
		{
			Current:    currentValue{Text: "All", Value: "$__all"},
			Datasource: datasourceRef{Type: "prometheus", UID: "mimir"},
			Definition: `label_values(parsed_sgw_database_doc_writes_bytes{databaseUuid="$databaseUuid"},database)`,
			IncludeAll: true,
			Multi:      true,
			Name:       "endpoint",
			Options:    []variableOption{},
			Query:      variableQuery{Query: `label_values(parsed_sgw_database_doc_writes_bytes{databaseUuid="$databaseUuid"},database)`, RefID: "PrometheusVariableQueryEditor-variableQuery"},
			Refresh:    1,
			Type:       "query",
		},
		{
			Current:    currentValue{Text: "All", Value: "$__all"},
			Datasource: datasourceRef{Type: "prometheus", UID: "mimir"},
			Definition: `label_values(parsed_sgw_collection_sync_function_count{databaseUuid="$databaseUuid"},collection)`,
			IncludeAll: true,
			Multi:      true,
			Name:       "collection",
			Options:    []variableOption{},
			Query:      variableQuery{Query: `label_values(parsed_sgw_collection_sync_function_count{databaseUuid="$databaseUuid"},collection)`, RefID: "PrometheusVariableQueryEditor-variableQuery"},
			Refresh:    1,
			Type:       "query",
		},
	},
}
