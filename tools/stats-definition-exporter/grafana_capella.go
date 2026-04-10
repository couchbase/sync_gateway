// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included
// in the file licenses/APL2.txt.

package main

var capellaConfig = grafanaFormatConfig{
	MetricPrefix:   "",
	DashboardUID:   "sync-gateway-all-stats",
	DashboardTitle: "Sync Gateway All Stats",
	SchemaVersion:  38,
	DatasourceType: "prometheus",
	DatasourceUID:  "${DataSource}",
	BaseLegend:     "{{node}}",
	BaseSelector:   `databaseId="$databaseId",couchbaseNode=~"$couchbaseNode"`,
	LabelSelectors: []labelSelector{
		{Label: "database", Selector: `,database=~"$endpoint"`, Legend: " {{database}}"},
		{Label: "collection", Selector: `,collection=~"$collection"`, Legend: " {{collection}}"},
		{Label: "replication", Legend: " {{replication}}"},
	},
	LabelReplaces: []labelReplace{
		{
			DstLabel:    "node",
			Replacement: "$1",
			SrcLabel:    "couchbaseNode",
			Regex:       `([^.]+).*`,
		},
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
			Datasource:  datasourceRef{Type: "prometheus", UID: "${DataSource}"},
			Enable:      false,
			Hide:        false,
			IconColor:   "#5794F2",
			Name:        "Show Restarts",
			Expr:        `sgw_resource_utilization_uptime{databaseId=~"$databaseId",couchbaseNode=~"$couchbaseNode"} <= 600000000000`,
			TextFormat:  "SG Restart: {{couchbaseNode}}",
			TitleFormat: "SG Restart: {{couchbaseNode}}",
		},
	},
	templateVars: []templateVariable{
		{
			Current: currentValue{Text: "ThanosV2", Value: "P5766748FE00546FA"},
			Name:    "DataSource",
			Options: []variableOption{},
			Query:   "prometheus",
			Refresh: 1,
			Regex:   "(ThanosV2|Thanos)",
			Type:    "datasource",
		},
		{
			Datasource:  datasourceRef{Type: "prometheus", UID: "P5DCFC7561CCDE821"},
			Definition:  "label_values(sgw_up,databaseId)",
			Description: "UUID of the cluster",
			Label:       "Cluster",
			Name:        "databaseId",
			Options:     []variableOption{},
			Query:       variableQuery{Query: "label_values(sgw_up,databaseId)", RefID: "PrometheusVariableQueryEditor-variableQuery"},
			Refresh:     1,
			Type:        "query",
		},
		{
			Current:     currentValue{Text: "All", Value: "$__all"},
			Datasource:  datasourceRef{Type: "prometheus", UID: "${DataSource}"},
			Definition:  `label_values(sgw_resource_utilization_uptime{databaseId="$databaseId"},couchbaseNode)`,
			Description: "SG node by hostname",
			IncludeAll:  true,
			Label:       "SG Node",
			Multi:       true,
			Name:        "couchbaseNode",
			Options:     []variableOption{},
			Query:       variableQuery{Query: `label_values(sgw_resource_utilization_uptime{databaseId="$databaseId"},couchbaseNode)`, RefID: "PrometheusVariableQueryEditor-variableQuery"},
			Refresh:     1,
			Type:        "query",
		},
		{
			Current:    currentValue{Text: "All", Value: "$__all"},
			Datasource: datasourceRef{Type: "prometheus", UID: "${DataSource}"},
			Definition: `label_values(sgw_database_doc_writes_bytes{databaseId="$databaseId"},database)`,
			IncludeAll: true,
			Multi:      true,
			Name:       "endpoint",
			Options:    []variableOption{},
			Query:      variableQuery{Query: `label_values(sgw_database_doc_writes_bytes{databaseId="$databaseId"},database)`, RefID: "PrometheusVariableQueryEditor-variableQuery"},
			Refresh:    1,
			Type:       "query",
		},
		{
			Current:    currentValue{Text: "All", Value: "$__all"},
			Datasource: datasourceRef{Type: "prometheus", UID: "${DataSource}"},
			Definition: `label_values(sgw_collection_sync_function_count{databaseId="$databaseId"},collection)`,
			IncludeAll: true,
			Multi:      true,
			Name:       "collection",
			Options:    []variableOption{},
			Query:      variableQuery{Query: `label_values(sgw_collection_sync_function_count{databaseId="$databaseId"},collection)`, RefID: "PrometheusVariableQueryEditor-variableQuery"},
			Refresh:    1,
			Type:       "query",
		},
		{
			Name:       "syncgatewayId",
			Datasource: datasourceRef{Type: "prometheus", UID: "P5DCFC7561CCDE821"},
			Definition: `label_values(sgw_up{databaseId=~"$databaseId"},syncgatewayId)`,
			Hide:       2,
			Query:      variableQuery{Query: `label_values(sgw_up{databaseId=~"$databaseId"},syncgatewayId)`, RefID: "PrometheusVariableQueryEditor-variableQuery"},
			Refresh:    1,
			Type:       "query",
		},
	},
}
