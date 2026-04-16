// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included
// in the file licenses/APL2.txt.

package main

import (
	"fmt"
	"time"

	"github.com/grafana/grafana-foundation-sdk/go/cog"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	sdkdashboard "github.com/grafana/grafana-foundation-sdk/go/dashboard"
)

// Capella Grafana datasource identifiers. These UIDs come from the Capella
// Grafana instance provisioning and must match those UIDs for generated
// dashboards to bind to the correct datasources at import time.
const (
	// capellaPrimaryDatasourceUID is the template-variable placeholder that
	// resolves to the user-selected Prometheus/Thanos datasource at view time.
	capellaPrimaryDatasourceUID = "${DataSource}"
	// capellaThanosV2UID is the literal UID of the ThanosV2 datasource; it is
	// the default value selected into the `DataSource` template variable.
	capellaThanosV2UID = "P5766748FE00546FA"
	// capellaGlobalPromUID is the literal UID of the global (cluster-discovery)
	// Prometheus datasource used to populate cluster-scoped template variables.
	capellaGlobalPromUID = "P5DCFC7561CCDE821"
)

// capellaRestartMask suppresses the "Show Restarts" annotation for the first
// window of a node's uptime. Capella's scrape can miss early samples after a
// node starts, so we allow a larger grace period than Supportal.
const capellaRestartMask = 10 * time.Minute

var capellaConfig = grafanaFormatConfig{
	metricPrefix:   "",
	dashboardUID:   "sync-gateway-all-stats",
	dashboardTitle: "Sync Gateway All Stats",
	schemaVersion:  38,
	datasourceType: "prometheus",
	datasourceUID:  capellaPrimaryDatasourceUID,
	baseLegend:     "{{node}}",
	baseSelector:   `databaseId="$databaseId",couchbaseNode=~"$couchbaseNode"`,
	labelSelectors: []labelSelector{
		{Label: "database", Selector: `,database=~"$endpoint"`, Legend: " {{database}}"},
		{Label: "collection", Selector: `,collection=~"$collection"`, Legend: " {{collection}}"},
		// No Selector: there is no $replication template variable on the
		// Capella dashboard, so we only surface replication IDs in the legend
		// and do not attempt to filter by them.
		{Label: "replication", Legend: " {{replication}}"},
	},
	labelReplaces: []labelReplace{
		{
			DstLabel:    "node",
			Replacement: "$1",
			SrcLabel:    "couchbaseNode",
			Regex:       `([^.]+).*`,
		},
	},
	annotations: []*sdkdashboard.AnnotationQueryBuilder{
		sdkdashboard.NewAnnotationQueryBuilder().
			Name("annotations & Alerts").
			Datasource(common.DataSourceRef{Type: ptr("grafana"), Uid: ptr("-- Grafana --")}).
			Enable(true).
			Hide(true).
			IconColor("rgba(0, 211, 255, 1)").
			BuiltIn(1).
			Type("dashboard"),
		sdkdashboard.NewAnnotationQueryBuilder().
			Name("Show Restarts").
			Datasource(common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(capellaPrimaryDatasourceUID)}).
			Enable(false).
			Hide(false).
			IconColor("#5794F2").
			Expr(fmt.Sprintf(`sgw_resource_utilization_uptime{databaseId=~"$databaseId",couchbaseNode=~"$couchbaseNode"} <= %d`, capellaRestartMask.Nanoseconds())),
	},
	templateVars: []cog.Builder[sdkdashboard.VariableModel]{
		sdkdashboard.NewDatasourceVariableBuilder("DataSource").
			Current(sdkdashboard.VariableOption{
				Text:  sdkdashboard.StringOrArrayOfString{String: ptr("ThanosV2")},
				Value: sdkdashboard.StringOrArrayOfString{String: ptr(capellaThanosV2UID)},
			}).
			Type("prometheus").
			Regex("(ThanosV2|Thanos)"),
		sdkdashboard.NewQueryVariableBuilder("databaseId").
			Label("Cluster").
			Description("UUID of the cluster").
			Datasource(common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(capellaGlobalPromUID)}).
			Definition("label_values(sgw_up,databaseId)").
			Query(varQueryPrometheus("label_values(sgw_up,databaseId)")).
			Refresh(sdkdashboard.VariableRefreshOnDashboardLoad),
		sdkdashboard.NewQueryVariableBuilder("couchbaseNode").
			Label("SG Node").
			Description("SG node by hostname").
			Datasource(common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(capellaPrimaryDatasourceUID)}).
			Definition(`label_values(sgw_resource_utilization_uptime{databaseId="$databaseId"},couchbaseNode)`).
			Query(varQueryPrometheus(`label_values(sgw_resource_utilization_uptime{databaseId="$databaseId"},couchbaseNode)`)).
			Current(selectAll()).
			IncludeAll(true).
			Multi(true).
			Refresh(sdkdashboard.VariableRefreshOnDashboardLoad),
		sdkdashboard.NewQueryVariableBuilder("endpoint").
			Datasource(common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(capellaPrimaryDatasourceUID)}).
			Definition(`label_values(sgw_database_doc_writes_bytes{databaseId="$databaseId"},database)`).
			Query(varQueryPrometheus(`label_values(sgw_database_doc_writes_bytes{databaseId="$databaseId"},database)`)).
			Current(selectAll()).
			IncludeAll(true).
			Multi(true).
			Refresh(sdkdashboard.VariableRefreshOnDashboardLoad),
		sdkdashboard.NewQueryVariableBuilder("collection").
			Datasource(common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(capellaPrimaryDatasourceUID)}).
			Definition(`label_values(sgw_collection_sync_function_count{databaseId="$databaseId"},collection)`).
			Query(varQueryPrometheus(`label_values(sgw_collection_sync_function_count{databaseId="$databaseId"},collection)`)).
			Current(selectAll()).
			IncludeAll(true).
			Multi(true).
			Refresh(sdkdashboard.VariableRefreshOnDashboardLoad),
		sdkdashboard.NewQueryVariableBuilder("syncgatewayId").
			Datasource(common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(capellaGlobalPromUID)}).
			Definition(`label_values(sgw_up{databaseId=~"$databaseId"},syncgatewayId)`).
			Query(varQueryPrometheus(`label_values(sgw_up{databaseId=~"$databaseId"},syncgatewayId)`)).
			Hide(sdkdashboard.VariableHideHideVariable).
			Refresh(sdkdashboard.VariableRefreshOnDashboardLoad),
	},
}
