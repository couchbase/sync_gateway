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

// Supportal Grafana datasource identifier. This UID must match the
// Prometheus/Mimir datasource provisioned in the Supportal Grafana instance
// for imported dashboards to bind correctly.
const supportalPromDatasourceUID = "mimir"

// supportalRestartMask suppresses the "Show Restarts" annotation for the first
// window of a node's uptime. Supportal scrape gaps are up to ~60s, so we use
// 2x headroom.
const supportalRestartMask = 2 * time.Minute

var supportalConfig = grafanaFormatConfig{
	metricPrefix:   "parsed_",
	dashboardUID:   "sync-gateway-all",
	dashboardTitle: "Sync Gateway All",
	datasourceType: "prometheus",
	datasourceUID:  supportalPromDatasourceUID,
	baseLegend:     "{{nodeHostname}}",
	baseSelector:   `databaseUuid="$databaseUuid",nodeHostname=~"$nodeHostname"`,
	labelSelectors: []labelSelector{
		{Label: "database", Selector: `,database=~"$endpoint"`, Legend: " {{database}}"},
		{Label: "collection", Selector: `,collection=~"$collection"`, Legend: " {{collection}}"},
		// No Selector: there is no $replication template variable on the
		// Supportal dashboard, so we only surface replication IDs in the
		// legend and do not attempt to filter by them.
		{Label: "replication", Legend: " {{replication}}"},
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
			Datasource(common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(supportalPromDatasourceUID)}).
			Enable(false).
			Hide(false).
			IconColor("#5794F2").
			Expr(fmt.Sprintf(`parsed_sgw_resource_utilization_uptime{databaseUuid="$databaseUuid",nodeHostname=~"$nodeHostname"} <= %d`, supportalRestartMask.Nanoseconds())),
	},
	templateVars: []cog.Builder[sdkdashboard.VariableModel]{
		sdkdashboard.NewQueryVariableBuilder("databaseUuid").
			Label("Cluster").
			Description("UUID of the cluster").
			Datasource(common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(supportalPromDatasourceUID)}).
			Definition("label_values(databaseUuid)").
			Query(varQueryPrometheus("label_values(databaseUuid)")).
			Refresh(sdkdashboard.VariableRefreshOnDashboardLoad),
		sdkdashboard.NewQueryVariableBuilder("nodeHostname").
			Label("SG Node").
			Description("SG node by hostname").
			Datasource(common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(supportalPromDatasourceUID)}).
			Definition(`label_values(parsed_sgw_resource_utilization_uptime{databaseUuid="$databaseUuid"},nodeHostname)`).
			Query(varQueryPrometheus(`label_values(parsed_sgw_resource_utilization_uptime{databaseUuid="$databaseUuid"},nodeHostname)`)).
			Current(selectAll()).
			IncludeAll(true).
			Multi(true).
			Refresh(sdkdashboard.VariableRefreshOnDashboardLoad),
		sdkdashboard.NewQueryVariableBuilder("endpoint").
			Datasource(common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(supportalPromDatasourceUID)}).
			Definition(`label_values(parsed_sgw_database_doc_writes_bytes{databaseUuid="$databaseUuid"},database)`).
			Query(varQueryPrometheus(`label_values(parsed_sgw_database_doc_writes_bytes{databaseUuid="$databaseUuid"},database)`)).
			Current(selectAll()).
			IncludeAll(true).
			Multi(true).
			Refresh(sdkdashboard.VariableRefreshOnDashboardLoad),
		sdkdashboard.NewQueryVariableBuilder("collection").
			Datasource(common.DataSourceRef{Type: ptr("prometheus"), Uid: ptr(supportalPromDatasourceUID)}).
			Definition(`label_values(parsed_sgw_collection_sync_function_count{databaseUuid="$databaseUuid"},collection)`).
			Query(varQueryPrometheus(`label_values(parsed_sgw_collection_sync_function_count{databaseUuid="$databaseUuid"},collection)`)).
			Current(selectAll()).
			IncludeAll(true).
			Multi(true).
			Refresh(sdkdashboard.VariableRefreshOnDashboardLoad),
	},
}
