// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const fleetManagerMetricsInterval = 1 * time.Hour // todo: CBG-5525 make this configurable

func (sc *ServerContext) reportFleetManagerMetrics(ctx context.Context) {
	hostname, err := os.Hostname()
	if err != nil {
		base.WarnfCtx(ctx, "Could not read hostname for fleet manager metrics: %v", err)
	}

	report := func() {
		metrics := base.CollectSGWFleetManagerMetrics(ctx, sc.NodeUID, hostname)
		if err := sc.sendFleetManagerMetrics(ctx, metrics); err != nil {
			base.WarnfCtx(ctx, "Could not report fleet manager metrics: %v", err)
		}
	}

	ticker := time.NewTicker(fleetManagerMetricsInterval)
	go func() {
		defer ticker.Stop()
		// Report once on startup rather than waiting a full interval for the first tick.
		report()
		for {
			select {
			case <-ticker.C:
				report()
			case <-ctx.Done():
				base.InfofCtx(ctx, base.KeyAll, "Stopping fleet manager metrics reporting: %v", context.Cause(ctx))
				return
			}
		}
	}()
	base.InfofCtx(ctx, base.KeyAll, "Starting fleet manager metrics reporting")
}

// sendFleetManagerMetrics POSTs the collected metrics to the ns_server fleet manager collector
// ingest endpoint on the Couchbase Server management API. A 404 means the connected server predates
// the collector endpoint (or it has been removed); this is treated as a benign skip so reporting
// starts automatically once the server is upgraded, rather than a hard error.
func (sc *ServerContext) sendFleetManagerMetrics(ctx context.Context, metrics base.SyncGatewayFleetManagerMetrics) error {
	endpoints, httpClient, err := sc.ObtainManagementEndpointsAndHTTPClient()
	if err != nil {
		return fmt.Errorf("could not obtain management endpoints: %w", err)
	}
	if len(endpoints) == 0 {
		return fmt.Errorf("no management endpoints available")
	}

	metricsJSON, err := base.JSONMarshal(metrics)
	if err != nil {
		return fmt.Errorf("could not marshal fleet manager metrics: %w", err)
	}

	uri := fmt.Sprintf("/_telemetryCollector/ingest?product_name=%s&instance_id=%s", base.ProductInfoName, url.QueryEscape(metrics.InstanceID))
	statusCode, _, err := doHTTPAuthRequest(ctx, httpClient, sc.Config.Bootstrap.Username, sc.Config.Bootstrap.Password, http.MethodPost, uri, "application/json", endpoints, metricsJSON)
	if err != nil {
		return err
	}
	switch statusCode {
	case http.StatusNoContent:
		// success
		return nil
	case http.StatusNotFound:
		// Server doesn't expose the collector endpoint (too old, or collector removed). Skip quietly
		// and retry on the next interval.
		base.DebugfCtx(ctx, base.KeyAll, "Fleet manager collector endpoint unavailable (status %d); will retry next interval", statusCode)
		return nil
	default:
		return fmt.Errorf("unexpected status %d from fleet manager collector", statusCode)
	}
}
