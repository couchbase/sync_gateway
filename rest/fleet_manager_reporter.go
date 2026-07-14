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
	"os"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// defaultFleetManagerCollectorSettings is used when the collector settings can't be read due to a
// transient failure (e.g. a network blip). Reporting stays on so we don't drop metrics over a read
// that is likely to recover, and Interval() falls back to the default reporting interval. Note a 404
// is handled separately in getCollectorSettings - it means the server predates the collector, so
// there is nothing to report to.
var defaultFleetManagerCollectorSettings = base.FleetManagerCollectorSettings{Enabled: true}

func (sc *ServerContext) reportFleetManagerMetrics(ctx context.Context) {
	hostname, err := os.Hostname()
	if err != nil {
		base.WarnfCtx(ctx, "Could not read hostname for fleet manager metrics: %v", err)
	}

	report := func(settings base.FleetManagerCollectorSettings) {
		if !settings.Enabled {
			return
		}
		metrics := base.CollectSGWFleetManagerMetrics(ctx, sc.NodeUID, hostname)
		if err := sc.sendFleetManagerMetrics(ctx, metrics); err != nil {
			base.WarnfCtx(ctx, "Could not report fleet manager metrics: %v", err)
		}
	}

	// currentSettings reads the latest collector settings, falling back to defaults (reporting on)
	// on a transient read failure so we don't drop metrics. Settings are re-read each tick so
	// operator changes to the interval or enabled flag are picked up over the lifetime of the node.
	currentSettings := func() base.FleetManagerCollectorSettings {
		settings, err := sc.getCollectorSettings(ctx)
		if err != nil {
			base.WarnfCtx(ctx, "Could not read fleet manager collector settings, using defaults: %v", err)
			return defaultFleetManagerCollectorSettings
		}
		return settings
	}

	go func() {
		settings := currentSettings()
		ticker := time.NewTicker(settings.Interval())
		defer ticker.Stop()
		base.InfofCtx(ctx, base.KeyAll, "Fleet manager metrics reporting interval set to %s (enabled=%t)", settings.Interval(), settings.Enabled)
		// Report once on startup rather than waiting a full interval for the first tick.
		report(settings)
		for {
			select {
			case <-ticker.C:
				refreshed := currentSettings()
				if refreshed.Enabled != settings.Enabled {
					base.InfofCtx(ctx, base.KeyAll, "Fleet manager metrics reporting enabled changed to %t", refreshed.Enabled)
				}
				if refreshed.Interval() != settings.Interval() {
					base.InfofCtx(ctx, base.KeyAll, "Fleet manager metrics reporting interval changed from %s to %s", settings.Interval(), refreshed.Interval())
					ticker.Reset(refreshed.Interval())
				}
				settings = refreshed
				report(settings)
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

	uri := base.TelemetryIngestURI(metrics.InstanceID)
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

// getCollectorSettings grabs the configured settings for the fleet manager collector on Couchbase
// Server. A 404 means the server predates the collector (the settings and ingest endpoints ship
// together), so it is reported as disabled rather than an error - there is nothing to report to.
func (sc *ServerContext) getCollectorSettings(ctx context.Context) (base.FleetManagerCollectorSettings, error) {
	endpoints, httpClient, err := sc.ObtainManagementEndpointsAndHTTPClient()
	if err != nil {
		return base.FleetManagerCollectorSettings{}, fmt.Errorf("could not obtain management endpoints: %w", err)
	}
	if len(endpoints) == 0 {
		return base.FleetManagerCollectorSettings{}, fmt.Errorf("no management endpoints available")
	}

	uri := base.TelemetrySettingsEndpoint
	statusCode, body, err := doHTTPAuthRequest(ctx, httpClient, sc.Config.Bootstrap.Username, sc.Config.Bootstrap.Password, http.MethodGet, uri, "", endpoints, nil)
	if err != nil {
		return base.FleetManagerCollectorSettings{}, err
	}
	switch statusCode {
	case http.StatusOK:
		var settings base.FleetManagerCollectorSettings
		if err := base.JSONUnmarshal(body, &settings); err != nil {
			return base.FleetManagerCollectorSettings{}, fmt.Errorf("could not unmarshal collector settings: %w", err)
		}
		return settings, nil
	case http.StatusNotFound:
		// Server predates the collector (too old, or collector removed); treat as disabled. Settings
		// are re-read each tick, so reporting starts automatically if the server is later upgraded.
		base.DebugfCtx(ctx, base.KeyAll, "Fleet manager collector settings endpoint unavailable (status %d); treating collector as disabled", statusCode)
		return base.FleetManagerCollectorSettings{}, nil
	default:
		return base.FleetManagerCollectorSettings{}, fmt.Errorf("unexpected status %d from fleet manager collector settings", statusCode)
	}
}
