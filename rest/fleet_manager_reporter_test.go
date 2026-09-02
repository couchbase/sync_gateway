/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
)

func TestSendingMetricsToNsServerCollector(t *testing.T) {
	if !sgtest.TestUseCouchbaseServer() {
		t.Skip("Fleet Manager Collector only works on CBS")
	}
	base.RequireAtLeastServerVersionForTest(t, "7.6.12", "8.0.3")
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()
	ctx := rt.Context()
	bucket := rt.Bucket()

	gocbV2Bucket, err := base.AsGocbV2Bucket(bucket)
	require.NoError(t, err)

	uri := base.TelemetrySettingsEndpoint
	respBytes, _, err := gocbV2Bucket.MgmtRequest(ctx, http.MethodGet, uri, "application/json", nil)
	require.NoError(t, err)
	var settings base.FleetManagerCollectorSettings
	require.NoError(t, base.JSONUnmarshal(respBytes, &settings))
	assert.True(t, settings.Enabled) // enabled by default
	assert.NotEmpty(t, settings.ReportingInterval)

	// Exercise the production send path rather than re-implementing the POST here.
	metrics := base.CollectSGWFleetManagerMetrics(ctx, "someNodeID", "myHost")
	require.NoError(t, rt.ServerContext().sendFleetManagerMetrics(ctx, metrics, rt.ServerContext().Config.Bootstrap.Username, rt.ServerContext().Config.Bootstrap.Password))
}

// TestSendingMetricsToNsServerCollectorAsMobileSyncGatewayRole verifies that a user holding only the
// role Sync Gateway is bootstrapped with (mobile_sync_gateway on Enterprise, bucket_full_access on
// Community) is authorized to POST to the ns_server collector endpoint. Operators are expected to
// bootstrap Sync Gateway with such a user, so we point the server's bootstrap credentials at a
// freshly-created user with that role and run the production send path as it.
func TestSendingMetricsToNsServerCollectorAsMobileSyncGatewayRole(t *testing.T) {
	if !sgtest.TestUseCouchbaseServer() {
		t.Skip("Fleet Manager Collector only works on CBS")
	}
	base.RequireAtLeastServerVersionForTest(t, "7.6.12", "8.0.3")
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()
	ctx := rt.Context()
	sc := rt.ServerContext()

	eps, httpClient, err := sc.getManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	roleName := MobileSyncGatewayRole.RoleName
	if base.TestsUseServerCE() {
		roleName = BucketFullAccessRole.RoleName
	}

	const username, password = "MobileSyncGatewayUser", "password"
	role := fmt.Sprintf("%s[%s]", roleName, rt.Bucket().GetName())
	base.MakeUser(t, httpClient, eps[0], username, password, []string{role})
	defer base.DeleteUser(t, httpClient, eps[0], username)

	// Redirect the send path's authentication to the scoped user for the test;
	// sendFleetManagerMetrics reads these credentials at call time.
	metrics := base.CollectSGWFleetManagerMetrics(ctx, "someNodeID", "myHost")
	require.NoError(t, sc.sendFleetManagerMetrics(ctx, metrics, username, password))
}

func TestSendingMetricsWhenCollectorDisabled(t *testing.T) {
	if !sgtest.TestUseCouchbaseServer() {
		t.Skip("Fleet Manager Collector only works on CBS")
	}
	base.RequireAtLeastServerVersionForTest(t, "7.6.12", "8.0.3")

	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()
	ctx := rt.Context()
	bucket := rt.Bucket()

	gocbV2Bucket, err := base.AsGocbV2Bucket(bucket)
	require.NoError(t, err)

	uri := base.TelemetrySettingsEndpoint
	disableBody := []byte(`{"enabled": false}`)
	respBytes, statusCode, err := gocbV2Bucket.MgmtRequest(ctx, http.MethodPost, uri, "application/json", bytes.NewReader(disableBody))
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, statusCode, "unexpected response: %s code: %d", string(respBytes), statusCode)

	// Re-enable the cluster-wide collector via defer so we don't leak the disabled state to other
	// tests on the shared cluster if an assertion below fails before we get to flip it back.
	defer func() {
		enableBody := []byte(`{"enabled": true}`)
		respBytes, statusCode, err := gocbV2Bucket.MgmtRequest(ctx, http.MethodPost, uri, "application/json", bytes.NewReader(enableBody))
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, statusCode)

		// verify enabled is true now
		settings := base.FleetManagerCollectorSettings{}
		require.NoError(t, base.JSONUnmarshal(respBytes, &settings))
		assert.True(t, settings.Enabled)
	}()

	var settings base.FleetManagerCollectorSettings
	require.NoError(t, base.JSONUnmarshal(respBytes, &settings))
	assert.False(t, settings.Enabled)

	metrics := base.CollectSGWFleetManagerMetrics(ctx, "someNodeID", "myHost")
	require.NoError(t, rt.ServerContext().sendFleetManagerMetrics(ctx, metrics, rt.ServerContext().Config.Bootstrap.Username, rt.ServerContext().Config.Bootstrap.Password))
}

func TestNoContentResponseForCollector(t *testing.T) {
	if !sgtest.TestUseCouchbaseServer() {
		t.Skip("Fleet Manager Collector only works on CBS")
	}
	base.RequireAtLeastServerVersionForTest(t, "7.6.12", "8.0.3")
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	gocbV2Bucket, err := base.AsGocbV2Bucket(rt.Bucket())
	require.NoError(t, err)

	metrics := base.CollectSGWFleetManagerMetrics(base.TestCtx(t), "someNodeID", "myHost")
	uri := base.TelemetryIngestURI("someID")
	metricsBytes, err := base.JSONMarshal(metrics)
	require.NoError(t, err)
	respBytes, statusCode, err := gocbV2Bucket.MgmtRequest(rt.Context(), http.MethodPost, uri, "application/json", bytes.NewReader(metricsBytes))
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, statusCode)
	assert.Empty(t, respBytes)
}

// TestGetCollectorSettings exercises the production getCollectorSettings path: it should return the
// server's enabled flag and reporting interval, reflecting an operator toggling the collector off.
func TestGetCollectorSettings(t *testing.T) {
	if !sgtest.TestUseCouchbaseServer() {
		t.Skip("Fleet Manager Collector only works on CBS")
	}
	base.RequireAtLeastServerVersionForTest(t, "7.6.12", "8.0.3")
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()
	ctx := rt.Context()
	sc := rt.ServerContext()

	// Collector is enabled by default, so a fresh read should report enabled with a positive interval.
	settings, err := sc.getCollectorSettings(ctx)
	require.NoError(t, err)
	assert.True(t, settings.Enabled, "collector should be enabled by default")
	assert.NotEmpty(t, settings.ReportingInterval)
	assert.NotZero(t, settings.Interval())

	gocbV2Bucket, err := base.AsGocbV2Bucket(rt.Bucket())
	require.NoError(t, err)
	uri := base.TelemetrySettingsEndpoint

	// Disable the cluster-wide collector and confirm getCollectorSettings reflects it. Re-enable via
	// defer so the disabled state doesn't leak to other tests on the shared cluster if an assertion
	// below fails first.
	_, statusCode, err := gocbV2Bucket.MgmtRequest(ctx, http.MethodPost, uri, "application/json", bytes.NewReader([]byte(`{"enabled": false}`)))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, statusCode)
	defer func() {
		_, statusCode, err := gocbV2Bucket.MgmtRequest(ctx, http.MethodPost, uri, "application/json", bytes.NewReader([]byte(`{"enabled": true}`)))
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, statusCode)
	}()

	settings, err = sc.getCollectorSettings(ctx)
	require.NoError(t, err)
	assert.False(t, settings.Enabled, "collector should report as disabled after being disabled")
}

func TestAllFleetManagerMetricsPopulated(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()
	ctx := rt.Context()

	metrics := base.CollectSGWFleetManagerMetrics(ctx, "someNodeID", "myHost")
	assert.NotEmpty(t, metrics.InstanceID)
	assert.NotZero(t, metrics.CpuCores)
	// RAM values are sampled directly at collection time, so they must be populated even before the
	// stats-logger ticker has run (a "0" string passes NotEmpty, which is why we check for non-zero).
	assert.NotEqual(t, "0", metrics.RamBytesTotal)
	assert.NotEqual(t, "0", metrics.RamBytesUsed)
	assert.NotEmpty(t, metrics.OSVersion)
	assert.NotEmpty(t, metrics.Hostname)
	assert.GreaterOrEqual(t, metrics.UptimeSeconds, 0)
	assert.NotEmpty(t, metrics.ProductInfo.Edition)
	assert.NotEmpty(t, metrics.ProductInfo.Version)
	assert.NotEmpty(t, metrics.ProductInfo.Name)
}

func TestFleetManagerReporterLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()

		var mutex sync.Mutex
		var reports []base.FleetManagerCollectorSettings
		current := base.FleetManagerCollectorSettings{Enabled: true, ReportingInterval: 1}
		getCollectorFunc := func() base.FleetManagerCollectorSettings {
			mutex.Lock()
			defer mutex.Unlock()
			return current
		}
		reportFunc := func(settings base.FleetManagerCollectorSettings) {
			mutex.Lock()
			defer mutex.Unlock()
			if settings.Enabled { // mirror our report closure in production code
				reports = append(reports, settings)
			}
		}
		reportCount := func() int {
			mutex.Lock()
			defer mutex.Unlock()
			return len(reports)
		}

		go runFleetManagerReportLoop(ctx, getCollectorFunc, reportFunc)

		synctest.Wait() // wait until first report runs and is blocking on the ticker interval
		require.Equal(t, 1, reportCount())

		time.Sleep(1 * time.Hour) // mock 1 hour passing, should trigger new report on ticker interval
		synctest.Wait()           // wait for report goroutine to block again
		require.Equal(t, 2, reportCount())

		mutex.Lock()
		current.Enabled = false
		mutex.Unlock()
		time.Sleep(1 * time.Hour)          // mock another hour passing
		synctest.Wait()                    // wait for report goroutine to block again after another report interval
		require.Equal(t, 2, reportCount()) // tick has fired above but enabled is false so no report should fire

		// update interval to two hours
		mutex.Lock()
		current.ReportingInterval = 2
		current.Enabled = true
		mutex.Unlock()
		time.Sleep(1 * time.Hour)
		synctest.Wait() // first tick after update uses the previous 1h ticker interval
		require.Equal(t, 3, reportCount())

		time.Sleep(1 * time.Hour)
		synctest.Wait() // ticker should have been reset to 2h on the prior tick
		require.Equal(t, 3, reportCount())

		// assert reports only contain entries of enabled=true
		mutex.Lock()
		for _, report := range reports {
			assert.True(t, report.Enabled)
		}
		mutex.Unlock()
	})
}
