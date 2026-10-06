// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package importtest

import (
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestCloseDatabaseWithoutImportDCPAccess makes sure that a database closes promptly while its import feeds fail to
// restart, because the bucket user lost DCP access.
func TestCloseDatabaseWithoutImportDCPAccess(t *testing.T) {
	base.TestRequiresCbgt(t)
	if !base.IsEnterpriseEdition() {
		t.Skip("Sharded import requires EE")
	}

	const username = "importFeedUser"
	const password = "password"
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		MutateStartupConfig: func(sc *rest.StartupConfig) {
			sc.DatabaseCredentials = rest.PerDatabaseCredentialsConfig{
				"db": {Username: username, Password: password},
			}
		},
	})
	defer rt.Close()

	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)
	bucketName := rt.Bucket().GetName()
	base.MakeUser(t, httpClient, eps[0], username, password, []string{fmt.Sprintf("%s[%s]", rest.MobileSyncGatewayRole.RoleName, bucketName)})
	defer base.DeleteUser(t, httpClient, eps[0], username)

	dbConfig := rt.NewDbConfig()
	dbConfig.ImportPartitions = new(uint16(2))
	rest.RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)
	dbCtx := rt.GetDatabase()

	mgr := dbCtx.ImportCbgtManager(t)
	queuedKicks := func() uint64 {
		var stats cbgt.ManagerStats
		mgr.StatsCopyTo(&stats)
		return stats.TotJanitorKick - stats.TotJanitorKickStart
	}

	// The attachment migration DCP feed can't stop without DCP access, so let it finish first.
	db.RequireBackgroundManagerState(t, dbCtx.AttachmentMigrationManager, db.BackgroundProcessStateCompleted)

	// Removing DCP access drops the feeds' connections. They then fail to restart, but the cbgt cfg in the bucket stays
	// readable, so each failed restart queues two more janitor kicks.
	base.MakeUser(t, httpClient, eps[0], username, password, []string{
		fmt.Sprintf("data_reader[%s]", bucketName),
		fmt.Sprintf("data_writer[%s]", bucketName),
	})
	// ClosePIndex waits behind the queued kicks. Unless the dests are stopped first, each failed start that the janitor
	// works through during the close queues two more kicks behind it.
	const minKickBacklog = 100
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, queuedKicks(), uint64(minKickBacklog))
	}, 5*time.Minute, 100*time.Millisecond)

	var wg sync.WaitGroup
	wg.Go(func() {
		rest.AssertStatus(t, rt.SendAdminRequest(http.MethodDelete, "/db/", ""), http.StatusOK)
	})
	base.WaitWithTimeout(t, &wg, time.Minute)

	// Kicks still queued after the manager stops were made while ClosePIndex waited, which only happens when the
	// janitor kept restarting feeds. Counting them instead of timing the close keeps the test independent of CI speed.
	assert.Less(t, queuedKicks(), uint64(minKickBacklog))
}
