package rest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests behaviour of CBG-2257 to poll only buckets in BucketCredentials that don't currently have a database
func TestServerlessPollBuckets(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	// Get test bucket
	tb1 := base.GetTestBucket(t)
	defer tb1.Close()

	config := bootstrapStartupConfigForTest(t)
	config.Unsupported.Serverless = base.BoolPtr(true)
	config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0)
	// Use invalid bucket to get past validation stage - will cause warnings throughout test
	config.BucketCredentials = map[string]*base.CredentialsConfig{"invalid_bucket": {}}

	sc, err := setupServerContext(&config, true)
	require.NoError(t, err)

	serverErr := make(chan error, 0)
	defer func() {
		sc.Close()
		require.NoError(t, <-serverErr)
	}()

	go func() {
		serverErr <- startServer(&config, sc)
	}()
	require.NoError(t, sc.waitForRESTAPIs())

	// Confirm fetch does not return any configs due to no databases existing
	configs, err := sc.fetchConfigs(false)
	require.NoError(t, err)
	assert.Empty(t, configs)

	// Create a database
	rt := NewRestTester(t, &RestTesterConfig{TestBucket: tb1, groupID: base.StringPtr(config.Bootstrap.ConfigGroupID), persistentConfig: true})
	defer rt.Close()
	// Create a new db on the RT to confirm fetch won't retrieve it (due to bucket not being in BucketCredentials)
	resp := rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{
		"bucket": "%s",
		"use_views": %t,
		"num_index_replicas": 0
	}`, tb1.GetName(), base.TestsDisableGSI()))
	requireStatus(t, resp, http.StatusCreated)

	// Confirm fetch does not return any configs due to no databases in the bucket credentials config
	configs, err = sc.fetchConfigs(false)
	require.NoError(t, err)
	assert.Empty(t, configs)

	// Add test bucket to bucket credentials config
	sc.config.BucketCredentials = map[string]*base.CredentialsConfig{
		tb1.GetName(): {
			Username: base.TestClusterUsername(),
			Password: base.TestClusterPassword(),
		},
	}

	// Update the CouchbaseCluster to include the new bucket credentials
	couchbaseCluster, err := createCouchbaseClusterFromStartupConfig(sc.config)
	require.NoError(t, err)
	sc.bootstrapContext.connection = couchbaseCluster

	// Confirm fetch does return config for db in tb1
	configs, err = sc.fetchConfigs(false)
	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.NotNil(t, configs["db"])
	count, err := sc.fetchAndLoadConfigs(false)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Confirm fetch does not return any configs due to db being known about already (so existing db does not get polled)
	// TODO: Enable as part of CBG-2280
	//configs, err = sc.fetchConfigs(false)
	//require.NoError(t, err)
	//assert.Empty(t, configs)
	//count, err = sc.fetchAndLoadConfigs(false)
	//require.NoError(t, err)
	//assert.Equal(t, 0, count)
}
