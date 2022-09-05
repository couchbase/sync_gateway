package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

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

	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb1,
		serverless:       true,
		persistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0)
		},
	})
	defer rt.Close()
	sc := rt.ServerContext()
	ctx := rt.Context()

	// Blank out all per-bucket creds
	perBucketCreds := sc.config.BucketCredentials
	rt.ReplacePerBucketCredentials(map[string]*base.CredentialsConfig{})

	// Confirm fetch does not return any configs due to no databases existing
	configs, err := sc.fetchConfigs(ctx, false)
	require.NoError(t, err)
	assert.Empty(t, configs)

	// Create a database
	rt2 := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb1, persistentConfig: true})
	defer rt2.Close()
	// Create a new db on the RT to confirm fetch won't retrieve it (due to bucket not being in BucketCredentials)
	resp := rt2.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{
		"bucket": "%s",
		"use_views": %t,
		"num_index_replicas": 0
	}`, tb1.GetName(), base.TestsDisableGSI()))
	RequireStatus(t, resp, http.StatusCreated)

	// Confirm fetch does not return any configs due to no databases in the bucket credentials config
	configs, err = sc.fetchConfigs(ctx, false)
	require.NoError(t, err)
	assert.Empty(t, configs)

	// Add the test bucket to bucket credentials config
	rt.ReplacePerBucketCredentials(perBucketCreds)

	// Confirm fetch does return config for db in tb1
	configs, err = sc.fetchConfigs(ctx, false)
	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.NotNil(t, configs["db"])
	count, err := sc.fetchAndLoadConfigs(ctx, false)
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

// Tests behaviour of CBG-2258 to force per bucket credentials to be used when setting up db in serverless mode
func TestServerlessDBSetupForceCreds(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	tb1 := base.GetTestBucket(t)
	defer tb1.Close()

	testCases := []struct {
		name                  string
		bucketName            string // Bucket to attempt to create DB on
		perBucketCreds        base.PerBucketCredentialsConfig
		dbCreationRespAsserts func(resp *TestResponse)
	}{
		{
			name:           "Correct credentials defined and force used",
			bucketName:     tb1.GetName(),
			perBucketCreds: nil,
			dbCreationRespAsserts: func(resp *TestResponse) {
				AssertStatus(t, resp, http.StatusCreated)
			},
		},
		{
			name:           "Credentials not defined",
			bucketName:     tb1.GetName(),
			perBucketCreds: map[string]*base.CredentialsConfig{"invalid_bucket": {}},
			dbCreationRespAsserts: func(resp *TestResponse) {
				AssertStatus(t, resp, http.StatusInternalServerError)
				assert.Contains(t, string(resp.BodyBytes()), "credentials are not defined in bucket_credentials")
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb1, serverless: true, persistentConfig: true})
			defer rt.Close()

			if test.perBucketCreds != nil {
				rt.ReplacePerBucketCredentials(test.perBucketCreds)
			}

			resp := rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{
				"bucket": "%s",
				"use_views": %t,
				"num_index_replicas": 0
			}`, tb1.GetName(), base.TestsDisableGSI()))
			test.dbCreationRespAsserts(resp)
		})
	}
}

// Tests behaviour of CBG-2258 to make sure fetch databases only uses buckets listed on StartupConfig.BucketCredentials
// when running in serverless mode
func TestServerlessBucketCredentialsFetchDatabases(t *testing.T) {
	base.LongRunningTest(t)

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	tb1 := base.GetTestBucket(t)
	defer tb1.Close()
	rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb1, persistentConfig: true, serverless: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0)
		},
	})
	defer rt.Close()
	ctx := rt.Context()

	resp := rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{
				"bucket": "%s",
				"use_views": %t,
				"num_index_replicas": 0
	}`, tb1.GetName(), base.TestsDisableGSI()))
	RequireStatus(t, resp, http.StatusCreated)

	// Make sure DB can be fetched
	found, _, err := rt.ServerContext().fetchDatabase(ctx, "db")
	assert.NoError(t, err)
	assert.True(t, found)

	// Limit SG to buckets defined on BucketCredentials map
	rt.ReplacePerBucketCredentials(map[string]*base.CredentialsConfig{})
	// Make sure fetch fails as it cannot see all buckets in cluster
	found, _, err = rt.ServerContext().fetchDatabase(ctx, "db")
	assert.NoError(t, err)
	assert.False(t, found)
}

func TestServerlessSuspendDatabase(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server due to updating database config using a Bootstrap connection")
	}

	// Get test bucket
	tb := base.GetTestBucket(t)
	defer tb.Close()

	rt := NewRestTester(t, &RestTesterConfig{TestBucket: tb, persistentConfig: true})
	defer rt.Close()

	sc := rt.ServerContext()

	resp := rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{
		"bucket": "%s",
		"use_views": %t,
		"num_index_replicas": 0
	}`, tb.GetName(), base.TestsDisableGSI()))
	requireStatus(t, resp, http.StatusCreated)

	assert.NotNil(t, sc.databases_["db"])
	assert.Equal(t, "db", sc.bucketDbName[tb.GetName()])
	assert.NotNil(t, sc.dbConfigs["db"])

	// Unsuspend db that is not suspended should just return db context
	dbCtx, err := sc.unsuspendDatabase("db")
	assert.NotNil(t, dbCtx)
	assert.NoError(t, err)

	// Confirm false returned when db does not exist
	suspended := sc.suspendDatabase("invalid_db")
	assert.False(t, suspended)

	// Confirm true returned when suspended a database successfully
	suspended = sc.suspendDatabase("db")
	assert.True(t, suspended)

	// Make sure database is suspended
	assert.Nil(t, sc.databases_["db"])
	assert.Empty(t, sc.bucketDbName[tb.GetName()])
	assert.NotNil(t, sc.dbConfigs["db"])

	// Update config in bucket to see if unsuspending check for updates
	cas, err := sc.bootstrapContext.connection.UpdateConfig(tb.GetName(), sc.config.Bootstrap.ConfigGroupID,
		func(rawBucketConfig []byte) (updatedConfig []byte, err error) {
			return json.Marshal(sc.dbConfigs["db"])
		},
	)
	require.NoError(t, err)
	assert.NotEqual(t, cas, sc.dbConfigs["db"].cas)

	// Unsuspend db
	dbCtx, err = sc.unsuspendDatabase("db")
	assert.NotNil(t, dbCtx)
	assert.NotNil(t, sc.databases_["db"])
	assert.Equal(t, "db", sc.bucketDbName[tb.GetName()])
	require.NotNil(t, sc.dbConfigs["db"])

	// Make sure updated config is being used
	assert.Equal(t, cas, sc.dbConfigs["db"].cas)

	// Attempt unsuspend of invalid db
	dbCtx, err = sc.unsuspendDatabase("invalid")
	assert.Nil(t, dbCtx)
	assert.Nil(t, sc.databases_["invalid"])
	assert.Nil(t, sc.dbConfigs["invalid"])
}

// Confirms that when the database config is not in sc.dbConfigs, the fetch callback is check if the config is in a bucket
func TestServerlessUnsuspendFetchFallback(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	tb := base.GetTestBucket(t)
	defer tb.Close()

	// Set up server context
	config := bootstrapStartupConfigForTest(t)
	config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0) // Make sure database does not get imported by fetchAndLoadConfigs

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

	resp := bootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.requireStatus(http.StatusCreated)

	// Suspend the database and remove it from dbConfigs, forcing unsuspendDatabase to fetch config from the bucket
	sc.suspendDatabase("db")
	delete(sc.dbConfigs, "db")
	assert.Nil(t, sc.databases_["db"])

	// Unsuspend db and confirm unsuspending worked
	dbCtx, err := sc.GetServerlessDatabase("db")
	assert.NoError(t, err)
	assert.NotNil(t, dbCtx)
	assert.NotNil(t, sc.databases_["db"])

	// Attempt to get invalid database
	dbCtx, err = sc.GetServerlessDatabase("invalid")
	assert.Contains(t, err.Error(), "no such database")
}

// Confirms that ServerContext.fetchConfigsCache works correctly
func TestServerlessFetchConfigsLimited(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	tb := base.GetTestBucket(t)
	defer tb.Close()

	// Set up server context
	config := bootstrapStartupConfigForTest(t)
	config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0) // Make sure database does not get imported by fetchAndLoadConfigs

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

	resp := bootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.requireStatus(http.StatusCreated)

	// Purposely make configs get caches
	sc.config.Unsupported.Serverless.FetchConfigsCacheTTL = base.NewConfigDuration(time.Hour)
	dbConfigsBefore, err := sc.fetchConfigsCache()
	require.NotEmpty(t, dbConfigsBefore["db"])
	timeCached := sc.fetchConfigsCacheUpdated
	assert.NotZero(t, timeCached)
	require.NoError(t, err)

	// Update database config in the bucket
	newCas, err := sc.bootstrapContext.connection.UpdateConfig(tb.GetName(), sc.config.Bootstrap.ConfigGroupID,
		func(rawBucketConfig []byte) (updatedConfig []byte, err error) {
			return json.Marshal(sc.dbConfigs["db"])
		},
	)

	// Fetch configs again and expect same config to be returned
	dbConfigsAfter, err := sc.fetchConfigsCache()
	require.NotEmpty(t, dbConfigsAfter["db"])
	assert.Equal(t, dbConfigsBefore["db"].cas, dbConfigsAfter["db"].cas)
	assert.Equal(t, timeCached, sc.fetchConfigsCacheUpdated)

	// Make caching 1ms so it will grab newest config
	sc.config.Unsupported.Serverless.FetchConfigsCacheTTL = base.NewConfigDuration(time.Millisecond)
	// Sleep to make sure enough time passes
	time.Sleep(time.Millisecond * 500)
	dbConfigsAfter, err = sc.fetchConfigsCache()
	require.NotEmpty(t, dbConfigsAfter["db"])
	assert.Equal(t, newCas, dbConfigsAfter["db"].cas)
	// Change back for next test before next config update (not fully necessary but just to be safe)
	sc.config.Unsupported.Serverless.FetchConfigsCacheTTL = base.NewConfigDuration(time.Hour)

	// Update database config in the bucket again to test caching disable case
	newCas, err = sc.bootstrapContext.connection.UpdateConfig(tb.GetName(), sc.config.Bootstrap.ConfigGroupID,
		func(rawBucketConfig []byte) (updatedConfig []byte, err error) {
			return json.Marshal(sc.dbConfigs["db"])
		},
	)

	// Disable caching and expect new config
	sc.config.Unsupported.Serverless.FetchConfigsCacheTTL = base.NewConfigDuration(0)
	dbConfigsAfter, err = sc.fetchConfigsCache()
	require.NotEmpty(t, dbConfigsAfter["db"])
	assert.Equal(t, newCas, dbConfigsAfter["db"].cas)
}

// Checks what happens to a suspended database when the config is modified by another node and the periodic fetchAndLoadConfigs gets called.
// Currently, it will be unsuspended however that behaviour may be changed in the future
func TestServerlessUpdateSuspendedDb(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	tb := base.GetTestBucket(t)
	defer tb.Close()

	// Set up server context
	config := bootstrapStartupConfigForTest(t)
	config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0) // Make sure fetchAndLoadConfigs does not get triggered

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

	resp := bootstrapAdminRequest(t, http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	resp.requireStatus(http.StatusCreated)

	// Suspend the database
	assert.True(t, sc.suspendDatabase("db"))
	// Update database config
	newCas, err := sc.bootstrapContext.connection.UpdateConfig(tb.GetName(), sc.config.Bootstrap.ConfigGroupID,
		func(rawBucketConfig []byte) (updatedConfig []byte, err error) {
			return json.Marshal(sc.dbConfigs["db"])
		},
	)
	// Confirm dbConfig cas did not update yet in SG, or get unsuspended
	assert.NotEqual(t, sc.dbConfigs["db"].cas, newCas)
	assert.Nil(t, sc.databases_["db"])
	// Trigger update frequency (would usually happen every ConfigUpdateFrequency seconds)
	count, err := sc.fetchAndLoadConfigs(false)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Check if it reloaded the database
	assert.NotNil(t, sc.databases_["db"])
}
