// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
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
	RequireBucketSpecificCredentials(t)

	ctx := base.TestCtx(t)
	// Get test bucket
	tb1 := base.GetTestBucket(t)
	defer tb1.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb1.NoCloseClone(),
		serverless:       true,
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0)
		},
	})
	defer rt.Close()
	sc := rt.ServerContext()
	ctx = rt.Context()

	// Blank out all per-bucket creds
	perBucketCreds := sc.Config.BucketCredentials
	rt.ReplacePerBucketCredentials(map[string]*base.CredentialsConfig{})

	// Confirm fetch does not return any configs due to no databases existing
	configs, err := sc.FetchConfigs(ctx, false)
	require.NoError(t, err)
	assert.Empty(t, configs)

	// Create a database
	rt2 := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb1.NoCloseClone(), PersistentConfig: true, GroupID: &sc.Config.Bootstrap.ConfigGroupID})
	defer rt2.Close()
	// Create a new db on the RT to confirm fetch won't retrieve it (due to bucket not being in BucketCredentials)
	resp := rt2.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{
		"bucket": "%s",
		"use_views": %t,
		"num_index_replicas": 0
	}`, tb1.GetName(), base.TestsDisableGSI()))
	RequireStatus(t, resp, http.StatusCreated)

	// Confirm fetch does not return any configs due to no databases in the bucket credentials config
	configs, err = sc.FetchConfigs(ctx, false)
	require.NoError(t, err)
	assert.Empty(t, configs)

	// Add the test bucket to bucket credentials config
	rt.ReplacePerBucketCredentials(perBucketCreds)

	// Confirm fetch does return config for db in tb1
	configs, err = sc.FetchConfigs(ctx, false)
	require.NoError(t, err)
	require.Len(t, configs, 1)
	assert.NotNil(t, configs["db"])
	count, err := sc.fetchAndLoadConfigs(ctx, false)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Confirm fetch does not return any configs due to db being known about already (so existing db does not get polled)
	// TODO: Enable as part of CBG-2280
	//configs, err = sc.FetchConfigs(false)
	//require.NoError(t, err)
	//assert.Empty(t, configs)
	//count, err = sc.fetchAndLoadConfigs(false)
	//require.NoError(t, err)
	//assert.Equal(t, 0, count)
}

// Tests behaviour of CBG-2258 to force per bucket credentials to be used when setting up db in serverless mode
func TestServerlessDBSetupForceCreds(t *testing.T) {
	RequireBucketSpecificCredentials(t)

	ctx := base.TestCtx(t)
	tb1 := base.GetTestBucket(t)
	defer tb1.Close(ctx)

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
			rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb1.NoCloseClone(), serverless: true, PersistentConfig: true})
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
	RequireBucketSpecificCredentials(t)

	ctx := base.TestCtx(t)
	tb1 := base.GetTestBucket(t)
	defer tb1.Close(ctx)
	rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb1.NoCloseClone(), PersistentConfig: true, serverless: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0)
		},
	})
	defer rt.Close()
	ctx = rt.Context()

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
	assert.Error(t, err)
	assert.False(t, found)
}

func TestServerlessGoCBConnectionString(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server due to testing gocb connection strings")
	}
	tests := []struct {
		name                 string
		expectedConnstrQuery string
		specKvConn           string
		kvConnCount          int
	}{
		{
			name:                 "serverless connection",
			expectedConnstrQuery: "?dcp_buffer_size=1048576&idle_http_connection_timeout=90000&kv_buffer_size=1048576&kv_pool_size=1&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			kvConnCount:          1,
		},
		{
			name:                 "serverless connection with kv pool specified",
			specKvConn:           "?idle_http_connection_timeout=90000&kv_pool_size=3&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			expectedConnstrQuery: "?dcp_buffer_size=1048576&idle_http_connection_timeout=90000&kv_buffer_size=1048576&kv_pool_size=3&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			kvConnCount:          3,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Skip("here")
			ctx := base.TestCtx(t)
			tb := base.GetTestBucket(t)
			defer tb.Close(ctx)
			bucketServer := tb.BucketSpec.Server
			expectedServer := bucketServer + test.expectedConnstrQuery

			if test.specKvConn != "" {
				tb.BucketSpec.Server = bucketServer + "?kv_pool_size=3"
			}

			rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb.NoCloseClone(), PersistentConfig: true, serverless: true})
			defer rt.Close()
			sc := rt.ServerContext()
			require.True(t, sc.Config.IsServerless())

			resp := rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{"bucket": "%s", "use_views": %t, "num_index_replicas": 0}`,
				tb.GetName(), base.TestsDisableGSI()))
			RequireStatus(t, resp, http.StatusCreated)

			assert.Equal(t, expectedServer, sc.getBucketSpec("db").Server)
		})
	}

}

func TestServerlessUnsupportedOptions(t *testing.T) {
	RequireBucketSpecificCredentials(t)
	tests := []struct {
		name            string
		expectedConnStr string
		kvBuffer        int
		dcpBuffer       int
	}{
		{
			name:            "unsupported options specified",
			expectedConnStr: "?dcp_buffer_size=3000&idle_http_connection_timeout=90000&kv_buffer_size=2000&kv_pool_size=1&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
			kvBuffer:        2000,
			dcpBuffer:       3000,
		},
		{
			name:            "default serverless",
			expectedConnStr: "?dcp_buffer_size=1048576&idle_http_connection_timeout=90000&kv_buffer_size=1048576&kv_pool_size=1&max_idle_http_connections=64000&max_perhost_idle_http_connections=256",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			tb := base.GetTestBucket(t)
			defer tb.Close(ctx)
			bucketServer := tb.BucketSpec.Server
			test.expectedConnStr = bucketServer + test.expectedConnStr

			rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb.NoCloseClone(), PersistentConfig: true, serverless: true})
			defer rt.Close()
			sc := rt.ServerContext()
			require.True(t, sc.Config.IsServerless())

			if test.name == "unsupported options specified" {
				resp := rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{"bucket": "%s", "use_views": %t, "num_index_replicas": 0, "unsupported": {"dcp_read_buffer": %d, "kv_buffer": %d}}`,
					tb.GetName(), base.TestsDisableGSI(), test.dcpBuffer, test.kvBuffer))
				RequireStatus(t, resp, http.StatusCreated)
			} else {
				resp := rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{"bucket": "%s", "use_views": %t, "num_index_replicas": 0}`,
					tb.GetName(), base.TestsDisableGSI()))
				RequireStatus(t, resp, http.StatusCreated)
			}
			assert.Equal(t, test.expectedConnStr, sc.getBucketSpec("db").Server)
		})
	}

}

func TestServerlessSuspendDatabase(t *testing.T) {
	RequireBucketSpecificCredentials(t)

	ctx := base.TestCtx(t)
	// Get test bucket
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb.NoCloseClone(), PersistentConfig: true, serverless: true})
	defer rt.Close()

	sc := rt.ServerContext()

	// suspendable should default to true when in serverless
	resp := rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{
		"bucket": "%s",
		"use_views": %t,
		"num_index_replicas": 0
	}`, tb.GetName(), base.TestsDisableGSI()))
	RequireStatus(t, resp, http.StatusCreated)

	assert.False(t, sc.isDatabaseSuspended(t, "db"))
	assert.NotNil(t, sc.databases_["db"])
	assert.NotNil(t, sc.dbRegistry["db"])
	assert.NotNil(t, sc.dbConfigs["db"])

	// Unsuspend db that is not suspended should just return db context
	dbCtx, err := sc.unsuspendDatabase(rt.Context(), "db")
	assert.NotNil(t, dbCtx)
	assert.NoError(t, err)

	// Confirm false returned when db does not exist
	err = sc.suspendDatabase(t, rt.Context(), "invalid_db")
	assert.ErrorIs(t, base.ErrNotFound, err)

	// Confirm true returned when suspended a database successfully
	err = sc.suspendDatabase(t, rt.Context(), "db")
	assert.NoError(t, err)

	// Make sure database is suspended
	assert.True(t, sc.isDatabaseSuspended(t, "db"))
	assert.Nil(t, sc.databases_["db"])
	assert.NotNil(t, sc.dbRegistry["db"])
	assert.NotNil(t, sc.dbConfigs["db"])

	// Update config in bucket to see if unsuspending check for updates
	sc.dbConfigs["db"].EnableXattrs = base.BoolPtr(true) // xattrs must be enabled
	cas, err := sc.BootstrapContext.UpdateConfig(base.TestCtx(t), tb.GetName(), sc.Config.Bootstrap.ConfigGroupID, "db", func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		config := sc.dbConfigs["db"].ToDatabaseConfig()
		config.cfgCas = bucketDbConfig.cfgCas
		return config, nil
	})
	require.NoError(t, err)
	assert.NotEqual(t, cas, sc.dbConfigs["db"].cfgCas)

	// Unsuspend db
	dbCtx, err = sc.unsuspendDatabase(rt.Context(), "db")
	require.NoError(t, err)
	assert.NotNil(t, dbCtx)
	assert.False(t, sc.isDatabaseSuspended(t, "db"))
	assert.NotNil(t, sc.databases_["db"])
	assert.NotNil(t, sc.dbRegistry["db"])
	require.NotNil(t, sc.dbConfigs["db"])

	// Make sure updated config is being used
	assert.Equal(t, cas, sc.dbConfigs["db"].cfgCas)

	// Attempt unsuspend of invalid db
	dbCtx, err = sc.unsuspendDatabase(rt.Context(), "invalid")
	require.Error(t, err)
	assert.Nil(t, dbCtx)
	assert.Nil(t, sc.databases_["invalid"])
	assert.Nil(t, sc.dbConfigs["invalid"])
}

// Confirms that when the database config is not in sc.dbConfigs, the fetch callback is check if the config is in a bucket
func TestServerlessUnsuspendFetchFallback(t *testing.T) {
	RequireBucketSpecificCredentials(t)
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		serverless:       true,
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0)
		},
	})
	defer rt.Close()
	sc := rt.ServerContext()

	resp := rt.SendAdminRequest(http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	RequireStatus(t, resp, http.StatusCreated)

	// Suspend the database and remove it from dbConfigs, forcing unsuspendDatabase to fetch config from the bucket
	err := sc.suspendDatabase(t, rt.Context(), "db")
	assert.NoError(t, err)
	delete(sc.dbConfigs, "db")
	delete(sc.dbRegistry, "db")
	assert.Nil(t, sc.databases_["db"])

	// Unsuspend db and confirm unsuspending worked
	dbCtx, err := sc.GetDatabase(rt.Context(), "db")
	assert.NoError(t, err)
	assert.NotNil(t, dbCtx)
	assert.NotNil(t, sc.databases_["db"])

	// Attempt to get invalid database
	_, err = sc.GetDatabase(rt.Context(), "invalid")
	assert.Contains(t, err.Error(), "no such database")
}

// Confirms that ServerContext.fetchConfigsWithTTL works correctly
func TestServerlessFetchConfigsLimited(t *testing.T) {
	RequireBucketSpecificCredentials(t)

	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0)
		},
	})
	defer rt.Close()
	sc := rt.ServerContext()

	resp := rt.SendAdminRequest(http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	RequireStatus(t, resp, http.StatusCreated)

	// Purposely make configs get caches
	sc.Config.Unsupported.Serverless.MinConfigFetchInterval = base.NewConfigDuration(time.Hour)
	dbConfigsBefore, err := sc.fetchConfigsSince(rt.Context(), sc.Config.Unsupported.Serverless.MinConfigFetchInterval)
	require.NoError(t, err)
	require.NotEmpty(t, dbConfigsBefore["db"])
	timeCached := sc.fetchConfigsLastUpdate
	require.NoError(t, err)
	assert.NotZero(t, timeCached)

	require.NoError(t, err)
	// Update database config in the bucket (increment version)
	newCas, err := sc.BootstrapContext.UpdateConfig(ctx, tb.GetName(), sc.Config.Bootstrap.ConfigGroupID, "db", func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		bucketDbConfig.Version, err = GenerateDatabaseConfigVersionID(rt.Context(), bucketDbConfig.Version, &bucketDbConfig.DbConfig)
		require.NoError(t, err)
		return bucketDbConfig, nil
	})
	require.NoError(t, err)

	// Fetch configs again and expect same config to be returned
	dbConfigsAfter, err := sc.fetchConfigsSince(rt.Context(), sc.Config.Unsupported.Serverless.MinConfigFetchInterval)
	require.NoError(t, err)
	require.NotEmpty(t, dbConfigsAfter["db"])
	assert.Equal(t, dbConfigsBefore["db"].cfgCas, dbConfigsAfter["db"].cfgCas)
	assert.Equal(t, timeCached, sc.fetchConfigsLastUpdate)

	// Make caching 1ms so it will grab newest config
	sc.Config.Unsupported.Serverless.MinConfigFetchInterval = base.NewConfigDuration(time.Millisecond)
	// Sleep to make sure enough time passes
	time.Sleep(time.Millisecond * 500)
	dbConfigsAfter, err = sc.fetchConfigsSince(rt.Context(), sc.Config.Unsupported.Serverless.MinConfigFetchInterval)
	require.NoError(t, err)
	require.NotEmpty(t, dbConfigsAfter["db"])
	assert.Equal(t, newCas, dbConfigsAfter["db"].cfgCas)

	// Change back for next test before next config update (not fully necessary but just to be safe)
	sc.Config.Unsupported.Serverless.MinConfigFetchInterval = base.NewConfigDuration(time.Hour)

	// Update database config in the bucket again to test caching disable case
	newCas, err = sc.BootstrapContext.UpdateConfig(ctx, tb.GetName(), sc.Config.Bootstrap.ConfigGroupID, "db", func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		bucketDbConfig.Version, err = GenerateDatabaseConfigVersionID(rt.Context(), bucketDbConfig.Version, &bucketDbConfig.DbConfig)
		if err != nil {
			return nil, err
		}
		return bucketDbConfig, nil
	})
	require.NoError(t, err)

	// Disable caching and expect new config
	sc.Config.Unsupported.Serverless.MinConfigFetchInterval = base.NewConfigDuration(0)
	dbConfigsAfter, err = sc.fetchConfigsSince(rt.Context(), sc.Config.Unsupported.Serverless.MinConfigFetchInterval)
	require.NoError(t, err)
	require.NotEmpty(t, dbConfigsAfter["db"])
	assert.Equal(t, newCas, dbConfigsAfter["db"].cfgCas)
}

// Checks what happens to a suspended database when the config is modified by another node and the periodic fetchAndLoadConfigs gets called.
// Currently, it will be unsuspended however that behaviour may be changed in the future
func TestServerlessUpdateSuspendedDb(t *testing.T) {
	RequireBucketSpecificCredentials(t)
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0)
		},
	})
	defer rt.Close()
	sc := rt.ServerContext()

	resp := rt.SendAdminRequest(http.MethodPut, "/db/",
		fmt.Sprintf(
			`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "use_views": %t, "suspendable": true}`,
			tb.GetName(), base.TestUseXattrs(), base.TestsDisableGSI(),
		),
	)
	RequireStatus(t, resp, http.StatusCreated)

	// Suspend the database
	assert.NoError(t, sc.suspendDatabase(t, rt.Context(), "db"))
	// Update database config
	newCas, err := sc.BootstrapContext.UpdateConfig(base.TestCtx(t), tb.GetName(), sc.Config.Bootstrap.ConfigGroupID, "db", func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		config := sc.dbConfigs["db"].ToDatabaseConfig()
		config.cfgCas = bucketDbConfig.cfgCas
		return config, nil
	})
	require.NoError(t, err)
	// Confirm dbConfig cas did not update yet in SG, or get unsuspended
	assert.NotEqual(t, sc.dbConfigs["db"].cfgCas, newCas)
	assert.True(t, sc.isDatabaseSuspended(t, "db"))
	assert.Nil(t, sc.databases_["db"])
	// Trigger update frequency (would usually happen every ConfigUpdateFrequency seconds)
	count, err := sc.fetchAndLoadConfigs(rt.Context(), false)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Make sure database is still suspended
	assert.True(t, sc.isDatabaseSuspended(t, "db"))
	assert.Nil(t, sc.databases_["db"])
}

// Tests scenarios a database is and is not allowed to suspend
func TestSuspendingFlags(t *testing.T) {
	RequireBucketSpecificCredentials(t)
	testCases := []struct {
		name             string
		serverlessMode   bool
		dbSuspendable    *bool
		expectCanSuspend bool
	}{
		{
			name:             "Serverless defaults suspendable flag on db to true",
			serverlessMode:   true,
			dbSuspendable:    nil,
			expectCanSuspend: true,
		},
		{
			name:             "Serverless with suspendable db disallowed",
			serverlessMode:   true,
			dbSuspendable:    base.BoolPtr(false),
			expectCanSuspend: false,
		},
		{
			name:             "Non-serverless with suspendable db",
			serverlessMode:   false,
			dbSuspendable:    base.BoolPtr(true),
			expectCanSuspend: true,
		},
		{
			name:             "Non-serverless with unsuspendable db",
			serverlessMode:   false,
			dbSuspendable:    base.BoolPtr(false),
			expectCanSuspend: false,
		},
		{
			name:             "Non-serverless with db default suspendable option",
			serverlessMode:   false,
			dbSuspendable:    nil,
			expectCanSuspend: false,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			tb := base.GetTestBucket(t)
			defer tb.Close(ctx)

			rt := NewRestTester(t,
				&RestTesterConfig{CustomTestBucket: tb.NoCloseClone(), PersistentConfig: true, serverless: test.serverlessMode})
			defer rt.Close()

			sc := rt.ServerContext()

			suspendableDbOption := ""
			if test.dbSuspendable != nil {
				suspendableDbOption = fmt.Sprintf(`"suspendable": %v,`, *test.dbSuspendable)
			}
			resp := rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{
				"bucket": "%s",
				"use_views": %t,
				%s
				"num_index_replicas": 0
			}`, tb.GetName(), base.TestsDisableGSI(), suspendableDbOption))
			RequireStatus(t, resp, http.StatusCreated)

			err := sc.suspendDatabase(t, rt.Context(), "db")
			if test.expectCanSuspend {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, ErrSuspendingDisallowed)
				return
			}

			dbc, err := sc.unsuspendDatabase(rt.Context(), "db")
			if test.expectCanSuspend {
				assert.NoError(t, err)
				assert.NotNil(t, dbc)
			}
		})
	}
}

// Tests the public API unsuspending a database automatically
func TestServerlessUnsuspendAPI(t *testing.T) {
	RequireBucketSpecificCredentials(t)
	ctx := base.TestCtx(t)
	// Get test bucket
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb.NoCloseClone(), PersistentConfig: true, serverless: true})
	defer rt.Close()

	sc := rt.ServerContext()

	resp := rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{
		"bucket": "%s",
		"use_views": %t,
		"num_index_replicas": 0
	}`, tb.GetName(), base.TestsDisableGSI()))
	RequireStatus(t, resp, http.StatusCreated)

	err := sc.suspendDatabase(t, rt.Context(), "db")
	assert.NoError(t, err)

	// Confirm db is suspended
	require.True(t, sc.isDatabaseSuspended(t, "db"))
	require.Nil(t, sc.databases_["db"])

	// Attempt to unsuspend using unauthenticated public API request
	resp = rt.SendRequest(http.MethodGet, "/db/doc", "")
	AssertStatus(t, resp, http.StatusUnauthorized)

	// Confirm db is unsuspended
	require.False(t, sc.isDatabaseSuspended(t, "db"))
	require.NotNil(t, sc.databases_["db"])
}

// Makes sure admin API calls do not unsuspend DB if they fail authentication
func TestServerlessUnsuspendAdminAuth(t *testing.T) {
	RequireBucketSpecificCredentials(t)
	ctx := base.TestCtx(t)
	// Get test bucket
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb.NoCloseClone(), PersistentConfig: true, serverless: true, AdminInterfaceAuthentication: true})
	defer rt.Close()

	sc := rt.ServerContext()

	resp := rt.SendAdminRequestWithAuth(http.MethodPut, "/db/", fmt.Sprintf(`{
		"bucket": "%s",
		"use_views": %t,
		"num_index_replicas": 0
	}`, tb.GetName(), base.TestsDisableGSI()), base.TestClusterUsername(), base.TestClusterPassword())
	RequireStatus(t, resp, http.StatusCreated)

	err := sc.suspendDatabase(t, rt.Context(), "db")
	assert.NoError(t, err)

	// Confirm db is suspended
	require.True(t, sc.isDatabaseSuspended(t, "db"))
	require.Nil(t, sc.databases_["db"])

	// Confirm unauthenticated admin request does not trigger unsuspend
	resp = rt.SendAdminRequest(http.MethodGet, "/db/doc", "")
	AssertStatus(t, resp, http.StatusUnauthorized)
	require.Nil(t, sc.databases_["db"]) // Confirm suspended
	require.True(t, sc.isDatabaseSuspended(t, "db"))

	// Confirm authenticated admin request triggers unsuspend
	resp = rt.SendAdminRequestWithAuth(http.MethodGet, "/db/doc", "", base.TestClusterUsername(), base.TestClusterPassword())
	AssertStatus(t, resp, http.StatusNotFound)
	require.NotNil(t, sc.databases_["db"]) // Confirm unsuspended
	require.False(t, sc.isDatabaseSuspended(t, "db"))

	// Attempt to get DB that does not exist
	resp = rt.SendAdminRequestWithAuth(http.MethodGet, "/invaliddb/doc", "", base.TestClusterUsername(), base.TestClusterPassword())
	assertHTTPErrorReason(t, resp, http.StatusForbidden, "")
}

func TestImportPartitionsServerless(t *testing.T) {
	RequireBucketSpecificCredentials(t)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test requires cbgt")
	}
	if !base.TestUseXattrs() {
		t.Skip("tests import which is not available without xattrs")
	}
	tests := []struct {
		name               string
		importPartition    *uint16
		expectedPartitions *uint16
		serverless         bool
	}{
		{
			name:               "serverless partitions",
			expectedPartitions: base.Uint16Ptr(6),
			serverless:         true,
		},
		{
			name:               "serverless partitions with import_partition specified",
			importPartition:    base.Uint16Ptr(8),
			expectedPartitions: base.Uint16Ptr(8),
			serverless:         true,
		},
		{
			name:               "non serverless partitions",
			expectedPartitions: base.Uint16Ptr(16),
			serverless:         false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expectedPartitions := test.expectedPartitions
			if !base.IsEnterpriseEdition() {
				t.Logf("Import partitions setting is only supported in EE")
				expectedPartitions = nil
			}

			ctx := base.TestCtx(t)
			tb := base.GetTestBucket(t)
			defer tb.Close(ctx)
			rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: tb.NoCloseClone(), PersistentConfig: true, serverless: test.serverless})
			defer rt.Close()
			sc := rt.ServerContext()

			var dbconf *DbConfig
			if test.name == "serverless partitions with import_partition specified" {
				resp := rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{"bucket": "%s", "use_views": %t, "num_index_replicas": 0, "import_partitions": 8}`,
					tb.GetName(), base.TestsDisableGSI()))
				RequireStatus(t, resp, http.StatusCreated)
				dbconf = sc.GetDbConfig("db")
			} else {
				dbconf = DefaultDbConfig(sc.Config, base.TestUseXattrs())
			}

			assert.Equal(t, expectedPartitions, dbconf.ImportPartitions)
		})
	}
}
