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

	rt := NewRestTester(t, &RestTesterConfig{
		TestBucket:       tb1,
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
	rt2 := NewRestTester(t, &RestTesterConfig{TestBucket: tb1, persistentConfig: true})
	defer rt2.Close()
	// Create a new db on the RT to confirm fetch won't retrieve it (due to bucket not being in BucketCredentials)
	resp := rt2.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{
		"bucket": "%s",
		"use_views": %t,
		"num_index_replicas": 0
	}`, tb1.GetName(), base.TestsDisableGSI()))
	requireStatus(t, resp, http.StatusCreated)

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
				assertStatus(t, resp, http.StatusCreated)
			},
		},
		{
			name:           "Credentials not defined",
			bucketName:     tb1.GetName(),
			perBucketCreds: map[string]*base.CredentialsConfig{"invalid_bucket": {}},
			dbCreationRespAsserts: func(resp *TestResponse) {
				assertStatus(t, resp, http.StatusInternalServerError)
				assert.Contains(t, string(resp.BodyBytes()), "credentials are not defined in bucket_credentials")
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{TestBucket: tb1, serverless: true, persistentConfig: true})
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
	rt := NewRestTester(t, &RestTesterConfig{TestBucket: tb1, persistentConfig: true, serverless: true,
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
	requireStatus(t, resp, http.StatusCreated)

	// Make sure DB can be fetched
	found, _, err := rt.ServerContext().fetchDatabase(ctx, "db")
	assert.NoError(t, err)
	assert.True(t, found)

	// Limit SG to buckets defined on BucketCredentials map
	rt.ReplacePerBucketCredentials(map[string]*base.CredentialsConfig{"invalid_bucket": {}})
	// Make sure fetch fails as it cannot see all buckets in cluster
	found, _, err = rt.ServerContext().fetchDatabase(ctx, "db")
	assert.NoError(t, err)
	assert.False(t, found)
}
