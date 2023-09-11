package rest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCorruptDbConfigHandling:
//   - Create persistent config rest tester
//   - Create a db with a non-corrupt config
//   - Grab the persisted config from the bucket for purposes of changing the bucket name on it (this simulates a
//     dbconfig becoming corrupt)
//   - Update the persisted config to change the bucket name to a non-existent bucket
//   - Assert that the db context and config are removed from server context and that operations on the database GET and
//     DELETE and operation to update the config fail with appropriate error message for user
//   - Test we are able to update the config to correct the corrupted db config using the /db1/ endpoint
//   - assert the db returns to the server context and is removed from corrupt database tracking AND that the bucket name
//     on the config now matches the rest tester bucket name
func TestCorruptDbConfigHandling(t *testing.T) {
	base.TestsRequireBootstrapConnection(t)

	rt := NewRestTester(t, &RestTesterConfig{
		TestBucket:       base.GetTestBucket(t),
		persistentConfig: true,
		/*MutateStartupConfig: func(config *StartupConfig) {
			// configure the interval time to pick up new configs from the bucket to every 1 seconds
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(1 * time.Second)
		},*/
	})
	defer rt.Close()

	// create db with correct config
	dbConfig := rt.NewDbConfig()
	resp, err := rt.CreateDatabase("db", dbConfig)
	require.NoError(t, err)
	RequireStatus(t, resp, http.StatusCreated)

	// wait for db to come online
	require.NoError(t, rt.WaitForDBOnline())

	// grab the persisted db config from the bucket
	databaseConfig := DatabaseConfig{}
	_, err = rt.ServerContext().bootstrapContext.connection.GetConfig(rt.Bucket().GetName(), rt.ServerContext().config.Bootstrap.ConfigGroupID, &databaseConfig)
	require.NoError(t, err)

	// update the persisted config to a fake bucket name
	newBucketName := "fakeBucket"
	_, err = rt.UpdatePersistedBucketName(&databaseConfig, &newBucketName)
	require.NoError(t, err)

	// wait for some time for interval to remove the db
	err = rt.WaitForConditionWithOptions(func() bool {
		list := rt.ServerContext().AllDatabaseNames()
		return len(list) == 0
	}, 200, 1000)
	require.NoError(t, err)

	// assert that the in memory representation of the db config on the server context is gone now we have broken the config
	responseConfig := rt.ServerContext().GetDbConfig("db")
	assert.Nil(t, responseConfig)

	// assert that fetching config fails with the correct error message to the user
	resp = rt.SendAdminRequest(http.MethodGet, "/db/_config", "")
	RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "You must update database config immediately")

	// assert trying to delete fails with the correct error message to the user
	resp = rt.SendAdminRequest(http.MethodDelete, "/db/", "")
	RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "You must update database config immediately")

	// correct the name through update to config
	resp, err = rt.ReplaceDbConfig("db", dbConfig)
	require.NoError(t, err)
	RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "You must update database config immediately")

	// create db of same name with correct db config to correct the corrupt db config
	resp, err = rt.CreateDatabase("db", dbConfig)
	require.NoError(t, err)
	RequireStatus(t, resp, http.StatusCreated)

	// wait some time for interval to pick up change
	err = rt.WaitForConditionWithOptions(func() bool {
		list := rt.ServerContext().AllDatabaseNames()
		return len(list) == 1
	}, 200, 1000)
	require.NoError(t, err)

	// assert that the config is back in memory even after another interval update pass and asser the persisted config
	// bucket name matches rest tester bucket name
	dbCtx, err := rt.ServerContext().GetDatabase("db")
	require.NoError(t, err)
	assert.NotNil(t, dbCtx)
	assert.Equal(t, rt.Bucket().GetName(), dbCtx.Bucket.GetName())
	rt.ServerContext().RequireInvalidDatabaseConfigNames(t, []string{})
}

// TestBadConfigInsertionToBucket:
//   - start a rest tester
//   - insert an invalid db config to the bucket while rest tester is running
//   - assert that the db config is picked up as an invalid db config
//   - assert that a call to the db endpoint will fail with correct error message
func TestBadConfigInsertionToBucket(t *testing.T) {
	base.TestsRequireBootstrapConnection(t)

	rt := NewRestTester(t, &RestTesterConfig{
		TestBucket:       base.GetTestBucket(t),
		persistentConfig: true,
		/*MutateStartupConfig: func(config *StartupConfig) {
			// configure the interval time to pick up new configs from the bucket to every 1 seconds
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(1 * time.Second)
		},
		*/
		DatabaseConfig: nil,
	})
	defer rt.Close()

	// create a new invalid db config and persist to bucket
	badName := "badBucketName"
	dbConfig := rt.NewDbConfig()
	dbConfig.Name = "db1"

	version, err := GenerateDatabaseConfigVersionID("", &dbConfig)
	require.NoError(t, err)

	dbConfig.Bucket = &badName
	persistedConfig := DatabaseConfig{
		Version:  version,
		DbConfig: dbConfig,
	}
	rt.InsertDbConfigToBucket(&persistedConfig, rt.Bucket().GetName())

	// asser that the config is picked up as invalid config on server context
	err = rt.WaitForConditionWithOptions(func() bool {
		invalidDatabases := rt.ServerContext().AllInvalidDatabases()
		return len(invalidDatabases) == 1
	}, 200, 1000)
	require.NoError(t, err)

	// assert that a request to the database fails with correct error message
	resp := rt.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "Must update database config immediately")
}

// TestMismatchedBucketNameOnDbConfigUpdate:
//   - Create a db on the rest tester
//   - attempt to update the config to change the bucket name on the config to a mismatched bucket name
//   - assert the request fails
func TestMismatchedBucketNameOnDbConfigUpdate(t *testing.T) {
	base.TestsRequireBootstrapConnection(t)
	base.RequireNumTestBuckets(t, 2)
	tb1 := base.GetTestBucket(t)
	defer tb1.Close()

	rt := NewRestTester(t, &RestTesterConfig{
		TestBucket:       base.GetTestBucket(t),
		persistentConfig: true,
		/*MutateStartupConfig: func(config *StartupConfig) {
			// configure the interval time to pick up new configs from the bucket to every 1 seconds
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(1 * time.Second)
		},*/
	})
	defer rt.Close()

	// create db with correct config
	dbConfig := rt.NewDbConfig()
	resp, err := rt.CreateDatabase("db1", dbConfig)
	require.NoError(t, err)
	RequireStatus(t, resp, http.StatusCreated)

	// wait for db to come online
	require.NoError(t, rt.WaitForDBOnline())
	badName := tb1.GetName()
	dbConfig.Bucket = &badName

	// assert request fails
	resp, err = rt.ReplaceDbConfig("db1", dbConfig)
	require.NoError(t, err)
	RequireStatus(t, resp, http.StatusNotFound)
}

// TestMultipleBucketWithBadDbConfigScenario1:
//   - in bucketA and bucketB, write two db configs with bucket name as bucketC
//   - Start new rest tester and ensure they aren't picked up as valid configs
func TestMultipleBucketWithBadDbConfigScenario1(t *testing.T) {
	base.TestsRequireBootstrapConnection(t)
	base.RequireNumTestBuckets(t, 3)
	tb1 := base.GetTestBucket(t)
	defer tb1.Close()
	tb2 := base.GetTestBucket(t)
	defer tb2.Close()
	tb3 := base.GetTestBucket(t)
	defer tb3.Close()

	const groupID = "60ce5544-c368-4b08-b0ed-4ca3b37973f9"

	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket:       tb1,
		persistentConfig: true,
		/*MutateStartupConfig: func(config *StartupConfig) {
			// all RestTesters all this test must use the same config ID
			config.Bootstrap.ConfigGroupID = groupID
		},*/
	})
	defer rt1.Close()

	// create a db config that has bucket C in the config and persist to rt1 bucket
	dbConfig := rt1.NewDbConfig()
	dbConfig.Name = "db1"
	rt1.PersistDbConfigToBucket(dbConfig, tb3.GetName())

	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket:       tb2,
		persistentConfig: true,
		/*MutateStartupConfig: func(config *StartupConfig) {
			// configure same config groupID
			config.Bootstrap.ConfigGroupID = groupID
		},
		*/
	})
	defer rt2.Close()

	// create a db config that has bucket C in the config and persist to rt2 bucket
	dbConfig = rt2.NewDbConfig()
	dbConfig.Name = "db1"
	rt2.PersistDbConfigToBucket(dbConfig, tb3.GetName())

	rt3 := NewRestTester(t, &RestTesterConfig{
		persistentConfig: true,
		TestBucket:       tb3,
		groupID:          base.StringPtr(groupID),
		/*MutateStartupConfig: func(config *StartupConfig) {
			// configure the interval time to pick up new configs from the bucket to every 1 seconds
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(1 * time.Second)
		},
		*/
	})
	defer rt3.Close()

	// assert the invalid database is picked up with new rest tester
	err := rt3.WaitForConditionWithOptions(func() bool {
		invalidDatabases := rt3.ServerContext().AllInvalidDatabases()
		return len(invalidDatabases) == 1
	}, 200, 1000)
	require.NoError(t, err)

	// assert that there are no valid db configs on the server context
	err = rt3.WaitForConditionWithOptions(func() bool {
		databaseNames := rt3.ServerContext().AllDatabaseNames()
		return len(databaseNames) == 0
	}, 200, 1000)
	require.NoError(t, err)

	// assert a request to the db fails with correct error message
	resp := rt3.SendAdminRequest(http.MethodGet, "/db1/_config", "")
	RequireStatus(t, resp, http.StatusNotFound)
	assert.Contains(t, resp.Body.String(), "Must update database config immediately")
}

// TestMultipleBucketWithBadDbConfigScenario2:
//   - create bucketA and bucketB with db configs that that both list bucket name as bucketA
//   - start a new rest tester and assert that invalid db config is picked up and the valid one is also picked up
func TestMultipleBucketWithBadDbConfigScenario2(t *testing.T) {
	base.TestsRequireBootstrapConnection(t)

	base.RequireNumTestBuckets(t, 3)
	tb1 := base.GetTestBucket(t)
	defer tb1.Close()
	tb2 := base.GetTestBucket(t)
	defer tb2.Close()

	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket:       tb1,
		persistentConfig: true,
		groupID:          base.StringPtr("60ce5544-c368-4b08-b0ed-4ca3b37973f9"),
	})
	// create a db config pointing to bucket C and persist to bucket A
	dbConfig := rt1.NewDbConfig()
	dbConfig.Name = "db1"
	rt1.PersistDbConfigToBucket(dbConfig, rt1.TestBucket.GetName())
	defer rt1.Close()

	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket:       tb2,
		persistentConfig: true,
		groupID:          base.StringPtr("60ce5544-c368-4b08-b0ed-4ca3b37973f9"),
	})
	defer rt2.Close()

	// create a db config pointing to bucket C and persist to bucket B
	dbConfig = rt2.NewDbConfig()
	dbConfig.Name = "db1"
	rt2.PersistDbConfigToBucket(dbConfig, "badName")

	rt3 := NewRestTester(t, &RestTesterConfig{
		persistentConfig: true,
		groupID:          base.StringPtr("60ce5544-c368-4b08-b0ed-4ca3b37973f9"),
	})
	defer rt3.Close()

	// assert that the invalid config is picked up by the new rest tester
	err := rt3.WaitForConditionWithOptions(func() bool {
		invalidDatabases := rt3.ServerContext().AllInvalidDatabases()
		return len(invalidDatabases) == 1
	}, 200, 1000)
	require.NoError(t, err)

	// assert that there is a valid database picked up as the invalid configs have this rest tester backing bucket
	err = rt3.WaitForConditionWithOptions(func() bool {
		validDatabase := rt3.ServerContext().AllDatabases()
		return len(validDatabase) == 1
	}, 200, 1000)
	require.NoError(t, err)
}

// TestMultipleBucketWithBadDbConfigScenario3:
//   - create a rest tester
//   - create a db on the rest tester
//   - persist that db config to another bucket
//   - assert that is picked up as an invalid db config
func TestMultipleBucketWithBadDbConfigScenario3(t *testing.T) {
	base.TestsRequireBootstrapConnection(t)

	tb1 := base.GetTestBucket(t)
	defer tb1.Close()
	tb2 := base.GetTestBucket(t)
	defer tb2.Close()

	rt := NewRestTester(t, &RestTesterConfig{
		TestBucket:       tb1,
		persistentConfig: true,
		/*MutateStartupConfig: func(config *StartupConfig) {
			// configure the interval time to pick up new configs from the bucket to every 1 seconds
			config.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(1 * time.Second)
		},
		*/
	})
	defer rt.Close()

	// create a new db
	dbConfig := rt.NewDbConfig()
	dbConfig.Name = "db1"
	dbConfig.BucketConfig.Bucket = base.StringPtr(rt.TestBucket.GetName())
	resp, err := rt.CreateDatabase("db1", dbConfig)
	require.NoError(t, err)
	RequireStatus(t, resp, http.StatusCreated)

	// persistence logic construction
	version, err := GenerateDatabaseConfigVersionID("", &dbConfig)
	require.NoError(t, err)

	badName := "badName"
	dbConfig.Bucket = &badName
	persistedConfig := DatabaseConfig{
		Version:  version,
		DbConfig: dbConfig,
	}
	// add the config to the other bucket
	rt.InsertDbConfigToBucket(&persistedConfig, tb2.GetName())

	// assert the config is picked as invalid db config
	err = rt.WaitForConditionWithOptions(func() bool {
		invalidDatabases := rt.ServerContext().AllInvalidDatabases()
		return len(invalidDatabases) == 1
	}, 200, 1000)
	require.NoError(t, err)
}

func (rt *RestTester) PersistDbConfigToBucket(dbConfig DbConfig, bucketName string) {

	dbConfig.Bucket = &bucketName
	persistedConfig := DatabaseConfig{
		DbConfig: dbConfig,
	}
	rt.InsertDbConfigToBucket(&persistedConfig, rt.TestBucket.GetName())
}

func (rt *RestTester) InsertDbConfigToBucket(config *DatabaseConfig, bucketName string) {
	_, insertErr := rt.ServerContext().bootstrapContext.connection.InsertConfig(bucketName, rt.ServerContext().config.Bootstrap.ConfigGroupID, config)
	require.NoError(rt.tb, insertErr)
}

// UpdatePersistedBucketName will update the persisted config bucket name to name specified in parameters
func (rt *RestTester) UpdatePersistedBucketName(dbConfig *DatabaseConfig, newBucketName *string) (*DatabaseConfig, error) {
	updatedDbConfig := DatabaseConfig{}
	_, err := rt.ServerContext().bootstrapContext.connection.UpdateConfig(*dbConfig.Bucket, rt.ServerContext().config.Bootstrap.ConfigGroupID, func(rawBucketConfig []byte) (newConfig []byte, err error) {
		var bucketDbConfig DatabaseConfig
		if err := base.JSONUnmarshal(rawBucketConfig, &bucketDbConfig); err != nil {
			return nil, err
		}

		bucketDbConfig = *dbConfig
		bucketDbConfig.Bucket = newBucketName

		return base.JSONMarshal(bucketDbConfig)
	})
	return &updatedDbConfig, err
}
