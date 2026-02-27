/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"text/template"
	"time"

	"github.com/couchbase/go-blip"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
)

// Testing utilities that have been included in the rest package so that they
// are available to any package that imports rest.  (if they were in a _test.go
// file, they wouldn't be publicly exported to other packages)

// RestTesterConfig represents configuration for sync gateway
type RestTesterConfig struct {
	GuestEnabled                    bool                        // If this is true, Admin Party is in full effect
	SyncFn                          string                      // put the sync() function source in here (optional)
	ImportFilter                    string                      // put the import filter function source in here (optional)
	DatabaseConfig                  *DatabaseConfig             // Supports additional config options.  BucketConfig, Name, Sync, Unsupported will be ignored (overridden)
	MutateStartupConfig             func(config *StartupConfig) // Function to mutate the startup configuration before the server context gets created. This overrides options the RT sets.
	InitSyncSeq                     uint64                      // If specified, initializes _sync:seq on bucket creation.  Not supported when running against walrus
	AllowConflicts                  bool                        // Enable conflicts mode.  By default, conflicts will not allowed
	EnableUserQueries               bool                        // Enable the feature-flag for user N1QL/etc queries
	CustomTestBucket                *base.TestBucket            // If set, use this bucket instead of requesting a new one.
	LeakyBucketConfig               *base.LeakyBucketConfig     // Set to create and use a leaky bucket on the RT and DB. A test bucket cannot be passed in if using this option.
	adminInterface                  string                      // adminInterface overrides the default admin interface.
	SgReplicateEnabled              bool                        // SgReplicateManager disabled by default for RestTester
	AutoImport                      *bool
	HideProductInfo                 bool
	AdminInterfaceAuthentication    bool
	metricsInterfaceAuthentication  bool
	enableAdminAuthPermissionsCheck bool
	useTLSServer                    bool // If true, TLS will be required for communications with CBS. Default: false
	PersistentConfig                bool
	GroupID                         *string
	serverless                      bool // Runs SG in serverless mode. Must be used in conjunction with persistent config
	collectionConfig                collectionConfiguration
	numCollections                  int
	syncGatewayVersion              *base.ComparableBuildVersion // alternate version of Sync Gateway to use on startup
	allowDbConfigEnvVars            *bool
	maxConcurrentRevs               *int
	UseXattrConfig                  bool
}

type collectionConfiguration uint8

const (
	useSingleCollection = iota
	useSingleCollectionDefaultOnly
	useMultipleCollection
)

var defaultTestingCORSOrigin = []string{"http://example.com", "*", "http://staging.example.com"}

// globalBlipTesterClients stores the active blip tester clients to ensure they are cleaned up at the end of a test
var globalBlipTesterClients *activeBlipTesterClients

func init() {
	globalBlipTesterClients = &activeBlipTesterClients{m: make(map[string]int32), lock: sync.Mutex{}}
}

// activeBlipTesterClients tracks the number of active blip tester clients to make sure they are closed at the end of a test and goroutines are not leaked.
type activeBlipTesterClients struct {
	m    map[string]int32
	lock sync.Mutex
}

// add increments the count of a blip tester client for a particular test
func (a *activeBlipTesterClients) add(name string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.m[name]++
}

// remove decrements the count of a blip tester client for a particular test
func (a *activeBlipTesterClients) remove(tb testing.TB, name string) {
	a.lock.Lock()
	defer a.lock.Unlock()
	require.Contains(tb, a.m, name, "Can not remove blip tester client '%s' that was never added", name)
	a.m[name]--
	if a.m[name] == 0 {
		delete(a.m, name)
	}
}

// RestTester provides a fake server for testing endpoints
type RestTester struct {
	*RestTesterConfig
	testingTB               atomic.Pointer[testing.TB]
	TestBucket              *base.TestBucket
	RestTesterServerContext *ServerContext
	AdminHandler            http.Handler
	adminHandlerOnce        sync.Once
	PublicHandler           http.Handler
	publicHandlerOnce       sync.Once
	MetricsHandler          http.Handler
	metricsHandlerOnce      sync.Once
	DiagnosticHandler       http.Handler
	diagnosticHandlerOnce   sync.Once
	closed                  bool
}

func (rt *RestTester) TB() testing.TB {
	return *rt.testingTB.Load()
}

// RestTesterDefaultUserPassword is usable as a default password for SendUserRequest
const RestTesterDefaultUserPassword = "letmein"

// NewRestTester returns a rest tester and corresponding keyspace backed by a single database and a single collection. This collection may be named or default collection based on global test configuration.
func NewRestTester(tb testing.TB, restConfig *RestTesterConfig) *RestTester {
	return newRestTester(tb, restConfig, useSingleCollection, 1)
}

// NewRestTesterPersistentConfigNoDB returns a rest tester with persistent config setup and no database. A convenience function for NewRestTester.
func NewRestTesterPersistentConfigNoDB(tb testing.TB) *RestTester {
	config := &RestTesterConfig{
		PersistentConfig: true,
	}
	rt := newRestTester(tb, config, useSingleCollection, 1)
	return rt
}

// NewRestTesterPersistentConfig returns a rest tester with persistent config setup and a single database. A convenience function for NewRestTester.
func NewRestTesterPersistentConfig(tb testing.TB) *RestTester {
	rt := NewRestTesterPersistentConfigNoDB(tb)
	RequireStatus(tb, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)
	return rt
}

// newRestTester creates the underlying rest testers, use public functions.
func newRestTester(tb testing.TB, restConfig *RestTesterConfig, collectionConfig collectionConfiguration, numCollections int) *RestTester {
	var rt RestTester
	if tb == nil {
		panic("tester parameter cannot be nil")
	}
	rt.testingTB.Store(&tb)
	if restConfig != nil {
		rt.RestTesterConfig = restConfig
	} else {
		rt.RestTesterConfig = &RestTesterConfig{}
	}
	rt.RestTesterConfig.collectionConfig = collectionConfig
	rt.RestTesterConfig.numCollections = numCollections
	rt.RestTesterConfig.useTLSServer = base.ServerIsTLS(base.UnitTestUrl())
	return &rt
}

// NewRestTesterDefaultCollection creates a rest tester backed by a single database and a single _default._default collection.
func NewRestTesterDefaultCollection(tb testing.TB, restConfig *RestTesterConfig) *RestTester {
	return newRestTester(tb, restConfig, useSingleCollectionDefaultOnly, 1)
}

// NewRestTesterMultipleCollections creates a rest tester backed by a single database with the specified number of collections.
func NewRestTesterMultipleCollections(tb testing.TB, restConfig *RestTesterConfig, numCollections int) *RestTester {
	if !base.TestsUseNamedCollections() {
		tb.Skip("This test requires named collections and is running against a bucket type that does not support them")
	}
	if numCollections == 0 {
		tb.Errorf("0 is not a valid number of collections to specify")
	}
	return newRestTester(tb, restConfig, useMultipleCollection, numCollections)
}

func (rt *RestTester) Bucket() base.Bucket {
	if rt.TB() == nil {
		panic("RestTester not properly initialized please use NewRestTester function")
	} else if rt.closed {
		panic("RestTester was closed!")
	}

	if rt.TestBucket != nil {
		return rt.TestBucket.Bucket
	}

	// If we have a TestBucket defined on the RestTesterConfig, use that instead of requesting a new one.
	testBucket := rt.RestTesterConfig.CustomTestBucket
	if testBucket == nil {
		testBucket = base.GetTestBucket(rt.TB())
		if rt.LeakyBucketConfig != nil {
			leakyConfig := *rt.LeakyBucketConfig
			// Ignore closures to avoid double closing panics
			leakyConfig.IgnoreClose = true
			testBucket = testBucket.LeakyBucketClone(leakyConfig)
		}
	} else if rt.LeakyBucketConfig != nil {
		rt.TB().Fatalf("A passed in TestBucket cannot be used on the RestTester when defining a LeakyBucketConfig")
	}
	rt.TestBucket = testBucket

	if rt.PersistentConfig != false {
		require.Zero(rt.TB(), rt.InitSyncSeq, "RestTesterConfig.InitSyncSeq is not supported with RestTesterConfig.PersistentConfig = true")
	}

	corsConfig := &auth.CORSConfig{
		Origin:      defaultTestingCORSOrigin,
		LoginOrigin: []string{"http://example.com"},
		Headers:     []string{},
		MaxAge:      1728000,
	}

	adminInterface := &DefaultAdminInterface
	if rt.RestTesterConfig.adminInterface != "" {
		adminInterface = &rt.RestTesterConfig.adminInterface
	}

	sc := DefaultStartupConfig("")

	username, password, _ := testBucket.BucketSpec.Auth.GetCredentials()

	// Disable config polling to avoid test flakiness and increase control of timing.
	// Rely on on-demand config fetching for consistency.
	sc.Bootstrap.ConfigUpdateFrequency = base.NewConfigDuration(0)

	sc.Bootstrap.Server = testBucket.BucketSpec.Server
	sc.Bootstrap.Username = username
	sc.Bootstrap.Password = password
	sc.API.AdminInterface = *adminInterface
	sc.API.CORS = corsConfig
	sc.API.HideProductVersion = base.Ptr(rt.RestTesterConfig.HideProductInfo)
	sc.DeprecatedConfig = &DeprecatedConfig{Facebook: &FacebookConfigLegacy{}}
	sc.API.AdminInterfaceAuthentication = &rt.AdminInterfaceAuthentication
	sc.API.MetricsInterfaceAuthentication = &rt.metricsInterfaceAuthentication
	sc.API.EnableAdminAuthenticationPermissionsCheck = &rt.enableAdminAuthPermissionsCheck
	sc.Bootstrap.UseTLSServer = &rt.RestTesterConfig.useTLSServer
	sc.Bootstrap.ServerTLSSkipVerify = base.Ptr(base.TestTLSSkipVerify())
	sc.Unsupported.Serverless.Enabled = &rt.serverless
	sc.Unsupported.AllowDbConfigEnvVars = rt.RestTesterConfig.allowDbConfigEnvVars
	sc.Unsupported.UseXattrConfig = &rt.UseXattrConfig
	sc.Replicator.MaxConcurrentRevs = rt.RestTesterConfig.maxConcurrentRevs
	if rt.serverless {
		if !rt.PersistentConfig {
			rt.TB().Fatalf("Persistent config must be used when running in serverless mode")
		}
		sc.BucketCredentials = map[string]*base.CredentialsConfig{
			testBucket.GetName(): {
				Username: base.TestClusterUsername(),
				Password: base.TestClusterPassword(),
			},
		}
	}

	if rt.RestTesterConfig.GroupID != nil {
		sc.Bootstrap.ConfigGroupID = *rt.RestTesterConfig.GroupID
	} else if rt.RestTesterConfig.PersistentConfig {
		// If running in persistent config mode, the database has to be manually created. If the db name is the same as a
		// past tests db name, a db already exists error could happen if the past tests bucket is still flushing. Prevent this
		// by using a unique group ID for each new rest tester.
		uniqueUUID, err := uuid.NewRandom()
		if err != nil {
			rt.TB().Fatalf("Could not generate random config group ID UUID: %v", err)
		}
		sc.Bootstrap.ConfigGroupID = uniqueUUID.String()
	}

	sc.Unsupported.UserQueries = base.Ptr(rt.EnableUserQueries)

	if rt.MutateStartupConfig != nil {
		rt.MutateStartupConfig(&sc)
	}

	sc.Unsupported.UserQueries = base.Ptr(rt.EnableUserQueries)

	// Allow EE-only config even in CE for testing using group IDs.
	require.NoError(rt.TB(), sc.Validate(base.TestCtx(rt.TB()), true))

	// Post-validation, we can lower the bcrypt cost beyond SG limits to reduce test runtime.
	sc.Auth.BcryptCost = bcrypt.MinCost

	rt.RestTesterServerContext = NewServerContext(base.TestCtx(rt.TB()), &sc, rt.RestTesterConfig.PersistentConfig)
	rt.RestTesterServerContext.allowScopesInPersistentConfig = true
	if rt.RestTesterConfig.syncGatewayVersion != nil {
		rt.RestTesterServerContext.BootstrapContext.sgVersion = *rt.RestTesterConfig.syncGatewayVersion
	}
	ctx := rt.Context()

	if !base.ServerIsWalrus(sc.Bootstrap.Server) {
		// Copy any testbucket cert info into boostrap server config
		// Required as present for X509 tests there is no way to pass this info to the bootstrap server context with a
		// RestTester directly - Should hopefully be alleviated by CBG-1460
		sc.Bootstrap.CACertPath = testBucket.BucketSpec.CACertPath
		sc.Bootstrap.X509CertPath = testBucket.BucketSpec.Certpath
		sc.Bootstrap.X509KeyPath = testBucket.BucketSpec.Keypath

		rt.TestBucket.BucketSpec.TLSSkipVerify = base.TestTLSSkipVerify()
		require.NoError(rt.TB(), rt.RestTesterServerContext.initializeGocbAdminConnection(ctx))
	}
	require.NoError(rt.TB(), rt.RestTesterServerContext.initializeBootstrapConnection(ctx))

	// Copy this startup config at this point into initial startup config
	require.NoError(rt.TB(), base.DeepCopyInefficient(&rt.RestTesterServerContext.initialStartupConfig, &sc))

	// tests must create their own databases in persistent mode
	if !rt.PersistentConfig {
		useXattrs := base.TestUseXattrs()

		if rt.DatabaseConfig == nil {
			// If no db config was passed in, create one
			rt.DatabaseConfig = &DatabaseConfig{}
		}
		if rt.DatabaseConfig.UseViews == nil {
			rt.DatabaseConfig.UseViews = base.Ptr(base.TestsDisableGSI())
		}
		if base.TestsUseNamedCollections() && rt.collectionConfig != useSingleCollectionDefaultOnly && (rt.DatabaseConfig.useGSI() || base.UnitTestUrlIsWalrus()) {
			// If scopes is already set, assume the caller has a plan
			if rt.DatabaseConfig.Scopes == nil {
				// Configure non default collections by default
				rt.DatabaseConfig.Scopes = GetCollectionsConfigWithFiltering(rt.TB(), testBucket, rt.numCollections, stringPtrOrNil(rt.SyncFn), stringPtrOrNil(rt.ImportFilter))
			}
		} else {
			// override SyncFn and ImportFilter if set
			if rt.SyncFn != "" {
				rt.DatabaseConfig.Sync = &rt.SyncFn
			}
			if rt.ImportFilter != "" {
				rt.DatabaseConfig.ImportFilter = &rt.ImportFilter
			}
		}

		// numReplicas set to 0 for test buckets, since it should assume that there may only be one indexing node.
		if rt.DatabaseConfig.Index == nil {
			rt.DatabaseConfig.Index = &IndexConfig{}
		}
		rt.DatabaseConfig.Index.NumReplicas = base.Ptr(uint(0))

		rt.DatabaseConfig.Bucket = &testBucket.BucketSpec.BucketName
		rt.DatabaseConfig.Username = username
		rt.DatabaseConfig.Password = password
		rt.DatabaseConfig.CACertPath = testBucket.BucketSpec.CACertPath
		rt.DatabaseConfig.CertPath = testBucket.BucketSpec.Certpath
		rt.DatabaseConfig.KeyPath = testBucket.BucketSpec.Keypath
		if rt.DatabaseConfig.Name == "" {
			rt.DatabaseConfig.Name = "db"
		}
		rt.DatabaseConfig.EnableXattrs = &useXattrs
		if rt.AllowConflicts {
			rt.DatabaseConfig.AllowConflicts = base.Ptr(true)
		}
		if rt.DatabaseConfig.StoreLegacyRevTreeData == nil {
			rt.DatabaseConfig.StoreLegacyRevTreeData = base.Ptr(db.DefaultStoreLegacyRevTreeData)
		}

		rt.DatabaseConfig.SGReplicateEnabled = base.Ptr(rt.RestTesterConfig.SgReplicateEnabled)

		if base.TestDisableRevCache() {
			if rt.DatabaseConfig.CacheConfig == nil {
				rt.DatabaseConfig.CacheConfig = &CacheConfig{}
			}
			if rt.DatabaseConfig.CacheConfig.RevCacheConfig == nil {
				rt.DatabaseConfig.CacheConfig.RevCacheConfig = &RevCacheConfig{}
			}
			rt.DatabaseConfig.CacheConfig.RevCacheConfig.MaxItemCount = base.Ptr[uint32](0)
		}

		// Check for override of AutoImport in the rt config
		if rt.AutoImport != nil {
			rt.DatabaseConfig.AutoImport = *rt.AutoImport
		}
		autoImport, _ := rt.DatabaseConfig.AutoImportEnabled(ctx)
		if rt.DatabaseConfig.ImportPartitions == nil && base.TestUseXattrs() && base.IsEnterpriseEdition() && autoImport {
			// Speed up test setup - most tests don't need more than one partition given we only have one node
			rt.DatabaseConfig.ImportPartitions = base.Ptr(uint16(1))
		}
		if rt.InitSyncSeq > 0 {
			metadataKeys := base.DefaultMetadataKeys
			syncSeqKey := metadataKeys.SyncSeqKey()
			base.InfofCtx(ctx, base.KeySGTest, "Initializing %s to %d", syncSeqKey, rt.InitSyncSeq)
			_, err := rt.TestBucket.GetMetadataStore().Incr(syncSeqKey, 0, rt.InitSyncSeq, 0)
			require.NoError(rt.TB(), err)
		}
		_, isLeaky := base.AsLeakyBucket(rt.TestBucket)
		var err error
		if rt.LeakyBucketConfig != nil || isLeaky {
			_, err = rt.RestTesterServerContext.AddDatabaseFromConfigWithBucket(ctx, rt.TB(), *rt.DatabaseConfig, testBucket.Bucket)
		} else {
			_, err = rt.RestTesterServerContext.AddDatabaseFromConfig(ctx, *rt.DatabaseConfig)
		}
		require.NoError(rt.TB(), err)
		ctx = rt.Context() // get new ctx with db info before passing it down

		// Update the testBucket Bucket to the one associated with the database context.  The new (dbContext) bucket
		// will be closed when the rest tester closes the server context. The original bucket will be closed using the
		// testBucket's closeFn
		rt.TestBucket.Bucket = rt.RestTesterServerContext.Database(ctx, rt.DatabaseConfig.Name).Bucket

		if rt.DatabaseConfig.Guest == nil {
			rt.SetAdminParty(rt.GuestEnabled)
		}
	}

	// PostStartup (without actually waiting 5 seconds)
	close(rt.RestTesterServerContext.hasStarted)
	return rt.TestBucket.Bucket
}

// MetadataStore returns the datastore for the database on the RestTester
func (rt *RestTester) MetadataStore() base.DataStore {
	return rt.GetDatabase().MetadataStore
}

// GetCollectionsConfig sets up a ScopesConfig from a TestBucket for use with non default collections.
func GetCollectionsConfig(t testing.TB, testBucket *base.TestBucket, numCollections int) ScopesConfig {
	return GetCollectionsConfigWithFiltering(t, testBucket, numCollections, nil, nil)
}

// GetCollectionsConfigWithFiltering sets up a ScopesConfig from a TestBucket for use with non default collections. The sync function will be passed for all collections.
func GetCollectionsConfigWithFiltering(t testing.TB, testBucket *base.TestBucket, numCollections int, syncFn *string, importFilter *string) ScopesConfig {
	// Get a datastore as provided by the test
	stores := testBucket.GetNonDefaultDatastoreNames()
	require.True(t, len(stores) >= numCollections, "Requested more collections %d than found on testBucket %d", numCollections, len(stores))
	defaultCollectionConfig := &CollectionConfig{}
	if syncFn != nil {
		defaultCollectionConfig.SyncFn = syncFn
	}
	if importFilter != nil {
		defaultCollectionConfig.ImportFilter = importFilter
	}

	scopesConfig := ScopesConfig{}
	for i := range numCollections {
		dataStoreName := stores[i]
		if scopeConfig, ok := scopesConfig[dataStoreName.ScopeName()]; ok {
			if _, ok := scopeConfig.Collections[dataStoreName.CollectionName()]; ok {
				// already present
			} else {
				scopeConfig.Collections[dataStoreName.CollectionName()] = defaultCollectionConfig
			}
		} else {
			scopesConfig[dataStoreName.ScopeName()] = ScopeConfig{
				Collections: map[string]*CollectionConfig{
					dataStoreName.CollectionName(): defaultCollectionConfig,
				}}
		}

	}
	return scopesConfig
}

// GetSingleDataStoreNamesFromScopes config returns a lexically sorted list of configured datastores.
func GetDataStoreNamesFromScopesConfig(config ScopesConfig) []sgbucket.DataStoreName {
	var names []string
	for scopeName, scopeConfig := range config {
		for collectionName, _ := range scopeConfig.Collections {
			names = append(names, fmt.Sprintf("%s%s%s", scopeName, base.ScopeCollectionSeparator, collectionName))
		}

	}
	sort.Strings(names)
	var dataStoreNames []sgbucket.DataStoreName
	for _, scopeAndCollection := range names {
		keyspace := strings.Split(scopeAndCollection, base.ScopeCollectionSeparator)
		dataStoreNames = append(dataStoreNames, base.ScopeAndCollectionName{Scope: keyspace[0], Collection: keyspace[1]})
	}
	return dataStoreNames

}

// LeakyBucket gets the bucket from the RestTester as a leaky bucket allowing for callbacks to be set on the fly.
// The RestTester must have been set up to create and use a leaky bucket by setting LeakyBucketConfig in the RT
// config when calling NewRestTester.
func (rt *RestTester) LeakyBucket() *base.LeakyDataStore {
	if rt.LeakyBucketConfig == nil {
		rt.TB().Fatalf("Cannot get leaky bucket when LeakyBucketConfig was not set on RestTester initialisation")
	}
	leakyDataStore, ok := base.AsLeakyDataStore(rt.Bucket().DefaultDataStore())
	if !ok {
		rt.TB().Fatalf("Could not get bucket (type %T) as a leaky bucket", rt.Bucket())
	}
	return leakyDataStore
}

func (rt *RestTester) ServerContext() *ServerContext {
	rt.Bucket()
	return rt.RestTesterServerContext
}

// CreateDatabase is a utility function to create a database through the REST API
func (rt *RestTester) CreateDatabase(dbName string, config DbConfig) (resp *TestResponse) {
	dbcJSON, err := base.JSONMarshal(config)
	require.NoError(rt.TB(), err)
	if rt.AdminInterfaceAuthentication {
		resp = rt.SendAdminRequestWithAuth(http.MethodPut, fmt.Sprintf("/%s/", dbName), string(dbcJSON), base.TestClusterUsername(), base.TestClusterPassword())
	} else {
		resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/", dbName), string(dbcJSON))
	}
	return resp
}

// ReplaceDbConfig is a utility function to replace a database config through the REST API
func (rt *RestTester) ReplaceDbConfig(dbName string, config DbConfig) *TestResponse {
	dbcJSON, err := base.JSONMarshal(config)
	require.NoError(rt.TB(), err)
	resp := rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_config", dbName), string(dbcJSON))
	return resp
}

// UpsertDbConfig is a utility function to upsert a database through the REST API
func (rt *RestTester) UpsertDbConfig(dbName string, config DbConfig) *TestResponse {
	dbcJSON, err := base.JSONMarshal(config)
	require.NoError(rt.TB(), err)
	resp := rt.SendAdminRequest(http.MethodPost, fmt.Sprintf("/%s/_config", dbName), string(dbcJSON))
	return resp
}

// GetDatabase Returns a database found for server context, if there is only one database. Fails the test harness if there is not a database defined.
func (rt *RestTester) GetDatabase() *db.DatabaseContext {
	require.Len(rt.TB(), rt.ServerContext().AllDatabases(), 1)
	for _, database := range rt.ServerContext().AllDatabases() {
		return database
	}
	return nil
}

// CreateUser creates a user with the default password and channels scoped to a single test collection.
func (rt *RestTester) CreateUser(username string, channels []string, roles ...string) {
	var response *TestResponse
	if rt.AdminInterfaceAuthentication {
		response = rt.SendAdminRequestWithAuth(http.MethodPut, "/{{.db}}/_user/"+username, GetUserPayload(rt.TB(), "", RestTesterDefaultUserPassword, "", rt.GetSingleDataStore(), channels, roles), base.TestClusterUsername(), base.TestClusterPassword())
	} else {
		response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/"+username, GetUserPayload(rt.TB(), "", RestTesterDefaultUserPassword, "", rt.GetSingleDataStore(), channels, roles))
	}
	RequireStatus(rt.TB(), response, http.StatusCreated)
}

// CreateRole creates a role with channels scoped to a single test collection.
func (rt *RestTester) CreateRole(rolename string, channels []string) {
	var response *TestResponse
	if rt.AdminInterfaceAuthentication {
		response = rt.SendAdminRequestWithAuth(http.MethodPut, "/{{.db}}/_role/"+rolename, GetRolePayload(rt.TB(), rolename, rt.GetSingleDataStore(), channels), base.TestClusterUsername(), base.TestClusterPassword())
	} else {
		response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_role/"+rolename, GetRolePayload(rt.TB(), rolename, rt.GetSingleDataStore(), channels))
	}
	RequireStatus(rt.TB(), response, http.StatusCreated)
}

func (rt *RestTester) GetUserAdminAPI(username string) auth.PrincipalConfig {
	response := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_user/"+username, "")
	RequireStatus(rt.TB(), response, http.StatusOK)
	var responseConfig auth.PrincipalConfig
	err := json.Unmarshal(response.Body.Bytes(), &responseConfig)
	require.NoError(rt.TB(), err)
	return responseConfig
}

// GetSingleTestDatabaseCollection will return a DatabaseCollection if there is only one. Depending on test environment configuration, it may or may not be the default collection.
func (rt *RestTester) GetSingleTestDatabaseCollection() (*db.DatabaseCollection, context.Context) {
	c := db.GetSingleDatabaseCollection(rt.TB(), rt.GetDatabase())
	ctx := base.UserLogCtx(c.AddCollectionContext(rt.Context()), "gotest", base.UserDomainBuiltin, nil)
	return c, ctx
}

// GetSingleTestDatabaseCollectionWithUser will return a DatabaseCollection if there is only one. Depending on test environment configuration, it may or may not be the default collection.
func (rt *RestTester) GetSingleTestDatabaseCollectionWithUser() (*db.DatabaseCollectionWithUser, context.Context) {
	c, ctx := rt.GetSingleTestDatabaseCollection()
	return &db.DatabaseCollectionWithUser{DatabaseCollection: c}, ctx
}

// GetSingleDataStore will return a datastore if there is only one collection configured on the RestTester database.
func (rt *RestTester) GetSingleDataStore() base.DataStore {
	collection, _ := rt.GetSingleTestDatabaseCollection()
	ds, err := rt.GetDatabase().Bucket.NamedDataStore(base.ScopeAndCollectionName{
		Scope:      collection.ScopeName,
		Collection: collection.Name,
	})
	require.NoError(rt.TB(), err)
	return ds
}

// WaitForDoc will wait for the specific docID to be available in the change cache by comparing the sequence number in the bucket to latest sequence processed by channel cache. Consider replacing with WaitForPendingChanges.
func (rt *RestTester) WaitForDoc(docid string) {
	seq := rt.SequenceForDoc(docid)
	rt.WaitForSequence(seq)
}

// SequenceForDoc returns the current sequence for a document from the bucket, failing the test if the document doesn't exist.
func (rt *RestTester) SequenceForDoc(docid string) (seq uint64) {
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	doc, err := collection.GetDocument(ctx, docid, db.DocUnmarshalAll)
	require.NoError(rt.TB(), err, "Error getting doc %q", docid)
	return doc.Sequence
}

// WaitForSequence waits for the sequence to be buffered by the channel cache
func (rt *RestTester) WaitForSequence(seq uint64) {
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	require.NoError(rt.TB(), collection.WaitForSequence(ctx, seq))
}

// WaitForPendingChanges waits all outstanding changes to be buffered by the channel cache.
func (rt *RestTester) WaitForPendingChanges() {
	ctx := rt.Context()
	for _, collection := range rt.GetDbCollections() {
		require.NoError(rt.TB(), collection.WaitForPendingChanges(ctx))
	}
}

// SetAdminParty toggles the guest user between disabled and enabled.  If enabled, the guest user is given access to the UserStar channel on all collections.
func (rt *RestTester) SetAdminParty(partyTime bool) {
	ctx := rt.Context()
	a := rt.GetDatabase().Authenticator(ctx)
	guest, err := a.GetUser("")
	require.NoError(rt.TB(), err)
	guest.SetDisabled(!partyTime)
	var chans channels.TimedSet
	if partyTime {
		chans = channels.AtSequence(base.SetOf(channels.UserStarChannel), 1)
	}

	if len(a.Collections) == 0 {
		guest.SetExplicitChannels(chans, 1)
	} else {
		for scopeName, scope := range a.Collections {
			for collectionName := range scope {
				guest.SetCollectionExplicitChannels(scopeName, collectionName, chans, 1)
			}
		}
	}
	require.NoError(rt.TB(), a.Save(guest))
}

func (rt *RestTester) Close() {
	if rt.TB() == nil {
		panic("RestTester not properly initialized please use NewRestTester function")
	}
	ctx := rt.Context() // capture ctx before closing rt
	rt.closed = true
	if rt.RestTesterServerContext != nil {
		rt.RestTesterServerContext.Close(ctx)
	}
	if rt.TestBucket != nil {
		rt.TestBucket.Close(ctx)
		rt.TestBucket = nil
	}
}

func (rt *RestTester) SendRequest(method, resource string, body string) *TestResponse {
	return rt.Send(Request(method, rt.mustTemplateResource(resource), body))
}

func (rt *RestTester) SendRequestWithHeaders(method, resource string, body string, headers map[string]string) *TestResponse {
	req := Request(method, rt.mustTemplateResource(resource), body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return rt.Send(req)
}

func (rt *RestTester) SendUserRequestWithHeaders(method, resource string, body string, headers map[string]string, username string, password string) *TestResponse {
	req := Request(method, rt.mustTemplateResource(resource), body)
	req.SetBasicAuth(username, password)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return rt.Send(req)
}

// templateResource is a non-fatal version of rt.mustTemplateResource
func (rt *RestTester) templateResource(resource string) (string, error) {
	tmpl, err := template.New("urltemplate").
		Option("missingkey=error").
		Parse(resource)
	if err != nil {
		return "", err
	}

	data := make(map[string]string)
	require.NotNil(rt.TB(), rt.ServerContext())
	if rt.ServerContext() != nil {
		databases := rt.ServerContext().AllDatabases()
		var dbNames []string
		for dbName := range databases {
			dbNames = append(dbNames, dbName)
		}
		sort.Strings(dbNames)
		multipleDatabases := len(dbNames) > 1
		for i, dbName := range dbNames {
			database := databases[dbName]
			dbPrefix := ""
			if !multipleDatabases {
				data["db"] = database.Name
			} else {
				dbPrefix = fmt.Sprintf("db%d", i+1)
				data[dbPrefix] = database.Name
			}
			if len(database.CollectionByID) == 1 {
				if multipleDatabases {
					data[fmt.Sprintf("db%dkeyspace", i+1)] = getKeyspaces(rt.TB(), database)[0]
				} else {
					keyspace := rt.GetSingleKeyspace()
					data["keyspace"] = keyspace
					data["scopeAndCollection"] = getScopeAndCollectionFromKeyspace(rt.TB(), keyspace)
				}
				continue
			}
			for j, keyspace := range getKeyspaces(rt.TB(), database) {
				if !multipleDatabases {
					data[fmt.Sprintf("keyspace%d", j+1)] = keyspace
					data[fmt.Sprintf("scopeAndCollection%d", j+1)] = getScopeAndCollectionFromKeyspace(rt.TB(), keyspace)
				} else {
					data[fmt.Sprintf("db%dkeyspace%d", i+1, j+1)] = keyspace
				}
			}
		}
	}
	var uri bytes.Buffer
	if err := tmpl.Execute(&uri, data); err != nil {
		return "", err
	}

	return uri.String(), nil
}

// mustTemplateResource provides some convenience templates for standard values.
//
// * If there is a single database: {{.db}} refers to single db
// * If there is only a single collection: {{.keyspace}} refers to a single collection, named or unamed
// * If there are multiple collections, defined is {{.keyspace1}},{{.keyspace2}},...
//
// This function causes the test to fail immediately if the given resource cannot be parsed.
func (rt *RestTester) mustTemplateResource(resource string) string {
	uri, err := rt.templateResource(resource)
	require.NoErrorf(rt.TB(), err, "URL template error: %v", err)
	return uri
}

func (rt *RestTester) SendAdminRequestWithAuth(method, resource string, body string, username string, password string) *TestResponse {
	request := Request(method, rt.mustTemplateResource(resource), body)

	request.SetBasicAuth(username, password)

	response := &TestResponse{ResponseRecorder: httptest.NewRecorder(), Req: request}

	rt.TestAdminHandler().ServeHTTP(response, request)
	return response
}

func (rt *RestTester) Send(request *http.Request) *TestResponse {
	response := &TestResponse{ResponseRecorder: httptest.NewRecorder(), Req: request}
	rt.TestPublicHandler().ServeHTTP(response, request)
	return response
}

func (rt *RestTester) SendMetricsRequest(method, resource, body string) *TestResponse {
	return rt.sendMetrics(Request(method, rt.mustTemplateResource(resource), body))
}

func (rt *RestTester) SendMetricsRequestWithHeaders(method, resource string, body string, headers map[string]string) *TestResponse {
	request := Request(method, rt.mustTemplateResource(resource), body)
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	return rt.sendMetrics(request)
}

func (rt *RestTester) sendMetrics(request *http.Request) *TestResponse {
	response := &TestResponse{ResponseRecorder: httptest.NewRecorder(), Req: request}
	rt.TestMetricsHandler().ServeHTTP(response, request)
	return response
}

// SendDiagnosticRequest runs a request against the diagnostic handler.
func (rt *RestTester) SendDiagnosticRequest(method, resource, body string) *TestResponse {
	request := Request(method, rt.mustTemplateResource(resource), body)
	response := &TestResponse{ResponseRecorder: httptest.NewRecorder(), Req: request}
	rt.TestDiagnosticHandler().ServeHTTP(response, Request(method, rt.mustTemplateResource(resource), body))
	return response
}

// SendDiagnosticRequestWithHeaders runs a request against the diagnostic handler with headers.
func (rt *RestTester) SendDiagnosticRequestWithHeaders(method, resource string, body string, headers map[string]string) *TestResponse {
	request := Request(method, rt.mustTemplateResource(resource), body)
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	response := &TestResponse{ResponseRecorder: httptest.NewRecorder(), Req: request}

	rt.TestDiagnosticHandler().ServeHTTP(response, request)
	return response
}

var fakeRestTesterIP = net.IPv4(127, 0, 0, 99)

func (rt *RestTester) TestAdminHandler() http.Handler {
	rt.adminHandlerOnce.Do(func() {
		rt.AdminHandler = CreateAdminHandler(rt.ServerContext())
		rt.ServerContext().addHTTPServer(adminServer, &serverInfo{nil, &net.TCPAddr{IP: fakeRestTesterIP, Port: 4985}})
	})
	return rt.AdminHandler
}

func (rt *RestTester) TestPublicHandler() http.Handler {
	rt.publicHandlerOnce.Do(func() {
		rt.PublicHandler = CreatePublicHandler(rt.ServerContext())
		rt.ServerContext().addHTTPServer(publicServer, &serverInfo{nil, &net.TCPAddr{IP: fakeRestTesterIP, Port: 4984}})
	})
	return rt.PublicHandler
}

func (rt *RestTester) TestMetricsHandler() http.Handler {
	rt.metricsHandlerOnce.Do(func() {
		rt.MetricsHandler = CreateMetricHandler(rt.ServerContext())
		rt.ServerContext().addHTTPServer(metricsServer, &serverInfo{nil, &net.TCPAddr{IP: fakeRestTesterIP, Port: 4986}})
	})
	return rt.MetricsHandler
}

// TestDiagnosticHandler is called to lazily create a handler against the diagnostic interface.
func (rt *RestTester) TestDiagnosticHandler() http.Handler {
	rt.diagnosticHandlerOnce.Do(func() {
		rt.DiagnosticHandler = createDiagnosticHandler(rt.ServerContext())
		rt.ServerContext().addHTTPServer(diagnosticServer, &serverInfo{nil, &net.TCPAddr{IP: fakeRestTesterIP, Port: 4987}})
	})
	return rt.DiagnosticHandler
}

type ChangesResults struct {
	Results  []db.ChangeEntry
	Last_Seq db.SequenceID
}

func (cr ChangesResults) RequireDocIDs(t testing.TB, docIDs []string) {
	require.Len(t, cr.Results, len(docIDs))
	for _, docID := range docIDs {
		var found bool
		for _, changeEntry := range cr.Results {
			if changeEntry.ID == docID {
				found = true
				break
			}
		}
		require.True(t, found, "DocID %q missing from results %v", docID, cr.Results)
	}
}

func (cr ChangesResults) RequireRevID(t testing.TB, revIDs []string) {
	require.Equal(t, len(revIDs), len(cr.Results))
	for _, rev := range revIDs {
		var found bool
		for _, changeEntry := range cr.Results {
			if changeEntry.Changes[0]["rev"] == rev {
				found = true
				break
			}
		}
		require.True(t, found, "RevID %q missing from results %v", rev, cr.Results)
	}
}

func (cr ChangesResults) Summary() string {
	var revs []string
	for _, changeEntry := range cr.Results {
		revs = append(revs, fmt.Sprintf("{ID:%s}", changeEntry.ID))
	}
	return strings.Join(revs, ", ")
}

// RequireChangeRev asserts that the given db.ChangeByVersionType returned a /_changes feed has the expected DocVersion entry, for a given versionType (rev or cv)
func RequireChangeRev(t *testing.T, expected DocVersion, changeRev db.ChangeByVersionType, versionType db.ChangesVersionType) {
	// Only one version type will be populated on a changes feed, based on what the original request demanded and what version types are available on that particular revision.
	var expectedStr string
	switch versionType {
	case db.ChangesVersionTypeRevTreeID:
		expectedStr = expected.RevTreeID
	case db.ChangesVersionTypeCV:
		expectedStr = expected.CV.String()
	default:
		t.Fatalf("Unexpected version type: %q", versionType)
	}
	require.Equalf(t, expectedStr, changeRev[versionType], "Expected changeRev[%q]==%s, got %s", versionType, expected.RevTreeID, changeRev[versionType])
}

// WaitForChanges waits for the specific number of changes to appear. Fails the test harness if more or fewer changes appear.
func (rt *RestTester) WaitForChanges(numChangesExpected int, changesURL, username string, useAdminPort bool) ChangesResults {
	waitTime := 20 * time.Second // some tests rely on cbgt import which can be quite slow if it needs to rollback
	if base.UnitTestUrlIsWalrus() && !base.IsRaceDetectorEnabled(rt.TB()) {
		// rosmar will never take a long time, so have faster failures
		waitTime = 1 * time.Second
	}
	var changes *ChangesResults
	url := rt.mustTemplateResource(changesURL)
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		var response *TestResponse
		if useAdminPort {
			response = rt.SendAdminRequest("GET", url, "")

		} else {
			response = rt.Send(RequestByUser("GET", url, "", username))
		}
		assert.NoError(c, base.JSONUnmarshal(response.Body.Bytes(), &changes))
		assert.Len(c, changes.Results, numChangesExpected, "Expected %d changes, got %d changes", numChangesExpected, len(changes.Results))
	}, waitTime, 10*time.Millisecond)
	return *changes
}

// WaitForCondition runs a retry loop that evaluates the provided function, and terminates
// when the function returns true.
func (rt *RestTester) WaitForCondition(successFunc func() bool) error {
	return rt.WaitForConditionWithOptions(successFunc, 200, 100)
}

func (rt *RestTester) WaitForConditionWithOptions(successFunc func() bool, maxNumAttempts, timeToSleepMs int) error {
	return WaitForConditionWithOptions(rt.Context(), successFunc, maxNumAttempts, timeToSleepMs)
}

func WaitForConditionWithOptions(ctx context.Context, successFunc func() bool, maxNumAttempts, timeToSleepMs int) error {
	waitForSuccess := func() (shouldRetry bool, err error, value any) {
		if successFunc() {
			return false, nil, nil
		}
		return true, nil, nil
	}

	sleeper := base.CreateSleeperFunc(maxNumAttempts, timeToSleepMs)
	err, _ := base.RetryLoop(ctx, "Wait for condition options", waitForSuccess, sleeper)
	if err != nil {
		return err
	}

	return nil
}

func (rt *RestTester) WaitForConditionShouldRetry(conditionFunc func() (shouldRetry bool, err error, value any), maxNumAttempts, timeToSleepMs int) error {
	sleeper := base.CreateSleeperFunc(maxNumAttempts, timeToSleepMs)
	err, _ := base.RetryLoop(rt.Context(), "Wait for condition options", conditionFunc, sleeper)
	if err != nil {
		return err
	}

	return nil
}

func (rt *RestTester) SendAdminRequest(method, resource, body string) *TestResponse {
	request := Request(method, rt.mustTemplateResource(resource), body)

	response := &TestResponse{ResponseRecorder: httptest.NewRecorder(), Req: request}

	rt.TestAdminHandler().ServeHTTP(response, request)
	return response
}

func (rt *RestTester) SendUserRequest(method, resource, body, username string) *TestResponse {
	return rt.Send(RequestByUser(method, rt.mustTemplateResource(resource), body, username))
}

func (rt *RestTester) WaitForNUserViewResults(numResultsExpected int, viewUrlPath string, user auth.User, password string) (viewResult sgbucket.ViewResult) {
	return rt.WaitForNViewResults(numResultsExpected, viewUrlPath, user, password)
}

func (rt *RestTester) WaitForNAdminViewResults(numResultsExpected int, viewUrlPath string) (viewResult sgbucket.ViewResult) {
	return rt.WaitForNViewResults(numResultsExpected, viewUrlPath, nil, "")
}

// Wait for a certain number of results to be returned from a view query
// viewUrlPath: is the path to the view, including the db name.  Eg: "/db/_design/foo/_view/bar"
func (rt *RestTester) WaitForNViewResults(numResultsExpected int, viewUrlPath string, user auth.User, password string) (viewResult sgbucket.ViewResult) {

	worker := func() (shouldRetry bool, err error, value sgbucket.ViewResult) {
		var response *TestResponse
		if user != nil {
			request := Request("GET", viewUrlPath, "")
			request.SetBasicAuth(user.Name(), password)
			response = rt.Send(request)
		} else {
			response = rt.SendAdminRequest("GET", viewUrlPath, ``)
		}

		// If the view is undefined, it might be a race condition where the view is still being created
		// See https://github.com/couchbase/sync_gateway/issues/3570#issuecomment-390487982
		if strings.Contains(response.Body.String(), "view_undefined") {
			base.InfofCtx(rt.Context(), base.KeyAll, "view_undefined error: %v.  Retrying", response.Body.String())
			return true, nil, sgbucket.ViewResult{}
		}

		if response.Code != 200 {
			return false, fmt.Errorf("Got response code: %d from view call.  Expected 200", response.Code), sgbucket.ViewResult{}
		}
		var result sgbucket.ViewResult
		require.NoError(rt.TB(), base.JSONUnmarshal(response.Body.Bytes(), &result))

		if len(result.Rows) >= numResultsExpected {
			// Got enough results, break out of retry loop
			return false, nil, result
		}

		// Not enough results, retry
		return true, nil, sgbucket.ViewResult{}

	}

	description := fmt.Sprintf("Wait for %d view results for query to %v", numResultsExpected, viewUrlPath)
	sleeper := base.CreateSleeperFunc(200, 100)
	err, returnVal := base.RetryLoop(rt.Context(), description, worker, sleeper)
	require.NoError(rt.TB(), err, "Error waiting for view results: %v", err)
	return returnVal
}

// Waits for view to be defined on the server.  Used to avoid view_undefined errors.
func (rt *RestTester) WaitForViewAvailable(viewURLPath string) (err error) {

	worker := func() (shouldRetry bool, err error, value any) {
		response := rt.SendAdminRequest("GET", viewURLPath, ``)

		if response.Code == 200 {
			return false, nil, nil
		}

		// Views unavailable, retry
		if response.Code == 500 {
			log.Printf("Error waiting for view to be available....will retry: %s", response.Body.Bytes())
			return true, fmt.Errorf("500 error"), nil
		}

		// Unexpected error, return
		return false, fmt.Errorf("Unexpected error response code while waiting for view available: %v", response.Code), nil

	}

	description := "Wait for view readiness"
	sleeper := base.CreateSleeperFunc(200, 100)
	err, _ = base.RetryLoop(rt.Context(), description, worker, sleeper)

	return err

}

func (rt *RestTester) GetDBState() string {
	var body db.Body
	resp := rt.SendAdminRequest("GET", "/{{.db}}/", "")
	RequireStatus(rt.TB(), resp, 200)
	require.NoError(rt.TB(), base.JSONUnmarshal(resp.Body.Bytes(), &body))
	return body["state"].(string)
}

// WaitForDBOnline waits for the database to be in the Online state. Fail the test harness if the state is not reached within the timeout.
func (rt *RestTester) WaitForDBOnline() {
	rt.WaitForDBState("Online")
}

// WaitForDBState waits for the database to be in the specified state. Fails the test harness if the state is not reached within the timeout.
func (rt *RestTester) WaitForDBState(stateWant string) {
	rt.WaitForDatabaseState(rt.GetDatabase().Name, stateWant)
}

// WaitForDatabaseState waits for the specified database to be in the specified state. Fails the test harness if the state is not reached within the timeout.
func (rt *RestTester) WaitForDatabaseState(dbName string, targetState string) {
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		assert.Equal(c, targetState, rt.GetDatabaseRoot(dbName).State)
	}, 10*time.Second, 100*time.Millisecond)
}

func (rt *RestTester) SendAdminRequestWithHeaders(method, resource string, body string, headers map[string]string) *TestResponse {
	request := Request(method, rt.mustTemplateResource(resource), body)
	for k, v := range headers {
		request.Header.Set(k, v)
	}
	response := &TestResponse{ResponseRecorder: httptest.NewRecorder(), Req: request}

	rt.TestAdminHandler().ServeHTTP(response, request)
	return response
}

// SetAdminChannels creates or updates a user with the specified channels.
func (rt *RestTester) SetAdminChannels(username string, password string, keyspace string, channels ...string) {
	dbName, scopeName, collectionName, err := ParseKeyspace(keyspace)
	require.NoError(rt.TB(), err)
	var currentConfig auth.PrincipalConfig
	user, err := rt.GetDatabase().Authenticator(rt.Context()).GetUser(username)
	require.NoError(rt.TB(), err)
	newUser := user == nil
	if !newUser {
		userResponse := rt.SendAdminRequest("GET", "/"+dbName+"/_user/"+username, "")
		if userResponse.Code != http.StatusNotFound {
			RequireStatus(rt.TB(), userResponse, http.StatusOK)
			require.NoError(rt.TB(), base.JSONUnmarshal(userResponse.Body.Bytes(), &currentConfig))
		}
	}
	currentConfig.Password = &password

	if scopeName == nil || collectionName == nil {
		if channels != nil {
			currentConfig.ExplicitChannels = base.SetFromArray(channels)
		}
	} else {
		currentConfig.SetExplicitChannels(*scopeName, *collectionName, channels...)
	}
	// Remove read only properties returned from the user api
	for _, scope := range currentConfig.CollectionAccess {
		for _, collectionAccess := range scope {
			collectionAccess.Channels_ = nil
			collectionAccess.JWTChannels_ = nil
			collectionAccess.JWTLastUpdated = nil
		}
	}

	newConfigBytes, err := base.JSONMarshal(currentConfig)
	require.NoError(rt.TB(), err)

	userResponse := rt.SendAdminRequest("PUT", "/"+dbName+"/_user/"+username, string(newConfigBytes))
	if newUser {
		RequireStatus(rt.TB(), userResponse, http.StatusCreated)
	} else {
		RequireStatus(rt.TB(), userResponse, http.StatusOK)
	}
}

// GetDocumentSequence looks up the sequence for a document using the _raw endpoint.
// Used by tests that need to validate sequences (for grants, etc)
func (rt *RestTester) GetDocumentSequence(key string) (sequence uint64) {
	response := rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/_raw/%s", key), "")
	require.Equal(rt.TB(), http.StatusOK, response.Code, "Error getting raw document %s", response.Body.String())

	var rawResponse RawDocResponse
	require.NoError(rt.TB(), base.JSONUnmarshal(response.BodyBytes(), &rawResponse))
	return rawResponse.Xattrs.Sync.Sequence
}

// GetRawDoc returns the raw document response for a given document ID using the _raw endpoint.
func (rt *RestTester) GetRawDoc(key string) RawDocResponse {
	response := rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/_raw/%s", key), "")
	require.Equal(rt.TB(), http.StatusOK, response.Code, "Error getting raw document %s", response.Body.String())
	var rawResponse RawDocResponse
	require.NoError(rt.TB(), base.JSONUnmarshal(response.BodyBytes(), &rawResponse))
	return rawResponse
}

// ReplacePerBucketCredentials replaces buckets defined on StartupConfig.BucketCredentials then recreates the couchbase
// cluster to pick up the changes
func (rt *RestTester) ReplacePerBucketCredentials(config base.PerBucketCredentialsConfig) {
	rt.ServerContext().Config.BucketCredentials = config
	// Update the CouchbaseCluster to include the new bucket credentials
	couchbaseCluster, err := CreateBootstrapConnectionFromStartupConfig(base.TestCtx(rt.TB()), rt.ServerContext().Config, base.PerUseClusterConnections)
	require.NoError(rt.TB(), err)
	rt.ServerContext().BootstrapContext.Connection = couchbaseCluster
}

// Context returns a context for a rest tester with server and database log context, if available an unambiguous.
func (rt *RestTester) Context() context.Context {
	ctx := base.TestCtx(rt.TB())
	if svrctx := rt.ServerContext(); svrctx != nil {
		ctx = svrctx.AddServerLogContext(ctx)
	}
	// this has the possibility of adding an ambiguous database log context if there is a database being created at this time.
	databases := rt.ServerContext().AllDatabases()
	if len(databases) == 1 {
		for _, database := range databases {
			ctx = database.AddDatabaseLogContext(ctx)
		}
	}
	return ctx
}

type TestResponse struct {
	*httptest.ResponseRecorder
	Req *http.Request

	bodyCache []byte
}

// BodyBytes takes a copy of the bytes in the response buffer, and saves them for future callers.
func (r TestResponse) BodyBytes() []byte {
	if r.bodyCache == nil {
		// since we are reading the underlying write buffer here, we do not need to close. If call r.Result().Body, then this needs to be closed.
		r.bodyCache = r.ResponseRecorder.Body.Bytes()
	}
	return r.bodyCache
}

// BodyString returns the string of the response body. This is cached once the first time it is called.
func (r TestResponse) BodyString() string {
	return string(r.BodyBytes())
}

// DumpBody returns the byte array of the response body. This is cached once the first time it is called.
func (r TestResponse) DumpBody() {
	log.Printf("%v", r.Body.String())
}

func (r TestResponse) GetRestDocument() RestDocument {
	restDoc := NewRestDocument()
	err := base.JSONUnmarshal(r.Body.Bytes(), restDoc)
	if err != nil {
		panic(fmt.Sprintf("Error parsing body into RestDocument.  Body: %s.  Err: %v", r.Body.String(), err))
	}
	return *restDoc
}

func Request(method, resource, body string) *http.Request {
	request, err := http.NewRequest(method, "http://127.0.0.1"+resource, bytes.NewBufferString(body))
	if err != nil {
		panic(fmt.Sprintf("http.NewRequest failed: %v", err))
	}
	request.RemoteAddr = "test.client.addr:99999" // usually populated by actual HTTP requests going into a HTTP Server
	request.RequestURI = resource                 // This doesn't get filled in by NewRequest
	FixQuotedSlashes(request)
	return request
}

func RequestByUser(method, resource, body, username string) *http.Request {
	r := Request(method, resource, body)
	r.SetBasicAuth(username, RestTesterDefaultUserPassword)
	return r
}

func RequireStatus(t testing.TB, response *TestResponse, expectedStatus int) {
	require.Equalf(t, expectedStatus, response.Code,
		"Response status %d %q (expected %d %q)\nfor %s <%s> : %s",
		response.Code, http.StatusText(response.Code),
		expectedStatus, http.StatusText(expectedStatus),
		response.Req.Method, response.Req.URL, response.Body)
}

func AssertStatus(t testing.TB, response *TestResponse, expectedStatus int) bool {
	return assert.Equalf(t, expectedStatus, response.Code,
		"Response status %d %q (expected %d %q)\nfor %s <%s> : %s",
		response.Code, http.StatusText(response.Code),
		expectedStatus, http.StatusText(expectedStatus),
		response.Req.Method, response.Req.URL, response.Body)
}

func NewSlowResponseRecorder(responseDelay time.Duration, responseRecorder *httptest.ResponseRecorder) *SlowResponseRecorder {

	responseStarted := sync.WaitGroup{}
	responseStarted.Add(1)

	responseFinished := sync.WaitGroup{}
	responseFinished.Add(1)

	return &SlowResponseRecorder{
		responseDelay:    responseDelay,
		ResponseRecorder: responseRecorder,
		responseStarted:  &responseStarted,
		responseFinished: &responseFinished,
	}

}

type SlowResponseRecorder struct {
	*httptest.ResponseRecorder
	responseDelay    time.Duration
	responseStarted  *sync.WaitGroup
	responseFinished *sync.WaitGroup
}

func (s *SlowResponseRecorder) WaitForResponseToStart() {
	s.responseStarted.Wait()
}

func (s *SlowResponseRecorder) WaitForResponseToFinish() {
	s.responseFinished.Wait()
}

func (s *SlowResponseRecorder) Write(buf []byte) (int, error) {

	s.responseStarted.Done()

	time.Sleep(s.responseDelay)

	numBytesWritten, err := s.ResponseRecorder.Write(buf)

	s.responseFinished.Done()

	return numBytesWritten, err
}

// AddDatabaseFromConfigWithBucket adds a database to the ServerContext and sets a specific bucket on the database context.
// If an existing config is found for the name, returns an error.
func (sc *ServerContext) AddDatabaseFromConfigWithBucket(ctx context.Context, tb testing.TB, config DatabaseConfig, bucket base.Bucket) (*db.DatabaseContext, error) {
	options := getOrAddDatabaseConfigOptions{
		useExisting: false,
		failFast:    false,
		connectToBucketFn: func(_ context.Context, spec base.BucketSpec, _ bool) (base.Bucket, error) {
			return bucket, nil
		},
	}
	return sc.getOrAddDatabaseFromConfig(ctx, config, options)
}

// The parameters used to create a BlipTester
type BlipTesterSpec struct {

	// Run Sync Gateway with allow_conflicts.  Will be propgated to the underlying RestTester
	allowConflicts bool

	// If an underlying RestTester is created, it will propagate this setting to the underlying RestTester.
	GuestEnabled bool

	// The Sync Gateway username to connect with. This will always use RestTesterDefaultUserPassword to connect if username is set.
	// If not set, then you may want to enabled Guest access.
	connectingUsername string

	// Allow tests to further customized a RestTester or re-use it across multiple BlipTesters if needed.
	// If a RestTester is passed in, certain properties of the BlipTester such as GuestEnabled will be ignored, since
	// those properties only affect the creation of the RestTester.
	// If nil, a default restTester will be created based on the properties in this spec
	// restTester *RestTester

	// Supported blipProtocols for the client to use in order of preference
	blipProtocols []string

	// If true, do not automatically initialize GetCollections handshake
	skipCollectionsInitialization bool

	// If set, use custom sync function for all collections.
	syncFn string

	// Represents Origin header values to be used in the blip handshake.
	origin *string

	// If true, pass Allow-Header-Origin: to the hostname in the blip handshake.
	useHostOrigin bool
}

// State associated with a BlipTester
// Note that it's not safe to have multiple goroutines access a single BlipTester due to the
// fact that certain methods register profile handlers on the BlipContext
type BlipTester struct {

	// The underlying RestTester which is used to bootstrap the initial blip websocket creation,
	// as well as providing a way for tests to access Sync Gateway over REST to hit admin-only endpoints
	// which are not available via blip.  Since a test may need to create multiple BlipTesters for multiple
	// user contexts, a single RestTester may be shared among multiple BlipTester instances.
	restTester *RestTester

	// This flag is used to avoid closing the contained restTester. This functionality is to avoid a double close in
	// some areas.
	avoidRestTesterClose bool

	// The blip context which contains blip related state and the sender/receiver goroutines associated
	// with this websocket connection
	blipContext       *blip.Context
	activeSubprotocol db.CBMobileSubprotocolVersion

	// The blip sender that can be used for sending messages over the websocket connection
	sender *blip.Sender

	// Set when we receive a reply to a getCollections request. Used to verify that all messages after that contain a
	// `collection` property.
	useCollections bool
}

// Close the bliptester
func (bt BlipTester) Close() {
	bt.sender.Close()
	if !bt.avoidRestTesterClose {
		bt.restTester.Close()
	}
}

// Returns database context for blipTester (assumes underlying rest tester is based on a single db - returns first it finds)
func (bt BlipTester) DatabaseContext() *db.DatabaseContext {
	dbs := bt.restTester.ServerContext().AllDatabases()
	for _, database := range dbs {
		return database
	}
	return nil
}

// getBlipTesterSpec returns a default tester specification.
func getDefaultBlipTesterSpec() BlipTesterSpec {
	return BlipTesterSpec{GuestEnabled: true}
}

// NewBlipTesterFromSpecWithRT creates a blip tester from an existing rest tester
func NewBlipTesterFromSpecWithRT(rt *RestTester, spec *BlipTesterSpec) *BlipTester {
	blipTesterSpec := spec
	if spec == nil {
		blipTesterSpec = &BlipTesterSpec{}
	}
	if blipTesterSpec.syncFn != "" {
		rt.TB().Errorf("Setting BlipTesterSpec.SyncFn is incompatible with passing a custom RestTester. Use SyncFn on RestTester")
	}
	if blipTesterSpec.GuestEnabled {
		rt.TB().Errorf("Setting BlipTesterSpec.GuestEnabled is incompatible with passing a custom RestTester. Use GuestEnabled on RestTester")
	}
	blipTester, err := createBlipTesterWithSpec(rt, *blipTesterSpec)
	require.NoError(rt.TB(), err)
	blipTester.avoidRestTesterClose = true

	return blipTester
}

// NewBlipTesterDefaultCollection creates a blip tester that has a RestTester only using a single database and `_default._default` collection.
func NewBlipTesterDefaultCollection(tb testing.TB) *BlipTester {
	return NewBlipTesterDefaultCollectionFromSpec(tb, BlipTesterSpec{GuestEnabled: true})
}

// NewBlipTesterDefaultCollectionFromSpec creates a blip tester that has a RestTester only using a single database and `_default._default` collection.
func NewBlipTesterDefaultCollectionFromSpec(tb testing.TB, spec BlipTesterSpec) *BlipTester {
	rtConfig := RestTesterConfig{
		AllowConflicts: spec.allowConflicts,
		GuestEnabled:   spec.GuestEnabled,
		DatabaseConfig: &DatabaseConfig{},
		SyncFn:         spec.syncFn,
	}
	rt := newRestTester(tb, &rtConfig, useSingleCollectionDefaultOnly, 1)
	bt, err := createBlipTesterWithSpec(rt, spec)
	require.NoError(tb, err)
	return bt
}

// NewBlipTester creates a blip tester with an underlying Rest Tester in the default configuration.
func NewBlipTester(tb testing.TB) *BlipTester {
	return NewBlipTesterFromSpec(tb, getDefaultBlipTesterSpec())
}

// NewBlipTesterFromSpec creates a BlipTester using options. This will create a RestTester underneath the BlipTester.
func NewBlipTesterFromSpec(tb testing.TB, spec BlipTesterSpec) *BlipTester {
	rtConfig := RestTesterConfig{
		AllowConflicts: spec.allowConflicts,
		GuestEnabled:   spec.GuestEnabled,
		SyncFn:         spec.syncFn,
	}
	rt := NewRestTester(tb, &rtConfig)
	if spec.connectingUsername != "" {
		rt.CreateUser(spec.connectingUsername, nil)
	}
	bt, err := createBlipTesterWithSpec(rt, spec)
	require.NoError(tb, err)
	return bt
}

// createBlipTesterWithSpec creates a blip tester targeting a specific RestTester. Returns an error to allow for
// testing connection error conditions. Use NewBlipTesterFromSpec for most tests.
func createBlipTesterWithSpec(rt *RestTester, spec BlipTesterSpec) (*BlipTester, error) {
	bt := &BlipTester{
		restTester: rt,
	}

	if !rt.GetDatabase().OnlyDefaultCollection() {
		bt.useCollections = true
	}

	// Since blip requests all go over the public handler, wrap the public handler with the httptest server
	publicHandler := bt.restTester.TestPublicHandler()

	// Create a _temporary_ test server bound to an actual port that is used to make the blip connection.
	// This is needed because the mock-based approach fails with a "Connection not hijackable" error when
	// trying to do the websocket upgrade.  Since it's only needed to setup the websocket, it can be closed
	// as soon as the websocket is established, hence the defer srv.Close() call.
	srv := httptest.NewServer(publicHandler)
	defer srv.Close()

	// Construct URL to connect to blipsync target endpoint
	destUrl := fmt.Sprintf("%s/%s/_blipsync", srv.URL, rt.GetDatabase().Name)
	u, err := url.Parse(destUrl)
	require.NoError(bt.TB(), err)
	u.Scheme = "ws"

	// If protocols are not set use V3 as a V3 client would
	protocols := spec.blipProtocols
	if len(protocols) == 0 {
		protocols = []string{db.CBMobileReplicationV3.SubprotocolString()}
	}

	origin, err := hostOnlyCORS(bt.restTester.GetDatabase().CORS.Origin)
	if err != nil {
		return nil, err
	}
	// Make BLIP/Websocket connection.  Not specifying cancellation context here as this is a
	// client blip context that doesn't require cancellation-based close
	_, bt.blipContext, err = db.NewSGBlipContextWithProtocols(rt.Context(), "", origin, protocols, nil)
	if err != nil {
		return nil, err
	}

	// Ensure that errors get correctly surfaced in tests
	bt.blipContext.FatalErrorHandler = func(err error) {
		bt.TB().Fatalf("BLIP fatal error: %v", err)
	}
	bt.blipContext.HandlerPanicHandler = func(request, response *blip.Message, err any) {
		stack := debug.Stack()
		bt.TB().Fatalf("Panic while handling %s: %v\n%s", request.Profile(), err, string(stack))
	}

	config := blip.DialOptions{
		URL: u.String(),
	}

	config.HTTPHeader = make(http.Header)
	if len(spec.connectingUsername) > 0 {
		config.HTTPHeader.Add("Authorization", GetBasicAuthHeader(bt.TB(), spec.connectingUsername, RestTesterDefaultUserPassword))
	}
	if spec.origin != nil {
		if spec.useHostOrigin {
			require.Fail(bt.TB(), "setting both origin and useHostOrigin is not supported")
		}
		config.HTTPHeader.Add("Origin", *spec.origin)
	} else if spec.useHostOrigin {
		config.HTTPHeader.Add("Origin", "https://"+u.Host)
	}

	bt.sender, err = bt.blipContext.DialConfig(&config)
	if err != nil {
		return nil, fmt.Errorf("Error dialing blip context: %w", err)
	}

	bt.activeSubprotocol, err = db.ParseSubprotocolString(bt.blipContext.ActiveSubprotocol())
	require.NoError(bt.TB(), err)

	collections := bt.restTester.getCollectionsForBLIP()
	if !spec.skipCollectionsInitialization && len(collections) > 0 {
		bt.initializeCollections(collections)
	}

	return bt, nil

}

// TB returns the current testing.TB
func (bt *BlipTester) TB() testing.TB {
	return bt.restTester.TB()
}

func (bt *BlipTester) initializeCollections(collections []string) {
	getCollectionsRequest := blip.NewRequest()
	getCollectionsRequest.SetProfile(db.MessageGetCollections)

	checkpointIDs := make([]string, len(collections))
	for i := range checkpointIDs {
		checkpointIDs[i] = "0"
	}

	requestBody := db.GetCollectionsRequestBody{
		Collections:   collections,
		CheckpointIDs: checkpointIDs,
	}
	body, err := base.JSONMarshal(requestBody)
	require.NoError(bt.restTester.TB(), err)

	getCollectionsRequest.SetBody(body)
	sent := bt.sender.Send(getCollectionsRequest)
	require.True(bt.restTester.TB(), sent)

	type CollectionsResponseEntry struct {
		LastSequence *int    `json:"last_sequence"`
		Rev          *string `json:"rev"`
	}

	response, err := getCollectionsRequest.Response().Body()
	require.NoError(bt.restTester.TB(), err)

	var collectionResponse []*CollectionsResponseEntry
	err = base.JSONUnmarshal(response, &collectionResponse)
	require.NoError(bt.restTester.TB(), err)

	for _, perCollectionResponse := range collectionResponse {
		require.NotNil(bt.restTester.TB(), perCollectionResponse)
	}
}

// newRequest returns a blip msg with a collection property enabled. This function is only ssafe to call if there is a single collection running.
func (bt *BlipTester) newRequest() *blip.Message {
	msg := blip.NewRequest()
	bt.addCollectionProperty(msg)
	return msg
}

// addCollectionProperty will automatically add a collection. If we are running with the default collection, or a single named collection, automatically add the right value. If there are multiple collections on the database, the test will fatally exit, since the behavior is undefined.
func (bt *BlipTester) addCollectionProperty(msg *blip.Message) *blip.Message {
	if bt.useCollections == true {
		require.Equal(bt.restTester.TB(), 1, len(bt.restTester.GetDatabase().CollectionByID), "Multiple collection exist on the database so we are unable to choose which collection to specify in BlipCollection property")
		msg.Properties[db.BlipCollection] = "0"
	}

	return msg
}

// SetCheckpoint sends a setCheckpoint message with the checkpoint docID and a specific and returns the response. Blocks waiting for the response, and checks error status.
func (bt *BlipTester) SetCheckpoint(client string, checkpointRev string, body []byte) *db.SetCheckpointResponse {

	scm := db.NewSetCheckpointMessage()
	scm.SetCompressed(true)
	scm.SetClient(client)
	scm.SetRev(checkpointRev)
	scm.SetBody(body)
	bt.addCollectionProperty(scm.Message)

	require.True(bt.TB(), bt.sender.Send(scm.Message))

	resp := scm.Response()
	body, err := resp.Body()
	require.NoError(bt.TB(), err)
	require.NotContains(bt.TB(), resp.Properties, "Error-Code", "Error in response to setCheckpoint request. Properties:%v Body:%s", resp.Properties, body)
	return &db.SetCheckpointResponse{Message: resp}
}

// newRevMessage constructs a rev message configured with the current collection and default set of parameters. properties will overwrite any default properties set by this function.
func (bt *BlipTester) newRevMessage(docID, docRev string, body []byte, properties blip.Properties) *blip.Message {

	revRequest := blip.NewRequest()
	revRequest.SetCompressed(true)
	revRequest.SetProfile("rev")

	revRequest.Properties["id"] = docID
	revRequest.Properties["rev"] = docRev
	revRequest.Properties["deleted"] = "false"
	maps.Copy(revRequest.Properties, properties)
	bt.addCollectionProperty(revRequest)
	revRequest.SetBody(body)
	return revRequest
}

// SendRevWithHistory sends an unsolicited rev message and waits for the response. The docHistory should be in the same format as expected by db.PutExistingRevWithBody(), or empty if this is the first revision
func (bt *BlipTester) SendRevWithHistory(docID, docRev string, revHistory []string, body []byte, properties blip.Properties) (res *blip.Message) {
	require.NotContains(bt.TB(), properties, "history", "If specifying history, use BlipTester.SendRev")
	if len(revHistory) > 0 {
		properties[db.RevMessageHistory] = strings.Join(revHistory, ",")
	}
	return bt.SendRev(docID, docRev, body, properties)
}

// SendRev sends an unsolicited rev message and waits for the response. The docHistory should be in the same format as expected by db.PutExistingRevWithBody(), or empty if this is the first revision
func (bt *BlipTester) SendRev(docID, docRev string, body []byte, properties blip.Properties) (res *blip.Message) {
	revRequest := bt.newRevMessage(docID, docRev, body, properties)
	bt.Send(revRequest)
	revResponse := revRequest.Response()
	rspBody, err := revResponse.Body()
	require.NoError(bt.TB(), err)
	require.Empty(bt.TB(), revResponse.Properties["Error-Code"], "Error in response to rev request. Properties:%v Body:%s", revResponse.Properties, rspBody)
	return revResponse
}

// SendRevExpectConflict sends an unsolicited rev message and waits for the response, expecting a conflict error (409). The docHistory should be in the same format as expected by db.PutExistingRevWithBody(), or empty if this is the first revision
func (bt *BlipTester) SendRevExpectConflict(docID, docRev string, body []byte, properties blip.Properties) (res *blip.Message) {
	revRequest := bt.newRevMessage(docID, docRev, body, properties)
	bt.Send(revRequest)
	revResponse := revRequest.Response()
	rspBody, err := revResponse.Body()
	require.NoError(bt.TB(), err)
	require.Equal(bt.TB(), "409", revResponse.Properties["Error-Code"], "Expected conflict in response to rev request. Properties:%v Body:%s", revResponse.Properties, rspBody)
	return revResponse
}

// Send a blip message but do not wait for a response.
func (bt *BlipTester) Send(rq *blip.Message) {
	require.True(bt.TB(), bt.sender.Send(rq))
}

// Run is equivalent to testing.T.Run() but updates the underlying RestTester's TB.
func (bt *BlipTester) Run(name string, test func(*testing.T)) {
	mainT := bt.restTester.TB().(*testing.T)
	mainT.Run(name, func(t *testing.T) {
		var tb testing.TB = t
		old := bt.restTester.testingTB.Swap(&tb)
		defer func() { bt.restTester.testingTB.Store(old) }()
		test(t)
	})
}

// PrincipalConfigForWrite is used by GetUserPayload, GetRolePayload to remove the omitempty for ExplicitRoleNames
// and ExplicitChannels, to build payloads with explicit removal of channels and roles after changes made in CBG-3883
type PrincipalConfigForWrite struct {
	auth.PrincipalConfig
	ExplicitChannels  *base.Set `json:"admin_channels,omitempty"`
	ExplicitRoleNames *base.Set `json:"admin_roles,omitempty"`
}

// GetUserPayload will take username, password, email, channels and roles you want to assign a user and create the appropriate payload for the _user endpoint.
// When using the default collection, chans and roles are handled as follows to align with CBG-3883:
//
//	nil: omitted from payload
//	empty slice: "[]"
//	populated slice: "["ABC"]"
func GetUserPayload(t testing.TB, username, password, email string, collection sgbucket.DataStore, chans, roles []string) string {
	config := PrincipalConfigForWrite{}
	if username != "" {
		config.Name = &username
	}
	if password != "" {
		config.Password = &password
	}
	if email != "" {
		config.Email = &email
	}

	if roles != nil {
		roleSet := base.SetOf(roles...)
		config.ExplicitRoleNames = &roleSet
	}

	marshalledConfig, err := addChannelsToPrincipal(config, collection, chans)
	require.NoError(t, err)
	return string(marshalledConfig)
}

// GetRolePayload will take roleName and channels you want to assign a particular role and return the appropriate payload for the _role endpoint
// For default collection, follows same handling as GetUserPayload for chans.
func GetRolePayload(t testing.TB, roleName string, collection sgbucket.DataStore, chans []string) string {
	config := PrincipalConfigForWrite{}
	if roleName != "" {
		config.Name = &roleName
	}
	marshalledConfig, err := addChannelsToPrincipal(config, collection, chans)
	require.NoError(t, err)
	return string(marshalledConfig)
}

// add channels to principal depending if running with collections or not. then marshal the principal config
func addChannelsToPrincipal(config PrincipalConfigForWrite, ds sgbucket.DataStore, chans []string) ([]byte, error) {
	if base.IsDefaultCollection(ds.ScopeName(), ds.CollectionName()) {
		if chans != nil {
			adminChannels := base.SetFromArray(chans)
			config.ExplicitChannels = &adminChannels
		}
	} else {
		config.SetExplicitChannels(ds.ScopeName(), ds.CollectionName(), chans...)
	}
	payload, err := json.Marshal(config)
	if err != nil {
		return []byte{}, err
	}
	return payload, nil
}

// getChangesHandler returns a changes handler which will respond to all changes messages and ask to be set rev or norev messages.
func getChangesHandler(t testing.TB, changesFinishedWg, revsFinishedWg *sync.WaitGroup) func(request *blip.Message) {
	return func(request *blip.Message) {
		// Send a response telling the other side we want ALL revisions

		body, err := request.Body()
		require.NoError(t, err, "Error getting request body")

		if string(body) == "null" {
			changesFinishedWg.Done()
			return
		}

		if !request.NoReply() {

			// unmarshal into json array
			changesBatch := [][]any{}

			require.NoError(t, base.JSONUnmarshal(body, &changesBatch))

			responseVal := [][]any{}
			for _, change := range changesBatch {
				revId := change[2].(string)
				responseVal = append(responseVal, []any{revId})
				revsFinishedWg.Add(1)
			}

			response := request.Response()
			responseValBytes, err := base.JSONMarshal(responseVal)
			log.Printf("responseValBytes: %s", responseValBytes)
			require.NoError(t, err, "Error marshalling response")
			response.SetBody(responseValBytes)

		}
	}
}

// GetDocAtRev sets up blip handlers to get a doc at a particular revision from Sync Gateway. Consider using BlipTesterClient to do this behavior.
//
// Warning: this can only be called from a single goroutine, given the fact it registers profile handlers.
//
// If that is not found, it will return an empty resultDoc with no errors.
//
// - Call subChanges (continuous=false) endpoint to get all changes from Sync Gateway
// - Respond to each "change" request telling the other side to send the revision
//   - NOTE: this could be made more efficient by only requesting the revision for the docid/revid pair
//     passed in the parameter.
//
// - If the rev handler is called back with the desired docid/revid pair, save that into a variable that will be returned
// - Block until all pending operations are complete
// - Return the resultDoc or an empty resultDoc
func (bt *BlipTester) GetDocAtRev(requestedDocID, requestedDocRev string) (resultDoc RestDocument, resultErr error) {

	docs := map[string]RestDocument{}
	changesFinishedWg := sync.WaitGroup{}
	revsFinishedWg := sync.WaitGroup{}

	defer func() {
		// Clean up all profile handlers that are registered as part of this test
		delete(bt.blipContext.HandlerForProfile, "changes")
		delete(bt.blipContext.HandlerForProfile, "rev")
		delete(bt.blipContext.HandlerForProfile, "norev")
	}()

	// -------- Changes handler callback --------
	bt.blipContext.HandlerForProfile["changes"] = getChangesHandler(bt.TB(), &changesFinishedWg, &revsFinishedWg)

	// -------- Norev handler callback --------
	bt.blipContext.HandlerForProfile["norev"] = func(request *blip.Message) {
		defer revsFinishedWg.Done()
		if request.Properties["id"] == requestedDocID && request.Properties["rev"] == requestedDocRev {
			resultErr = fmt.Errorf("got error from norev: %v %v", request.Properties["error"], request.Properties["reason"])
		}
	}

	// -------- Rev handler callback --------
	bt.blipContext.HandlerForProfile["rev"] = func(request *blip.Message) {

		defer revsFinishedWg.Done()
		body, err := request.Body()
		require.NoError(bt.TB(), err, "Error getting request body")
		var doc RestDocument
		require.NoError(bt.TB(), base.JSONUnmarshal(body, &doc))
		docId := request.Properties["id"]
		docRev := request.Properties["rev"]
		doc.SetID(docId)
		doc.SetRevID(docRev)
		docs[docId] = doc

		if docId == requestedDocID && docRev == requestedDocRev {
			resultDoc = doc
		}
	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	changesFinishedWg.Add(1)
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	bt.addCollectionProperty(subChangesRequest)

	bt.Send(subChangesRequest)

	WaitWithTimeout(bt.TB(), &changesFinishedWg, time.Second*30)
	WaitWithTimeout(bt.TB(), &revsFinishedWg, time.Second*30)
	return resultDoc, resultErr
}

type SendRevWithAttachmentInput struct {
	docId            string
	revId            string
	attachmentName   string
	attachmentLength int
	attachmentBody   string
	attachmentDigest string
	history          []string
	body             []byte
}

// SendRevWithAttachment will send a single rev message and block until the attachments are returned. The rev message is returned and must be checked for errors.
// Warning: this can only be called from a single goroutine, given the fact it registers profile handlers.
func (bt *BlipTester) SendRevWithAttachment(input SendRevWithAttachmentInput) (res *blip.Message) {

	defer func() {
		// Clean up all profile handlers that are registered as part of this test
		delete(bt.blipContext.HandlerForProfile, "getAttachment")
	}()

	// Create a doc with an attachment
	myAttachment := db.DocAttachment{
		ContentType: "application/json",
		Digest:      input.attachmentDigest,
		Length:      input.attachmentLength,
		Revpos:      1,
		Stub:        true,
	}

	doc := NewRestDocument()
	if len(input.body) > 0 {
		require.NoError(bt.TB(), json.Unmarshal(input.body, &doc))
	}

	doc.SetAttachments(db.AttachmentMap{
		input.attachmentName: myAttachment,
	})

	docBody, err := base.JSONMarshal(doc)
	require.NoError(bt.TB(), err, "Error marshalling doc")

	getAttachmentWg := sync.WaitGroup{}

	bt.blipContext.HandlerForProfile["getAttachment"] = func(request *blip.Message) {
		defer getAttachmentWg.Done()
		require.Equal(bt.TB(), myAttachment.Digest, request.Properties["digest"])
		response := request.Response()
		response.SetBody([]byte(input.attachmentBody))
	}

	// Push a rev with an attachment.
	getAttachmentWg.Add(1)
	rq := bt.newRevMessage(input.docId, input.revId, docBody, blip.Properties{
		db.RevMessageHistory: strings.Join(input.history, ","),
	})
	bt.Send(rq)
	// Expect a callback to the getAttachment endpoint
	getAttachmentWg.Wait()

	return rq.Response()
}

// WaitForNumChanges waits for at least the number of document changes and returns the changes as they are in the changes messages:
//
//	[[sequence, docID, revID, deleted], [sequence, docID, revID, deleted]]
func (bt *BlipTester) WaitForNumChanges(numChangesExpected int) (changes [][]any) {

	retryWorker := func() (shouldRetry bool, err error, value [][]any) {
		currentChanges := bt.GetChanges()
		if len(currentChanges) >= numChangesExpected {
			return false, nil, currentChanges
		}

		// haven't seen numDocsExpected yet, so wait and retry
		return true, nil, nil

	}

	err, changes := base.RetryLoop(
		bt.restTester.Context(),
		"WaitForNumChanges",
		retryWorker,
		base.CreateDoublingSleeperFunc(10, 10),
	)
	require.NoError(bt.restTester.TB(), err, "WaitForNumChanges failed")
	return changes

}

// Returns changes in form of [[sequence, docID, revID, deleted], [sequence, docID, revID, deleted]]
// Warning: this can only be called from a single goroutine, given the fact it registers profile handlers.
func (bt *BlipTester) GetChanges() (changes [][]any) {

	defer func() {
		// Clean up all profile handlers that are registered as part of this test
		delete(bt.blipContext.HandlerForProfile, "changes") // a handler for this profile is registered in SubscribeToChanges
	}()

	collectedChanges := [][]any{}
	chanChanges := make(chan *blip.Message)
	bt.SubscribeToChanges(false, chanChanges)

	for changeMsg := range chanChanges {

		body, err := changeMsg.Body()
		require.NoError(bt.TB(), err, "Error getting request body")

		if string(body) == "null" {
			// the other side indicated that it's done sending changes.
			// this only works (I think) because continuous=false.
			close(chanChanges)
			break
		}

		// unmarshal into json array
		changesBatch := [][]any{}

		require.NoError(bt.TB(), base.JSONUnmarshal(body, &changesBatch), "Error unmarshalling changes. Body: %v", string(body))

		collectedChanges = append(collectedChanges, changesBatch...)

	}

	return collectedChanges

}

// WaitForNumDocsViaChanges waits for the number of documents to be seen on the BlipTester from a subChanges request. Fails the test if expected number of documents is not found.
func (bt *BlipTester) WaitForNumDocsViaChanges(numDocsExpected int) (docs map[string]RestDocument) {

	require.EventuallyWithT(bt.restTester.TB(), func(c *assert.CollectT) {
		docs = bt.PullDocs()
		assert.Len(c, docs, numDocsExpected)
	}, 10*time.Second, 10*time.Millisecond)
	return docs
}

// PullDocs gets all documents and their attachments via the following steps:
//
// - Invoking one-shot subChanges request
// - Responding to all incoming "changes" requests from peer to request the changed rev, and accumulate rev body
// - Responding to all incoming "rev" requests from peer to get all attachments, and accumulate them
// - Return accumulated docs + attachments to caller
//
// It is basically a pull replication without the checkpointing
// Warning: this can only be called from a single goroutine, given the fact it registers profile handlers.
func (bt *BlipTester) PullDocs() (docs map[string]RestDocument) {

	docs = map[string]RestDocument{}

	// Mutex to avoid write contention on docs while PullDocs is running (as rev messages may be processed concurrently)
	var docsLock sync.Mutex
	changesFinishedWg := sync.WaitGroup{}
	revsFinishedWg := sync.WaitGroup{}

	defer func() {
		// Clean up all profile handlers that are registered as part of this test
		delete(bt.blipContext.HandlerForProfile, "changes")
		delete(bt.blipContext.HandlerForProfile, "rev")
	}()

	// -------- Changes handler callback --------
	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	bt.blipContext.HandlerForProfile["changes"] = getChangesHandler(bt.TB(), &changesFinishedWg, &revsFinishedWg)

	// -------- Rev handler callback --------
	bt.blipContext.HandlerForProfile["rev"] = func(request *blip.Message) {

		defer revsFinishedWg.Done()
		body, err := request.Body()
		require.NoError(bt.TB(), err)

		var doc RestDocument
		require.NoError(bt.TB(), base.JSONUnmarshal(body, &doc))
		docId := request.Properties["id"]
		docRev := request.Properties["rev"]
		doc.SetID(docId)
		doc.SetRevID(docRev)

		docsLock.Lock()
		docs[docId] = doc
		docsLock.Unlock()

		attachments, err := doc.GetAttachments()
		require.NoError(bt.TB(), err)

		for _, attachment := range attachments {

			// Get attachments and append to RestDocument
			getAttachmentRequest := blip.NewRequest()
			getAttachmentRequest.SetProfile(db.MessageGetAttachment)
			getAttachmentRequest.Properties[db.GetAttachmentDigest] = attachment.Digest
			if bt.activeSubprotocol >= db.CBMobileReplicationV3 {
				getAttachmentRequest.Properties[db.GetAttachmentID] = docId
			}
			bt.addCollectionProperty(getAttachmentRequest)
			bt.Send(getAttachmentRequest)
			getAttachmentResponse := getAttachmentRequest.Response()
			getAttachmentBody, getAttachmentErr := getAttachmentResponse.Body()
			require.NoError(bt.TB(), getAttachmentErr, "Error getting attachment body")
			attachment.Data = getAttachmentBody
		}

		// Send response to rev request
		if !request.NoReply() {
			response := request.Response()
			response.SetBody([]byte{}) // Empty response to indicate success
		}

	}

	// -------- Norev handler callback --------
	bt.blipContext.HandlerForProfile["norev"] = func(request *blip.Message) {
		// If a norev is received, then don't bother waiting for one of the expected revisions, since it will never come.
		// The norev could be added to the returned docs map, but so far there is no need for that.  The ability
		// to assert on the number of actually received revisions (which norevs won't affect) meets current test requirements.
		defer revsFinishedWg.Done()
	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	changesFinishedWg.Add(1)
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	bt.addCollectionProperty(subChangesRequest)

	bt.Send(subChangesRequest)

	changesFinishedWg.Wait()

	revsFinishedWg.Wait()

	return docs

}

func (bt *BlipTester) SubscribeToChanges(continuous bool, changes chan<- *blip.Message) {

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		changes <- request

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []any{}
			emptyResponseValBytes, err := base.JSONMarshal(emptyResponseVal)
			require.NoError(bt.TB(), err)
			response.SetBody(emptyResponseValBytes)
		}

	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	bt.addCollectionProperty(subChangesRequest)
	switch continuous {
	case true:
		subChangesRequest.Properties["continuous"] = "true"
	default:
		subChangesRequest.Properties["continuous"] = "false"
	}

	bt.Send(subChangesRequest)
	subChangesResponse := subChangesRequest.Response()
	require.Equal(bt.TB(), subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())
	require.NotContains(bt.TB(), subChangesResponse.Properties, db.BlipErrorCode, "Error in response to subChanges request. Properties:%v", subChangesResponse.Properties)
}

// Helper for comparing BLIP changes received with expected BLIP changes
type ExpectedChange struct {
	docId    string // DocId or "*" for any doc id
	revId    string // RevId or "*" for any rev id
	sequence string // Sequence or "*" for any sequence
	deleted  *bool  // Deleted status or nil for any deleted status
}

func (e ExpectedChange) Equals(change []any) error {

	// TODO: this is commented because it's giving an error: panic: interface conversion: interface {} is float64, not string [recovered].
	// TODO: I think this should be addressed by adding a BlipChange struct stronger typing than a slice of empty interfaces.  TBA.
	// changeSequence := change[0].(string)

	var changeDeleted *bool

	changeDocId := change[1].(string)
	changeRevId := change[2].(string)
	if len(change) > 3 {
		changeDeletedVal := change[3].(bool)
		changeDeleted = &changeDeletedVal
	}

	if e.docId != "*" && changeDocId != e.docId {
		return fmt.Errorf("changeDocId (%s) != expectedChangeDocId (%s)", changeDocId, e.docId)
	}

	if e.revId != "*" && changeRevId != e.revId {
		return fmt.Errorf("changeRevId (%s) != expectedChangeRevId (%s)", changeRevId, e.revId)
	}

	// TODO: commented due to reasons given above
	// if e.sequence != "*" && changeSequence != e.sequence {
	//	return fmt.Errorf("changeSequence (%s) != expectedChangeSequence (%s)", changeSequence, e.sequence)
	// }

	if changeDeleted != nil && e.deleted != nil && *changeDeleted != *e.deleted {
		return fmt.Errorf("changeDeleted (%v) != expectedChangeDeleted (%v)", *changeDeleted, *e.deleted)
	}

	return nil
}

// Model "CouchDB" style REST documents which define the following special fields:
//
// - _id
// - _rev
// - _removed
// - _deleted (not accounted for yet)
// - _attachments
//
// This struct wraps a map and provides convenience methods for getting at the special
// fields with the appropriate types (string in the id/rev case, db.AttachmentMap in the attachments case).
// Currently only used in tests, but if similar functionality needed in primary codebase, could be moved.
type RestDocument map[string]any

func NewRestDocument() *RestDocument {
	emptyBody := make(map[string]any)
	restDoc := RestDocument(emptyBody)
	return &restDoc
}

func (d RestDocument) ID() string {
	rawID, hasID := d[db.BodyId]
	if !hasID {
		return ""
	}
	return rawID.(string)

}

func (d RestDocument) SetID(docId string) {
	d[db.BodyId] = docId
}

func (d RestDocument) RevID() string {
	rawRev, hasRev := d[db.BodyRev]
	if !hasRev {
		return ""
	}
	return rawRev.(string)
}

func (d RestDocument) SetRevID(revId string) {
	d[db.BodyRev] = revId
}

func (d RestDocument) SetAttachments(attachments db.AttachmentMap) {
	d[db.BodyAttachments] = attachments
}

func (d RestDocument) GetAttachments() (db.AttachmentMap, error) {

	rawAttachments, hasAttachments := d[db.BodyAttachments]

	// If the map doesn't even have the _attachments key, return an empty attachments map
	if !hasAttachments {
		return db.AttachmentMap{}, nil
	}

	// Otherwise, create an AttachmentMap from the value in the raw map
	attachmentMap := db.AttachmentMap{}
	switch v := rawAttachments.(type) {
	case db.AttachmentMap:
		// If it's already an AttachmentMap (maybe due to previous call to SetAttachments), then return as-is
		return v, nil
	default:
		rawAttachmentsMap := v.(map[string]any)
		for attachmentName, attachmentVal := range rawAttachmentsMap {

			// marshal attachmentVal into a byte array, then unmarshal into a DocAttachment
			attachmentValMarshalled, err := base.JSONMarshal(attachmentVal)
			if err != nil {
				return db.AttachmentMap{}, err
			}
			docAttachment := db.DocAttachment{}
			if err := base.JSONUnmarshal(attachmentValMarshalled, &docAttachment); err != nil {
				return db.AttachmentMap{}, err
			}

			attachmentMap[attachmentName] = docAttachment
		}

		// Avoid the unnecessary re-Marshal + re-Unmarshal
		d.SetAttachments(attachmentMap)
	}

	return attachmentMap, nil

}

func (d RestDocument) IsRemoved() bool {
	removed, ok := d[db.BodyRemoved]
	if !ok {
		return false
	}
	return removed.(bool)
}

// WaitWithTimeout calls for the WaitGroup.Wait() and fails the test if the Wait does not return within the timeout.
func WaitWithTimeout(t testing.TB, wg *sync.WaitGroup, timeout time.Duration) {

	// Create a channel so that a goroutine waiting on the waitgroup can send it's result (if any)
	wgFinished := make(chan bool)

	go func() {
		wg.Wait()
		wgFinished <- true
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-wgFinished:
		return
	case <-timer.C:
		require.FailNow(t, fmt.Sprintf("Timed out waiting after %.2f sec", timeout.Seconds()))
	}
}

// NewHTTPTestServerOnListener returns a new httptest server, which is configured to listen on the given listener.
// This is useful when you need to know the listen address before you start up a server.
func NewHTTPTestServerOnListener(h http.Handler, l net.Listener) *httptest.Server {
	s := &httptest.Server{
		Config:   &http.Server{Handler: h},
		Listener: l,
	}
	s.Start()
	return s
}

func WaitAndAssertCondition(t testing.TB, fn func() bool, failureMsgAndArgs ...any) {
	t.Helper()
	t.Log("starting WaitAndAssertCondition")
	for i := 0; i <= 20; i++ {
		if i == 20 {
			assert.Fail(t, "Condition failed to be satisfied", failureMsgAndArgs...)
		}
		if fn() {
			break
		}
		time.Sleep(time.Millisecond * 250)
	}
}

func WaitAndAssertConditionTimeout(t *testing.T, timeout time.Duration, fn func() bool, failureMsgAndArgs ...any) {
	t.Helper()
	start := time.Now()
	tick := time.NewTicker(timeout / 20)
	defer tick.Stop()
	for range tick.C {
		if time.Since(start) > timeout {
			assert.Fail(t, "Condition failed to be satisfied", failureMsgAndArgs...)
		}
		if fn() {
			return
		}
	}
}

type DocVersion = db.DocVersion

// RequireDocVersionNotNil calls t.Fail if two document version is not specified.
func RequireDocVersionNotNil(t *testing.T, version DocVersion) {
	require.NotEqual(t, "", version.RevTreeID)
}

// RequireDocVersionEqual calls t.Fail if two document versions are not equal.
func RequireDocVersionEqual(t testing.TB, expected, actual DocVersion) {
	require.Equal(t, expected.CV, actual.CV, "Versions mismatch.  Expected: %v, Actual: %v", expected, actual)
	require.Equal(t, expected.RevTreeID, actual.RevTreeID, "Versions mismatch.  Expected: %v, Actual: %v", expected, actual)
}

// RequireHistoryContains fails test if rev tree does not contain all expected revIDs
func RequireHistoryContains(t *testing.T, docHistory db.RevTree, expHistoryIDs []string) {
	require.Lenf(t, docHistory, len(expHistoryIDs), "Expected history to contain %d revIDs, but it had %d.  History: %v", len(expHistoryIDs), len(docHistory), docHistory)
	for _, revID := range expHistoryIDs {
		_, ok := docHistory[revID]
		require.Truef(t, ok, "Expected history to contain revID %s, but it was not found.  History: %v", revID, docHistory)
	}
}

// RequireDocRevTreeEqual fails test if rev tree id's are not equal
func RequireDocRevTreeEqual(t *testing.T, expected, actual DocVersion) {
	require.Equal(t, expected.RevTreeID, actual.RevTreeID)
}

// RequireDocVersionNotEqual calls t.Fail if two document versions are equal.
func RequireDocVersionNotEqual(t *testing.T, expected, actual DocVersion) {
	require.NotEqual(t, expected.CV.String(), actual.CV.String(), "Versions mismatch.  Expected: %v, Actual: %v", expected, actual)
	require.NotEqual(t, expected.RevTreeID, actual.RevTreeID, "Versions mismatch.  Expected: %v, Actual: %v", expected.RevTreeID, actual.RevTreeID)
}

// RequireDocumentCV asserts that the document's CV matches the expected version.
func RequireDocumentCV(t *testing.T, expected DocVersion, actualVersion DocVersion) {
	require.Equal(t, expected.CV, actualVersion.CV)
}

// EmptyDocVersion represents an empty document version.
func EmptyDocVersion() *DocVersion {
	return nil
}

// NewDocVersionFromFakeRev returns a new DocVersion from the given fake rev ID, intended for use when we explicit create conflicts.
func NewDocVersionFromFakeRev(fakeRev string) DocVersion {
	return DocVersion{RevTreeID: fakeRev}
}

// DocVersionFromPutResponse returns a DocVersion from the given response, or fails the given test if a version was not returned.
func DocVersionFromPutResponse(t testing.TB, response *TestResponse) DocVersion {
	var r struct {
		DocID *string `json:"id"`
		RevID *string `json:"rev"`
		CV    *string `json:"cv"`
	}
	respBody := response.BodyString()
	require.NoErrorf(t, json.Unmarshal(response.BodyBytes(), &r), "error unmarshalling response body: %s", respBody)
	require.NotNilf(t, r.RevID, "expecting non-nil 'rev' from response: %s", respBody)
	require.NotNilf(t, r.CV, "expecting non-nil 'cv' from response: %s", respBody)
	require.NotEqualf(t, "", *r.RevID, "expecting non-empty 'rev' from response: %s", respBody)
	require.NotEqualf(t, "", *r.CV, "expecting non-empty 'cv' from response: %s", respBody)
	cv, err := db.ParseVersion(*r.CV)
	require.NoErrorf(t, err, "error parsing CV %q: %v", *r.CV, err)
	return DocVersion{RevTreeID: *r.RevID, CV: cv}
}

func MarshalConfig(t *testing.T, config db.ReplicationConfig) string {
	replicationPayload, err := json.Marshal(config)
	require.NoError(t, err)
	return string(replicationPayload)
}

func (sc *ServerContext) isDatabaseSuspended(t *testing.T, dbName string) bool {
	sc._databasesLock.RLock()
	defer sc._databasesLock.RUnlock()
	return sc._isDatabaseSuspended(dbName)
}

func (sc *ServerContext) suspendDatabase(t *testing.T, ctx context.Context, dbName string) error {
	sc._databasesLock.Lock()
	defer sc._databasesLock.Unlock()

	return sc._suspendDatabase(ctx, dbName)
}

// getRESTkeyspace returns a keyspace for REST URIs
func getRESTKeyspace(_ testing.TB, dbName string, collection *db.DatabaseCollection) string {
	if base.IsDefaultCollection(collection.ScopeName, collection.Name) {
		// for backwards compatibility (and user-friendliness),
		// we can optionally just use `/db/` instead of `/db._default._default/`
		// Return this format to get coverage of both formats.
		return dbName
	}
	return strings.Join([]string{dbName, collection.ScopeName, collection.Name}, base.ScopeCollectionSeparator)
}

// getScopeAndCollectionFromKeyspace returns the scope and collection from a keyspace, e.g. /db/ -> _default._default , or /db.scope1.collection1 -> scope1.collection1
func getScopeAndCollectionFromKeyspace(t testing.TB, keyspace string) string {
	_, scope, collection, err := ParseKeyspace(keyspace)
	require.NoError(t, err)
	if scope == nil && collection == nil {
		return strings.Join([]string{base.DefaultScope, base.DefaultCollection}, base.ScopeCollectionSeparator)
	}
	require.NotNil(t, scope, "Expected scope to be non-nil for %s", keyspace)
	require.NotNil(t, collection, "Expected collection to be non-nil for %s", keyspace)
	return strings.Join([]string{*scope, *collection}, base.ScopeCollectionSeparator)
}

// getKeyspaces returns the names of all the keyspaces on the rest tester. Currently assumes a single database.
func getKeyspaces(t testing.TB, database *db.DatabaseContext) []string {
	var keyspaces []string
	for _, collection := range database.CollectionByID {
		keyspaces = append(keyspaces, getRESTKeyspace(t, database.Name, collection))
	}
	sort.Strings(keyspaces)
	return keyspaces
}

// GetKeyspaces returns the names of all the keyspaces on the rest tester. Currently assumes a single database.
func (rt *RestTester) GetKeyspaces() []string {
	db := rt.GetDatabase()
	var keyspaces []string
	for _, collection := range db.CollectionByID {
		keyspaces = append(keyspaces, getRESTKeyspace(rt.TB(), db.Name, collection))
	}
	sort.Strings(keyspaces)
	return keyspaces
}

// GetDbCollections returns a lexicographically sorted list of collections on the database for compatibility with GetKeyspaces and getCollectionsForBLIP
func (rt *RestTester) GetDbCollections() []*db.DatabaseCollection {
	var collections []*db.DatabaseCollection
	for _, collection := range rt.GetDatabase().CollectionByID {
		collections = append(collections, collection)
	}
	sort.Slice(collections, func(i, j int) bool {
		return collections[i].ScopeName <= collections[j].ScopeName &&
			collections[i].Name < collections[j].Name
	})
	return collections
}

// GetSingleKeyspace the name of the keyspace if there is only one test collection on one database.
func (rt *RestTester) GetSingleKeyspace() string {
	db := rt.GetDatabase()
	require.Equal(rt.TB(), 1, len(db.CollectionByID), "Database must be configured with only one collection to use this function")
	for _, collection := range db.CollectionByID {
		return getRESTKeyspace(rt.TB(), db.Name, collection)
	}
	rt.TB().Fatal("Had no collection to return a keyspace for") // should be unreachable given length check above
	return ""
}

// getCollectionsForBLIP returns scope.collection strings for blip to process GetCollections messages. To test legacy functionality when SG_TEST_USE_DEFAULT_COLLECTION=true, don't return default collection if it is the only collection available.
func (rt *RestTester) getCollectionsForBLIP() []string {
	db := rt.GetDatabase()
	var collections []string
	if db.OnlyDefaultCollection() {
		return collections
	}
	for _, collection := range db.CollectionByID {
		collections = append(collections, base.ScopeAndCollectionName{
			Scope:      collection.ScopeName,
			Collection: collection.Name,
		}.String())
	}
	sort.Strings(collections)
	return collections
}

// ReadContinuousChanges reads the output continuous changes feed rest response into slice of ChangeEntry
func (rt *RestTester) ReadContinuousChanges(response *TestResponse) []db.ChangeEntry {
	var change db.ChangeEntry
	changes := make([]db.ChangeEntry, 0)
	reader := bufio.NewReader(response.Body)
	for {
		entry, readError := reader.ReadBytes('\n')
		if readError == io.EOF {
			// done
			break
		}
		require.NoError(rt.TB(), readError)
		entry = bytes.TrimSpace(entry)
		if len(entry) > 0 {
			require.NoError(rt.TB(), base.JSONUnmarshal(entry, &change))
			changes = append(changes, change)
			log.Printf("Got change ==> %v", change)
		}

	}
	return changes
}

// RequireContinuousFeedChangesCount Calls a changes feed on every collection and asserts that the nth expected change is
// the number of changes for the nth collection.
func (rt *RestTester) RequireContinuousFeedChangesCount(t testing.TB, username string, keyspace int, expectedChanges int, timeout int) {
	resp := rt.SendUserRequest("GET", fmt.Sprintf("/{{.keyspace%d}}/_changes?feed=continuous&timeout=%d", keyspace, timeout), "", username)
	changes := rt.ReadContinuousChanges(resp)
	require.Len(t, changes, expectedChanges)
}

// GetChanges returns the set of changes from a GET request for a given user.
func (rt *RestTester) GetChanges(uri string, username string) ChangesResults {
	changesResponse := rt.SendUserRequest(http.MethodGet, uri, "", username)
	RequireStatus(rt.TB(), changesResponse, http.StatusOK)
	var changes ChangesResults
	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(rt.TB(), err, "Error unmarshalling changes response")
	return changes
}

// PostChanges issues a changes POST request for a given user.
func (rt *RestTester) PostChanges(uri, body, username string) ChangesResults {
	changesResponse := rt.SendUserRequest(http.MethodPost, uri, body, username)
	RequireStatus(rt.TB(), changesResponse, http.StatusOK)
	var changes ChangesResults
	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(rt.TB(), err, "Error unmarshalling changes response")
	return changes
}

// PostChangesAdmin issues a changes POST request for a given user.
func (rt *RestTester) PostChangesAdmin(uri, body string) ChangesResults {
	changesResponse := rt.SendAdminRequest(http.MethodPost, uri, body)
	RequireStatus(rt.TB(), changesResponse, http.StatusOK)
	var changes ChangesResults
	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(rt.TB(), err, "Error unmarshalling changes response")
	return changes
}

// NewDbConfig returns a DbConfig for the given RestTester. This sets up a config appropriate to collections, xattrs, import filter and sync function.
func (rt *RestTester) NewDbConfig() DbConfig {
	// make sure bucket has been initialized
	config := DbConfig{
		BucketConfig: BucketConfig{
			Bucket: base.Ptr(rt.Bucket().GetName()),
		},
		EnableXattrs: base.Ptr(base.TestUseXattrs()),
	}
	if base.TestsDisableGSI() {
		// Walrus is peculiar in that it needs to run with views, but can run most GSI tests, including collections
		if !base.UnitTestUrlIsWalrus() {
			config.UseViews = base.Ptr(true)
		}
	} else {
		config.Index = &IndexConfig{
			NumReplicas: base.Ptr(uint(0)),
		}
	}

	if base.TestDisableRevCache() {
		config.CacheConfig = &CacheConfig{
			RevCacheConfig: &RevCacheConfig{
				MaxItemCount: base.Ptr[uint32](0),
			},
		}
	}

	// Setup scopes.
	if base.TestsUseNamedCollections() && rt.collectionConfig != useSingleCollectionDefaultOnly && (base.UnitTestUrlIsWalrus() || config.useGSI()) {
		config.Scopes = GetCollectionsConfigWithFiltering(rt.TB(), rt.TestBucket, rt.numCollections, stringPtrOrNil(rt.SyncFn), stringPtrOrNil(rt.ImportFilter))
	} else {
		config.Sync = stringPtrOrNil(rt.SyncFn)
		config.ImportFilter = stringPtrOrNil(rt.ImportFilter)
	}

	if rt.GuestEnabled {
		config.Guest = &auth.PrincipalConfig{
			Name:     stringPtrOrNil(base.GuestUsername),
			Disabled: base.Ptr(false),
		}
		setChannelsAllCollections(config, config.Guest, "*")
	}

	return config
}

// updatePersistedConfig is a helper function to update the persisted db config for a given db in the rest tester, bypassing the REST API and not (yet) reloading the database.
// this can be used to test upgrades or changes in behaviour on older configurations (i.e. change a config to an older state that may not pass new validation rules)
func (rt *RestTester) updatePersistedConfig(dbName string, updateFunc func(*DatabaseConfig)) {
	// it's safe to just hold the read lock to fetch 'dbName' entry, since we're not modifying the map itself (adding or removing a database by name)
	rt.ServerContext()._databasesLock.RLock()
	defer rt.ServerContext()._databasesLock.RUnlock()
	updateFunc(&rt.ServerContext()._dbConfigs[dbName].DatabaseConfig)
}

func setChannelsAllCollections(dbConfig DbConfig, principal *auth.PrincipalConfig, channels ...string) {
	if dbConfig.Scopes == nil {
		principal.ExplicitChannels = base.SetOf(channels...)
		return
	}
	for scope, scopeConfig := range dbConfig.Scopes {
		for collection := range scopeConfig.Collections {
			principal.SetExplicitChannels(scope, collection, "*")
		}
	}
}

// stringPtrOrNil returns a stringPtr for the given string, or nil if the string is empty
func stringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return base.Ptr(s)
}

func DropAllTestIndexes(t *testing.T, tb *base.TestBucket) {
	dropAllNonPrimaryIndexes(t, tb.GetMetadataStore())

	dsNames := tb.GetNonDefaultDatastoreNames()
	for i := range dsNames {
		ds, err := tb.GetNamedDataStore(i)
		require.NoError(t, err)
		dropAllNonPrimaryIndexes(t, ds)
	}
}

func DropAllTestIndexesIncludingPrimary(t *testing.T, tb *base.TestBucket) {

	ctx := base.TestCtx(t)
	n1qlStore, ok := base.AsN1QLStore(tb.GetMetadataStore())
	require.True(t, ok)
	dropErr := base.DropAllIndexes(ctx, n1qlStore)
	require.NoError(t, dropErr)

	dsNames := tb.GetNonDefaultDatastoreNames()
	for i := range dsNames {
		ds, err := tb.GetNamedDataStore(i)
		require.NoError(t, err)
		n1qlStore, ok := base.AsN1QLStore(ds)
		require.True(t, ok)
		dropErr := base.DropAllIndexes(ctx, n1qlStore)
		require.NoError(t, dropErr)
	}
}

func (sc *ServerContext) RequireInvalidDatabaseConfigNames(t *testing.T, expectedDbNames []string) {
	sc.invalidDatabaseConfigTracking.m.RLock()
	defer sc.invalidDatabaseConfigTracking.m.RUnlock()

	dbNames := make([]string, 0, len(sc.invalidDatabaseConfigTracking.dbNames))

	for name := range sc.invalidDatabaseConfigTracking.dbNames {
		dbNames = append(dbNames, name)
	}
	require.ElementsMatch(t, expectedDbNames, dbNames)
}

// ForceDbConfigsReload forces the reload db config from bucket process (like the ConfigUpdate background process)
func (sc *ServerContext) ForceDbConfigsReload(t *testing.T, ctx context.Context) {
	_, err := sc.fetchAndLoadConfigs(ctx, false)
	require.NoError(t, err)
}

// AllInvalidDatabaseNames returns the names of all the databases that have invalid configs. Testing only since this locks the database context.
func (sc *ServerContext) AllInvalidDatabaseNames(_ *testing.T) []string {
	sc.invalidDatabaseConfigTracking.m.RLock()
	defer sc.invalidDatabaseConfigTracking.m.RUnlock()
	dbs := make([]string, 0, len(sc.invalidDatabaseConfigTracking.dbNames))
	for db := range sc.invalidDatabaseConfigTracking.dbNames {
		dbs = append(dbs, db)
	}
	return dbs
}

// Calls DropAllIndexes to remove all indexes, then restores the primary index for TestBucketPool readier requirements
func dropAllNonPrimaryIndexes(t *testing.T, dataStore base.DataStore) {

	n1qlStore, ok := base.AsN1QLStore(dataStore)
	require.True(t, ok)
	ctx := base.TestCtx(t)
	dropErr := base.DropAllIndexes(ctx, n1qlStore)
	require.NoError(t, dropErr)
	err := n1qlStore.CreatePrimaryIndex(ctx, base.PrimaryIndexName, nil)
	require.NoError(t, err, "Unable to recreate primary index")
}

// RequireBucketSpecificCredentials skips tests if bucket specific credentials are required
func RequireBucketSpecificCredentials(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server since rosmar has no bucket specific credentials")
	}
}

// RequireN1QLIndexes skips tests if N1QL indexes are required
func RequireN1QLIndexes(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server since rosmar has no support for N1QL indexes")
	}
}

// RequireGocbDCPResync skips tests if not gocb backed buckets.
func RequireGocbDCPResync(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server since rosmar has no support for DCP resync")
	}
}

// SafeDatabaseName returns a database name free of any special characters for use in tests.
func SafeDatabaseName(t *testing.T, name string) string {
	dbName := strings.ToLower(name)
	for _, c := range []string{" ", "<", ">", "/", "="} {
		dbName = strings.ReplaceAll(dbName, c, "_")
	}
	return dbName
}

// SafeDocumentName returns a document name free of any special characters for use in tests.
func SafeDocumentName(t *testing.T, name string) string {
	docName := strings.ToLower(name)
	for _, c := range []string{" ", "<", ">", "/", "="} {
		docName = strings.ReplaceAll(docName, c, "_")
	}
	require.Less(t, len(docName), 251, "Document name %s is too long, must be less than 251 characters", name)
	return docName
}

func JsonToMap(t *testing.T, jsonStr string) map[string]any {
	result := make(map[string]any)
	err := json.Unmarshal([]byte(jsonStr), &result)
	require.NoError(t, err)
	return result
}

// reloadDatabaseWithConfigLoadFromBucket forces reload of db as if it was being picked up from the bucket
func (sc *ServerContext) reloadDatabaseWithConfigLoadFromBucket(nonContextStruct base.NonCancellableContext, config DatabaseConfig) error {
	sc._databasesLock.Lock()
	defer sc._databasesLock.Unlock()
	return sc._reloadDatabaseWithConfig(nonContextStruct.Ctx, config, true, true)
}

// TestBucketPoolRestWithIndexes is the main function that should be used for TestMain in subpackages of rest.
func TestBucketPoolRestWithIndexes(ctx context.Context, m *testing.M, tbpOptions base.TestBucketPoolOptions) {
	tbpOptions.TeardownFuncs = append(tbpOptions.TeardownFuncs, func() {
		if len(globalBlipTesterClients.m) != 0 {
			// must panic to bubble up through test harness
			panic(fmt.Sprintf("%v active blip tester clients should be 0 at end of tests", globalBlipTesterClients.m))
		}
	})
	db.TestBucketPoolWithIndexes(ctx, m, tbpOptions)
}

func RequireNotFoundError(t *testing.T, response *TestResponse) {
	RequireStatus(t, response, http.StatusNotFound)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	require.Equal(t, db.Body{"error": "not_found", "reason": "missing"}, body)
}

func AssertHTTPErrorReason(t testing.TB, response *TestResponse, expectedStatus int, expectedReason string) {
	var httpError struct {
		Reason string `json:"reason"`
	}
	err := base.JSONUnmarshal(response.BodyBytes(), &httpError)
	require.NoError(t, err, "Failed to unmarshal HTTP error: %v", response.BodyBytes())

	AssertStatus(t, response, expectedStatus)

	assert.Equal(t, expectedReason, httpError.Reason)
}

func AssertRevTreeAfterHLVConflictResolution(t *testing.T, doc *db.Document, expectedActiveRevTreeID, expectedTombstoneParentID string) {
	activeLeafCount := 0
	for _, revID := range doc.History.GetLeaves() {
		revItem := doc.History[revID]
		if revItem.Deleted {
			assert.Equal(t, expectedTombstoneParentID, revItem.Parent)
		} else {
			activeLeafCount++
			assert.Equal(t, expectedActiveRevTreeID, revID)
		}
	}
	// should only have one active leaf
	require.Equal(t, 1, activeLeafCount)
}
