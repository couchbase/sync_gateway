/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// WaitForPrimaryIndexEmpty waits for #primary to be empty.
// Workaround SG #3570 by doing a polling loop until the star channel query returns 0 results.
func WaitForPrimaryIndexEmpty(ctx context.Context, store base.N1QLStore) error {

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {
		empty, err := isPrimaryIndexEmpty(ctx, store)
		if err != nil {
			return true, err, nil
		}
		return !empty, nil, empty
	}

	// Kick off the retry loop
	err, _ := base.RetryLoop(
		ctx,
		"Wait for index to be empty",
		retryWorker,
		base.CreateMaxDoublingSleeperFunc(60, 500, 5000),
	)
	var retryError *base.RetryTimeoutError
	if errors.As(err, &retryError) {
		documents, err := getPrimaryIndexDocuments(ctx, store, true)
		if err != nil {
			return fmt.Errorf("Error getting documents from primary index: %w", err)
		}
		return fmt.Errorf("Documents left behind after waiting for primary index to be emptied: %s", documents)
	}
	return err
}

// isPrimaryIndexEmpty returns true if there are no documents in the primary index
func isPrimaryIndexEmpty(ctx context.Context, store base.N1QLStore) (bool, error) {
	// only look for a single doc to make query faster
	docs, err := getPrimaryIndexDocuments(ctx, store, false)
	if err != nil {
		return false, err
	}
	return len(docs) == 0, err
}

// getPrimaryIndexDocuments returs true if there are no documents in the primary index and returns the documents. If allDocuments is false, only check for a single document.
func getPrimaryIndexDocuments(ctx context.Context, store base.N1QLStore, allDocuments bool) ([]string, error) {
	// Create the star channel query
	statement := fmt.Sprintf("SELECT META().id FROM %s", base.KeyspaceQueryToken)
	if !allDocuments {
		statement += " LIMIT 1"
	}
	params := map[string]interface{}{}
	params[QueryParamStartSeq] = 0
	params[QueryParamEndSeq] = N1QLMaxInt64

	// Execute the query
	results, err := store.Query(ctx, statement, params, base.RequestPlus, true)

	// If there was an error, then retry.  Assume it's an "index rollback" error which happens as
	// the index processes the bucket flush operation
	if err != nil {
		return nil, err
	}

	var documents []string
	var queryRow map[string]string
	for results.Next(ctx, &queryRow) {
		documents = append(documents, queryRow["id"])
	}
	err = results.Close()
	return documents, err
}

func (db *DatabaseContext) CacheCompactActive() bool {
	channelCache := db.changeCache.getChannelCache()
	compactingCache, ok := channelCache.(*channelCacheImpl)
	if !ok {
		return false
	}
	return compactingCache.isCompactActive()
}

func (db *DatabaseContext) WaitForCaughtUp(targetCount int64) error {
	for i := 0; i < 100; i++ {
		// caughtUpCount := base.ExpvarVar2Int(db.DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsCaughtUp))
		caughtUpCount := db.DbStats.CBLReplicationPull().NumPullReplCaughtUp.Value()
		if caughtUpCount >= targetCount {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("WaitForCaughtUp didn't catch up")
}

func (db *DatabaseContext) WaitForTotalCaughtUp(targetCount int64) error {
	for i := 0; i < 100; i++ {
		caughtUpCount := db.DbStats.CBLReplicationPull().NumPullReplTotalCaughtUp.Value()
		if caughtUpCount >= targetCount {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("WaitForCaughtUp didn't catch up")
}

type StatWaiter struct {
	initCount   int64            // Document cached count when NewStatWaiter is called
	targetCount int64            // Target count used when Wait is called
	stat        *base.SgwIntStat // Stat to wait on
	tb          testing.TB       // Raises tb.Fatalf on wait timeout
}

func (db *DatabaseContext) NewStatWaiter(stat *base.SgwIntStat, tb testing.TB) *StatWaiter {
	return &StatWaiter{
		initCount:   stat.Value(),
		targetCount: stat.Value(),
		stat:        stat,
		tb:          tb,
	}
}

func (db *DatabaseContext) NewDCPCachingCountWaiter(tb testing.TB) *StatWaiter {
	return db.NewStatWaiter(db.DbStats.Database().DCPCachingCount, tb)
}

func (db *DatabaseContext) NewPullReplicationCaughtUpWaiter(tb testing.TB) *StatWaiter {
	return db.NewStatWaiter(db.DbStats.CBLReplicationPull().NumPullReplCaughtUp, tb)
}

func (db *DatabaseContext) NewCacheRevsActiveWaiter(tb testing.TB) *StatWaiter {
	return db.NewStatWaiter(db.DbStats.Cache().ChannelCacheRevsActive, tb)
}

func (sw *StatWaiter) Add(count int) {
	sw.targetCount += int64(count)
}

func (sw *StatWaiter) AddAndWait(count int) {
	sw.targetCount += int64(count)
	sw.Wait()
}

// Wait uses backoff retry for up to ~33s
func (sw *StatWaiter) Wait() {
	actualCount := sw.stat.Value()
	if actualCount >= sw.targetCount {
		return
	}

	waitTime := 1 * time.Millisecond
	for i := 0; i < 14; i++ {
		waitTime *= 2
		time.Sleep(waitTime)
		actualCount = sw.stat.Value()
		if actualCount >= sw.targetCount {
			return
		}
	}

	sw.tb.Fatalf("StatWaiter.Wait timed out waiting for stat to reach %d (actual: %d) %s", sw.targetCount, actualCount, base.GetCallersName(2, true))
}

func AssertEqualBodies(t *testing.T, expected, actual Body) {
	expectedCanonical, err := base.JSONMarshalCanonical(expected)
	assert.NoError(t, err)
	actualCanonical, err := base.JSONMarshalCanonical(actual)
	assert.NoError(t, err)
	assert.Equal(t, string(expectedCanonical), string(actualCanonical))
}

func WaitForUserWaiterChange(userWaiter *ChangeWaiter) bool {
	var isChanged bool
	for i := 0; i < 100; i++ {
		isChanged = userWaiter.RefreshUserCount()
		if isChanged {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return isChanged
}

// EmptyPrimaryIndex deletes all docs from primary index
func EmptyPrimaryIndex(ctx context.Context, dataStore sgbucket.DataStore) error {
	n1qlStore, ok := base.AsN1QLStore(dataStore)
	if !ok {
		return fmt.Errorf("bucket was not a n1ql store")
	}

	statement := `DELETE FROM ` + base.KeyspaceQueryToken
	results, err := n1qlStore.Query(ctx, statement, nil, base.RequestPlus, true)
	if err != nil {
		return err
	}
	return results.Close()
}

// emptyAllDocsIndex ensures the AllDocs index for the given bucket is empty, including tombstones which aren't found when emptying the primary index.
func emptyAllDocsIndex(ctx context.Context, dataStore sgbucket.DataStore, tbp *base.TestBucketPool) (numCompacted int, err error) {
	purgedDocCount := 0
	purgeBody := Body{"_purged": true}

	n1qlStore, ok := base.AsN1QLStore(dataStore)
	if !ok {
		return 0, fmt.Errorf("bucket was not a n1ql store")
	}

	// A stripped down version of db.Compact() that works on AllDocs instead of tombstones
	statement := `SELECT META(ks).id AS id
FROM ` + base.KeyspaceQueryToken + ` AS ks USE INDEX (sg_allDocs_x1)`
	statement += " WHERE META(ks).xattrs._sync.`sequence` IS NOT MISSING"
	statement += ` AND META(ks).id NOT LIKE '\\_sync:%'`
	results, err := n1qlStore.Query(ctx, statement, nil, base.RequestPlus, true)
	if err != nil {
		return 0, err
	}

	var purgeErrors *base.MultiError
	var row QueryIdRow
	for results.Next(ctx, &row) {
		// First, attempt to purge.
		var purgeErr error
		if base.TestUseXattrs() {
			purgeErr = dataStore.DeleteWithXattrs(ctx, row.Id, []string{base.SyncXattrName})
		} else {
			purgeErr = dataStore.Delete(row.Id)
		}
		if base.IsKeyNotFoundError(dataStore, purgeErr) {
			// If key no longer exists, need to add and remove to trigger removal from view
			_, addErr := dataStore.Add(row.Id, 0, purgeBody)
			if addErr != nil {
				purgeErrors = purgeErrors.Append(addErr)
				tbp.Logf(ctx, "Error adding key %s to force deletion. %v", row.Id, addErr)
				continue
			}

			if delErr := dataStore.Delete(row.Id); delErr != nil {
				purgeErrors = purgeErrors.Append(delErr)
				tbp.Logf(ctx, "Error deleting key %s.  %v", row.Id, delErr)
			}
			purgedDocCount++
		} else if purgeErr != nil {
			purgeErrors = purgeErrors.Append(purgeErr)
			tbp.Logf(ctx, "Error removing key %s (purge). %v", row.Id, purgeErr)
		}
	}
	err = results.Close()
	if err != nil {
		return 0, err
	}

	tbp.Logf(ctx, "Finished emptying all docs index ... Total docs purged: %d", purgedDocCount)
	return purgedDocCount, purgeErrors.ErrorOrNil()
}

// viewsAndGSIBucketReadier empties the bucket, initializes Views, and waits until GSI indexes are empty. It is run asynchronously as soon as a test is finished with a bucket.
var viewsAndGSIBucketReadier base.TBPBucketReadierFunc = func(ctx context.Context, b base.Bucket, tbp *base.TestBucketPool) error {
	if base.TestsDisableGSI() {
		tbp.Logf(ctx, "flushing bucket and readying views")
		if err := base.FlushBucketEmptierFunc(ctx, b, tbp); err != nil {
			return err
		}
		// Exit early if we're not using GSI.
		return viewBucketReadier(ctx, b.DefaultDataStore(), tbp)
	}

	tbp.Logf(ctx, "emptying bucket via N1QL, readying views and indexes")
	if err := base.N1QLBucketEmptierFunc(ctx, b, tbp); err != nil {
		return err
	}

	dataStores, err := b.ListDataStores()
	if err != nil {
		return err
	}
	for _, dataStoreName := range dataStores {
		dataStore, err := b.NamedDataStore(dataStoreName)
		if err != nil {
			return err
		}
		if _, err := emptyAllDocsIndex(ctx, dataStore, tbp); err != nil {
			return err
		}
		if err := EmptyPrimaryIndex(ctx, dataStore); err != nil {
			return err
		}
		n1qlStore, ok := base.AsN1QLStore(dataStore)
		if !ok {
			return errors.New("attempting to empty indexes with non-N1QL store")
		}
		tbp.Logf(ctx, "waiting for empty bucket indexes %s.%s.%s", b.GetName(), dataStore.ScopeName(), dataStore.CollectionName())
		// we can't init indexes concurrently, so we'll just wait for them to be empty after emptying instead of recreating.
		if err := WaitForPrimaryIndexEmpty(ctx, n1qlStore); err != nil {
			tbp.Logf(ctx, "waitForPrimaryIndexEmpty returned an error: %v", err)
			return err
		}
		tbp.Logf(ctx, "bucket indexes empty")
	}
	if len(dataStores) == 1 {
		dataStoreName := dataStores[0]
		if base.IsDefaultCollection(dataStoreName.ScopeName(), dataStoreName.CollectionName()) {
			dataStore, err := b.NamedDataStore(dataStoreName)
			if err != nil {
				return err
			}
			if err := viewBucketReadier(ctx, dataStore, tbp); err != nil {
				return err
			}
		}
	}
	return nil
}

// viewsAndGSIBucketInit is run synchronously only once per-bucket to do any initial setup. For non-integration Walrus buckets, this is run for each new Walrus bucket.
var viewsAndGSIBucketInit base.TBPBucketInitFunc = func(ctx context.Context, b base.Bucket, tbp *base.TestBucketPool) error {
	skipGSI := false

	if base.TestsDisableGSI() {
		tbp.Logf(ctx, "bucket not a gocb bucket... skipping GSI setup")
		skipGSI = true
	}

	tbp.Logf(ctx, "Starting bucket init function")

	dataStores, err := b.ListDataStores()
	if err != nil {
		return err
	}

	for _, dataStoreName := range dataStores {
		ctx := base.KeyspaceLogCtx(ctx, b.GetName(), dataStoreName.ScopeName(), dataStoreName.CollectionName())
		dataStore, err := b.NamedDataStore(dataStoreName)
		if err != nil {
			return err
		}

		// Views
		if skipGSI || base.TestsDisableGSI() {
			if err := viewBucketReadier(ctx, dataStore, tbp); err != nil {
				return err
			}
			continue
		}

		// GSI
		n1qlStore, ok := base.AsN1QLStore(dataStore)
		if !ok {
			return fmt.Errorf("bucket %T was not a N1QL store", b)
		}

		tbp.Logf(ctx, "dropping existing bucket indexes")
		if err := base.DropAllIndexes(ctx, n1qlStore); err != nil {
			tbp.Logf(ctx, "Failed to drop bucket indexes: %v", err)
			return err
		}
		tbp.Logf(ctx, "creating SG bucket indexes")
		options := InitializeIndexOptions{
			UseXattrs:       base.TestUseXattrs(),
			NumReplicas:     0,
			FailFast:        false,
			Serverless:      false,
			MetadataIndexes: IndexesWithoutMetadata,
		}
		if base.IsDefaultCollection(dataStore.ScopeName(), dataStore.CollectionName()) {
			options.MetadataIndexes = IndexesAll
		}
		if err := InitializeIndexes(ctx, n1qlStore, options); err != nil {
			return err
		}

		err = n1qlStore.CreatePrimaryIndex(ctx, base.PrimaryIndexName, nil)
		if err != nil {
			return err
		}
		tbp.Logf(ctx, "finished creating SG bucket indexes")
	}
	return nil
}

// viewBucketReadier removes any existing views and installs a new set into the given bucket.
func viewBucketReadier(ctx context.Context, dataStore base.DataStore, tbp *base.TestBucketPool) error {
	viewStore, ok := base.AsViewStore(dataStore)
	if !ok {
		return fmt.Errorf("dataStore %T was not a View store", dataStore)
	}
	ddocs, err := viewStore.GetDDocs()
	if err != nil {
		return err
	}

	for ddocName := range ddocs {
		tbp.Logf(ctx, "removing existing view: %s", ddocName)
		if err := viewStore.DeleteDDoc(ddocName); err != nil {
			return err
		}
	}

	tbp.Logf(ctx, "initializing bucket views")
	err = InitializeViews(ctx, dataStore)
	if err != nil {
		return err
	}

	tbp.Logf(ctx, "bucket views initialized")
	return nil
}

func (db *DatabaseContext) GetChannelQueryCount() int64 {
	if db.UseViews() {
		return db.DbStats.Query(fmt.Sprintf(base.StatViewFormat, DesignDocSyncGateway(), ViewChannels)).QueryCount.Value()
	}

	return db.DbStats.Query(QueryTypeChannels).QueryCount.Value()
}

// GetLocalActiveReplicatorForTest is a test util for retrieving an Active Replicator for deeper introspection/assertions.
func (m *sgReplicateManager) GetLocalActiveReplicatorForTest(t testing.TB, replicationID string) (ar *ActiveReplicator, ok bool) {
	// Check if replication is assigned locally
	m.activeReplicatorsLock.RLock()
	replication, isLocal := m.activeReplicators[replicationID]
	m.activeReplicatorsLock.RUnlock()
	return replication, isLocal
}

// SuspendSequenceBatching disables sequence batching for multi-RT tests (pending CBG-1000)
func SuspendSequenceBatching() func() {
	oldFrequency := MaxSequenceIncrFrequency
	MaxSequenceIncrFrequency = 0 * time.Millisecond
	return func() { MaxSequenceIncrFrequency = oldFrequency }
}

// Public channel view call - for unit test support
func (dbc *DatabaseContext) ChannelViewForTest(tb testing.TB, channelName string, startSeq, endSeq uint64) (LogEntries, error) {
	collection, err := dbc.GetDefaultDatabaseCollection()
	if err != nil {
		return nil, err
	}
	return dbc.CollectionChannelViewForTest(tb, collection, channelName, startSeq, endSeq)
}

func (dbc *DatabaseContext) CollectionChannelViewForTest(tb testing.TB, collection *DatabaseCollection, channelName string, startSeq, endSeq uint64) (LogEntries, error) {
	return collection.getChangesInChannelFromQuery(base.TestCtx(tb), channelName, startSeq, endSeq, 0, false)
}

// Test-only version of GetPrincipal that doesn't trigger channel/role recalculation
func (dbc *DatabaseContext) GetPrincipalForTest(tb testing.TB, name string, isUser bool) (info *auth.PrincipalConfig, err error) {
	ctx := base.TestCtx(tb)
	var princ auth.Principal
	if isUser {
		princ, err = dbc.Authenticator(ctx).GetUser(name)
	} else {
		princ, err = dbc.Authenticator(ctx).GetRole(name)
	}
	if princ == nil {
		return
	}
	info = new(auth.PrincipalConfig)
	info.Name = &name
	info.ExplicitChannels = princ.CollectionExplicitChannels(base.DefaultScope, base.DefaultCollection).AsSet()
	if user, ok := princ.(auth.User); ok {
		info.Channels = user.InheritedCollectionChannels(base.DefaultScope, base.DefaultCollection).AsSet()
		email := user.Email()
		info.Email = &email
		info.Disabled = base.BoolPtr(user.Disabled())
		info.ExplicitRoleNames = user.ExplicitRoles().AsSet()
		info.RoleNames = user.RoleNames().AllKeys()
	} else {
		info.Channels = princ.Channels().AsSet()
	}
	return
}

// TestBucketPoolWithIndexes runs a TestMain for packages that require creation of indexes
func TestBucketPoolWithIndexes(ctx context.Context, m *testing.M, tbpOptions base.TestBucketPoolOptions) {
	base.TestBucketPoolMain(ctx, m, viewsAndGSIBucketReadier, viewsAndGSIBucketInit, tbpOptions)
}

// Parse the plan looking for use of the fetch operation (appears as the key/value pair "#operator":"Fetch")
// If there's no fetch operator in the plan, we can assume the query is covered by the index.
// The plan returned by an EXPLAIN is a nested hierarchy with operators potentially appearing at different
// depths, so need to traverse the JSON object.
// https://docs.couchbase.com/server/6.0/n1ql/n1ql-language-reference/explain.html
func IsCovered(plan map[string]interface{}) bool {
	for key, value := range plan {
		switch value := value.(type) {
		case string:
			if key == "#operator" && value == "Fetch" {
				return false
			}
		case map[string]interface{}:
			if !IsCovered(value) {
				return false
			}
		case []interface{}:
			for _, arrayValue := range value {
				jsonArrayValue, ok := arrayValue.(map[string]interface{})
				if ok {
					if !IsCovered(jsonArrayValue) {
						return false
					}
				}
			}
		default:
		}
	}
	return true
}

// If certain environment variables are set, for example to turn on XATTR support, then update
// the DatabaseContextOptions accordingly
func AddOptionsFromEnvironmentVariables(dbcOptions *DatabaseContextOptions) {
	if base.TestUseXattrs() {
		dbcOptions.EnableXattr = true
	}

	if base.TestsDisableGSI() {
		dbcOptions.UseViews = true
	}
}

// Sets up test db with the specified database context options.  Note that environment variables can
// override somedbcOptions properties.
func SetupTestDBWithOptions(t testing.TB, dbcOptions DatabaseContextOptions) (*Database, context.Context) {
	tBucket := base.GetTestBucket(t)
	return SetupTestDBForDataStoreWithOptions(t, tBucket, dbcOptions)
}

func SetupTestDBForDataStoreWithOptions(t testing.TB, tBucket *base.TestBucket, dbcOptions DatabaseContextOptions) (*Database, context.Context) {
	ctx := base.TestCtx(t)
	AddOptionsFromEnvironmentVariables(&dbcOptions)
	if dbcOptions.Scopes == nil {
		dbcOptions.Scopes = GetScopesOptions(t, tBucket, 1)
	}

	dbCtx, err := NewDatabaseContext(ctx, "db", tBucket, false, dbcOptions)
	require.NoError(t, err, "Couldn't create context for database 'db'")

	err = dbCtx.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	db, err := CreateDatabase(dbCtx)
	require.NoError(t, err, "Couldn't create database 'db'")

	ctx = db.AddDatabaseLogContext(ctx)
	return db, ctx
}

// GetScopesOptions sets up a ScopesOptions from a TestBucket. This will set up default or non default collections depending on the test harness use of SG_TEST_USE_DEFAULT_COLLECTION and whether the backing store supports collections.
func GetScopesOptions(t testing.TB, testBucket *base.TestBucket, numCollections int) ScopesOptions {
	if !base.TestsUseNamedCollections() {
		if numCollections != 1 {
			t.Fatal("Setting numCollections on a test that can't use collections is invalid")
		}
		return GetScopesOptionsDefaultCollectionOnly(t)
	}
	// Get a datastore as provided by the test
	stores := testBucket.GetNonDefaultDatastoreNames()
	require.True(t, len(stores) >= numCollections, "Requested more collections %d than found on testBucket %d", numCollections, len(stores))

	scopesConfig := ScopesOptions{}
	for i := 0; i < numCollections; i++ {
		dataStoreName := stores[i]
		if scopeConfig, ok := scopesConfig[dataStoreName.ScopeName()]; ok {
			if _, ok := scopeConfig.Collections[dataStoreName.CollectionName()]; ok {
				// already present
			} else {
				scopeConfig.Collections[dataStoreName.CollectionName()] = CollectionOptions{}
			}
		} else {
			scopesConfig[dataStoreName.ScopeName()] = ScopeOptions{
				Collections: map[string]CollectionOptions{
					dataStoreName.CollectionName(): {},
				}}
		}

	}
	return scopesConfig
}

// Return Scopes options without any configuration for only the default collection.
func GetScopesOptionsDefaultCollectionOnly(_ testing.TB) ScopesOptions {
	return map[string]ScopeOptions{
		base.DefaultScope: ScopeOptions{
			Collections: map[string]CollectionOptions{
				base.DefaultCollection: {},
			},
		},
	}
}

func GetSingleDatabaseCollectionWithUser(tb testing.TB, database *Database) *DatabaseCollectionWithUser {
	return &DatabaseCollectionWithUser{
		DatabaseCollection: GetSingleDatabaseCollection(tb, database.DatabaseContext),
		user:               database.user,
	}
}

func GetSingleDatabaseCollection(tb testing.TB, database *DatabaseContext) *DatabaseCollection {
	require.Equal(tb, 1, len(database.CollectionByID), fmt.Sprintf("Database must only have a single collection configured has %d", len(database.CollectionByID)))
	for _, collection := range database.CollectionByID {
		return collection
	}
	tb.Fatalf("Could not find a collection")
	return nil
}

// AllocateTestSequence allocates a sequence via the sequenceAllocator.  For use by non-db tests
func AllocateTestSequence(database *DatabaseContext) (uint64, error) {
	return database.sequences.incrementSequence(1)
}

// ReleaseTestSequence releases a sequence via the sequenceAllocator.  For use by non-db tests
func ReleaseTestSequence(ctx context.Context, database *DatabaseContext, sequence uint64) error {
	return database.sequences.releaseSequence(ctx, sequence)
}

func (a *ActiveReplicator) GetActiveReplicatorConfig() *ActiveReplicatorConfig {
	return a.config
}

func (apr *ActivePullReplicator) GetBlipSender() *blip.Sender {
	return apr.blipSender
}

func DefaultMutateInOpts() *sgbucket.MutateInOptions {
	return &sgbucket.MutateInOptions{
		MacroExpansion: macroExpandSpec(base.SyncXattrName),
	}
}
