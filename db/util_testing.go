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
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

// purgeWithDCPFeed purges all documents seen on a DCP feed with system xattrs, including tombstones which aren't found when emptying the primary index.
func purgeWithDCPFeed(ctx context.Context, dataStore sgbucket.DataStore, tbp *base.TestBucketPool) (numCompacted int, err error) {
	purgeTimeout := 60 * time.Second
	purgeBody := Body{"_purged": true}
	var processedDocCount atomic.Int64
	var purgedDocCount atomic.Int64

	var purgeErrors *base.MultiError
	collection, err := base.AsCollection(dataStore)
	if err != nil {
		return 0, fmt.Errorf("dataStore was not a gocb collection: %w", err)
	}

	var collectionIDs []uint32
	if collection.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		collectionIDs = append(collectionIDs, collection.GetCollectionID())
	}

	dcpClientOpts := base.DCPClientOptions{
		OneShot:           true,
		FailOnRollback:    false,
		CollectionIDs:     collectionIDs,
		MetadataStoreType: base.DCPMetadataStoreInMemory,
	}

	purgeCallback := func(event sgbucket.FeedEvent) bool {
		var purgeErr error

		processedDocCount.Add(1)
		// We only need to purge mutations/deletions
		if event.Opcode != sgbucket.FeedOpMutation && event.Opcode != sgbucket.FeedOpDeletion {
			return false
		}

		// If it's a deletion but doesn't have xattrs, ignore it
		if event.Opcode == sgbucket.FeedOpDeletion && event.DataType&base.MemcachedDataTypeXattr == 0 {
			return false
		}

		key := string(event.Key)

		if base.TestUseXattrs() {
			purgeErr = dataStore.DeleteWithXattrs(ctx, key, []string{base.SyncXattrName})
		} else {
			purgeErr = dataStore.Delete(key)
		}
		if base.IsDocNotFoundError(purgeErr) {
			// If key no longer exists, need to add and remove to trigger removal from view
			_, addErr := dataStore.Add(key, 0, purgeBody)
			if addErr != nil {
				purgeErrors = purgeErrors.Append(addErr)
				tbp.Logf(ctx, "Error adding key %s to force deletion. %v", key, addErr)
				return false
			}

			if delErr := dataStore.Delete(key); delErr != nil {
				purgeErrors = purgeErrors.Append(delErr)
				tbp.Logf(ctx, "Error deleting key %s.  %v", key, delErr)
			}
			purgedDocCount.Add(1)
		} else if purgeErr != nil {
			purgeErrors = purgeErrors.Append(purgeErr)
			tbp.Logf(ctx, "Error removing key %s (purge). %v", key, purgeErr)
		}
		return false
	}
	feedID := "purgeFeed-" + collection.CollectionName()
	dcpClient, err := base.NewDCPClient(ctx, feedID, purgeCallback, dcpClientOpts, collection.Bucket)
	if err != nil {
		return 0, err
	}
	doneChan, err := dcpClient.Start()
	if err != nil {
		return 0, fmt.Errorf("error starting purge DCP feed: %w", err)
	}
	// wait for feed to complete
	timeout := time.After(purgeTimeout)
	select {
	case err := <-doneChan:
		if err != nil {
			tbp.Logf(ctx, "purgeDCPFeed finished with error: %v", err)
		}
	case <-timeout:
		return 0, fmt.Errorf("timeout waiting for purge DCP feed to complete")
	}
	closeErr := dcpClient.Close()
	if closeErr != nil {
		tbp.Logf(ctx, "error closing purge DCP feed: %v", closeErr)
	}

	tbp.Logf(ctx, "Finished purge DCP feed ... Total docs purged: %d", purgedDocCount.Load())
	tbp.Logf(ctx, "Finished purge DCP feed ... Total docs processed: %d", processedDocCount.Load())
	return int(purgedDocCount.Load()), purgeErrors.ErrorOrNil()
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

	tbp.Logf(ctx, "emptying bucket via DCP purge")
	dataStores, err := b.ListDataStores()
	if err != nil {
		return err
	}
	for _, dataStoreName := range dataStores {
		dataStore, err := b.NamedDataStore(dataStoreName)
		if err != nil {
			return err
		}
		if _, err := purgeWithDCPFeed(ctx, dataStore, tbp); err != nil {
			return err
		}
	}
	if len(dataStores) == 1 {
		dataStoreName := dataStores[0]
		if base.IsDefaultCollection(dataStoreName.ScopeName(), dataStoreName.CollectionName()) {
			dataStore, err := b.NamedDataStore(dataStoreName)
			if err != nil {
				return err
			}
			tbp.Logf(ctx, "readying views for bucket")
			if err := viewBucketReadier(ctx, dataStore, tbp); err != nil {
				return err
			}
		}
	}
	return nil
}

// deleteDocsAndIndexesBucketReadier purges the datastore using DCP and drops any indexes on the bucket
var deleteDocsAndIndexesBucketReadier base.TBPBucketReadierFunc = func(ctx context.Context, b base.Bucket, tbp *base.TestBucketPool) error {
	dataStores, err := b.ListDataStores()
	if err != nil {
		return err
	}
	for _, dataStoreName := range dataStores {
		dataStore, err := b.NamedDataStore(dataStoreName)
		if err != nil {
			return err
		}
		if _, err := purgeWithDCPFeed(ctx, dataStore, tbp); err != nil {
			return err
		}
		n1qlStore, ok := base.AsN1QLStore(dataStore)
		if !ok {
			return errors.New("attempting to empty indexes with non-N1QL store")
		}
		tbp.Logf(ctx, "dropping existing bucket indexes %s.%s.%s", b.GetName(), dataStore.ScopeName(), dataStore.CollectionName())
		if err := base.DropAllIndexes(ctx, n1qlStore); err != nil {
			tbp.Logf(ctx, "Failed to drop bucket indexes: %v", err)
			return err
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
			UseXattrs:                  base.TestUseXattrs(),
			NumReplicas:                0,
			WaitForIndexesOnlineOption: base.WaitForIndexesDefault,
			Serverless:                 false,
			MetadataIndexes:            IndexesWithoutMetadata,
			NumPartitions:              DefaultNumIndexPartitions,
		}
		if base.IsDefaultCollection(dataStore.ScopeName(), dataStore.CollectionName()) {
			options.MetadataIndexes = IndexesAll
		}
		if err := InitializeIndexes(ctx, n1qlStore, options); err != nil {
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

// FlushRevisionCacheForTest creates a new revision cache. This is currently at the database level. Only use this in test code.
func (db *DatabaseContext) FlushRevisionCacheForTest() {
	backingStores := make(map[uint32]RevisionCacheBackingStore, len(db.CollectionByID))
	for i, v := range db.CollectionByID {
		backingStores[i] = v
	}

	db.revisionCache = NewRevisionCache(
		db.Options.RevisionCacheOptions,
		backingStores,
		db.DbStats.Cache(),
	)

}

// TestBucketPoolWithIndexes runs a TestMain for packages that require creation of indexes
func TestBucketPoolWithIndexes(ctx context.Context, m *testing.M, tbpOptions base.TestBucketPoolOptions) {
	base.TestBucketPoolMain(ctx, m, viewsAndGSIBucketReadier, viewsAndGSIBucketInit, tbpOptions)
}

// TestBucketPoolEnsureNoIndexes runs a TestMain for packages that expects no indexes to exist.
func TestBucketPoolEnsureNoIndexes(ctx context.Context, m *testing.M, tbpOptions base.TestBucketPoolOptions) {
	base.TestBucketPoolMain(ctx, m, deleteDocsAndIndexesBucketReadier, base.NoopInitFunc, tbpOptions)
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

	ctx = dbCtx.AddDatabaseLogContext(ctx)
	err = dbCtx.StartOnlineProcesses(ctx)
	require.NoError(t, err)

	db, err := CreateDatabase(dbCtx)
	require.NoError(t, err, "Couldn't create database 'db'")

	return db, addDatabaseAndTestUserContext(ctx, db)
}

// addDatabaseAndTestUserContext adds a fake user to the context
func addDatabaseAndTestUserContext(ctx context.Context, db *Database) context.Context {
	return db.AddDatabaseLogContext(base.UserLogCtx(ctx, "gotest", base.UserDomainBuiltin, nil))
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

func GetSingleDatabaseCollectionWithUser(ctx context.Context, tb testing.TB, database *Database) (*DatabaseCollectionWithUser, context.Context) {
	c := &DatabaseCollectionWithUser{
		DatabaseCollection: GetSingleDatabaseCollection(tb, database.DatabaseContext),
		user:               database.user,
	}
	return c, c.AddCollectionContext(ctx)
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

func RawDocWithInlineSyncData(_ testing.TB) string {
	return `
{
  "_sync": {
    "rev": "1-ca9ad22802b66f662ff171f226211d5c",
    "sequence": 1,
    "recent_sequences": [1],
    "history": {
      "revs": ["1-ca9ad22802b66f662ff171f226211d5c"],
      "parents": [-1],
      "channels": [null]
    },
    "cas": "",
    "time_saved": "2017-11-29T12:46:13.456631-08:00"
  }
}
`
}

// DisableSequenceWaitOnDbStart disables the release sequence wait on db start.  Appropriate for tests
// that make changes to database config after first startup, and don't assert/require on sequence correctness
func DisableSequenceWaitOnDbRestart(tb testing.TB) {
	//
	BypassReleasedSequenceWait.Store(true)
	tb.Cleanup(func() {
		BypassReleasedSequenceWait.Store(false)
	})
}

// WriteDirect will write a document named doc-{sequence} with a given set of channels. This is used to simulate out of order sequence writes by bypassing typical Sync Gateway CRUD functions.
func WriteDirect(t *testing.T, collection *DatabaseCollection, channelArray []string, sequence uint64) {
	key := fmt.Sprintf("doc-%v", sequence)

	rev := "1-a"
	chanMap := make(map[string]*channels.ChannelRemoval, 10)

	for _, channel := range channelArray {
		chanMap[channel] = nil
	}

	syncData := &SyncData{
		CurrentRev: rev,
		Sequence:   sequence,
		Channels:   chanMap,
		TimeSaved:  time.Now(),
	}
	body := fmt.Sprintf(`{"key": "%s"}`, key)
	if base.TestUseXattrs() {

		opts := &sgbucket.MutateInOptions{
			MacroExpansion: macroExpandSpec(base.SyncXattrName),
		}
		ctx := base.TestCtx(t)
		_, err := collection.dataStore.WriteWithXattrs(ctx, key, 0, 0, []byte(body), map[string][]byte{base.SyncXattrName: base.MustJSONMarshal(t, syncData)}, nil, opts)
		require.NoError(t, err)
	} else {
		_, err := collection.dataStore.Add(key, 0, Body{base.SyncPropertyName: syncData, "key": key})
		require.NoError(t, err)
	}
}
