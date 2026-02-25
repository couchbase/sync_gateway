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
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
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
	for range 100 {
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
	for range 100 {
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

// StartChangeCache is called from the cache benchmarking tool to init the change cache, not to be called
// from outside test code
func (db *DatabaseContext) StartChangeCache(t *testing.T, ctx context.Context) {
	notifyChange := func(ctx context.Context, changedChannels channels.Set) {
		db.mutationListener.Notify(ctx, changedChannels)
	}

	// Initialize the change cache
	if err := db.changeCache.Init(
		ctx,
		db,
		db.channelCache,
		notifyChange,
		db.Options.CacheOptions,
		db.MetadataKeys,
	); err != nil {
		t.Fatal(err)
	}
	db.mutationListener.OnChangeCallback = db.changeCache.DocChanged
	db.changeCache.lock.Unlock()

	// start broadcast goroutine
	db.mutationListener.StartNotifierBroadcaster(ctx)
}

// CallProcessEntry allows the cache benchmarking tool to call directly into processEntry, not to
// be used from outside test code
func (db *DatabaseContext) CallProcessEntry(t *testing.T, ctx context.Context, log *LogEntry) {
	db.changeCache.processEntry(ctx, log)
}

// GetCachedChanges will grab cached changes form channel cache for caching tool, not to be used outside test code.
func (db *DatabaseContext) GetCachedChanges(t *testing.T, ctx context.Context, chanID channels.ID) ([]*LogEntry, error) {
	logs, err := db.changeCache.getChannelCache().GetCachedChanges(ctx, chanID)
	return logs, err
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
	for range 14 {
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
	for range 100 {
		isChanged = userWaiter.RefreshUserCount()
		if isChanged {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return isChanged
}

// purgeWithDCPFeed purges all documents seen on a DCP feed with system xattrs, including tombstones.
func purgeWithDCPFeed(ctx context.Context, bucket base.Bucket, tbp *base.TestBucketPool) error {
	purgeTimeout := 60 * time.Second
	purgeBody := Body{"_purged": true}
	var processedDocCount atomic.Int64
	var purgedDocCount atomic.Int64

	var purgeErrors *base.MultiError

	collections := make(map[uint32]sgbucket.DataStore)
	if bucket.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		dataStores, err := bucket.ListDataStores()
		if err != nil {
			return err
		}
		for _, dataStoreName := range dataStores {
			collection, err := bucket.NamedDataStore(dataStoreName)
			if err != nil {
				return err
			}
			collections[collection.GetCollectionID()] = collection
		}
	}

	dcpClientOpts := base.DCPClientOptions{
		FeedID:            "purgeFeed-" + bucket.GetName(),
		OneShot:           true,
		FailOnRollback:    false,
		CollectionIDs:     slices.Collect(maps.Keys(collections)),
		MetadataStoreType: base.DCPMetadataStoreInMemory,
	}

	purgeCallback := func(event sgbucket.FeedEvent) bool {
		processedDocCount.Add(1)
		// We only need to purge mutations/deletions
		if event.Opcode != sgbucket.FeedOpMutation && event.Opcode != sgbucket.FeedOpDeletion {
			return false
		}

		// If it's a deletion but doesn't have xattrs, ignore it
		if event.Opcode == sgbucket.FeedOpDeletion && event.DataType&base.MemcachedDataTypeXattr == 0 {
			return false
		}

		docID := string(event.Key)

		var xattrs []string
		if event.DataType&base.MemcachedDataTypeXattr != 0 {
			var err error
			systemOnly := false
			xattrs, err = sgbucket.DecodeXattrNames(event.Value, systemOnly)
			if err != nil {
				purgeErrors = purgeErrors.Append(fmt.Errorf("Could not decode xattrs from doc %s: %w", docID, err))
				return false
			}
		}
		dataStore, ok := collections[event.CollectionID]
		if !ok {
			purgeErrors = purgeErrors.Append(fmt.Errorf("Could not find collection ID %d for %#+v doc over DCP", event.CollectionID, event))
		}
		// If Couchbase Server < 7.6, we need to delete xattrs one at a time, since it doesn't support subdoc multi-xattr operations.
		if len(xattrs) >= 1 && !bucket.IsSupported(sgbucket.BucketStoreFeatureMultiXattrSubdocOperations) {
			for _, xattr := range xattrs {
				err := dataStore.DeleteSubDocPaths(ctx, docID, xattr)
				if err != nil {
					purgeErrors = purgeErrors.Append(fmt.Errorf("error purging xattr %s from docID %s: %w", xattr, docID, err))
					tbp.Logf(ctx, "%s", err)
					return false
				}
			}
			xattrs = nil // reset xattrs to nil so we don't try to delete the doc with xattrs

		}

		purgeErr := dataStore.DeleteWithXattrs(ctx, docID, xattrs)
		if base.IsDocNotFoundError(purgeErr) { // doc is a tombstone
			// If key no longer exists, need to add and and remove to remove a Sync Gateway tombstone.
			_, addErr := dataStore.Add(docID, 0, purgeBody)
			if addErr != nil {
				purgeErrors = purgeErrors.Append(addErr)
				tbp.Logf(ctx, "Error adding docID %s to force deletion. %v", docID, addErr)
				return false
			}

			if delErr := dataStore.Delete(docID); delErr != nil {
				purgeErrors = purgeErrors.Append(delErr)
				tbp.Logf(ctx, "Error deleting docID %s.  %v", docID, delErr)
			}
			purgedDocCount.Add(1)
		} else if purgeErr != nil {
			err := fmt.Errorf("error purging docID %s, xattrs=%s: %w", docID, xattrs, purgeErr)
			purgeErrors = purgeErrors.Append(err)
			tbp.Logf(ctx, "%s", err)
		}
		return false
	}
	gocbBucket, err := base.AsGocbV2Bucket(bucket)
	if err != nil {
		return err
	}
	dcpClient, err := base.NewDCPClient(ctx, purgeCallback, dcpClientOpts, gocbBucket)
	if err != nil {
		return err
	}
	doneChan, err := dcpClient.Start()
	if err != nil {
		return fmt.Errorf("error starting purge DCP feed: %w", err)
	}
	// wait for feed to complete
	timeout := time.After(purgeTimeout)
	select {
	case err := <-doneChan:
		if err != nil {
			tbp.Logf(ctx, "purgeDCPFeed finished with error: %v", err)
		}
	case <-timeout:
		return fmt.Errorf("timeout waiting for purge DCP feed to complete")
	}
	closeErr := dcpClient.Close()
	if closeErr != nil {
		tbp.Logf(ctx, "error closing purge DCP feed: %v", closeErr)
	}

	tbp.Logf(ctx, "Finished purge DCP feed ... Total docs purged: %d", purgedDocCount.Load())
	tbp.Logf(ctx, "Finished purge DCP feed ... Total docs processed: %d", processedDocCount.Load())
	return purgeErrors.ErrorOrNil()
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
	return purgeWithDCPFeed(ctx, b, tbp)
}

// deleteDocsAndIndexesBucketReadier purges the datastore using DCP and drops any indexes on the bucket
var deleteDocsAndIndexesBucketReadier base.TBPBucketReadierFunc = func(ctx context.Context, b base.Bucket, tbp *base.TestBucketPool) error {
	err := purgeWithDCPFeed(ctx, b, tbp)
	if err != nil {
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
			LegacySyncDocsIndex:        false,
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
		channels, err := user.InheritedCollectionChannels(base.DefaultScope, base.DefaultCollection)
		require.NoError(tb, err)
		info.Channels = channels.AsSet()
		email := user.Email()
		info.Email = &email
		info.Disabled = base.Ptr(user.Disabled())
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
func IsCovered(plan map[string]any) bool {
	for key, value := range plan {
		switch value := value.(type) {
		case string:
			if key == "#operator" && value == "Fetch" {
				return false
			}
		case map[string]any:
			if !IsCovered(value) {
				return false
			}
		case []any:
			for _, arrayValue := range value {
				jsonArrayValue, ok := arrayValue.(map[string]any)
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

	if base.TestDisableRevCache() {
		if dbcOptions.RevisionCacheOptions == nil {
			dbcOptions.RevisionCacheOptions = DefaultRevisionCacheOptions()
		}
		dbcOptions.RevisionCacheOptions.MaxItemCount = 0
	}
}

// SetupTestDBWithOptions creates an online test db with the specified database context options. Note that environment variables will override values (SG_TEST_USE_XATTRS, SG_TEST_USE_DEFAULT_COLLECTION).
// override somedbcOptions properties.
func SetupTestDBWithOptions(t testing.TB, dbcOptions DatabaseContextOptions) (*Database, context.Context) {
	tBucket := base.GetTestBucket(t)
	return SetupTestDBForBucketWithOptions(t, tBucket, dbcOptions)
}

// SetupTestDBForBucketWithOptions sets up a test database with the specified database context options.  Note that environment variables will override values (SG_TEST_USE_XATTRS, SG_TEST_USE_DEFAULT_COLLECTION).
func SetupTestDBForBucketWithOptions(t testing.TB, tBucket base.Bucket, dbcOptions DatabaseContextOptions) (*Database, context.Context) {
	AddOptionsFromEnvironmentVariables(&dbcOptions)
	if dbcOptions.Scopes == nil {
		dbcOptions.Scopes = GetScopesOptions(t, tBucket, 1)
	}
	return CreateTestDatabase(t, tBucket, dbcOptions)
}

// CreateTestDatabase creates a DatabaseContext and makes it online. Returns a context suitable for use with the database which has information sufficient for audit logging.
func CreateTestDatabase(t testing.TB, tBucket base.Bucket, dbcOptions DatabaseContextOptions) (*Database, context.Context) {
	ctx := base.TestCtx(t)
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
func GetScopesOptions(t testing.TB, bucket base.Bucket, numCollections int) ScopesOptions {
	if !base.TestsUseNamedCollections() {
		if numCollections != 1 {
			t.Fatal("Setting numCollections on a test that can't use collections is invalid")
		}
		return GetScopesOptionsDefaultCollectionOnly(t)
	}
	// Get a datastore as provided by the test
	stores := base.GetNonDefaultDatastoreNames(t, bucket)
	require.GreaterOrEqual(t, len(stores), numCollections, "Requested more collections %d than found on testBucket %d", numCollections, len(stores))

	scopesConfig := ScopesOptions{}
	for i := range numCollections {
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
	database.sequences.mutex.Lock()
	defer database.sequences.mutex.Unlock()
	return database.sequences._incrementSequence(1)
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
	chanSetMap := base.Set{}

	for _, channel := range channelArray {
		chanMap[channel] = nil
		chanSetMap[channel] = struct{}{}
	}

	revInf := RevInfo{
		ID:       rev,
		Channels: chanSetMap,
	}
	revTree := RevTree{
		rev: &revInf,
	}

	syncData := &SyncData{
		RevAndVersion: channels.RevAndVersion{
			RevTreeID: rev,
		},
		Sequence:  sequence,
		Channels:  chanMap,
		TimeSaved: time.Now(),
		History:   revTree,
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
		_, err := collection.dataStore.Add(key, 0, base.MustJSONMarshal(t, Body{base.SyncPropertyName: syncData, "key": key}))
		require.NoError(t, err)
	}
}

// GetIndexPartitionCount returns the number of partitions for a given index. This function queries index nodes directly and would not be suitable for production use, since this port is not generally accessible.
func GetIndexPartitionCount(t testing.TB, bucket *base.GocbV2Bucket, dsName sgbucket.DataStoreName, indexName string) uint32 {
	agent, err := bucket.GetGoCBAgent()
	require.NoError(t, err)
	gsiEps := agent.GSIEps()
	require.Greater(t, len(gsiEps), 0, "No available Couchbase Server nodes for GSI")

	var username, password string
	if bucket.Spec.Auth != nil {
		username, password, _ = bucket.Spec.Auth.GetCredentials()
	}
	ctx := base.TestCtx(t)
	uri := "/getIndexStatus"
	respBytes, statusCode, err := base.MgmtRequest(bucket.HttpClient(ctx), gsiEps[0], http.MethodGet, uri, "application/json", username, password, nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, statusCode, "unexpected status code for %s", respBytes)
	var output struct {
		Status []struct {
			IndexName    string `json:"indexName"`
			Bucket       string `json:"bucket"`
			Collection   string `json:"collection"`
			Scope        string `json:"scope"`
			NumPartition uint32 `json:"numPartition"`
		} `json:"status"`
	}
	require.NoError(t, base.JSONUnmarshal(respBytes, &output), "error unmarshalling %s", respBytes)
	for _, idx := range output.Status {
		if idx.Bucket != bucket.BucketName() || idx.Collection != dsName.CollectionName() || idx.Scope != dsName.ScopeName() {
			continue
		}
		if idx.IndexName != indexName {
			continue
		}
		return idx.NumPartition
	}
	require.Failf(t, "index not found", "index %s not found in %+v", indexName, output)
	return 0
}

// GetMutationListener retrieves mutation listener form database context, to be used only for testing purposes.
func (db *DatabaseContext) GetMutationListener(t *testing.T) *changeListener {
	return &db.mutationListener
}

// InitChannel is a test-only function to initialize a channel in the channel cache.
func (db *DatabaseContext) InitChannel(ctx context.Context, t *testing.T, chanName string) error {
	_, err := db.channelCache.getSingleChannelCache(ctx, channels.NewID(chanName, base.DefaultCollectionID))
	return err
}

func CreateTestDocument(docID string, revID string, body Body, deleted bool, expiry uint32) (newDoc *Document) {
	newDoc = &Document{
		ID:        docID,
		Deleted:   deleted,
		DocExpiry: expiry,
		RevID:     revID,
		_body:     body,
	}
	return newDoc
}

// requireCurrentVersion fetches the document by key, and validates that cv matches.
func (c *DatabaseCollection) RequireCurrentVersion(t *testing.T, key string, source string, version uint64) {
	ctx := base.TestCtx(t)
	doc, err := c.GetDocument(ctx, key, DocUnmarshalSync)
	require.NoError(t, err)
	if doc.HLV == nil {
		require.Equal(t, "", source)
		require.Equal(t, "", version)
		return
	}

	require.Equal(t, doc.HLV.SourceID, source)
	require.Equal(t, doc.HLV.Version, version)
}

// GetDocumentCurrentVersion fetches the document by key and returns the current version
func (c *DatabaseCollection) GetDocumentCurrentVersion(t testing.TB, key string) (source string, version uint64) {
	ctx := base.TestCtx(t)
	doc, err := c.GetDocument(ctx, key, DocUnmarshalSync)
	require.NoError(t, err)
	if doc.HLV == nil {
		return "", 0
	}
	return doc.HLV.SourceID, doc.HLV.Version
}

// UpsertTestDocWithVersion upserts document 'key' with the specified body and version.  Used for testing with
// version values specified by the test.
func (c *DatabaseCollectionWithUser) UpsertTestDocWithVersion(ctx context.Context, t testing.TB, key string, body Body, versionString string, mergeVersionsStr string) *Document {
	currentDoc, currentBucketDoc, _ := c.GetDocumentWithRaw(ctx, key, DocUnmarshalSync)
	var newDocHLV *HybridLogicalVector
	var newDoc *Document
	if currentDoc != nil {
		newDoc = currentDoc
		newDocHLV = currentDoc.HLV
	} else {
		newDoc = &Document{
			ID:    key,
			_body: body,
		}
		newDocHLV = NewHybridLogicalVector()
	}

	version, versionErr := ParseVersion(versionString)
	require.NoError(t, versionErr)
	require.NoError(t, newDocHLV.AddVersion(version))
	if mergeVersionsStr != "" {
		versions, _, err := parseVectorValues(mergeVersionsStr)
		require.NoError(t, err, "malformed mergeVersionsStr")
		for _, version := range versions {
			newDocHLV.SetMergeVersion(version.SourceID, version.Value)
		}
	}

	opts := PutDocOptions{
		NewDocHLV:   newDocHLV,
		NewDoc:      newDoc,
		ExistingDoc: currentBucketDoc,
	}
	doc, _, _, err := c.PutExistingCurrentVersion(ctx, opts)
	require.NoError(t, err)
	return doc
}

// retrieveDocRevSeNo will take the $document xattr and return the revSeqNo defined in that xattr
func RetrieveDocRevSeqNo(t *testing.T, docxattr []byte) uint64 {
	require.NotNil(t, docxattr)
	var retrievedDocumentRevNo string
	require.NoError(t, base.JSONUnmarshal(docxattr, &retrievedDocumentRevNo))

	revNo, err := strconv.ParseUint(retrievedDocumentRevNo, 10, 64)
	require.NoError(t, err)
	return revNo
}

// MoveAttachmentXattrFromGlobalToSync is a test only function that will move any defined attachment metadata in _globalSync.attachments_meta to _sync.attachments. This turns a document written with Sync Gateway 4.0 style attachments to a document with Sync Gateway <4.0 style attachments.
func MoveAttachmentXattrFromGlobalToSync(t *testing.T, dataStore base.DataStore, docID string, value []byte, macroExpand bool) {
	docSync := GetRawSyncXattr(t, dataStore, docID)
	docSync.AttachmentsPre4dot0 = GetRawGlobalSync(t, dataStore, docID).Attachments

	opts := &sgbucket.MutateInOptions{}
	// this should be true for cases we want to move the attachment metadata without causing a new import feed event
	if macroExpand {
		spec := macroExpandSpec(base.SyncXattrName)
		opts.MacroExpansion = spec
	} else {
		opts = nil
		docSync.Cas = ""
	}

	newSync, err := base.JSONMarshal(docSync)
	require.NoError(t, err)

	_, cas, err := dataStore.GetRaw(docID)
	require.NoError(t, err)
	ctx := base.TestCtx(t)
	_, err = dataStore.WriteWithXattrs(ctx, docID, 0, cas, value, map[string][]byte{base.SyncXattrName: newSync}, []string{base.GlobalXattrName}, opts)
	require.NoError(t, err)
}

// WaitForBackgroundManagerHeartbeatDocRemoval waits for removal of heartbeat document or fails the test harness.
//
// After a background manager state transition to completed, stopped, error is followed by immediate removal of the
// heartbeat document. When restarting a background manager, the state of the heartbeat document is checked, allowing
// for a small race if you try to stop and immediately restart a background manager.
func WaitForBackgroundManagerHeartbeatDocRemoval(t testing.TB, mgr *BackgroundManager) {
	if !mgr.isClusterAware() {
		return
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		exists, ok := mgr.clusterAwareOptions.metadataStore.Exists(mgr.clusterAwareOptions.HeartbeatDocID())
		require.NoError(t, ok)
		assert.False(c, exists, "BackgroundManager heartbeat document was not removed in expected time")
	}, 10*time.Second, 10*time.Millisecond)
}

// RequireBackgroundManagerState waits for a BackgroundManager to reach a given state or fails test harness.
func RequireBackgroundManagerState(t testing.TB, mgr *BackgroundManager, expState BackgroundProcessState) BackgroundManagerStatus {
	waitTime := 10 * time.Second
	if !base.UnitTestUrlIsWalrus() {
		// Increase wait time for CI tests against Couchbase Server, they can take longer to run.
		// Generally everything runs in 10 seconds, but when it does not, it is not worth flagging the failures.
		waitTime = 30 * time.Second
	}
	ctx := base.TestCtx(t)
	var status *BackgroundManagerStatus
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		status = nil
		rawStatus, err := mgr.GetStatus(ctx)
		assert.NoError(c, err)
		assert.NoError(c, base.JSONUnmarshal(rawStatus, &status))
		assert.Equal(c, expState, status.State, "BackgroundManager did not reach expected state in %d seconds. Current status: %s", int(waitTime.Seconds()), string(rawStatus))
	}, waitTime, time.Millisecond*10)

	if slices.Contains([]BackgroundProcessState{BackgroundProcessStateCompleted, BackgroundProcessStateStopped, BackgroundProcessStateError}, expState) {
		WaitForBackgroundManagerHeartbeatDocRemoval(t, mgr)
	}
	return *status
}

// AssertSyncInfoMetaVersion will assert that meta version is equal to current product version
func AssertSyncInfoMetaVersion(t *testing.T, ds base.DataStore) {
	var syncInfo base.SyncInfo
	_, err := ds.Get(base.SGSyncInfo, &syncInfo)
	require.NoError(t, err)
	assert.Equal(t, "4.0.0", syncInfo.MetaDataVersion)
}

// GetRawSyncXattr retrieves the _sync xattr from the bucket without going through typical CRUD processing.
func GetRawSyncXattr(t *testing.T, collection base.DataStore, docID string) SyncData {
	xattrs, _, err := collection.GetXattrs(base.TestCtx(t), docID, []string{base.SyncXattrName})
	require.NoError(t, err, "Could not find _sync xattr for %s", docID)
	require.Contains(t, xattrs, base.SyncXattrName, "Could not find _sync xattr for %s", docID)
	var syncData SyncData
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))
	return syncData
}

// GetRawGlobalSync retrieves the _globalSync xattr. Fails if the xattr is not found.
func GetRawGlobalSync(t *testing.T, collection base.DataStore, docID string) GlobalSyncData {
	xattrs, _, err := collection.GetXattrs(base.TestCtx(t), docID, []string{base.GlobalXattrName})
	require.NoError(t, err, "Could not find _globalSync xattr for %s", docID)
	require.Contains(t, xattrs, base.GlobalXattrName, "Could not find _globalSync xattr for %s", docID)
	var globalSyncData GlobalSyncData
	require.NoError(t, base.JSONUnmarshal(xattrs[base.GlobalXattrName], &globalSyncData))
	return globalSyncData
}

// GetRawGlobalSyncAttachments retrieves the attachments from the _globalSync.attachments xattr for a given document ID.
func GetRawGlobalSyncAttachments(t *testing.T, collection base.DataStore, docID string) AttachmentMap {
	xattrs, _, err := collection.GetXattrs(base.TestCtx(t), docID, []string{base.GlobalXattrName})
	require.NoError(t, err, "Could not find _globalSync xattr for %s", docID)
	require.Contains(t, xattrs, base.GlobalXattrName, "Could not find _globalSync xattr for %s", docID)
	var globalSyncData struct {
		Attachments AttachmentMap `json:"attachments_meta"`
	}
	require.NoError(t, base.JSONUnmarshal(xattrs[base.GlobalXattrName], &globalSyncData))
	return globalSyncData.Attachments
}

// GetAttachmentsFromInlineBody returns the attachment data when it is part of a json body. This could be from:
// - GET /ks/{docID}
// blip message with _attachments field.
func GetAttachmentsFromInlineBody(t *testing.T, responseBody []byte) AttachmentMap {
	var body struct {
		Attachments AttachmentMap `json:"_attachments"`
	}
	require.NoError(t, base.JSONUnmarshal(responseBody, &body))
	return body.Attachments
}

// GetAttachmentsFrom1xBody returns the attachment data when it returned from Get1xBody functions, where the data is already db.Body.
func GetAttachmentsFrom1xBody(t *testing.T, body Body) AttachmentMap {
	return GetAttachmentsFromInlineBody(t, base.MustJSONMarshal(t, body))
}

func GetChangeEntryCV(t *testing.T, entry *ChangeEntry) Version {
	require.NotNil(t, entry.Changes)

	if changeCV, ok := entry.Changes[0][ChangesVersionTypeCV]; ok {
		changeVersion, err := ParseVersion(changeCV)
		require.NoError(t, err)
		return changeVersion
	} else {
		require.FailNow(t, "no CV found for change entry")
	}
	return Version{}
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
