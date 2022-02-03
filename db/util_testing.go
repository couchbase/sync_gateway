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
	"math"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

// Workaround SG #3570 by doing a polling loop until the star channel query returns 0 results.
// Uses the star channel index as a proxy to indicate that _all_ indexes are empty (which might not be true)
func WaitForIndexEmpty(store base.N1QLStore, useXattrs bool) error {

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {
		empty, err := isIndexEmpty(store, useXattrs)
		if err != nil {
			return true, err, nil
		}
		return !empty, nil, empty
	}

	// Kick off the retry loop
	err, _ := base.RetryLoop(
		"Wait for index to be empty",
		retryWorker,
		base.CreateMaxDoublingSleeperFunc(60, 500, 5000),
	)
	return err

}

func isIndexEmpty(store base.N1QLStore, useXattrs bool) (bool, error) {
	// Create the star channel query
	statement := fmt.Sprintf("%s LIMIT 1", QueryStarChannel.statement) // append LIMIT 1 since we only care if there are any results or not
	starChannelQueryStatement := replaceActiveOnlyFilter(statement, false)
	starChannelQueryStatement = replaceSyncTokensQuery(starChannelQueryStatement, useXattrs)
	starChannelQueryStatement = replaceIndexTokensQuery(starChannelQueryStatement, sgIndexes[IndexAllDocs], useXattrs)
	params := map[string]interface{}{}
	params[QueryParamStartSeq] = 0
	params[QueryParamEndSeq] = math.MaxInt64

	// Execute the query
	results, err := store.Query(starChannelQueryStatement, params, base.RequestPlus, true)

	// If there was an error, then retry.  Assume it's an "index rollback" error which happens as
	// the index processes the bucket flush operation
	if err != nil {
		return false, err
	}

	// If it's empty, we're done
	var queryRow AllDocsIndexQueryRow
	found := results.Next(&queryRow)
	resultsCloseErr := results.Close()
	if resultsCloseErr != nil {
		return false, err
	}

	return !found, nil
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

// Wait uses backoff retry for up to ~27s
func (sw *StatWaiter) Wait() {
	actualCount := sw.stat.Value()
	if actualCount >= sw.targetCount {
		return
	}

	waitTime := 1 * time.Millisecond
	for i := 0; i < 13; i++ {
		waitTime = waitTime * 2
		time.Sleep(waitTime)
		actualCount = sw.stat.Value()
		if actualCount >= sw.targetCount {
			return
		}
	}

	sw.tb.Errorf("StatWaiter.Wait timed out waiting for stat to reach %d (actual: %d) %s", sw.targetCount, actualCount, base.GetCallersName(2, true))
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

// emptyAllDocsIndex ensures the AllDocs index for the given bucket is empty. Works similarly to db.Compact, except on a different index.
func emptyAllDocsIndex(ctx context.Context, b base.Bucket, tbp *base.TestBucketPool) (numCompacted int, err error) {
	purgedDocCount := 0
	purgeBody := Body{"_purged": true}

	dbCtx, err := NewDatabaseContext(b.GetName(), base.NoCloseClone(b), false, DatabaseContextOptions{
		UseViews:    base.TestsDisableGSI(),
		EnableXattr: base.TestUseXattrs(),
	})
	if err != nil {
		return 0, err
	}
	defer dbCtx.Close()

	database, err := GetDatabase(dbCtx, nil)
	if err != nil {
		return 0, err
	}

	// A stripped down version of db.Compact() that works on AllDocs instead of tombstones
	for {
		results, err := database.QueryChannels(ctx, "*", 0, 0, 0, false)
		if err != nil {
			return 0, err
		}
		var tombstonesRow QueryIdRow
		var resultCount int
		for results.Next(&tombstonesRow) {
			resultCount++
			tbp.Logf(ctx, "compactTestBucket deleting %q", tombstonesRow.Id)
			// First, attempt to purge.
			purgeErr := database.Purge(tombstonesRow.Id)
			if purgeErr == nil {
			} else if base.IsKeyNotFoundError(b, purgeErr) {
				// If key no longer exists, need to add and remove to trigger removal from view
				_, addErr := b.Add(tombstonesRow.Id, 0, purgeBody)
				if addErr != nil {
					tbp.Logf(ctx, "Error compacting key %s (add) - will not be compacted.  %v", tombstonesRow.Id, addErr)
					continue
				}

				if delErr := b.Delete(tombstonesRow.Id); delErr != nil {
					tbp.Logf(ctx, "Error compacting key %s (delete) - will not be compacted.  %v", tombstonesRow.Id, delErr)
				}
				purgedDocCount++
			} else {
				tbp.Logf(ctx, "Error compacting key %s (purge) - will not be compacted.  %v", tombstonesRow.Id, purgeErr)
			}
		}
		err = results.Close()
		if err != nil {
			return 0, err
		}

		tbp.Logf(ctx, "Compacted %v docs in batch", purgedDocCount)

		if resultCount < QueryTombstoneBatch {
			break
		}
	}

	tbp.Logf(ctx, "Finished compaction ... Total docs purged: %d", purgedDocCount)
	return purgedDocCount, nil
}

// ViewsAndGSIBucketReadier empties the bucket, initializes Views, and waits until GSI indexes are empty. It is run asynchronously as soon as a test is finished with a bucket.
var ViewsAndGSIBucketReadier base.TBPBucketReadierFunc = func(ctx context.Context, b base.Bucket, tbp *base.TestBucketPool) error {

	if base.TestsDisableGSI() {
		tbp.Logf(ctx, "flushing bucket and readying views")
		if err := base.FlushBucketEmptierFunc(ctx, b, tbp); err != nil {
			return err
		}
		// Exit early if we're not using GSI.
		return viewBucketReadier(ctx, b, tbp)
	}

	tbp.Logf(ctx, "emptying bucket via N1QL, readying views and indexes")
	if err := base.N1QLBucketEmptierFunc(ctx, b, tbp); err != nil {
		return err
	}

	if _, err := emptyAllDocsIndex(ctx, b, tbp); err != nil {
		return err
	}

	if err := viewBucketReadier(ctx, b, tbp); err != nil {
		return err
	}

	n1qlStore, ok := base.AsN1QLStore(b)
	if !ok {
		return errors.New("attempting to empty indexes with non-N1QL store")
	}
	tbp.Logf(ctx, "waiting for empty bucket indexes")
	// we can't init indexes concurrently, so we'll just wait for them to be empty after emptying instead of recreating.
	if err := WaitForIndexEmpty(n1qlStore, base.TestUseXattrs()); err != nil {
		tbp.Logf(ctx, "WaitForIndexEmpty returned an error: %v", err)
		return err
	}
	tbp.Logf(ctx, "bucket indexes empty")

	return nil
}

// ViewsAndGSIBucketInit is run synchronously only once per-bucket to do any initial setup. For non-integration Walrus buckets, this is run for each new Walrus bucket.
var ViewsAndGSIBucketInit base.TBPBucketInitFunc = func(ctx context.Context, b base.Bucket, tbp *base.TestBucketPool) error {
	n1qlStore, ok := base.AsN1QLStore(b)
	if !ok {
		// Check we're not running with an invalid combination of backing store and xattrs.
		if base.TestUseXattrs() {
			return fmt.Errorf("xattrs not supported when using Walrus buckets")
		}

		tbp.Logf(ctx, "bucket not a gocb bucket... skipping GSI setup")
		return viewBucketReadier(ctx, b, tbp)
	}

	// Exit early if we're not using GSI.
	if base.TestsDisableGSI() {
		return nil
	}

	if empty, err := isIndexEmpty(n1qlStore, base.TestUseXattrs()); empty && err == nil {
		tbp.Logf(ctx, "indexes already created, and already empty - skipping")
		return nil
	} else {
		tbp.Logf(ctx, "indexes not empty (or doesn't exist) - %v %v", empty, err)
	}

	tbp.Logf(ctx, "dropping existing bucket indexes")
	if err := base.DropAllBucketIndexes(n1qlStore); err != nil {
		tbp.Logf(ctx, "Failed to drop bucket indexes: %v", err)
		return err
	}

	tbp.Logf(ctx, "creating SG bucket indexes")
	if err := InitializeIndexes(n1qlStore, base.TestUseXattrs(), 0); err != nil {
		return err
	}

	err := n1qlStore.CreatePrimaryIndex(base.PrimaryIndexName, nil)
	if err != nil {
		return err
	}

	return nil
}

// viewBucketReadier removes any existing views and installs a new set into the given bucket.
func viewBucketReadier(ctx context.Context, b base.Bucket, tbp *base.TestBucketPool) error {

	ddocs, err := b.GetDDocs()
	if err != nil {
		return err
	}

	for ddocName, _ := range ddocs {
		tbp.Logf(ctx, "removing existing view: %s", ddocName)
		if err := b.DeleteDDoc(ddocName); err != nil {
			return err
		}
	}

	tbp.Logf(ctx, "initializing bucket views")
	err = InitializeViews(b)
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
	return dbc.getChangesInChannelFromQuery(base.TestCtx(tb), channelName, startSeq, endSeq, 0, false)
}

// Test-only version of GetPrincipal that doesn't trigger channel/role recalculation
func (dbc *DatabaseContext) GetPrincipalForTest(tb testing.TB, name string, isUser bool) (info *PrincipalConfig, err error) {
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
	info = new(PrincipalConfig)
	info.Name = &name
	info.ExplicitChannels = princ.ExplicitChannels().AsSet()
	if user, ok := princ.(auth.User); ok {
		info.Channels = user.InheritedChannels().AsSet()
		info.Email = user.Email()
		info.Disabled = base.BoolPtr(user.Disabled())
		info.ExplicitRoleNames = user.ExplicitRoles().AllKeys()
		info.RoleNames = user.RoleNames().AllKeys()
	} else {
		info.Channels = princ.Channels().AsSet()
	}
	return
}
