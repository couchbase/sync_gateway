/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"expvar"
	"math"
	"strings"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// A wrapper around a Bucket's TapFeed that allows any number of client goroutines to wait for
// changes.
type changeListener struct {
	bucket                base.Bucket
	bucketName            string                 // Used for logging
	tapFeed               base.TapFeed           // Observes changes to bucket
	tapNotifier           *sync.Cond             // Posts notifications when documents are updated
	FeedArgs              sgbucket.FeedArguments // The Tap Args (backfill, etc)
	counter               uint64                 // Event counter; increments on every doc update
	terminateCheckCounter uint64                 // Termination Event counter; increments on every notifyCheckForTermination
	keyCounts             map[string]uint64      // Latest count at which each doc key was updated
	OnDocChanged          DocChangedFunc         // Called when change arrives on feed
	terminator            chan bool              // Signal to cause cbdatasource bucketdatasource.Close() to be called, which removes dcp receiver
	sgCfgPrefix           string                 // SG config key prefix
}

type DocChangedFunc func(event sgbucket.FeedEvent)

func (listener *changeListener) Init(name string, groupID string) {
	listener.bucketName = name
	listener.counter = 1
	listener.terminateCheckCounter = 0
	listener.keyCounts = map[string]uint64{}
	listener.tapNotifier = sync.NewCond(&sync.Mutex{})
	listener.sgCfgPrefix = base.SGCfgPrefixWithGroupID(groupID)
}

// Starts a changeListener on a given Bucket.
func (listener *changeListener) Start(bucket base.Bucket, dbStats *expvar.Map) error {

	listener.terminator = make(chan bool)
	listener.bucket = bucket
	listener.bucketName = bucket.GetName()
	listener.FeedArgs = sgbucket.FeedArguments{
		ID:         base.DCPCachingFeedID,
		Backfill:   sgbucket.FeedNoBackfill,
		Terminator: listener.terminator,
		DoneChan:   make(chan struct{}),
	}

	return listener.StartMutationFeed(bucket, dbStats)
}

func (listener *changeListener) StartMutationFeed(bucket base.Bucket, dbStats *expvar.Map) error {

	// Uses DCP by default, unless TAP is explicitly specified
	feedType := base.GetFeedType(bucket)
	switch feedType {
	case base.TapFeedType:
		// TAP Feed
		//    TAP feed is a go-channel of Tap events served by the bucket.  Start the feed, then
		//    start a goroutine to work the event channel, calling ProcessEvent for each event
		var err error
		listener.tapFeed, err = bucket.StartTapFeed(listener.FeedArgs, dbStats)
		if err != nil {
			return err
		}
		go func() {
			defer func() {
				if listener.FeedArgs.DoneChan != nil {
					close(listener.FeedArgs.DoneChan)
				}
			}()
			defer base.FatalPanicHandler()
			defer listener.notifyStopping()
			for event := range listener.tapFeed.Events() {
				event.TimeReceived = time.Now()
				listener.ProcessFeedEvent(event)
			}
		}()
		return nil
	default:
		// DCP Feed
		//    DCP receiver isn't go-channel based - DCPReceiver calls ProcessEvent directly.
		base.InfofCtx(context.TODO(), base.KeyDCP, "Using DCP feed for bucket: %q (based on feed_type specified in config file)", base.MD(bucket.GetName()))
		return bucket.StartDCPFeed(listener.FeedArgs, listener.ProcessFeedEvent, dbStats)
	}
}

// ProcessFeedEvent is invoked for each mutate or delete event seen on the server's mutation feed (TAP or DCP).  Uses document
// key to determine handling, based on whether the incoming mutation is an internal Sync Gateway document.
func (listener *changeListener) ProcessFeedEvent(event sgbucket.FeedEvent) bool {
	requiresCheckpointPersistence := true
	if event.Opcode == sgbucket.FeedOpMutation || event.Opcode == sgbucket.FeedOpDeletion {
		key := string(event.Key)
		if !strings.HasPrefix(key, base.SyncPrefix) { // Anything other than internal SG docs can go straight to OnDocChanged
			if listener.OnDocChanged != nil {
				listener.OnDocChanged(event)
			}
		} else if strings.HasPrefix(key, base.UserPrefix) ||
			strings.HasPrefix(key, base.RolePrefix) { // SG users and roles
			if listener.OnDocChanged != nil && event.Opcode == sgbucket.FeedOpMutation {
				listener.OnDocChanged(event)
			}
			listener.Notify(base.SetOf(key))
		} else if strings.HasPrefix(key, base.UnusedSeqPrefix) || strings.HasPrefix(key, base.UnusedSeqRangePrefix) { // SG unused sequence marker docs
			if listener.OnDocChanged != nil && event.Opcode == sgbucket.FeedOpMutation {
				listener.OnDocChanged(event)
			}
		} else if strings.HasPrefix(key, base.DCPCheckpointPrefix) { // SG DCP checkpoint docs (including other config group IDs)
			// Do not require checkpoint persistence when DCP checkpoint docs come back over DCP - otherwise
			// we'll end up in a feedback loop for their vbucket if persistence is enabled
			// NOTE: checkpoint persistence is disabled altogether for the caching feed.  Leaving this check in place
			// defensively.
			requiresCheckpointPersistence = false
		} else if strings.HasPrefix(key, listener.sgCfgPrefix) {
			if listener.OnDocChanged != nil {
				listener.OnDocChanged(event)
			}
		}
	}
	return requiresCheckpointPersistence
}

// MutationFeedStopMaxWait is the maximum amount of time to wait for
// mutation feed worker goroutine to terminate before the server is stopped.
const MutationFeedStopMaxWait = 30 * time.Second

// Stops a changeListener. Any pending Wait() calls will immediately return false.
func (listener *changeListener) Stop() {

	logCtx := context.TODO()
	base.DebugfCtx(logCtx, base.KeyChanges, "changeListener.Stop() called")

	if listener.terminator != nil {
		close(listener.terminator)
	}

	if listener.tapNotifier != nil {
		// Unblock any change listeners blocked on tapNotifier.Wait()
		listener.tapNotifier.Broadcast()
	}

	if listener.tapFeed != nil {
		err := listener.tapFeed.Close()
		if err != nil {
			base.DebugfCtx(logCtx, base.KeyChanges, "Error closing listener tap feed: %v", err)
		}
	}

	// Wait for mutation feed worker to terminate.
	waitTime := MutationFeedStopMaxWait
	select {
	case <-listener.FeedArgs.DoneChan:
		// Mutation feed worker goroutine is terminated and doneChan is already closed.
	case <-time.After(waitTime):
		base.WarnfCtx(logCtx, "Timeout after %v of waiting for mutation feed worker to terminate", waitTime)
	}
}

func (listener changeListener) TapFeed() base.TapFeed {
	return listener.tapFeed
}

//////// NOTIFICATIONS:

// Changes the counter, notifying waiting clients.
func (listener *changeListener) Notify(keys base.Set) {

	if len(keys) == 0 {
		return
	}
	listener.tapNotifier.L.Lock()
	listener.counter++
	for key := range keys {
		listener.keyCounts[key] = listener.counter
	}
	base.DebugfCtx(context.TODO(), base.KeyChanges, "Notifying that %q changed (keys=%q) count=%d",
		base.MD(listener.bucketName), base.UD(keys), listener.counter)
	listener.tapNotifier.Broadcast()
	listener.tapNotifier.L.Unlock()
}

// Changes the counter, notifying waiting clients.
func (listener *changeListener) NotifyCheckForTermination(keys base.Set) {
	if len(keys) == 0 {
		return
	}
	listener.tapNotifier.L.Lock()

	//Increment terminateCheckCounter, but loop back to zero
	//if we have reached maximum value for uint64 type
	if listener.terminateCheckCounter < math.MaxUint64 {
		listener.terminateCheckCounter++
	} else {
		listener.terminateCheckCounter = 0
	}

	base.DebugfCtx(context.TODO(), base.KeyChanges, "Notifying to check for _changes feed termination")
	listener.tapNotifier.Broadcast()
	listener.tapNotifier.L.Unlock()
}

func (listener *changeListener) notifyStopping() {
	listener.tapNotifier.L.Lock()
	listener.counter = 0
	listener.keyCounts = map[string]uint64{}
	base.DebugfCtx(context.TODO(), base.KeyChanges, "Notifying that changeListener is stopping")
	listener.tapNotifier.Broadcast()
	listener.tapNotifier.L.Unlock()
}

// Waits until either the counter, or terminateCheckCounter exceeds the given value. Returns the new counters.
func (listener *changeListener) Wait(keys []string, counter uint64, terminateCheckCounter uint64) (uint64, uint64) {
	listener.tapNotifier.L.Lock()
	defer listener.tapNotifier.L.Unlock()
	base.DebugfCtx(context.TODO(), base.KeyChanges, "No new changes to send to change listener.  Waiting for %q's count to pass %d",
		base.MD(listener.bucketName), counter)

	for {
		curCounter := listener._currentCount(keys)

		if curCounter != counter || listener.terminateCheckCounter != terminateCheckCounter {
			return curCounter, listener.terminateCheckCounter
		}

		listener.tapNotifier.Wait()

		// Don't go back through the for loop if this changeListener was terminated
		select {
		case <-listener.terminator:
			return 0, 0
		default:
			// do nothing
		}

	}
}

// Returns the max value of the counter for all the given keys
func (listener *changeListener) CurrentCount(keys []string) uint64 {
	listener.tapNotifier.L.Lock()
	defer listener.tapNotifier.L.Unlock()
	return listener._currentCount(keys)
}

func (listener *changeListener) _currentCount(keys []string) uint64 {
	var max uint64 = 0
	for _, key := range keys {
		if count := listener.keyCounts[key]; count > max {
			max = count
		}
	}
	return max
}

//////// CHANGE WAITER

// Helper for waiting on a changeListener. Every call to wait() will wait for the
// listener's counter to increment from the value at the last call.
type ChangeWaiter struct {
	listener                  *changeListener
	keys                      []string
	userKeys                  []string
	lastCounter               uint64
	lastTerminateCheckCounter uint64
	lastUserCount             uint64
}

// Creates a new ChangeWaiter that will wait for changes for the given document keys.
func (listener *changeListener) NewWaiter(keys []string) *ChangeWaiter {
	return &ChangeWaiter{
		listener:                  listener,
		keys:                      keys,
		lastCounter:               listener.CurrentCount(keys),
		lastTerminateCheckCounter: listener.terminateCheckCounter,
	}
}

func (listener *changeListener) NewWaiterWithChannels(chans base.Set, user auth.User) *ChangeWaiter {
	waitKeys := make([]string, 0, 5)
	for channel := range chans {
		waitKeys = append(waitKeys, channel)
	}
	var userKeys []string
	if user != nil {
		userKeys = []string{base.UserPrefix + user.Name()}
		for role := range user.RoleNames() {
			userKeys = append(userKeys, base.RolePrefix+role)
		}
		waitKeys = append(waitKeys, userKeys...)
	}
	waiter := listener.NewWaiter(waitKeys)
	waiter.userKeys = userKeys
	if userKeys != nil {
		waiter.lastUserCount = listener.CurrentCount(userKeys)
	}
	return waiter
}

// Waits for the changeListener's counter to change from the last time Wait() was called.
func (waiter *ChangeWaiter) Wait() uint32 {

	lastTerminateCheckCounter := waiter.lastTerminateCheckCounter
	lastCounter := waiter.lastCounter
	waiter.lastCounter, waiter.lastTerminateCheckCounter = waiter.listener.Wait(waiter.keys, waiter.lastCounter, waiter.lastTerminateCheckCounter)
	if waiter.userKeys != nil {
		waiter.lastUserCount = waiter.listener.CurrentCount(waiter.userKeys)
	}
	countChanged := waiter.lastCounter > lastCounter

	//Uses != to compare as value can cycle back through 0
	terminateCheckCountChanged := waiter.lastTerminateCheckCounter != lastTerminateCheckCounter

	if countChanged {
		return WaiterHasChanges
	} else if terminateCheckCountChanged {
		return WaiterCheckTerminated
	} else {
		return WaiterClosed
	}
}

// Returns the current counter value for the waiter's user (and roles).
// If this value changes, it means the user or roles have been updated.
func (waiter *ChangeWaiter) CurrentUserCount() uint64 {
	return waiter.lastUserCount
}

// Refreshes the last user count from the listener (without Wait being triggered).  Returns true if the count has changed
func (waiter *ChangeWaiter) RefreshUserCount() bool {
	previousCount := waiter.lastUserCount
	waiter.lastUserCount = waiter.listener.CurrentCount(waiter.userKeys)
	return waiter.lastUserCount != previousCount
}

// Updates the set of channel keys in the ChangeWaiter (maintains the existing set of user keys)
func (waiter *ChangeWaiter) UpdateChannels(chans channels.TimedSet) {
	initialCapacity := len(chans) + len(waiter.userKeys)
	updatedKeys := make([]string, 0, initialCapacity)
	for channel := range chans {
		updatedKeys = append(updatedKeys, channel)
	}
	if len(waiter.userKeys) > 0 {
		updatedKeys = append(updatedKeys, waiter.userKeys...)
	}
	waiter.keys = updatedKeys

}

// Returns the set of user keys for this ChangeWaiter
func (waiter *ChangeWaiter) GetUserKeys() (result []string) {
	if len(waiter.userKeys) == 0 {
		return result
	}
	result = make([]string, len(waiter.userKeys))
	copy(result, waiter.userKeys)
	return result
}

// Refresh user keys refreshes the waiter's userKeys (users and roles).  Required
// when the user associated with a waiter has roles, and the user doc is updated.
// Does NOT add the keys to waiter.keys - UpdateChannels must be invoked if
// that's required.
func (waiter *ChangeWaiter) RefreshUserKeys(user auth.User) {
	if user != nil {
		// waiter.userKeys only need to be updated if roles have changed - skip if
		// the previous user didn't have roles, and the new user doesn't have roles.
		if len(waiter.userKeys) == 1 && len(user.RoleNames()) == 0 {
			return
		}
		waiter.userKeys = []string{base.UserPrefix + user.Name()}
		for role := range user.RoleNames() {
			waiter.userKeys = append(waiter.userKeys, base.RolePrefix+role)
		}
		waiter.lastUserCount = waiter.listener.CurrentCount(waiter.userKeys)

	}
}

func (db *Database) NewUserWaiter() *ChangeWaiter {
	return db.mutationListener.NewWaiterWithChannels(base.Set{}, db.User())
}
