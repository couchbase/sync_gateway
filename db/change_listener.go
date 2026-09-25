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
	"bytes"
	"context"
	"expvar"
	"math"
	"sync"
	"sync/atomic"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

const (
	DefaultBroadcastChangesTime         = 50 * time.Millisecond
	SkippedSequenceBroadcastChangesTime = 500 * time.Millisecond
)

// A wrapper around a Bucket's TapFeed that allows any number of client goroutines to wait for
// changes.
type changeListener struct {
	ctx                    context.Context
	dbCtx                  *DatabaseContext
	bucket                 base.Bucket
	bucketName             string                 // Used for logging
	tapNotifier            *sync.Cond             // Posts notifications when documents are updated
	counter                uint64                 // Event counter; increments on every doc update
	_terminateCheckCounter uint64                 // Termination Event counter; increments on every notifyCheckForTermination
	keyCounts              map[channels.ID]uint64 // Latest count at which each doc key was updated
	// principalCountsLock guards insertion into principalCounts.  It is a leaf lock: it must never
	// be taken while tapNotifier.L is held (see notifyKey), so it can never nest with it either way.
	principalCountsLock sync.Mutex
	// principalCounts is a per-principal copy of keyCounts, populated only for user/role keys (see
	// notifyKey), readable without tapNotifier.L.  This is what lets refreshUser check the user
	// count on every inbound BLIP message without queueing behind a caching-feed broadcast.
	// Entries are created once, on first use, and are never replaced or removed: a ChangeWaiter
	// caches the *atomic.Uint64 pointer directly, so replacing the map entry would silently orphan
	// every waiter still holding the old pointer.  Waiter construction creates entries too, not just
	// notifyKey, so the map retains ~100 bytes per principal that has ever connected, for the
	// lifetime of the listener.
	principalCounts          map[channels.ID]*atomic.Uint64
	OnChangeCallback         DocChangedFunc
	terminator               chan bool          // Signal to cause DCP feed to exit
	doneChan                 <-chan error       // Channel that's closed when DCP feed has exited
	broadcastChangesDoneChan chan struct{}      // Channel to signal that broadcast changes goroutine has terminated
	sgCfgPrefix              string             // SG config key prefix
	started                  base.AtomicBool    // whether the feed has been started
	metaKeys                 *base.MetadataKeys // Metadata key formatter
	feedDelay                time.Duration      // testing seam to add an artificial delay to processing of each DCP event in the caching feed
	principalDocFeedDelay    time.Duration      // testing seam to add an artificial delay to processing of principal doc DCP events in the caching feed
}

// unusedSeqChannelID marks the unused sequence key for the channel cache. This is a marker that is global to all collections.
var unusedSeqChannelID = channels.NewID(unusedSeqKey, unusedSeqCollectionID)

// principalDocCollectionIDForChannelID is the collection ID for construction of channel ID for principal documents (users, roles).
const principalDocCollectionIDForChannelID = 0

type DocChangedFunc func(event sgbucket.FeedEvent, docType DocumentType)

// newChangeListener creates a new changeListener to listen for feed events.
func newChangeListener(name string, groupID string, db *DatabaseContext) (*changeListener, error) {
	listener := &changeListener{}
	listener.bucketName = name
	listener.counter = 1
	listener._terminateCheckCounter = 0
	listener.keyCounts = map[channels.ID]uint64{}
	listener.principalCounts = map[channels.ID]*atomic.Uint64{}
	listener.tapNotifier = sync.NewCond(&sync.Mutex{})
	listener.sgCfgPrefix = db.MetadataKeys.SGCfgPrefix(groupID)
	listener.metaKeys = db.MetadataKeys
	listener.broadcastChangesDoneChan = make(chan struct{})
	listener.dbCtx = db
	var err error
	listener.feedDelay, err = GetCachingFeedDelay()
	if err != nil {
		return nil, err
	}
	listener.principalDocFeedDelay, err = GetCachingFeedPrincipalDocDelay()
	if err != nil {
		return nil, err
	}
	return listener, nil
}

func (listener *changeListener) OnDocChanged(event sgbucket.FeedEvent, docType DocumentType) {
	if listener.feedDelay > 0 {
		time.Sleep(listener.feedDelay)
	}
	// TODO: When principal grants are implemented (CBG-2333), perform collection filtering here
	listener.OnChangeCallback(event, docType)
}

// cachingFeedCollections returns the set of (scope, collection) pairs that the caching DCP
// feed must subscribe to. When metadataStore is a *base.MetadataStore whose migration is still
// in flight, both its primary (_system._mobile) and fallback (_default._default) datastores are
// included so that _sync:* mutations are observed regardless of which collection currently holds
// the doc. Once migration is complete, metadata lives solely on primary, so the fallback is
// omitted — it would otherwise be redundant, and a customer who has dropped _default._default
// post-migration would fail the DCP feed Start with "collection _default not found".
func cachingFeedCollections(metadataStore base.DataStore, scopes map[string]Scope) base.CollectionNameSet {
	collectionNames := base.NewCollectionNameSet()
	if ms, ok := metadataStore.(*base.MetadataStore); ok {
		collectionNames.Add(ms.Primary())
		if ms.FallbackReadsEnabled() {
			collectionNames.Add(ms.Fallback())
		}
	} else {
		collectionNames.Add(metadataStore)
	}
	for scopeName, collections := range scopes {
		for collectionName := range collections.Collections {
			collectionNames.Add(sgbucket.DataStoreNameImpl{Scope: scopeName, Collection: collectionName})
		}
	}
	return collectionNames
}

// Starts a changeListener on a given Bucket.
func (listener *changeListener) Start(ctx context.Context, bucket base.Bucket, dbStats *expvar.Map, scopes map[string]Scope, metadataStore base.DataStore) error {

	listener.terminator = make(chan bool)
	listener.bucket = bucket
	listener.bucketName = bucket.GetName()

	collectionNames := cachingFeedCollections(metadataStore, scopes)
	listener.StartNotifierBroadcaster(ctx) // start broadcast changes goroutine

	opts := base.DCPClientOptions{
		FeedID:             base.DCPCachingFeedID,
		Callback:           listener.ProcessFeedEvent,
		Terminator:         listener.terminator,
		FromLatestSequence: true,
		CollectionNames:    collectionNames,
		DBStats:            dbStats,
		MetadataStoreType:  base.DCPMetadataStoreInMemory,
		FeedContent:        sgbucket.FeedContentXattrOnly,
	}
	var err error
	listener.doneChan, err = base.StartDCPFeed(ctx, bucket, opts)
	if err != nil {
		return err
	}
	listener.started.Set(true)
	return nil
}

// DocumentType returns the type of document received over mutation feed based on its key prefix.
func (listener *changeListener) DocumentType(key []byte) DocumentType {
	if bytes.HasPrefix(key, []byte(listener.metaKeys.UserKeyPrefix())) {
		return DocTypeUser
	} else if bytes.HasPrefix(key, []byte(listener.metaKeys.RoleKeyPrefix())) {
		return DocTypeRole
	} else if bytes.HasPrefix(key, []byte(listener.metaKeys.UnusedSeqPrefix())) {
		return DocTypeUnusedSeq
	} else if bytes.HasPrefix(key, []byte(listener.metaKeys.UnusedSeqRangePrefix())) {
		return DocTypeUnusedSeqRange
	}
	return DocTypeUnknown
}

// ProcessFeedEvent is invoked for each mutate or delete event seen on the server's mutation feed (TAP or DCP).  Uses document
// key to determine handling, based on whether the incoming mutation is an internal Sync Gateway document.
func (listener *changeListener) ProcessFeedEvent(event sgbucket.FeedEvent) bool {
	if event.Opcode == sgbucket.FeedOpMutation || event.Opcode == sgbucket.FeedOpDeletion {
		if !bytes.HasPrefix(event.Key, []byte(base.SyncDocPrefix)) {
			listener.OnDocChanged(event, DocTypeDocument)
			return true
		}
	} else {
		// backfill or unknown opcodes
		return true
	}
	// SG DCP checkpoint docs (including other config group IDs)
	if bytes.HasPrefix(event.Key, []byte(base.DCPCheckpointRootPrefix)) {
		// Do not require checkpoint persistence when DCP checkpoint docs come back over DCP - otherwise
		// we'll end up in a feedback loop for their vbucket if persistence is enabled
		// NOTE: checkpoint persistence is disabled altogether for the caching feed.  Leaving this check in place
		// defensively.
		return false
	}

	// Cfg callback supports both mutation and deletion events
	if bytes.HasPrefix(event.Key, []byte(listener.sgCfgPrefix)) {
		listener.OnDocChanged(event, DocTypeSGCfg)
		return true
	}

	// Notify for principal mutations *and* deletions to wake changes feeds.
	docType := listener.DocumentType(event.Key)
	if docType == DocTypeUser || docType == DocTypeRole {
		if listener.principalDocFeedDelay > 0 {
			time.Sleep(listener.principalDocFeedDelay)
		}
		// defer to notify after callback completion
		key := channels.NewID(string(event.Key), principalDocCollectionIDForChannelID)
		defer listener.notifyKey(listener.ctx, key)
	}

	if event.Opcode != sgbucket.FeedOpMutation {
		// nothing more to handle at this point if the event is not a mutation
		return true
	}

	listener.OnDocChanged(event, docType)
	return true
}

// MutationFeedStopMaxWait is the maximum amount of time to wait for
// mutation feed worker goroutine to terminate before the server is stopped.
const MutationFeedStopMaxWait = 30 * time.Second

// Stops a changeListener. Any pending Wait() calls will immediately return false.
func (listener *changeListener) Stop(ctx context.Context) {

	base.DebugfCtx(ctx, base.KeyChanges, "changeListener.Stop() called")

	if !listener.started.CompareAndSwap(true, false) {
		// not started, nothing to do
		return
	}

	if listener.terminator != nil {
		close(listener.terminator)
	}

	if listener.tapNotifier != nil {
		// Unblock any change listeners blocked on tapNotifier.Wait()
		listener.tapNotifier.Broadcast()
	}

	// Wait for mutation feed worker to terminate.
	waitTime := MutationFeedStopMaxWait
	select {
	case <-listener.doneChan:
		// Mutation feed worker goroutine is terminated and doneChan is already closed.
	case <-time.After(waitTime):
		base.WarnfCtx(ctx, "Timeout after %v of waiting for mutation feed worker to terminate", waitTime)
	}

	// wait for the broadcast changes goroutine to terminate
	select {
	case <-listener.broadcastChangesDoneChan:
		// Broadcast changes goroutine has terminated
	case <-time.After(waitTime):
		base.WarnfCtx(ctx, "Timeout after %v of waiting for broadcast changes goroutine to terminate", waitTime)
	}
}

//////// NOTIFICATIONS:

// Changes the counter, notifying waiting clients.
//
// Notify must not be used for principal (user/role) keys.  notifyKey is the only writer of
// principal keys into keyCounts, and it also stores into principalCounts so that the per-BLIP-
// message user count check (RefreshUserCount) can read it without tapNotifier.L.  Routing a
// principal key through Notify instead would advance keyCounts without advancing the matching
// principalCounts entry: the feed would still wake, but checkForUserUpdates would see no user
// count change and skip the reload, silently serving stale channel access.
func (listener *changeListener) Notify(ctx context.Context, keys channels.Set) {

	if len(keys) == 0 {
		return
	}
	listener.tapNotifier.L.Lock()
	listener.counter++
	for key := range keys {
		listener.keyCounts[key] = listener.counter
	}
	base.DebugfCtx(ctx, base.KeyChanges, "Listener keys %q for %s have changed, count=%d",
		base.UD(keys), base.MD(listener.bucketName), listener.counter)
	listener.tapNotifier.L.Unlock()
}

func (listener *changeListener) StartNotifierBroadcaster(ctx context.Context) {
	ticker := time.NewTicker(listener.broadcastInterval(false))
	// boolean to indicate whether ticker is using the default value, this is needed so we don't call reset on ticker
	// for a value it already has
	broadcastSlowMode := false
	go func(terminator chan bool, doneChan chan struct{}) {
		defer func() {
			close(doneChan)
		}()
		var currCount uint64
		for {
			select {
			case <-terminator:
				ticker.Stop()
				return
			case <-ticker.C:
				// if the counter has changed, notify waiting clients
				listener.tapNotifier.L.Lock()
				if listener.counter > currCount {
					base.DebugfCtx(ctx, base.KeyChanges, "Notifying changes for %s count=%d", base.MD(listener.bucketName), listener.counter)
					listener.tapNotifier.Broadcast()
					currCount = listener.counter
				}
				listener.tapNotifier.L.Unlock()

				// check if we need to reset ticker value based on skipped sequence presence
				newBroadcastSlowMode := listener.dbCtx.BroadcastSlowMode.Load()
				if broadcastSlowMode != newBroadcastSlowMode {
					// broadcast changes interval has changed, reset ticker
					duration := listener.broadcastInterval(newBroadcastSlowMode)
					base.DebugfCtx(ctx, base.KeyChanges, "Updating broadcast changes interval for %q to %v", base.MD(listener.bucketName), duration)
					broadcastSlowMode = newBroadcastSlowMode
					ticker.Reset(duration)
				}
			}
		}
	}(listener.terminator, listener.broadcastChangesDoneChan)
}

// broadcastInterval returns the duration for the broadcast ticker based on whether skipped sequences are
// present, honoring the optional per-DB CacheOptions override and falling back to the package defaults when
// unset. The override is an internal tuning (primarily to keep tests fast); production uses the defaults.
func (listener *changeListener) broadcastInterval(skippedSequencePresent bool) time.Duration {
	opts := listener.dbCtx.Options.CacheOptions
	if skippedSequencePresent {
		if opts != nil && opts.SkippedSequenceBroadcastInterval > 0 {
			return opts.SkippedSequenceBroadcastInterval
		}
		return SkippedSequenceBroadcastChangesTime
	}
	if opts != nil && opts.BroadcastChangesInterval > 0 {
		return opts.BroadcastChangesInterval
	}
	return DefaultBroadcastChangesTime
}

// Changes the counter, notifying waiting clients. Only use for a key update.
func (listener *changeListener) notifyKey(ctx context.Context, key channels.ID) {
	// Resolve the principal counter before taking tapNotifier.L: principalCountsLock is a leaf
	// lock and must never be taken while tapNotifier.L is held.
	principalCount := listener.principalCounter(key)

	listener.tapNotifier.L.Lock()
	defer listener.tapNotifier.L.Unlock()
	listener.counter++
	listener.keyCounts[key] = listener.counter
	// Store while still holding tapNotifier.L, with the same value just written to keyCounts.  A
	// waiter woken by this notification re-acquires tapNotifier.L (inside Cond.Wait) before
	// checking keyCounts, so this store happens-before that check, and the waiter is guaranteed to
	// observe at least this value when it reads the counter lock-free afterwards.
	principalCount.Store(listener.counter)
	base.DebugfCtx(ctx, base.KeyChanges, "Notifying that %q changed (key=%q) count=%d",
		base.MD(listener.bucketName), base.UD(key), listener.counter)
	listener.tapNotifier.Broadcast()
}

// Changes the counter, notifying waiting clients.
func (listener *changeListener) NotifyCheckForTermination(ctx context.Context, keys base.Set) {
	if len(keys) == 0 {
		return
	}
	listener.tapNotifier.L.Lock()

	// Increment terminateCheckCounter, but loop back to zero
	//if we have reached maximum value for uint64 type
	if listener._terminateCheckCounter < math.MaxUint64 {
		listener._terminateCheckCounter++
	} else {
		listener._terminateCheckCounter = 0
	}

	base.DebugfCtx(ctx, base.KeyChanges, "Notifying to check for _changes feed termination")
	listener.tapNotifier.Broadcast()
	listener.tapNotifier.L.Unlock()
}

// Waits until either the counter, or terminateCheckCounter exceeds the given value. Returns the new counters.
func (listener *changeListener) Wait(ctx context.Context, keys []channels.ID, counter uint64, terminateCheckCounter uint64) (uint64, uint64) {
	listener.tapNotifier.L.Lock()
	defer listener.tapNotifier.L.Unlock()
	base.DebugfCtx(ctx, base.KeyChanges, "No new changes to send to change listener.  Waiting for %q's count to pass %d",
		base.MD(listener.bucketName), counter)

	for {
		curCounter := listener._currentCount(keys)

		if curCounter != counter || listener._terminateCheckCounter != terminateCheckCounter {
			return curCounter, listener._terminateCheckCounter
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

func (listener *changeListener) _currentCount(keys []channels.ID) uint64 {
	var max uint64 = 0
	for _, key := range keys {
		if count := listener.keyCounts[key]; count > max {
			max = count
		}
	}
	return max
}

// principalCounter returns the counter for a principal key, creating it if this is the first time
// the key has been seen.  The returned pointer is stable for the lifetime of the listener - see the
// comment on _principalCounter - so a caller can read from it without holding tapNotifier.L, which
// is what lets refreshUser check the user count without queueing behind a caching-feed broadcast.
func (listener *changeListener) principalCounter(key channels.ID) *atomic.Uint64 {
	listener.principalCountsLock.Lock()
	defer listener.principalCountsLock.Unlock()
	return listener._principalCounter(key)
}

// principalCounters returns the counters for a set of principal keys, creating any that don't
// already exist.  Returns nil for an empty key set (the nil-user waiter shape).
func (listener *changeListener) principalCounters(keys []channels.ID) []*atomic.Uint64 {
	if len(keys) == 0 {
		return nil
	}
	counters := make([]*atomic.Uint64, 0, len(keys))
	listener.principalCountsLock.Lock()
	defer listener.principalCountsLock.Unlock()
	for _, key := range keys {
		counters = append(counters, listener._principalCounter(key))
	}
	return counters
}

// _principalCounter requires principalCountsLock to be held.  An existing counter is always
// returned as-is: replacing it would orphan any ChangeWaiter that has already cached the old
// pointer, silently cutting it off from further updates for that key.
func (listener *changeListener) _principalCounter(key channels.ID) *atomic.Uint64 {
	if counter, ok := listener.principalCounts[key]; ok {
		return counter
	}
	counter := &atomic.Uint64{}
	listener.principalCounts[key] = counter
	return counter
}

// maxPrincipalCount returns the highest value held by the given counters, read without any lock.
// This is safe because every counter is a snapshot of the single, monotonically increasing
// changeListener.counter (see notifyKey): a store that lands after a scan begins used a counter
// value already greater than every value the scan could have read on ANY key, so a change can
// never be hidden behind a stale read of one of the other counters.  A torn read across keys can
// only ever move the result forward relative to an earlier scan, never back.
func maxPrincipalCount(counters []*atomic.Uint64) uint64 {
	var highest uint64
	for _, counter := range counters {
		if count := counter.Load(); count > highest {
			highest = count
		}
	}
	return highest
}

//////// CHANGE WAITER

// Helper for waiting on a changeListener. Every call to wait() will wait for the
// listener's counter to increment from the value at the last call.
type ChangeWaiter struct {
	listener                  *changeListener
	keys                      []channels.ID
	userKeys                  []channels.ID
	userCounters              []*atomic.Uint64 // Counters for userKeys, read without tapNotifier.L
	lastCounter               uint64
	lastTerminateCheckCounter uint64
	lastUserCount             uint64
	trackUnusedSequences      bool // track unused sequences in Wait functions
}

// NewWaiter a new ChangeWaiter that will wait for changes for the given document keys, and will optionally track unused sequences.
func (listener *changeListener) NewWaiter(keys []channels.ID, trackUnusedSequences bool) *ChangeWaiter {
	listener.tapNotifier.L.Lock()
	defer listener.tapNotifier.L.Unlock()
	return listener._newWaiter(keys, trackUnusedSequences)
}

// _newWaiter a new ChangeWaiter that will wait for changes for the given document keys, and will optionally track unused sequences.
func (listener *changeListener) _newWaiter(keys []channels.ID, trackUnusedSequences bool) *ChangeWaiter {
	return &ChangeWaiter{
		listener:                  listener,
		keys:                      keys,
		lastCounter:               listener._currentCount(keys),
		lastTerminateCheckCounter: listener._terminateCheckCounter,
		trackUnusedSequences:      trackUnusedSequences,
	}
}

// NewWaiterWithChannels creates ChangeWaiter for a given channel and user, and will optionally track unused sequences.
func (listener *changeListener) NewWaiterWithChannels(chans channels.Set, user auth.User, trackUnusedSequences bool) *ChangeWaiter {
	waitKeys := make([]channels.ID, 0, 5)
	for channel := range chans {
		waitKeys = append(waitKeys, channel)
	}
	var userKeys []channels.ID
	if user != nil {
		usrID := channels.NewID(listener.metaKeys.UserKey(user.Name()), principalDocCollectionIDForChannelID)
		userKeys = []channels.ID{usrID}
		for role := range user.RoleNames() {
			userKeys = append(userKeys, channels.NewID(listener.metaKeys.RoleKey(role), principalDocCollectionIDForChannelID))
		}
		waitKeys = append(waitKeys, userKeys...)
	}
	// Resolve the user's counters and baseline the user count before taking tapNotifier.L below.
	// Reading it here rather than under L can only make the baseline older, never newer: at worst
	// that costs one redundant user reload later, whereas a baseline taken after a concurrent
	// principal update landed could swallow it (see the equivalent tradeoff in RefreshUserKeys).
	userCounters := listener.principalCounters(userKeys)
	lastUserCount := maxPrincipalCount(userCounters)

	listener.tapNotifier.L.Lock()
	defer listener.tapNotifier.L.Unlock()
	waiter := listener._newWaiter(waitKeys, trackUnusedSequences)

	waiter.userKeys = userKeys
	waiter.userCounters = userCounters
	waiter.lastUserCount = lastUserCount
	return waiter
}

// Waits for the changeListener's counter to change from the last time Wait() was called.
func (waiter *ChangeWaiter) Wait(ctx context.Context) uint32 {

	lastTerminateCheckCounter := waiter.lastTerminateCheckCounter
	lastCounter := waiter.lastCounter
	waiter.lastCounter, waiter.lastTerminateCheckCounter = waiter.listener.Wait(ctx, waiter.keys, waiter.lastCounter, waiter.lastTerminateCheckCounter)
	// listener.Wait (above) re-acquires tapNotifier.L before returning, and notifyKey stores into
	// userCounters inside that same critical section (see notifyKey), so this read is guaranteed to
	// observe any principal update that could have satisfied the wait - no separate acquisition of
	// tapNotifier.L is needed here, removing what was previously a second lock acquisition per wake.
	waiter.lastUserCount = maxPrincipalCount(waiter.userCounters)
	countChanged := waiter.lastCounter > lastCounter

	// Uses != to compare as value can cycle back through 0
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
	waiter.lastUserCount = maxPrincipalCount(waiter.userCounters)
	return waiter.lastUserCount != previousCount
}

// Updates the set of channel keys in the ChangeWaiter (maintains the existing set of user keys)
func (waiter *ChangeWaiter) UpdateChannels(collectionID uint32, timedSet channels.TimedSet) {
	// This capacity is not right can not accommodate channels without iteration.
	initialCapacity := len(waiter.userKeys)
	updatedKeys := make([]channels.ID, 0, initialCapacity)
	for channelName, _ := range timedSet {
		updatedKeys = append(updatedKeys, channels.NewID(channelName, collectionID))
	}
	if waiter.trackUnusedSequences {
		updatedKeys = append(updatedKeys, unusedSeqChannelID)
	}
	if len(waiter.userKeys) > 0 {
		updatedKeys = append(updatedKeys, waiter.userKeys...)
	}
	waiter.keys = updatedKeys

}

// Refresh user keys refreshes the waiter's userKeys (users and roles).  Required
// when the user associated with a waiter has roles, and the user doc is updated.
// Does NOT add the keys to waiter.keys - UpdateChannels must be invoked if
// that's required.
func (waiter *ChangeWaiter) RefreshUserKeys(user auth.User, metaKeys *base.MetadataKeys) {
	if user != nil {
		// waiter.userKeys only need to be updated if roles have changed - skip if
		// the previous user didn't have roles, and the new user doesn't have roles.
		if len(waiter.userKeys) == 1 && len(user.RoleNames()) == 0 {
			return
		}
		waiter.userKeys = []channels.ID{channels.NewID(metaKeys.UserKey(user.Name()), principalDocCollectionIDForChannelID)}
		for role := range user.RoleNames() {
			waiter.userKeys = append(waiter.userKeys, channels.NewID(metaKeys.RoleKey(role), principalDocCollectionIDForChannelID))
		}
		waiter.userCounters = waiter.listener.principalCounters(waiter.userKeys)
		waiter.lastUserCount = maxPrincipalCount(waiter.userCounters)

	}
}

// NewUserWaiter creates a change waiter with all keys for the matching user.
func (db *Database) NewUserWaiter() *ChangeWaiter {
	trackUnusedSequences := false
	return db.mutationListener.NewWaiterWithChannels(channels.Set{}, db.User(), trackUnusedSequences)
}
