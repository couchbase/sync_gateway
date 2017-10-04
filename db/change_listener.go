package db

import (
	"math"
	"strings"
	"sync"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// A wrapper around a Bucket's TapFeed that allows any number of client goroutines to wait for
// changes.
type changeListener struct {
	bucket                base.Bucket
	bucketName            string                  // Used for logging
	tapFeed               base.TapFeed            // Observes changes to bucket
	tapNotifier           *sync.Cond              // Posts notifications when documents are updated
	FeedArgs              sgbucket.FeedArguments  // The Tap Args (backfill, etc)
	counter               uint64                  // Event counter; increments on every doc update
	terminateCheckCounter uint64                  // Termination Event counter; increments on every notifyCheckForTermination
	keyCounts             map[string]uint64       // Latest count at which each doc key was updated
	DocChannel            chan sgbucket.FeedEvent // Passthru channel for doc mutations
	OnDocChanged          DocChangedFunc          // Called when change arrives on feed
	trackDocs             bool                    // Whether events should be routed to DocChannel passthru

}

type DocChangedFunc func(event sgbucket.FeedEvent)

func (listener *changeListener) Init(name string) {
	listener.bucketName = name
	listener.counter = 1
	listener.terminateCheckCounter = 0
	listener.keyCounts = map[string]uint64{}
	listener.tapNotifier = sync.NewCond(&sync.Mutex{})
}

// Starts a changeListener on a given Bucket.
func (listener *changeListener) Start(bucket base.Bucket, trackDocs bool, backfillMode uint64, terminator chan bool, bucketStateNotify sgbucket.BucketNotifyFn) error {
	listener.bucket = bucket
	listener.bucketName = bucket.GetName()
	listener.FeedArgs = sgbucket.FeedArguments{
		Backfill:   backfillMode,
		Notify:     bucketStateNotify,
		Terminator: terminator,
	}

	if trackDocs {
		listener.DocChannel = make(chan sgbucket.FeedEvent, 100)
		listener.trackDocs = true
	}

	return listener.StartMutationFeed(bucket)
}

func (listener *changeListener) StartMutationFeed(bucket base.Bucket) error {

	// Uses DCP by default, unless TAP is explicitly specified
	feedType := base.GetFeedType(bucket)
	switch feedType {
	case base.TapFeedType:
		// TAP Feed
		//    TAP feed is a go-channel of Tap events served by the bucket.  Start the feed, then
		//    start a goroutine to work the event channel, calling ProcessEvent for each event
		var err error
		listener.tapFeed, err = bucket.StartTapFeed(listener.FeedArgs)
		if err != nil {
			return err
		}
		go func() {
			defer func() {
				listener.notifyStopping()
				if listener.DocChannel != nil {
					close(listener.DocChannel)
				}
			}()
			for event := range listener.tapFeed.Events() {
				listener.ProcessFeedEvent(event)
			}
		}()
		return nil
	default:
		// DCP Feed
		//    DCP receiver isn't go-channel based - DCPReceiver calls ProcessEvent directly.
		base.LogTo("Feed", "Using DCP feed for bucket: %q (based on feed_type specified in config file)", bucket.GetName())
		return bucket.StartDCPFeed(listener.FeedArgs, listener.ProcessFeedEvent)
	}
}

// ProcessFeedEvent is invoked for each mutate or delete event seen on the server's mutation feed (TAP or DCP).  Uses document
// key to determine handling, based on whether the incoming mutation is an internal Sync Gateway document.
func (listener *changeListener) ProcessFeedEvent(event sgbucket.FeedEvent) bool {
	requiresCheckpointPersistence := true
	if event.Opcode == sgbucket.FeedOpMutation || event.Opcode == sgbucket.FeedOpDeletion {
		key := string(event.Key)
		if strings.HasPrefix(key, auth.UserKeyPrefix) ||
			strings.HasPrefix(key, auth.RoleKeyPrefix) { // SG users and roles
			if listener.OnDocChanged != nil && event.Opcode == sgbucket.FeedOpMutation {
				listener.OnDocChanged(event)
			}
			listener.Notify(base.SetOf(key))
		} else if strings.HasPrefix(key, UnusedSequenceKeyPrefix) { // SG unused sequence marker docs
			if listener.OnDocChanged != nil {
				listener.OnDocChanged(event)
			}
		} else if strings.HasPrefix(key, base.DCPCheckpointPrefix) { // SG DCP checkpoint docs
			// Do not require checkpoint persistence when DCP checkpoint docs come back over DCP - otherwise
			// we'll end up in a feedback loop for their vbucket
			requiresCheckpointPersistence = false
		} else if !strings.HasPrefix(key, KSyncKeyPrefix) && !strings.HasPrefix(key, base.KIndexPrefix) { // Non-SG docs
			if listener.OnDocChanged != nil {
				listener.OnDocChanged(event)
			}
			if listener.trackDocs {
				listener.DocChannel <- event
			}

		}
	}
	return requiresCheckpointPersistence
}

// Stops a changeListener. Any pending Wait() calls will immediately return false.
func (listener *changeListener) Stop() {
	if listener.tapFeed != nil {
		listener.tapFeed.Close()
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
	base.LogTo("Changes+", "Notifying that %q changed (keys=%q) count=%d",
		listener.bucketName, keys, listener.counter)
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

	base.LogTo("Changes+", "Notifying to check for _changes feed termination")
	listener.tapNotifier.Broadcast()
	listener.tapNotifier.L.Unlock()
}

func (listener *changeListener) notifyStopping() {
	listener.tapNotifier.L.Lock()
	listener.counter = 0
	listener.keyCounts = map[string]uint64{}
	base.LogTo("Changes+", "Notifying that changeListener is stopping")
	listener.tapNotifier.Broadcast()
	listener.tapNotifier.L.Unlock()
}

// Waits until either the counter, or terminateCheckCounter exceeds the given value. Returns the new counters.
func (listener *changeListener) Wait(keys []string, counter uint64, terminateCheckCounter uint64) (uint64, uint64) {
	listener.tapNotifier.L.Lock()
	defer listener.tapNotifier.L.Unlock()
	base.LogTo("Changes+", "No new changes to send to change listener.  Waiting for %q's count to pass %d",
		listener.bucketName, counter)
	for {
		curCounter := listener._currentCount(keys)

		if curCounter != counter || listener.terminateCheckCounter != terminateCheckCounter {
			return curCounter, listener.terminateCheckCounter
		}
		listener.tapNotifier.Wait()
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
type changeWaiter struct {
	listener                  *changeListener
	keys                      []string
	userKeys                  []string
	lastCounter               uint64
	lastTerminateCheckCounter uint64
	lastUserCount             uint64
}

// Creates a new changeWaiter that will wait for changes for the given document keys.
func (listener *changeListener) NewWaiter(keys []string) *changeWaiter {
	return &changeWaiter{
		listener:                  listener,
		keys:                      keys,
		lastCounter:               listener.CurrentCount(keys),
		lastTerminateCheckCounter: listener.terminateCheckCounter,
	}
}

func (listener *changeListener) NewWaiterWithChannels(chans base.Set, user auth.User) *changeWaiter {
	waitKeys := make([]string, 0, 5)
	for channel := range chans {
		waitKeys = append(waitKeys, channel)
	}
	var userKeys []string
	if user != nil {
		userKeys = []string{auth.UserKeyPrefix + user.Name()}
		for role := range user.RoleNames() {
			userKeys = append(userKeys, auth.RoleKeyPrefix+role)
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
func (waiter *changeWaiter) Wait() uint32 {

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
func (waiter *changeWaiter) CurrentUserCount() uint64 {
	return waiter.lastUserCount
}

// Updates the set of channel keys in the ChangeWaiter (maintains the existing set of user keys)
func (waiter *changeWaiter) UpdateChannels(chans channels.TimedSet) {
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
func (waiter *changeWaiter) GetUserKeys() (result []string) {
	if len(waiter.userKeys) == 0 {
		return result
	}
	result = make([]string, len(waiter.userKeys))
	copy(result, waiter.userKeys)
	return result
}
