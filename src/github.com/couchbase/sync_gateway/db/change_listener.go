package db

import (
	"log"
	"strings"
	"sync"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// A wrapper around a Bucket's TapFeed that allows any number of client goroutines to wait for
// changes.
type changeListener struct {
	bucket       base.Bucket
	tapFeed      base.TapFeed           // Observes changes to bucket
	tapNotifier  *sync.Cond             // Posts notifications when documents are updated
	counter      uint64                 // Event counter; increments on every doc update
	keyCounts    map[string]uint64      // Latest count at which each doc key was updated
	DocChannel   chan sgbucket.TapEvent // Passthru channel for doc mutations
	OnDocChanged func(docID string, jsonData []byte)
}

// Starts a changeListener on a given Bucket.
func (listener *changeListener) Start(bucket base.Bucket, trackDocs bool) error {
	listener.bucket = bucket
	tapFeed, err := bucket.StartTapFeed(sgbucket.TapArguments{Backfill: sgbucket.TapNoBackfill})
	if err != nil {
		return err
	}

	listener.tapFeed = tapFeed
	listener.counter = 1
	listener.keyCounts = map[string]uint64{}
	listener.tapNotifier = sync.NewCond(&sync.Mutex{})
	if trackDocs {
		listener.DocChannel = make(chan sgbucket.TapEvent, 100)
	}

	// Start a goroutine to broadcast to the tapNotifier whenever a channel or user/role changes:
	go func() {
		defer func() {
			listener.notifyStopping()
			if listener.DocChannel != nil {
				close(listener.DocChannel)
			}
		}()
		for event := range tapFeed.Events() {
			if event.Opcode == sgbucket.TapMutation || event.Opcode == sgbucket.TapDeletion {
				key := string(event.Key)
				if strings.HasPrefix(key, auth.UserKeyPrefix) ||
					strings.HasPrefix(key, auth.RoleKeyPrefix) {
					if listener.OnDocChanged != nil {
						listener.OnDocChanged(key, event.Value)
					}
					listener.Notify(base.SetOf(key))
				} else if trackDocs && !strings.HasPrefix(key, kSyncKeyPrefix) {
					if listener.OnDocChanged != nil {
						listener.OnDocChanged(key, event.Value)
					}
					listener.DocChannel <- event
				}
			}
		}
	}()

	return nil
}

// Stops a changeListener. Any pending Wait() calls will immediately return false.
func (listener *changeListener) Stop() {
	if listener.tapFeed != nil {
		listener.tapFeed.Close()
	}
}

//////// NOTIFICATIONS:

// Changes the counter, notifying waiting clients.
func (listener *changeListener) Notify(keys base.Set) {
	if len(keys) == 0 {
		return
	}
	listener.tapNotifier.L.Lock()
	listener.counter++
	for key, _ := range keys {
		listener.keyCounts[key] = listener.counter
	}
	base.LogTo("Changes+", "Notifying that %q changed (keys=%q) count=%d",
		listener.bucket.GetName(), keys, listener.counter)
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

// Waits until the counter exceeds the given value. Returns the new counter.
func (listener *changeListener) Wait(keys []string, counter uint64) uint64 {
	listener.tapNotifier.L.Lock()
	defer listener.tapNotifier.L.Unlock()
	base.LogTo("Changes+", "Waiting for %q's count to pass %d",
		listener.bucket.GetName(), counter)
	for {
		base.LogTo("Changes+", "getting curCounter for keys: %d", keys)
		curCounter := listener._currentCount(keys)
		base.LogTo("Changes+", "comparing curCounter, counter:%d, %d", curCounter, counter)
		if curCounter != counter {
			base.LogTo("Changes+", "returning curCounter: %d", curCounter)
			return curCounter
		}
		base.LogTo("Changes+", "starting wait")
		listener.tapNotifier.Wait()
		base.LogTo("Changes+", "woke up from wait")
	}
}

// Returns the max value of the counter for all the given keys
func (listener *changeListener) CurrentCount(keys []string) uint64 {
	listener.tapNotifier.L.Lock()
	defer listener.tapNotifier.L.Unlock()
	log.Println("current count with keys:%v ==> %d", keys, listener._currentCount(keys))
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
	listener    *changeListener
	keys        []string
	userKeys    []string
	lastCounter uint64
}

// Creates a new changeWaiter that will wait for changes for the given document keys.
func (listener *changeListener) NewWaiter(keys []string) *changeWaiter {
	return &changeWaiter{
		listener:    listener,
		keys:        keys,
		lastCounter: listener.CurrentCount(keys),
	}
}

func (listener *changeListener) NewWaiterWithChannels(chans base.Set, user auth.User) *changeWaiter {
	waitKeys := make([]string, 0, 5)
	for channel, _ := range chans {
		waitKeys = append(waitKeys, channel)
	}
	var userKeys []string
	if user != nil {
		userKeys = []string{auth.UserKeyPrefix + user.Name()}
		for role, _ := range user.RoleNames() {
			userKeys = append(userKeys, auth.RoleKeyPrefix+role)
		}
		waitKeys = append(waitKeys, userKeys...)
	}
	waiter := listener.NewWaiter(waitKeys)
	waiter.userKeys = userKeys
	return waiter
}

// Waits for the changeListener's counter to change from the last time Wait() was called.
func (waiter *changeWaiter) Wait() bool {
	waiter.lastCounter = waiter.listener.Wait(waiter.keys, waiter.lastCounter)
	return waiter.lastCounter > 0
}

// Returns the current counter value for the waiter's user (and roles).
// If this value changes, it means the user or roles have been updated.
func (waiter *changeWaiter) CurrentUserCount() uint64 {
	if waiter.userKeys == nil {
		return 0
	}
	return waiter.listener.CurrentCount(waiter.userKeys)
}

// Updates the set of channel keys in the ChangeWaiter (maintains the existing set of user keys)
func (waiter *changeWaiter) UpdateChannels(chans channels.TimedSet) {
	initialCapacity := len(chans) + len(waiter.userKeys)
	updatedKeys := make([]string, 0, initialCapacity)
	for channel, _ := range chans {
		updatedKeys = append(updatedKeys, channel)
	}
	if len(waiter.userKeys) > 0 {
		updatedKeys = append(updatedKeys, waiter.userKeys...)
	}
	waiter.keys = updatedKeys

}
