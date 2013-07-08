package db

import (
	"strings"
	"sync"

	"github.com/couchbaselabs/walrus"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
)

// A wrapper around a Bucket's TapFeed that allows any number of client goroutines to wait for
// changes.
type changeListener struct {
	bucket      base.Bucket
	tapFeed     base.TapFeed // Observes changes to bucket
	tapNotifier *sync.Cond   // Posts notifications when documents are updated
	counter     uint64
}

// Starts a changeListener on a given Bucket.
func (listener *changeListener) Start(bucket base.Bucket) error {
	listener.bucket = bucket
	tapFeed, err := bucket.StartTapFeed(walrus.TapArguments{Backfill: walrus.TapNoBackfill})
	if err != nil {
		return err
	}

	listener.tapFeed = tapFeed
	listener.counter = 1
	listener.tapNotifier = sync.NewCond(&sync.Mutex{})

	// Start a goroutine to broadcast to the tapNotifier whenever a channel or user/role changes:
	go func() {
		defer listener.notify("")
		for event := range tapFeed.Events() {
			if event.Opcode == walrus.TapMutation || event.Opcode == walrus.TapDeletion {
				key := string(event.Key)
				if strings.HasPrefix(key, kChannelLogKeyPrefix) ||
					strings.HasPrefix(key, auth.UserKeyPrefix) ||
					strings.HasPrefix(key, auth.RoleKeyPrefix) {
					listener.notify(key)
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

// Changes the counter, notifying waiting clients.
func (listener *changeListener) notify(key string) {
	listener.tapNotifier.L.Lock()
	if key == "" {
		listener.counter = 0
	} else {
		listener.counter++
	}
	base.LogTo("Changes+", "Notifying that %q changed (key=%q) count=%d",
		listener.bucket.GetName(), key, listener.counter)
	listener.tapNotifier.Broadcast()
	listener.tapNotifier.L.Unlock()
}

// Waits until the counter exceeds the given value. Returns the new counter.
func (listener *changeListener) Wait(counter uint64) uint64 {
	listener.tapNotifier.L.Lock()
	defer listener.tapNotifier.L.Unlock()
	base.LogTo("Changes+", "Waiting for %q's count to pass %d",
		listener.bucket.GetName(), listener.counter)
	for listener.counter == counter {
		listener.tapNotifier.Wait()
	}
	return listener.counter
}

// Returns the current value of the counter.
func (listener *changeListener) CurrentCount() uint64 {
	listener.tapNotifier.L.Lock()
	defer listener.tapNotifier.L.Unlock()
	return listener.counter
}

// Helper for waiting on a changeListener. Every call to wait() will wait for the
// listener's counter to increment from the value at the last call.
type changeWaiter struct {
	listener    *changeListener
	lastCounter uint64
}

// Creates a new changeWaiter.
func (listener *changeListener) NewWaiter() changeWaiter {
	return changeWaiter{
		listener:    listener,
		lastCounter: listener.CurrentCount(),
	}
}

// Waits for the changeListener's counter to change from the last time Wait() was called.
func (waiter *changeWaiter) Wait() bool {
	waiter.lastCounter = waiter.listener.Wait(waiter.lastCounter)
	return waiter.lastCounter > 0
}
