package channels

import (
	"expvar"
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

// activeChannels is a concurrency-safe map of active replications per channel, modified via
// incr/decr operations.
// Incrementing a channel not already in the map adds it to the map.
// Decrementing a channel's active count to zero removes its entry from the map.
//
// Note: private properties shouldn't be accessed directly, to support potential
// refactoring of activeChannels to use a sharded map as needed.
type ActiveChannels struct {
	channelCounts map[string]uint64 // Count of active pull replications (changes) per channel
	lock          sync.RWMutex      // Mutex for channelCounts map access
	countStat     *expvar.Int       // Channel count stat
}

func NewActiveChannels(activeChannelCountStat *expvar.Int) ActiveChannels {
	return ActiveChannels{
		channelCounts: make(map[string]uint64),
		countStat:     activeChannelCountStat,
	}
}

// Update changed increments/decrements active channel counts based on a set of changed channels.  Triggered
// when the set of channels being replicated by a given replication changes.
func (ac *ActiveChannels) UpdateChanged(changedChannels ChangedKeys) {
	ac.lock.Lock()
	for channelName, isIncrement := range changedChannels {
		if isIncrement {
			ac._incr(channelName)
		} else {
			ac._decr(channelName)
		}
	}

	ac.lock.Unlock()
}

// Active channel counts track channels being replicated by an active changes request.
func (ac *ActiveChannels) IncrChannels(chans TimedSet) {
	ac.lock.Lock()
	for channelName, _ := range chans {
		ac._incr(channelName)
	}
	ac.lock.Unlock()
}

func (ac *ActiveChannels) DecrChannels(chans TimedSet) {
	ac.lock.Lock()
	for channelName, _ := range chans {
		ac._decr(channelName)
	}
	ac.lock.Unlock()
}

func (ac *ActiveChannels) isActive(channelName string) bool {
	ac.lock.RLock()
	_, ok := ac.channelCounts[channelName]
	ac.lock.RUnlock()
	return ok
}

func (ac *ActiveChannels) _incr(channelName string) {
	current, ok := ac.channelCounts[channelName]
	if !ok {
		ac.countStat.Add(1)
	}
	ac.channelCounts[channelName] = current + 1
}

func (ac *ActiveChannels) _decr(channelName string) {
	current, ok := ac.channelCounts[channelName]
	if !ok {
		base.Warnf(base.KeyCache, "Attempt made to decrement inactive channel %s - will be ignored", base.UD(channelName))
		return
	}
	if current <= 1 {
		delete(ac.channelCounts, channelName)
		ac.countStat.Add(-1)
	} else {
		ac.channelCounts[channelName] = current - 1
	}
}
