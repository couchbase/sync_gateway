package db

import (
	"sync"
)

// Tracks usage count of a resource, such as the _changes feed. (Thread-safe.)
type Statistics struct {
	lock         sync.RWMutex
	currentCount uint32
	maxCount     uint32
	totalCount   uint32
}

func (stats *Statistics) Increment() {
	stats.lock.Lock()
	stats.totalCount++
	stats.currentCount++
	if stats.currentCount > stats.maxCount {
		stats.maxCount = stats.currentCount
	}
	stats.lock.Unlock()
}

func (stats *Statistics) Decrement() {
	stats.lock.Lock()
	stats.currentCount--
	stats.lock.Unlock()
}

func (stats *Statistics) TotalCount() uint32 {
	stats.lock.RLock()
	defer stats.lock.RUnlock()
	return stats.totalCount
}

func (stats *Statistics) MaxCount() uint32 {
	stats.lock.RLock()
	defer stats.lock.RUnlock()
	return stats.maxCount
}

func (stats *Statistics) Reset() {
	stats.lock.Lock()
	stats.maxCount = stats.currentCount
	stats.totalCount = 0
	stats.lock.Unlock()
}
