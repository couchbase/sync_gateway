// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"sync"
)

// LoggingMutex is a normal sync.Mutex that logs before and after each operation. Recommended for dev-time debugging only.
type LoggingMutex struct {
	ctx  context.Context
	name string
	sync.Mutex
}

var _ sync.Locker = &LoggingMutex{}

// NewLoggingMutex returns a new LoggingMutex with the given context/name.
func NewLoggingMutex(ctx context.Context, name string) LoggingMutex {
	return LoggingMutex{ctx: ctx, name: name}
}

// Lock locks m.
// If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (m *LoggingMutex) Lock() {
	TracefCtx(m.ctx, KeyAll, "Before %s.Lock() %s", m.getName(), GetCallersName(1, true))
	m.Mutex.Lock()
	TracefCtx(m.ctx, KeyAll, "After %s.Lock() %s", m.getName(), GetCallersName(1, true))
}

// Unlock unlocks m.
// It is a run-time error if m is not locked on entry to Unlock.
//
// A locked Mutex is not associated with a particular goroutine.
// It is allowed for one goroutine to lock a Mutex and then
// arrange for another goroutine to unlock it.
func (m *LoggingMutex) Unlock() {
	TracefCtx(m.ctx, KeyAll, "Before %s.Unlock() %s", m.getName(), GetCallersName(1, true))
	m.Mutex.Unlock()
	TracefCtx(m.ctx, KeyAll, "After %s.Unlock() %s", m.getName(), GetCallersName(1, true))
}

// TryLock tries to lock m and reports whether it succeeded.
//
// Note that while correct uses of TryLock do exist, they are rare,
// and use of TryLock is often a sign of a deeper problem
// in a particular use of mutexes.
func (m *LoggingMutex) TryLock() bool {
	TracefCtx(m.ctx, KeyAll, "Before %s.TryLock() %s", m.getName(), GetCallersName(1, true))
	result := m.Mutex.TryLock()
	TracefCtx(m.ctx, KeyAll, "After %s.TryLock() %s", m.getName(), GetCallersName(1, true))
	return result
}

// getName returns the name of the mutex for logging.
func (m *LoggingMutex) getName() string {
	if m.name != "" {
		return m.name
	}
	return "LoggingMutex"
}

// LoggingRWMutex is a normal sync.RWMutex that logs before and after each operation. Recommended for dev-time debugging only.
type LoggingRWMutex struct {
	ctx  context.Context
	name string
	sync.RWMutex
}

var _ sync.Locker = &LoggingRWMutex{}

// NewLoggingRWMutex returns a new LoggingRWMutex with the given context/name.
func NewLoggingRWMutex(ctx context.Context, name string) LoggingRWMutex {
	return LoggingRWMutex{ctx: ctx, name: name}
}

// Lock locks rw for writing.
// If the lock is already locked for reading or writing,
// Lock blocks until the lock is available.
func (m *LoggingRWMutex) Lock() {
	TracefCtx(m.ctx, KeyAll, "Before %s.Lock() %s", m.getName(), GetCallersName(1, true))
	m.RWMutex.Lock()
	TracefCtx(m.ctx, KeyAll, "After %s.Lock() %s", m.getName(), GetCallersName(1, true))
}

// Unlock unlocks rw for writing. It is a run-time error if rw is
// not locked for writing on entry to Unlock.
//
// As with Mutexes, a locked RWMutex is not associated with a particular
// goroutine. One goroutine may RLock (Lock) a RWMutex and then
// arrange for another goroutine to RUnlock (Unlock) it.
func (m *LoggingRWMutex) Unlock() {
	TracefCtx(m.ctx, KeyAll, "Before %s.Unlock() %s", m.getName(), GetCallersName(1, true))
	m.RWMutex.Unlock()
	TracefCtx(m.ctx, KeyAll, "After %s.Unlock() %s", m.getName(), GetCallersName(1, true))
}

// Happens-before relationships are indicated to the race detector via:
// - Unlock  -> Lock:  readerSem
// - Unlock  -> RLock: readerSem
// - RUnlock -> Lock:  writerSem
//
// The methods below temporarily disable handling of race synchronization
// events in order to provide the more precise model above to the race
// detector.
//
// For example, atomic.AddInt32 in RLock should not appear to provide
// acquire-release semantics, which would incorrectly synchronize racing
// readers, thus potentially missing races.

// RLock locks rw for reading.
//
// It should not be used for recursive read locking; a blocked Lock
// call excludes new readers from acquiring the lock. See the
// documentation on the RWMutex type.
func (m *LoggingRWMutex) RLock() {
	TracefCtx(m.ctx, KeyAll, "Before %s.RLock() %s", m.getName(), GetCallersName(1, true))
	m.RWMutex.RLock()
	TracefCtx(m.ctx, KeyAll, "After %s.RLock() %s", m.getName(), GetCallersName(1, true))
}

// RUnlock undoes a single RLock call;
// it does not affect other simultaneous readers.
// It is a run-time error if rw is not locked for reading
// on entry to RUnlock.
func (m *LoggingRWMutex) RUnlock() {
	TracefCtx(m.ctx, KeyAll, "Before %s.RUnlock() %s", m.getName(), GetCallersName(1, true))
	m.RWMutex.RUnlock()
	TracefCtx(m.ctx, KeyAll, "After %s.RUnlock() %s", m.getName(), GetCallersName(1, true))
}

// TryLock tries to lock rw for writing and reports whether it succeeded.
//
// Note that while correct uses of TryLock do exist, they are rare,
// and use of TryLock is often a sign of a deeper problem
// in a particular use of mutexes.
func (m *LoggingRWMutex) TryLock() bool {
	TracefCtx(m.ctx, KeyAll, "Before %s.TryLock() %s", m.getName(), GetCallersName(1, true))
	result := m.RWMutex.TryLock()
	TracefCtx(m.ctx, KeyAll, "After %s.TryLock() %s", m.getName(), GetCallersName(1, true))
	return result
}

// TryRLock tries to lock rw for reading and reports whether it succeeded.
//
// Note that while correct uses of TryRLock do exist, they are rare,
// and use of TryRLock is often a sign of a deeper problem
// in a particular use of mutexes.
func (m *LoggingRWMutex) TryRLock() bool {
	TracefCtx(m.ctx, KeyAll, "Before %s.TryRLock() %s", m.getName(), GetCallersName(1, true))
	result := m.RWMutex.TryRLock()
	TracefCtx(m.ctx, KeyAll, "After %s.TryRLock() %s", m.getName(), GetCallersName(1, true))
	return result
}

// getName returns the name of the mutex for logging.
func (m *LoggingRWMutex) getName() string {
	if m.name != "" {
		return m.name
	}
	return "LoggingRWMutex"
}
