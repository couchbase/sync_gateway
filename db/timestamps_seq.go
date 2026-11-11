//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// NumTimestampsEntries is the number of entries that the timestamps document holds across one purge interval.
const NumTimestampsEntries = 60

// TimestampsSeqPair records the database sequence at a point in time.
type TimestampsSeqPair struct {
	Seq       uint64 `json:"s"`
	Timestamp int64  `json:"t"` // epoch seconds
}

// TimestampsSeqArray is the content of the timestamps document, ordered from the oldest to the newest entry.
type TimestampsSeqArray struct {
	Timestamps []TimestampsSeqPair `json:"timestamps"`
}

// isEmpty returns true if the array has no entries.
func (ts *TimestampsSeqArray) isEmpty() bool {
	return len(ts.Timestamps) == 0
}

// oldestTimestamp returns the first entry in epoch seconds, or 0 if the array is empty.
func (ts *TimestampsSeqArray) oldestTimestamp() int64 {
	if ts.isEmpty() {
		return 0
	}
	return ts.Timestamps[0].Timestamp
}

// newestTimestamp returns the last entry in epoch seconds, or 0 if the array is empty.
func (ts *TimestampsSeqArray) newestTimestamp() int64 {
	if ts.isEmpty() {
		return 0
	}
	return ts.Timestamps[len(ts.Timestamps)-1].Timestamp
}

// append adds a pair for seq, taken at now (epoch seconds), as the newest entry.
func (ts *TimestampsSeqArray) append(seq uint64, now int64) {
	ts.Timestamps = append(ts.Timestamps, TimestampsSeqPair{Seq: seq, Timestamp: now})
}

// removeInvalidEntries drops every entry whose sequence is greater than seq, and returns the entries it dropped.
func (ts *TimestampsSeqArray) removeInvalidEntries(seq uint64) []TimestampsSeqPair {
	var removed []TimestampsSeqPair
	ts.Timestamps = slices.DeleteFunc(ts.Timestamps, func(pair TimestampsSeqPair) bool {
		invalid := pair.Seq > seq
		if invalid {
			removed = append(removed, pair)
		}
		return invalid
	})
	return removed
}

// trim drops the entries older than purgeTimestamp (epoch seconds), but keeps the newest entry at or before it
// so that the oldest retained entry still covers the start of the purge interval. If no entry is newer than
// purgeTimestamp, trim leaves the array unchanged.
func (ts *TimestampsSeqArray) trim(purgeTimestamp int64) {
	// idx is the first entry newer than purgeTimestamp
	idx := slices.IndexFunc(ts.Timestamps, func(pair TimestampsSeqPair) bool {
		return pair.Timestamp > purgeTimestamp
	})
	// idx == -1: every entry is at or before purgeTimestamp. idx == 0: every entry is newer. Either way, nothing is trimmed.
	if idx == -1 || idx == 0 {
		return
	}
	ts.Timestamps = ts.Timestamps[idx-1:]
}

func (ts *TimestampsSeqArray) isDue(now int64, updateFrequency time.Duration) bool {
	// an empty array has a newest timestamp of 0, so it is always due
	return now >= ts.newestTimestamp()+int64(updateFrequency/time.Second)
}

// PurgeIntervalFunc returns the interval after which timestamps entries are no longer needed.
type PurgeIntervalFunc func() time.Duration

// GetSequenceFunc returns the cluster-wide current sequence (the _sync:seq counter), not a node-local value such as
// the last sequence this node allocated. A node-local value would make removeInvalidEntries delete the valid entries
// that other nodes added.
type GetSequenceFunc func(ctx context.Context) (uint64, error)

// TimestampsManager maintains the timestamps document in the metadata store, and keeps an in-memory copy of it.
type TimestampsManager struct {
	timestampsSeqArray          atomic.Pointer[TimestampsSeqArray]
	metadataStore               base.DataStore
	metaKeys                    *base.MetadataKeys
	purgeIntervalFunc           PurgeIntervalFunc
	getSequenceFunc             GetSequenceFunc
	nowFunc                     func() time.Time
	backgroundTasks             []BackgroundTask
	terminator                  chan bool
	stopped                     base.AtomicBool
	dbName                      string
	testUpdateFrequencyOverride *time.Duration
	testCheckIntervalOverride   *time.Duration
}

// TimestampsMgrOpts holds the dependencies of a TimestampsManager.
type TimestampsMgrOpts struct {
	metadataStore               base.DataStore
	metaKeys                    *base.MetadataKeys
	purgeIntervalFunc           PurgeIntervalFunc
	getSequenceFunc             GetSequenceFunc
	nowFunc                     func() time.Time // the clock, so that tests can control time. Defaults to time.Now.
	dbName                      string
	testUpdateFrequencyOverride *time.Duration
	testCheckIntervalOverride   *time.Duration
}

// NewTimestampsManager returns a TimestampsManager with an empty in-memory array. It does not touch the metadata store until Start is called.
func NewTimestampsManager(opts TimestampsMgrOpts) *TimestampsManager {
	tm := &TimestampsManager{
		metadataStore:               base.NewIdleKVOpDataStore(opts.metadataStore),
		metaKeys:                    opts.metaKeys,
		purgeIntervalFunc:           opts.purgeIntervalFunc,
		getSequenceFunc:             opts.getSequenceFunc,
		nowFunc:                     opts.nowFunc,
		dbName:                      opts.dbName,
		terminator:                  make(chan bool),
		testUpdateFrequencyOverride: opts.testUpdateFrequencyOverride,
		testCheckIntervalOverride:   opts.testCheckIntervalOverride,
	}
	if tm.nowFunc == nil {
		tm.nowFunc = time.Now
	}
	tm.timestampsSeqArray.Store(&TimestampsSeqArray{Timestamps: []TimestampsSeqPair{}})
	return tm
}

// purgeInterval returns the interval after which entries are no longer needed. A non-positive client partition window
// would keep every entry, so DefaultClientPartitionWindow is used instead.
func (tm *TimestampsManager) purgeInterval() time.Duration {
	if purgeInterval := tm.purgeIntervalFunc(); purgeInterval > 0 {
		return purgeInterval
	}
	return base.DefaultClientPartitionWindow
}

// updateFrequency returns the minimum time between two entries: the purge interval split across
// NumTimestampsEntries, but never less than 10 minutes.
func (tm *TimestampsManager) updateFrequency() time.Duration {
	if tm.testUpdateFrequencyOverride != nil {
		return *tm.testUpdateFrequencyOverride
	}
	return max(tm.purgeInterval()/NumTimestampsEntries, time.Minute*10)
}

// timestampsCheckInterval is the time between two checks of the in-memory array for a due entry.
const timestampsCheckInterval = 10 * time.Minute

// checkInterval returns the time between two runs of the background task.
func (tm *TimestampsManager) checkInterval() time.Duration {
	if tm.testCheckIntervalOverride != nil {
		return *tm.testCheckIntervalOverride
	}
	return timestampsCheckInterval
}

// entryDue returns true if the in-memory array is empty, or if its newest entry is at least the update frequency old.
func (tm *TimestampsManager) entryDue() bool {
	now := tm.nowFunc().Unix()
	tsArray := tm.timestampsSeqArray.Load()
	return tsArray.isDue(now, tm.updateFrequency())
}

// checkEntry adds an entry if the in-memory array shows that one is due, so that most checks make no KV call. An error
// is logged rather than returned, because NewBackgroundTask stops on an error; the array still shows the entry as due,
// so the next check retries.
func (tm *TimestampsManager) checkEntry(ctx context.Context) error {
	if !tm.entryDue() {
		return nil
	}
	if err := tm.addEntry(ctx); err != nil {
		base.WarnfCtx(ctx, "Unable to add timestamps entry, retrying in %v: %v", tm.checkInterval(), err)
	}
	return nil
}

// updateTimestampsArray replaces the in-memory array. The pointer is swapped atomically, so readers never see a partial update.
func (tm *TimestampsManager) updateTimestampsArray(timestampArray TimestampsSeqArray) {
	tm.timestampsSeqArray.Store(&timestampArray)
}

// Start calls addEntry: it loads the document into memory, or writes the in-memory array (empty on a new node)
// if the document is missing. It then starts the background task that runs every check interval.
func (tm *TimestampsManager) Start(ctx context.Context) error {
	err := tm.addEntry(ctx)
	if err != nil {
		base.WarnfCtx(ctx, "error while adding entry: %v", err)
	}

	bgt, err := NewBackgroundTask(ctx, "TimestampsSequenceManager", tm.checkEntry, tm.checkInterval(), tm.terminator)
	if err != nil {
		base.WarnfCtx(ctx, "error starting TimestampsSequenceManager: %v", err)
		return err
	}
	tm.backgroundTasks = append(tm.backgroundTasks, bgt)
	return nil
}

func (tm *TimestampsManager) Stop(ctx context.Context) {

	if !tm.stopped.CompareAndSwap(false, true) {
		base.WarnfCtx(ctx, "TimestampsManager already stopped")
		return
	}

	close(tm.terminator)

	waitForBGTCompletion(ctx, BGTCompletionMaxWait, tm.backgroundTasks, tm.dbName)
}

// addEntry writes the in-memory array if the timestamps document is missing, without removing invalid entries. Otherwise
// it removes entries ahead of the current sequence or, if none were removed and an entry is due, appends one and trims.
func (tm *TimestampsManager) addEntry(ctx context.Context) error {
	key := tm.metaKeys.TimestampsSeqKey()
	// the bytes of the last successful write, used to refresh the in-memory array
	var newValBytes []byte
	var marshalErr error
	// the entries dropped by the latest attempt of the Update callback, logged once after a successful write
	var removedEntries []TimestampsSeqPair
	now := tm.nowFunc().Unix()
	_, err := tm.metadataStore.Update(ctx, key, 0, func(current []byte) ([]byte, *uint32, bool, error) {
		removedEntries = nil
		var timestampsSeqArray TimestampsSeqArray
		if current == nil {
			newVal := tm.timestampsSeqArray.Load()
			newValBytes, marshalErr = base.JSONMarshal(newVal)
			return newValBytes, nil, false, marshalErr
		}
		if unmarshalErr := base.JSONUnmarshal(current, &timestampsSeqArray); unmarshalErr != nil {
			base.WarnfCtx(ctx, "Unable to parse the timestamps document, resetting it to an empty array: %v", unmarshalErr)
			// reset a document that can't be parsed rather than fail every call: an empty array only delays purges until
			// entries cover the purge interval again, and newValBytes replaces the in-memory copy with it
			newValBytes, marshalErr = base.JSONMarshal(TimestampsSeqArray{})
			return newValBytes, nil, false, marshalErr
		}

		lastSeq, err := tm.getSequenceFunc(ctx)
		base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Add(1)
		if err != nil {
			return nil, nil, false, err
		}

		// Runs before the update-frequency check so entries left ahead of a rolled-back sequence are removed
		// on the next call, not after a full update interval.
		removedEntries = timestampsSeqArray.removeInvalidEntries(lastSeq)
		removed := len(removedEntries) > 0

		// If entries were removed, persist the cleaned array without appending. The next call adds an entry.
		if !removed {
			// Timestamps come from each node's local clock. Skip the update if this node's clock is behind the newest
			// entry, since appending would break the sorted order that trim relies on.
			if now < timestampsSeqArray.newestTimestamp() {
				newValBytes = current
				return nil, nil, false, base.ErrUpdateCancel
			}

			if !timestampsSeqArray.isDue(now, tm.updateFrequency()) {
				newValBytes = current
				return nil, nil, false, base.ErrUpdateCancel
			}
			timestampsSeqArray.append(lastSeq, now)
			// trim after the append, so that the new entry counts when the array is trimmed
			purgeTimestamp := now - int64(tm.purgeInterval()/time.Second)
			timestampsSeqArray.trim(purgeTimestamp)
		}

		newValBytes, err = base.JSONMarshal(timestampsSeqArray)
		if err != nil {
			return nil, nil, false, err
		}

		return newValBytes, nil, false, nil
	})
	if err != nil && !errors.Is(err, base.ErrUpdateCancel) {
		return err
	}
	if err == nil && len(removedEntries) > 0 {
		base.WarnfCtx(ctx, "Removed %d timestamp entries with a sequence greater than the current sequence: %+v", len(removedEntries), removedEntries)
	}

	var updatedTimestampsSeqArray TimestampsSeqArray
	err = base.JSONUnmarshal(newValBytes, &updatedTimestampsSeqArray)
	if err != nil {
		return err
	}
	tm.updateTimestampsArray(updatedTimestampsSeqArray)
	return nil
}
