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
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/require"
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

// removeInvalidEntries drops every entry whose sequence is greater than seq.
func (ts *TimestampsSeqArray) removeInvalidEntries(ctx context.Context, seq uint64) {
	ts.Timestamps = slices.DeleteFunc(ts.Timestamps, func(pair TimestampsSeqPair) bool {
		invalid := pair.Seq > seq
		if invalid {
			base.WarnfCtx(ctx, "Removing timestamp entry with a sequence greater than %d: %+v", seq, pair)
		}
		return invalid
	})
}

// trim drops the entries older than purgeTimestamp (epoch seconds). It keeps the newest entry at or before
// purgeTimestamp, so the oldest retained entry still covers the start of the purge interval.
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

// PurgeIntervalFunc returns the interval after which timestamps entries are no longer needed.
type PurgeIntervalFunc func() time.Duration

// GetSequenceFunc returns the current database sequence.
type GetSequenceFunc func(ctx context.Context) (uint64, error)

// TimestampsManager maintains the timestamps document in the metadata store, and keeps an in-memory copy of it.
type TimestampsManager struct {
	ctx                context.Context
	timestampsSeqArray atomic.Pointer[TimestampsSeqArray]
	metadataStore      base.DataStore
	metaKeys           *base.MetadataKeys
	purgeIntervalFunc  PurgeIntervalFunc
	getSequenceFunc    GetSequenceFunc
}

// TimestampsMgrOpts holds the dependencies of a TimestampsManager.
type TimestampsMgrOpts struct {
	ctx               context.Context
	metadataStore     base.DataStore
	metaKeys          *base.MetadataKeys
	purgeIntervalFunc PurgeIntervalFunc
	getSequenceFunc   GetSequenceFunc
}

// NewTimestampsManager returns a TimestampsManager with an empty in-memory array. It does not touch the metadata store until Start is called.
func NewTimestampsManager(opts TimestampsMgrOpts) *TimestampsManager {
	tm := &TimestampsManager{
		ctx:               opts.ctx,
		metadataStore:     opts.metadataStore,
		metaKeys:          opts.metaKeys,
		purgeIntervalFunc: opts.purgeIntervalFunc,
		getSequenceFunc:   opts.getSequenceFunc,
	}
	tm.timestampsSeqArray.Store(&TimestampsSeqArray{})
	return tm
}

// updateFrequency returns the minimum time between two entries: the purge interval split across
// NumTimestampsEntries, but never less than 10 minutes.
func (tm *TimestampsManager) updateFrequency() time.Duration {
	purgeInterval := tm.purgeIntervalFunc()
	return max(purgeInterval/NumTimestampsEntries, time.Minute*10)
}

// getNewestTimestamp returns the newest entry of the in-memory array in epoch seconds, or 0 if there is none.
func (tm *TimestampsManager) getNewestTimestamp() int64 {
	tsArray := tm.timestampsSeqArray.Load()
	if tsArray == nil {
		return 0
	}
	return tsArray.newestTimestamp()
}

// isTimestampsArrayEmpty returns true if the in-memory array has no entries.
func (tm *TimestampsManager) isTimestampsArrayEmpty() bool {
	tsArray := tm.timestampsSeqArray.Load()
	if tsArray == nil {
		return true
	}
	return tsArray.isEmpty()
}

// entryDue returns true if the in-memory array is empty, or if its newest entry is older than the update frequency.
func (tm *TimestampsManager) entryDue() bool {
	now := time.Now().Unix()
	newestEntry := tm.getNewestTimestamp()
	updateFrequency := tm.updateFrequency()
	entryDue := tm.isTimestampsArrayEmpty() || now > newestEntry+int64(updateFrequency/time.Second)
	return entryDue
}

// updateTimestampsArray replaces the in-memory array. The pointer is swapped atomically, so readers never see a partial update.
func (tm *TimestampsManager) updateTimestampsArray(timestampArray TimestampsSeqArray) {
	tm.timestampsSeqArray.Store(&timestampArray)
}

// Start creates the timestamps document with an empty array, unless another node already created it.
func (tm *TimestampsManager) Start() error {
	key := tm.metaKeys.TimestampsSeqKey()
	_, err := tm.metadataStore.Update(tm.ctx, key, 0, func(currentBytes []byte) ([]byte, *uint32, bool, error) {
		if currentBytes != nil {
			// another node has already created the document
			return nil, nil, false, base.ErrUpdateCancel
		}
		newVal := tm.timestampsSeqArray.Load()
		newBytes, marshalErr := base.JSONMarshal(newVal)
		return newBytes, nil, false, marshalErr
	})
	// TODO: Add BackgroundTask here
	return err
}

// addEntry adds an entry for the current sequence to the timestamps document, unless the newest entry is
// within the update frequency. It then drops entries that are ahead of the sequence or past the purge interval.
func (tm *TimestampsManager) addEntry() error {
	key := tm.metaKeys.TimestampsSeqKey()
	updateFrequency := tm.updateFrequency()
	// the bytes of the last successful write, used to refresh the in-memory array
	var newValBytes []byte
	now := time.Now().Unix()
	_, err := tm.metadataStore.Update(tm.ctx, key, 0, func(current []byte) ([]byte, *uint32, bool, error) {
		var timestampsSeqArray TimestampsSeqArray
		if current == nil {
			base.WarnfCtx(tm.ctx, "TimestampsSeqArray is nil. Returning nil")
			return nil, nil, false, base.ErrUpdateCancel
		}
		if err := base.JSONUnmarshal(current, &timestampsSeqArray); err != nil {
			return nil, nil, false, err
		}

		withinUpdateFrequency := !timestampsSeqArray.isEmpty() &&
			(now < timestampsSeqArray.newestTimestamp()+int64(updateFrequency/time.Second))
		if withinUpdateFrequency {
			// not due, or another node added an entry first
			return nil, nil, false, base.ErrUpdateCancel
		}
		lastSeq, err := tm.getSequenceFunc(tm.ctx)
		if err != nil {
			return nil, nil, false, err
		}

		// an entry can not be ahead of the current sequence
		timestampsSeqArray.removeInvalidEntries(tm.ctx, lastSeq)
		timestampsSeqArray.append(lastSeq, now)
		// trim after the append, so that the new entry counts when the array is trimmed
		purgeTimestamp := now - int64(tm.purgeIntervalFunc()/time.Second)
		timestampsSeqArray.trim(purgeTimestamp)

		newValBytes, err = base.JSONMarshal(timestampsSeqArray)
		if err != nil {
			return nil, nil, false, err
		}

		return newValBytes, nil, false, nil
	})
	if err != nil {
		return err
	}

	var updatedTimestampsSeqArray TimestampsSeqArray
	err = base.JSONUnmarshal(newValBytes, &updatedTimestampsSeqArray)
	if err != nil {
		return err
	}
	tm.updateTimestampsArray(updatedTimestampsSeqArray)
	return nil
}

// CreateTSDocForTest writes tsArray to the timestamps document, replacing any existing content.
func (tm *TimestampsManager) CreateTSDocForTest(t *testing.T, tsArray TimestampsSeqArray) {
	t.Helper()
	_, err := tm.metadataStore.Update(tm.ctx, tm.metaKeys.TimestampsSeqKey(), 0, func(current []byte) ([]byte, *uint32, bool, error) {
		valBytes, err := base.JSONMarshal(tsArray)
		return valBytes, nil, false, err
	})
	require.NoError(t, err)
}
