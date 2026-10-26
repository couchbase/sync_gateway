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

const NumTimestampsEntries = 60

type TimestampsSeqPair struct {
	Seq       uint64 `json:"s"`
	Timestamp int64  `json:"t"` // epoch seconds
}

type TimestampsSeqArray struct {
	Timestamps []TimestampsSeqPair `json:"timestamps"`
}

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

func (ts *TimestampsSeqArray) trim(purgeTimestamp int64) {
	idx := slices.IndexFunc(ts.Timestamps, func(pair TimestampsSeqPair) bool {
		return pair.Timestamp > purgeTimestamp
	})
	if idx == -1 || idx == 0 {
		return
	}
	//ts.Timestamps = append([]TimestampsSeqPair{ts.Timestamps[0]}, ts.Timestamps[idx:]...)
	ts.Timestamps = ts.Timestamps[idx-1:]
}

type PurgeIntervalFunc func() time.Duration
type GetSequenceFunc func(ctx context.Context) (uint64, error)
type TimestampsManager struct {
	ctx                context.Context
	timestampsSeqArray atomic.Pointer[TimestampsSeqArray]
	metadataStore      base.DataStore
	metaKeys           *base.MetadataKeys
	purgeIntervalFunc  PurgeIntervalFunc
	getSequenceFunc    GetSequenceFunc
}

type TimestampsMgrOpts struct {
	ctx               context.Context
	metadataStore     base.DataStore
	metaKeys          *base.MetadataKeys
	purgeIntervalFunc PurgeIntervalFunc
	getSequenceFunc   GetSequenceFunc
}

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

func (tm *TimestampsManager) updateFrequency() time.Duration {
	purgeInterval := tm.purgeIntervalFunc()
	return max(purgeInterval/NumTimestampsEntries, time.Minute*10)
}

func (tm *TimestampsManager) getNewestTimestamp() int64 {
	tsArray := tm.timestampsSeqArray.Load()
	if tsArray == nil {
		return 0
	}
	return tsArray.newestTimestamp()
}

func (tm *TimestampsManager) isTimestampsArrayEmpty() bool {
	tsArray := tm.timestampsSeqArray.Load()
	if tsArray == nil {
		return true
	}
	return tsArray.isEmpty()
}

func (tm *TimestampsManager) entryDue() bool {
	now := time.Now().Unix()
	newestEntry := tm.getNewestTimestamp()
	updateFrequency := tm.updateFrequency()
	entryDue := tm.isTimestampsArrayEmpty() || now > newestEntry+int64(updateFrequency/time.Second)
	return entryDue
}

func (tm *TimestampsManager) updateTimestampsArray(timestampArray TimestampsSeqArray) {
	tm.timestampsSeqArray.Store(&timestampArray)
}

func (tm *TimestampsManager) Start() error {
	// first create a empty document
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

func (tm *TimestampsManager) addEntry() error {
	key := tm.metaKeys.TimestampsSeqKey()
	updateFrequency := tm.updateFrequency()
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
			return nil, nil, false, base.ErrUpdateCancel
		}
		lastSeq, err := tm.getSequenceFunc(tm.ctx)
		if err != nil {
			return nil, nil, false, err
		}

		timestampsSeqArray.removeInvalidEntries(tm.ctx, lastSeq)
		timestampsSeqArray.append(lastSeq, now)
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

func (tm *TimestampsManager) CreateTSDocForTest(t *testing.T, tsArray TimestampsSeqArray) {
	t.Helper()
	_, err := tm.metadataStore.Update(tm.ctx, tm.metaKeys.TimestampsSeqKey(), 0, func(current []byte) ([]byte, *uint32, bool, error) {
		valBytes, err := base.JSONMarshal(tsArray)
		return valBytes, nil, false, err
	})
	require.NoError(t, err)
}
