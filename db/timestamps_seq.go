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
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

const NumTimestampsEntries = 60

type TimestampsSeqPair struct {
	Seq       uint64 `json:"s"`
	Timestamp int64  `json:"t"` // epoch seconds
}

type TimestampsSeqArray struct {
	Timestamps []TimestampsSeqPair `json:"timestamps"`
}

func (ts TimestampsSeqArray) IsEmpty() bool {
	return len(ts.Timestamps) == 0
}

// lastTimestamp returns the newest entry in epoch seconds, or 0 if the array is empty.
func (ts TimestampsSeqArray) lastTimestamp() int64 {
	if ts.IsEmpty() {
		return 0
	}
	return ts.Timestamps[len(ts.Timestamps)-1].Timestamp
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

func (tm *TimestampsManager) entryDue() bool {
	//TODO: Implement later
	return false
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
	_, err := tm.metadataStore.Update(tm.ctx, key, 0, func(current []byte) ([]byte, *uint32, bool, error) {
		var timestampsSeqArray TimestampsSeqArray
		if current == nil {
			base.WarnfCtx(tm.ctx, "TimestampsSeqArray is nil. Returning nil")
			return nil, nil, false, base.ErrUpdateCancel
		}
		if err := base.JSONUnmarshal(current, &timestampsSeqArray); err != nil {
			return nil, nil, false, err
		}

		withinUpdateFrequency := !timestampsSeqArray.IsEmpty()
		return nil, nil, false, nil
	})
	return err
}
