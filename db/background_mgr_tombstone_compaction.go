//  Copyright 2012-Present Couchbase, Inc.
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
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// =====================================================================
// Tombstone Compaction Implementation of Background Manager Process
// =====================================================================

// TombstoneCompactionManager implements the tombstone compaction background process.
type TombstoneCompactionManager struct {
	PurgedDocCount int64
}

// TombstoneCompactionOptions defines options for running the tombstone compaction process.
type TombstoneCompactionOptions struct {
	// Database is the reference to the Database context compaction is running on.
	Database *Database
}

// validate returns an error if the options are not usable by Init/Run.
func (o TombstoneCompactionOptions) validate() error {
	if o.Database == nil {
		return errors.New("tombstone compaction requires a Database")
	}
	return nil
}

var _ BackgroundManagerProcessI[TombstoneCompactionOptions] = &TombstoneCompactionManager{}

func NewTombstoneCompactionManager() *BackgroundManager[TombstoneCompactionOptions] {
	return &BackgroundManager[TombstoneCompactionOptions]{
		name:       "tombstone_compaction",
		Process:    &TombstoneCompactionManager{},
		terminator: base.NewSafeTerminator(),
	}
}

func (t *TombstoneCompactionManager) Init(ctx context.Context, options TombstoneCompactionOptions, clusterStatus []byte) (backgroundManagerInitMode, error) {
	if err := options.validate(); err != nil {
		return backgroundManagerInitReset, err
	}

	options.Database.DbStats.Database().CompactionTombstoneStartTime.Set(uint64(time.Now().UTC().Unix()))

	return backgroundManagerInitReset, nil
}

func (t *TombstoneCompactionManager) Run(ctx context.Context, options TombstoneCompactionOptions, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	if err := options.validate(); err != nil {
		return err
	}

	defer atomic.CompareAndSwapUint32(&options.Database.CompactState, DBCompactRunning, DBCompactNotRunning)
	updateStatusCallback := func(docsPurged *int) {
		atomic.StoreInt64(&t.PurgedDocCount, int64(*docsPurged))
	}

	_, err := options.Database.Compact(ctx, true, updateStatusCallback, terminator, false)
	if err != nil {
		return err
	}

	return nil
}

type TombstoneManagerResponse struct {
	BackgroundManagerStatus
	DocsPurged int64 `json:"docs_purged"`
}

func (t *TombstoneCompactionManager) SetProcessStatus(context.Context, []byte, []byte) {
	return
}

func (t *TombstoneCompactionManager) GetProcessStatus(backgroundManagerStatus BackgroundManagerStatus, _ []byte) ([]byte, []byte, error) {
	retStatus := TombstoneManagerResponse{
		BackgroundManagerStatus: backgroundManagerStatus,
		DocsPurged:              atomic.LoadInt64(&t.PurgedDocCount),
	}

	statusJSON, err := base.JSONMarshal(retStatus)
	return statusJSON, nil, err
}

func (t *TombstoneCompactionManager) ResetStatus() {
	atomic.StoreInt64(&t.PurgedDocCount, 0)
}
