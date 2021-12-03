//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// =====================================================================
// Tombstone Compaction Implementation of Background Manager Process
// =====================================================================

type TombstoneCompactionManager struct {
	PurgedDocCount int64
}

var _ BackgroundManagerProcessI = &TombstoneCompactionManager{}

func NewTombstoneCompactionManager() *BackgroundManager {
	return &BackgroundManager{
		Process:    &TombstoneCompactionManager{},
		terminator: base.NewSafeTerminator(),
	}
}

func (t *TombstoneCompactionManager) Init(options map[string]interface{}, clusterStatus []byte) error {
	database := options["database"].(*Database)
	database.DbStats.Database().CompactionAttachmentStartTime.Set(time.Now().UTC().Unix())

	return nil
}

func (t *TombstoneCompactionManager) Run(options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	database := options["database"].(*Database)

	defer atomic.CompareAndSwapUint32(&database.CompactState, DBCompactRunning, DBCompactNotRunning)
	callback := func(docsPurged *int) {
		atomic.StoreInt64(&t.PurgedDocCount, int64(*docsPurged))
	}

	_, err := database.Compact(true, callback, terminator)
	if err != nil {
		return err
	}

	return nil
}

type TombstoneManagerResponse struct {
	BackgroundManagerStatus
	DocsPurged int64 `json:"docs_purged"`
}

func (t *TombstoneCompactionManager) GetProcessStatus(backgroundManagerStatus BackgroundManagerStatus) ([]byte, []byte, error) {
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
