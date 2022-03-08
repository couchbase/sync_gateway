//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"sync"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
)

// =====================================================================
// Attachment Compaction Implementation of Background Manager Process
// =====================================================================

type AttachmentCompactionManager struct {
	MarkedAttachments base.AtomicInt
	PurgedAttachments base.AtomicInt
	CompactID         string
	Phase             string
	VBUUIDs           []uint64
	dryRun            bool
	lock              sync.Mutex
}

var _ BackgroundManagerProcessI = &AttachmentCompactionManager{}

func NewAttachmentCompactionManager(bucket base.Bucket) *BackgroundManager {
	return &BackgroundManager{
		Process: &AttachmentCompactionManager{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			bucket:        bucket,
			processSuffix: "compact",
		},
		terminator: base.NewSafeTerminator(),
	}
}

func (a *AttachmentCompactionManager) Init(options map[string]interface{}, clusterStatus []byte) error {
	database := options["database"].(*Database)
	database.DbStats.Database().CompactionAttachmentStartTime.Set(time.Now().UTC().Unix())

	newRunInit := func() error {
		uniqueUUID, err := uuid.NewRandom()
		if err != nil {
			return err
		}

		dryRun, _ := options["dryRun"].(bool)
		if dryRun {
			base.InfofCtx(database.Ctx, base.KeyAll, "Attachment Compaction: Running as dry run. No attachments will be purged")
		}

		a.dryRun = dryRun
		a.CompactID = uniqueUUID.String()
		base.InfofCtx(database.Ctx, base.KeyAll, "Attachment Compaction: Starting new compaction run with compact ID: %q", a.CompactID)
		return nil
	}

	if clusterStatus != nil {
		var statusDoc AttachmentManagerStatusDoc
		err := base.JSONUnmarshal(clusterStatus, &statusDoc)

		reset, ok := options["reset"].(bool)
		if reset && ok {
			base.InfofCtx(database.Ctx, base.KeyAll, "Attachment Compaction: Resetting compaction process. Will not  resume any "+
				"partially completed process")
		}

		// If the previous run completed, or there was an error during unmarshalling the status we will start the
		// process from scratch with a new compaction ID. Otherwise, we should resume with the compact ID, phase and
		// stats specified in the doc.
		if statusDoc.State == BackgroundProcessStateCompleted || err != nil || (reset && ok) {
			return newRunInit()
		} else {
			a.CompactID = statusDoc.CompactID
			a.Phase = statusDoc.Phase
			a.dryRun = statusDoc.DryRun
			a.MarkedAttachments.Set(statusDoc.MarkedAttachments)
			a.PurgedAttachments.Set(statusDoc.PurgedAttachments)
			a.VBUUIDs = statusDoc.VBUUIDs

			base.InfofCtx(database.Ctx, base.KeyAll, "Attachment Compaction: Attempting to resume compaction with compact ID: %q phase %q", a.CompactID, a.Phase)
		}

		return nil

	}

	return newRunInit()
}

func (a *AttachmentCompactionManager) Run(options map[string]interface{}, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	database := options["database"].(*Database)

	persistClusterStatus := func() {
		err := persistClusterStatusCallback()
		if err != nil {
			base.WarnfCtx(database.Ctx, "Failed to persist cluster status on-demand following completion of phase: %v", err)
		}
	}

	defer persistClusterStatus()

	// Need to check the current phase in the event we are resuming - No need to run mark again if we got as far as
	// cleanup last time...
	var err error
	switch a.Phase {
	case "mark", "":
		a.SetPhase("mark")
		persistClusterStatus()
		_, a.VBUUIDs, err = attachmentCompactMarkPhase(database, a.CompactID, terminator, &a.MarkedAttachments)
		if err != nil || terminator.IsClosed() {
			return err
		}
		fallthrough
	case "sweep":
		a.SetPhase("sweep")
		persistClusterStatus()
		_, err := attachmentCompactSweepPhase(database, a.CompactID, a.VBUUIDs, a.dryRun, terminator, &a.PurgedAttachments)
		if err != nil || terminator.IsClosed() {
			return err
		}
		fallthrough
	case "cleanup":
		a.SetPhase("cleanup")
		persistClusterStatus()
		err := attachmentCompactCleanupPhase(database, a.CompactID, a.VBUUIDs, terminator)
		if err != nil || terminator.IsClosed() {
			return err
		}
	}

	a.SetPhase("")
	return nil
}

func (a *AttachmentCompactionManager) SetPhase(phase string) {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.Phase = phase
}

type AttachmentManagerResponse struct {
	BackgroundManagerStatus
	MarkedAttachments int64  `json:"marked_attachments"`
	PurgedAttachments int64  `json:"purged_attachments"`
	CompactID         string `json:"compact_id"`
	Phase             string `json:"phase,omitempty"`
	DryRun            bool   `json:"dry_run,omitempty"`
}

type AttachmentManagerMeta struct {
	VBUUIDs []uint64 `json:"vbuuids"`
}

type AttachmentManagerStatusDoc struct {
	AttachmentManagerResponse `json:"status"`
	AttachmentManagerMeta     `json:"meta"`
}

func (a *AttachmentCompactionManager) GetProcessStatus(status BackgroundManagerStatus) ([]byte, []byte, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	response := AttachmentManagerResponse{
		BackgroundManagerStatus: status,
		MarkedAttachments:       a.MarkedAttachments.Value(),
		PurgedAttachments:       a.PurgedAttachments.Value(),
		CompactID:               a.CompactID,
		Phase:                   a.Phase,
		DryRun:                  a.dryRun,
	}

	meta := AttachmentManagerMeta{
		VBUUIDs: a.VBUUIDs,
	}

	statusJSON, err := base.JSONMarshal(response)
	if err != nil {
		return nil, nil, err
	}

	metaJSON, err := base.JSONMarshal(meta)
	if err != nil {
		return nil, nil, err
	}

	return statusJSON, metaJSON, err
}

func (a *AttachmentCompactionManager) ResetStatus() {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.MarkedAttachments.Set(0)
	a.PurgedAttachments.Set(0)
	a.dryRun = false
}
