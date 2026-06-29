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
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
)

// =====================================================================
// Attachment Compaction Implementation of Background Manager Process
// =====================================================================

// runFunctionStartedCallbackFunc is a test seam called at the top of Run before any compaction phases execute.
type runFunctionStartedCallbackFunc func(context.Context, map[string]any, updateStatusCallbackFunc, *base.SafeTerminator)

// AttachmentCompactionManager implements the attachment compaction background process. Compaction
// runs in three sequential phases — mark, sweep, cleanup.
//
// Fields _compactID, _phase, _vbuuids, _dryRun are protected by lock.
type AttachmentCompactionManager struct {
	// MarkedAttachments counts attachments marked as live during the mark phase.
	MarkedAttachments base.AtomicInt
	// PurgedAttachments counts attachments deleted during the sweep phase.
	PurgedAttachments base.AtomicInt

	// The following fields are protected by lock and must be accessed via their getters/setters.
	_compactID string   // unique identifier for the current compaction run, used to tag marked attachments
	_phase     string   // current phase: "mark", "sweep", "cleanup", or "" when idle
	_vbuuids   []uint64 // vBucket UUIDs captured after the mark phase, used to detect DCP rollbacks in cleanup
	_dryRun    bool     // when true, sweep phase reports but does not delete attachments

	lock                       sync.RWMutex
	runFunctionStartedCallback atomic.Pointer[runFunctionStartedCallbackFunc]
}

var _ BackgroundManagerProcessI[map[string]any] = &AttachmentCompactionManager{}

func NewAttachmentCompactionManager(metadataStore base.DataStore, metaKeys *base.MetadataKeys) *BackgroundManager[map[string]any] {
	return &BackgroundManager[map[string]any]{
		name:    "attachment_compaction",
		Process: &AttachmentCompactionManager{},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: metadataStore,
			metaKeys:      metaKeys,
			processSuffix: "compact",
		},
		terminator: base.NewSafeTerminator(),
	}
}

func (a *AttachmentCompactionManager) Init(ctx context.Context, options map[string]any, clusterStatus []byte) (backgroundManagerInitMode, error) {
	database := options["database"].(*Database)
	database.DbStats.Database().CompactionAttachmentStartTime.Set(uint64(time.Now().UTC().Unix()))

	newRunInit := func() error {
		uniqueUUID, err := uuid.NewRandom()
		if err != nil {
			return err
		}

		dryRun, _ := options["dryRun"].(bool)
		if dryRun {
			base.InfofCtx(ctx, base.KeyAll, "Attachment Compaction: Running as dry run. No attachments will be purged")
		}

		a._dryRun = dryRun
		a._compactID = uniqueUUID.String()
		base.InfofCtx(ctx, base.KeyAll, "Attachment Compaction: Starting new compaction run with compact ID: %q", a._compactID)
		return nil
	}

	if clusterStatus != nil {
		var statusDoc AttachmentManagerStatusDoc
		err := base.JSONUnmarshal(clusterStatus, &statusDoc)

		reset, ok := options["reset"].(bool)
		if reset && ok {
			base.InfofCtx(ctx, base.KeyAll, "Attachment Compaction: Resetting compaction process. Will not  resume any "+
				"partially completed process")
		}

		// If the previous run completed, or there was an error during unmarshalling the status we will start the
		// process from scratch with a new compaction ID. Otherwise, we should resume with the compact ID, phase and
		// stats specified in the doc.
		if statusDoc.State == BackgroundProcessStateCompleted || err != nil || (reset && ok) {
			return backgroundManagerInitReset, newRunInit()
		} else {
			a.initializeFromPreviousStatus(statusDoc)
			base.InfofCtx(ctx, base.KeyAll, "Attachment Compaction: Attempting to resume compaction with compact ID: %q phase %q", a._compactID, a._phase)
		}

		return backgroundManagerInitResume, nil

	}

	return backgroundManagerInitReset, newRunInit()
}

func (a *AttachmentCompactionManager) Run(ctx context.Context, options map[string]any, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	if cb := a.runFunctionStartedCallback.Load(); cb != nil {
		(*cb)(ctx, options, persistClusterStatusCallback, terminator)
		a.runFunctionStartedCallback.Store(nil)
	}
	database := options["database"].(*Database)

	// Attachment compaction only needs to operate the default scope/collection,
	// because all collection-based attachments will be written by 3.1+ and will be stored in the new format that doesn't need compaction.
	//
	// This may not be true if a user migrated/XDCR'd an existing _default collection into a named collection,
	// but we'll consider that a follow-up enhancement to point this compaction operation at arbitrary collections.
	dataStore := database.Bucket.DefaultDataStore(ctx)
	collectionID := base.DefaultCollectionID

	persistClusterStatus := func() {
		err := persistClusterStatusCallback(ctx)
		if err != nil {
			base.WarnfCtx(ctx, "Failed to persist cluster status on-demand following completion of phase: %v", err)
		}
	}

	defer persistClusterStatus()

	var rollbackErr gocbcore.DCPRollbackError

	// Need to check the current phase in the event we are resuming - No need to run mark again if we got as far as
	// cleanup last time...
	var err error
	switch a.getPhase() {
	case "mark", "":
		a.SetPhase("mark")
		worker := func() (shouldRetry bool, err error, value any) {
			persistClusterStatus()
			_, dcpClient, err := attachmentCompactMarkPhase(ctx, dataStore, collectionID, database, a.getCompactID(), terminator, &a.MarkedAttachments)
			if dcpClient != nil {
				a.setVBUUIDs(base.GetVBUUIDs(dcpClient.GetMetadata()))
			}
			if err != nil {
				// if dcpClient is nil, then dcpClient.GetMetadataKeyPrefix() will panic. This isn't a rollback
				// error, this is a non retryable error.
				if dcpClient == nil {
					return false, err, nil
				}

				shouldRetry, err = a.handleAttachmentCompactionRollbackError(ctx, options, dataStore, database, err, MarkPhase, dcpClient.GetMetadataKeyPrefix())
			}
			return shouldRetry, err, nil
		}
		// retry loop for handling a rollback during mark phase of compaction process
		err, _ = base.RetryLoop(ctx, "attachmentCompactMarkPhase", worker, base.CreateMaxDoublingSleeperFunc(25, 100, 10000))
		if err != nil || terminator.IsClosed() {
			if errors.As(err, &rollbackErr) || errors.Is(err, base.ErrVbUUIDMismatch) {
				// log warning to show we hit max number of retries
				base.WarnfCtx(ctx, "maximum retry attempts reached on mark phase: %v", err)
			}
			return err
		}
		fallthrough
	case "sweep":
		a.SetPhase("sweep")
		persistClusterStatus()
		_, _, err := attachmentCompactSweepPhase(ctx, dataStore, collectionID, database, a.getCompactID(), a.getVBUUIDs(), a.getDryRun(), terminator, &a.PurgedAttachments)
		if err != nil || terminator.IsClosed() {
			return err
		}
		fallthrough
	case "cleanup":
		a.SetPhase("cleanup")
		worker := func() (shouldRetry bool, err error, value any) {
			persistClusterStatus()
			metadataKeyPrefix, err := attachmentCompactCleanupPhase(ctx, dataStore, collectionID, database, a.getCompactID(), a.getVBUUIDs(), terminator)
			if err != nil {
				shouldRetry, err = a.handleAttachmentCompactionRollbackError(ctx, options, dataStore, database, err, CleanupPhase, metadataKeyPrefix)
			}
			return shouldRetry, err, nil
		}
		// retry loop for handling a rollback during mark phase of compaction process
		err, _ = base.RetryLoop(ctx, "attachmentCompactCleanupPhase", worker, base.CreateMaxDoublingSleeperFunc(25, 100, 10000))
		if err != nil || terminator.IsClosed() {
			if errors.As(err, &rollbackErr) || errors.Is(err, base.ErrVbUUIDMismatch) {
				// log warning to show we hit max number of retries
				base.WarnfCtx(ctx, "maximum retry attempts reached on cleanup phase: %v", err)
			}
			return err
		}
	}

	a.SetPhase("")
	return nil
}

// purgeCheckpoints removes the checkpoints for a specific checkpointPrefix and feed name.
func (*AttachmentCompactionManager) purgeCheckpoints(ctx context.Context, database *Database, checkpointPrefix string) error {
	return base.PurgeDCPCheckpoints(
		ctx,
		database.MetadataStore,
		checkpointPrefix,
		database.dcpFeedMode(),
	)
}

func (a *AttachmentCompactionManager) handleAttachmentCompactionRollbackError(ctx context.Context, options map[string]any, dataStore base.DataStore, database *Database, err error, phase attachmentCompactionPhase, keyPrefix string) (bool, error) {
	var rollbackErr gocbcore.DCPRollbackError
	if errors.As(err, &rollbackErr) || errors.Is(err, base.ErrVbUUIDMismatch) {
		base.InfofCtx(ctx, base.KeyDCP, "rollback indicated on %s phase of attachment compaction, resetting the task", phase)
		// to rollback any phase for attachment compaction we need to purge all persisted dcp metadata
		base.InfofCtx(ctx, base.KeyDCP, "Purging invalid checkpoints for background task run %s", a.getCompactID())
		err = a.purgeCheckpoints(ctx, database, keyPrefix)
		if err != nil {
			base.WarnfCtx(ctx, "error occurred during purging of dcp metadata: %s", err)
			return false, err
		}
		if phase == MarkPhase {
			// initialise new compaction run as we want to start the phase mark again in event of rollback
			_, err = a.Init(ctx, options, nil)
			if err != nil {
				base.WarnfCtx(ctx, "error on initialization of new run after rollback has been indicated: %s", err)
				return false, err
			}
		} else {
			// we only handle rollback for mark and cleanup so if we call here it will be for cleanup phase
			// we need to clear the vbUUID's on the manager for cleanup phase otherwise we will end up in loop of constant rollback
			// as these are used for the initial metadata on the client
			a.setVBUUIDs(nil)
		}
		// we should try again if it is rollback error
		return true, nil
	}
	// if error isn't rollback then assume it's not recoverable
	return false, err
}

func (a *AttachmentCompactionManager) SetPhase(phase string) {
	a.lock.Lock()
	defer a.lock.Unlock()

	a._phase = phase
}

// setVBUUIDs updates the VBUUIDs stored on the manager.
func (a *AttachmentCompactionManager) setVBUUIDs(vbuuids []uint64) {
	a.lock.Lock()
	defer a.lock.Unlock()

	a._vbuuids = vbuuids
}

// initializeFromPreviousStatus restores in-memory state from a previously persisted status document
// so that a resumed run starts with the correct accumulated counts and identifiers.
func (a *AttachmentCompactionManager) initializeFromPreviousStatus(statusDoc AttachmentManagerStatusDoc) {
	a.lock.Lock()
	defer a.lock.Unlock()

	a._compactID = statusDoc.CompactID
	a._phase = statusDoc.Phase
	a._dryRun = statusDoc.DryRun
	a.MarkedAttachments.Set(statusDoc.MarkedAttachments)
	a.PurgedAttachments.Set(statusDoc.PurgedAttachments)
	a._vbuuids = statusDoc.VBUUIDs
}

// getCompactID returns the unique identifier for the current compaction run.
func (a *AttachmentCompactionManager) getCompactID() string {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a._compactID
}

// getPhase returns the current compaction phase ("mark", "sweep", "cleanup", or "" when idle).
func (a *AttachmentCompactionManager) getPhase() string {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a._phase
}

// getDryRun returns whether the compaction is running in dry-run mode, where attachments are
// identified but not deleted.
func (a *AttachmentCompactionManager) getDryRun() bool {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a._dryRun
}

// getVBUUIDs returns the vBucket UUIDs recorded after the mark phase.
func (a *AttachmentCompactionManager) getVBUUIDs() []uint64 {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a._vbuuids
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

func (a *AttachmentCompactionManager) SetProcessStatus(context.Context, []byte, []byte) {}

func (a *AttachmentCompactionManager) GetProcessStatus(status BackgroundManagerStatus, _ []byte) ([]byte, []byte, error) {
	a.lock.RLock()
	defer a.lock.RUnlock()

	response := AttachmentManagerResponse{
		BackgroundManagerStatus: status,
		MarkedAttachments:       a.MarkedAttachments.Value(),
		PurgedAttachments:       a.PurgedAttachments.Value(),
		CompactID:               a._compactID,
		Phase:                   a._phase,
		DryRun:                  a._dryRun,
	}

	meta := AttachmentManagerMeta{
		VBUUIDs: a._vbuuids,
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
	a._dryRun = false
}
