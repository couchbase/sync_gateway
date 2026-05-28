/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
)

type MetadataMigrationManager struct {
	docsProcessed  atomic.Int64 // cumulative successful moves/deletes across all passes
	docsFailed     atomic.Int64 // cumulative per-doc errors across all passes
	docsOutOfScope atomic.Int64 // last-pass snapshot - same static set is re-scanned each pass
	passes         atomic.Int64 // number of MigrateMetadata invocations attempted
	MigrationID    string
	dbContext      *DatabaseContext
	lock           sync.RWMutex
}

const MetadataMigrationManagerName = "metadata_migration"

var _ BackgroundManagerProcessI = &MetadataMigrationManager{}

func NewMetadataMigrationManager(dbContext *DatabaseContext) *BackgroundManager {
	return &BackgroundManager{
		name:    MetadataMigrationManagerName,
		Process: &MetadataMigrationManager{dbContext: dbContext},
		clusterAwareOptions: &ClusterAwareBackgroundManagerOptions{
			metadataStore: dbContext.MetadataStore,
			metaKeys:      dbContext.MetadataKeys,
			processSuffix: MetadataMigrationManagerName,
		},
		terminator: base.NewSafeTerminator(),
	}
}

type MigrationManagerResponse struct {
	BackgroundManagerStatus
	DocsProcessed  int64  `json:"docs_processed"`    // cumulative successful moves/deletes
	DocsFailed     int64  `json:"docs_failed"`       // cumulative per-doc errors
	DocsAttempted  int64  `json:"docs_attempted"`    // docs_processed + docs_failed
	DocsOutOfScope int64  `json:"docs_out_of_scope"` // last-pass snapshot
	Passes         int64  `json:"passes"`            // number of MigrateMetadata invocations
	MigrationID    string `json:"migration_id"`
}

type MigrationManagerStatusDoc struct {
	MigrationManagerResponse `json:"status"`
}

func (m *MetadataMigrationManager) Init(ctx context.Context, options map[string]any, clusterStatus []byte) error {
	newRunInit := func() error {
		uniqueUUID, err := uuid.NewRandom()
		if err != nil {
			return err
		}

		m.MigrationID = uniqueUUID.String()
		base.InfofCtx(ctx, base.KeyAll, "Metadata Migration: Starting new migration run with migration ID: %s", m.MigrationID)
		return nil
	}

	if clusterStatus != nil {
		var status MigrationManagerStatusDoc
		err := base.JSONUnmarshal(clusterStatus, &status)
		// If the previous run completed, or there was an error during unmarshalling the status start again
		if status.State == BackgroundProcessStateCompleted || err != nil {
			return newRunInit()
		}
		m.docsProcessed.Store(status.DocsProcessed)
		m.docsFailed.Store(status.DocsFailed)
		m.docsOutOfScope.Store(status.DocsOutOfScope)
		m.passes.Store(status.Passes)
		m.MigrationID = status.MigrationID
		base.InfofCtx(ctx, base.KeyAll, "Metadata Migration: Resuming migration run with migration ID: %s, docs processed: %d, docs failed: %d, passes: %d", m.MigrationID, status.DocsProcessed, status.DocsFailed, status.Passes)
		return nil
	}
	return newRunInit()
}

func (m *MetadataMigrationManager) Run(ctx context.Context, options map[string]any, persistClusterStatusCallback updateStatusCallbackFunc, terminator *base.SafeTerminator) error {
	metadataMigrationLoggingID := "Metadata Migration: " + m.MigrationID

	persistClusterStatus := func() {
		err := persistClusterStatusCallback(ctx)
		if err != nil {
			base.WarnfCtx(ctx, "[%s] Failed to persist latest cluster status for metadata migration: %v", metadataMigrationLoggingID, err)
		}
	}
	defer persistClusterStatus()

	metadataID := m.dbContext.Options.MetadataID
	if metadataID == "" {
		base.WarnfCtx(ctx, "[%s] No MetadataID set on DatabaseContext, cannot record per-DB status", metadataMigrationLoggingID)
		return nil
	}

	if m.dbContext.MetadataMigrationStatusUpdater == nil {
		base.WarnfCtx(ctx, "[%s] MetadataMigrationStatusUpdater not wired on DatabaseContext, skipping status tracking", metadataMigrationLoggingID)
		return nil
	}

	now := time.Now().UTC()
	if err := m.dbContext.MetadataMigrationStatusUpdater(ctx, func(s *base.MetadataMigrationStatus) error {
		entry, ok := s.Databases[metadataID]
		if !ok || entry == nil {
			entry = &base.DatabaseMigrationStatus{}
			s.Databases[metadataID] = entry
		}
		entry.State = base.MigrationStateInProgress
		entry.StartedAt = &now
		entry.CompletedAt = nil
		return nil
	}); err != nil {
		return base.RedactErrorf("[%s] Failed to mark per-DB migration in_progress: %v", metadataMigrationLoggingID, err)
	}

	// If the MetadataStore is not a dual-collection wrapper there is nothing to copy —
	// fall through to the per-DB complete write so the bucket-level all-complete trigger
	// can still fire and the entry doesn't wedge in in_progress.
	if ms, ok := m.dbContext.MetadataStore.(*base.MetadataStore); ok {
		// Bridge the terminator into context cancellation so long-running ops inside
		// migrateSeqCounter (retry loop) and MigrateMetadata (range scan iteration) can
		// observe stop requests without us having to thread *SafeTerminator through every
		// function signature.
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-terminator.Done():
				cancel()
			case <-runCtx.Done():
			}
		}()

		// One-shot setup: poison-pill the fallback seq counter and nudge the wrapper to
		// promote it to primary. Runs once per migration attempt — it has no per-doc-write
		// race to recover from, so it does not belong inside the pass loop below.
		seqStats := &MigrationStats{}
		if err := migrateSeqCounter(runCtx, ms, base.NewMetadataKeys(metadataID).SyncSeqKey(), seqStats); err != nil {
			return fmt.Errorf("[%s] seq counter migration: %w", metadataMigrationLoggingID, err)
		}
		if applied := seqStats.SeqPoisonPillApplied.Load(); applied > 0 {
			base.InfofCtx(ctx, base.KeyAll, "[%s] seq counter pilled and promoted to primary", metadataMigrationLoggingID)
		}

		// The migration loop runs until either:
		// - there are no in-scope remaining docs
		// - we hit the max retries limit and give up
		//
		// Each pass scans the fallback DataStore, and remaining are in-scope docs we didn't move on this pass
		const maxPasses = 16
		for pass := 0; ; pass++ {
			if terminator.IsClosed() {
				// Mid-run stop: leave the per-DB entry in in_progress so the next manager
				// invocation resumes from where we left off.
				base.InfofCtx(ctx, base.KeyAll, "[%s] terminated mid-run after %d pass(es)", metadataMigrationLoggingID, pass)
				return nil
			}

			stats := &MigrationStats{}
			remaining, err := MigrateMetadata(runCtx, ms, metadataID, stats)
			m.passes.Add(1)
			m.docsProcessed.Add(stats.DocsMigrated.Load())
			m.docsFailed.Add(stats.Errors.Load())
			// Out-of-scope reflects what is left on the fallback, not work done - record
			// the latest pass's view rather than accumulating across re-scans.
			m.docsOutOfScope.Store(stats.DocsOutOfScope.Load())
			base.InfofCtx(ctx, base.KeyAll,
				"[%s] pass %d: migrated=%d failed=%d out_of_scope=%d remaining=%d (cumulative: processed=%d failed=%d)",
				metadataMigrationLoggingID, pass+1,
				stats.DocsMigrated.Load(), stats.Errors.Load(), stats.DocsOutOfScope.Load(), remaining,
				m.docsProcessed.Load(), m.docsFailed.Load())
			persistClusterStatus()
			if err != nil {
				return err
			}

			if remaining == 0 {
				// finished successfully
				break
			}

			if pass+1 >= maxPasses {
				base.WarnfCtx(ctx, "[%s] gave up after %d passes with %d in-scope docs remaining", metadataMigrationLoggingID, maxPasses, remaining)
				return fmt.Errorf("%s still not clear of metadata after %d passes: %d unknown-prefix doc(s) remain", ms.Fallback().GetName(), maxPasses, remaining)
			}
		}

		ms.SetMigrationComplete()
		base.InfofCtx(ctx, base.KeyAll, "[%s] Metadata migration complete after %d pass(es): %d migrated, %d failed, %d out of scope",
			metadataMigrationLoggingID, m.passes.Load(), m.docsProcessed.Load(), m.docsFailed.Load(), m.docsOutOfScope.Load())
	} else {
		base.InfofCtx(ctx, base.KeyAll, "[%s] MetadataStore not running in primary/fallback mode - nothing to migrate", metadataMigrationLoggingID)
	}

	completedAt := time.Now().UTC()
	if err := m.dbContext.MetadataMigrationStatusUpdater(ctx, func(s *base.MetadataMigrationStatus) error {
		entry, ok := s.Databases[metadataID]
		if !ok || entry == nil {
			entry = &base.DatabaseMigrationStatus{StartedAt: &now}
			s.Databases[metadataID] = entry
		}
		entry.State = base.MigrationStateComplete
		entry.CompletedAt = &completedAt
		return nil
	}); err != nil {
		return base.RedactErrorf("[%s] Failed to mark per-DB migration complete: %v", metadataMigrationLoggingID, err)
	}

	if m.dbContext.PostMetadataMigrationCompleteFunc != nil {
		if err := m.dbContext.PostMetadataMigrationCompleteFunc(ctx); err != nil {
			base.WarnfCtx(ctx, "[%s] Post-completion hook returned an error: %v", metadataMigrationLoggingID, err)
		}
	}
	return nil
}

func (m *MetadataMigrationManager) ResetStatus() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.docsProcessed.Store(0)
	m.docsFailed.Store(0)
	m.docsOutOfScope.Store(0)
	m.passes.Store(0)
	m.MigrationID = ""
}

func (m *MetadataMigrationManager) SetProcessStatus(ctx context.Context, previousStatus []byte, newStatus []byte) {
	// no-op
}

func (m *MetadataMigrationManager) GetProcessStatus(status BackgroundManagerStatus, previousStatus []byte) (statusOut []byte, meta []byte, err error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	processed := m.docsProcessed.Load()
	failed := m.docsFailed.Load()
	resp := MigrationManagerResponse{
		BackgroundManagerStatus: status,
		DocsProcessed:           processed,
		DocsFailed:              failed,
		DocsAttempted:           processed + failed,
		DocsOutOfScope:          m.docsOutOfScope.Load(),
		Passes:                  m.passes.Load(),
		MigrationID:             m.MigrationID,
	}
	statusOut, err = base.JSONMarshal(resp)
	if err != nil {
		return nil, nil, err
	}
	return statusOut, nil, nil
}
