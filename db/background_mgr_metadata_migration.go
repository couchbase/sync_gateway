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
	"sync"
	"sync/atomic"

	"github.com/couchbase/sync_gateway/base"
	"github.com/google/uuid"
)

type MetadataMigrationManager struct {
	docsProcessed atomic.Int64
	docsFailed    atomic.Int64
	MigrationID   string
	dbContext     *DatabaseContext
	lock          sync.RWMutex
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
	DocsProcessed int64  `json:"docs_processed"`
	DocsFailed    int64  `json:"docs_failed"`
	MigrationID   string `json:"migration_id"`
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
		m.MigrationID = status.MigrationID
		base.InfofCtx(ctx, base.KeyAll, "Metadata Migration: Resuming migration run with migration ID: %s, docs processed: %d, docs failed: %d", m.MigrationID, status.DocsProcessed, status.DocsFailed)
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

	base.InfofCtx(ctx, base.KeyAll, "[%s] Metadata Migration TODO: CBG-5228", metadataMigrationLoggingID)
	return nil
}

func (m *MetadataMigrationManager) ResetStatus() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.docsProcessed.Store(0)
	m.docsFailed.Store(0)
	m.MigrationID = ""
}

func (m *MetadataMigrationManager) SetProcessStatus(ctx context.Context, previousStatus []byte, newStatus []byte) {
	// no-op
}

func (m *MetadataMigrationManager) GetProcessStatus(status BackgroundManagerStatus, previousStatus []byte) (statusOut []byte, meta []byte, err error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	resp := MigrationManagerResponse{
		BackgroundManagerStatus: status,
		DocsProcessed:           m.docsProcessed.Load(),
		DocsFailed:              m.docsFailed.Load(),
		MigrationID:             m.MigrationID,
	}
	statusOut, err = base.JSONMarshal(resp)
	if err != nil {
		return nil, nil, err
	}
	return statusOut, nil, nil
}
