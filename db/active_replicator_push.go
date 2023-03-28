/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// ActivePushReplicator is a unidirectional push active replicator.
type ActivePushReplicator struct {
	*activeReplicatorCommon
}

func NewPushReplicator(ctx context.Context, config *ActiveReplicatorConfig) (*ActivePushReplicator, error) {
	replicator, err := newActiveReplicatorCommon(ctx, config, ActiveReplicatorTypePush)
	if err != nil {
		return nil, err
	}
	apr := ActivePushReplicator{
		activeReplicatorCommon: replicator,
	}
	replicator._getStatusCallback = apr._getStatus
	apr.replicatorConnectFn = apr._connect
	return &apr, nil
}

func (apr *ActivePushReplicator) Start(ctx context.Context) error {
	apr.lock.Lock()
	defer apr.lock.Unlock()

	if apr == nil {
		return fmt.Errorf("nil ActivePushReplicator, can't start")
	}

	if apr.ctx != nil && apr.ctx.Err() == nil {
		return fmt.Errorf("ActivePushReplicator already running")
	}

	apr.setState(ReplicationStateStarting)
	// intentionally reset the context from having db information on it?
	logCtx := base.LogContextWith(ctx, &base.LogContext{CorrelationID: apr.config.ID + "-" + string(ActiveReplicatorTypePush)})
	apr.ctx, apr.ctxCancel = context.WithCancel(logCtx)

	err := apr._connect()
	if err != nil {
		_ = apr.setError(err)
		base.WarnfCtx(apr.ctx, "Couldn't connect: %s", err)
		if errors.Is(err, fatalReplicatorConnectError) {
			base.WarnfCtx(apr.ctx, "Stopping replication connection attempt")
		} else if apr.config.TotalReconnectTimeout != 0 {
			base.InfofCtx(apr.ctx, base.KeyReplicate, "Attempting to reconnect in background: %v", err)
			apr.reconnectActive.Set(true)
			go apr.reconnectLoop()
		}
	}
	apr._publishStatus()
	return err
}

var PreHydrogenTargetAllowConflictsError = errors.New("cannot run replication to target with allow_conflicts=false. Change to allow_conflicts=true or upgrade to 2.8")

// _connect opens up a connection, and starts replicating.
func (apr *ActivePushReplicator) _connect() error {
	var err error
	apr.blipSender, apr.blipSyncContext, err = connect(apr.activeReplicatorCommon, "-push")
	if err != nil {
		return err
	}

	// TODO: If this were made a config option, and the default conflict resolver not enforced on
	// 	the pull side, it would be feasible to run sgr-2 in 'manual conflict resolution' mode
	apr.blipSyncContext.sendRevNoConflicts = true

	if apr.config.CollectionsEnabled {
		if err := apr._startPushWithCollections(); err != nil {
			return err
		}
	} else {
		// for backwards compatibility use no collection-specific handling/messages
		if err := apr._startPushNonCollection(); err != nil {
			return err
		}
	}

	if err := apr.startStatusReporter(); err != nil {
		return err
	}

	apr.setState(ReplicationStateRunning)
	return nil
}

// Complete gracefully shuts down a replication, waiting for all in-flight revisions to be processed
// before stopping the replication
func (apr *ActivePushReplicator) Complete() {
	base.TracefCtx(apr.ctx, base.KeyReplicate, "ActivePushReplicator.Complete()")
	apr.lock.Lock()
	if apr == nil {
		apr.lock.Unlock()
		return
	}

	// Wait for any pending changes responses to arrive and be processed
	err := apr._waitForPendingChangesResponse()
	if err != nil {
		base.InfofCtx(apr.ctx, base.KeyReplicate, "Timeout waiting for pending changes response for replication %s - stopping: %v", apr.config.ID, err)
	}

	_ = apr.forEachCollection(func(c *activeReplicatorCollection) error {
		if err := c.Checkpointer.waitForExpectedSequences(); err != nil {
			base.InfofCtx(apr.ctx, base.KeyReplicate, "Timeout draining replication %s - stopping: %v", apr.config.ID, err)
		}
		return nil
	})

	apr._stop()

	stopErr := apr._disconnect()
	if stopErr != nil {
		base.InfofCtx(apr.ctx, base.KeyReplicate, "Error attempting to stop replication %s: %v", apr.config.ID, stopErr)
	}
	apr.setState(ReplicationStateStopped)

	// unlock the replication before triggering callback, in case callback attempts to re-acquire the lock
	onCompleteCallback := apr.onReplicatorComplete
	apr._publishStatus()
	apr.lock.Unlock()

	if onCompleteCallback != nil {
		onCompleteCallback()
	}
}

// initCheckpointer starts a checkpointer. The remoteCheckpoints are only for collections and indexed by the blip collectionIdx. If using default collection only, replicationCheckpoints is an empty array.
func (apr *ActivePushReplicator) _initCheckpointer(remoteCheckpoints []replicationCheckpoint) error {
	// wrap the replicator context with a cancelFunc that can be called to abort the checkpointer from _disconnect
	apr.checkpointerCtx, apr.checkpointerCtxCancel = context.WithCancel(apr.ctx)

	err := apr.forEachCollection(func(c *activeReplicatorCollection) error {
		checkpointHash, hashErr := apr.config.CheckpointHash(c.collectionIdx)
		if hashErr != nil {
			return hashErr
		}

		c.Checkpointer = NewCheckpointer(apr.checkpointerCtx, c.metadataStore, c.collectionDataStore, apr.CheckpointID, checkpointHash, apr.blipSender, apr.config, c.collectionIdx)

		if apr.config.CollectionsEnabled {
			err := c.Checkpointer.setLastCheckpointSeq(&remoteCheckpoints[*c.collectionIdx])
			if err != nil {
				return err
			}
		} else {
			err := c.Checkpointer.fetchDefaultCollectionCheckpoints()
			if err != nil {
				return err
			}
		}

		if err := apr.registerCheckpointerCallbacks(c); err != nil {
			return err
		}

		c.Checkpointer.Start()
		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (apr *ActivePushReplicator) _getStatus() *ReplicationStatus {
	status := &ReplicationStatus{}
	status.Status, status.ErrorMessage = apr._getStateWithErrorMessage()

	pushStats := apr.replicationStats
	status.DocsWritten = pushStats.SendRevCount.Value()
	status.DocsCheckedPush = pushStats.SendChangesCount.Value()
	status.DocWriteFailures = pushStats.SendRevErrorTotal.Value()
	status.DocWriteConflict = pushStats.SendRevErrorConflictCount.Value()
	status.RejectedRemote = pushStats.SendRevErrorRejectedCount.Value()
	status.DeltasSent = pushStats.SendRevDeltaSentCount.Value()
	status.LastSeqPush = apr.getCheckpointHighSeq()
	if apr.initialStatus != nil {
		status.PushReplicationStatus.Add(apr.initialStatus.PushReplicationStatus)
	}
	return status
}

// GetStatus is used externally to retrieve pull replication status.  Combines current running stats with
// initialStatus.
func (apr *ActivePushReplicator) GetStatus() *ReplicationStatus {
	apr.lock.RLock()
	defer apr.lock.RUnlock()
	return apr._getStatus()
}

// reset performs a reset on the replication by removing the local checkpoint document.
func (apr *ActivePushReplicator) reset() error {
	if apr.state != ReplicationStateStopped {
		return fmt.Errorf("reset invoked for replication %s when the replication was not stopped", apr.config.ID)
	}

	apr.lock.Lock()
	defer apr.lock.Unlock()

	if err := apr.forEachCollection(func(c *activeReplicatorCollection) error {
		if err := resetLocalCheckpoint(c.collectionDataStore, apr.CheckpointID); err != nil {
			return err
		}
		c.Checkpointer = nil
		return nil
	}); err != nil {
		return err
	}

	return removeLocalStatus(apr.ctx, apr.config.ActiveDB.MetadataStore, apr.CheckpointID)
}

// registerCheckpointerCallbacks registers appropriate callback functions for checkpointing.
func (apr *ActivePushReplicator) registerCheckpointerCallbacks(c *activeReplicatorCollection) error {
	blipSyncContextCollection, err := apr.blipSyncContext.collections.get(c.collectionIdx)
	if err != nil {
		base.WarnfCtx(apr.ctx, "Unable to get blipSyncContextCollection for collection %v", c.collectionIdx)
		return err
	}

	blipSyncContextCollection.sgr2PushAlreadyKnownSeqsCallback = c.Checkpointer.AddAlreadyKnownSeq
	blipSyncContextCollection.sgr2PushAddExpectedSeqsCallback = c.Checkpointer.AddExpectedSeqs
	blipSyncContextCollection.sgr2PushProcessedSeqCallback = c.Checkpointer.AddProcessedSeq

	return nil
}

// waitForExpectedSequences waits for the pending changes response count
// to drain to zero.  Intended to be used once the replication has been stopped, to wait for
// in-flight changes responses to arrive.
// Waits up to 10s, polling every 100ms.
func (apr *ActivePushReplicator) _waitForPendingChangesResponse() error {
	waitCount := 0
	for waitCount < 100 {
		if apr.blipSyncContext == nil {
			return nil
		}
		pendingCount := atomic.LoadInt64(&apr.blipSyncContext.changesPendingResponseCount)
		if pendingCount <= 0 {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
		waitCount++
	}
	return errors.New("checkpointer _waitForPendingChangesResponse failed to complete after waiting 10s")
}

// Stop stops the push replication and waits for the send changes goroutine to finish.
func (apr *ActivePushReplicator) Stop() error {
	if err := apr.stopAndDisconnect(); err != nil {
		return err
	}
	teardownStart := time.Now()
	for apr.activeSendChanges.IsTrue() && (time.Since(teardownStart) < time.Second*10) {
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func (apr *ActivePushReplicator) _startPushNonCollection() error {
	dbCollection, err := apr.config.ActiveDB.GetDefaultDatabaseCollection()
	if err != nil {
		return err
	}
	apr.blipSyncContext.collections.setNonCollectionAware(newBlipSyncCollectionContext(dbCollection))

	if err := apr._initCheckpointer(nil); err != nil {
		// clean up anything we've opened so far
		base.TracefCtx(apr.ctx, base.KeyReplicate, "Error initialising checkpoint in _connect. Closing everything.")
		apr.checkpointerCtx = nil
		apr.blipSender.Close()
		apr.blipSyncContext.Close()
		return err
	}

	dbCollectionWithUser := &DatabaseCollectionWithUser{
		DatabaseCollection: dbCollection,
		user:               apr.config.ActiveDB.user,
	}
	bh := blipHandler{
		BlipSyncContext: apr.blipSyncContext,
		db:              apr.config.ActiveDB,
		collection:      dbCollectionWithUser,
		serialNumber:    apr.blipSyncContext.incrementSerialNumber(),
	}

	var channels base.Set
	if filteredChannels := apr.config.getFilteredChannels(nil); len(filteredChannels) > 0 {
		channels = base.SetFromArray(filteredChannels)
	}

	apr.blipSyncContext.fatalErrorCallback = func(err error) {
		if strings.Contains(err.Error(), ErrUseProposeChanges.Message) {
			err = ErrUseProposeChanges
			_ = apr.setError(PreHydrogenTargetAllowConflictsError)
			err = apr.stopAndDisconnect()
			if err != nil {
				base.ErrorfCtx(apr.ctx, "Failed to stop and disconnect replication: %v", err)
			}
		} else if strings.Contains(err.Error(), ErrDatabaseWentAway.Message) {
			err = apr.reconnect()
			if err != nil {
				base.ErrorfCtx(apr.ctx, "Failed to reconnect replication: %v", err)
			}
		}
		// No special handling for error
	}

	collectionCtx, err := bh.collections.get(nil)
	if err != nil {
		return err
	}

	apr.activeSendChanges.Set(true)
	go func(s *blip.Sender) {
		defer apr.activeSendChanges.Set(false)
		isComplete := bh.sendChanges(s, &sendChangesOptions{
			docIDs:            apr.config.DocIDs,
			since:             apr.defaultCollection.Checkpointer.lastCheckpointSeq,
			continuous:        apr.config.Continuous,
			activeOnly:        apr.config.ActiveOnly,
			batchSize:         int(apr.config.ChangesBatchSize),
			revocations:       apr.config.PurgeOnRemoval,
			channels:          channels,
			clientType:        clientTypeSGR2,
			ignoreNoConflicts: true, // force the passive side to accept a "changes" message, even in no conflicts mode.
			changesCtx:        collectionCtx.changesCtx,
		})
		// On a normal completion, call complete for the replication
		if isComplete {
			apr.Complete()
		}
	}(apr.blipSender)

	return nil
}
