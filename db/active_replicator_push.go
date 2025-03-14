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

// NewPushReplicator creates an ISGR push replicator.
func NewPushReplicator(ctx context.Context, config *ActiveReplicatorConfig) (*ActivePushReplicator, error) {
	replicator, err := newActiveReplicatorCommon(ctx, config, ActiveReplicatorTypePush)
	if err != nil {
		return nil, err
	}
	apr := ActivePushReplicator{
		activeReplicatorCommon: replicator,
	}
	replicator.registerFunctions(apr._getStatus, apr._connect, apr.registerCheckpointerCallbacks)
	return &apr, nil
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

	apr.setState(ReplicationStateRunning)
	return nil
}

// Complete gracefully shuts down a replication, waiting for all in-flight revisions to be processed
// before stopping the replication
func (apr *ActivePushReplicator) Complete() {
	base.TracefCtx(apr.ctx, base.KeyReplicate, "ActivePushReplicator.Complete()")
	apr.lock.Lock()

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

// _getStatus returns current replicator status. Requires holding ActivePushReplicator.lock as a read lock.
func (apr *ActivePushReplicator) _getStatus() *ReplicationStatus {
	status := &ReplicationStatus{}
	status.Status, status.ErrorMessage = apr.getStateWithErrorMessage()

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
	for apr.activeSendChanges.Load() != 0 && (time.Since(teardownStart) < time.Second*10) {
		time.Sleep(10 * time.Millisecond)
	}
	return nil
}

func (apr *ActivePushReplicator) _startPushNonCollection() error {
	dbCollection, err := apr.config.ActiveDB.GetDefaultDatabaseCollection()
	if err != nil {
		return err
	}
	apr.blipSyncContext.collections.setNonCollectionAware(newBlipSyncCollectionContext(apr.ctx, dbCollection))

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
	bh := newBlipHandler(apr.ctx, apr.blipSyncContext, apr.config.ActiveDB, apr.blipSyncContext.incrementSerialNumber())
	bh.collection = dbCollectionWithUser
	bh.loggingCtx = bh.collection.AddCollectionContext(bh.BlipSyncContext.loggingCtx)

	return apr._startSendingChanges(bh, apr.defaultCollection.Checkpointer.lastCheckpointSeq)
}

// _startSendingChanges starts a changes feed for a given collection in a goroutine and starts sending changes to the passive peer from a starting sequence value.
func (apr *ActivePushReplicator) _startSendingChanges(bh *blipHandler, since SequenceID) error {
	collectionCtx, err := bh.collections.get(bh.collectionIdx)
	if err != nil {
		return err
	}
	var channels base.Set
	if filteredChannels := apr.config.getFilteredChannels(bh.collectionIdx); len(filteredChannels) > 0 {
		channels = base.SetFromArray(filteredChannels)
	}

	apr.blipSyncContext.fatalErrorCallback = func(err error) {
		if strings.Contains(err.Error(), ErrUseProposeChanges.Message) {
			err = ErrUseProposeChanges
			apr.setError(PreHydrogenTargetAllowConflictsError)
			err = apr.stopAndDisconnect()
			if err != nil {
				base.ErrorfCtx(apr.ctx, "Failed to stop and disconnect replication: %v", err)
			}
		} else if strings.Contains(err.Error(), ErrDatabaseWentAway.Message) {
			err = apr.disconnect()
			if err != nil {
				base.ErrorfCtx(apr.ctx, "Failed to disconnect replication after database went away: %v", err)
			}
		}
		// No special handling for error
	}

	apr.activeSendChanges.Add(1)
	go func(s *blip.Sender) {
		defer apr.activeSendChanges.Add(-1)
		isComplete, err := bh.sendChanges(s, &sendChangesOptions{
			docIDs:            apr.config.DocIDs,
			since:             since,
			continuous:        apr.config.Continuous,
			activeOnly:        apr.config.ActiveOnly,
			batchSize:         int(apr.config.ChangesBatchSize),
			revocations:       apr.config.PurgeOnRemoval,
			channels:          channels,
			clientType:        clientTypeSGR2,
			ignoreNoConflicts: true, // force the passive side to accept a "changes" message, even in no conflicts mode.
			changesCtx:        collectionCtx.changesCtx,
		})
		if err != nil {
			base.InfofCtx(apr.ctx, base.KeyReplicate, "Terminating blip connection due to changes feed error: %v", err)
			bh.ctxCancelFunc()
			apr.setError(err)
			apr.publishStatus()
			return
		}
		if isComplete {
			// On a normal completion, call complete for the replication
			apr.Complete()
		}
	}(apr.blipSender)
	return nil
}
