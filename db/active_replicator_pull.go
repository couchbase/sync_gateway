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
	"fmt"

	"github.com/couchbase/sync_gateway/base"
)

// ActivePullReplicator is a unidirectional pull active replicator.
type ActivePullReplicator struct {
	*activeReplicatorCommon
}

func NewPullReplicator(config *ActiveReplicatorConfig) *ActivePullReplicator {
	apr := ActivePullReplicator{
		activeReplicatorCommon: newActiveReplicatorCommon(config, ActiveReplicatorTypePull),
	}
	apr.replicatorConnectFn = apr._connect
	return &apr
}

func (apr *ActivePullReplicator) Start(ctx context.Context) error {

	apr.lock.Lock()
	defer apr.lock.Unlock()

	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't start")
	}

	if apr.ctx != nil && apr.ctx.Err() == nil {
		return fmt.Errorf("ActivePullReplicator already running")
	}

	apr.setState(ReplicationStateStarting)
	logCtx := base.LogContextWith(ctx, &base.LogContext{CorrelationID: apr.config.ID + "-" + string(ActiveReplicatorTypePull)})
	apr.ctx, apr.ctxCancel = context.WithCancel(logCtx)

	err := apr._connect()
	if err != nil {
		_ = apr.setError(err)
		base.WarnfCtx(apr.ctx, "Couldn't connect. Attempting to reconnect in background: %v", err)
		apr.reconnectActive.Set(true)
		go apr.reconnectLoop()
	}
	apr._publishStatus()
	return err
}

func (apr *ActivePullReplicator) _connect() error {
	var err error
	apr.blipSender, apr.blipSyncContext, err = connect(apr.activeReplicatorCommon, "-pull")
	if err != nil {
		return err
	}

	if apr.config.ConflictResolverFunc != nil {
		apr.blipSyncContext.conflictResolver = NewConflictResolver(apr.config.ConflictResolverFunc, apr.config.ReplicationStatsMap)
	}
	apr.blipSyncContext.purgeOnRemoval = apr.config.PurgeOnRemoval

	if apr.config.CollectionsEnabled {
		if err := apr._startPullWithCollections(); err != nil {
			return err
		}
	} else {
		// for backwards compatibility use no collection-specific handling/messages
		if err := apr._startPullNonCollection(); err != nil {
			return err
		}
	}

	if apr.blipSyncContext.blipContext.ActiveSubprotocol() == BlipCBMobileReplicationV2 && apr.config.PurgeOnRemoval {
		base.ErrorfCtx(apr.ctx, "Pull replicator ID:%s running with revocations enabled but target does not support revocations. Sync Gateway 3.0 required.", apr.config.ID)
	}

	apr.setState(ReplicationStateRunning)

	return nil
}

// _startPullNonCollection starts a pull replication without collection-specific handling
// for backwards compatibility with SG 3.0 and earlier
func (apr *ActivePullReplicator) _startPullNonCollection() error {
	defaultCollection, err := apr.config.ActiveDB.GetDefaultDatabaseCollection()
	if err != nil {
		return err
	}
	apr.blipSyncContext.collections.setNonCollectionAware(&blipSyncCollectionContext{
		dbCollection: defaultCollection,
	})

	if err := apr._initCheckpointer(); err != nil {
		// clean up anything we've opened so far
		base.TracefCtx(apr.ctx, base.KeyReplicate, "Error initialising checkpoint in _connect. Closing everything.")
		apr.checkpointerCtx = nil
		apr.blipSender.Close()
		apr.blipSyncContext.Close()
		return err
	}

	since := apr.defaultCollection.Checkpointer.lastCheckpointSeq.String()

	if err := apr._subChanges(nil, since); err != nil {
		base.TracefCtx(apr.ctx, base.KeyReplicate, "cancelling the checkpointer context inside _connect where we send blip request")
		apr.checkpointerCtxCancel()
		apr.checkpointerCtx = nil
		apr.blipSender.Close()
		apr.blipSyncContext.Close()
		return err
	}

	return nil
}

func (apr *ActivePullReplicator) _subChanges(collectionIdx *int, since string) error {
	subChangesRequest := SubChangesRequest{
		Continuous:     apr.config.Continuous,
		Batch:          apr.config.ChangesBatchSize,
		Since:          since,
		Filter:         apr.config.Filter,
		FilterChannels: apr.config.FilterChannels,
		DocIDs:         apr.config.DocIDs,
		ActiveOnly:     apr.config.ActiveOnly,
		clientType:     clientTypeSGR2,
		Revocations:    apr.config.PurgeOnRemoval,
		CollectionIdx:  collectionIdx,
	}
	return subChangesRequest.Send(apr.blipSender)
}

// Complete gracefully shuts down a replication, waiting for all in-flight revisions to be processed
// before stopping the replication
func (apr *ActivePullReplicator) Complete() {
	apr.lock.Lock()
	if apr == nil {
		apr.lock.Unlock()
		return
	}
	_ = apr.forEachCollection(func(c *activeReplicatorCollection) error {
		base.TracefCtx(apr.ctx, base.KeyReplicate, "Before calling waitForExpectedSequences in Complete()")
		if err := c.Checkpointer.waitForExpectedSequences(); err != nil {
			base.InfofCtx(apr.ctx, base.KeyReplicate, "Couldn't drain replication %s - stopping anyway: %v", apr.config.ID, err)
		}
		base.TracefCtx(apr.ctx, base.KeyReplicate, "After calling waitForExpectedSequences in Complete()")
		return nil
	})

	apr._stop()

	base.TracefCtx(apr.ctx, base.KeyReplicate, "Calling disconnect from Complete() in active replicator pull")
	stopErr := apr._disconnect()
	if stopErr != nil {
		base.InfofCtx(apr.ctx, base.KeyReplicate, "Error attempting to stop replication %s: %v", apr.config.ID, stopErr)
	}
	apr.setState(ReplicationStateStopped)

	// unlock the replication before triggering callback, in case callback attempts to access replication information
	// from the replicator
	onCompleteCallback := apr.onReplicatorComplete

	apr._publishStatus()
	apr.lock.Unlock()

	if onCompleteCallback != nil {
		onCompleteCallback()
	}
}

func (apr *ActivePullReplicator) _initCheckpointer() error {
	// wrap the replicator context with a cancelFunc that can be called to abort the checkpointer from _disconnect
	apr.checkpointerCtx, apr.checkpointerCtxCancel = context.WithCancel(apr.ctx)

	checkpointHash, hashErr := apr.config.CheckpointHash()
	if hashErr != nil {
		return hashErr
	}

	err := apr.forEachCollection(func(c *activeReplicatorCollection) error {
		c.Checkpointer = NewCheckpointer(apr.checkpointerCtx, c.dataStore, apr.CheckpointID, checkpointHash, apr.blipSender, apr.config, apr.getPullStatus, c.collectionIdx)

		if !apr.config.CollectionsEnabled {
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

// GetStatus is used externally to retrieve pull replication status.  Combines current running stats with
// initialStatus.
func (apr *ActivePullReplicator) GetStatus() *ReplicationStatus {
	return apr.getPullStatus(apr.getCheckpointHighSeq())
}

// getPullStatus is used internally, and passed as statusCallback to checkpointer
func (apr *ActivePullReplicator) getPullStatus(lastSeqPulled string) *ReplicationStatus {
	status := &ReplicationStatus{}
	status.Status, status.ErrorMessage = apr.getStateWithErrorMessage()

	pullStats := apr.replicationStats
	status.DocsRead = pullStats.HandleRevCount.Value()
	status.DocsCheckedPull = pullStats.HandleChangesCount.Value()
	status.DocsPurged = pullStats.HandleRevDocsPurgedCount.Value()
	status.RejectedLocal = pullStats.HandleRevErrorCount.Value()
	status.DeltasRecv = pullStats.HandleRevDeltaRecvCount.Value()
	status.DeltasRequested = pullStats.HandleChangesDeltaRequestedCount.Value()
	status.LastSeqPull = lastSeqPulled
	if apr.initialStatus != nil {
		status.PullReplicationStatus.Add(apr.initialStatus.PullReplicationStatus)
	}
	return status
}

func (apr *ActivePullReplicator) reset() error {
	if apr.state != ReplicationStateStopped {
		return fmt.Errorf("reset invoked for replication %s when the replication was not stopped", apr.config.ID)
	}

	if apr.config.CollectionsEnabled {
		for collectionName, _ := range apr.namedCollections {
			dbCollection, err := apr.config.ActiveDB.GetDatabaseCollection(collectionName.ScopeName(), collectionName.CollectionName())
			if err != nil {
				return err
			}
			if err := resetLocalCheckpoint(dbCollection.dataStore, apr.CheckpointID); err != nil {
				return err
			}
		}
	} else {
		collection, err := apr.config.ActiveDB.GetDefaultDatabaseCollection()
		if err != nil {
			return err
		}
		if err := resetLocalCheckpoint(collection.dataStore, apr.CheckpointID); err != nil {
			return err
		}
	}

	apr.lock.Lock()
	defer apr.lock.Unlock()

	return apr.forEachCollection(func(c *activeReplicatorCollection) error {
		c.Checkpointer = nil
		return nil
	})
}

// registerCheckpointerCallbacks registers appropriate callback functions for checkpointing.
func (apr *ActivePullReplicator) registerCheckpointerCallbacks(c *activeReplicatorCollection) error {
	blipSyncContextCollection, err := apr.blipSyncContext.collections.get(c.collectionIdx)
	if err != nil {
		base.WarnfCtx(apr.ctx, "Unable to get blipSyncContextCollection for collection %d: %v", c.collectionIdx, err)
		return err
	}

	blipSyncContextCollection.sgr2PullAlreadyKnownSeqsCallback = c.Checkpointer.AddAlreadyKnownSeq
	blipSyncContextCollection.sgr2PullAddExpectedSeqsCallback = c.Checkpointer.AddExpectedSeqIDAndRevs
	blipSyncContextCollection.sgr2PullProcessedSeqCallback = c.Checkpointer.AddProcessedSeqIDAndRev

	// Trigger complete for non-continuous replications when caught up
	if !apr.config.Continuous {
		blipSyncContextCollection.emptyChangesMessageCallback = func() {
			// Complete blocks waiting for pending rev messages, so needs its own goroutine
			base.TracefCtx(apr.ctx, base.KeyReplicate, "calling complete from registerCheckpointerCallbacks, because we have empty callback")
			go apr.Complete()
		}
	}

	return nil
}

// Stop stops the pull replication and waits for the sub changes goroutine to finish.
func (apr *ActivePullReplicator) Stop() error {
	base.TracefCtx(apr.ctx, base.KeyReplicate, "Calling stop and disconnect from Stop()")
	if err := apr.stopAndDisconnect(); err != nil {
		return err
	}
	return nil
}
