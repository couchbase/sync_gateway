// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"encoding/json"
	"fmt"

	"github.com/couchbase/sync_gateway/base"
)

func getScopeAndCollectionName(scopeAndCollectionStr string) (base.ScopeAndCollectionName, error) {
	scopeName, collectionName, err := parseScopeAndCollection(scopeAndCollectionStr)
	if err != nil {
		return base.ScopeAndCollectionName{}, err
	} else if scopeName == nil || collectionName == nil {
		return base.ScopeAndCollectionName{}, fmt.Errorf("scope and collection name must be specified: %q", scopeAndCollectionStr)
	}
	return base.ScopeAndCollectionName{Scope: *scopeName, Collection: *collectionName}, nil
}

// _initCollections will negotiate the set of collections with the peer using GetCollections and returns the set of checkpoints for each of them.
func (arc *activeReplicatorCommon) _initCollections() ([]replicationCheckpoint, error) {

	var (
		remoteCollectionsKeyspaces  base.ScopeAndCollectionNames
		localCollectionsKeyspaces   base.ScopeAndCollectionNames
		getCollectionsCheckpointIDs []string
	)

	if remoteLen := len(arc.config.CollectionsRemote); remoteLen > 0 {
		if localLen := len(arc.config.CollectionsLocal); localLen != remoteLen {
			return nil, fmt.Errorf("local and remote collections must be the same length... had %d and %d", localLen, remoteLen)
		}
	}

	if arc.config.CollectionsLocal != nil {
		for i, localScopeAndCollection := range arc.config.CollectionsLocal {
			localScopeAndCollectionName, err := getScopeAndCollectionName(localScopeAndCollection)
			if err != nil {
				return nil, err
			}
			localCollectionsKeyspaces = append(localCollectionsKeyspaces, localScopeAndCollectionName)

			// remap collection name to remote if set
			if remoteScopeAndCollection := arc.config.CollectionsRemote[i]; remoteScopeAndCollection != "" {
				base.DebugfCtx(arc.ctx, base.KeyReplicate, "Mapping local %q to remote %q", localScopeAndCollection, remoteScopeAndCollection)
				remoteScopeAndCollectionName, err := getScopeAndCollectionName(remoteScopeAndCollection)
				if err != nil {
					return nil, err
				}
				remoteCollectionsKeyspaces = append(remoteCollectionsKeyspaces, remoteScopeAndCollectionName)
			} else {
				remoteCollectionsKeyspaces = append(remoteCollectionsKeyspaces, localScopeAndCollectionName)
			}
			getCollectionsCheckpointIDs = append(getCollectionsCheckpointIDs, arc.CheckpointID)
		}
	} else {
		// collections to replicate wasn't set - so build a full set based on local database
		for _, dbCollection := range arc.blipSyncContext.blipContextDb.CollectionByID {
			localCollectionsKeyspaces = append(localCollectionsKeyspaces, base.ScopeAndCollectionName{Scope: dbCollection.ScopeName, Collection: dbCollection.Name})
			remoteCollectionsKeyspaces = append(remoteCollectionsKeyspaces, base.ScopeAndCollectionName{Scope: dbCollection.ScopeName, Collection: dbCollection.Name})
			getCollectionsCheckpointIDs = append(getCollectionsCheckpointIDs, arc.CheckpointID)
		}
	}

	msg, err := NewGetCollectionsMessage(GetCollectionsRequestBody{
		Collections:   remoteCollectionsKeyspaces.ScopeAndCollectionNames(),
		CheckpointIDs: getCollectionsCheckpointIDs,
	})
	if err != nil {
		return nil, err
	}

	if !arc.blipSender.Send(msg) {
		return nil, fmt.Errorf("unable to send GetCollections message")
	}

	var resp []json.RawMessage
	r := msg.Response()

	if errDomain, ok := r.Properties[BlipErrorDomain]; ok {
		errCode := r.Properties[BlipErrorCode]
		if errDomain == "BLIP" && errCode == "404" {
			return nil, fmt.Errorf("Remote does not support collections")
		}
		return nil, fmt.Errorf("Error getting collections from remote: %s %s", errDomain, errCode)
	}

	rBody, err := r.Body()
	if err != nil {
		return nil, err
	}
	err = base.JSONUnmarshal(rBody, &resp)
	if err != nil {
		return nil, err
	}

	blipSyncCollectionContexts := make([]*blipSyncCollectionContext, len(resp))
	collectionCheckpoints := make([]replicationCheckpoint, len(resp))

	for i, checkpointBody := range resp {
		var checkpoint *replicationCheckpoint
		err = base.JSONUnmarshal(checkpointBody, &checkpoint)
		if err != nil {
			return nil, err
		}

		if checkpoint == nil {
			return nil, fmt.Errorf("peer does not have collection %q", remoteCollectionsKeyspaces[i])
		} else if checkpoint.LastSeq == "" {
			// collection valid but no checkpoint - start from sequence zero
			checkpoint.LastSeq = CreateZeroSinceValue().String()
		}

		// remap back to local collection name for the active side of the replication
		dbCollection, err := arc.blipSyncContext.blipContextDb.GetDatabaseCollection(localCollectionsKeyspaces[i].ScopeName(), localCollectionsKeyspaces[i].CollectionName())
		if err != nil {
			return nil, err
		}

		collectionContext := newBlipSyncCollectionContext(dbCollection)
		blipSyncCollectionContexts[i] = collectionContext
		collectionCheckpoints[i] = *checkpoint

		arc.namedCollections[localCollectionsKeyspaces[i]] = &activeReplicatorCollection{collectionIdx: base.IntPtr(i), dataStore: dbCollection.dataStore}
	}

	arc.blipSyncContext.collections.set(blipSyncCollectionContexts)

	return collectionCheckpoints, nil
}

// forEachCollection runs the callback function for each collection on the replicator.
func (arc *activeReplicatorCommon) forEachCollection(callback func(*activeReplicatorCollection) error) error {
	if arc.config.CollectionsEnabled {
		for _, collection := range arc.namedCollections {
			if err := callback(collection); err != nil {
				return err
			}
		}
	} else {
		if err := callback(arc.defaultCollection); err != nil {
			return err
		}
	}
	return nil
}
