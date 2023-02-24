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

// _initCollections will negotiate the set of collections with the peer using GetCollections and returns the set of checkpoints for each of them.
func (arc *activeReplicatorCommon) _initCollections() ([]replicationCheckpoint, error) {

	var (
		getCollectionsKeyspaces     base.ScopeAndCollectionNames
		getCollectionsCheckpointIDs []string
	)

	if len(arc.config.CollectionsLocal) != len(arc.config.CollectionsRemote) {
		return nil, fmt.Errorf("local and remote collections must be the same length... had %d and %d", len(arc.config.CollectionsLocal), len(arc.config.CollectionsRemote))
	}

	if arc.config.CollectionsLocal != nil {
		// TODO: CBG-2319 - Implement filtering
		return nil, fmt.Errorf("CBG-2319 not yet implemented to pass a list of collections to replicate")
	} else {
		// collections to replicate wasn't set - so build a full set based on local database
		for _, dbCollection := range arc.blipSyncContext.blipContextDb.CollectionByID {
			getCollectionsKeyspaces = append(getCollectionsKeyspaces, base.ScopeAndCollectionName{Scope: dbCollection.ScopeName, Collection: dbCollection.Name})
			getCollectionsCheckpointIDs = append(getCollectionsCheckpointIDs, arc.CheckpointID)
		}
	}

	msg, err := NewGetCollectionsMessage(GetCollectionsRequestBody{
		Collections:   getCollectionsKeyspaces.ScopeAndCollectionNames(),
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
			return nil, fmt.Errorf("peer does not have collection %q", getCollectionsKeyspaces[i])
		} else if checkpoint.LastSeq == "" {
			// collection valid but no checkpoint - start from sequence zero
			checkpoint.LastSeq = CreateZeroSinceValue().String()
		}

		dbCollection, err := arc.blipSyncContext.blipContextDb.GetDatabaseCollection(getCollectionsKeyspaces[i].ScopeName(), getCollectionsKeyspaces[i].CollectionName())
		if err != nil {
			return nil, err
		}

		blipSyncCollectionContexts[i] = &blipSyncCollectionContext{dbCollection: dbCollection}
		collectionCheckpoints[i] = *checkpoint

		arc.namedCollections[getCollectionsKeyspaces[i]] = &activeReplicatorCollection{collectionIdx: base.IntPtr(i), dataStore: dbCollection.dataStore}
	}

	arc.blipSyncContext.collections.set(blipSyncCollectionContexts)

	return collectionCheckpoints, nil
}

// forEachCollection runs the callback function for each collection on the replicator.
func (a *activeReplicatorCommon) forEachCollection(callback func(*activeReplicatorCollection) error) error {
	if a.config.CollectionsEnabled {
		for _, collection := range a.namedCollections {
			if err := callback(collection); err != nil {
				return err
			}
		}
	} else {
		if err := callback(a.defaultCollection); err != nil {
			return err
		}
	}
	return nil
}
