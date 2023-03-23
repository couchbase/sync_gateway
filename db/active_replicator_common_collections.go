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

	"github.com/couchbase/go-blip"
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

// validateCollectionsConfig validates the collections config for the active replicator.
func (arc *activeReplicatorCommon) validateCollectionsConfig() error {
	// ensure remote collection set is the same length as local collection set
	if remoteLen := len(arc.config.CollectionsRemote); remoteLen > 0 {
		if localLen := len(arc.config.CollectionsLocal); localLen != remoteLen {
			return fmt.Errorf("local and remote collections must be the same length... had %d and %d", localLen, remoteLen)
		}
	}

	// ensure channel filter set is the same length as local collection set
	if collectionsChannelFilterLen := len(arc.config.CollectionsChannelFilter); collectionsChannelFilterLen > 0 {
		if localLen := len(arc.config.CollectionsLocal); localLen != collectionsChannelFilterLen {
			return fmt.Errorf("local collections and channel filter set must be the same length... had %d and %d", localLen, collectionsChannelFilterLen)
		}
		if channelFilterLen := len(arc.config.FilterChannels); channelFilterLen > 0 {
			return fmt.Errorf("channel filter and collection channel filter set cannot both be set")
		}
	}

	return nil
}

// buildGetCollectionsMessage returns a GetCollections BLIP message for the given collection names.
func (arc *activeReplicatorCommon) buildGetCollectionsMessage(remoteCollectionsKeyspaces base.ScopeAndCollectionNames) (*blip.Message, error) {
	getCollectionsCheckpointIDs := make([]string, 0, len(remoteCollectionsKeyspaces))
	for i := 0; i < len(remoteCollectionsKeyspaces); i++ {
		getCollectionsCheckpointIDs = append(getCollectionsCheckpointIDs, arc.CheckpointID)
	}

	return NewGetCollectionsMessage(GetCollectionsRequestBody{
		Collections:   remoteCollectionsKeyspaces.ScopeAndCollectionNames(),
		CheckpointIDs: getCollectionsCheckpointIDs,
	})
}

// buildCollectionsSetWithExplicitMappings returns a list of local collection names, and remote collection names according to any explicit mappings set.
func (arc *activeReplicatorCommon) buildCollectionsSetWithExplicitMappings() (localCollectionsKeyspaces, remoteCollectionsKeyspaces base.ScopeAndCollectionNames, err error) {
	if arc.config.CollectionsLocal != nil {
		for i, localScopeAndCollection := range arc.config.CollectionsLocal {
			localScopeAndCollectionName, err := getScopeAndCollectionName(localScopeAndCollection)
			if err != nil {
				return nil, nil, err
			}
			localCollectionsKeyspaces = append(localCollectionsKeyspaces, localScopeAndCollectionName)

			// remap collection name to remote if set
			if len(arc.config.CollectionsRemote) > 0 && arc.config.CollectionsRemote[i] != "" {
				base.DebugfCtx(arc.ctx, base.KeyReplicate, "Mapping local %q to remote %q", localScopeAndCollection, arc.config.CollectionsRemote[i])
				remoteScopeAndCollectionName, err := getScopeAndCollectionName(arc.config.CollectionsRemote[i])
				if err != nil {
					return nil, nil, err
				}
				remoteCollectionsKeyspaces = append(remoteCollectionsKeyspaces, remoteScopeAndCollectionName)
			} else {
				// no mapping set - use local collection name
				remoteCollectionsKeyspaces = append(remoteCollectionsKeyspaces, localScopeAndCollectionName)
			}
		}
	} else {
		// collections to replicate wasn't set - so build a full set based on local database
		for _, dbCollection := range arc.blipSyncContext.blipContextDb.CollectionByID {
			localCollectionsKeyspaces = append(localCollectionsKeyspaces, base.ScopeAndCollectionName{Scope: dbCollection.ScopeName, Collection: dbCollection.Name})
			remoteCollectionsKeyspaces = append(remoteCollectionsKeyspaces, base.ScopeAndCollectionName{Scope: dbCollection.ScopeName, Collection: dbCollection.Name})
		}
	}

	return localCollectionsKeyspaces, remoteCollectionsKeyspaces, nil
}

// _initCollections will negotiate the set of collections with the peer using GetCollections and returns the set of checkpoints for each of them.
func (arc *activeReplicatorCommon) _initCollections() ([]replicationCheckpoint, error) {

	if err := arc.validateCollectionsConfig(); err != nil {
		return nil, err
	}

	localCollectionsKeyspaces, remoteCollectionsKeyspaces, err := arc.buildCollectionsSetWithExplicitMappings()
	if err != nil {
		return nil, err
	}

	msg, err := arc.buildGetCollectionsMessage(remoteCollectionsKeyspaces)
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
		return nil, fmt.Errorf("couldn't unmarshal response body: %q: %w", rBody, err)
	}

	blipSyncCollectionContexts := make([]*blipSyncCollectionContext, len(resp))
	collectionCheckpoints := make([]replicationCheckpoint, len(resp))

	for i, checkpointBody := range resp {
		// json-iterator handling for nil json.RawMessage (cannot unmarshal nil)
		if checkpointBody == nil {
			return nil, fmt.Errorf("peer does not have collection %q", remoteCollectionsKeyspaces[i])
		}

		var checkpoint *replicationCheckpoint
		err = base.JSONUnmarshal(checkpointBody, &checkpoint)
		if err != nil {
			return nil, fmt.Errorf("couldn't unmarshal checkpoint body: %q: %w", checkpointBody, err)
		}

		// stdlib json handling for explicit "null" json.RawMessage
		if checkpoint == nil {
			return nil, fmt.Errorf("peer does not have collection %q", remoteCollectionsKeyspaces[i])
		}

		if checkpoint.LastSeq == "" {
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

// getFilteredChannels returns the filtered channels.
// collectionIdx can be nil if replicating without collections enabled.
func (config ActiveReplicatorConfig) getFilteredChannels(collectionIdx *int) []string {
	if collectionIdx != nil {
		if len(config.CollectionsChannelFilter)-1 >= *collectionIdx {
			return config.CollectionsChannelFilter[*collectionIdx]
		}
	}
	return config.FilterChannels
}
