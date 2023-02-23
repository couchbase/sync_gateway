// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"fmt"
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

// blipSyncCollectionContext stores information about a single collection for a BlipSyncContext
type blipSyncCollectionContext struct {
	dbCollection     *DatabaseCollection
	activeSubChanges base.AtomicBool // Flag for whether there is a subChanges subscription currently active.  Atomic access
}

// blipCollections is a container for all collections blip is aware of.
type blipCollections struct {
	nonCollectionAwareContext *blipSyncCollectionContext   // A collection represented by no Collection property message or prior GetCollections message.
	collectionContexts        []*blipSyncCollectionContext // Indexed by replication collectionIdx to store per-collection information on a replication
	sync.RWMutex
}

// setNonCollectionAware adds a single collection matching _default._default collection, to be refered to if no Collection property is set on a blip message.
func (b *blipCollections) setNonCollectionAware(collectionCtx *blipSyncCollectionContext) {
	b.Lock()
	defer b.Unlock()
	if b.nonCollectionAwareContext == nil {
		b.nonCollectionAwareContext = collectionCtx
	}
}

// set adds a set of collections to this contexts struct.
func (b *blipCollections) set(collectionCtxs []*blipSyncCollectionContext) {
	b.Lock()
	defer b.Unlock()
	b.collectionContexts = collectionCtxs
}

// getCollectionContext returns a collection matching the blip collection idx set by the initial GetCollections handshake. If collectionIdx is nil, assume that the messages are not collection aware.
func (b *blipCollections) get(collectionIdx *int) (*blipSyncCollectionContext, error) {
	b.RLock()
	defer b.RUnlock()
	if collectionIdx == nil {
		if b.nonCollectionAwareContext == nil {
			return nil, fmt.Errorf("No default collection has been specified")
		}
		return b.nonCollectionAwareContext, nil
	}
	if len(b.collectionContexts) <= *collectionIdx {
		return nil, fmt.Errorf("Collection index %d is outside range indexes set by GetCollections", *collectionIdx)
	}
	if b.collectionContexts[*collectionIdx] == nil {
		return nil, fmt.Errorf("Collection index %d was not a valid collection set by GetCollections", *collectionIdx)
	}
	return b.collectionContexts[*collectionIdx], nil
}

// getAll returns all collection contexts.
func (b *blipCollections) getAll() []*blipSyncCollectionContext {
	b.RLock()
	defer b.RUnlock()
	var collections []*blipSyncCollectionContext
	if b.nonCollectionAwareContext != nil {
		collections = append(collections, b.nonCollectionAwareContext)
	}
	collections = append(collections, b.collectionContexts...)
	return collections
}

// hasNamedCollections returns true if named collections have been set.
func (b *blipCollections) hasNamedCollections() bool {
	b.RLock()
	defer b.RUnlock()
	return len(b.collectionContexts) != 0
}
