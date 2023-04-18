// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"context"
	"fmt"
	"sync"

	"github.com/couchbase/sync_gateway/base"
)

// blipSyncCollectionContext stores information about a single collection for a BlipSyncContext
type blipSyncCollectionContext struct {
	dbCollection          *DatabaseCollection
	activeSubChanges      base.AtomicBool // Flag for whether there is a subChanges subscription currently active.  Atomic access
	changesCtxLock        sync.Mutex
	changesCtx            context.Context    // Used for the unsub changes Blip message to check if the subChanges feed should stop
	changesCtxCancel      context.CancelFunc // Cancel function for changesCtx to cancel subChanges being sent
	pendingInsertionsLock sync.Mutex
	pendingInsertions     base.Set // DocIDs from handleProposeChanges that aren't in the db

	sgr2PullAddExpectedSeqsCallback  func(expectedSeqs map[IDAndRev]SequenceID)     // sgr2PullAddExpectedSeqsCallback is called after successfully handling an incoming changes message
	sgr2PullProcessedSeqCallback     func(remoteSeq *SequenceID, idAndRev IDAndRev) // sgr2PullProcessedSeqCallback is called after successfully handling an incoming rev message
	sgr2PullAlreadyKnownSeqsCallback func(alreadyKnownSeqs ...SequenceID)           // sgr2PullAlreadyKnownSeqsCallback is called to mark the sequences as being immediately processed
	sgr2PushAddExpectedSeqsCallback  func(expectedSeqs ...SequenceID)               // sgr2PushAddExpectedSeqsCallback is called after sync gateway has sent a revision, but is still awaiting an acknowledgement
	sgr2PushProcessedSeqCallback     func(remoteSeq SequenceID)                     // sgr2PushProcessedSeqCallback is called after receiving acknowledgement of a sent revision
	sgr2PushAlreadyKnownSeqsCallback func(alreadyKnownSeqs ...SequenceID)           // sgr2PushAlreadyKnownSeqsCallback is called to mark the sequence as being immediately processed
	emptyChangesMessageCallback      func()                                         // emptyChangesMessageCallback is called when an empty changes message is received

}

// blipCollections is a container for all collections blip is aware of.
type blipCollections struct {
	nonCollectionAwareContext *blipSyncCollectionContext   // A collection represented by no Collection property message or prior GetCollections message.
	collectionContexts        []*blipSyncCollectionContext // Indexed by replication collectionIdx to store per-collection information on a replication
	sync.RWMutex
}

// newBlipSyncCollection constructs a context to hold all blip data for a given collection.
func newBlipSyncCollectionContext(dbCollection *DatabaseCollection) *blipSyncCollectionContext {
	c := &blipSyncCollectionContext{
		dbCollection: dbCollection,
	}
	c.changesCtx, c.changesCtxCancel = context.WithCancel(context.Background())
	return c
}

// Remembers a docID that doesn't exist in the collection at the time handleProposeChanges ran.
func (bsc *blipSyncCollectionContext) notePendingInsertion(docID string) {
	bsc.pendingInsertionsLock.Lock() // TODO: Rename this lock?
	defer bsc.pendingInsertionsLock.Unlock()
	if bsc.pendingInsertions == nil {
		bsc.pendingInsertions = base.Set{}
	}
	bsc.pendingInsertions.Add(docID)
}

// True if this docID was known not to exist in the collection when handleProposeChanges ran.
// (If so, this fn also forgets the docID, so any subsequent call will return false.)
func (bsc *blipSyncCollectionContext) checkPendingInsertion(docID string) (found bool) {
	bsc.pendingInsertionsLock.Lock()
	defer bsc.pendingInsertionsLock.Unlock()
	if found = bsc.pendingInsertions.Contains(docID); found {
		delete(bsc.pendingInsertions, docID)
	}
	return
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
