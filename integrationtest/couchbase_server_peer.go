// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package integrationtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/xdcr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CouchbaseServerPeer represents an instance of a backing server (bucket). This is rosmar unless SG_TEST_BACKING_STORE=couchbase is set.
type CouchbaseServerPeer struct {
	tb               testing.TB
	bucket           base.Bucket
	pullReplications map[Peer]xdcr.Manager
	pushReplications map[Peer]xdcr.Manager
	name             string
}

// CouchbaseServerReplication represents a unidirectional replication between two CouchbaseServerPeers. These are two buckets, using bucket to bucket XDCR. A rosmar implementation is used if SG_TEST_BACKING_STORE is unset.
type CouchbaseServerReplication struct {
	t           testing.TB
	ctx         context.Context
	activePeer  Peer
	passivePeer Peer
	manager     xdcr.Manager
}

// ActivePeer returns the peer sending documents
func (r *CouchbaseServerReplication) ActivePeer() Peer {
	return r.activePeer
}

// PassivePeer returns the peer receiving documents
func (r *CouchbaseServerReplication) PassivePeer() Peer {
	return r.passivePeer
}

// Start starts the replication
func (r *CouchbaseServerReplication) Start() {
	require.NoError(r.t, r.manager.Start(r.ctx))
}

// Stop halts the replication. The replication can be restarted after it is stopped.
func (r *CouchbaseServerReplication) Stop() {
	require.NoError(r.t, r.manager.Stop(r.ctx))
}

func (p *CouchbaseServerPeer) String() string {
	return p.name
}

func (p *CouchbaseServerPeer) ctx() context.Context {
	return base.TestCtx(p.tb)
}

func (p *CouchbaseServerPeer) getCollection(dsName sgbucket.DataStoreName) sgbucket.DataStore {
	collection, err := p.bucket.NamedDataStore(dsName)
	require.NoError(p.tb, err)
	return collection
}

// GetDocument returns the latest version of a document. The test will fail the document does not exist.
func (p *CouchbaseServerPeer) GetDocument(dsName sgbucket.DataStoreName, docID string) (rest.DocVersion, db.Body) {
	docBytes, _, _, err := p.getCollection(dsName).GetWithXattrs(p.ctx(), docID, []string{base.SyncXattrName, base.VvXattrName})
	require.NoError(p.tb, err)
	// get hlv to construct DocVersion
	var body db.Body
	require.NoError(p.tb, base.JSONUnmarshal(docBytes, &body))
	return rest.EmptyDocVersion(), body
}

// WriteDocument writes a document to the peer. The test will fail if the write does not succeed.
func (p *CouchbaseServerPeer) WriteDocument(dsName sgbucket.DataStoreName, docID string, body []byte) rest.DocVersion {
	err := p.getCollection(dsName).Set(docID, 0, nil, body)
	require.NoError(p.tb, err)
	return rest.EmptyDocVersion()
}

// WaitForDocVersion waits for a document to reach a specific version. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseServerPeer) WaitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected rest.DocVersion) db.Body {
	var docBytes []byte
	require.EventuallyWithT(p.tb, func(c *assert.CollectT) {
		var err error
		docBytes, _, _, err = p.getCollection(dsName).GetWithXattrs(p.ctx(), docID, []string{base.SyncXattrName, base.VvXattrName})
		assert.NoError(c, err)
	}, 5*time.Second, 100*time.Millisecond)
	// get hlv to construct DocVersion
	var body db.Body
	require.NoError(p.tb, base.JSONUnmarshal(docBytes, &body), "couldn't unmarshal docID %s: %s", docID, docBytes)
	return body
}

// RequireDocNotFound asserts that a document does not exist on the peer.
func (p *CouchbaseServerPeer) RequireDocNotFound(dsName sgbucket.DataStoreName, docID string) {
	_, err := p.getCollection(dsName).Get(docID, nil)
	base.RequireDocNotFoundError(p.tb, err)
}

// Close will shut down the peer and close any active replications on the peer.
func (p *CouchbaseServerPeer) Close() {
	for _, r := range p.pullReplications {
		assert.NoError(p.tb, r.Stop(p.ctx()))
	}
	for _, r := range p.pushReplications {
		assert.NoError(p.tb, r.Stop(p.ctx()))
	}
}

// CreateReplication creates an XDCR manager.
func (p *CouchbaseServerPeer) CreateReplication(passivePeer Peer, config PeerReplicationConfig) PeerReplication {
	switch config.direction {
	case PeerReplicationDirectionPull:
		_, ok := p.pullReplications[passivePeer]
		if ok {
			require.Fail(p.tb, fmt.Sprintf("pull replication already exists for %s-%s", p, passivePeer))
		}
		r, err := xdcr.NewXDCR(p.ctx(), passivePeer.GetBackingBucket(), p.bucket, xdcr.XDCROptions{Mobile: xdcr.MobileOn})
		require.NoError(p.tb, err)
		p.pullReplications[passivePeer] = r

		return &CouchbaseServerReplication{
			activePeer:  p,
			passivePeer: passivePeer,
			t:           p.tb.(*testing.T),
			ctx:         p.ctx(),
			manager:     r,
		}
	case PeerReplicationDirectionPush:
		_, ok := p.pushReplications[passivePeer]
		if ok {
			require.Fail(p.tb, fmt.Sprintf("pull replication already exists for %s-%s", p, passivePeer))
		}
		r, err := xdcr.NewXDCR(p.ctx(), p.bucket, passivePeer.GetBackingBucket(), xdcr.XDCROptions{Mobile: xdcr.MobileOn})
		require.NoError(p.tb, err)
		p.pushReplications[passivePeer] = r
		return &CouchbaseServerReplication{
			activePeer:  p,
			passivePeer: passivePeer,
			t:           p.tb.(*testing.T),
			ctx:         p.ctx(),
			manager:     r,
		}
	default:
		require.Fail(p.tb, fmt.Sprintf("unsupported replication direction %d for %s-%s", config.direction, p, passivePeer))
	}
	return nil
}

// GetBackingBucket returns the backing bucket for the peer.
func (p *CouchbaseServerPeer) GetBackingBucket() base.Bucket {
	return p.bucket
}
