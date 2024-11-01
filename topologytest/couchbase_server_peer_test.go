// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

import (
	"context"
	"encoding/json"
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
	sourceID         string
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

// Context returns the context for the peer.
func (p *CouchbaseServerPeer) Context() context.Context {
	return base.TestCtx(p.tb)
}

func (p *CouchbaseServerPeer) getCollection(dsName sgbucket.DataStoreName) sgbucket.DataStore {
	collection, err := p.bucket.NamedDataStore(dsName)
	require.NoError(p.tb, err)
	return collection
}

// GetDocument returns the latest version of a document. The test will fail the document does not exist.
func (p *CouchbaseServerPeer) GetDocument(dsName sgbucket.DataStoreName, docID string) (rest.DocVersion, db.Body) {
	return getBodyAndVersion(p, p.getCollection(dsName), docID)
}

// CreateDocument creates a document on the peer. The test will fail if the document already exists.
func (p *CouchbaseServerPeer) CreateDocument(dsName sgbucket.DataStoreName, docID string, body []byte) rest.DocVersion {
	p.tb.Logf("%s: Creating document %s", p, docID)
	// create document with xattrs to prevent XDCR from doing a round trip replication in this scenario:
	// CBS1: write document (cas1, no _vv)
	// CBS1->CBS2: XDCR replication
	// CBS2->CBS1: XDCR replication, creates a new _vv
	cas, err := p.getCollection(dsName).WriteWithXattrs(p.Context(), docID, 0, 0, body, map[string][]byte{"userxattr": []byte(`{"dummy": "xattr"}`)}, nil, nil)
	require.NoError(p.tb, err)
	return rest.DocVersion{
		HLV: &db.HybridLogicalVector{
			SourceID:          p.SourceID(),
			Version:           cas,
			CurrentVersionCAS: cas,
		},
		HasImplicitCV: true,
	}
}

// WriteDocument writes a document to the peer. The test will fail if the write does not succeed.
func (p *CouchbaseServerPeer) WriteDocument(dsName sgbucket.DataStoreName, docID string, body []byte) rest.DocVersion {
	p.tb.Logf("%s: Writing document %s", p, docID)
	// write the document LWW, ignoring any in progress writes
	callback := func(_ []byte) (updated []byte, expiry *uint32, shouldDelete bool, err error) {
		return body, nil, false, nil
	}
	cas, err := p.getCollection(dsName).Update(docID, 0, callback)
	require.NoError(p.tb, err)
	return rest.DocVersion{
		// FIXME: this should actually probably show the HLV persisted, and then also the implicit CV
		HLV: &db.HybridLogicalVector{
			SourceID:          p.SourceID(),
			Version:           cas,
			CurrentVersionCAS: cas,
		},
		HasImplicitCV: true,
	}
}

// DeleteDocument deletes a document on the peer. The test will fail if the document does not exist.
func (p *CouchbaseServerPeer) DeleteDocument(dsName sgbucket.DataStoreName, docID string) rest.DocVersion {
	// delete the document, ignoring any in progress writes. We are allowed to delete a document that does not exist.
	callback := func(_ []byte) (updated []byte, expiry *uint32, shouldDelete bool, err error) {
		return nil, nil, true, nil
	}
	cas, err := p.getCollection(dsName).Update(docID, 0, callback)
	require.NoError(p.tb, err)
	return rest.DocVersion{
		HLV: &db.HybridLogicalVector{
			SourceID:          p.SourceID(),
			Version:           cas,
			CurrentVersionCAS: cas,
		},
		HasImplicitCV: true,
	}
}

// WaitForDocVersion waits for a document to reach a specific version. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseServerPeer) WaitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected rest.DocVersion) db.Body {
	var docBytes []byte
	var version rest.DocVersion
	require.EventuallyWithT(p.tb, func(c *assert.CollectT) {
		var err error
		var xattrs map[string][]byte
		var cas uint64
		docBytes, xattrs, cas, err = p.getCollection(dsName).GetWithXattrs(p.Context(), docID, []string{base.VvXattrName})
		if !assert.NoError(c, err) {
			return
		}
		// have to use p.tb instead of c because of the assert.CollectT doesn't implement TB
		version = getDocVersion(p, cas, xattrs)
		// assert.Equal(c, expected.GetLatestHLVVersion(), version.GetLatestHLVVersion(), "Could not find matching CV on %s for peer %s (sourceID:%s)\nexpected: %+v\nactual:   %+v", docID, p, p.SourceID(), expected, version)
		assert.Equal(c, expected.CV, version.CV, "Could not find matching CV on %s for peer %s (sourceID:%s)\nexpected: %+v\nactual:   %+v\n          body: %+v\n", docID, p, p.SourceID(), expected, version, string(docBytes))

	}, 5*time.Second, 100*time.Millisecond)
	p.tb.Logf("found version %+v for doc %s on %s", version, docID, p)
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
		assert.NoError(p.tb, r.Stop(p.Context()))
	}
	for _, r := range p.pushReplications {
		assert.NoError(p.tb, r.Stop(p.Context()))
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
		r, err := xdcr.NewXDCR(p.Context(), passivePeer.GetBackingBucket(), p.bucket, xdcr.XDCROptions{Mobile: xdcr.MobileOn})
		require.NoError(p.tb, err)
		p.pullReplications[passivePeer] = r

		return &CouchbaseServerReplication{
			activePeer:  p,
			passivePeer: passivePeer,
			t:           p.tb.(*testing.T),
			ctx:         p.Context(),
			manager:     r,
		}
	case PeerReplicationDirectionPush:
		_, ok := p.pushReplications[passivePeer]
		if ok {
			require.Fail(p.tb, fmt.Sprintf("pull replication already exists for %s-%s", p, passivePeer))
		}
		r, err := xdcr.NewXDCR(p.Context(), p.bucket, passivePeer.GetBackingBucket(), xdcr.XDCROptions{Mobile: xdcr.MobileOn})
		require.NoError(p.tb, err)
		p.pushReplications[passivePeer] = r
		return &CouchbaseServerReplication{
			activePeer:  p,
			passivePeer: passivePeer,
			t:           p.tb.(*testing.T),
			ctx:         p.Context(),
			manager:     r,
		}
	default:
		require.Fail(p.tb, fmt.Sprintf("unsupported replication direction %d for %s-%s", config.direction, p, passivePeer))
	}
	return nil
}

// SourceID returns the source ID for the peer used in <val>@sourceID.
func (p *CouchbaseServerPeer) SourceID() string {
	return p.sourceID
}

// GetBackingBucket returns the backing bucket for the peer.
func (p *CouchbaseServerPeer) GetBackingBucket() base.Bucket {
	return p.bucket
}

// TB returns the testing.TB for the peer.
func (p *CouchbaseServerPeer) TB() testing.TB {
	return p.tb
}

// getDocVersion returns a DocVersion from a cas and xattrs with _vv (hlv) and _sync (RevTreeID).
func getDocVersion(peer Peer, cas uint64, xattrs map[string][]byte) rest.DocVersion {
	docVersion := rest.DocVersion{
		Cas: cas,
	}
	mouBytes, ok := xattrs[base.MouXattrName]
	if ok {
		require.NoError(peer.TB(), json.Unmarshal(mouBytes, &docVersion.Mou))
	}
	hlvBytes, ok := xattrs[base.VvXattrName]
	if ok {
		require.NoError(peer.TB(), json.Unmarshal(hlvBytes, &docVersion.HLV))
		/*
			if docVersion.HLV.CurrentVersionCAS != cas && docVersion.Mou != nil {
				docVersion.CV = db.Version{SourceID: docVersion.HLV.SourceID, Value: base.HexCasToUint64(docVersion.Mou.PreviousCAS)}
			} else {
				docVersion.CV = db.Version{SourceID: docVersion.HLV.SourceID, Value: docVersion.HLV.Version}
			}
		*/
	} else {
		docVersion.HLV = &db.HybridLogicalVector{
			SourceID:          peer.SourceID(),
			Version:           cas,
			CurrentVersionCAS: cas,
		}
		docVersion.HasImplicitCV = true
	}
	sync, ok := xattrs[base.SyncXattrName]
	if ok {
		var syncData *db.SyncData
		require.NoError(peer.TB(), json.Unmarshal(sync, &syncData))
		docVersion.RevTreeID = syncData.CurrentRev
	}
	return docVersion
}

// getBodyAndVersion returns the body and version of a document from a sgbucket.DataStore.
func getBodyAndVersion(peer Peer, collection sgbucket.DataStore, docID string) (rest.DocVersion, db.Body) {
	docBytes, xattrs, cas, err := collection.GetWithXattrs(peer.Context(), docID, []string{base.VvXattrName})
	require.NoError(peer.TB(), err)
	// get hlv to construct DocVersion
	var body db.Body
	require.NoError(peer.TB(), base.JSONUnmarshal(docBytes, &body))
	return getDocVersion(peer, cas, xattrs), body
}
