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
	"sync/atomic"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/xdcr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dummySystemXattr is created for XDCR testing. This prevents a document echo after an initial write. The dummy xattr also means that the document will always have xattrs when deleting it, which is necessary for WriteUpdateWithXattrs.
const dummySystemXattr = "_dummyxattr"

var metadataXattrNames = []string{base.VvXattrName, base.MouXattrName, base.SyncXattrName, dummySystemXattr}

// CouchbaseServerPeer represents an instance of a backing server (bucket). This is rosmar unless SG_TEST_BACKING_STORE=couchbase is set.
type CouchbaseServerPeer struct {
	t                  atomic.Pointer[testing.T]
	bucket             base.Bucket
	sourceID           string
	pullReplications   map[Peer]xdcr.Manager
	pushReplications   map[Peer]xdcr.Manager
	name               string
	symmetricRedundant bool
}

// CouchbaseServerReplication represents a unidirectional replication between two CouchbaseServerPeers. These are two buckets, using bucket to bucket XDCR. A rosmar implementation is used if SG_TEST_BACKING_STORE is unset.
type CouchbaseServerReplication struct {
	t           testing.TB
	ctx         context.Context
	activePeer  Peer
	passivePeer Peer
	direction   PeerReplicationDirection
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
	r.t.Logf("starting XDCR replication %s", r)
	require.NoError(r.t, r.manager.Start(r.ctx))
}

// Stop halts the replication. The replication can be restarted after it is stopped.
func (r *CouchbaseServerReplication) Stop() {
	r.t.Logf("stopping XDCR replication %s", r)
	require.NoError(r.t, r.manager.Stop(r.ctx))
}

func (r *CouchbaseServerReplication) String() string {
	switch r.direction {
	case PeerReplicationDirectionPush:
		return fmt.Sprintf("%s->%s", r.activePeer, r.passivePeer)
	case PeerReplicationDirectionPull:
		return fmt.Sprintf("%s->%s", r.passivePeer, r.activePeer)
	}
	return fmt.Sprintf("%s-%s (direction unknown)", r.activePeer, r.passivePeer)
}

func (r *CouchbaseServerReplication) Stats() string {
	stats, err := r.manager.Stats(r.ctx)
	if err != nil {
		return fmt.Sprintf("error getting stats: %v", err)
	}
	return fmt.Sprintf("%+v", *stats)
}

func (p *CouchbaseServerPeer) String() string {
	return fmt.Sprintf("%s (bucket:%s,sourceid:%s)", p.name, p.bucket.GetName(), p.sourceID)
}

// Context returns the context for the peer.
func (p *CouchbaseServerPeer) Context() context.Context {
	return base.TestCtx(p.TB())
}

func (p *CouchbaseServerPeer) getCollection(dsName sgbucket.DataStoreName) sgbucket.DataStore {
	collection, err := p.bucket.NamedDataStore(dsName)
	require.NoError(p.TB(), err)
	return collection
}

// GetDocument returns the latest version of a document. The test will fail the document does not exist.
func (p *CouchbaseServerPeer) GetDocument(dsName sgbucket.DataStoreName, docID string) (DocMetadata, db.Body) {
	return getBodyAndVersion(p, p.getCollection(dsName), docID)
}

// GetDocument returns the latest version of a document. The test will fail the document does not exist.
func (p *CouchbaseServerPeer) GetDocumentIfExists(dsName sgbucket.DataStoreName, docID string) (meta DocMetadata, body *db.Body, exists bool) {
	return getBodyAndVersionIfExists(p, p.getCollection(dsName), docID)
}

// CreateDocument creates a document on the peer. The test will fail if the document already exists.
func (p *CouchbaseServerPeer) CreateDocument(dsName sgbucket.DataStoreName, docID string, body []byte) BodyAndVersion {
	// create document with xattrs to prevent XDCR from doing a round trip replication in this scenario:
	// CBS1: write document (cas1, no _vv)
	// CBS1->CBS2: XDCR replication
	// CBS2->CBS1: XDCR replication, creates a new _vv
	cas, err := p.getCollection(dsName).WriteWithXattrs(p.Context(), docID, 0, 0, body, map[string][]byte{dummySystemXattr: []byte(`{"dummy": "xattr"}`)}, nil, nil)
	require.NoError(p.TB(), err)
	implicitHLV := db.NewHybridLogicalVector()
	require.NoError(p.TB(), implicitHLV.AddVersion(db.Version{SourceID: p.SourceID(), Value: cas}))
	docMetadata := DocMetadata{
		DocID:       docID,
		Cas:         cas,
		ImplicitHLV: implicitHLV,
	}
	p.TB().Logf("%s: Created document %s with %#v", p, docID, docMetadata)
	return BodyAndVersion{
		docMeta:    docMetadata,
		body:       body,
		updatePeer: p.name,
	}
}

// WriteDocument writes a document to the peer. The test will fail if the write does not succeed.
func (p *CouchbaseServerPeer) WriteDocument(dsName sgbucket.DataStoreName, docID string, body []byte) BodyAndVersion {
	var lastXattrs map[string][]byte
	// write the document LWW, ignoring any in progress writes
	callback := func(existingBody []byte, xattrs map[string][]byte, _ uint64) (sgbucket.UpdatedDoc, error) {
		doc := sgbucket.UpdatedDoc{Doc: body}
		// only set lastXattrs if existing document is not a tombstone, they will not be preserved if this is a resurrection
		if len(existingBody) > 0 {
			lastXattrs = xattrs
		} else {
			// create resurrection document with xattrs to prevent XDCR from doing a round trip replication when one peer has a _vv xattr and the other does not.
			doc.Xattrs = map[string][]byte{
				dummySystemXattr: []byte(`{"dummy": "xattr"}`),
			}
		}
		return doc, nil
	}
	cas, err := p.getCollection(dsName).WriteUpdateWithXattrs(p.Context(), docID, metadataXattrNames, 0, nil, nil, callback)
	require.NoError(p.TB(), err)
	docMeta := getDocVersion(docID, p, cas, lastXattrs)
	p.TB().Logf("%s: Wrote document %s with %#+v", p, docID, docMeta)
	return BodyAndVersion{
		docMeta:    docMeta,
		body:       body,
		updatePeer: p.name,
	}
}

// DeleteDocument deletes a document on the peer. The test will fail if the document does not exist.
func (p *CouchbaseServerPeer) DeleteDocument(dsName sgbucket.DataStoreName, docID string) DocMetadata {
	// delete the document, ignoring any in progress writes. We are allowed to delete a document that does not exist.
	var lastXattrs map[string][]byte
	// write the document LWW, ignoring any in progress writes
	callback := func(_ []byte, xattrs map[string][]byte, _ uint64) (sgbucket.UpdatedDoc, error) {
		lastXattrs = xattrs
		return sgbucket.UpdatedDoc{Doc: nil, IsTombstone: true, Xattrs: xattrs}, nil
	}
	cas, err := p.getCollection(dsName).WriteUpdateWithXattrs(p.Context(), docID, metadataXattrNames, 0, nil, nil, callback)
	require.NoError(p.TB(), err)
	version := getDocVersion(docID, p, cas, lastXattrs)
	p.TB().Logf("%s: Deleted document %s with %#+v", p, docID, version)
	return version
}

// WaitForDocVersion waits for a document to reach a specific version. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseServerPeer) WaitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, topology Topology) db.Body {
	docBytes := p.waitForDocVersion(dsName, docID, expected, topology)
	var body db.Body
	require.NoError(p.TB(), base.JSONUnmarshal(docBytes, &body), "couldn't unmarshal docID %s: %s", docID, docBytes)
	return body
}

// WaitForCV waits for a document to reach a specific CV. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseServerPeer) WaitForCV(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, topology Topology) db.Body {
	docBytes := p.waitForCV(dsName, docID, expected, topology)
	var body db.Body
	require.NoError(p.TB(), base.JSONUnmarshal(docBytes, &body), "couldn't unmarshal docID %s: %s", docID, docBytes)
	return body
}

// WaitForTombstoneVersion waits for a document to reach a specific version, this must be a tombstone. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseServerPeer) WaitForTombstoneVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, topology Topology) {
	docBytes := p.waitForDocVersion(dsName, docID, expected, topology)
	require.Empty(p.TB(), docBytes, "expected tombstone for docID %s, got %s.%s", docID, docBytes, topology)
}

// waitForDocVersion waits for a document to reach a specific version and returns the body in bytes. The bytes will be nil if the document is a tombstone. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseServerPeer) waitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, topology Topology) []byte {
	var docBytes []byte
	var version DocMetadata
	require.EventuallyWithT(p.TB(), func(c *assert.CollectT) {
		var err error
		var xattrs map[string][]byte
		var cas uint64
		docBytes, xattrs, cas, err = p.getCollection(dsName).GetWithXattrs(p.Context(), docID, metadataXattrNames)
		if !assert.NoError(c, err) {
			return
		}
		version = getDocVersion(docID, p, cas, xattrs)
		assertHLVEqual(c, dsName, docID, p.name, version, docBytes, expected, topology)
	}, totalWaitTime, pollInterval)
	return docBytes
}

// waitForCV waits for a document to reach a specific CV and returns the body in bytes. The bytes will be nil if the document is a tombstone. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseServerPeer) waitForCV(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, topology Topology) []byte {
	var docBytes []byte
	var version DocMetadata
	require.EventuallyWithT(p.TB(), func(c *assert.CollectT) {
		var err error
		var xattrs map[string][]byte
		var cas uint64
		docBytes, xattrs, cas, err = p.getCollection(dsName).GetWithXattrs(p.Context(), docID, metadataXattrNames)
		if !assert.NoError(c, err) {
			return
		}
		version = getDocVersion(docID, p, cas, xattrs)
		assertCVEqual(c, dsName, docID, p.name, version, docBytes, expected, topology)
	}, totalWaitTime, pollInterval)
	return docBytes
}

// Close will shut down the peer and close any active replications on the peer.
func (p *CouchbaseServerPeer) Close() {
	for _, r := range p.pullReplications {
		assert.NoError(p.TB(), r.Stop(p.Context()))
	}
	for _, r := range p.pushReplications {
		assert.NoError(p.TB(), r.Stop(p.Context()))
	}
}

func (p *CouchbaseServerPeer) GetReplications() map[Peer]xdcr.Manager {
	return nil
}

// Type returns PeerTypeCouchbaseServer.
func (p *CouchbaseServerPeer) Type() PeerType {
	return PeerTypeCouchbaseServer
}

// IsSymmetricRedundant returns true if there is another peer set up that is identical to this one, and this peer doesn't need to participate in unique actions.
func (p *CouchbaseServerPeer) IsSymmetricRedundant() bool {
	return p.symmetricRedundant
}

// CreateReplication creates an XDCR manager.
func (p *CouchbaseServerPeer) CreateReplication(passivePeer Peer, config PeerReplicationConfig) PeerReplication {
	switch config.direction {
	case PeerReplicationDirectionPull:
		_, ok := p.pullReplications[passivePeer]
		if ok {
			require.Fail(p.TB(), fmt.Sprintf("pull replication already exists for %s-%s", p, passivePeer))
		}
		r, err := xdcr.NewXDCR(p.Context(), passivePeer.GetBackingBucket(), p.bucket, xdcr.XDCROptions{Mobile: xdcr.MobileOn})
		require.NoError(p.TB(), err)
		p.pullReplications[passivePeer] = r
		return &CouchbaseServerReplication{
			activePeer:  p,
			passivePeer: passivePeer,
			direction:   config.direction,
			t:           p.TB().(*testing.T),
			ctx:         p.Context(),
			manager:     r,
		}
	case PeerReplicationDirectionPush:
		_, ok := p.pushReplications[passivePeer]
		if ok {
			require.Fail(p.TB(), fmt.Sprintf("pull replication already exists for %s-%s", p, passivePeer))
		}
		r, err := xdcr.NewXDCR(p.Context(), p.bucket, passivePeer.GetBackingBucket(), xdcr.XDCROptions{Mobile: xdcr.MobileOn})
		require.NoError(p.TB(), err)
		p.pushReplications[passivePeer] = r
		return &CouchbaseServerReplication{
			activePeer:  p,
			passivePeer: passivePeer,
			direction:   config.direction,
			t:           p.TB().(*testing.T),
			ctx:         p.Context(),
			manager:     r,
		}
	default:
		require.Fail(p.TB(), fmt.Sprintf("unsupported replication direction %d for %s-%s", config.direction, p, passivePeer))
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
	return p.t.Load()
}

func (p *CouchbaseServerPeer) UpdateTB(t *testing.T) {
	p.t.Store(t)
}

// useImplicitHLV returns true if the document's HLV is not up to date and an HLV should be composed of current sourceID and cas.
func useImplicitHLV(doc DocMetadata) bool {
	if doc.HLV == nil {
		return true
	}
	if doc.HLV.CurrentVersionCAS == doc.Cas {
		return false
	}
	if doc.Mou == nil {
		return true
	}
	return doc.Mou.CAS() != doc.Cas
}

// getDocVersion returns a DocVersion from a cas and xattrs with _vv (hlv) and _sync (RevTreeID).
func getDocVersion(docID string, peer Peer, cas uint64, xattrs map[string][]byte) DocMetadata {
	docVersion := DocMetadata{
		DocID: docID,
		Cas:   cas,
	}
	mouBytes, ok := xattrs[base.MouXattrName]
	if ok {
		require.NoError(peer.TB(), json.Unmarshal(mouBytes, &docVersion.Mou))
	}
	hlvBytes, ok := xattrs[base.VvXattrName]
	if ok {
		require.NoError(peer.TB(), json.Unmarshal(hlvBytes, &docVersion.HLV))
	}
	if useImplicitHLV(docVersion) {
		if docVersion.HLV == nil {
			docVersion.ImplicitHLV = db.NewHybridLogicalVector()
		} else {
			require.NoError(peer.TB(), json.Unmarshal(hlvBytes, &docVersion.ImplicitHLV))
		}
		require.NoError(peer.TB(), docVersion.ImplicitHLV.AddVersion(db.Version{SourceID: peer.SourceID(), Value: cas}))
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
func getBodyAndVersion(peer Peer, collection sgbucket.DataStore, docID string) (DocMetadata, db.Body) {
	docBytes, xattrs, cas, err := collection.GetWithXattrs(peer.Context(), docID, metadataXattrNames)
	require.NoError(peer.TB(), err)
	// get hlv to construct DocVersion
	var body db.Body
	require.NoError(peer.TB(), base.JSONUnmarshal(docBytes, &body))
	return getDocVersion(docID, peer, cas, xattrs), body
}

// getBodyAndVersionIfExists returns the body and version of a document from a sgbucket.DataStore.
func getBodyAndVersionIfExists(peer Peer, collection sgbucket.DataStore, docID string) (meta DocMetadata, body *db.Body, exists bool) {
	docBytes, xattrs, cas, err := collection.GetWithXattrs(peer.Context(), docID, metadataXattrNames)
	if base.IsDocNotFoundError(err) {
		return DocMetadata{}, nil, false
	}
	require.NoError(peer.TB(), err)
	meta = getDocVersion(docID, peer, cas, xattrs)
	if len(docBytes) == 0 {
		// document is a tombstone, return empty body
		return meta, nil, true
	}
	// get hlv to construct DocVersion
	require.NoError(peer.TB(), base.JSONUnmarshal(docBytes, &body))
	return meta, body, true
}
