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
	"fmt"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PeerBlipTesterClient is a wrapper around a BlipTesterClientRunner and BlipTesterClient, which need to match for a given Couchbase Lite interface.
type PeerBlipTesterClient struct {
	btcRunner *rest.BlipTestClientRunner
	btc       *rest.BlipTesterClient
}

// ID returns the unique ID of the blip client.
func (p *PeerBlipTesterClient) ID() uint32 {
	return p.btc.ID()
}

func (p *PeerBlipTesterClient) CollectionClient(dsName sgbucket.DataStoreName) *rest.BlipTesterCollectionClient {
	return p.btcRunner.Collection(p.ID(), fmt.Sprintf("%s.%s", dsName.ScopeName(), dsName.CollectionName()))
}

// CouchbaseLiteMockPeer represents an in-memory Couchbase Lite peer. This utilizes BlipTesterClient from the rest package to send and receive blip messages.
type CouchbaseLiteMockPeer struct {
	t                  *testing.T
	blipClients        map[string]*PeerBlipTesterClient
	name               string
	symmetricRedundant bool // there is another peer that is symmetric to this one
}

func (p *CouchbaseLiteMockPeer) String() string {
	return fmt.Sprintf("%s (sourceid:%s)", p.name, p.SourceID())
}

// getLatestDocVersion returns the latest body and version of a document. If the document does not exist, it will return nil.
func (p *CouchbaseLiteMockPeer) getLatestDocVersion(dsName sgbucket.DataStoreName, docID string) ([]byte, *DocMetadata) {
	client := p.getSingleSGBlipClient().CollectionClient(dsName)
	body, hlv, version := client.GetDoc(docID)
	if version == nil {
		return nil, nil
	}
	meta := DocMetadataFromDocVersion(p.TB(), docID, hlv, *version)
	meta.HLV = hlv
	return body, &meta
}

// GetDocument returns the latest version of a document. The test will fail the document does not exist.
func (p *CouchbaseLiteMockPeer) GetDocument(dsName sgbucket.DataStoreName, docID string) (DocMetadata, db.Body) {
	bodyBytes, meta := p.getLatestDocVersion(dsName, docID)
	require.NotNil(p.TB(), meta, "docID:%s not found on %s", docID, p)
	var body db.Body
	// it's easier if all clients can return consistent bodies for tombstones
	// lets just settle on nil, since we still need special handling anyway for `` vs `{}` so unmarshal doesn't barf
	if len(bodyBytes) > 0 && string(bodyBytes) != base.EmptyDocument {
		require.NoError(p.TB(), base.JSONUnmarshal(bodyBytes, &body))
	}
	return *meta, body
}

// getSingleSGBlipClient returns the single blip client for the peer. If there are multiple clients, or no clients it will fail the test. This is temporary to stub support for multiple Sync Gateway peers, see CBG-4433.
func (p *CouchbaseLiteMockPeer) getSingleSGBlipClient() *PeerBlipTesterClient {
	// couchbase lite peer can't exist separately from sync gateway peer, CBG-4433
	require.Len(p.TB(), p.blipClients, 1, "blipClients haven't been created for %s, a temporary limitation of CouchbaseLiteMockPeer", p)
	for _, c := range p.blipClients {
		return c
	}
	require.Fail(p.t, "no blipClients found for %s", p)
	return nil
}

// CreateDocument creates a document on the peer. The test will fail if the document already exists.
func (p *CouchbaseLiteMockPeer) CreateDocument(dsName sgbucket.DataStoreName, docID string, body []byte) BodyAndVersion {
	client := p.getSingleSGBlipClient().CollectionClient(dsName)
	docVersion, hlv := client.AddHLVRev(docID, rest.EmptyDocVersion(), body)
	docMetadata := DocMetadataFromDocVersion(p.TB(), docID, hlv, docVersion)
	p.TB().Logf("%s: Created document %s with %#v", p, docID, docMetadata)
	return BodyAndVersion{
		docMeta:    docMetadata,
		body:       body,
		updatePeer: p.name,
	}
}

// WriteDocument writes a document to the peer. The test will fail if the write does not succeed.
func (p *CouchbaseLiteMockPeer) WriteDocument(dsName sgbucket.DataStoreName, docID string, body []byte) BodyAndVersion {
	client := p.getSingleSGBlipClient().CollectionClient(dsName)
	_, parentMeta := p.getLatestDocVersion(dsName, docID)
	parentVersion := rest.EmptyDocVersion()
	if parentMeta != nil {
		parentVersion = &db.DocVersion{CV: parentMeta.CV(p.TB())}
	}
	docVersion, hlv := client.AddHLVRev(docID, parentVersion, body)
	docMetadata := DocMetadataFromDocVersion(p.TB(), docID, hlv, docVersion)
	p.TB().Logf("%s: Wrote document %s with %#+v", p, docID, docMetadata)
	return BodyAndVersion{
		docMeta:    docMetadata,
		body:       body,
		updatePeer: p.name,
	}
}

// DeleteDocument deletes a document on the peer. The test will fail if the document does not exist.
func (p *CouchbaseLiteMockPeer) DeleteDocument(dsName sgbucket.DataStoreName, docID string) DocMetadata {
	client := p.getSingleSGBlipClient().CollectionClient(dsName)
	_, parentMeta := p.getLatestDocVersion(dsName, docID)
	parentVersion := rest.EmptyDocVersion()
	if parentMeta != nil {
		parentVersion = &db.DocVersion{CV: parentMeta.CV(p.TB())}
	}
	docVersion, hlv := client.Delete(docID, parentVersion)
	docMeta := DocMetadataFromDocVersion(p.TB(), docID, hlv, docVersion)
	p.TB().Logf("%s: Deleted document %s with %#+v", p, docID, docMeta)
	return docMeta
}

// WaitForDocVersion waits for a document to reach a specific version. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseLiteMockPeer) WaitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, replications Replications) db.Body {
	var data []byte
	require.EventuallyWithT(p.TB(), func(c *assert.CollectT) {
		var actual *DocMetadata
		data, actual = p.getLatestDocVersion(dsName, docID)
		if !assert.NotNil(c, actual, "Could not find docID:%+v on %p\nVersion %#v", docID, p, expected) {
			return
		}
		assertHLVEqual(c, docID, p.name, *actual, data, expected, replications)
	}, totalWaitTime, pollInterval)
	var body db.Body
	require.NoError(p.TB(), base.JSONUnmarshal(data, &body))
	return body
}

// WaitForTombstoneVersion waits for a document to reach a specific version, this must be a tombstone. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseLiteMockPeer) WaitForTombstoneVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, replications Replications) {
	client := p.getSingleSGBlipClient().CollectionClient(dsName)
	expectedVersion := db.DocVersion{CV: expected.CV(p.TB())}
	require.EventuallyWithT(p.TB(), func(c *assert.CollectT) {
		isTombstone, err := client.IsVersionTombstone(docID, expectedVersion)
		require.NoError(c, err)
		assert.True(c, isTombstone, "expected docID %s on peer %s to be deleted. Replications:\n%s", docID, p, replications.Stats())
	}, totalWaitTime, pollInterval)
}

// Close will shut down the peer and close any active replications on the peer.
func (p *CouchbaseLiteMockPeer) Close() {
	for _, c := range p.blipClients {
		c.btc.Close()
	}
}

// Type returns PeerTypeCouchbaseLite.
func (p *CouchbaseLiteMockPeer) Type() PeerType {
	return PeerTypeCouchbaseLite
}

// IsSymmetricRedundant returns true if there is another peer set up that is identical to this one, and this peer doesn't need to participate in unique actions.
func (p *CouchbaseLiteMockPeer) IsSymmetricRedundant() bool {
	return p.symmetricRedundant
}

// CreateReplication creates a replication instance
func (p *CouchbaseLiteMockPeer) CreateReplication(peer Peer, config PeerReplicationConfig) PeerReplication {
	sg, ok := peer.(*SyncGatewayPeer)
	if !ok {
		require.Fail(p.t, fmt.Sprintf("unsupported peer type %T for pull replication", peer))
	}

	// check for existing blip runner/client and use if present - avoids creating multiple clients for the same peer
	if pbtc, ok := p.blipClients[sg.String()]; ok {
		return &CouchbaseLiteMockReplication{
			activePeer:  p,
			passivePeer: peer,
			btc:         pbtc.btc,
			btcRunner:   pbtc.btcRunner,
			direction:   config.direction,
		}
	}

	replication := &CouchbaseLiteMockReplication{
		activePeer:  p,
		passivePeer: peer,
		btcRunner:   rest.NewBlipTesterClientRunner(sg.rt.TB().(*testing.T)),
		direction:   config.direction,
	}
	replication.btc = replication.btcRunner.NewBlipTesterClientOptsWithRT(sg.rt, &rest.BlipTesterClientOpts{
		Username:               "user",
		Channels:               []string{"*"},
		SupportedBLIPProtocols: []string{db.CBMobileReplicationV4.SubprotocolString()},
		AllowCreationWithoutBlipTesterClientRunner: true,
		SourceID: peer.SourceID(),
	},
	)
	p.blipClients[sg.String()] = &PeerBlipTesterClient{
		btcRunner: replication.btcRunner,
		btc:       replication.btc,
	}
	return replication
}

// SourceID returns the source ID for the peer used in <val>@<sourceID>.
func (p *CouchbaseLiteMockPeer) SourceID() string {
	return p.name
}

// Context returns the context for the peer.
func (p *CouchbaseLiteMockPeer) Context() context.Context {
	return base.TestCtx(p.TB())
}

// TB returns the testing.TB for the peer.
func (p *CouchbaseLiteMockPeer) TB() testing.TB {
	return p.t
}

// UpdateTB updates the testing.TB for the peer.
func (p *CouchbaseLiteMockPeer) UpdateTB(t *testing.T) {
	p.t = t
}

// GetBackingBucket returns the backing bucket for the peer. This is always nil.
func (p *CouchbaseLiteMockPeer) GetBackingBucket() base.Bucket {
	return nil
}

// CouchbaseLiteMockReplication represents a replication between Couchbase Lite and Sync Gateway. This can be a push or pull replication.
type CouchbaseLiteMockReplication struct {
	activePeer  Peer
	passivePeer Peer
	btc         *rest.BlipTesterClient
	btcRunner   *rest.BlipTestClientRunner
	direction   PeerReplicationDirection
}

// ActivePeer returns the peer sending documents
func (r *CouchbaseLiteMockReplication) ActivePeer() Peer {
	return r.activePeer
}

// PassivePeer returns the peer receiving documents
func (r *CouchbaseLiteMockReplication) PassivePeer() Peer {
	return r.passivePeer
}

// Start starts the replication
func (r *CouchbaseLiteMockReplication) Start() {
	r.btc.TB().Logf("starting CBL replication: %s", r)
	switch r.direction {
	case PeerReplicationDirectionPush:
		r.btcRunner.StartPush(r.btc.ID())
	case PeerReplicationDirectionPull:
		r.btcRunner.StartPull(r.btc.ID())
	default:
		require.Fail(r.btc.TB(), "unsupported replication direction %q", r.direction)
	}
}

// Stop halts the replication. The replication can be restarted after it is stopped.
func (r *CouchbaseLiteMockReplication) Stop() {
	r.btc.TB().Logf("stopping CBL replication: %s", r)
	switch r.direction {
	case PeerReplicationDirectionPush:
		r.btcRunner.StopPush(r.btc.ID())
	case PeerReplicationDirectionPull:
		r.btcRunner.UnsubPullChanges(r.btc.ID())
	}
}

func (r *CouchbaseLiteMockReplication) String() string {
	directionArrow := "->"
	if r.direction == PeerReplicationDirectionPull {
		directionArrow = "<-"
	}
	return fmt.Sprintf("%s%s%s", r.activePeer, directionArrow, r.passivePeer)
}

func (r *CouchbaseLiteMockReplication) Stats() string {
	return "No CBL stats yet"
}
