// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

import (
	"fmt"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
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

// CouchbaseLiteMockPeer represents an in-memory Couchbase Lite peer. This utilizes BlipTesterClient from the rest package to send and receive blip messages.
type CouchbaseLiteMockPeer struct {
	t           *testing.T
	blipClients map[string]*PeerBlipTesterClient
	name        string
}

func (p *CouchbaseLiteMockPeer) String() string {
	return p.name
}

// GetDocument returns the latest version of a document. The test will fail the document does not exist.
func (p *CouchbaseLiteMockPeer) GetDocument(dsName sgbucket.DataStoreName, docID string) (rest.DocVersion, db.Body) {
	// this isn't yet collection aware, using single default collection
	return rest.EmptyDocVersion(), nil
}

// getSingleBlipClient returns the single blip client for the peer. If there are multiple clients, or not clients it will fail the test. This is temporary to stub support for multiple Sync Gateway peers.
func (p *CouchbaseLiteMockPeer) getSingleBlipClient() *PeerBlipTesterClient {
	// this isn't yet collection aware, using single default collection
	if len(p.blipClients) != 1 {
		require.Fail(p.t, "blipClients haven't been created for %s, a temporary limitation of CouchbaseLiteMockPeer", p)
	}
	for _, c := range p.blipClients {
		return c
	}
	require.Fail(p.t, "no blipClients found for %s", p)
	return nil
}

// CreateDocument creates a document on the peer. The test will fail if the document already exists.
func (p *CouchbaseLiteMockPeer) CreateDocument(dsName sgbucket.DataStoreName, docID string, body []byte) rest.DocVersion {
	return rest.EmptyDocVersion()
}

// WriteDocument writes a document to the peer. The test will fail if the write does not succeed.
func (p *CouchbaseLiteMockPeer) WriteDocument(dsName sgbucket.DataStoreName, docID string, body []byte) rest.DocVersion {
	// this isn't yet collection aware, using single default collection
	client := p.getSingleBlipClient()
	// set an HLV here.
	docVersion, err := client.btcRunner.PushRev(client.ID(), docID, rest.EmptyDocVersion(), body)
	require.NoError(client.btcRunner.TB(), err)
	return docVersion
}

// DeleteDocument deletes a document on the peer. The test will fail if the document does not exist.
func (p *CouchbaseLiteMockPeer) DeleteDocument(dsName sgbucket.DataStoreName, docID string) rest.DocVersion {
	return rest.EmptyDocVersion()
}

// WaitForDocVersion waits for a document to reach a specific version. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseLiteMockPeer) WaitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected rest.DocVersion) db.Body {
	// this isn't yet collection aware, using single default collection
	client := p.getSingleBlipClient()
	// FIXME: waiting for a specific version isn't working yet.
	bodyBytes := client.btcRunner.WaitForDoc(client.ID(), docID)
	var body db.Body
	require.NoError(p.t, base.JSONUnmarshal(bodyBytes, &body))
	return body
}

// RequireDocNotFound asserts that a document does not exist on the peer.
func (p *CouchbaseLiteMockPeer) RequireDocNotFound(dsName sgbucket.DataStoreName, docID string) {
	// not implemented yet in blip client tester
	// _, err := p.btcRunner.GetDoc(p.btc.id, docID)
	// base.RequireDocNotFoundError(p.btcRunner.TB(), err)
}

// Close will shut down the peer and close any active replications on the peer.
func (p *CouchbaseLiteMockPeer) Close() {
	for _, c := range p.blipClients {
		c.btc.Close()
	}
}

// CreateReplication creates a replication instance
func (p *CouchbaseLiteMockPeer) CreateReplication(peer Peer, config PeerReplicationConfig) PeerReplication {
	sg, ok := peer.(*SyncGatewayPeer)
	if !ok {
		require.Fail(p.t, fmt.Sprintf("unsupported peer type %T for pull replication", peer))
	}
	replication := &CouchbaseLiteMockReplication{
		activePeer:  p,
		passivePeer: peer,
		btcRunner:   rest.NewBlipTesterClientRunner(sg.rt.TB().(*testing.T)),
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

// SourceID returns the source ID for the peer used in <val>@sourceID.
func (r *CouchbaseLiteMockPeer) SourceID() string {
	return r.name
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
	r.btcRunner.StartPull(r.btc.ID())
}

// Stop halts the replication. The replication can be restarted after it is stopped.
func (r *CouchbaseLiteMockReplication) Stop() {
	_, err := r.btcRunner.UnsubPullChanges(r.btc.ID())
	require.NoError(r.btcRunner.TB(), err)
}
