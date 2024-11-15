// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// Package topologytest implements code to be able to test with Couchbase Server, Sync Gateway, and Couchbase Lite from a go test. This can be with Couchbase Server or rosmar depending on SG_TEST_BACKING_STORE. Couchbase Lite can either be an in memory implementation of a Couchbase Lite peer, or a real Couchbase Lite peer.
package topologytest

import (
	"context"
	"fmt"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/xdcr"
	"github.com/stretchr/testify/require"
)

// Peer represents a peer in an Mobile workflow. The types of Peers are Couchbase Server, Sync Gateway, or Couchbase Lite.
type Peer interface {
	// GetDocument returns the latest version of a document. The test will fail the document does not exist.
	GetDocument(dsName sgbucket.DataStoreName, docID string) (DocMetadata, db.Body)
	// CreateDocument creates a document on the peer. The test will fail if the document already exists.
	CreateDocument(dsName sgbucket.DataStoreName, docID string, body []byte) DocMetadata
	// WriteDocument upserts a document to the peer. The test will fail if the write does not succeed. Reasons for failure might be sync function rejections for Sync Gateway rejections.
	WriteDocument(dsName sgbucket.DataStoreName, docID string, body []byte) DocMetadata
	// DeleteDocument deletes a document on the peer. The test will fail if the document does not exist.
	DeleteDocument(dsName sgbucket.DataStoreName, docID string) DocMetadata

	// WaitForDocVersion waits for a document to reach a specific version. Returns the state of the document at that version. The test will fail if the document does not reach the expected version in 20s.
	WaitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata) db.Body

	// WaitForDeletion waits for a document to be deleted. This document must be a tombstone. The test will fail if the document still exists after 20s.
	WaitForDeletion(dsName sgbucket.DataStoreName, docID string)

	// WaitForTombstoneVersion waits for a document to reach a specific version. This document must be a tombstone. The test will fail if the document does not reach the expected version in 20s.
	WaitForTombstoneVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata)

	// RequireDocNotFound asserts that a document does not exist on the peer.
	RequireDocNotFound(dsName sgbucket.DataStoreName, docID string)

	// CreateReplication creates a replication instance
	CreateReplication(Peer, PeerReplicationConfig) PeerReplication

	// Close will shut down the peer and close any active replications on the peer.
	Close()

	internalPeer
}

// internalPeer represents Peer interface that are only intdeded to be used from within a Peer or Replication class, but not by tests themselves.
type internalPeer interface {
	// SourceID returns the source ID for the peer used in <val>@<sourceID>.
	SourceID() string

	// GetBackingBucket returns the backing bucket for the peer. This is nil when the peer is a Couchbase Lite peer.
	GetBackingBucket() base.Bucket

	// TB returns the testing.TB for the peer.
	TB() testing.TB

	// Context returns the context for the peer.
	Context() context.Context
}

// PeerReplication represents a replication between two peers. This replication is unidirectional since all bi-directional replications are represented by two unidirectional instances.
type PeerReplication interface {
	// ActivePeer returns the peer sending documents
	ActivePeer() Peer
	// PassivePeer returns the peer receiving documents
	PassivePeer() Peer
	// Start starts the replication
	Start()
	// Stop halts the replication. The replication can be restarted after it is stopped.
	Stop()
}

var _ PeerReplication = &CouchbaseLiteMockReplication{}
var _ PeerReplication = &CouchbaseServerReplication{}
var _ PeerReplication = &CouchbaseServerReplication{}

// PeerReplicationDirection represents the direction of a replication from the active peer.
type PeerReplicationDirection int

const (
	// PeerReplicationDirectionPush pushes data from an active peer to a passive peer.
	PeerReplicationDirectionPush PeerReplicationDirection = iota
	// PeerReplicationDirectionPull pulls data from an active peer to a passive peer.
	PeerReplicationDirectionPull
)

// PeerReplicationConfig represents the configuration for a given replication.
type PeerReplicationConfig struct {
	direction PeerReplicationDirection
	// oneShot   bool // not implemented, would only be supported for SG <-> CBL, XDCR is always continuous
}

// PeerReplicationDefinition defines a pair of peers and a configuration.
type PeerReplicationDefinition struct {
	activePeer  string
	passivePeer string
	config      PeerReplicationConfig
}

var _ Peer = &CouchbaseServerPeer{}
var _ Peer = &CouchbaseLiteMockPeer{}
var _ Peer = &SyncGatewayPeer{}

// PeerType represents the type of a peer. These will be:
//
// - Couchbase Server (backed by TestBucket)
//   - rosmar default
//   - Couchbase Server based on SG_TEST_BACKING_STORE=couchbase
//
// - Sync Gateway (backed by RestTester)
//
// - Couchbase Lite
//   - CouchbaseLiteMockPeer is in memory backed by BlipTesterClient
//   - CouchbaseLitePeer (backed by Test Server) Not Yet Implemented
type PeerType int

const (
	// PeerTypeCouchbaseServer represents a Couchbase Server peer. This can be backed by rosmar or couchbase server (controlled by SG_TEST_BACKING_STORE).
	PeerTypeCouchbaseServer PeerType = iota
	// PeerTypeCouchbaseLite represents a Couchbase Lite peer. This is currently backed in memory but will be backed by in memory structure that will send and receive blip messages. Future expansion to real Couchbase Lite peer in CBG-4260.
	PeerTypeCouchbaseLite
	// PeerTypeSyncGateway represents a Sync Gateway peer backed by a RestTester.
	PeerTypeSyncGateway
)

// PeerBucketID represents a specific bucket for a test. This allows multiple Sync Gateway instances to point to the same bucket, or a different buckets. There is no significance to the numbering of the buckets. We can use as many buckets as the MainTestBucketPool allows.
type PeerBucketID int

const (
	// PeerBucketNoBackingBucket represents a peer that does not have a backing bucket. This is used for Couchbase Lite peers.
	PeerBucketNoBackingBucket PeerBucketID = iota
	// PeerBucketID1 represents the first bucket in the test.
	PeerBucketID1 // start at 1 to avoid 0 value being accidentally used
	// PeerBucketID2 represents the second bucket in the test.
	PeerBucketID2
)

// PeerOptions are options to create a peer.
type PeerOptions struct {
	Type     PeerType
	BucketID PeerBucketID // BucketID is used to identify the bucket for a Couchbase Server or Sync Gateway peer. This option is ignored for Couchbase Lite peers.
}

// NewPeer creates a new peer for replication. The buckets must be created before the peers are created.
func NewPeer(t *testing.T, name string, buckets map[PeerBucketID]*base.TestBucket, opts PeerOptions) Peer {
	switch opts.Type {
	case PeerTypeCouchbaseServer:
		bucket, ok := buckets[opts.BucketID]
		require.True(t, ok, "bucket not found for bucket ID %d", opts.BucketID)
		sourceID, err := xdcr.GetSourceID(base.TestCtx(t), bucket)
		require.NoError(t, err)
		return &CouchbaseServerPeer{
			name:             name,
			tb:               t,
			bucket:           bucket,
			sourceID:         sourceID,
			pullReplications: make(map[Peer]xdcr.Manager),
			pushReplications: make(map[Peer]xdcr.Manager),
		}
	case PeerTypeCouchbaseLite:
		require.Equal(t, PeerBucketNoBackingBucket, opts.BucketID, "bucket should not be specified for Couchbase Lite peer %+v", opts)
		_, ok := buckets[opts.BucketID]
		require.False(t, ok, "bucket should not be specified for Couchbase Lite peer")
		return &CouchbaseLiteMockPeer{
			t:           t,
			name:        name,
			blipClients: make(map[string]*PeerBlipTesterClient),
		}
	case PeerTypeSyncGateway:
		bucket, ok := buckets[opts.BucketID]
		require.True(t, ok, "bucket not found for bucket ID %d", opts.BucketID)
		return newSyncGatewayPeer(t, name, bucket)
	default:
		require.Fail(t, fmt.Sprintf("unsupported peer type %T", opts.Type))
	}
	return nil
}

// createPeerReplications creates a list of peers and replications. The replications will not have started.
func createPeerReplications(t *testing.T, peers map[string]Peer, configs []PeerReplicationDefinition) []PeerReplication {
	replications := make([]PeerReplication, 0, len(configs))
	for _, config := range configs {
		activePeer, ok := peers[config.activePeer]
		require.True(t, ok, "active peer %s not found", config.activePeer)
		passivePeer, ok := peers[config.passivePeer]
		require.True(t, ok, "passive peer %s not found", config.passivePeer)
		replications = append(replications, activePeer.CreateReplication(passivePeer, config.config))
	}
	return replications
}

// getPeerBuckets returns a map of bucket IDs to buckets for a list of peers. This requires sufficient number of buckets in the bucket pool. The buckets will be released with a testing.T.Cleanup function.
func getPeerBuckets(t *testing.T, peerOptions map[string]PeerOptions) map[PeerBucketID]*base.TestBucket {
	buckets := make(map[PeerBucketID]*base.TestBucket)
	for _, p := range peerOptions {
		if p.BucketID == PeerBucketNoBackingBucket {
			continue
		}
		_, ok := buckets[p.BucketID]
		if !ok {
			bucket := base.GetTestBucket(t)
			buckets[p.BucketID] = bucket
			t.Cleanup(func() {
				bucket.Close(base.TestCtx(t))
			})
		}
	}
	return buckets
}

// createPeers will create a sets of peers. The underlying buckets will be created. The peers will be closed and the buckets will be destroyed.
func createPeers(t *testing.T, peersOptions map[string]PeerOptions) map[string]Peer {
	buckets := getPeerBuckets(t, peersOptions)
	peers := make(map[string]Peer, len(peersOptions))
	for id, peerOptions := range peersOptions {
		peer := NewPeer(t, id, buckets, peerOptions)
		t.Logf("TopologyTest: created peer %s, SourceID=%+v", id, peer.SourceID())
		t.Cleanup(func() {
			peer.Close()
		})
		peers[id] = peer
	}
	return peers
}

// setupTests returns a map of peers and a list of replications. The peers will be closed and the buckets will be destroyed by t.Cleanup.
func setupTests(t *testing.T, topology Topology, activePeerID string) (map[string]Peer, []PeerReplication) {
	peers := createPeers(t, topology.peers)
	replications := createPeerReplications(t, peers, topology.replications)

	if topology.skipIf != nil {
		topology.skipIf(t, activePeerID, peers)
	}
	for _, replication := range replications {
		// temporarily start the replication before writing the document, limitation of CouchbaseLiteMockPeer as active peer since WriteDocument is calls PushRev
		replication.Start()
	}
	return peers, replications
}

func TestPeerImplementation(t *testing.T) {
	testCases := []struct {
		name       string
		peerOption PeerOptions
	}{
		{
			name: "cbs",
			peerOption: PeerOptions{
				Type:     PeerTypeCouchbaseServer,
				BucketID: PeerBucketID1,
			},
		},
		{
			name: "sg",
			peerOption: PeerOptions{
				Type:     PeerTypeSyncGateway,
				BucketID: PeerBucketID1,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			peers := createPeers(t, map[string]PeerOptions{tc.name: tc.peerOption})
			peer := peers[tc.name]

			docID := t.Name()
			collectionName := getSingleDsName()

			peer.RequireDocNotFound(collectionName, docID)
			// Create
			createBody := []byte(`{"op": "creation"}`)
			createVersion := peer.CreateDocument(collectionName, docID, []byte(`{"op": "creation"}`))
			require.NotEmpty(t, createVersion.CV)
			if tc.peerOption.Type == PeerTypeCouchbaseServer {
				require.Empty(t, createVersion.RevTreeID)
			} else {
				require.NotEmpty(t, createVersion.RevTreeID)
			}

			peer.WaitForDocVersion(collectionName, docID, createVersion)
			// Check Get after creation
			roundtripGetVersion, roundtripGetbody := peer.GetDocument(collectionName, docID)
			require.Equal(t, createVersion, roundtripGetVersion)
			require.JSONEq(t, string(createBody), string(base.MustJSONMarshal(t, roundtripGetbody)))

			// Update
			updateBody := []byte(`{"op": "update"}`)
			updateVersion := peer.WriteDocument(collectionName, docID, updateBody)
			require.NotEmpty(t, updateVersion.CV)
			require.NotEqual(t, updateVersion.CV(), createVersion.CV())
			if tc.peerOption.Type == PeerTypeCouchbaseServer {
				require.Empty(t, updateVersion.RevTreeID)
			} else {
				require.NotEmpty(t, updateVersion.RevTreeID)
				require.NotEqual(t, updateVersion.RevTreeID, createVersion.RevTreeID)
			}
			peer.WaitForDocVersion(collectionName, docID, updateVersion)

			// Check Get after update
			roundtripGetVersion, roundtripGetbody = peer.GetDocument(collectionName, docID)
			require.Equal(t, updateVersion, roundtripGetVersion)
			require.JSONEq(t, string(updateBody), string(base.MustJSONMarshal(t, roundtripGetbody)))

			// Delete
			deleteVersion := peer.DeleteDocument(collectionName, docID)
			require.NotEmpty(t, deleteVersion.CV())
			require.NotEqual(t, deleteVersion.CV(), updateVersion.CV())
			require.NotEqual(t, deleteVersion.CV(), createVersion.CV())
			if tc.peerOption.Type == PeerTypeCouchbaseServer {
				require.Empty(t, deleteVersion.RevTreeID)
			} else {
				require.NotEmpty(t, deleteVersion.RevTreeID)
				require.NotEqual(t, deleteVersion.RevTreeID, createVersion.RevTreeID)
				require.NotEqual(t, deleteVersion.RevTreeID, updateVersion.RevTreeID)
			}
			peer.RequireDocNotFound(collectionName, docID)

			// Resurrection

			resurrectionBody := []byte(`{"op": "resurrection"}`)
			resurrectionVersion := peer.WriteDocument(collectionName, docID, resurrectionBody)
			require.NotEmpty(t, resurrectionVersion.CV())
			require.NotEqual(t, resurrectionVersion.CV(), deleteVersion.CV())
			require.NotEqual(t, resurrectionVersion.CV(), updateVersion.CV())
			require.NotEqual(t, resurrectionVersion.CV(), createVersion.CV())
			if tc.peerOption.Type == PeerTypeCouchbaseServer {
				require.Empty(t, resurrectionVersion.RevTreeID)
			} else {
				require.NotEmpty(t, resurrectionVersion.RevTreeID)
				require.NotEqual(t, resurrectionVersion.RevTreeID, createVersion.RevTreeID)
				require.NotEqual(t, resurrectionVersion.RevTreeID, updateVersion.RevTreeID)
				require.NotEqual(t, resurrectionVersion.RevTreeID, deleteVersion.RevTreeID)
			}
			peer.WaitForDocVersion(collectionName, docID, resurrectionVersion)

		})
	}

}
