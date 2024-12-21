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
	"iter"
	"maps"
	"runtime"
	"slices"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/xdcr"
	"github.com/stretchr/testify/require"
)

// totalWaitTime is the time to wait for a document on a peer. This time is low for rosmar and high for Couchbase Server.
var totalWaitTime = 3 * time.Second

// pollInterval is the time to poll to see if a document is updated on a peer
var pollInterval = 1 * time.Millisecond

func init() {
	if !base.UnitTestUrlIsWalrus() || raceEnabled {
		totalWaitTime = 40 * time.Second
	}
}

// Peer represents a peer in an Mobile workflow. The types of Peers are Couchbase Server, Sync Gateway, or Couchbase Lite.
type Peer interface {
	// GetDocument returns the latest version of a document. The test will fail the document does not exist.
	GetDocument(dsName sgbucket.DataStoreName, docID string) (DocMetadata, db.Body)
	// CreateDocument creates a document on the peer. The test will fail if the document already exists.
	CreateDocument(dsName sgbucket.DataStoreName, docID string, body []byte) BodyAndVersion
	// WriteDocument upserts a document to the peer. The test will fail if the write does not succeed. Reasons for failure might be sync function rejections for Sync Gateway rejections.
	WriteDocument(dsName sgbucket.DataStoreName, docID string, body []byte) BodyAndVersion
	// DeleteDocument deletes a document on the peer. The test will fail if the document does not exist.
	DeleteDocument(dsName sgbucket.DataStoreName, docID string) DocMetadata

	// WaitForDocVersion waits for a document to reach a specific version. Returns the state of the document at that version. The test will fail if the document does not reach the expected version in 20s.
	WaitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata) db.Body

	// WaitForTombstoneVersion waits for a document to reach a specific version. This document must be a tombstone. The test will fail if the document does not reach the expected version in 20s.
	WaitForTombstoneVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata)

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

	// UpdateTB updates the testing.TB for the peer.
	UpdateTB(*testing.T)

	// Context returns the context for the peer.
	Context() context.Context

	// Type returns the type of the peer.
	Type() PeerType

	// IsSymmetricRedundant returns true if the peer is symmetric and redundant in the topology and doesn't need to be tested as an uniquely active peer for writing.
	IsSymmetricRedundant() bool
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

// Peers represents a set of peers. The peers are indexed by name.
type Peers map[string]Peer

// SortedPeers returns a sorted list of peers by name, for deterministic output.
func (p Peers) SortedPeers() iter.Seq2[string, Peer] {
	keys := slices.Collect(maps.Keys(p))
	slices.Sort(keys)
	return func(yield func(k string, v Peer) bool) {
		for _, peerName := range keys {
			if !yield(peerName, p[peerName]) {
				return
			}
		}
	}
}

// UniqueTopologyPeers returns a list of unique peers in the topology. If there is an identical symmetric peer, do not return it.
func (p Peers) ActivePeers() iter.Seq2[string, Peer] {
	keys := slices.Collect(maps.Keys(p))
	slices.Sort(keys)
	return func(yield func(k string, v Peer) bool) {
		for _, peerName := range keys {
			peer := p[peerName]
			if peer.IsSymmetricRedundant() {
				continue
			}
			if !yield(peerName, peer) {
				return
			}
		}
	}
}

var _ PeerReplication = &CouchbaseLiteMockReplication{}
var _ PeerReplication = &CouchbaseServerReplication{}
var _ PeerReplication = &CouchbaseServerReplication{}

// Replications are a collection of PeerReplications.
type Replications []PeerReplication

// Stop stops all replications.
func (r Replications) Stop() {
	for _, replication := range r {
		replication.Stop()
	}
}

// Start starts all replications.
func (r Replications) Start() {
	for _, replication := range r {
		replication.Start()
	}
}

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
	Type      PeerType
	Symmetric bool         // There is a peer identical to this one in the topology
	BucketID  PeerBucketID // BucketID is used to identify the bucket for a Couchbase Server or Sync Gateway peer. This option is ignored for Couchbase Lite peers.
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
			name:               name,
			tb:                 t,
			bucket:             bucket,
			sourceID:           sourceID,
			pullReplications:   make(map[Peer]xdcr.Manager),
			pushReplications:   make(map[Peer]xdcr.Manager),
			symmetricRedundant: opts.Symmetric,
		}
	case PeerTypeCouchbaseLite:
		require.Equal(t, PeerBucketNoBackingBucket, opts.BucketID, "bucket should not be specified for Couchbase Lite peer %+v", opts)
		_, ok := buckets[opts.BucketID]
		require.False(t, ok, "bucket should not be specified for Couchbase Lite peer")
		return &CouchbaseLiteMockPeer{
			t:                  t,
			name:               name,
			blipClients:        make(map[string]*PeerBlipTesterClient),
			symmetricRedundant: opts.Symmetric,
		}
	case PeerTypeSyncGateway:
		bucket, ok := buckets[opts.BucketID]
		require.True(t, ok, "bucket not found for bucket ID %d", opts.BucketID)
		return newSyncGatewayPeer(t, name, bucket, opts.Symmetric)
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
func createPeers(t *testing.T, peersOptions map[string]PeerOptions) Peers {
	buckets := getPeerBuckets(t, peersOptions)
	peers := make(Peers, len(peersOptions))
	for id, peerOptions := range peersOptions {
		peer := NewPeer(t, id, buckets, peerOptions)
		t.Logf("TopologyTest: created peer %s", peer)
		t.Cleanup(func() {
			peer.Close()
		})
		peers[id] = peer
	}
	return peers
}

func updatePeersT(t *testing.T, peers map[string]Peer) {
	for _, peer := range peers {
		oldTB := peer.TB().(*testing.T)
		t.Cleanup(func() { peer.UpdateTB(oldTB) })
		peer.UpdateTB(t)
	}
}

// setupTests returns a map of peers and a list of replications. The peers will be closed and the buckets will be destroyed by t.Cleanup.
func setupTests(t *testing.T, topology Topology) (base.ScopeAndCollectionName, Peers, Replications) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyVV)
	peers := createPeers(t, topology.peers)
	replications := createPeerReplications(t, peers, topology.replications)

	for _, replication := range replications {
		// temporarily start the replication before writing the document, limitation of CouchbaseLiteMockPeer as active peer since WriteDocument is calls PushRev
		replication.Start()
	}
	return getSingleDsName(), peers, replications
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
		{
			name: "cbl",
			peerOption: PeerOptions{
				Type: PeerTypeCouchbaseLite,
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := map[string]PeerOptions{tc.name: tc.peerOption}
			// couchbase lite peer can't exist separately from sync gateway peer, CBG-4433
			if tc.peerOption.Type == PeerTypeCouchbaseLite {
				opts["sg"] = PeerOptions{Type: PeerTypeSyncGateway, BucketID: PeerBucketID1}
			}
			peers := createPeers(t, opts)
			peer := peers[tc.name]
			// couchbase lite peer can't exist separately from sync gateway peer, CBG-4433
			if peer.Type() == PeerTypeCouchbaseLite {
				replication := peer.CreateReplication(peers["sg"], PeerReplicationConfig{})
				defer replication.Stop()
			}

			docID := t.Name()
			collectionName := getSingleDsName()

			// Create
			createBody := []byte(`{"op": "creation"}`)
			createVersion := peer.CreateDocument(collectionName, docID, []byte(`{"op": "creation"}`))
			require.NotEmpty(t, createVersion.docMeta.CV)
			if tc.peerOption.Type == PeerTypeSyncGateway {
				require.NotEmpty(t, createVersion.docMeta.RevTreeID)
			} else {
				require.Empty(t, createVersion.docMeta.RevTreeID)
			}

			peer.WaitForDocVersion(collectionName, docID, createVersion.docMeta)
			// Check Get after creation
			roundtripGetVersion, roundtripGetbody := peer.GetDocument(collectionName, docID)
			require.Equal(t, createVersion.docMeta, roundtripGetVersion)
			require.JSONEq(t, string(createBody), string(base.MustJSONMarshal(t, roundtripGetbody)))

			// Update
			updateBody := []byte(`{"op": "update"}`)
			updateVersion := peer.WriteDocument(collectionName, docID, updateBody)
			require.NotEmpty(t, updateVersion.docMeta.CV)
			require.NotEqual(t, updateVersion.docMeta.CV(t), createVersion.docMeta.CV(t))
			if peer.Type() == PeerTypeSyncGateway {
				require.NotEmpty(t, updateVersion.docMeta.RevTreeID)
				require.NotEqual(t, updateVersion.docMeta.RevTreeID, createVersion.docMeta.RevTreeID)
			} else {
				require.Empty(t, updateVersion.docMeta.RevTreeID)
			}
			peer.WaitForDocVersion(collectionName, docID, updateVersion.docMeta)

			// Check Get after update
			roundtripGetVersion, roundtripGetbody = peer.GetDocument(collectionName, docID)
			require.Equal(t, updateVersion.docMeta, roundtripGetVersion)
			require.JSONEq(t, string(updateBody), string(base.MustJSONMarshal(t, roundtripGetbody)))

			// Delete
			deleteVersion := peer.DeleteDocument(collectionName, docID)
			require.NotEmpty(t, deleteVersion.CV(t))
			require.NotEqual(t, deleteVersion.CV(t), updateVersion.docMeta.CV(t))
			require.NotEqual(t, deleteVersion.CV(t), createVersion.docMeta.CV(t))
			if tc.peerOption.Type == PeerTypeSyncGateway {
				require.NotEmpty(t, deleteVersion.RevTreeID)
				require.NotEqual(t, deleteVersion.RevTreeID, createVersion.docMeta.RevTreeID)
				require.NotEqual(t, deleteVersion.RevTreeID, updateVersion.docMeta.RevTreeID)
			} else {
				require.Empty(t, deleteVersion.RevTreeID)
			}
			peer.WaitForTombstoneVersion(collectionName, docID, deleteVersion)

			// Resurrection
			resurrectionBody := []byte(`{"op": "resurrection"}`)
			resurrectionVersion := peer.WriteDocument(collectionName, docID, resurrectionBody)
			require.NotEmpty(t, resurrectionVersion.docMeta.CV(t))
			// FIXME: CBG-4440 - Windows timestamp resolution not good enough for this test
			if runtime.GOOS != "windows" {
				// need to switch to a HLC so we can have unique versions even in the same timestamp window
				require.NotEqual(t, resurrectionVersion.docMeta.CV(t), deleteVersion.CV(t))
				require.NotEqual(t, resurrectionVersion.docMeta.CV(t), updateVersion.docMeta.CV(t))
				require.NotEqual(t, resurrectionVersion.docMeta.CV(t), createVersion.docMeta.CV(t))
			}
			if tc.peerOption.Type == PeerTypeSyncGateway {
				require.NotEmpty(t, resurrectionVersion.docMeta.RevTreeID)
				require.NotEqual(t, resurrectionVersion.docMeta.RevTreeID, createVersion.docMeta.RevTreeID)
				require.NotEqual(t, resurrectionVersion.docMeta.RevTreeID, updateVersion.docMeta.RevTreeID)
				require.NotEqual(t, resurrectionVersion.docMeta.RevTreeID, deleteVersion.RevTreeID)
			} else {
				require.Empty(t, resurrectionVersion.docMeta.RevTreeID)
			}
			peer.WaitForDocVersion(collectionName, docID, resurrectionVersion.docMeta)

		})
	}

}
