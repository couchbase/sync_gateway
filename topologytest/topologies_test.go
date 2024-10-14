// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

// Topology defines a topology for a set of peers and replications. This can include Couchbase Server, Sync Gateway, and Couchbase Lite peers, with push or pull replications between them.
type Topology struct {
	description  string
	peers        map[string]PeerOptions
	replications []PeerReplicationDefinition
	skipIf       func(t *testing.T, activePeerID string, peers map[string]Peer) // allow temporary skips while the code is being ironed out
}

// Topologies represents user configurations of replications.
var Topologies = []Topology{
	{
		/*
			+ - - - - - - +
			' +---------+ '
			' |  cbs1   | '
			' +---------+ '
			' +---------+ '
			' |   sg1   | '
			' +---------+ '
			+ - - - - - - +
			      ^
			      |
			      |
			      v
			  +---------+
			  |   cbl1  |
			  +---------+
		*/
		description: "CBL <-> Sync Gateway <-> CBS 1.1",
		peers: map[string]PeerOptions{
			"cbs1": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID1},
			"sg1":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID1},
			"cbl1": {Type: PeerTypeCouchbaseLite},
		},
		replications: []PeerReplicationDefinition{
			{
				activePeer:  "cbl1",
				passivePeer: "sg1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPull,
				},
			},
			{
				activePeer:  "cbl1",
				passivePeer: "sg1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPush,
				},
			},
		},
		skipIf: func(t *testing.T, activePeer string, peers map[string]Peer) {
			switch activePeer {
			case "cbs1":
				t.Skip("CBG-4289 imported documents get CV updated")
			}
			if base.UnitTestUrlIsWalrus() {
				switch activePeer {
				case "cbl1":
					t.Skip("CBG-4257, docs don't get CV when set from CBL")
				}
			}
		},
	},
	{
		/*
			Test topology 1.2

			+ - - - - - - +      +- - - - - - -+
			'  cluster A  '      '  cluster B  '
			' +---------+ '      ' +---------+ '
			' |  cbs1   | ' <--> ' |  cbs2   | '
			' +---------+ '      ' +---------+ '
			' +---------+ '      + - - - - - - +
			' |   sg1   | '
			' +---------+ '
			+ - - - - - - +
			      ^
			      |
			      |
			      v
			  +---------+
			  |   cbl1  |
			  +---------+
		*/
		description: "CBL<->SG<->CBS1 CBS1<->CBS2 1.2",
		peers: map[string]PeerOptions{
			"sg1":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID1},
			"cbs1": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID1},

			"cbs2": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID2},
			"cbl1": {Type: PeerTypeCouchbaseLite},
		},
		replications: []PeerReplicationDefinition{
			{
				activePeer:  "cbs2",
				passivePeer: "cbs1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPull,
				},
			},
			{
				activePeer:  "cbs2",
				passivePeer: "cbs1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPush,
				},
			},
			{
				activePeer:  "cbl1",
				passivePeer: "sg1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPull,
				},
			},
			{
				activePeer:  "cbl1",
				passivePeer: "sg1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPush,
				},
			},
		},
		skipIf: func(t *testing.T, activePeer string, peers map[string]Peer) {
			if base.UnitTestUrlIsWalrus() {
				switch activePeer {
				case "cbs1", "cbs2":
					t.Skip("CBG-4289 imported documents get CV updated")
				case "cbl1":
					t.Skip("CBG-4257, docs don't get CV when set from CBL")
				}
			}
		},
	},
	{
		/*
			Test topology 1.3

			+ - - - - - - +      +- - - - - - -+
			'  cluster A  '      '  cluster B  '
			' +---------+ '      ' +---------+ '
			' |  cbs1   | ' <--> ' |  cbs2   | '
			' +---------+ '      ' +---------+ '
			' +---------+ '      ' +---------+ '
			' |   sg1   | '      ' |   sg2   | '
			' +---------+ '      ' +---------+ '
			+ - - - - - - +      +- - - - - - -+
			      ^		 	   ^
			      |	     		   |
			      |	     		   |
			      v	     		   v
			  +---------+          +---------+
			  |   cbl1  |          |   cbl2  |
			  +---------+          +---------+
		*/
		description: "2x CBL<->SG<->CBS XDCR only 1.3",
		peers: map[string]PeerOptions{
			"cbs1": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID1},
			"cbs2": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID2},
			"sg1":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID1},
			"sg2":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID2},
			"cbl1": {Type: PeerTypeCouchbaseLite},
			// TODO: CBG-4270, push replication only exists empemerally
			"cbl2": {Type: PeerTypeCouchbaseLite},
		},
		replications: []PeerReplicationDefinition{
			{
				activePeer:  "cbs2",
				passivePeer: "cbs1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPull,
				},
			},
			{
				activePeer:  "cbs2",
				passivePeer: "cbs1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPush,
				},
			},
			{
				activePeer:  "cbl1",
				passivePeer: "sg1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPull,
				},
			},
			{
				activePeer:  "cbl1",
				passivePeer: "sg1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPush,
				},
			},
			{
				activePeer:  "cbl2",
				passivePeer: "sg2",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPull,
				},
			},
			{
				activePeer:  "cbl2",
				passivePeer: "sg2",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPush,
				},
			},
		},
		skipIf: func(t *testing.T, activePeer string, peers map[string]Peer) {
			switch activePeer {
			case "cbs1", "cbs2":
				t.Skip("CBG-4289 imported documents get CV updated")
			}
			if base.UnitTestUrlIsWalrus() {
				switch activePeer {
				case "cbl1", "cbl2":
					t.Skip("CBG-4257, docs don't get CV when set from CBL")
				}
			}
		},
	},
	// topology 1.4 not present, no P2P supported yet
	{
		/*
				Test topology 1.5

				+ - - - - - - +      +- - - - - - -+
				'  cluster A  '      '  cluster B  '
				' +---------+ '      ' +---------+ '
				' |  cbs1   | ' <--> ' |  cbs2   | '
				' +---------+ '      ' +---------+ '
				' +---------+ '      ' +---------+ '
				' |   sg1   | '      ' |   sg2   | '
				' +---------+ '      ' +---------+ '
				+ - - - - - - +      +- - - - - - -+
			   	      ^ 	         ^
				      |                  |
				      |                  |
				      |                  |
				      |     +------+     |
				      +---> | cbl1 | <---+
				            +------+
		*/
		/* This test doesn't work yet, CouchbaseLiteMockPeer doesn't support writing data to multiple Sync Gateway peers yet
				description: "Sync Gateway -> Couchbase Server -> Couchbase Server",
				peers: map[string]PeerOptions{
					"cbs1": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID1},
					"cbs2": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID2},
					"sg1":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID1},
					"sg2":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID2},
					"cbl1": {Type: PeerTypeCouchbaseLite},
				},
				replications: []PeerReplicationDefinition{
					{
						activePeer:  "cbs2",
						passivePeer: "cbs1",
						config: PeerReplicationConfig{
							direction: PeerReplicationDirectionPull,
						},
					},
		{
						activePeer:  "cbs2",
						passivePeer: "cbs1",
						config: PeerReplicationConfig{
							direction: PeerReplicationDirectionPull,
						},
					},

					{
						activePeer:  "cbl1",
						passivePeer: "sg1",
						config: PeerReplicationConfig{
							direction: PeerReplicationDirectionPull,
						},
					},
					{
						activePeer:  "cbl1",
						passivePeer: "sg1",
						config: PeerReplicationConfig{
							direction: PeerReplicationDirectionPush,
						},
					},
					{
						activePeer:  "cbl1",
						passivePeer: "sg2",
						config: PeerReplicationConfig{
							direction: PeerReplicationDirectionPull,
						},
					},
					{
						activePeer:  "cbl1",
						passivePeer: "sg2",
						config: PeerReplicationConfig{
							direction: PeerReplicationDirectionPush,
						},
					},
				},
		*/
	},
}

// simpleTopologies represents simplified topologies to make testing the integration test code easier.
// nolint: unused
var simpleTopologies = []Topology{
	{

		/*
			+------+      +------+
			| cbs1 | <--> | cbs2 |
			+------+      +------+
		*/
		description: "Couchbase Server -> Couchbase Server",
		peers: map[string]PeerOptions{
			"cbs1": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID1},
			"cbs2": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID2}},
		replications: []PeerReplicationDefinition{
			{
				activePeer:  "cbs1",
				passivePeer: "cbs2",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPush,
				},
			},
			{
				activePeer:  "cbs1",
				passivePeer: "cbs2",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPull,
				},
			},
		},
		skipIf: func(t *testing.T, activePeer string, peers map[string]Peer) {
			if base.UnitTestUrlIsWalrus() {
				t.Skip("CBG-4300, need to construct a _vv on source if none is present, to then call setWithMeta")
			}
		},
	},
	{
		/*
			+ - - - - - - +      +- - - - - - -+
			'  cluster A  '      '  cluster B  '
			' +---------+ '      ' +---------+ '
			' |  cbs1   | ' <--> ' |  cbs2   | '
			' +---------+ '      ' +---------+ '
			' +---------+ '      + - - - - - - +
			' |   sg1   | '
			' +---------+ '
			+ - - - - - - +
		*/
		description: "Couchbase Server (with SG) -> Couchbase Server",
		peers: map[string]PeerOptions{
			"cbs1": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID1},
			"sg1":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID1},
			"cbs2": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID2},
		},
		replications: []PeerReplicationDefinition{
			{
				activePeer:  "cbs1",
				passivePeer: "cbs2",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPush,
				},
			},
			{
				activePeer:  "cbs1",
				passivePeer: "cbs2",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPull,
				},
			},
		},
		skipIf: func(t *testing.T, activePeer string, peers map[string]Peer) {
			if base.UnitTestUrlIsWalrus() {
				switch activePeer {
				case "cbs1":
					t.Skip("CBG-4289 imported documents get CV updated")
				case "cbs2":
					t.Skip("CBG-4300, need to construct a _vv on source if none is present, to then call setWithMeta")
				}
			}
		},
	},
}
