// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

// TopologySpecification defines a topology for a set of peers and replications. This can include Couchbase Server, Sync Gateway, and Couchbase Lite peers, with push or pull replications between them.
type TopologySpecification struct {
	description  string
	peers        map[string]PeerOptions
	replications []PeerReplicationDefinition
}

// Topologies represents user configurations of replications.
var TopologySpecifications = []TopologySpecification{
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
		description: "CBL<->SG<->CBS 1.1",
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
			"cbs2": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID2, Symmetric: true},
			"sg1":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID1},
			"sg2":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID2, Symmetric: true},
			"cbl1": {Type: PeerTypeCouchbaseLite},
			"cbl2": {Type: PeerTypeCouchbaseLite, Symmetric: true},
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
		description: "2x CBL (swap)<->SG<->CBS XDCR 1.5",
		peers: map[string]PeerOptions{
			"cbs1": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID1},
			"cbs2": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID2},
			"sg1":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID1},
			"sg2":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID2},
			"cbl1": {Type: PeerTypeCouchbaseLite},
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
				activePeer:           "cbl1",
				passivePeer:          "sg1",
				alternatePassivePeer: "sg2",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPull,
				},
			},
			{
				activePeer:           "cbl1",
				passivePeer:          "sg1",
				alternatePassivePeer: "sg2",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPush,
				},
			},
			{
				activePeer:           "cbl2",
				passivePeer:          "sg2",
				alternatePassivePeer: "sg1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPull,
				},
			},
			{
				activePeer:           "cbl2",
				passivePeer:          "sg2",
				alternatePassivePeer: "sg1",
				config: PeerReplicationConfig{
					direction: PeerReplicationDirectionPush,
				},
			},
		},
	},
}

// simpleTopologySpecifications represents simplified topologies to make testing the integration test code easier.
// nolint: unused
var simpleTopologySpecifications = []TopologySpecification{
	{

		/*
			+------+      +------+
			| cbs1 | <--> | cbs2 |
			+------+      +------+
		*/
		description: "CBS<->CBS",
		peers: map[string]PeerOptions{
			"cbs1": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID1},
			"cbs2": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID2, Symmetric: true}},
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
		description: "CBS+SG<->CBS",
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
	},
	{
		/*
			+ - - - - - - +      +- - - - - - -+
			'  cluster A  '      '  cluster B  '
			' +---------+ '      ' +---------+ '
			' |  cbs1   | ' <--> ' |  cbs2   | '
			' +---------+ '      ' +---------+ '
			' +---------+ '      ' +---------+ '
			' |   sg1   | '      ' |   sg2   | '
			' +---------+ '      ' +---------+ '
			+ - - - - - - +      +- - - - - - -+
		*/
		description: "CBS+SG<->CBS+SG",
		peers: map[string]PeerOptions{
			"cbs1": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID1},
			"sg1":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID1},
			"cbs2": {Type: PeerTypeCouchbaseServer, BucketID: PeerBucketID2, Symmetric: true},
			"sg2":  {Type: PeerTypeSyncGateway, BucketID: PeerBucketID2, Symmetric: true},
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
	},
}
