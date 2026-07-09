// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

import (
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/rosmar"
	"github.com/stretchr/testify/require"
)

// This file supports the forceClockTie dimension of the TestMultiActorConflict* tests in
// multi_actor_conflict_test.go, to allow conflicting documents to be generated with matching timestamps.
//
// There are several independent clocks:
//
// - Sync Gateway, used for REST writes
// - Couchbase Lite mock client
// - Couchbase Server
//   - Couchbase Server has independent clocks per bucket
//   - Rosmar shares one clock across all buckets (implementation detail)
//
// controllableClockPeerCount returns the number of peers in the spec whose HLC clock forceHLCClockTieForTest can
// override. Used to skip the ClockTie subtest before setupTests, since skipping a topology mid-test (after
// peers and replications are configured but before they're started) trips replication teardown, which assumes
// StartReplications was called.
func controllableClockPeerCount(topologySpec TopologySpecification) (count int) {
	for _, opts := range topologySpec.peers {
		switch opts.Type {
		case PeerTypeSyncGateway, PeerTypeCouchbaseLite, PeerTypeCouchbaseLiteV3:
			count++
		case PeerTypeCouchbaseServer:
			// A Couchbase Server peer's clock is only controllable when it's backed by rosmar - see
			// forceHLCClockTieForTest.
			if base.UnitTestUrlIsWalrus() {
				count++
			}
		} // exhaustive:enforce
	}
	return count
}

// forceHLCClockTieForTest pins the HLC clocks of every controllable peer in the topology to the same fixed
// timestamp, forcing each of their next brand-new-document writes to generate an identical HLV current-version
// Value. Returns the number of peers whose clock was overridden.
//
// Unlike a Sync Gateway/Couchbase Lite peer's clock (a fresh instance per peer, discarded with the test), rosmar's
// clock is a single instance shared by every rosmar bucket in the whole test process. Freezing it here without
// restoring it would leak into every later test in the process (CAS values frozen in the past, or colliding with
// already-written docs) - so t is required purely to register a t.Cleanup that restores the real clock once this
// specific (sub)test finishes, before any sibling or later test runs.
func forceHLCClockTieForTest(t testing.TB, topology Topology) (forced int) {
	ctx := base.TestCtx(t)
	fixedTime := sgbucket.HLCWallClock()
	tieClock := func() uint64 { return fixedTime }
	rosmarClockSet := false
	for name, peer := range topology.peers.NonImportSortedPeers() {
		switch peer.Type() {
		case PeerTypeSyncGateway:
			p, ok := peer.(*SyncGatewayPeer)
			require.True(t, ok, "peer %s (%T) is a PeerTypeSyncGateway but not a *SyncGatewayPeer", name, peer)
			p.rt.GetDatabase().SetHLCClockForTest(tieClock)
			forced++
		case PeerTypeCouchbaseLite, PeerTypeCouchbaseLiteV3:
			p, ok := peer.(*CouchbaseLiteMockPeer)
			require.True(t, ok, "peer %s (%T) is a %s but not a *CouchbaseLiteMockPeer", name, peer, peer.Type())
			p.getSingleSGBlipClient().btc.SetHLCClockForTest(tieClock)
			forced++
		case PeerTypeCouchbaseServer:
			if !base.UnitTestUrlIsWalrus() {
				base.InfofCtx(ctx, base.KeySGTest, "forceHLCClockTieForTest: not overriding clock for peer %s (%T), Couchbase Server-backed peers can only have their clock forced when backed by rosmar", name, peer)
				continue
			}
			if !rosmarClockSet {
				rosmar.SetClockForTest(tieClock)
				t.Cleanup(func() { rosmar.SetClockForTest(sgbucket.HLCWallClock) })
				rosmarClockSet = true
			}
			forced++
		} // exhaustive:enforce
	}
	return forced
}
