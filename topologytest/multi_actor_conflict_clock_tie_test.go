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

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
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
// Value.
//
// Rosmar's clock is shared by every rosmar bucket in the process, so t registers a cleanup that restores it before
// any later test runs.
//
// Wall clock jumps (for example an NTP step on a CI agent):
//   - Forward, before this call: safe. Every frozen clock takes the same, later fixed time.
//   - Backward, before this call: safe for a new document on Sync Gateway and Couchbase Lite peers, because the
//     fixed time starts above each peer's highest HLC value. An update can still miss the tie when the document's
//     HLV gives a Sync Gateway peer a higher floor, for example an imported CAS.
//   - Backward, rosmar: not covered. Rosmar has no accessor for its highest CAS, so a doc can get a CAS below its
//     previous CAS, and the mock XDCR then skips that write.
//   - Any jump while frozen: no effect on frozen clocks. A Couchbase Server bucket keeps its own clock, and
//     correctVersionAheadOfCAS waits (up to 1s) when a Sync Gateway version is ahead of the server CAS.
func forceHLCClockTieForTest(t testing.TB, topology Topology) {
	t.Helper()
	ctx := base.TestCtx(t)
	var highestHLC uint64
	var setClocks []func(func() uint64)
	rosmarClockSet := false
	for name, peer := range topology.peers.NonImportSortedPeers() {
		switch peer.Type() {
		case PeerTypeSyncGateway:
			p, ok := peer.(*SyncGatewayPeer)
			require.True(t, ok, "peer %s (%T) is a PeerTypeSyncGateway but not a *SyncGatewayPeer", name, peer)
			dbCtx := p.rt.GetDatabase()
			highestHLC = max(highestHLC, dbCtx.GetHLCValueForTest(0))
			setClocks = append(setClocks, dbCtx.SetHLCClockForTest)
		case PeerTypeCouchbaseLite, PeerTypeCouchbaseLiteV3:
			p, ok := peer.(*CouchbaseLiteMockPeer)
			require.True(t, ok, "peer %s (%T) is a %s but not a *CouchbaseLiteMockPeer", name, peer, peer.Type())
			btc := p.getSingleSGBlipClient().btc
			highestHLC = max(highestHLC, btc.GetHLCValueForTest())
			setClocks = append(setClocks, btc.SetHLCClockForTest)
		case PeerTypeCouchbaseServer:
			if !base.UnitTestUrlIsWalrus() {
				base.InfofCtx(ctx, base.KeySGTest, "forceHLCClockTieForTest: not overriding clock for peer %s (%T), Couchbase Server-backed peers can only have their clock forced when backed by rosmar", name, peer)
				continue
			}
			if !rosmarClockSet {
				setClocks = append(setClocks, func(clock func() uint64) { sgtest.SetRosmarClockForTest(t, clock) })
				rosmarClockSet = true
			}
		} // exhaustive:enforce
	}
	// HLC.Now drops the logical bits of the clock value, so the next physical tick keeps every peer strictly above.
	fixedTime := sgtest.NextClockTickForTest(highestHLC)
	tieClock := func() uint64 { return fixedTime }
	for _, setClock := range setClocks {
		setClock(tieClock)
	}
}
