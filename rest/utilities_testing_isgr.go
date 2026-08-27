// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"net/http/httptest"
	"net/url"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestISGRPeerOpts has configuration for ISGR peers in a test setup. Everything else about the peers is set by
// SetupISGRPeersWithOpts.
type TestISGRPeerOpts struct {
	// supported protocols for the active peer for ISGR only. Nil means the default protocols; a non-empty slice forces a specific set of protocols.
	ActivePeerSupportedBLIPSubProtocols []string
	// UseDeltas enables delta sync on both peers - a replication only uses deltas if both ends have them enabled.
	UseDeltas bool
	// PassiveMaxWaitPending sets the passive peer's channel cache max_wait_pending, in milliseconds.
	PassiveMaxWaitPending *uint32
	// UserChannelAccess is list of channels the passive side user needs access to
	UserChannelAccess []string
	// AvoidUserCreation if true, don't create the user on the passive peer
	AvoidUserCreation bool
}

// deltaSyncConfig returns a delta sync config when enabled, and nil - the database default - when not.
func deltaSyncConfig(enabled bool) *DeltaSyncConfig {
	if !enabled {
		return nil
	}
	return &DeltaSyncConfig{Enabled: base.Ptr(true)}
}

// TestISGRPeers contains two RestTesters to be used for ISGR testing.
type TestISGRPeers struct {
	// ActiveRT represents the peer that initiates a replication.
	ActiveRT *RestTester
	// PassiveRT represents the peer that receives a replication.
	PassiveRT *RestTester
	// PassiveDBURL is used to create replications from ActiveRT to PassiveRT and contains a username+addr.
	PassiveDBURL string
	// opts and activeTestBucket are what nodes in the active cluster are built from - see activeRTConfig.
	opts             TestISGRPeerOpts
	activeTestBucket *base.TestBucket
}

// activeRTConfig returns the config for a node in the active cluster. Built per node, since RestTester rewrites the
// config it holds as it starts up.
func (p *TestISGRPeers) activeRTConfig() *RestTesterConfig {
	return &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Name:      "activedb",
			DeltaSync: deltaSyncConfig(p.opts.UseDeltas),
		}},
		SgReplicateEnabled:            true,
		SyncFn:                        channels.DocChannelsSyncFunction,
		ISGRSupportedBLIPSubprotocols: p.opts.ActivePeerSupportedBLIPSubProtocols,
		CustomTestBucket:              p.activeTestBucket.NoCloseClone(),
	}
}

// AddActiveRT starts another node in the active cluster and returns it, for tests that need more than one active
// node - replication rebalance, cross-node status. It shares ActiveRT's bucket, database and config group ID, which
// is what puts them in the same SGR cluster. Closed when the test ends, so don't close it.
func (p *TestISGRPeers) AddActiveRT(t *testing.T) *RestTester {
	activeRT := NewRestTester(t, p.activeRTConfig())
	t.Cleanup(activeRT.Close)
	// Trigger the lazy load of bucket for RestTester startup
	_ = activeRT.Bucket()
	return activeRT
}

type SGRTestRunner struct {
	// t is the subtest Run/RunSubprotocolV3/RunSubprotocolV4 is currently executing, or the test the runner was
	// created with outside of those. Failures raised against the parent from a subtest go to the wrong test.
	t                           atomic.Pointer[testing.T]
	initialisedInsideRunnerCode bool
	SkipSubtest                 map[string]bool
	SupportedSubprotocols       []string
}

// NewSGRTestRunner returns a new SGRTestRunner instance.
func NewSGRTestRunner(t *testing.T) *SGRTestRunner {
	// If BypassReleasedSequenceWait is true, tests like TestReplicationRebalancePush can miss sequences due to a
	// race between SGReplicateMgr assigning nodes and the sequenceAllocator/changeListener starting.
	//
	// See CBG-5267.
	previousBypassReleasedSequenceWait := db.BypassReleasedSequenceWait.Load()
	t.Cleanup(func() {
		db.BypassReleasedSequenceWait.Store(previousBypassReleasedSequenceWait)
	})
	db.BypassReleasedSequenceWait.Store(false)

	runner := &SGRTestRunner{
		SkipSubtest: make(map[string]bool),
	}
	runner.t.Store(t)
	return runner
}

// TB returns the testing.TB the runner is currently using.
func (runner *SGRTestRunner) TB() testing.TB {
	return runner.t.Load()
}

// Run will call create two subtests for revtree and version vector modes.
func (runner *SGRTestRunner) Run(test func(t *testing.T)) {
	if runner.initialisedInsideRunnerCode {
		require.FailNow(runner.TB(), "must not initialise SGRPeers outside Run() method")
	}

	parentT := runner.t.Load()
	runner.initialisedInsideRunnerCode = true
	defer func() {
		// reset bool post test run to ensure no one can setup SetupSGRPeers outside run method upon completion of Run()
		runner.initialisedInsideRunnerCode = false
		runner.t.Store(parentT)
	}()

	if !runner.SkipSubtest[RevtreeSubtestName] {
		parentT.Run(RevtreeSubtestName, func(t *testing.T) {
			runner.t.Store(t)
			runner.SupportedSubprotocols = []string{db.CBMobileReplicationV3.SubprotocolString()}
			test(t)
		})
	}
	if !runner.SkipSubtest[VersionVectorSubtestName] {
		parentT.Run(VersionVectorSubtestName, func(t *testing.T) {
			runner.t.Store(t)
			runner.SupportedSubprotocols = []string{db.CBMobileReplicationV4.SubprotocolString()}
			test(t)
		})
	}
}

// RunSubprotocolV3 forces a run of revtree protocol only.
func (runner *SGRTestRunner) RunSubprotocolV3(test func(t *testing.T)) {
	if runner.initialisedInsideRunnerCode {
		require.FailNow(runner.TB(), "must not initialise SGRPeers outside Run() method")
	}
	parentT := runner.t.Load()
	runner.initialisedInsideRunnerCode = true
	defer func() {
		// reset bool post test run to ensure no one can setup SetupSGRPeers outside
		runner.initialisedInsideRunnerCode = false
		runner.t.Store(parentT)
	}()

	if !runner.SkipSubtest[RevtreeSubtestName] {
		parentT.Run(RevtreeSubtestName, func(t *testing.T) {
			runner.t.Store(t)
			runner.SupportedSubprotocols = []string{db.CBMobileReplicationV3.SubprotocolString()}
			test(t)
		})
	}
}

// RunSubprotocolV4 forces a run of version vectors only.
func (runner *SGRTestRunner) RunSubprotocolV4(test func(t *testing.T)) {
	if runner.initialisedInsideRunnerCode {
		require.FailNow(runner.TB(), "must not initialise SGRPeers outside Run() method")
	}

	parentT := runner.t.Load()
	runner.initialisedInsideRunnerCode = true
	defer func() {
		// reset bool post test run to ensure no one can setup SetupSGRPeers outside run method upon completion of Run()
		runner.initialisedInsideRunnerCode = false
		runner.t.Store(parentT)
	}()

	if !runner.SkipSubtest[VersionVectorSubtestName] {
		parentT.Run(VersionVectorSubtestName, func(t *testing.T) {
			runner.t.Store(t)
			runner.SupportedSubprotocols = []string{db.CBMobileReplicationV4.SubprotocolString()}
			test(t)
		})
	}
}

// IsV4Protocol is true if the underlying RestTesters are using version vectors for their BLIP communication.
func (runner *SGRTestRunner) IsV4Protocol() bool {
	return slices.Contains(runner.SupportedSubprotocols, db.CBMobileReplicationV4.SubprotocolString())
}

// WaitForVersion will wait for revtree if v3 protocol or full version otherwise.
func (runner *SGRTestRunner) WaitForVersion(docID string, rt *RestTester, version DocVersion) {
	rt.TB().Helper()
	if !runner.IsV4Protocol() {
		// only assert on rev tree IDs when we're not replicating using v4 protocol
		rt.WaitForVersionRevIDOnly(docID, version)
		return
	}
	rt.WaitForVersion(docID, version)
}

func (runner *SGRTestRunner) WaitForTombstone(docID string, rt *RestTester, version DocVersion) {
	rt.TB().Helper()
	if !slices.Contains(runner.SupportedSubprotocols, db.CBMobileReplicationV4.SubprotocolString()) {
		// only assert on rev tree IDs when we're not replicating using v4 protocol
		rt.WaitForTombstoneRevIDOnly(docID, version)
		return
	}
	rt.WaitForTombstone(docID, version)
}

// Run is equivalent to testing.T.Run() but updates underlying the RestTesters' TB to the new testing.T.
func (p *TestISGRPeers) Run(t *testing.T, name string, test func(*testing.T)) {
	t.Run(name, func(t *testing.T) {
		originalActiveTB := p.ActiveRT.TB()
		defer p.ActiveRT.UpdateTB(originalActiveTB)
		originalPassiveTB := p.PassiveRT.TB()
		defer p.PassiveRT.UpdateTB(originalPassiveTB)
		p.ActiveRT.UpdateTB(t)
		p.PassiveRT.UpdateTB(t)
		test(t)
	})
}

// SetupSGRPeers sets up two rest testers to be used for ISGR testing:
//
//	ActiveRT:
//	  - backed by test bucket
//	PassiveRT:
//	  - backed by different test bucket
//	  - user 'alice' created with star channel access
//	  - http server wrapping the public API, PassiveDBURL targets its database as alice (e.g. http://alice:pass@host/db)
func (runner *SGRTestRunner) SetupSGRPeers(t *testing.T) *TestISGRPeers {
	return runner.SetupSGRPeersWithOptions(t, TestISGRPeerOpts{})
}

// SetupSGRPeersWithOptions is SetupSGRPeers with the configuration in opts. The runner's current subprotocols are
// used unless opts names its own.
func (runner *SGRTestRunner) SetupSGRPeersWithOptions(t *testing.T, opts TestISGRPeerOpts) *TestISGRPeers {
	if !runner.initialisedInsideRunnerCode {
		require.FailNow(runner.TB(), "must initialise ISGRPeers inside Run() method")
	}
	if len(opts.ActivePeerSupportedBLIPSubProtocols) == 0 {
		opts.ActivePeerSupportedBLIPSubProtocols = runner.SupportedSubprotocols
	}
	return SetupISGRPeersWithOpts(t, opts)
}

// SetupISGRPeersWithOpts sets up two rest testers backed by separate buckets.
// PassiveRT has user 'alice' created with star channel access and is listening on an HTTP port.
func SetupISGRPeersWithOpts(t *testing.T, opts TestISGRPeerOpts) *TestISGRPeers {
	ctx := base.TestCtx(t)
	// Set up passive RestTester (rt2)
	passiveRTConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Name:      "passivedb",
			DeltaSync: deltaSyncConfig(opts.UseDeltas),
		}},
		SyncFn: channels.DocChannelsSyncFunction,
	}
	if opts.PassiveMaxWaitPending != nil {
		passiveRTConfig.DatabaseConfig.CacheConfig = &CacheConfig{
			ChannelCacheConfig: &ChannelCacheConfig{
				MaxWaitPending: opts.PassiveMaxWaitPending,
			},
		}
	}
	// Hand the bucket to the RestTester rather than letting NewRestTester fetch its own: taking one and leaving it
	// unused reserves two pool buckets per peer, so a test asking for RequireNumTestBuckets(t, 2) needs four.
	passiveTestBucket := base.GetTestBucket(t)
	t.Cleanup(func() { passiveTestBucket.Close(ctx) })
	passiveRTConfig.CustomTestBucket = passiveTestBucket.NoCloseClone()
	passiveRT := NewRestTester(t, passiveRTConfig)
	t.Cleanup(passiveRT.Close)

	if !opts.AvoidUserCreation {
		if len(opts.UserChannelAccess) > 0 {
			// Create user with access to specified channels
			passiveRT.CreateUser("alice", opts.UserChannelAccess)
		} else {
			passiveRT.CreateUser("alice", []string{"*"})
		}
	}

	// Make passiveRT listen on an actual HTTP port, so it can receive the blipsync request from activeRT
	srv := httptest.NewServer(passiveRT.TestPublicHandler())
	t.Cleanup(srv.Close)

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/" + passiveRT.GetDatabase().Name)
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword("alice", RestTesterDefaultUserPassword)

	// As above for the active cluster's bucket, shared by every node in it.
	activeTestBucket := base.GetTestBucket(t)
	t.Cleanup(func() { activeTestBucket.Close(ctx) })

	// ActiveRT is built by AddActiveRT, like any node added later.
	peers := &TestISGRPeers{
		PassiveRT:        passiveRT,
		PassiveDBURL:     passiveDBURL.String(),
		opts:             opts,
		activeTestBucket: activeTestBucket,
	}
	peers.ActiveRT = peers.AddActiveRT(t)

	return peers
}

// DbReplicatorStats returns the replication stats for the given database and replication ID. Stats are cached per
// replication ID, so replicators needing independent stats need distinct IDs.
func DbReplicatorStats(t testing.TB, database *db.DatabaseContext, replicationID string) *base.DbReplicatorStats {
	dbstats, err := database.DbStats.DBReplicatorStats(replicationID)
	require.NoError(t, err)
	return dbstats
}
