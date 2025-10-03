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
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

// TestISGRPeerOpts has configuration for ISGR peers in a test setup.
type TestISGRPeerOpts struct {
	// supported protocols for the active peer for ISGR only. Empty means the default protocols.
	ActivePeerSupportedBLIPSubProtocols []string
	// UseDeltas indicates whether to enable delta sync on the ISGR replication
	UseDeltas bool
	// ActiveRestTesterConfig allows passing a custom RestTesterConfig for the active peer.
	ActiveRestTesterConfig *RestTesterConfig
	// PassiveRestTesterConfig allows passing a custom RestTesterConfig for the passive peer.
	PassiveRestTesterConfig *RestTesterConfig
	// UserChannelAccess is list of channels the passive side user needs access to
	UserChannelAccess []string
	// AvoidUserCreation if true, don't create the user on the passive peer
	AvoidUserCreation bool
}

// TestISGRPeers contains two RestTesters to be used for ISGR testing.
type TestISGRPeers struct {
	// ActiveRT represents the peer that initiatiates a replication.
	ActiveRT *RestTester
	// PassiveRT represents the peer that receives a replication.
	PassiveRT *RestTester
	// PassiveDBURL is used to create replications from ActiveRT to PassiveRT and contains a username+addr.
	PassiveDBURL string
}

type SGRTestRunner struct {
	t                           *testing.T
	initialisedInsideRunnerCode bool
	SkipSubtest                 map[string]bool
	SupportedSubprotocols       []string
}

// NewSGRTestRunner returns a new SGRTestRunner instance.
func NewSGRTestRunner(t *testing.T) *SGRTestRunner {
	return &SGRTestRunner{
		t:           t,
		SkipSubtest: make(map[string]bool),
	}
}

func (runner *SGRTestRunner) TB() testing.TB {
	return runner.t
}

func (runner *SGRTestRunner) Run(test func(t *testing.T)) {
	if runner.initialisedInsideRunnerCode {
		require.FailNow(runner.TB(), "must not initialise SGRPeers outside Run() method")
	}

	runner.initialisedInsideRunnerCode = true
	defer func() {
		// reset bool post test run to ensure no one can setup SetupSGRPeers outside run method upon completion of Run()
		runner.initialisedInsideRunnerCode = false
	}()

	if !runner.SkipSubtest[RevtreeSubtestName] {
		runner.t.Run(RevtreeSubtestName, func(t *testing.T) {
			runner.SupportedSubprotocols = []string{db.CBMobileReplicationV3.SubprotocolString()}
			test(t)
		})
	}
	if !runner.SkipSubtest[VersionVectorSubtestName] {
		runner.t.Run(VersionVectorSubtestName, func(t *testing.T) {
			runner.SupportedSubprotocols = []string{db.CBMobileReplicationV4.SubprotocolString()}
			test(t)
		})
	}
}

func (runner *SGRTestRunner) RunSubprotocolV3(test func(t *testing.T)) {
	if runner.initialisedInsideRunnerCode {
		require.FailNow(runner.TB(), "must not initialise SGRPeers outside Run() method")
	}
	runner.initialisedInsideRunnerCode = true
	defer func() {
		// reset bool post test run to ensure no one can setup SetupSGRPeers outside
		runner.initialisedInsideRunnerCode = false
	}()
	if !runner.SkipSubtest[RevtreeSubtestName] {
		runner.t.Run(RevtreeSubtestName, func(t *testing.T) {
			runner.SupportedSubprotocols = []string{db.CBMobileReplicationV3.SubprotocolString()}
			test(t)
		})
	}
}

func (runner *SGRTestRunner) RunSubprotocolV4(test func(t *testing.T)) {
	if runner.initialisedInsideRunnerCode {
		require.FailNow(runner.TB(), "must not initialise SGRPeers outside Run() method")
	}

	runner.initialisedInsideRunnerCode = true
	defer func() {
		// reset bool post test run to ensure no one can setup SetupSGRPeers outside run method upon completion of Run()
		runner.initialisedInsideRunnerCode = false
	}()

	if !runner.SkipSubtest[VersionVectorSubtestName] {
		runner.t.Run(VersionVectorSubtestName, func(t *testing.T) {
			runner.SupportedSubprotocols = []string{db.CBMobileReplicationV4.SubprotocolString()}
			test(t)
		})
	}
}

func (runner *SGRTestRunner) IsV4Protocol() bool {
	return slices.Contains(runner.SupportedSubprotocols, db.CBMobileReplicationV4.SubprotocolString())
}

func (runner *SGRTestRunner) WaitForVersion(docID string, rt *RestTester, version DocVersion) {
	if !slices.Contains(runner.SupportedSubprotocols, db.CBMobileReplicationV4.SubprotocolString()) {
		// only assert on rev tree IDs when we're not replicating using v4 protocol
		rt.WaitForVersionRevIDOnly(docID, version)
		return
	}
	rt.WaitForVersion(docID, version)
}

func (runner *SGRTestRunner) WaitForTombstone(docID string, rt *RestTester, version DocVersion) {
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
		test(t)
	})
}

// SetupSGRPeers sets up two rest testers to be used for ISGR testing with the following configuration:
//
//	activeRT:
//	  - backed by test bucket
//	passiveRT:
//	  - backed by different test bucket
//	  - user 'alice' created with star channel access
//	  - http server wrapping the public API, remoteDBURLString targets the rt2 database as user alice (e.g. http://alice:pass@host/db)
func (runner *SGRTestRunner) SetupSGRPeers(t *testing.T) (activeRT *RestTester, passiveRT *RestTester, remoteDBURLString string) {
	if !runner.initialisedInsideRunnerCode {
		require.FailNow(runner.TB(), "must initialise ISGRPeers inside Run() method")
	}
	peers := SetupISGRPeersWithOpts(t, TestISGRPeerOpts{
		ActivePeerSupportedBLIPSubProtocols: runner.SupportedSubprotocols,
	})
	return peers.ActiveRT, peers.PassiveRT, peers.PassiveDBURL
}

func (runner *SGRTestRunner) SetupSGRPeersWithOptions(t *testing.T, opts TestISGRPeerOpts) (activeRT *RestTester, passiveRT *RestTester, remoteDBURLString string) {
	if !runner.initialisedInsideRunnerCode {
		require.FailNow(runner.TB(), "must initialise ISGRPeers inside Run() method")
	}
	if len(opts.ActivePeerSupportedBLIPSubProtocols) == 0 {
		opts.ActivePeerSupportedBLIPSubProtocols = runner.SupportedSubprotocols
	}
	peers := SetupISGRPeersWithOpts(t, opts)
	return peers.ActiveRT, peers.PassiveRT, peers.PassiveDBURL
}

// SetupISGRPeersWithOpts sets up two rest testers backed by separate buckets.
// PassiveRT has user 'alice' created with star channel access and is listening on an HTTP port.
func SetupISGRPeersWithOpts(t *testing.T, opts TestISGRPeerOpts) TestISGRPeers {
	ctx := base.TestCtx(t)
	// Set up passive RestTester (rt2)
	passiveTestBucket := base.GetTestBucket(t)
	t.Cleanup(func() { passiveTestBucket.Close(ctx) })

	var passiveRTConfig *RestTesterConfig
	if opts.PassiveRestTesterConfig != nil {
		passiveRTConfig = opts.PassiveRestTesterConfig
	} else {
		passiveRTConfig = &RestTesterConfig{
			CustomTestBucket: passiveTestBucket.NoCloseClone(),
			DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
				Name: "passivedb",
			}},
			SyncFn: channels.DocChannelsSyncFunction,
		}
	}
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
	passiveDBURL, _ := url.Parse(srv.URL + "/" + passiveRT.GetDatabase().Name)
	passiveDBURL.User = url.UserPassword("alice", RestTesterDefaultUserPassword)

	activeTestBucket := base.GetTestBucket(t)
	t.Cleanup(func() { activeTestBucket.Close(ctx) })

	var activeRTConfig *RestTesterConfig
	if opts.ActiveRestTesterConfig != nil {
		activeRTConfig = opts.ActiveRestTesterConfig
	} else {
		activeRTConfig = &RestTesterConfig{
			DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
				Name: "activedb",
			}},
			CustomTestBucket:   activeTestBucket.NoCloseClone(),
			SgReplicateEnabled: true,
			SyncFn:             channels.DocChannelsSyncFunction,
		}
	}
	activeRT := NewRestTester(t, activeRTConfig)
	t.Cleanup(activeRT.Close)
	// Initialize RT and bucket
	_ = activeRT.Bucket()
	activeRT.GetDatabase().SGReplicateMgr.SupportedBLIPSubprotocols = opts.ActivePeerSupportedBLIPSubProtocols

	return TestISGRPeers{
		ActiveRT:     activeRT,
		PassiveRT:    passiveRT,
		PassiveDBURL: passiveDBURL.String(),
	}
}
