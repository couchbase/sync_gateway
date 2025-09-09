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
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// TestISGRPeerOpts has configuration for ISGR peers in a test setup.
type TestISGRPeerOpts struct {
	// supported protocols for the active peer for ISGR only. Empty means the default protocols.
	ActivePeerSupportedBLIPSubProtocols []string
}

// TestISGRPeers contains two RestTesters to be used for ISGR testing.
type TestISGRPeers struct {
	// ActiveRT represents the peer that initiatiates a replication.
	ActiveRT *RestTester
	// PassiveRT represents the peer that receives a replication.
	PassiveRT *RestTester
	// PassiveDBURL is used to create replications from ActiveRT to PassiveRT and contains a username+addr.
		originalPassiveTB := p.PassiveRT.TB()
		defer p.PassiveRT.UpdateTB(originalPassiveTB)

// Run is equivalent to testing.T.Run() but updates underlying the RestTesters' TB to the new testing.T.
func (p *TestISGRPeers) Run(t *testing.T, name string, test func(*testing.T)) {
	t.Run(name, func(t *testing.T) {
		originalActiveTB := p.ActiveRT.TB()
		defer p.ActiveRT.UpdateTB(originalActiveTB)
		originalActiveTB = p.PassiveRT.TB()
		defer p.PassiveRT.UpdateTB(originalActiveTB)
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
func SetupSGRPeers(t *testing.T) (activeRT *RestTester, passiveRT *RestTester, remoteDBURLString string) {
	peers := SetupISGRPeersWithOpts(t, TestISGRPeerOpts{})
	return peers.ActiveRT, peers.PassiveRT, peers.PassiveDBURL
}

// SetupISGRPeersWithOpts sets up two rest testers backed by separate buckets.
// PassiveRT has user 'alice' created with star channel access and is listening on an HTTP port.
func SetupISGRPeersWithOpts(t *testing.T, opts TestISGRPeerOpts) TestISGRPeers {
	ctx := base.TestCtx(t)
	// Set up passive RestTester (rt2)
	passiveTestBucket := base.GetTestBucket(t)
	t.Cleanup(func() { passiveTestBucket.Close(ctx) })

	passiveRTConfig := &RestTesterConfig{
		CustomTestBucket: passiveTestBucket.NoCloseClone(),
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Name: "passivedb",
		}},
		SyncFn: channels.DocChannelsSyncFunction,
	}
	passiveRT := NewRestTester(t, passiveRTConfig)
	t.Cleanup(passiveRT.Close)
	passiveRT.CreateUser("alice", []string{"*"})

	// Make passiveRT listen on an actual HTTP port, so it can receive the blipsync request from activeRT
	srv := httptest.NewServer(passiveRT.TestPublicHandler())
	t.Cleanup(srv.Close)

	// Build passiveDBURL with basic auth creds
	passiveDBURL, _ := url.Parse(srv.URL + "/" + passiveRT.GetDatabase().Name)
	passiveDBURL.User = url.UserPassword("alice", RestTesterDefaultUserPassword)

	activeTestBucket := base.GetTestBucket(t)
	t.Cleanup(func() { activeTestBucket.Close(ctx) })
	activeRTConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Name: "activedb",
		}},
		CustomTestBucket:   activeTestBucket.NoCloseClone(),
		SgReplicateEnabled: true,
		SyncFn:             channels.DocChannelsSyncFunction,
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
