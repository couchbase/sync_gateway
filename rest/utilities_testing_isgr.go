// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"context"
	"maps"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// RequireRevTreeConvergence waits for two or more peers to agree on a document's current revision, then will
// assert that any other leaf revision(s) are tombstone revisions. Uses GetDocSyncData to avoid read side
// repair of corrupt metadata through GetDocumentWithRaw. It will return the converged revID and the per peer sync data
// for callers to assert on.
func RequireRevTreeConvergence(t *testing.T, docID string, peers ...*RestTester) (convergedRev string, perPeer []db.SyncData) {
	t.Helper()
	require.GreaterOrEqual(t, len(peers), 2, "convergence needs at least two peers")

	collections := make([]*db.DatabaseCollectionWithUser, len(peers))
	ctxs := make([]context.Context, len(peers))
	for i, peer := range peers {
		collections[i], ctxs[i] = peer.GetSingleTestDatabaseCollectionWithUser()
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var first string
		for i := range peers {
			syncData, err := collections[i].GetDocSyncData(ctxs[i], docID)
			if !assert.NoError(c, err, "peer %d does not have doc %q yet", i, docID) {
				return
			}
			if i == 0 {
				first = syncData.GetRevTreeID()
				continue
			}
			assert.Equal(c, first, syncData.GetRevTreeID(), "peer %d has not converged with peer 0", i)
		}
	}, 20*time.Second, 100*time.Millisecond)

	perPeer = make([]db.SyncData, len(peers))
	for i := range peers {
		syncData, err := collections[i].GetDocSyncData(ctxs[i], docID)
		require.NoError(t, err)
		perPeer[i] = syncData

		if i == 0 {
			convergedRev = syncData.GetRevTreeID()
		}
		require.Equal(t, convergedRev, syncData.GetRevTreeID(), "peer %d disagrees on the current revision", i)

		current, ok := syncData.History[convergedRev]
		require.True(t, ok, "peer %d names %q as current but does not have it in its rev tree", i, convergedRev)
		assert.False(t, current.Deleted, "peer %d converged on a tombstone, %q", i, convergedRev)

		for _, leaf := range syncData.History.GetLeaves() {
			if leaf == convergedRev {
				continue
			}
			assert.True(t, syncData.History[leaf].Deleted,
				"peer %d still holds live conflicting leaf %q alongside converged revision %q", i, leaf, convergedRev)
		}
	}
	return convergedRev, perPeer
}

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
	return &DeltaSyncConfig{Enabled: new(true)}
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

// ExpectedISGRDoc describes the state a document must be in on a peer after replication. Version is always checked;
// the remaining fields are checked when set, except in RequireDocReplicated where every field comes from the source.
type ExpectedISGRDoc struct {
	// Version is waited for before anything else is asserted - revTreeID only in v3, revTreeID and CV in v4.
	Version DocVersion
	// Deleted is true if the current revision must be a tombstone.
	Deleted bool
	// HLV is compared in full apart from cvCAS, which each peer sets from its own write. Only checked in v4, since in
	// v3 the receiving peer mints a new HLV of its own.
	HLV *db.HybridLogicalVector
	// RevChain is the ancestry of the current revision, current revision first.
	RevChain []string
	// Channels are the channels the document is currently in, ignoring channels it has been removed from.
	Channels []string
	// Body is the expected document body as JSON. Not checked for tombstones.
	Body string
	// Attachments maps attachment name to digest.
	Attachments map[string]string
}

// RequireDocReplicated waits for version to arrive on dest, then requires dest to hold the same document as source:
// HLV (v4), rev tree ancestry, channels, body and attachments. Both peers are waited on to reach version, so it can't
// be used where the peers legitimately end up different - use RequireDoc with an explicit expectation there. Only the
// parts of version that are set are waited on, so a revTreeID alone is enough.
func (runner *SGRTestRunner) RequireDocReplicated(docID string, source, dest *RestTester, version DocVersion) *db.Document {
	t := dest.TB()
	t.Helper()
	// Wait on both peers: source's state can itself be the product of the replication (a conflict resolution, say),
	// and dest can already be at version before the replication has done anything.
	runner.waitForDocVersion(source, docID, version)
	runner.waitForDocVersion(dest, docID, version)
	return runner.RequireDoc(docID, dest, ExpectedISGRDocFromPeer(t, source, docID))
}

// RequireDoc waits for exp.Version to arrive on rt, then requires the document to match every field set in exp.
func (runner *SGRTestRunner) RequireDoc(docID string, rt *RestTester, exp ExpectedISGRDoc) *db.Document {
	t := rt.TB()
	t.Helper()
	runner.waitForDocVersion(rt, docID, exp.Version)

	doc := rt.GetDocument(docID)
	actual := expectedISGRDocFromDoc(t, rt.Context(), doc)
	peer := rt.GetDatabase().Name

	require.NotNil(t, doc.HLV, "doc %q has no HLV on %s", docID, peer)
	if runner.IsV4Protocol() {
		if exp.HLV != nil {
			assert.True(t, hlvEqualAllowingEncodedRevs(t, exp.HLV, doc.HLV, exp.RevChain),
				"HLV mismatch for doc %q on %s. Expected: %s, Actual: %s", docID, peer, exp.HLV.HLVDebugString(), doc.HLV.HLVDebugString())
		}
	} else {
		assert.Equal(t, rt.GetDatabase().EncodedSourceID, doc.HLV.SourceID,
			"doc %q on %s should have a CV minted locally in v3, HLV: %s", docID, peer, doc.HLV.HLVDebugString())
	}

	assert.Equal(t, exp.Deleted, actual.Deleted, "deleted mismatch for doc %q on %s", docID, peer)
	if exp.RevChain != nil {
		assert.Equal(t, exp.RevChain, actual.RevChain, "rev tree ancestry mismatch for doc %q on %s", docID, peer)
	}
	// Any branch other than the current revision's must have been resolved to a tombstone.
	for _, leaf := range doc.History.GetLeaves() {
		if leaf != doc.GetRevTreeID() {
			assert.True(t, doc.History[leaf].Deleted, "doc %q on %s has live conflicting leaf %q alongside current revision %q",
				docID, peer, leaf, doc.GetRevTreeID())
		}
	}
	if exp.Channels != nil {
		assert.ElementsMatch(t, exp.Channels, actual.Channels, "channel mismatch for doc %q on %s", docID, peer)
	}
	if exp.Body != "" && !exp.Deleted {
		assert.JSONEq(t, exp.Body, actual.Body, "body mismatch for doc %q on %s", docID, peer)
	}
	if exp.Attachments != nil {
		assert.Equal(t, exp.Attachments, actual.Attachments, "attachment mismatch for doc %q on %s", docID, peer)
	}
	return doc
}

// waitForDocVersion waits for docID on rt to be at version - revTreeID only in v3, where the receiving peer mints its own
// CV. It reads the document directly rather than over REST so that waiting doesn't populate the revision cache, and so
// that tombstones and live documents are waited for the same way.
func (runner *SGRTestRunner) waitForDocVersion(rt *RestTester, docID string, version DocVersion) {
	t := rt.TB()
	t.Helper()
	checkCV := runner.IsV4Protocol() && !version.CV.IsEmpty()
	require.True(t, version.RevTreeID != "" || checkCV, "nothing to wait for in version %#v", version)
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalSync)
		if !assert.NoError(c, err, "doc %q not found on %s", docID, rt.GetDatabase().Name) {
			return
		}
		if version.RevTreeID != "" {
			assert.Equal(c, version.RevTreeID, doc.GetRevTreeID(), "doc %q on %s", docID, rt.GetDatabase().Name)
		}
		if checkCV {
			assert.Equal(c, version.CV.String(), doc.HLV.GetCurrentVersionString(), "doc %q on %s", docID, rt.GetDatabase().Name)
		}
	}, 10*time.Second, 50*time.Millisecond)
}

// hlvEqualAllowingEncodedRevs is HybridLogicalVector.Equal, except that actual may also carry pv entries for the
// revTreeID-encoded version of revisions in revChain. A peer that received a document while it had no HLV stores its
// revTreeID as an encoded CV, and keeps that in pv once the document is updated - but the peer that wrote the update
// never stored the encoded version, since it only ever knew the revision through its rev tree. Both peers know the
// same versions, so the HLVs are equivalent.
func hlvEqualAllowingEncodedRevs(t testing.TB, expected, actual *db.HybridLogicalVector, revChain []string) bool {
	t.Helper()
	if expected.SourceID != actual.SourceID || expected.Version != actual.Version ||
		!maps.Equal(expected.MergeVersions, actual.MergeVersions) {
		return false
	}
	// Every encoded version shares one source ID, so collect them as versions rather than keyed by source.
	encodedRevs := make(map[db.Version]struct{}, len(revChain))
	for _, revID := range revChain {
		encoded, err := db.LegacyRevToRevTreeEncodedVersion(revID)
		if err != nil {
			// A revTreeID without a hex digest (a fake one written by a test) can't be encoded, so no peer can hold an
			// encoded version of it.
			continue
		}
		encodedRevs[encoded] = struct{}{}
	}
	for source, value := range expected.PreviousVersions {
		if actualValue, ok := actual.PreviousVersions[source]; !ok || actualValue != value {
			return false
		}
	}
	for source, value := range actual.PreviousVersions {
		if _, ok := expected.PreviousVersions[source]; ok {
			continue
		}
		if _, ok := encodedRevs[db.Version{SourceID: source, Value: value}]; !ok {
			return false
		}
	}
	return true
}

// ExpectedISGRDocFromPeer returns the current state of docID on rt as an ExpectedISGRDoc. Taken before a replication
// starts, it lets RequireDoc assert that a peer the replication shouldn't touch is left unchanged.
func ExpectedISGRDocFromPeer(t testing.TB, rt *RestTester, docID string) ExpectedISGRDoc {
	t.Helper()
	return expectedISGRDocFromDoc(t, rt.Context(), rt.GetDocument(docID))
}

// expectedISGRDocFromDoc returns the replication-relevant state of doc, with every field populated.
func expectedISGRDocFromDoc(t testing.TB, ctx context.Context, doc *db.Document) ExpectedISGRDoc {
	t.Helper()
	hlv := doc.HLV.Copy()
	if hlv == nil {
		// A document written before HLVs existed replicates with a CV encoded from its revTreeID, so that's the HLV a
		// peer receiving it must end up with.
		encodedCV, err := db.LegacyRevToRevTreeEncodedVersion(doc.GetRevTreeID())
		require.NoError(t, err)
		hlv = db.NewHybridLogicalVector()
		require.NoError(t, hlv.AddVersion(encodedCV))
	}
	exp := ExpectedISGRDoc{
		Version:     DocVersion{RevTreeID: doc.GetRevTreeID(), CV: *hlv.ExtractCurrentVersionFromHLV()},
		Deleted:     doc.IsDeleted(),
		HLV:         hlv,
		RevChain:    []string{},
		Channels:    []string{},
		Attachments: map[string]string{},
	}
	for revID := doc.GetRevTreeID(); revID != ""; {
		revInfo, ok := doc.History[revID]
		require.True(t, ok, "rev %q missing from rev tree of doc %q", revID, doc.ID)
		exp.RevChain = append(exp.RevChain, revID)
		revID = revInfo.Parent
	}
	for channel, removal := range doc.Channels {
		if removal == nil {
			exp.Channels = append(exp.Channels, channel)
		}
	}
	slices.Sort(exp.Channels)
	for name, meta := range doc.Attachments() {
		metaMap, ok := meta.(map[string]any)
		require.True(t, ok, "attachment %q of doc %q has unexpected metadata %T", name, doc.ID, meta)
		digest, _ := metaMap["digest"].(string)
		exp.Attachments[name] = digest
	}
	if !exp.Deleted {
		body, err := doc.BodyBytes(ctx)
		require.NoError(t, err)
		exp.Body = string(body)
	}
	return exp
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

// WaitForISGRPullSequence waits for the pull replication on rt to have checkpointed the given remote sequence.
// A pulled document is written before its sequence reaches the checkpointer, so it can be readable on rt while the
// replication is still unable to checkpoint it - stopping in that window rewinds the replication on the next start.
func WaitForISGRPullSequence(rt *RestTester, replicationID string, seq uint64) {
	rt.TB().Helper()
	expectedSeq := strconv.FormatUint(seq, 10)
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		assert.Equal(c, expectedSeq, rt.GetReplicationStatus(replicationID).LastSeqPull)
	}, 20*time.Second, 10*time.Millisecond)
}

// DbReplicatorStats returns the replication stats for the given database and replication ID. Stats are cached per
// replication ID, so replicators needing independent stats need distinct IDs.
func DbReplicatorStats(t testing.TB, database *db.DatabaseContext, replicationID string) *base.DbReplicatorStats {
	dbstats, err := database.DbStats.DBReplicatorStats(replicationID)
	require.NoError(t, err)
	return dbstats
}
