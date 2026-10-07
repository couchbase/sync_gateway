// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/cbltestclient"
	"github.com/couchbase/sync_gateway/testing/require"
)

// envRealCouchbaseLite makes PeerTypeCouchbaseLite peers real Couchbase Lite clients, run by
// cbltestclient, instead of CouchbaseLiteMockPeer.
const envRealCouchbaseLite = "SG_TEST_TOPOLOGY_REAL_CBL"

// useRealCouchbaseLite reports whether PeerTypeCouchbaseLite peers are real Couchbase Lite clients.
func useRealCouchbaseLite() bool {
	enabled, _ := strconv.ParseBool(os.Getenv(envRealCouchbaseLite))
	return enabled
}

// cblUsername is the Sync Gateway user every Couchbase Lite peer replicates as.
const cblUsername = "user"

// CouchbaseLitePeer is a real Couchbase Lite 4.x client, run by a Couchbase Lite test server of its
// own.
//
// The test server cannot read back a tombstone: a deleted document looks the same as one that never
// existed.  So this peer cannot report the version of a delete, and can only check that a document
// is gone, not which version deleted it.  DeleteDocument returns DocMetadata with VersionUnknown set
// for this reason.
type CouchbaseLitePeer struct {
	t                  atomic.Pointer[testing.T]
	name               string
	symmetricRedundant bool
	server             *cbltestclient.Server
	// database is the name of this peer's database on the test server.
	database string
	// sourceID is learned from the first document this peer writes, since the test server does not
	// report it any other way.
	sourceID     string
	replications []*CouchbaseLiteReplication
}

// newCouchbaseLitePeer starts a test server of its own for the peer, which is stopped and deleted
// when the test that made the peer ends.
func newCouchbaseLitePeer(t *testing.T, name string, symmetricRedundant bool) *CouchbaseLitePeer {
	// The database name is only seen by this peer's own server, so the peer name is enough.
	database := cblDatabaseName(name)
	p := &CouchbaseLitePeer{
		name:               name,
		symmetricRedundant: symmetricRedundant,
		server: cbltestclient.NewServer(t, cbltestclient.Options{Databases: map[string]cbltestclient.DatabaseSpec{
			database: {Collections: []string{cblCollectionName(getSingleDsName())}},
		}}),
		database: database,
	}
	p.UpdateTB(t)
	return p
}

func (p *CouchbaseLitePeer) String() string {
	return fmt.Sprintf("%s (sourceid:%s)", p.name, p.SourceID())
}

// getLatestDocVersion returns the current properties and version of a document, or false if the
// document does not exist or is a tombstone.
func (p *CouchbaseLitePeer) getLatestDocVersion(t require.TestingT, dsName sgbucket.DataStoreName, docID string) (db.Body, DocMetadata, bool) {
	doc, err := p.server.Client.GetDocument(p.Context(), p.database, p.ref(dsName, docID))
	// A replicator saving the document while the test server reads it makes the read fail, and the
	// next one sees the saved document.
	for attempt := 1; isDocumentOutdated(err) && attempt < maxOutdatedReadAttempts; attempt++ {
		doc, err = p.server.Client.GetDocument(p.Context(), p.database, p.ref(dsName, docID))
	}
	if cbltestclient.IsDocumentNotFound(err) {
		return nil, DocMetadata{}, false
	}
	require.NoError(t, err)
	hlv, _, err := doc.HLV()
	require.NoError(t, err)
	meta := DocMetadataFromDocVersion(p.TB(), docID, hlv, db.DocVersion{CV: *hlv.ExtractCurrentVersionFromHLV()})
	return doc.Properties(), meta, true
}

// GetDocument returns the latest version of a document. The test will fail the document does not exist.
func (p *CouchbaseLitePeer) GetDocument(dsName sgbucket.DataStoreName, docID string) (DocMetadata, db.Body) {
	p.TB().Helper()
	body, meta, exists := p.getLatestDocVersion(p.TB(), dsName, docID)
	require.True(p.TB(), exists, "docID:%s not found on %s", docID, p)
	return meta, body
}

// GetDocumentIfExists returns the latest version of a document if it exists.  A tombstone is
// reported as not existing, since the test server cannot read one back.
func (p *CouchbaseLitePeer) GetDocumentIfExists(dsName sgbucket.DataStoreName, docID string) (DocMetadata, *db.Body, bool) {
	p.TB().Helper()
	body, meta, exists := p.getLatestDocVersion(p.TB(), dsName, docID)
	if !exists {
		return DocMetadata{}, nil, false
	}
	return meta, &body, true
}

// CreateDocument creates a document on the peer. The test will fail if the document already exists.
func (p *CouchbaseLitePeer) CreateDocument(dsName sgbucket.DataStoreName, docID string, body []byte) BodyAndVersion {
	p.TB().Helper()
	_, _, exists := p.getLatestDocVersion(p.TB(), dsName, docID)
	require.False(p.TB(), exists, "docID:%s already exists on %s", docID, p)
	return p.write(dsName, docID, body, nil)
}

// WriteDocument upserts a document to the peer. The test will fail if the write does not succeed.
func (p *CouchbaseLitePeer) WriteDocument(dsName sgbucket.DataStoreName, docID string, body []byte) BodyAndVersion {
	p.TB().Helper()
	current, _, _ := p.getLatestDocVersion(p.TB(), dsName, docID)
	return p.write(dsName, docID, body, current)
}

// write sets a document's properties and returns the version it ended up with.
func (p *CouchbaseLitePeer) write(dsName sgbucket.DataStoreName, docID string, body []byte, current db.Body) BodyAndVersion {
	p.TB().Helper()
	var properties map[string]any
	require.NoError(p.TB(), base.JSONUnmarshal(body, &properties))
	// The test server merges properties into the document rather than replacing its body, so a
	// property the new body drops would be left behind.  The topology tests always write the same
	// set of properties, so this only catches a test that starts doing otherwise.
	for key := range current {
		_, kept := properties[key]
		require.True(p.TB(), kept, "%s cannot remove property %q from docID:%s, the test server only merges properties", p, key, docID)
	}
	require.NoError(p.TB(), p.server.Client.UpdateDatabase(p.Context(), p.database, []cbltestclient.DatabaseUpdateItem{{
		Type:              cbltestclient.UpdateTypeUpdate,
		Collection:        cblCollectionName(dsName),
		DocumentID:        docID,
		UpdatedProperties: []map[string]any{properties},
	}}))

	_, meta, exists := p.getLatestDocVersion(p.TB(), dsName, docID)
	require.True(p.TB(), exists, "docID:%s not found on %s after writing it", docID, p)
	if p.sourceID == "" {
		p.sourceID = meta.CV(p.TB()).SourceID
	}
	base.InfofCtx(p.Context(), base.KeySGTest, "%s: Wrote document %s with %#v", p, docID, meta.HLVString())
	return BodyAndVersion{
		docMeta:    meta,
		body:       body,
		updatePeer: p.name,
	}
}

// DeleteDocument deletes a document on the peer. The test will fail if the document does not exist.
//
// The returned DocMetadata has VersionUnknown set, since the test server cannot read back the
// tombstone the delete leaves.
func (p *CouchbaseLitePeer) DeleteDocument(dsName sgbucket.DataStoreName, docID string) DocMetadata {
	p.TB().Helper()
	_, _, exists := p.getLatestDocVersion(p.TB(), dsName, docID)
	require.True(p.TB(), exists, "docID:%s not found on %s", docID, p)
	require.NoError(p.TB(), p.server.Client.UpdateDatabase(p.Context(), p.database, []cbltestclient.DatabaseUpdateItem{{
		Type:       cbltestclient.UpdateTypeDelete,
		Collection: cblCollectionName(dsName),
		DocumentID: docID,
	}}))
	base.InfofCtx(p.Context(), base.KeySGTest, "%s: Deleted document %s", p, docID)
	return DocMetadata{DocID: docID, VersionUnknown: true}
}

// WaitForDocVersion waits for a document to reach a specific version. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseLitePeer) WaitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, topology Topology) db.Body {
	p.TB().Helper()
	return p.waitFor(dsName, docID, expected, topology, func(c *assert.CollectT, actual DocMetadata, body db.Body) {
		data, _ := base.JSONMarshal(body)
		assertHLVEqual(c, dsName, docID, p.name, actual, data, expected, topology)
	})
}

// WaitForCV waits for a document to reach a specific CV. Returns the state of the document at that version. The test will fail if the document does not reach the expected version in 20s.
func (p *CouchbaseLitePeer) WaitForCV(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, topology Topology) db.Body {
	p.TB().Helper()
	return p.waitFor(dsName, docID, expected, topology, func(c *assert.CollectT, actual DocMetadata, body db.Body) {
		data, _ := base.JSONMarshal(body)
		assertCVEqual(c, dsName, docID, p.name, actual, data, expected, topology)
	})
}

// waitForVersionAndBody waits for a document to reach a version and a body together.  They have to be
// checked in the same read: while Couchbase Lite resolves a conflict it can briefly report the winning
// version alongside the losing body, so a body checked after the version can be the wrong one.  With
// cvOnly, only the current version is compared, as WaitForCV does.
func (p *CouchbaseLitePeer) waitForVersionAndBody(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, expectedBody []byte, cvOnly bool, topology Topology) {
	p.TB().Helper()
	p.waitFor(dsName, docID, expected, topology, func(c *assert.CollectT, actual DocMetadata, body db.Body) {
		data, _ := base.JSONMarshal(body)
		if cvOnly {
			assertCVEqual(c, dsName, docID, p.name, actual, data, expected, topology)
		} else {
			assertHLVEqual(c, dsName, docID, p.name, actual, data, expected, topology)
		}
		assert.JSONEq(c, string(expectedBody), string(data), "body of docID:%s on %s", docID, p)
	})
}

// waitFor polls a document until check passes.
func (p *CouchbaseLitePeer) waitFor(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, topology Topology, check func(*assert.CollectT, DocMetadata, db.Body)) db.Body {
	p.TB().Helper()
	var body db.Body
	require.EventuallyWithT(p.TB(), func(c *assert.CollectT) {
		var actual DocMetadata
		var exists bool
		body, actual, exists = p.getLatestDocVersion(c, dsName, docID)
		if !assert.True(c, exists, "Could not find docID:%+v on %s\nVersion %#v", docID, p, expected) {
			return
		}
		check(c, actual, body)
	}, totalWaitTime, pollInterval, "%s", p.server.LogTail())
	return body
}

// WaitForTombstoneVersion waits for a document to be deleted.  The version cannot be checked,
// since the test server cannot read back a tombstone.
func (p *CouchbaseLitePeer) WaitForTombstoneVersion(dsName sgbucket.DataStoreName, docID string, _ DocMetadata, topology Topology) {
	p.TB().Helper()
	require.EventuallyWithT(p.TB(), func(c *assert.CollectT) {
		_, _, exists := p.getLatestDocVersion(c, dsName, docID)
		assert.False(c, exists, "expected docID %s on peer %s to be deleted", docID, p)
	}, totalWaitTime, pollInterval, topology.GetDocState(p.TB(), dsName, docID))
}

// CreateReplication creates a replication instance.  Only Sync Gateway is supported as the passive peer.
func (p *CouchbaseLitePeer) CreateReplication(peer Peer, config PeerReplicationConfig) PeerReplication {
	sg, ok := peer.(*SyncGatewayPeer)
	require.True(p.TB(), ok, "unsupported peer type %T for a Couchbase Lite replication", peer)
	sg.createCouchbaseLiteUser()
	replication := &CouchbaseLiteReplication{
		activePeer:  p,
		passivePeer: sg,
		direction:   config.direction,
	}
	p.replications = append(p.replications, replication)
	return replication
}

// Close stops any replications this peer is running.  Its test server is stopped, and its data
// deleted, when the test that made the peer ends.
func (p *CouchbaseLitePeer) Close() {
	for _, replication := range p.replications {
		replication.Stop()
	}
}

// Type returns PeerTypeCouchbaseLite.
func (p *CouchbaseLitePeer) Type() PeerType {
	return PeerTypeCouchbaseLite
}

// IsSymmetricRedundant returns true if there is another peer set up that is identical to this one, and this peer doesn't need to participate in unique actions.
func (p *CouchbaseLitePeer) IsSymmetricRedundant() bool {
	return p.symmetricRedundant
}

// SourceID returns the source ID for the peer used in <val>@<sourceID>.  It is empty until the peer
// has written a document.
func (p *CouchbaseLitePeer) SourceID() string {
	return p.sourceID
}

// Context returns the context for the peer.
func (p *CouchbaseLitePeer) Context() context.Context {
	return base.TestCtx(p.TB())
}

// TB returns the testing.TB for the peer.
func (p *CouchbaseLitePeer) TB() testing.TB {
	return p.t.Load()
}

// UpdateTB updates the testing.TB for the peer.
func (p *CouchbaseLitePeer) UpdateTB(t *testing.T) {
	p.t.Store(t)
}

// GetBackingBucket returns the backing bucket for the peer. This is always nil.
func (p *CouchbaseLitePeer) GetBackingBucket() base.Bucket {
	return nil
}

func (p *CouchbaseLitePeer) ref(dsName sgbucket.DataStoreName, docID string) cbltestclient.DocumentRef {
	return cbltestclient.DocumentRef{Collection: cblCollectionName(dsName), ID: docID}
}

// CouchbaseLiteReplication is a one-way continuous replication between a CouchbaseLitePeer and a
// Sync Gateway.  Each Start creates a new replicator on the test server, which picks up from the
// checkpoint the previous one left.
type CouchbaseLiteReplication struct {
	activePeer   *CouchbaseLitePeer
	passivePeer  *SyncGatewayPeer
	direction    PeerReplicationDirection
	replicatorID string
}

// ActivePeer returns the peer sending documents
func (r *CouchbaseLiteReplication) ActivePeer() Peer {
	return r.activePeer
}

// PassivePeer returns the peer receiving documents
func (r *CouchbaseLiteReplication) PassivePeer() Peer {
	return r.passivePeer
}

// Start starts the replication
func (r *CouchbaseLiteReplication) Start() {
	p := r.activePeer
	p.TB().Helper()
	replicatorType := cbltestclient.ReplicatorTypePush
	if r.direction == PeerReplicationDirectionPull {
		replicatorType = cbltestclient.ReplicatorTypePull
	}
	base.InfofCtx(p.Context(), base.KeySGTest, "Starting CBL replication: %s", r)
	replicatorID, err := p.server.Client.StartReplicator(p.Context(), cbltestclient.ReplicatorConfig{
		Database:       p.database,
		Collections:    []cbltestclient.ReplicationCollection{{Names: []string{cblCollectionName(getSingleDsName())}}},
		Endpoint:       r.passivePeer.blipEndpoint(),
		ReplicatorType: replicatorType,
		Continuous:     true,
		Authenticator: &cbltestclient.Authenticator{
			Type:     cbltestclient.AuthenticatorTypeBasic,
			Username: cblUsername,
			Password: rest.RestTesterDefaultUserPassword,
		},
	}, false)
	require.NoError(p.TB(), err)
	r.replicatorID = replicatorID
}

// Stop halts the replication and waits for it to stop. The replication can be restarted after it is
// stopped.  Stopping a replication that is not running does nothing.
func (r *CouchbaseLiteReplication) Stop() {
	if r.replicatorID == "" {
		return
	}
	p := r.activePeer
	p.TB().Helper()
	base.InfofCtx(p.Context(), base.KeySGTest, "Stopping CBL replication: %s", r)
	require.NoError(p.TB(), p.server.Client.StopReplicator(p.Context(), r.replicatorID))
	require.EventuallyWithT(p.TB(), func(c *assert.CollectT) {
		status, err := p.server.Client.ReplicatorStatus(p.Context(), r.replicatorID)
		assert.NoError(c, err)
		assert.Equal(c, cbltestclient.ReplicatorActivityStopped, status.Activity)
	}, totalWaitTime, pollInterval, "replication %s did not stop; %s", r, p.server.LogTail())
	r.replicatorID = ""
}

func (r *CouchbaseLiteReplication) String() string {
	directionArrow := "->"
	if r.direction == PeerReplicationDirectionPull {
		directionArrow = "<-"
	}
	return fmt.Sprintf("%s%s%s", r.activePeer, directionArrow, r.passivePeer)
}

// Stats returns the replicator's activity, and its error if it has one.
func (r *CouchbaseLiteReplication) Stats() string {
	if r.replicatorID == "" {
		return "not running"
	}
	p := r.activePeer
	status, err := p.server.Client.ReplicatorStatus(p.Context(), r.replicatorID)
	if err != nil {
		return fmt.Sprintf("could not get replicator status: %v", err)
	}
	if status.Error != nil {
		return fmt.Sprintf("%s, error: %v", status.Activity, status.Error)
	}
	return string(status.Activity)
}

// maxOutdatedReadAttempts is how many times a read that raced a replicator's save is tried.
const maxOutdatedReadAttempts = 10

// cblErrorConflict is the Couchbase Lite error code for a document that changed while it was being read.
const cblErrorConflict = 8

// isDocumentOutdated reports whether err means the document changed while the test server read it.
func isDocumentOutdated(err error) bool {
	var apiErr *cbltestclient.APIError
	return errors.As(err, &apiErr) && apiErr.Domain == "CBL" && apiErr.Code == cblErrorConflict
}

// cblCollectionName returns the "<scope>.<collection>" name the test server uses for a collection.
func cblCollectionName(dsName sgbucket.DataStoreName) string {
	return dsName.ScopeName() + "." + dsName.CollectionName()
}

// cblDatabaseName turns a test and peer name into a Couchbase Lite database name, which the test
// server uses as a directory name.
func cblDatabaseName(name string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, name)
}
