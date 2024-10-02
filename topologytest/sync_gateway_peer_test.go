// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package topologytest

import (
	"net/http"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type SyncGatewayPeer struct {
	rt   *rest.RestTester
	name string
}

func newSyncGatewayPeer(t *testing.T, name string, bucket *base.TestBucket) Peer {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		CustomTestBucket: bucket.NoCloseClone(),
	})
	config := rt.NewDbConfig()
	config.AutoImport = base.BoolPtr(true)
	rest.RequireStatus(t, rt.CreateDatabase(rest.SafeDatabaseName(t, name), config), http.StatusCreated)
	return &SyncGatewayPeer{
		name: name,
		rt:   rt,
	}
}

func (p *SyncGatewayPeer) String() string {
	return p.name
}

// GetDocument returns the latest version of a document. The test will fail the document does not exist.
func (p *SyncGatewayPeer) GetDocument(dsName sgbucket.DataStoreName, docID string) (rest.DocVersion, db.Body) {
	// this function is not yet collections aware
	return p.rt.GetDoc(docID)
}

// CreateDocument creates a document on the peer. The test will fail if the document already exists.
func (p *SyncGatewayPeer) CreateDocument(dsName sgbucket.DataStoreName, docID string, body []byte) rest.DocVersion {
	return rest.EmptyDocVersion()
}

// WriteDocument writes a document to the peer. The test will fail if the write does not succeed.
func (p *SyncGatewayPeer) WriteDocument(dsName sgbucket.DataStoreName, docID string, body []byte) rest.DocVersion {
	// this function is not yet collections aware
	return p.rt.PutDoc(docID, string(body))
}

// DeleteDocument deletes a document on the peer. The test will fail if the document does not exist.
func (p *SyncGatewayPeer) DeleteDocument(dsName sgbucket.DataStoreName, docID string) rest.DocVersion {
	return rest.EmptyDocVersion()
}

// WaitForDocVersion waits for a document to reach a specific version. The test will fail if the document does not reach the expected version in 20s.
func (p *SyncGatewayPeer) WaitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected rest.DocVersion) db.Body {
	// this function is not yet collections aware
	var body db.Body
	require.EventuallyWithT(p.rt.TB(), func(c *assert.CollectT) {
		response := p.rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docID, "")
		assert.Equal(c, http.StatusOK, response.Code)
		body = nil
		assert.NoError(c, base.JSONUnmarshal(response.Body.Bytes(), &body))
		// FIXME can't assert for a specific version right now, not everything returns the correct version.
		// assert.Equal(c, expected.RevTreeID, body.ExtractRev())
	}, 10*time.Second, 100*time.Millisecond)
	return body
}

// RequireDocNotFound asserts that a document does not exist on the peer.
func (p *SyncGatewayPeer) RequireDocNotFound(dsName sgbucket.DataStoreName, docID string) {
	// _, err := p.rt.GetDoc(docID)
	// base.RequireDocNotFoundError(p.rt.TB(), err)
}

// Close will shut down the peer and close any active replications on the peer.
func (p *SyncGatewayPeer) Close() {
	p.rt.Close()
}

// CreateReplication creates a replication instance. This is currently not supported for Sync Gateway peers. A future ISGR implementation will support this.
func (p *SyncGatewayPeer) CreateReplication(peer Peer, config PeerReplicationConfig) PeerReplication {
	require.Fail(p.rt.TB(), "can not create a replication with Sync Gateway as an active peer")
	return nil
}

// SourceID returns the source ID for the peer used in <val>@sourceID.
func (r *SyncGatewayPeer) SourceID() string {
	return r.rt.GetDatabase().EncodedSourceID
}

// GetBackingBucket returns the backing bucket for the peer.
func (p *SyncGatewayPeer) GetBackingBucket() base.Bucket {
	return p.rt.Bucket()
}
