// Copyright 2024-Present Couchbase, Inc.
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
	"net/http"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type SyncGatewayPeer struct {
	rt                 *rest.RestTester
	name               string
	symmetricRedundant bool
}

func newSyncGatewayPeer(t *testing.T, name string, bucket *base.TestBucket, symmetricRedundant bool) Peer {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		CustomTestBucket: bucket.NoCloseClone(),
	})
	config := rt.NewDbConfig()
	config.AutoImport = base.Ptr(true)
	rest.RequireStatus(t, rt.CreateDatabase(rest.SafeDatabaseName(t, name), config), http.StatusCreated)
	return &SyncGatewayPeer{
		name:               name,
		rt:                 rt,
		symmetricRedundant: symmetricRedundant,
	}
}

func (p *SyncGatewayPeer) String() string {
	return fmt.Sprintf("%s (bucket:%s,sourceid:%s)", p.name, p.rt.Bucket().GetName(), p.SourceID())
}

// getCollection returns the collection for the given data store name and a related context. The special context is needed to add fields for audit logging, required by build tag cb_sg_devmode.
func (p *SyncGatewayPeer) getCollection(dsName sgbucket.DataStoreName) (*db.DatabaseCollectionWithUser, context.Context) {
	dbCtx, err := db.GetDatabase(p.rt.GetDatabase(), nil)
	require.NoError(p.TB(), err)
	collection, err := dbCtx.GetDatabaseCollectionWithUser(dsName.ScopeName(), dsName.CollectionName())
	require.NoError(p.TB(), err)
	ctx := base.UserLogCtx(collection.AddCollectionContext(p.Context()), "gotest", base.UserDomainBuiltin, nil)
	return collection, ctx
}

// GetDocument returns the latest version of a document. The test will fail the document does not exist.
func (p *SyncGatewayPeer) GetDocument(dsName sgbucket.DataStoreName, docID string) (DocMetadata, db.Body) {
	collection, ctx := p.getCollection(dsName)
	doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
	require.NoError(p.TB(), err)
	return DocMetadataFromDocument(doc), doc.Body(ctx)
}

// GetDocumentIfExists returns the latest version of a document if it exists.
func (p *SyncGatewayPeer) GetDocumentIfExists(dsName sgbucket.DataStoreName, docID string) (meta DocMetadata, body *db.Body, exists bool) {
	collection, ctx := p.getCollection(dsName)
	doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
	if base.IsDocNotFoundError(err) {
		return DocMetadata{}, nil, false
	}
	require.NoError(p.TB(), err)
	meta = DocMetadataFromDocument(doc)
	if doc.IsDeleted() {
		return meta, nil, true
	}
	return meta, base.Ptr(doc.Body(ctx)), true
}

// CreateDocument creates a document on the peer. The test will fail if the document already exists.
func (p *SyncGatewayPeer) CreateDocument(dsName sgbucket.DataStoreName, docID string, body []byte) BodyAndVersion {
	docMetadata := p.writeDocument(dsName, docID, body)
	p.TB().Logf("%s: Created document %s with %#+v", p, docID, docMetadata)
	return BodyAndVersion{
		docMeta:    docMetadata,
		body:       body,
		updatePeer: p.name,
	}
}

// writeDocument writes a document to the peer. The test will fail if the write does not succeed.
func (p *SyncGatewayPeer) writeDocument(dsName sgbucket.DataStoreName, docID string, body []byte) DocMetadata {
	collection, ctx := p.getCollection(dsName)

	var doc *db.Document
	// loop to write document in the case that there is a conflict while writing the document
	err, _ := base.RetryLoop(ctx, "write document", func() (shouldRetry bool, err error, value any) {
		var bodyMap db.Body
		err = base.JSONUnmarshal(body, &bodyMap)
		require.NoError(p.TB(), err)

		// allow upsert rev
		existingDoc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
		if err == nil {
			bodyMap[db.BodyRev] = existingDoc.CurrentRev
		}
		_, doc, err = collection.Put(ctx, docID, bodyMap)
		if err != nil {
			var httpError *base.HTTPError
			if errors.As(err, &httpError) && httpError.Status == http.StatusConflict {
				return true, err, nil
			}
			require.NoError(p.TB(), err)
		}
		return false, nil, nil
	}, base.CreateSleeperFunc(5, 100))
	require.NoError(p.TB(), err)
	return DocMetadataFromDocument(doc)
}

// WriteDocument writes a document to the peer. The test will fail if the write does not succeed.
func (p *SyncGatewayPeer) WriteDocument(dsName sgbucket.DataStoreName, docID string, body []byte) BodyAndVersion {
	docMetadata := p.writeDocument(dsName, docID, body)
	p.TB().Logf("%s: Wrote document %s with %#+v", p, docID, docMetadata)
	return BodyAndVersion{
		docMeta:    docMetadata,
		body:       body,
		updatePeer: p.name,
	}
}

// DeleteDocument deletes a document on the peer. The test will fail if the document does not exist.
func (p *SyncGatewayPeer) DeleteDocument(dsName sgbucket.DataStoreName, docID string) DocMetadata {
	collection, ctx := p.getCollection(dsName)
	doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
	var revID string
	if err == nil {
		revID = doc.CurrentRev
	}
	_, doc, err = collection.DeleteDoc(ctx, docID, revID)
	require.NoError(p.TB(), err)
	docMeta := DocMetadataFromDocument(doc)
	p.TB().Logf("%s: Deleted document %s with %#+v", p, docID, docMeta)
	return docMeta
}

// WaitForDocVersion waits for a document to reach a specific version. The test will fail if the document does not reach the expected version in 20s.
func (p *SyncGatewayPeer) WaitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, topology Topology) db.Body {
	collection, ctx := p.getCollection(dsName)
	var doc *db.Document
	require.EventuallyWithT(p.TB(), func(c *assert.CollectT) {
		var err error
		doc, err = collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
		assert.NoError(c, err)
		if doc == nil {
			return
		}
		version := DocMetadataFromDocument(doc)
		// Only assert on CV since RevTreeID might not be present if this was a Couchbase Server write
		bodyBytes, err := doc.BodyBytes(ctx)
		assert.NoError(c, err)
		assertHLVEqual(c, dsName, docID, p.name, version, bodyBytes, expected, topology)
	}, totalWaitTime, pollInterval)
	return doc.Body(ctx)
}

// WaitForCV waits for a document to reach a specific CV. The test will fail if the document does not reach the expected version in 20s.
func (p *SyncGatewayPeer) WaitForCV(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, topology Topology) db.Body {
	collection, ctx := p.getCollection(dsName)
	var doc *db.Document
	require.EventuallyWithT(p.TB(), func(c *assert.CollectT) {
		var err error
		doc, err = collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
		assert.NoError(c, err)
		if doc == nil {
			return
		}
		version := DocMetadataFromDocument(doc)
		bodyBytes, err := doc.BodyBytes(ctx)
		assert.NoError(c, err)
		assertCVEqual(c, dsName, docID, p.name, version, bodyBytes, expected, topology)
	}, totalWaitTime, pollInterval)
	return doc.Body(ctx)
}

// WaitForTombstoneVersion waits for a document to reach a specific version, this must be a tombstone. The test will fail if the document does not reach the expected version in 20s.
func (p *SyncGatewayPeer) WaitForTombstoneVersion(dsName sgbucket.DataStoreName, docID string, expected DocMetadata, topology Topology) {
	docBytes := p.WaitForDocVersion(dsName, docID, expected, topology)
	require.Empty(p.TB(), docBytes, "expected tombstone for docID %s, got %s. Replications:\n%s", docID, docBytes, topology.GetDocState(p.TB(), dsName, docID))
}

// Close will shut down the peer and close any active replications on the peer.
func (p *SyncGatewayPeer) Close() {
	p.rt.Close()
}

// Type returns PeerTypeSyncGateway.
func (p *SyncGatewayPeer) Type() PeerType {
	return PeerTypeSyncGateway
}

// IsSymmetricRedundant returns true if there is another peer set up that is identical to this one, and this peer doesn't need to participate in unique actions.
func (p *SyncGatewayPeer) IsSymmetricRedundant() bool {
	return p.symmetricRedundant
}

// CreateReplication creates a replication instance. This is currently not supported for Sync Gateway peers. A future ISGR implementation will support this.
func (p *SyncGatewayPeer) CreateReplication(_ Peer, _ PeerReplicationConfig) PeerReplication {
	require.Fail(p.rt.TB(), "can not create a replication with Sync Gateway as an active peer")
	return nil
}

// SourceID returns the source ID for the peer used in <val>@<sourceID>.
func (p *SyncGatewayPeer) SourceID() string {
	return p.rt.GetDatabase().EncodedSourceID
}

// Context returns the context for the peer.
func (p *SyncGatewayPeer) Context() context.Context {
	return p.rt.Context()
}

// TB returns the testing.TB for the peer.
func (p *SyncGatewayPeer) TB() testing.TB {
	return p.rt.TB()
}

// UpdateTB updates the testing.TB for the peer.
func (p *SyncGatewayPeer) UpdateTB(t *testing.T) {
	p.rt.UpdateTB(t)
}

// GetBackingBucket returns the backing bucket for the peer.
func (p *SyncGatewayPeer) GetBackingBucket() base.Bucket {
	return p.rt.Bucket()
}
