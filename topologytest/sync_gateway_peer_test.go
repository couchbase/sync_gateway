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
func (p *SyncGatewayPeer) GetDocument(dsName sgbucket.DataStoreName, docID string) (rest.DocVersion, db.Body) {
	collection, ctx := p.getCollection(dsName)
	doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
	require.NoError(p.TB(), err)
	return docVersionFromDocument(doc), doc.Body(ctx)
}

// CreateDocument creates a document on the peer. The test will fail if the document already exists.
func (p *SyncGatewayPeer) CreateDocument(dsName sgbucket.DataStoreName, docID string, body []byte) rest.DocVersion {
	return p.WriteDocument(dsName, docID, body)
}

// WriteDocument writes a document to the peer. The test will fail if the write does not succeed.
func (p *SyncGatewayPeer) WriteDocument(dsName sgbucket.DataStoreName, docID string, body []byte) rest.DocVersion {
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
	return docVersionFromDocument(doc)
}

// DeleteDocument deletes a document on the peer. The test will fail if the document does not exist.
func (p *SyncGatewayPeer) DeleteDocument(dsName sgbucket.DataStoreName, docID string) rest.DocVersion {
	collection, ctx := p.getCollection(dsName)
	doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
	var revID string
	if err == nil {
		revID = doc.CurrentRev
	}
	_, doc, err = collection.DeleteDoc(ctx, docID, revID)
	require.NoError(p.TB(), err)
	return docVersionFromDocument(doc)
}

// WaitForDocVersion waits for a document to reach a specific version. The test will fail if the document does not reach the expected version in 20s.
func (p *SyncGatewayPeer) WaitForDocVersion(dsName sgbucket.DataStoreName, docID string, expected rest.DocVersion) db.Body {
	collection, ctx := p.getCollection(dsName)
	var doc *db.Document
	require.EventuallyWithT(p.TB(), func(c *assert.CollectT) {
		var err error
		doc, err = collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
		assert.NoError(c, err)
		docVersion := docVersionFromDocument(doc)
		// Only assert on CV since RevTreeID might not be present if this was a Couchbase Server write
		assert.Equal(c, expected.CV, docVersion.CV)
	}, 5*time.Second, 100*time.Millisecond)
	return doc.Body(ctx)
}

// RequireDocNotFound asserts that a document does not exist on the peer.
func (p *SyncGatewayPeer) RequireDocNotFound(dsName sgbucket.DataStoreName, docID string) {
	collection, ctx := p.getCollection(dsName)
	doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
	if err == nil {
		require.True(p.TB(), doc.IsDeleted(), "expected %s to be deleted", doc)
		return
	}
	base.RequireDocNotFoundError(p.TB(), err)
}

// Close will shut down the peer and close any active replications on the peer.
func (p *SyncGatewayPeer) Close() {
	p.rt.Close()
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

// GetBackingBucket returns the backing bucket for the peer.
func (p *SyncGatewayPeer) GetBackingBucket() base.Bucket {
	return p.rt.Bucket()
}

// docVersionFromDocument sets the DocVersion from the current revision of the document.
func docVersionFromDocument(doc *db.Document) rest.DocVersion {
	sourceID, value := doc.HLV.GetCurrentVersion()
	return rest.DocVersion{
		RevTreeID: doc.CurrentRev,
		CV: db.Version{
			SourceID: sourceID,
			Value:    value,
		},
	}
}
