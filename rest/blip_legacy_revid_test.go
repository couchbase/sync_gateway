/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLegacyProposeChanges:
//   - Build propose changes request of docs that are all new to SGW in legacy format
//   - Assert that the response is as expected (empty response)
func TestLegacyProposeChanges(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()

	proposeChangesRequest := bt.newRequest()
	proposeChangesRequest.SetProfile("proposeChanges")
	proposeChangesRequest.SetCompressed(true)

	changesBody := `
[["foo", "1-abc"],
["foo2", "1-abc"]]
`
	proposeChangesRequest.SetBody([]byte(changesBody))
	sent := bt.sender.Send(proposeChangesRequest)
	assert.True(t, sent)
	proposeChangesResponse := proposeChangesRequest.Response()
	body, err := proposeChangesResponse.Body()
	require.NoError(t, err)

	var changeList [][]interface{}
	err = base.JSONUnmarshal(body, &changeList)
	require.NoError(t, err)

	assert.Len(t, changeList, 0)
}

// TestProposeChangesHandlingWithExistingRevs:
//   - Build up propose changes request for conflicting and non conflicting docs with legacy revs
//   - Assert that the response sent from SGW is as expected
func TestProposeChangesHandlingWithExistingRevs(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	collection, _ := rt.GetSingleTestDatabaseCollection()

	resp := rt.PutDoc("conflictingInsert", `{"version":1}`)
	conflictingInsertRev := resp.RevTreeID

	resp = rt.PutDoc("conflictingUpdate", `{"version":1}`)
	conflictingUpdateRev1 := resp.RevTreeID
	conflictingUpdateRev2 := rt.UpdateDocRev("conflictingUpdate", resp.RevTreeID, `{"version":2}`)
	source, value := collection.GetDocumentCurrentVersion(t, "conflictingUpdate")
	conflictingUpdateVersion2 := db.Version{SourceID: source, Value: value}

	resp = rt.PutDoc("newUpdate", `{"version":1}`)
	newUpdateRev1 := resp.RevTreeID

	resp = rt.PutDoc("existingDoc", `{"version":1}`)
	existingDocRev := resp.RevTreeID
	source, value = collection.GetDocumentCurrentVersion(t, "existingDoc")
	existingVersion := db.Version{SourceID: source, Value: value}
	existingVersionString := existingVersion.String()

	type proposeChangesCase struct {
		description   string
		key           string
		revID         string
		parentRevID   string
		expectedValue interface{}
	}

	proposeChangesCases := []proposeChangesCase{
		{
			description:   "conflicting insert, legacy rev",
			key:           "conflictingInsert",
			revID:         "1-abc",
			parentRevID:   "",
			expectedValue: map[string]interface{}{"status": float64(db.ProposedRev_Conflict), "rev": conflictingInsertRev},
		},
		{
			description:   "successful insert, legacy rev",
			key:           "newInsert",
			revID:         "1-abc",
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_OK),
		},
		{
			description:   "conflicting update, legacy rev",
			key:           "conflictingUpdate",
			revID:         "2-abc",
			parentRevID:   conflictingUpdateRev1,
			expectedValue: map[string]interface{}{"status": float64(db.ProposedRev_Conflict), "rev": conflictingUpdateRev2},
		},
		{
			description:   "successful update, legacy rev",
			key:           "newUpdate",
			revID:         "2-abc",
			parentRevID:   newUpdateRev1,
			expectedValue: float64(db.ProposedRev_OK),
		},
		{
			description:   "insert, existing doc, legacy rev",
			key:           "existingDoc",
			revID:         existingDocRev,
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_Exists),
		},
		{
			description:   "successful update, new version, legacy parent",
			key:           "newUpdate",
			revID:         "1000@CBL1",
			parentRevID:   newUpdateRev1,
			expectedValue: float64(db.ProposedRev_OK),
		},
		{
			description:   "conflicting update, new version, legacy parent",
			key:           "conflictingUpdate",
			revID:         "1000@CBL1",
			parentRevID:   conflictingUpdateRev1,
			expectedValue: map[string]interface{}{"status": float64(db.ProposedRev_Conflict), "rev": conflictingUpdateVersion2.String()},
		},
		{
			description:   "already known, existing version, legacy parent is ancestor",
			key:           "conflictingUpdate",
			revID:         conflictingUpdateVersion2.String(),
			parentRevID:   conflictingUpdateRev1,
			expectedValue: float64(db.ProposedRev_Exists),
		},
		{
			description:   "full HLV in new rev, CBG-4460",
			key:           "fullHLVinRev",
			revID:         "1000@CBL1;900@CBL2",
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_OK),
		},
		{
			description:   "full HLV in new rev with mv only, CBG-4460",
			key:           "fullHLVinRevWithMV",
			revID:         "1000@CBL1,900@CBL1,900@CBL2",
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_OK),
		},
		{
			description:   "full HLV in new rev with mv and pv, CBG-4460",
			key:           "fullHLVinRevWithMVandPV",
			revID:         "1000@CBL1,900@CBL1,900@CBL2;900@CBL2",
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_OK),
		},
		{
			description:   "full HLV in existing rev, CBG-4460",
			key:           "existingDoc",
			revID:         existingVersionString + ";900@CBL2",
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_Exists),
		},
		{
			description:   "full HLV in existing rev with mv only, CBG-4460",
			key:           "existingDoc",
			revID:         existingVersionString + ",900@CBL1,900@CBL2",
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_Exists),
		},
		{
			description:   "full HLV in existing rev with mv and pv, CBG-4460",
			key:           "existingDoc",
			revID:         existingVersionString + ",900@CBL1,900@CBL2;900@CBL2",
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_Exists),
		},
	}

	proposeChangesRequest := bt.newRequest()
	proposeChangesRequest.SetProfile("proposeChanges")
	proposeChangesRequest.SetCompressed(true)
	proposeChangesRequest.Properties[db.ProposeChangesConflictsIncludeRev] = "true"

	proposedChanges := make([][]interface{}, 0)
	for _, c := range proposeChangesCases {
		changeEntry := []interface{}{
			c.key,
			c.revID,
		}
		if c.parentRevID != "" {
			changeEntry = append(changeEntry, c.parentRevID)
		}
		proposedChanges = append(proposedChanges, changeEntry)
	}
	proposeChangesBody, marshalErr := json.Marshal(proposedChanges)
	require.NoError(t, marshalErr)

	proposeChangesRequest.SetBody(proposeChangesBody)
	sent := bt.sender.Send(proposeChangesRequest)
	assert.True(t, sent)
	proposeChangesResponse := proposeChangesRequest.Response()
	bodyReader, err := proposeChangesResponse.BodyReader()
	require.NoError(t, err)

	var changeList []interface{}
	decoder := base.JSONDecoder(bodyReader)
	decodeErr := decoder.Decode(&changeList)
	require.NoError(t, decodeErr)

	for i, entry := range changeList {
		assert.Equal(t, proposeChangesCases[i].expectedValue, entry, "mismatch in expected value for case %q", proposeChangesCases[i].description)
	}
}

// TestProcessLegacyRev:
//   - Create doc on SGW
//   - Push new revision of this doc form client in legacy rev mode
//   - Assert that the new doc is created and given a new source version pair
//   - Send a new rev that SGW hasn;t yet seen unsolicited and assert that the doc is added correctly and given a source version pair
func TestProcessLegacyRev(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	collection, _ := rt.GetSingleTestDatabaseCollection()

	// add doc to SGW
	docVersion := rt.PutDocDirectly("doc1", db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	// Send another rev of same doc
	history := []string{rev1ID}
	sent, _, _, err := bt.SendRevWithHistory("doc1", "2-bcd", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	assert.NoError(t, err)
	rt.WaitForVersion("doc1", DocVersion{RevTreeID: "2-bcd"})

	// assert we can fetch this doc rev
	resp := rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1?rev=2-bcd", "")
	RequireStatus(t, resp, 200)

	// assert this legacy doc has been given source version pair
	docSource, docVrs := collection.GetDocumentCurrentVersion(t, "doc1")
	assert.Equal(t, docVersion.CV.SourceID, docSource)
	assert.NotEqual(t, docVersion.CV.Value, docVrs)

	// try new rev to process
	_, _, _, err = bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)
	assert.NoError(t, err)

	rt.WaitForVersion("foo", DocVersion{RevTreeID: "1-abc"})
	// assert we can fetch this doc rev
	resp = rt.SendAdminRequest("GET", "/{{.keyspace}}/foo?rev=1-abc", "")
	RequireStatus(t, resp, 200)

	// assert this legacy doc has been given source version pair
	docSource, docVrs = collection.GetDocumentCurrentVersion(t, "doc1")
	assert.NotEqual(t, "", docSource)
	assert.NotEqual(t, uint64(0), docVrs)
}

// TestProcessRevWithLegacyHistory:
//   - 1. CBL sends rev=1010@CBL1, history=1-abc when SGW has current rev 1-abc (document underwent an update before being pushed to SGW)
//   - 2. CBL sends rev=1010@CBL1, history=1000@CBL2,1-abc when SGW has current rev 1-abc (document underwent multiple p2p updates before being pushed to SGW)
//   - 3. CBL sends rev=1010@CBL1, history=1000@CBL2,2-abc,1-abc when SGW has current rev 1-abc (document underwent multiple legacy and p2p updates before being pushed to SGW)
//   - 4. CBL sends rev=1010@CBL1, history=1-abc when SGW does not have the doc (document underwent multiple legacy and p2p updates before being pushed to SGW)
//   - 5. CBL sends rev=1010@CBL1, history=2-abc and SGW has 1000@CBL2, 2-abc
//   - 6. CBL sends rev=1010@CBL1, history=3-abc,2-abc,1-abc and SGW has 1000@SGW, 1-abc
//   - Assert that the bucket doc resulting on each operation is as expected
func TestProcessRevWithLegacyHistory(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	ds := rt.GetSingleDataStore()
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	const (
		docID  = "doc1"
		docID2 = "doc2"
		docID3 = "doc3"
		docID4 = "doc4"
		docID5 = "doc5"
		docID6 = "doc6"
	)

	// 1. CBL sends rev=1010@CBL1, history=1-abc when SGW has current rev 1-abc (document underwent an update before being pushed to SGW)
	docVersion := rt.PutDocDirectly(docID, db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	require.NoError(t, ds.RemoveXattrs(ctx, docID, []string{base.VvXattrName}, docVersion.CV.Value))
	rt.GetDatabase().FlushRevisionCacheForTest()

	// Have CBL send an update to that doc, with history in revTreeID format
	history := []string{rev1ID}
	sent, _, _, err := bt.SendRevWithHistory(docID, "1000@CBL1", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "1000@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.NotNil(t, bucketDoc.History[rev1ID])

	// 2. CBL sends rev=1010@CBL1, history=1000@CBL2,1-abc when SGW has current rev 1-abc (document underwent multiple p2p updates before being pushed to SGW)
	docVersion = rt.PutDocDirectly(docID2, db.Body{"test": "doc"})
	rev1ID = docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	require.NoError(t, ds.RemoveXattrs(ctx, docID2, []string{base.VvXattrName}, docVersion.CV.Value))
	rt.GetDatabase().FlushRevisionCacheForTest()

	// Have CBL send an update to that doc, with history in HLV + revTreeID format
	history = []string{"1000@CBL2", rev1ID}
	sent, _, _, err = bt.SendRevWithHistory(docID2, "1001@CBL1", history, []byte(`{"some": "update"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)

	// assert that the bucket doc is as expected
	bucketDoc, _, err = collection.GetDocWithXattrs(ctx, docID2, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "1001@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.Equal(t, uint64(4096), bucketDoc.HLV.PreviousVersions["CBL2"])
	assert.NotNil(t, bucketDoc.History[rev1ID])

	// 3. CBL sends rev=1010@CBL1, history=1000@CBL2,2-abc,1-abc when SGW has current rev 1-abc (document underwent multiple legacy and p2p updates before being pushed to SGW)
	docVersion = rt.PutDocDirectly(docID3, db.Body{"test": "doc"})
	rev1ID = docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	require.NoError(t, ds.RemoveXattrs(ctx, docID3, []string{base.VvXattrName}, docVersion.CV.Value))
	rt.GetDatabase().FlushRevisionCacheForTest()

	history = []string{"1000@CBL2", "2-abc", rev1ID}
	sent, _, _, err = bt.SendRevWithHistory(docID3, "1010@CBL1", history, []byte(`{"some": "update"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)

	// assert that the bucket doc is as expected
	bucketDoc, _, err = collection.GetDocWithXattrs(ctx, docID3, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "1010@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.Equal(t, uint64(4096), bucketDoc.HLV.PreviousVersions["CBL2"])
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.NotNil(t, bucketDoc.History["2-abc"])

	// 4. CBL sends rev=1010@CBL1, history=1-abc when SGW does not have the doc (document underwent multiple legacy and p2p updates before being pushed to SGW)
	history = []string{"1000@CBL2", "1-abc"}
	sent, _, _, err = bt.SendRevWithHistory(docID4, "1010@CBL1", history, []byte(`{"some": "update"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)

	// assert that the bucket doc is as expected
	bucketDoc, _, err = collection.GetDocWithXattrs(ctx, docID4, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "1010@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.Equal(t, uint64(4096), bucketDoc.HLV.PreviousVersions["CBL2"])
	assert.NotNil(t, bucketDoc.History["1-abc"])

	// 5. CBL sends rev=1010@CBL1, history=2-abc and SGW has 1000@CBL2, 2-abc
	// although HLV's are in conflict, this should pass conflict check as local current rev is parent of incoming rev
	docVersion = rt.PutDocDirectly(docID5, db.Body{"test": "doc"})

	docVersion = rt.UpdateDocDirectly(docID5, docVersion, db.Body{"some": "update"})
	version := docVersion.CV.Value
	rev2ID := docVersion.RevTreeID
	pushedRev := db.Version{
		Value:    version + 1000,
		SourceID: "CBL1",
	}

	history = []string{rev2ID}
	sent, _, _, err = bt.SendRevWithHistory(docID5, pushedRev.String(), history, []byte(`{"some": "update"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)

	// assert that the bucket doc is as expected
	bucketDoc, _, err = collection.GetDocWithXattrs(ctx, docID5, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, pushedRev.String(), bucketDoc.HLV.GetCurrentVersionString())
	assert.Equal(t, docVersion.CV.Value, bucketDoc.HLV.PreviousVersions[docVersion.CV.SourceID])
	assert.NotNil(t, bucketDoc.History[rev2ID])

	// 6. CBL sends rev=1010@CBL1, history=3-abc,2-abc,1-abc and SGW has 1000@SGW, 1-abc
	// replicates the following:
	// - a new doc being created on SGW 4.0,
	// - a pre 4.0 client pulling this doc on one shot replication
	// - then this doc being updated a couple of times on client before client gets upgraded to 4.0
	// - after the upgrade client updates it again and pushes to SGW
	docVersion = rt.PutDocDirectly(docID6, db.Body{"test": "doc"})
	rev1ID = docVersion.RevTreeID

	pushedRev = db.Version{
		Value:    version + 1000,
		SourceID: "CBL1",
	}
	history = []string{"3-abc", "2-abc", rev1ID}
	sent, _, _, err = bt.SendRevWithHistory(docID6, pushedRev.String(), history, []byte(`{"some": "update"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)

	// assert that the bucket doc is as expected
	bucketDoc, _, err = collection.GetDocWithXattrs(ctx, docID6, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, pushedRev.String(), bucketDoc.HLV.GetCurrentVersionString())
	assert.Equal(t, docVersion.CV.Value, bucketDoc.HLV.PreviousVersions[docVersion.CV.SourceID])
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.NotNil(t, bucketDoc.History["2-abc"])
	assert.NotNil(t, bucketDoc.History["3-abc"])
}

// TestProcessRevWithLegacyHistoryConflict:
//   - 1. conflicting changes with legacy rev on both sides of communication (no upgrade of doc at all)
//   - 2. conflicting changes with legacy rev on client side and HLV on SGW side
//   - 3. CBL sends rev=1010@CBL1, history=1000@CBL2,1-abc when SGW has current rev 2-abc (document underwent multiple p2p updates before being pushed to SGW)
//   - 4. CBL sends rev=1010@CBL1, history=2-abc and SGW has 1000@CBL2, 2-abc
func TestProcessRevWithLegacyHistoryConflict(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelTrace, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyCRUD, base.KeyChanges, base.KeyImport)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	ds := rt.GetSingleDataStore()
	const (
		docID  = "doc1"
		docID2 = "doc2"
		docID3 = "doc3"
		docID4 = "doc4"
	)

	// 1. conflicting changes with legacy rev on both sides of communication (no upgrade of doc at all)
	docVersion := rt.PutDocDirectly(docID, db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	docVersion = rt.UpdateDocDirectly(docID, docVersion, db.Body{"some": "update"})
	rev2ID := docVersion.RevTreeID

	docVersion = rt.UpdateDocDirectly(docID, docVersion, db.Body{"some": "update2"})

	// remove hlv here to simulate a legacy rev
	require.NoError(t, ds.RemoveXattrs(base.TestCtx(t), docID, []string{base.VvXattrName}, docVersion.CV.Value))
	rt.GetDatabase().FlushRevisionCacheForTest()

	history := []string{rev2ID, rev1ID}
	sent, _, _, err := bt.SendRevWithHistory(docID, "3-abc", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.ErrorContains(t, err, "Document revision conflict")

	// 2. same as above but not having the rev be legacy on SGW side (don't remove the hlv)
	docVersion = rt.PutDocDirectly(docID2, db.Body{"test": "doc"})
	rev1ID = docVersion.RevTreeID

	docVersion = rt.UpdateDocDirectly(docID2, docVersion, db.Body{"some": "update"})
	rev2ID = docVersion.RevTreeID

	docVersion = rt.UpdateDocDirectly(docID2, docVersion, db.Body{"some": "update2"})

	history = []string{rev2ID, rev1ID}
	sent, _, _, err = bt.SendRevWithHistory(docID2, "3-abc", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.ErrorContains(t, err, "Document revision conflict")

	// 3. CBL sends rev=1010@CBL1, history=1000@CBL2,1-abc when SGW has current rev 2-abc (document underwent multiple p2p updates before being pushed to SGW)
	docVersion = rt.PutDocDirectly(docID3, db.Body{"test": "doc"})
	rev1ID = docVersion.RevTreeID

	docVersion = rt.UpdateDocDirectly(docID3, docVersion, db.Body{"some": "update"})

	// remove hlv here to simulate a legacy rev
	require.NoError(t, ds.RemoveXattrs(base.TestCtx(t), docID3, []string{base.VvXattrName}, docVersion.CV.Value))
	rt.GetDatabase().FlushRevisionCacheForTest()

	history = []string{"1000@CBL2", rev1ID}
	sent, _, _, err = bt.SendRevWithHistory(docID3, "1010@CBL1", history, []byte(`{"some": "update"}`), blip.Properties{})
	assert.True(t, sent)
	require.ErrorContains(t, err, "Document revision conflict")
}

// TestChangesResponseLegacyRev:
//   - Create doc
//   - Update doc through SGW, creating a new revision
//   - Send subChanges request and have custom changes handler to force a revID change being constructed
//   - Have custom rev handler to assert the subsequent rev message is as expected with cv as rev + full rev
//     tree in history. No hlv in history is expected here.
func TestChangesResponseLegacyRev(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester

	docVersion := rt.PutDocDirectly("doc1", db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	docVersion2 := rt.UpdateDocDirectly("doc1", docVersion, db.Body{"test": "update"})
	// wait for pending change to avoid flakes where changes feed didn't pick up this change
	rt.WaitForPendingChanges()
	receivedChangesRequestWg := sync.WaitGroup{}
	revsFinishedWg := sync.WaitGroup{}

	bt.blipContext.HandlerForProfile["rev"] = func(request *blip.Message) {
		defer revsFinishedWg.Done()
		log.Printf("received rev request")

		// assert the rev property contains cv
		rev := request.Properties["rev"]
		assert.Equal(t, docVersion2.CV.String(), rev)

		// assert that history contain current revID and previous revID
		history := request.Properties["history"]
		historyList := strings.Split(history, ",")
		assert.Len(t, historyList, 2)
		assert.Equal(t, docVersion2.RevTreeID, historyList[0])
		assert.Equal(t, docVersion.RevTreeID, historyList[1])
	}

	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		log.Printf("got changes message: %+v", request)
		body, err := request.Body()
		log.Printf("changes body: %v, err: %v", string(body), err)

		knownRevs := []interface{}{}

		if string(body) != "null" {
			var changesReqs [][]interface{}
			err = base.JSONUnmarshal(body, &changesReqs)
			require.NoError(t, err)

			knownRevs = make([]interface{}, len(changesReqs))

			for i, changesReq := range changesReqs {
				docID := changesReq[1].(string)
				revID := changesReq[2].(string)
				log.Printf("change: %s %s", docID, revID)

				// fill known rev with revision 1 of doc1, this will replicate a situation where client has legacy rev of
				// a document that SGW had a newer version of
				knownRevs[i] = []string{rev1ID}
			}
		}

		if !request.NoReply() {
			response := request.Response()
			emptyResponseValBytes, err := base.JSONMarshal(knownRevs)
			require.NoError(t, err)
			response.SetBody(emptyResponseValBytes)
		}
		receivedChangesRequestWg.Done()
	}

	subChangesRequest := bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	sent := bt.sender.Send(subChangesRequest)
	assert.True(t, sent)
	// changes will be called again with empty changes so hence the wait group of 2
	receivedChangesRequestWg.Add(2)

	// expect 1 rev message
	revsFinishedWg.Add(1)

	subChangesResponse := subChangesRequest.Response()
	assert.Equal(t, subChangesRequest.SerialNumber(), subChangesResponse.SerialNumber())

	timeoutErr := WaitWithTimeout(&receivedChangesRequestWg, time.Second*10)
	require.NoError(t, timeoutErr, "Timed out waiting")

	timeoutErr = WaitWithTimeout(&revsFinishedWg, time.Second*10)
	require.NoError(t, timeoutErr, "Timed out waiting")

}

// TestChangesResponseWithHLVInHistory:
//   - Create doc
//   - Update doc with hlv agent to mock update from a another peer
//   - Send subChanges request and have custom changes handler to force a revID change being constructed
//   - Have custom rev handler to asser the subsequent rev message is as expected with cv as rev and pv + full rev
//     tree in history
func TestChangesResponseWithHLVInHistory(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	collection, ctx := rt.GetSingleTestDatabaseCollection()

	docVersion := rt.PutDocDirectly("doc1", db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	newDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)

	agent := db.NewHLVAgent(t, rt.GetSingleDataStore(), "newSource", base.VvXattrName)
	_ = agent.UpdateWithHLV(ctx, "doc1", newDoc.Cas, newDoc.HLV)

	// force import
	newDoc, err = collection.GetDocument(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)
	// wait for pending change to avoid flakes where changes feed didn't pick up this change
	rt.WaitForPendingChanges()

	receivedChangesRequestWg := sync.WaitGroup{}
	receivedChangesRequestWg.Add(2)
	revsFinishedWg := sync.WaitGroup{}
	revsFinishedWg.Add(1)

	bt.blipContext.HandlerForProfile["rev"] = func(request *blip.Message) {
		defer revsFinishedWg.Done()
		log.Printf("received rev request")

		// assert the rev property contains cv
		rev := request.Properties["rev"]
		assert.Equal(t, newDoc.HLV.GetCurrentVersionString(), rev)

		// assert that history contain current revID and previous revID + pv of HLV
		history := request.Properties["history"]
		historyList := strings.Split(history, ",")
		assert.Len(t, historyList, 3)
		assert.Equal(t, newDoc.CurrentRev, historyList[1])
		assert.Equal(t, docVersion.RevTreeID, historyList[2])
		assert.Equal(t, docVersion.CV.String(), historyList[0])
	}

	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {
		defer receivedChangesRequestWg.Done()

		log.Printf("got changes message: %+v", request)
		body, err := request.Body()
		log.Printf("changes body: %v, err: %v", string(body), err)

		knownRevs := []interface{}{}

		if string(body) != "null" {
			var changesReqs [][]interface{}
			err = base.JSONUnmarshal(body, &changesReqs)
			require.NoError(t, err)

			knownRevs = make([]interface{}, len(changesReqs))

			for i, changesReq := range changesReqs {
				docID := changesReq[1].(string)
				revID := changesReq[2].(string)
				log.Printf("change: %s %s", docID, revID)

				// fill known rev with revision 1 of doc1, this will replicate a situation where client has legacy rev of
				// a document that SGW had a newer version of
				knownRevs[i] = []string{rev1ID}
			}
		}

		if !request.NoReply() {
			response := request.Response()
			emptyResponseValBytes, err := base.JSONMarshal(knownRevs)
			require.NoError(t, err)
			response.SetBody(emptyResponseValBytes)
		}
	}

	subChangesRequest := bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	sent := bt.sender.Send(subChangesRequest)
	assert.True(t, sent)

	subChangesResponse := subChangesRequest.Response()
	assert.Equal(t, subChangesRequest.SerialNumber(), subChangesResponse.SerialNumber())

	timeoutErr := WaitWithTimeout(&receivedChangesRequestWg, time.Second*10)
	require.NoError(t, timeoutErr, "Timed out waiting")

	timeoutErr = WaitWithTimeout(&revsFinishedWg, time.Second*10)
	require.NoError(t, timeoutErr, "Timed out waiting")
}

// TestCBLHasPreUpgradeMutationThatHasNotBeenReplicated:
//   - Test case 2 of non conflict plan from design doc
func TestCBLHasPreUpgradeMutationThatHasNotBeenReplicated(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	ds := rt.GetSingleDataStore()

	docVersion := rt.PutDocDirectly("doc1", db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	require.NoError(t, ds.RemoveXattrs(ctx, "doc1", []string{base.VvXattrName}, docVersion.CV.Value))
	rt.GetDatabase().FlushRevisionCacheForTest()

	history := []string{rev1ID}
	sent, _, _, err := bt.SendRevWithHistory("doc1", "2-abc", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)
	// assert a cv was assigned
	assert.NotEqual(t, "", bucketDoc.HLV.GetCurrentVersionString())
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.Equal(t, "2-abc", bucketDoc.CurrentRev)
}

// TestCBLHasOfPreUpgradeMutationThatSGWAlreadyKnows:
//   - Test case 3 of non conflict plan from design doc
func TestCBLHasOfPreUpgradeMutationThatSGWAlreadyKnows(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	ds := rt.GetSingleDataStore()

	docVersion := rt.PutDocDirectly("doc1", db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	docVersion = rt.UpdateDocDirectly("doc1", docVersion, db.Body{"test": "update"})
	rev2ID := docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	require.NoError(t, ds.RemoveXattrs(ctx, "doc1", []string{base.VvXattrName}, docVersion.CV.Value))
	rt.GetDatabase().FlushRevisionCacheForTest()

	history := []string{rev1ID}
	sent, _, _, err := bt.SendRevWithHistory("doc1", rev2ID, history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, rev2ID, bucketDoc.CurrentRev)
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.NotNil(t, bucketDoc.History[rev2ID])
}

// TestPushOfPostUpgradeMutationThatHasCommonAncestorToSGWVersion:
//   - Test case 6 of non conflict plan from design doc
func TestPushOfPostUpgradeMutationThatHasCommonAncestorToSGWVersion(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	ds := rt.GetSingleDataStore()

	docVersion := rt.PutDocDirectly("doc1", db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	docVersion = rt.UpdateDocDirectly("doc1", docVersion, db.Body{"test": "update"})
	rev2ID := docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	require.NoError(t, ds.RemoveXattrs(ctx, "doc1", []string{base.VvXattrName}, docVersion.CV.Value))
	rt.GetDatabase().FlushRevisionCacheForTest()

	// send 100@CBL1
	sent, _, _, err := bt.SendRevWithHistory("doc1", "100@CBL1", nil, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)

	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.NotEqual(t, rev2ID, bucketDoc.CurrentRev)
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.NotNil(t, bucketDoc.History[rev2ID])
	assert.Equal(t, "100@CBL1", bucketDoc.HLV.GetCurrentVersionString())
}

// TestPushDocConflictBetweenPreUpgradeCBLMutationAndPreUpgradeSGWMutation:
//   - Test case 1 of conflict test plan from design doc
func TestPushDocConflictBetweenPreUpgradeCBLMutationAndPreUpgradeSGWMutation(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	ds := rt.GetSingleDataStore()

	docVersion := rt.PutDocDirectly("doc1", db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	docVersion = rt.UpdateDocDirectly("doc1", docVersion, db.Body{"test": "update"})
	rev2ID := docVersion.RevTreeID

	docVersion = rt.UpdateDocDirectly("doc1", docVersion, db.Body{"test": "update1"})
	rev3ID := docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	require.NoError(t, ds.RemoveXattrs(ctx, "doc1", []string{base.VvXattrName}, docVersion.CV.Value))
	rt.GetDatabase().FlushRevisionCacheForTest()

	// send rev 3-def
	history := []string{rev2ID, rev1ID}
	sent, _, _, err := bt.SendRevWithHistory("doc1", "3-def", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.ErrorContains(t, err, "Document revision conflict")

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, rev3ID, bucketDoc.CurrentRev)
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.NotNil(t, bucketDoc.History[rev2ID])
}

// TestPushDocConflictBetweenPreUpgradeCBLMutationAndPostUpgradeSGWMutation:
//   - Test case 3 of conflict test plan from design doc
func TestPushDocConflictBetweenPreUpgradeCBLMutationAndPostUpgradeSGWMutation(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	collection, ctx := rt.GetSingleTestDatabaseCollection()

	docVersion := rt.PutDocDirectly("doc1", db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	docVersion = rt.UpdateDocDirectly("doc1", docVersion, db.Body{"test": "update"})
	rev2ID := docVersion.RevTreeID

	docVersion = rt.UpdateDocDirectly("doc1", docVersion, db.Body{"test": "update1"})
	rev3ID := docVersion.RevTreeID

	// send rev 3-def
	history := []string{rev2ID, rev1ID}
	sent, _, _, err := bt.SendRevWithHistory("doc1", "3-def", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.ErrorContains(t, err, "Document revision conflict")

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, rev3ID, bucketDoc.CurrentRev)
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.NotNil(t, bucketDoc.History[rev2ID])
}

// TestConflictBetweenPostUpgradeCBLMutationAndPostUpgradeSGWMutation:
//   - Test case 6 of conflict test plan from design doc
//   - First sent rev will not conflict as current local rev is parent of incoming rev
//   - Second sent rev will conflict as incoming rev has no common ancestor with local rev and HLV's are in conflict
func TestConflictBetweenPostUpgradeCBLMutationAndPostUpgradeSGWMutation(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	const (
		docID  = "doc1"
		docID2 = "doc2"
	)

	docVersion := rt.PutDocDirectly(docID, db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	history := []string{rev1ID}
	sent, _, _, err := bt.SendRevWithHistory(docID, "100@CBL1", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "100@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.Equal(t, docVersion.CV.Value, bucketDoc.HLV.PreviousVersions[docVersion.CV.SourceID])

	// conflict rev
	docVersion = rt.PutDocDirectly(docID2, db.Body{"some": "doc"})
	rev1ID = docVersion.RevTreeID

	history = []string{"1-abc"}
	sent, _, _, err = bt.SendRevWithHistory(docID2, "100@CBL1", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.ErrorContains(t, err, "Document revision conflict")

	// assert that the bucket doc is as expected
	bucketDoc, _, err = collection.GetDocWithXattrs(ctx, docID2, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, rev1ID, bucketDoc.CurrentRev)
	assert.Equal(t, docVersion.CV.String(), bucketDoc.HLV.GetCurrentVersionString())
}

func TestLegacyRevNotInConflict(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyCRUD)
	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	const docID = "doc1"

	docVersion := rt.PutDocDirectly(docID, db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	// have two history entries, 1 rev from a different CBL and 1 legacy rev, should generate conflict
	history := []string{"1-CBL2", "1-abc"}
	sent, _, _, err := bt.SendRevWithHistory(docID, "100@CBL1", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.ErrorContains(t, err, "Document revision conflict")

	history = []string{docVersion.CV.String(), "1-abc"}
	sent, _, _, err = bt.SendRevWithHistory(docID, "100@CBL1", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "100@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.Equal(t, docVersion.CV.Value, bucketDoc.HLV.PreviousVersions[docVersion.CV.SourceID])

}
