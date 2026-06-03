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

	var changeList [][]any
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
		expectedValue any
	}

	proposeChangesCases := []proposeChangesCase{
		{
			description:   "conflicting insert, legacy rev",
			key:           "conflictingInsert",
			revID:         "1-abc",
			parentRevID:   "",
			expectedValue: map[string]any{"status": float64(db.ProposedRev_Conflict), "rev": conflictingInsertRev},
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
			expectedValue: map[string]any{"status": float64(db.ProposedRev_Conflict), "rev": conflictingUpdateRev2},
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
			expectedValue: map[string]any{"status": float64(db.ProposedRev_Conflict), "rev": conflictingUpdateVersion2.String()},
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

	proposedChanges := make([][]any, 0)
	for _, c := range proposeChangesCases {
		changeEntry := []any{
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

	var changeList []any
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
	base.SetUpTestLogging(t, base.LevelInfo, base.KeySync, base.KeySyncMsg, base.KeyCRUD)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester
	collection, ctx := rt.GetSingleTestDatabaseCollection()

	// add doc to SGW
	docVersion := rt.PutDoc("doc1", `{"test": "doc"}`)
	rev1ID := docVersion.RevTreeID

	// Send another rev of same doc
	history := []string{rev1ID}
	bt.SendRevWithHistory("doc1", "2-bcd", history, []byte(`{"key": "val"}`), blip.Properties{})
	rt.WaitForVersion("doc1", DocVersion{RevTreeID: "2-bcd"})

	// assert we can fetch this doc rev
	resp := rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1?rev=2-bcd", "")
	RequireStatus(t, resp, 200)

	encoded2bcd, err := db.LegacyRevToRevTreeEncodedVersion("2-bcd")
	require.NoError(t, err)
	// assert this legacy doc has been given source version pair
	doc1, err := collection.GetDocument(ctx, "doc1", db.DocUnmarshalSync)
	require.NoError(t, err)
	require.Equal(t, *doc1.HLV, db.HybridLogicalVector{
		SourceID:          encoded2bcd.SourceID,
		Version:           encoded2bcd.Value,
		CurrentVersionCAS: doc1.Cas,
		PreviousVersions: db.HLVVersions{
			docVersion.CV.SourceID: docVersion.CV.Value,
		},
	})

	// try new rev to process
	bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)

	rt.WaitForVersion("foo", DocVersion{RevTreeID: "1-abc"})
	// assert we can fetch this doc rev
	resp = rt.SendAdminRequest("GET", "/{{.keyspace}}/foo?rev=1-abc", "")
	RequireStatus(t, resp, 200)

	encoded1abc, err := db.LegacyRevToRevTreeEncodedVersion("1-abc")
	require.NoError(t, err)

	foo, err := collection.GetDocument(ctx, "foo", db.DocUnmarshalSync)
	require.NoError(t, err)

	require.Equal(t, *foo.HLV, db.HybridLogicalVector{
		SourceID:          encoded1abc.SourceID,
		Version:           encoded1abc.Value,
		CurrentVersionCAS: foo.Cas,
	})
}

func TestSendUnsolicitedRevWithRTEDerivedFromLocalRevID(t *testing.T) {
	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts: false,
		GuestEnabled:   true,
		blipProtocols:  []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	rt := bt.restTester

	docID := SafeDocumentName(t, t.Name())
	doc := rt.CreateDocNoHLV(docID, db.Body{"key": "val"})
	sgwVersion := doc.ExtractDocVersion()

	encodedVersion, err := db.LegacyRevToRevTreeEncodedVersion(sgwVersion.RevTreeID)
	require.NoError(t, err)

	// convert to transport format
	cvStr := encodedVersion.String()

	// send unsolicited rev
	bt.SendRev(
		docID,
		cvStr,
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)

	// send marker rev
	bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)
	rt.WaitForVersion("foo", DocVersion{RevTreeID: "1-abc"})

	// assert that rev with cv encoded from same revID server has is not synced
	docVersion, _ := rt.GetDoc(docID)
	assert.Equal(t, sgwVersion.RevTreeID, docVersion.RevTreeID)
	assert.True(t, docVersion.CV.IsEmpty())
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
	docVersion := rt.PutDoc(docID, `{"test": "doc"}`)
	rev1ID := docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	removeHLV(rt, docID)
	rt.GetDatabase().FlushRevisionCacheForTest()

	// Have CBL send an update to that doc, with history in revTreeID format
	history := []string{rev1ID}
	bt.SendRevWithHistory(docID, "1000@CBL1", history, []byte(`{"key": "val"}`), blip.Properties{})

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "1000@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.NotNil(t, bucketDoc.History[rev1ID])

	// 2. CBL sends rev=1010@CBL1, history=1000@CBL2,1-abc when SGW has current rev 1-abc (document underwent multiple p2p updates before being pushed to SGW)
	docVersion = rt.PutDoc(docID2, `{"test": "doc"}`)
	rev1ID = docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	removeHLV(rt, docID2)
	rt.GetDatabase().FlushRevisionCacheForTest()

	// Have CBL send an update to that doc, with history in HLV + revTreeID format
	history = []string{"1000@CBL2", rev1ID}
	bt.SendRevWithHistory(docID2, "1001@CBL1", history, []byte(`{"some": "update"}`), blip.Properties{})

	// assert that the bucket doc is as expected
	bucketDoc, _, err = collection.GetDocWithXattrs(ctx, docID2, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "1001@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.Equal(t, uint64(4096), bucketDoc.HLV.PreviousVersions["CBL2"])
	assert.NotNil(t, bucketDoc.History[rev1ID])

	// 3. CBL sends rev=1010@CBL1, history=1000@CBL2,2-abc,1-abc when SGW has current rev 1-abc (document underwent multiple legacy and p2p updates before being pushed to SGW)
	docVersion = rt.PutDoc(docID3, `{"test": "doc"}`)
	rev1ID = docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	removeHLV(rt, docID3)
	rt.GetDatabase().FlushRevisionCacheForTest()

	history = []string{"1000@CBL2", "2-abc", rev1ID}
	bt.SendRevWithHistory(docID3, "1010@CBL1", history, []byte(`{"some": "update"}`), blip.Properties{})

	// assert that the bucket doc is as expected
	bucketDoc, _, err = collection.GetDocWithXattrs(ctx, docID3, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "1010@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.Equal(t, uint64(4096), bucketDoc.HLV.PreviousVersions["CBL2"])
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.NotNil(t, bucketDoc.History["2-abc"])

	// 4. CBL sends rev=1010@CBL1, history=1-abc when SGW does not have the doc (document underwent multiple legacy and p2p updates before being pushed to SGW)
	history = []string{"1000@CBL2", "1-abc"}
	bt.SendRevWithHistory(docID4, "1010@CBL1", history, []byte(`{"some": "update"}`), blip.Properties{})

	// assert that the bucket doc is as expected
	bucketDoc, _, err = collection.GetDocWithXattrs(ctx, docID4, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "1010@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.Equal(t, uint64(4096), bucketDoc.HLV.PreviousVersions["CBL2"])
	assert.NotNil(t, bucketDoc.History["1-abc"])

	// 5. CBL sends rev=1010@CBL1, history=2-abc and SGW has 1000@CBL2, 2-abc
	// although HLV's are in conflict, this should pass conflict check as local current rev is parent of incoming rev
	docVersion = rt.PutDoc(docID5, `{"test": "doc"}`)

	docVersion = rt.UpdateDoc(docID5, docVersion, `{"some": "update"}`)
	version := docVersion.CV.Value
	rev2ID := docVersion.RevTreeID
	pushedRev := db.Version{
		Value:    version + 1000,
		SourceID: "CBL1",
	}

	history = []string{rev2ID}
	bt.SendRevWithHistory(docID5, pushedRev.String(), history, []byte(`{"some": "update"}`), blip.Properties{})

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
	docVersion = rt.PutDoc(docID6, `{"test": "doc"}`)
	rev1ID = docVersion.RevTreeID

	pushedRev = db.Version{
		Value:    version + 1000,
		SourceID: "CBL1",
	}
	history = []string{"3-abc", "2-abc", rev1ID}
	bt.SendRevWithHistory(docID6, pushedRev.String(), history, []byte(`{"some": "update"}`), blip.Properties{})

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
	const (
		docID  = "doc1"
		docID2 = "doc2"
		docID3 = "doc3"
		docID4 = "doc4"
	)

	// 1. conflicting changes with legacy rev on both sides of communication (no upgrade of doc at all)
	docVersion := rt.PutDoc(docID, `{"test": "doc"}`)
	rev1ID := docVersion.RevTreeID

	docVersion = rt.UpdateDoc(docID, docVersion, `{"some": "update"}`)
	rev2ID := docVersion.RevTreeID

	docVersion = rt.UpdateDoc(docID, docVersion, `{"some": "update2"}`)

	// remove hlv here to simulate a legacy rev
	removeHLV(rt, docID)
	rt.GetDatabase().FlushRevisionCacheForTest()

	bt.SendRevExpectConflict(docID, "3-abc", []byte(`{"key": "val"}`),
		blip.Properties{db.RevMessageHistory: strings.Join([]string{rev2ID, rev1ID}, ",")})

	// 2. same as above but not having the rev be legacy on SGW side (don't remove the hlv)
	docVersion = rt.PutDoc(docID2, `{"test": "doc"}`)
	rev1ID = docVersion.RevTreeID

	docVersion = rt.UpdateDoc(docID2, docVersion, `{"some": "update"}`)
	rev2ID = docVersion.RevTreeID

	docVersion = rt.UpdateDoc(docID2, docVersion, `{"some": "update2"}`)

	bt.SendRevExpectConflict(docID2, "3-abc", []byte(`{"key": "val"}`),
		blip.Properties{db.RevMessageHistory: strings.Join([]string{rev2ID, rev1ID}, ",")})

	// 3. CBL sends rev=1010@CBL1, history=1000@CBL2,1-abc when SGW has current rev 2-abc (document underwent multiple p2p updates before being pushed to SGW)
	docVersion = rt.PutDoc(docID3, `{"test": "doc"}`)
	rev1ID = docVersion.RevTreeID

	docVersion = rt.UpdateDoc(docID3, docVersion, `{"some": "update"}`)

	// remove hlv here to simulate a legacy rev
	removeHLV(rt, docID3)
	rt.GetDatabase().FlushRevisionCacheForTest()

	bt.SendRevExpectConflict(docID3, "1010@CBL1", []byte(`{"key": "val"}`),
		blip.Properties{db.RevMessageHistory: strings.Join([]string{"1000@CBL2", rev1ID}, ",")})
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

	docVersion := rt.PutDoc("doc1", `{"test": "doc"}`)
	rev1ID := docVersion.RevTreeID

	docVersion2 := rt.UpdateDoc("doc1", docVersion, `{"test": "update"}`)
	// wait for pending change to avoid flakes where changes feed didn't pick up this change
	rt.WaitForPendingChanges()
	receivedChangesRequestWg := sync.WaitGroup{}
	// changes will be called:
	// 1. doc1 changes
	// 2. empty changes to indicate feed is complete
	receivedChangesRequestWg.Add(2)

	revsFinishedWg := sync.WaitGroup{}
	// expect 1 rev message for doc1
	revsFinishedWg.Add(1)

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
		defer receivedChangesRequestWg.Done()
		log.Printf("got changes message: %+v", request)
		body, err := request.Body()
		log.Printf("changes body: %v, err: %v", string(body), err)

		knownRevs := []any{}

		if string(body) != "null" {
			var changesReqs [][]any
			err = base.JSONUnmarshal(body, &changesReqs)
			require.NoError(t, err)

			knownRevs = make([]any, len(changesReqs))

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

	base.WaitWithTimeout(t, &receivedChangesRequestWg, time.Second*10)
	base.WaitWithTimeout(t, &revsFinishedWg, time.Second*10)

}

// TestDeltaSyncSendHistoryWithLegacyClient verifies that when a delta is sent to a client that has a legacy revision
// (pre-HLV), the history property on the rev message includes the rev tree so the client can detect conflicts.
//
// Setup:
//   - Create doc rev1 on SGW
//   - Update doc to create rev2 on SGW
//   - Client pre-populated with rev1 as a legacy revtree revision (no HLV)
//   - Client pulls with delta sync enabled
//   - Server computes delta from rev1 → rev2 and sends via sendDelta
//
// Expected rev message (buildRevHistory scenario 3):
//
//	history property: [hlvHistory, rev2RevTreeID, rev1RevTreeID] (no hlv pv here since doc has only been updated by one hlv aware peer)
//	deltaSrc:         rev1RevTreeID
//	body:             the delta (not full body)
func TestDeltaSyncSendRevTreeHistoryWithClientHavingLegacyRev(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("Delta sync requires EE")
	}

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true // V4-specific: tests legacy rev in HLV-aware client

	sgUseDeltas := true
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{
			GuestEnabled: true,
			DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
				DeltaSync: &DeltaSyncConfig{
					Enabled: &sgUseDeltas,
				},
			}},
		})
		defer rt.Close()

		docID := SafeDocumentName(t, t.Name())

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			ClientDeltas: true,
		})
		defer client.Close()

		// Create rev1 and rev2 on SGW
		docVersion1 := rt.PutDoc(docID, `{"key": "val1"}`)
		rev1ID := docVersion1.RevTreeID
		docVersion2 := rt.UpdateDoc(docID, docVersion1, `{"key": "val2"}`)
		rt.WaitForPendingChanges()

		// Pre-populate the client with rev1 as a legacy revtree revision so that when SGW sends changes
		// for rev2, the client reports rev1ID as its known rev (triggering delta from rev1).
		btcRunner.AddRevTreeRev(client.id, docID, rev1ID, EmptyDocVersion(), []byte(`{"key": "val1"}`))

		btcRunner.StartOneshotPull(client.id)
		msg := btcRunner.WaitForPullRevMessage(client.id, docID, docVersion2)

		// Should be sent as delta with rev1 as the delta source
		assert.Equal(t, rev1ID, msg.Properties[db.RevMessageDeltaSrc], "delta source should be the legacy revID the client reported")

		// The rev property should be the CV (HLV-aware identifier)
		assert.Equal(t, docVersion2.CV.String(), msg.Properties[db.RevMessageRev])

		// History should contain rev2's revTreeID then rev1's revTreeID
		// (scenario 3 in buildRevHistory: local HLV-aware, remote legacy; no HLV pv since same source)
		history := msg.Properties[db.RevMessageHistory]
		require.NotEmpty(t, history, "history must not be empty — rev tree history is required for legacy client conflict detection")
		historyList := strings.Split(history, ",")
		require.Len(t, historyList, 2, "history should have rev tree entries only since hlv history is empty for this doc")
		assert.Equal(t, docVersion2.RevTreeID, historyList[0], "first history entry should be current revTreeID")
		assert.Equal(t, docVersion1.RevTreeID, historyList[1], "second history entry should be parent revTreeID")

		// Should NOT have a separate revTreeHistory property (that's only for SGR2 peers)
		assert.Empty(t, msg.Properties[db.RevMessageTreeHistory], "revTreeHistory property should not be set for non-SGR2 clients")
	})
}

// TestDeltaSyncSendRevTreeHistoryWithHLVHistoryClientHavingLegacyRev verifies that when a delta is sent to a client that has a
// legacy revision and the document has multi-source HLV history, the history property contains the HLV previous
// versions followed by the rev tree.
//
// This extends the coverage from TestDeltaSyncSendHistoryWithLegacyClient to the case where HLV history
// is non-empty (the document was updated by a different peer before SGW created rev2).
//
// Setup:
//   - Create doc rev1 on SGW (gets revTreeID + HLV cv)
//   - Update doc via HLV agent to simulate a different peer creating rev2 (gets a new HLV cv with rev1's cv as pv)
//   - Client pre-populated with rev1 as a legacy revtree revision (no HLV)
//   - Client pulls with delta sync enabled
//
// Expected rev message (buildRevHistory scenario 3):
//
//	history property: [pv (rev1's cv), rev2RevTreeID, rev1RevTreeID]
//	deltaSrc:         rev1RevTreeID
func TestDeltaSyncSendRevTreeHistoryWithHLVHistoryClientHavingLegacyRev(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("Delta sync requires EE")
	}

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true // V4-specific: tests legacy rev in HLV-aware client

	sgUseDeltas := true
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{
			GuestEnabled: true,
			DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
				DeltaSync: &DeltaSyncConfig{
					Enabled: &sgUseDeltas,
				},
			}},
		})
		defer rt.Close()

		docID := SafeDocumentName(t, t.Name())

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			ClientDeltas: true,
		})
		defer client.Close()

		collection, ctx := rt.GetSingleTestDatabaseCollection()

		// Create rev1 on SGW — gets revTreeID + HLV
		docVersion1 := rt.PutDoc(docID, `{"key": "val1"}`)
		rev1ID := docVersion1.RevTreeID

		// Pre-populate the client with rev1 as a legacy revtree revision so that when SGW sends changes
		// for the updated doc, the client reports rev1ID as its known rev (triggering delta from rev1).
		btcRunner.AddRevTreeRev(client.id, docID, rev1ID, EmptyDocVersion(), []byte(`{"key": "val1"}`))

		// Update doc via HLV agent to simulate a different peer creating rev2.
		// This gives the doc a new CV from "peerSource" with rev1's CV as a previous version in the HLV.
		newDoc, _, err := collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalAll)
		require.NoError(t, err)
		agent := db.NewHLVAgent(t, rt.GetSingleDataStore(), "peerSource", base.VvXattrName)
		_ = agent.UpdateWithHLV(ctx, docID, newDoc.Cas, newDoc.HLV)

		// Force import so SGW picks up the HLV agent's update
		newDoc, err = collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
		require.NoError(t, err)
		rt.WaitForPendingChanges()

		docVersion2 := newDoc.ExtractDocVersion()
		btcRunner.StartOneshotPull(client.id)
		msg := btcRunner.WaitForPullRevMessage(client.id, docID, docVersion2)

		// Should be sent as delta with rev1 as the delta source
		assert.Equal(t, rev1ID, msg.Properties[db.RevMessageDeltaSrc], "delta source should be the legacy revID the client reported")

		// Rev property should be the new CV from the HLV agent update
		assert.Equal(t, docVersion2.CV.String(), msg.Properties[db.RevMessageRev])

		// History should be: [pv (rev1's original cv), rev2RevTreeID, rev1RevTreeID]
		// - First entry is HLV previous version (rev1's cv becomes pv after the HLV agent update)
		// - Last two entries are the rev tree (scenario 3: local HLV-aware, remote legacy)
		history := msg.Properties[db.RevMessageHistory]
		require.NotEmpty(t, history, "history must not be empty")
		historyList := strings.Split(history, ",")
		require.Len(t, historyList, 3, "history should have HLV pv + 2 rev tree entries")
		assert.Equal(t, docVersion1.CV.String(), historyList[0], "first history entry should be HLV previous version (rev1's cv)")
		assert.Equal(t, docVersion2.RevTreeID, historyList[1], "second history entry should be current revTreeID")
		assert.Equal(t, docVersion1.RevTreeID, historyList[2], "third history entry should be parent revTreeID")

		assert.Empty(t, msg.Properties[db.RevMessageTreeHistory], "revTreeHistory property should not be set for non-SGR2 clients")
	})
}

// TestBothSidesLegacyRevDeltaSync verifies that when a delta is sent to a client that has a legacy revision
// and the document on SGW is also a legacy revision (no HLV on either side), the history property contains
// only the parent revTreeID and the rev property is the current revTreeID (not a CV).
//
// Setup:
//   - Create doc rev1 and rev2 on SGW with no HLV (legacy revs)
//   - Client pre-populated with rev1 as a legacy revtree revision (no HLV)
//   - Client pulls with delta sync enabled
//
// Expected rev message (buildRevHistory scenario 1):
//
//	history property: [rev1RevTreeID] (no hlv pv since neither side is HLV-aware)
//	rev property:     rev2RevTreeID (not a CV, since both sides are legacy)
//	deltaSrc:         rev1RevTreeID
func TestBothSidesLegacyRevDeltaSync(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skip("Delta sync requires EE")
	}

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true // V4-specific: tests legacy rev in HLV-aware client

	sgUseDeltas := true
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{
			GuestEnabled: true,
			DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
				DeltaSync: &DeltaSyncConfig{
					Enabled: &sgUseDeltas,
				},
			}},
		})
		defer rt.Close()

		docID := SafeDocumentName(t, t.Name())

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			ClientDeltas: true,
		})
		defer client.Close()

		// create rev1, rev 2 on SGW with no HLV (legacy revs)
		docRev1 := rt.CreateDocNoHLV(docID, db.Body{"test": "doc"})
		docRev2 := rt.CreateDocNoHLV(docID, db.Body{"test": "update", db.BodyRev: docRev1.GetRevTreeID()})
		rt.WaitForPendingChanges()

		// Pre-populate the client with rev1 as a legacy revtree revision so that when SGW sends changes
		// for rev2, the client reports rev1ID as its known rev (triggering delta from rev1).
		btcRunner.AddRevTreeRev(client.id, docID, docRev1.GetRevTreeID(), EmptyDocVersion(), []byte(`{"test": "doc"}`))

		btcRunner.StartOneshotPull(client.id)
		msg := btcRunner.WaitForPullRevMessage(client.id, docID, docRev2.ExtractDocVersion())

		// Should be sent as delta with rev1 as the delta source
		assert.Equal(t, docRev1.GetRevTreeID(), msg.Properties[db.RevMessageDeltaSrc], "delta source should be the legacy revID the client reported")

		// Rev property should be rev2's rev tree ID since both sides are legacy
		assert.Equal(t, docRev2.GetRevTreeID(), msg.Properties[db.RevMessageRev])

		// History should contain rev1's revTreeID
		history := msg.Properties[db.RevMessageHistory]
		require.NotEmpty(t, history, "history must not be empty — rev tree history is required for legacy client conflict detection")
		historyList := strings.Split(history, ",")
		require.Len(t, historyList, 1, "history should have rev tree entries only since hlv history is empty for this doc")
		assert.Equal(t, docRev1.GetRevTreeID(), historyList[0], "only history entry should be parent revTreeID since both sides are legacy")

		assert.Empty(t, msg.Properties[db.RevMessageTreeHistory], "revTreeHistory property should not be set for non-SGR2 clients")
	})
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

	docVersion := rt.PutDoc("doc1", `{"test": "doc"}`)
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
		assert.Equal(t, newDoc.GetRevTreeID(), historyList[1])
		assert.Equal(t, docVersion.RevTreeID, historyList[2])
		assert.Equal(t, docVersion.CV.String(), historyList[0])
	}

	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {
		defer receivedChangesRequestWg.Done()

		log.Printf("got changes message: %+v", request)
		body, err := request.Body()
		log.Printf("changes body: %v, err: %v", string(body), err)

		knownRevs := []any{}

		if string(body) != "null" {
			var changesReqs [][]any
			err = base.JSONUnmarshal(body, &changesReqs)
			require.NoError(t, err)

			knownRevs = make([]any, len(changesReqs))

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

	base.WaitWithTimeout(t, &receivedChangesRequestWg, time.Second*10)
	base.WaitWithTimeout(t, &revsFinishedWg, time.Second*10)
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

	docVersion := rt.PutDoc("doc1", `{"test": "doc"}`)
	rev1ID := docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	removeHLV(rt, "doc1")
	rt.GetDatabase().FlushRevisionCacheForTest()

	history := []string{rev1ID}
	bt.SendRevWithHistory("doc1", "2-abc", history, []byte(`{"key": "val"}`), blip.Properties{})

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)
	// assert a cv was assigned
	assert.NotEqual(t, "", bucketDoc.HLV.GetCurrentVersionString())
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.Equal(t, "2-abc", bucketDoc.GetRevTreeID())
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

	docVersion := rt.PutDoc("doc1", `{"test": "doc"}`)
	rev1ID := docVersion.RevTreeID

	docVersion = rt.UpdateDoc("doc1", docVersion, `{"test": "update"}`)
	rev2ID := docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	removeHLV(rt, "doc1")
	rt.GetDatabase().FlushRevisionCacheForTest()

	history := []string{rev1ID}
	bt.SendRevWithHistory("doc1", rev2ID, history, []byte(`{"key": "val"}`), blip.Properties{})

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, rev2ID, bucketDoc.GetRevTreeID())
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

	docVersion := rt.PutDoc("doc1", `{"test": "doc"}`)
	rev1ID := docVersion.RevTreeID

	docVersion = rt.UpdateDoc("doc1", docVersion, `{"test": "update"}`)
	rev2ID := docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	removeHLV(rt, "doc1")
	rt.GetDatabase().FlushRevisionCacheForTest()

	// send 100@CBL1
	bt.SendRevWithHistory("doc1", "100@CBL1", nil, []byte(`{"key": "val"}`), blip.Properties{})

	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.NotEqual(t, rev2ID, bucketDoc.GetRevTreeID())
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

	docVersion := rt.PutDoc("doc1", `{"test": "doc"}`)
	rev1ID := docVersion.RevTreeID

	docVersion = rt.UpdateDoc("doc1", docVersion, `{"test": "update"}`)
	rev2ID := docVersion.RevTreeID

	docVersion = rt.UpdateDoc("doc1", docVersion, `{"test": "update1"}`)
	rev3ID := docVersion.RevTreeID

	// remove hlv here to simulate a legacy rev
	removeHLV(rt, "doc1")
	rt.GetDatabase().FlushRevisionCacheForTest()

	// send rev 3-def
	history := []string{rev2ID, rev1ID}
	bt.SendRevExpectConflict("doc1", "3-def", []byte(`{"key": "val"}`), blip.Properties{db.RevMessageHistory: strings.Join(history, ",")})

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, rev3ID, bucketDoc.GetRevTreeID())
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

	docVersion := rt.PutDoc("doc1", `{"test": "doc"}`)
	rev1ID := docVersion.RevTreeID

	docVersion = rt.UpdateDoc("doc1", docVersion, `{"test": "update"}`)
	rev2ID := docVersion.RevTreeID

	docVersion = rt.UpdateDoc("doc1", docVersion, `{"test": "update1"}`)
	rev3ID := docVersion.RevTreeID

	// send rev 3-def
	history := []string{rev2ID, rev1ID}
	bt.SendRevExpectConflict("doc1", "3-def", []byte(`{"key": "val"}`), blip.Properties{db.RevMessageHistory: strings.Join(history, ",")})

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, rev3ID, bucketDoc.GetRevTreeID())
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

	docVersion := rt.PutDoc(docID, `{"test": "doc"}`)
	rev1ID := docVersion.RevTreeID

	history := []string{rev1ID}
	bt.SendRevWithHistory(docID, "100@CBL1", history, []byte(`{"key": "val"}`), blip.Properties{})

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "100@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.Equal(t, docVersion.CV.Value, bucketDoc.HLV.PreviousVersions[docVersion.CV.SourceID])

	// conflict rev
	docVersion = rt.PutDoc(docID2, `{"some": "doc"}`)
	rev1ID = docVersion.RevTreeID

	bt.SendRevExpectConflict(docID2, "100@CBL1", []byte(`{"key": "val"}`), blip.Properties{db.RevMessageHistory: "1-abc"})

	// assert that the bucket doc is as expected
	bucketDoc, _, err = collection.GetDocWithXattrs(ctx, docID2, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, rev1ID, bucketDoc.GetRevTreeID())
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

	docVersion := rt.PutDoc(docID, `{"test": "doc"}`)
	rev1ID := docVersion.RevTreeID

	// have two history entries, 1 rev from a different CBL and 1 legacy rev, should generate conflict
	history := []string{"1-CBL2", "1-abc"}
	bt.SendRevExpectConflict(docID, "100@CBL1", []byte(`{"key": "val"}`), blip.Properties{db.RevMessageHistory: strings.Join(history, ",")})

	history = []string{docVersion.CV.String(), "1-abc"}
	bt.SendRev(docID, "100@CBL1", []byte(`{"key": "val"}`), blip.Properties{db.RevMessageHistory: strings.Join(history, ",")})

	// assert that the bucket doc is as expected
	bucketDoc, _, err := collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "100@CBL1", bucketDoc.HLV.GetCurrentVersionString())
	assert.NotNil(t, bucketDoc.History[rev1ID])
	assert.Equal(t, docVersion.CV.Value, bucketDoc.HLV.PreviousVersions[docVersion.CV.SourceID])

}

func TestLegacyRevBlipTesterClient(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeySGTest, base.KeyCRUD, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCRUD)
	rtConfig := RestTesterConfig{GuestEnabled: true}
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer client.Close()

		t.Run("push CBL legacy rev", func(t *testing.T) {
			btcRunner.StartPush(client.id)
			defer btcRunner.StopPush(client.id)

			docID := SafeDocumentName(t, t.Name())
			cblDocVersion1 := btcRunner.AddRevTreeRev(client.id, docID, "1-abc", EmptyDocVersion(), []byte(`{"action": "create"}`))
			rt.WaitForVersion(docID, cblDocVersion1)

			cblDocVersion2 := btcRunner.AddRev(client.id, docID, &cblDocVersion1, []byte(`{"action": "update"}`))
			rt.WaitForVersion(docID, cblDocVersion2)
		})

		t.Run("pull SG legacy rev", func(t *testing.T) {
			btcRunner.StartPull(client.id)
			btcRunner.StartPush(client.id)
			defer btcRunner.UnsubPullChanges(client.id)
			defer btcRunner.StopPush(client.id)

			docID := SafeDocumentName(t, t.Name())
			dbc, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
			revTreeID1, _ := dbc.CreateDocNoHLV(t, ctx, docID, db.Body{"action": "create"})
			sgDocVersion1 := DocVersion{RevTreeID: revTreeID1}
			btcRunner.WaitForVersion(client.id, docID, sgDocVersion1)

			// _rev: revTreeID1 to allow updating using CreateDocNoHLV
			revtreeID2, _ := dbc.CreateDocNoHLV(t, ctx, docID, db.Body{"_rev": revTreeID1, "action": "update"})
			sgDocVersion2 := DocVersion{RevTreeID: revtreeID2}
			btcRunner.WaitForVersion(client.id, docID, sgDocVersion2)

			sgVersion3 := btcRunner.AddRev(client.id, docID, &sgDocVersion2, []byte(`{"action": "cbl update"}`))
			require.NotNil(t, sgVersion3)
			rt.WaitForVersion(docID, sgVersion3)
		})
		t.Run("pull SG legacy rev 2-bcd, both sides have 1-abc", func(t *testing.T) {
			docID := SafeDocumentName(t, t.Name())
			dbc, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
			sgVersion1, _ := dbc.CreateDocNoHLV(t, ctx, docID, db.Body{"action": "create"})
			cblVersion1 := btcRunner.AddRevTreeRev(client.id, docID, sgVersion1, EmptyDocVersion(), []byte(`{"action": "create"}`))
			require.Equal(t, sgVersion1, cblVersion1.RevTreeID)
			sgVersion2, _ := dbc.CreateDocNoHLV(t, ctx, docID, db.Body{"_rev": sgVersion1, "action": "update"})
			btcRunner.StartPull(client.id)
			btcRunner.WaitForVersion(client.id, docID, DocVersion{RevTreeID: sgVersion2})
		})
		t.Run("push CBL legacy rev 2-bcd, both sides have 1-abc", func(t *testing.T) {
			docID := SafeDocumentName(t, t.Name())
			dbc, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
			sgVersion1, _ := dbc.CreateDocNoHLV(t, ctx, docID, db.Body{"action": "create"})
			cblVersion1 := btcRunner.AddRevTreeRev(client.id, docID, sgVersion1, EmptyDocVersion(), []byte(`{"action": "create"}`))
			require.Equal(t, sgVersion1, cblVersion1.RevTreeID)

			cblVersion2 := btcRunner.AddRevTreeRev(client.id, docID, "2-bcd", &cblVersion1, []byte(`{"action": "update"}`))

			btcRunner.StartPush(client.id)
			rt.WaitForVersion(docID, cblVersion2)
		})
	})
}

func TestCBLPushEncodedCVDerivedFromSGWLocalRevID(t *testing.T) {
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.SkipSubtest[RevtreeSubtestName] = true // vv specific test
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{
			GuestEnabled: true,
		})
		defer rt.Close()

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer btc.Close()

		docID := SafeDocumentName(t, t.Name())

		// add legacy rev on SGW
		doc := rt.CreateDocNoHLV(docID, db.Body{"key": "val"})
		originalSGWVersion := doc.ExtractDocVersion()

		cblVersion := btcRunner.AddEncodedCVRev(btc.id, docID, originalSGWVersion.RevTreeID, EmptyDocVersion(), []byte(`{"key":"val"}`))
		require.Equal(t, "Revision+Tree+Encoding", cblVersion.CV.SourceID) // we must be saving this rev as legacy encoded cv on client

		btcRunner.StartPush(btc.id)

		// add marker doc
		markerVersion := btcRunner.AddRev(btc.id, "markerDoc", EmptyDocVersion(), []byte(`{"marker":"doc"}`))
		rt.WaitForVersion("markerDoc", markerVersion)

		// assert doc on SGW is still original rev added and not saved as new encoded CV from CBL
		sgwVersion, _ := rt.GetDoc(docID)
		assert.Equal(t, originalSGWVersion.RevTreeID, sgwVersion.RevTreeID)
		assert.True(t, sgwVersion.CV.IsEmpty(), "CV should be empty")
	})
}

// removeHLV removes _vv and clears _sync.ver and _sync.src from a document. Consider instead using CreateDocNoHLV
func removeHLV(rt *RestTester, docID string) {
	ds := rt.GetSingleDataStore()
	ctx := base.TestCtx(rt.TB())
	cas, err := ds.Get(ctx, docID, nil)
	require.NoError(rt.TB(), err)
	require.NoError(rt.TB(), ds.RemoveXattrs(ctx, docID, []string{base.VvXattrName}, cas))
	xattrs, cas, err := ds.GetXattrs(ctx, docID, []string{base.SyncXattrName})
	require.NoError(rt.TB(), err)
	var syncData *db.SyncData
	require.NoError(rt.TB(), base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))
	syncData.RevAndVersion.CurrentSource = ""
	syncData.RevAndVersion.CurrentVersion = ""
	_, err = ds.UpdateXattrs(ctx, docID, 0, cas, map[string][]byte{
		base.SyncXattrName: base.MustJSONMarshal(rt.TB(), syncData),
	}, db.DefaultMutateInOpts())
	require.NoError(rt.TB(), err)
}
