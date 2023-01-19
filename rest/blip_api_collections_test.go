// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

func TestBlipGetCollections(t *testing.T) {
	// FIXME as part of CBG-2203 to enable subtest checkpointExistsWithErrorInNonDefaultCollection
	base.TestRequiresCollections(t)

	// checkpointIDWithError := "checkpointError"

	const defaultScopeAndCollection = "_default._default"
	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{GuestEnabled: true}, 1)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt,
		&BlipTesterClientOpts{
			SkipCollectionsInitialization: true,
		},
	)
	require.NoError(t, err)
	defer btc.Close()

	checkpointID1 := "checkpoint1"
	checkpoint1Body := db.Body{"seq": "123"}
	collection := rt.GetSingleTestDatabaseCollection()
	scopeAndCollection := fmt.Sprintf("%s.%s", collection.ScopeName(), collection.Name())
	revID, err := collection.PutSpecial(db.DocTypeLocal, db.CheckpointDocIDPrefix+checkpointID1, checkpoint1Body)
	require.NoError(t, err)
	checkpoint1RevID := "0-1"
	require.Equal(t, checkpoint1RevID, revID)
	testCases := []struct {
		name        string
		requestBody db.GetCollectionsRequestBody
		resultBody  []db.Body
		errorCode   string
	}{
		{
			name: "noDocInDefaultCollection",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{"id"},
				Collections:   []string{defaultScopeAndCollection},
			},
			resultBody: []db.Body{nil},
			errorCode:  "",
		},
		{
			name: "mismatchedLengthOnInput",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{"id", "id2"},
				Collections:   []string{defaultScopeAndCollection},
			},
			resultBody: []db.Body{nil},
			errorCode:  fmt.Sprintf("%d", http.StatusBadRequest),
		},
		{
			name: "inDefaultCollection",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{checkpointID1},
				Collections:   []string{defaultScopeAndCollection},
			},
			resultBody: []db.Body{nil},
			errorCode:  "",
		},
		{
			name: "badScopeSpecificationEmptyString",
			// bad scope specification - empty string
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{checkpointID1},
				Collections:   []string{""},
			},
			resultBody: []db.Body{nil},
			errorCode:  fmt.Sprintf("%d", http.StatusBadRequest),
		},
		{
			name: "presentNonDefaultCollection",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{checkpointID1},
				Collections:   []string{scopeAndCollection},
			},
			resultBody: []db.Body{checkpoint1Body},
			errorCode:  "",
		},
		{
			name: "unseenInNonDefaultCollection",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{"id"},
				Collections:   []string{scopeAndCollection},
			},
			resultBody: []db.Body{db.Body{}},
			errorCode:  "",
		},
		// {
		//	name: "checkpointExistsWithErrorInNonDefaultCollection",
		//	requestBody: db.GetCollectionsRequestBody{
		//		CheckpointIDs: []string{checkpointIDWithError},
		//		Collections:   []string{scopeAndCollection},
		//	},
		//	resultBody: []db.Body{nil},
		//	errorCode:  "",
		// },
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			getCollectionsRequest, err := db.NewGetCollectionsMessage(testCase.requestBody)
			require.NoError(t, err)

			require.NoError(t, btc.pushReplication.sendMsg(getCollectionsRequest))

			// Check that the response we got back was processed by the norev handler
			resp := getCollectionsRequest.Response()
			require.NotNil(t, resp)
			errorCode, hasErrorCode := resp.Properties[db.BlipErrorCode]
			require.Equal(t, hasErrorCode, testCase.errorCode != "", "Request returned unexpected error %+v", resp.Properties)
			require.Equal(t, errorCode, testCase.errorCode)
			if testCase.errorCode != "" {
				return
			}
			var checkpoints []db.Body
			err = resp.ReadJSONBody(&checkpoints)
			require.NoErrorf(t, err, "Actual error %+v", checkpoints)

			require.Equal(t, testCase.resultBody, checkpoints)
		})
	}
}

func TestBlipReplicationNoDefaultCollection(t *testing.T) {
	base.TestRequiresCollections(t)

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	checkpointID1 := "checkpoint1"
	checkpoint1Body := db.Body{"seq": "123"}
	collection := rt.GetSingleTestDatabaseCollection()
	revID, err := collection.PutSpecial(db.DocTypeLocal, db.CheckpointDocIDPrefix+checkpointID1, checkpoint1Body)
	require.NoError(t, err)
	checkpoint1RevID := "0-1"
	require.Equal(t, checkpoint1RevID, revID)

	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile(db.MessageSubChanges)

	require.NoError(t, btc.pullReplication.sendMsg(subChangesRequest))
	resp := subChangesRequest.Response()
	require.Equal(t, strconv.Itoa(http.StatusBadRequest), resp.Properties[db.BlipErrorCode])
}

func TestBlipGetCollectionsAndSetCheckpoint(t *testing.T) {
	base.TestRequiresCollections(t)

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	checkpointID1 := "checkpoint1"
	checkpoint1Body := db.Body{"seq": "123"}
	collection := rt.GetSingleTestDatabaseCollection()
	revID, err := collection.PutSpecial(db.DocTypeLocal, db.CheckpointDocIDPrefix+checkpointID1, checkpoint1Body)
	require.NoError(t, err)
	checkpoint1RevID := "0-1"
	require.Equal(t, checkpoint1RevID, revID)
	getCollectionsRequest, err := db.NewGetCollectionsMessage(db.GetCollectionsRequestBody{
		CheckpointIDs: []string{checkpointID1},
		Collections:   []string{fmt.Sprintf("%s.%s", collection.ScopeName(), collection.Name())},
	})

	require.NoError(t, err)

	require.NoError(t, btc.pushReplication.sendMsg(getCollectionsRequest))

	// Check that the response we got back was processed by the GetCollections
	resp := getCollectionsRequest.Response()
	require.NotNil(t, resp)
	errorCode, hasErrorCode := resp.Properties[db.BlipErrorCode]
	require.False(t, hasErrorCode)
	require.Equal(t, errorCode, "")
	var checkpoints []db.Body
	err = resp.ReadJSONBody(&checkpoints)
	require.NoErrorf(t, err, "Actual error %+v", checkpoints)
	require.Equal(t, []db.Body{checkpoint1Body}, checkpoints)

	// make sure other functions get called

	requestGetCheckpoint := blip.NewRequest()
	requestGetCheckpoint.SetProfile(db.MessageGetCheckpoint)
	requestGetCheckpoint.Properties[db.BlipClient] = checkpointID1
	requestGetCheckpoint.Properties[db.BlipCollection] = "0"
	require.NoError(t, btc.pushReplication.sendMsg(requestGetCheckpoint))
	resp = requestGetCheckpoint.Response()
	require.NotNil(t, resp)
	errorCode, hasErrorCode = resp.Properties[db.BlipErrorCode]
	require.Equal(t, errorCode, "")
	require.False(t, hasErrorCode)
	var checkpoint db.Body
	err = resp.ReadJSONBody(&checkpoint)
	require.NoErrorf(t, err, "Actual error %+v", checkpoint)

	require.Equal(t, db.Body{"seq": "123"}, checkpoint)

}

func TestCollectionsReplication(t *testing.T) {
	base.TestRequiresCollections(t)

	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", "{}")
	RequireStatus(t, resp, http.StatusCreated)

	require.NoError(t, rt.WaitForPendingChanges())

	btcCollection := btc.SingleCollection()

	err = btcCollection.StartOneshotPull()
	require.NoError(t, err)

	_, ok := btcCollection.WaitForRev("doc1", "1-ca9ad22802b66f662ff171f226211d5c")
	require.True(t, ok)
}
