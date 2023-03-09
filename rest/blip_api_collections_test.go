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
	"github.com/stretchr/testify/assert"
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
	checkpoint1Body := Body{"seq": "123"}
	collection := rt.GetSingleTestDatabaseCollection()
	scopeAndCollection := fmt.Sprintf("%s.%s", collection.ScopeName, collection.Name)
	revID, err := collection.PutSpecial(db.DocTypeLocal, db.CheckpointDocIDPrefix+checkpointID1, checkpoint1Body)
	require.NoError(t, err)
	checkpoint1RevID := "0-1"
	require.Equal(t, checkpoint1RevID, revID)
	testCases := []struct {
		name        string
		requestBody db.GetCollectionsRequestBody
		resultBody  []Body
		errorCode   string
	}{
		{
			name: "noDocInDefaultCollection",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{"id"},
				Collections:   []string{defaultScopeAndCollection},
			},
			resultBody: []Body{nil},
			errorCode:  "",
		},
		{
			name: "mismatchedLengthOnInput",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{"id", "id2"},
				Collections:   []string{defaultScopeAndCollection},
			},
			resultBody: []Body{nil},
			errorCode:  fmt.Sprintf("%d", http.StatusBadRequest),
		},
		{
			name: "inDefaultCollection",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{checkpointID1},
				Collections:   []string{defaultScopeAndCollection},
			},
			resultBody: []Body{nil},
			errorCode:  "",
		},
		{
			name: "badScopeSpecificationEmptyString",
			// bad scope specification - empty string
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{checkpointID1},
				Collections:   []string{""},
			},
			resultBody: []Body{nil},
			errorCode:  fmt.Sprintf("%d", http.StatusBadRequest),
		},
		{
			name: "presentNonDefaultCollection",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{checkpointID1},
				Collections:   []string{scopeAndCollection},
			},
			resultBody: []Body{checkpoint1Body},
			errorCode:  "",
		},
		{
			name: "unseenInNonDefaultCollection",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{"id"},
				Collections:   []string{scopeAndCollection},
			},
			resultBody: []Body{Body{}},
			errorCode:  "",
		},
		// {
		//	name: "checkpointExistsWithErrorInNonDefaultCollection",
		//	requestBody: db.GetCollectionsRequestBody{
		//		CheckpointIDs: []string{checkpointIDWithError},
		//		Collections:   []string{scopeAndCollection},
		//	},
		//	resultBody: []Body{nil},
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
			var checkpoints []Body
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
	checkpoint1Body := Body{"seq": "123"}
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
	checkpoint1Body := Body{"seq": "123"}
	collection := rt.GetSingleTestDatabaseCollection()
	revID, err := collection.PutSpecial(db.DocTypeLocal, db.CheckpointDocIDPrefix+checkpointID1, checkpoint1Body)
	require.NoError(t, err)
	checkpoint1RevID := "0-1"
	require.Equal(t, checkpoint1RevID, revID)
	getCollectionsRequest, err := db.NewGetCollectionsMessage(db.GetCollectionsRequestBody{
		CheckpointIDs: []string{checkpointID1},
		Collections:   []string{fmt.Sprintf("%s.%s", collection.ScopeName, collection.Name)},
	})

	require.NoError(t, err)

	require.NoError(t, btc.pushReplication.sendMsg(getCollectionsRequest))

	// Check that the response we got back was processed by the GetCollections
	resp := getCollectionsRequest.Response()
	require.NotNil(t, resp)
	errorCode, hasErrorCode := resp.Properties[db.BlipErrorCode]
	require.False(t, hasErrorCode)
	require.Equal(t, errorCode, "")
	var checkpoints []Body
	err = resp.ReadJSONBody(&checkpoints)
	require.NoErrorf(t, err, "Actual error %+v", checkpoints)
	require.Equal(t, []Body{checkpoint1Body}, checkpoints)

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
	var checkpoint Body
	err = resp.ReadJSONBody(&checkpoint)
	require.NoErrorf(t, err, "Actual error %+v", checkpoint)

	require.Equal(t, Body{"seq": "123"}, checkpoint)

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

func TestBlipReplicationMultipleCollections(t *testing.T) {
	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{
		GuestEnabled: true,
	}, 2)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	docName := "doc1"
	docRevID := "1-cd809becc169215072fd567eebd8b8de"
	body := Body{}
	bodyBytes := []byte(`{"foo":"bar"}`)
	require.NoError(t, body.Unmarshal(bodyBytes))
	for _, collection := range rt.GetDatabase().CollectionByID {
		collectionWithAdmin := db.DatabaseCollectionWithUser{DatabaseCollection: collection}
		revID, _, err := collectionWithAdmin.Put(base.TestCtx(t), docName, body)
		require.NoError(t, err)
		require.Equal(t, docRevID, revID)

	}

	// start all the clients first
	for _, collectionClient := range btc.collectionClients {
		require.NoError(t, collectionClient.StartPull())
	}

	for _, collectionClient := range btc.collectionClients {
		msg, ok := collectionClient.WaitForRev(docName, docRevID)
		require.True(t, ok)
		require.Equal(t, bodyBytes, msg)
	}

	for _, collectionClient := range btc.collectionClients {
		resp, err := collectionClient.UnsubPullChanges()
		assert.NoError(t, err, "Error unsubing: %+v", resp)
	}

}

func TestBlipReplicationMultipleCollectionsMismatchedDocSizes(t *testing.T) {
	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{
		GuestEnabled: true,
	}, 2)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	body := Body{}
	bodyBytes := []byte(`{"foo":"bar"}`)
	require.NoError(t, body.Unmarshal(bodyBytes))
	collectionRevIDs := make(map[string][]string)
	collectionDocIDs := make(map[string][]string)
	require.Len(t, rt.GetDatabase().CollectionByID, 2)
	for i, collection := range rt.GetDatabase().CollectionByID {
		collectionWithAdmin := db.DatabaseCollectionWithUser{DatabaseCollection: collection}
		// intentionally create collections with different size replications to ensure one collection finishing won't cancel another one
		docCount := 10
		if i == 0 {
			docCount = 1
		}
		blipName := fmt.Sprintf("%s.%s", collection.ScopeName, collection.Name)
		for j := 0; j < docCount; j++ {
			docName := fmt.Sprintf("doc%d", j)
			revID, _, err := collectionWithAdmin.Put(base.TestCtx(t), docName, body)
			require.NoError(t, err)
			collectionRevIDs[blipName] = append(collectionRevIDs[blipName], revID)
			collectionDocIDs[blipName] = append(collectionDocIDs[blipName], docName)
		}
	}
	require.NoError(t, rt.WaitForPendingChanges())

	// start all the clients first
	for _, collectionClient := range btc.collectionClients {
		require.NoError(t, collectionClient.StartOneshotPull())
	}

	for _, collectionClient := range btc.collectionClients {
		revIDs := collectionRevIDs[collectionClient.collection]
		docIDs := collectionDocIDs[collectionClient.collection]
		msg, ok := collectionClient.WaitForRev(docIDs[len(docIDs)-1], revIDs[len(revIDs)-1])
		require.True(t, ok)
		require.Equal(t, bodyBytes, msg)
	}

	for _, collectionClient := range btc.collectionClients {
		resp, err := collectionClient.UnsubPullChanges()
		assert.NoError(t, err, "Error unsubing: %+v", resp)
	}

}
