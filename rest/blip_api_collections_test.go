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
	rtConfig := &RestTesterConfig{GuestEnabled: true}
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTesterMultipleCollections(t, rtConfig, 1)
		defer rt.Close()
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			SkipCollectionsInitialization: true,
			SupportedBLIPProtocols:        SupportedBLIPProtocols,
		})
		defer btc.Close()

		checkpointID1 := "checkpoint1"
		checkpoint1Body := db.Body{"seq": "123"}
		collection := btc.rt.GetSingleTestDatabaseCollection()
		scopeAndCollection := fmt.Sprintf("%s.%s", collection.ScopeName, collection.Name)
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
			rt.Run(testCase.name, func(t *testing.T) {
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
	})
}

func TestBlipReplicationNoDefaultCollection(t *testing.T) {
	base.TestRequiresCollections(t)

	rtConfig := &RestTesterConfig{
		GuestEnabled: true,
	}
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t, rtConfig)
		defer rt.Close()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()
		checkpointID1 := "checkpoint1"
		checkpoint1Body := db.Body{"seq": "123"}
		collection := btc.rt.GetSingleTestDatabaseCollection()
		revID, err := collection.PutSpecial(db.DocTypeLocal, db.CheckpointDocIDPrefix+checkpointID1, checkpoint1Body)
		require.NoError(t, err)
		checkpoint1RevID := "0-1"
		require.Equal(t, checkpoint1RevID, revID)

		subChangesRequest := blip.NewRequest()
		subChangesRequest.SetProfile(db.MessageSubChanges)

		require.NoError(t, btc.pullReplication.sendMsg(subChangesRequest))
		resp := subChangesRequest.Response()
		require.Equal(t, strconv.Itoa(http.StatusBadRequest), resp.Properties[db.BlipErrorCode])
	})
}

func TestBlipGetCollectionsAndSetCheckpoint(t *testing.T) {
	base.TestRequiresCollections(t)

	rtConfig := &RestTesterConfig{
		GuestEnabled: true,
	}
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t, rtConfig)
		defer rt.Close()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		checkpointID1 := "checkpoint1"
		checkpoint1Body := db.Body{"seq": "123"}
		collection := btc.rt.GetSingleTestDatabaseCollection()
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
	})
}

func TestCollectionsReplication(t *testing.T) {
	base.TestRequiresCollections(t)

	rtConfig := &RestTesterConfig{
		GuestEnabled: true,
	}
	const docID = "doc1"
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTester(t, rtConfig)
		defer rt.Close()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		version := btc.rt.PutDocDirectly(docID, db.Body{})
		require.NoError(t, btc.rt.WaitForPendingChanges())

		btcCollection := btcRunner.SingleCollection(btc.id)

		err := btcCollection.StartOneshotPull()
		require.NoError(t, err)

		btcCollection.WaitForVersion(docID, version)
	})
}

func TestBlipReplicationMultipleCollections(t *testing.T) {
	rtConfig := &RestTesterConfig{
		GuestEnabled: true,
	}
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTesterMultipleCollections(t, rtConfig, 2)
		defer rt.Close()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		docName := "doc1"
		body := `{"foo":"bar"}`
		versions := make([]DocVersion, 0, len(btc.rt.GetKeyspaces()))
		for _, collection := range btc.rt.GetDbCollections() {
			docVersion := rt.PutDocDirectlyInCollection(collection, docName, db.Body{"foo": "bar"})
			versions = append(versions, docVersion)
		}
		require.NoError(t, btc.rt.WaitForPendingChanges())

		// start all the clients first
		for _, collectionClient := range btc.collectionClients {
			require.NoError(t, collectionClient.StartPull())
		}

		for i, collectionClient := range btc.collectionClients {
			msg := collectionClient.WaitForVersion(docName, versions[i])
			require.Equal(t, body, string(msg))
		}

		for _, collectionClient := range btc.collectionClients {
			resp, err := collectionClient.UnsubPullChanges()
			assert.NoError(t, err, "Error unsubing: %+v", resp)
		}
	})
}

func TestBlipReplicationMultipleCollectionsMismatchedDocSizes(t *testing.T) {
	rtConfig := &RestTesterConfig{
		GuestEnabled: true,
	}
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T, SupportedBLIPProtocols []string) {
		rt := NewRestTesterMultipleCollections(t, rtConfig, 2)
		defer rt.Close()

		opts := &BlipTesterClientOpts{SupportedBLIPProtocols: SupportedBLIPProtocols}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		body := `{"foo":"bar"}`
		collectionDocIDs := make(map[string][]string)
		collectionVersions := make(map[string][]DocVersion)
		require.Len(t, btc.rt.GetKeyspaces(), 2)
		for i, collection := range btc.rt.GetDbCollections() {
			// intentionally create collections with different size replications to ensure one collection finishing won't cancel another one
			docCount := 10
			if i == 0 {
				docCount = 1
			}
			blipName := btc.rt.getCollectionsForBLIP()[i]
			for j := 0; j < docCount; j++ {
				docName := fmt.Sprintf("doc%d", j)
				version := rt.PutDocDirectlyInCollection(collection, docName, db.Body{"foo": "bar"})

				collectionVersions[blipName] = append(collectionVersions[blipName], version)
				collectionDocIDs[blipName] = append(collectionDocIDs[blipName], docName)
			}
		}
		require.NoError(t, btc.rt.WaitForPendingChanges())

		// start all the clients first
		for _, collectionClient := range btc.collectionClients {
			require.NoError(t, collectionClient.StartOneshotPull())
		}

		for _, collectionClient := range btc.collectionClients {
			versions := collectionVersions[collectionClient.collection]
			docIDs := collectionDocIDs[collectionClient.collection]
			msg := collectionClient.WaitForVersion(docIDs[len(docIDs)-1], versions[len(versions)-1])
			require.Equal(t, body, string(msg))
		}

		for _, collectionClient := range btc.collectionClients {
			resp, err := collectionClient.UnsubPullChanges()
			assert.NoError(t, err, "Error unsubing: %+v", resp)
		}
	})
}
