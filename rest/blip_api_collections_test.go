package rest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

func TestBlipGetCollections(t *testing.T) {
	base.TestRequiresCollections(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{
		guestEnabled: true,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				Scopes: ScopesConfig{
					"fooScope": ScopeConfig{
						Collections: map[string]CollectionConfig{
							"fooCollection": {},
						},
					},
				},
			},
		},
		createScopesAndCollections: true,
		// This code will not work until leaky bucket works with collections CBG-2201
		// TestBucket:                 base.GetTestBucket(t).LeakyBucketClone(base.LeakyBucketConfig{}),
	})

	defer rt.Close()
	// This code will not work until leaky bucket works with collections CBG-2201
	// checkpointIDWithError := "checkpointError"
	// leakyBucket, ok := base.AsLeakyBucket(rt.Bucket())
	// require.True(t, ok)
	// leakyBucket.SetGetRawCallback(func(key string) error {
	//		if key == db.CheckpointDocIDPrefix+checkpointIDWithError {
	//			return fmt.Errorf("a unique error")
	//		}
	//	return nil
	//	})

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	checkpointID1 := "checkpoint1"
	checkpoint1Body := db.Body{"seq": "123"}
	dbInstance := db.Database{DatabaseContext: rt.GetDatabase()}
	revID, err := dbInstance.PutSpecial(db.DocTypeLocal, db.CheckpointDocIDPrefix+checkpointID1, checkpoint1Body)
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
				Collections:   []string{"_default._default"},
			},
			resultBody: []db.Body{nil},
			errorCode:  "",
		},
		{
			name: "mismatchedLengthOnInput",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{"id", "id2"},
				Collections:   []string{"_default._default"},
			},
			resultBody: []db.Body{nil},
			errorCode:  fmt.Sprintf("%d", http.StatusBadRequest),
		},
		{
			name: "inDefaultCollection",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{checkpointID1},
				Collections:   []string{"_default._default"},
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
				Collections:   []string{"fooScope.fooCollection"},
			},
			resultBody: []db.Body{checkpoint1Body},
			errorCode:  "",
		},
		{
			name: "unseenInNonDefaultCollection",
			requestBody: db.GetCollectionsRequestBody{
				CheckpointIDs: []string{"id"},
				Collections:   []string{"fooScope.fooCollection"},
			},
			resultBody: []db.Body{db.Body{}},
			errorCode:  "",
		},
		// This code will not work until leaky bucket works with collections CBG-2201
		//{
		//	name: "checkpointExistsWithErrorInNonDefaultCollection",
		//	requestBody: db.GetCollectionsRequestBody{
		//		CheckpointIDs: []string{checkpointIDWithError},
		//		Collections:   []string{"fooScope.fooCollection"},
		//	},
		//	resultBody: []db.Body{nil},
		//	errorCode:  "",
		//},
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
