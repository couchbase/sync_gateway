// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"net/http"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

func TestCollectionBlipHandler(t *testing.T) {
	allDBContext := &Database{DatabaseContext: &DatabaseContext{Name: "allDBContext"}}
	realCollectionDB0 := &Database{DatabaseContext: &DatabaseContext{Name: "realCollectionDB0"}}
	realCollectionDB1 := &Database{DatabaseContext: &DatabaseContext{Name: "realCollectionDB1"}}
	testCases := []struct {
		name              string
		blipMessage       *blip.Message
		err               *base.HTTPError
		collection        *Database
		collectionMapping []*Database
	}{
		{
			name:        "NoCollections",
			blipMessage: &blip.Message{},
			err:         nil,
			collection:  allDBContext,
		},
		{
			name: "emptyCollectionNoAnInt",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "a",
				},
			},
			err:        &base.HTTPError{Status: http.StatusBadRequest},
			collection: nil,
		},
		{
			name: "emptyCollectionMapping",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "",
				},
			},
			err:        &base.HTTPError{Status: http.StatusBadRequest},
			collection: nil,
		},
		{
			name: "missingCollectionMapping",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "1",
				},
			},
			err:        &base.HTTPError{Status: http.StatusBadRequest},
			collection: nil,
		},
		{
			name: "missingCollectionMapping",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "1",
				},
			},
			err:        &base.HTTPError{Status: http.StatusBadRequest},
			collection: nil,
		},
		{
			name: "singlePresentCollection",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "0",
				},
			},
			err:               nil,
			collection:        realCollectionDB0,
			collectionMapping: []*Database{realCollectionDB0},
		},
		{
			name: "twoPresentCollections",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "1",
				},
			},
			err:               nil,
			collection:        realCollectionDB1,
			collectionMapping: []*Database{realCollectionDB0, realCollectionDB1},
		},
		{
			name: "collectionPassedInGetCollectionsButHitErrorInGetCollections",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "1",
				},
			},
			err:               &base.HTTPError{Status: http.StatusBadRequest},
			collection:        nil,
			collectionMapping: []*Database{realCollectionDB0, nil},
		},
		{
			name: "outOfRangeCollections",
			blipMessage: &blip.Message{
				Properties: blip.Properties{
					BlipCollection: "2",
				},
			},
			err:               &base.HTTPError{Status: http.StatusBadRequest},
			collection:        nil,
			collectionMapping: []*Database{realCollectionDB0, realCollectionDB1},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			bh := blipHandler{
				db:              allDBContext,
				BlipSyncContext: &BlipSyncContext{collectionMapping: testCase.collectionMapping},
			}

			passedMiddleware := false
			success := func(bh *blipHandler, bm *blip.Message) error {
				passedMiddleware = true
				return nil
			}
			middleware := collectionBlipHandler(success)
			err := middleware(&bh, testCase.blipMessage)
			if testCase.err == nil {
				require.NoError(t, err)
				require.True(t, passedMiddleware)
			} else {
				require.Error(t, err)
				require.Equal(t, testCase.err.Status, err.(*base.HTTPError).Status)
				require.False(t, passedMiddleware)
			}
			require.Equal(t, bh.collection, testCase.collection)
		})
	}
}
