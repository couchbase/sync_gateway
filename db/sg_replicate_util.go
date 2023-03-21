/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
)

// CollectionChannelsFromQueryParams returns the channels associated with the byChannel replication filter from the generic queryParams.
// The channels may be passed in one of two ways:
//
//  1. As a JSON array of strings directly:
//     ["channel1", "channel2"]
//
//  2. As a JSON array of strings embedded in a JSON object with the "channels" property:
//     {"channels": ["channel1", "channel2"] }
//
// 3. As a JSON object with a property for each collection, with the value being an array of channels:
//
//	{
//	  "collections_channels": {
//	    "collection1": ["scope1.channel1"],
//	    "collection2": ["scope1.channel1", "scope1.channel2"]
//	  }
//	}
func CollectionChannelsFromQueryParams(namedCollections []string, queryParams interface{}) (perCollectionChannels [][]string, allCollectionsChannels []string, err error) {
	switch val := queryParams.(type) {
	case map[string]interface{}:
		if chanarray, ok := val["channels"].([]interface{}); ok {
			allCollectionsChannels, err = interfaceValsToStringVals(chanarray)
			return nil, allCollectionsChannels, err
		}

		if collectionChannels, ok := val["collections_channels"].(map[string]interface{}); ok {
			perCollectionChannels = make([][]string, len(namedCollections))
			for i, collection := range namedCollections {
				collectionQueryParams := collectionChannels[collection]
				collectionQueryParamsArray, ok := collectionQueryParams.([]interface{})
				if !ok {
					return nil, nil, errors.New("query_params must be a map of collection name to array of channels")
				}
				channels, err := interfaceValsToStringVals(collectionQueryParamsArray)
				if err != nil {
					return nil, nil, err
				}
				perCollectionChannels[i] = channels
			}
		}
		return perCollectionChannels, nil, nil
	case []interface{}:
		allCollectionsChannels, err = interfaceValsToStringVals(val)
		return nil, allCollectionsChannels, err
	default:
		return nil, nil, base.HTTPErrorf(http.StatusBadRequest, ConfigErrorBadChannelsArray)
	}
}

// interfaceValsToStringVals takes a []interface{} and returns a []string
func interfaceValsToStringVals(interfaceVals []interface{}) ([]string, error) {
	stringVals := make([]string, len(interfaceVals))
	for i := range interfaceVals {
		str, ok := interfaceVals[i].(string)
		if !ok {
			return nil, fmt.Errorf("Bad channel name %v (%T) in query_params for sync_gateway/bychannel filter - expecting strings", interfaceVals[i], interfaceVals[i])
		}
		stringVals[i] = str
	}
	return stringVals, nil
}
