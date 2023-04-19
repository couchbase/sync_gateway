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
	"net/http"

	"github.com/couchbase/sync_gateway/base"
)

// CollectionChannelsFromQueryParams returns the channels associated with the byChannel replication filter from the generic queryParams.
// The channels may be passed in one of three ways:
//
//  1. As a JSON array of strings directly. These channel names apply to all collections.
//     ["channel1", "channel2"]
//
//  2. As a JSON array of strings embedded in a JSON object with the "channels" property. These channel names apply to all collections.
//     {"channels": ["channel1", "channel2"] }
//
// 3. As a JSON object with a property for each collection, with the value being an array of channels. Each set of channels is specific to each collection.
//
//	{
//	  "collections_channels": {
//	    "collection1": ["scope1.channel1"],
//	    "collection2": ["scope1.channel1", "scope1.channel2"]
//	  }
//	}
func CollectionChannelsFromQueryParams(localCollections []string, queryParams interface{}) (perCollectionChannels [][]string, allCollectionsChannels []string, err error) {
	switch val := queryParams.(type) {
	case map[string]interface{}:
		_, hasChannels := val["channels"]
		_, hasCollectionsChannels := val["collections_channels"]
		if hasChannels && hasCollectionsChannels {
			return nil, nil, errors.New("query_params cannot contain both 'channels' and 'collections_channels'")
		}

		if hasChannels {
			if chanarray, ok := val["channels"].([]interface{}); ok {
				allCollectionsChannels, err = interfaceValsToStringVals(chanarray)
				return nil, allCollectionsChannels, err
			} else {
				return nil, nil, errors.New("query_params value for 'channels' must be an array of channels")
			}
		}

		if hasCollectionsChannels {
			if localCollections == nil {
				return nil, nil, errors.New("channel filters using 'collections_channels' also requires 'collections_local' to be specified")
			}
			if collectionChannels, ok := val["collections_channels"].(map[string]interface{}); ok {
				perCollectionChannels = make([][]string, len(localCollections))
				for i, collection := range localCollections {
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
			} else {
				return nil, nil, errors.New("query_params value for 'collections_channels' must be a map of collection name to array of channels")
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
			return nil, base.RedactErrorf("Expecting string channel but got %v (%T) in query_params for sync_gateway/bychannel filter", base.UD(interfaceVals[i]), interfaceVals[i])
		}
		stringVals[i] = str
	}
	return stringVals, nil
}
