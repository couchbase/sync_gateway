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

// ChannelsFromQueryParams returns the channels associated with the byChannel replication filter from the generic queryParams.
// The channels may be passed in one of two ways:
//
//  1. As a JSON array of strings directly:
//     ["channel1", "channel2"]
//
//  2. As a JSON array of strings embedded in a JSON object with the "channels" property:
//     {"channels": ["channel1", "channel2"] }
func ChannelsFromQueryParams(queryParams interface{}) (channels []string, err error) {
	var chanarray []interface{}

	switch val := queryParams.(type) {
	case map[string]interface{}:
		var ok bool
		chanarray, ok = val["channels"].([]interface{})
		if !ok {
			return nil, errors.New("Replication specifies sync_gateway/bychannel filter, but query_params is missing channels property")
		}
	case []interface{}:
		chanarray = val
	default:
		return nil, base.HTTPErrorf(http.StatusBadRequest, ConfigErrorBadChannelsArray)
	}

	return interfaceValsToStringVals(chanarray)
}

// CollectionChannelsFromQueryParams returns the channels for each collection associated with the byChannel replication filter from the generic queryParams.
// An object for each collection containing a string array of channel names is set inside a "collection_channels" property of a JSON object.
//
// Example:
//
//	{
//	  "collection_channels": {
//	    "collection1": ["scope1.channel1"],
//	    "collection2": ["scope1.channel1", "scope1.channel2"]
//	  }
//	}
func CollectionChannelsFromQueryParams(collections []string, queryParams interface{}) (collectionChannels [][]string, err error) {
	channelsByCollection, ok := queryParams.(map[string]interface{})
	if !ok {
		return nil, errors.New("query_params must be a map of collection name to channels")
	}

	collectionChannels = make([][]string, len(collections))
	for i, collection := range collections {
		collectionQueryParams := channelsByCollection[collection]
		collectionQueryParamsArray, ok := collectionQueryParams.([]interface{})
		if !ok {
			return nil, errors.New("query_params must be a map of collection name to channels")
		}
		channels, err := interfaceValsToStringVals(collectionQueryParamsArray)
		if err != nil {
			return nil, err
		}
		collectionChannels[i] = channels
	}
	return collectionChannels, nil
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
