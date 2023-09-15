// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

func parseScopeAndCollection(sc string) (scope, collection *string, err error) {

	parts := strings.Split(sc, base.ScopeCollectionSeparator)
	switch len(parts) {
	case 1:
		scope = base.StringPtr(base.DefaultScope)
		collection = &parts[0]
	case 2:
		scope = &parts[0]
		collection = &parts[1]
	default:
		return nil, nil, fmt.Errorf("unknown keyspace format: %q - expected 1-2 fields", sc)
	}

	if *scope == "" {
		return nil, nil, fmt.Errorf("Scope in [scope.]collection can not be empty %q", sc)
	}

	if *collection == "" {
		return nil, nil, fmt.Errorf("Collection in [scope.]collection can not be empty %q", sc)
	}

	return scope, collection, nil
}

func (bh *blipHandler) handleGetCollections(rq *blip.Message) error {
	var requestBody GetCollectionsRequestBody
	err := rq.ReadJSONBody(&requestBody)
	if err != nil {
		return fmt.Errorf("Error deserializing json Sync err: %w body: %s", err, requestBody)
	}
	bh.logEndpointEntry(rq.Profile(), requestBody.String())

	if len(requestBody.Collections) != len(requestBody.CheckpointIDs) {
		return base.HTTPErrorf(http.StatusBadRequest, "Length of collections must match length of checkpoint ids. Request body: %v", requestBody)
	}

	checkpoints := make([]Body, len(requestBody.Collections))
	collectionContexts := make([]*blipSyncCollectionContext, len(requestBody.Collections))
	for i, scopeAndCollection := range requestBody.Collections {
		scope, collectionName, err := parseScopeAndCollection(scopeAndCollection)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid specification for collection: %s", err)
		}

		scopeOnDb, ok := bh.db.Scopes[*scope]
		if !ok {
			checkpoints[i] = nil
			collectionContexts[i] = nil
			continue
		}
		collection, ok := scopeOnDb.Collections[*collectionName]
		if !ok {
			checkpoints[i] = nil
			collectionContexts[i] = nil
			continue
		}
		key := CheckpointDocIDPrefix + requestBody.CheckpointIDs[i]
		value, err := collection.GetSpecial(DocTypeLocal, key)
		if err != nil {
			status, _ := base.ErrorAsHTTPStatus(err)
			if status == http.StatusNotFound {
				checkpoints[i] = Body{}
				collectionContexts[i] = newBlipSyncCollectionContext(bh.loggingCtx, collection)
			} else {
				errMsg := fmt.Sprintf("Unable to fetch client checkpoint %q for collection %s: %s", key, scopeAndCollection, err)
				base.WarnfCtx(bh.loggingCtx, errMsg)
				return base.HTTPErrorf(http.StatusServiceUnavailable, errMsg)
			}
			continue
		}
		delete(value, BodyId)
		checkpoints[i] = value
		collectionContexts[i] = newBlipSyncCollectionContext(bh.loggingCtx, collection)
	}
	bh.collections.set(collectionContexts)
	response := rq.Response()
	if response == nil {
		return fmt.Errorf("Internal go-blip error generating request response")
	}
	return response.SetJSONBody(checkpoints)
}
