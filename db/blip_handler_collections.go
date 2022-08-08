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
	for i, collection := range requestBody.Collections {
		scope, collectionName, err := parseScopeAndCollection(collection)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid specification for collection: %s", err)
		}

		scopeOnDb, ok := bh.db.Scopes[*scope]
		if !ok {
			checkpoints[i] = nil
			bh.collectionMapping = append(bh.collectionMapping, nil)
			continue
		}
		collection, ok := scopeOnDb.Collections[*collectionName]
		if !ok {
			checkpoints[i] = nil
			bh.collectionMapping = append(bh.collectionMapping, nil)
			continue
		}
		key := CheckpointDocIDPrefix + requestBody.CheckpointIDs[i]
		collectionDB := &Database{DatabaseContext: collection.CollectionCtx, user: bh.blipContextDb.User(), Ctx: bh.db.Ctx}
		value, err := collectionDB.GetLocal(key)
		if err != nil {
			status, _ := base.ErrorAsHTTPStatus(err)
			if status == http.StatusNotFound {
				checkpoints[i] = Body{}
				bh.collectionMapping = append(bh.collectionMapping, collectionDB)
			} else {
				// TODO: CBG-2203 - should we return an error such that the client disconnects here?
				base.WarnfCtx(bh.loggingCtx, "Unable to fetch client checkpoint %q for collection %s.%s", key, *scope, *collectionName)
				checkpoints[i] = nil
				bh.collectionMapping = append(bh.collectionMapping, nil)
			}
			continue
		}
		delete(value, BodyId)
		checkpoints[i] = value
		bh.collectionMapping = append(bh.collectionMapping, collectionDB)
	}
	response := rq.Response()
	if response == nil {
		return fmt.Errorf("Internal go-blip error generating request response")
	}
	return response.SetJSONBody(checkpoints)
}
