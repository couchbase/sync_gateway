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
	for i, scopeAndCollection := range requestBody.Collections {
		scope, collectionName, err := parseScopeAndCollection(scopeAndCollection)
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
		collectionDB := &Database{DatabaseContext: collection.CollectionCtx, user: bh.blipContextDb.User()}
		value, err := collectionDB.GetSpecial(DocTypeLocal, key)
		if err != nil {
			status, _ := base.ErrorAsHTTPStatus(err)
			if status == http.StatusNotFound {
				checkpoints[i] = Body{}
				bh.collectionMapping = append(bh.collectionMapping, collectionDB)
			} else {
				errMsg := fmt.Sprintf("Unable to fetch client checkpoint %q for collection %s: %s", key, scopeAndCollection, err)
				base.WarnfCtx(bh.loggingCtx, errMsg)
				return base.HTTPErrorf(http.StatusServiceUnavailable, errMsg)
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

func (bsc *BlipSyncContext) getCollectionIndexForDB(db *Database) (int, bool) {
	if bsc.collectionMapping == nil {
		return 0, false
	}
	for i, iDB := range bsc.collectionMapping {
		if iDB.BucketSpec.Scope == db.BucketSpec.Scope && iDB.BucketSpec.Collection == db.BucketSpec.Collection {
			return i, true
		}
	}
	return 0, false
}
