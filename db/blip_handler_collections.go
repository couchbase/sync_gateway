package db

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/couchbase/go-blip"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

func isDefaultScopeAndCollection(scope, collection string) bool {
	return scope == base.DefaultScope && collection == base.DefaultCollection
}

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
	// assert that collections and checkpoint ids match len
	checkpoints := make([]Body, len(requestBody.Collections))
	for i, collection := range requestBody.Collections {
		scope, collectionName, err := parseScopeAndCollection(collection)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Invalid specification for collection: %s", err)
		}

		var db *Database
		scopeOnDb, ok := bh.db.Scopes[*scope]
		if !ok {
			checkpoints[i] = nil
			continue
		}
		collection, ok := scopeOnDb.Collections[*collectionName]
		if !ok {
			checkpoints[i] = nil
			continue
		}
		db = &Database{DatabaseContext: collection.CollectionCtx}

		key := CheckpointDocIDPrefix + requestBody.CheckpointIDs[i]
		value, err := db.GetSpecial(DocTypeLocal, key)
		if err != nil {

			var missingError sgbucket.MissingError
			if errors.As(err, &missingError) {
				checkpoints[i] = Body{}
			} else {
				checkpoints[i] = nil
			}
			continue
		}
		delete(value, BodyId)
		checkpoints[i] = value
	}
	response := rq.Response()
	if response == nil {
		return fmt.Errorf("Internal go-blip error generating request response")
	}
	return response.SetJSONBody(checkpoints)
}
