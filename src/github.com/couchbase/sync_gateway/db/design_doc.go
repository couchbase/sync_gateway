package db

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

type DesignDoc sgbucket.DesignDoc

const (
	DesignDocSyncGateway      = "sync_gateway"
	DesignDocSyncHousekeeping = "sync_housekeeping"
	ViewPrincipals            = "principals"
	ViewChannels              = "channels"
	ViewAccess                = "access"
	ViewRoleAccess            = "role_access"
	ViewAllBits               = "all_bits"
	ViewAllDocs               = "all_docs"
	ViewImport                = "import"
	ViewOldRevs               = "old_revs"
	ViewSessions              = "sessions"
)

func isInternalDDoc(ddocName string) bool {
	return strings.HasPrefix(ddocName, "sync_")
}

// Enforces access by admins only, and not to the built-in Sync Gateway design docs:
func (db *Database) checkDDocAccess(ddocName string) error {
	if db.user != nil || isInternalDDoc(ddocName) {
		return base.HTTPErrorf(http.StatusForbidden, "forbidden")
	}
	return nil
}

func (db *Database) GetDesignDoc(ddocName string, result interface{}) (err error) {
	if err = db.checkDDocAccess(ddocName); err == nil {
		err = db.Bucket.GetDDoc(ddocName, result)
	}
	return
}

func (db *Database) PutDesignDoc(ddocName string, ddoc DesignDoc) (err error) {
	wrap := true
	if opts := ddoc.Options; opts != nil {
		if opts.Raw == true {
			wrap = false
		}
	}
	if wrap {
		wrapViews(&ddoc)
	}
	if err = db.checkDDocAccess(ddocName); err == nil {
		err = db.Bucket.PutDDoc(ddocName, ddoc)
	}
	return
}

func wrapViews(ddoc *DesignDoc) {
	// Wrap the map functions to ignore special docs and strip _sync metadata:
	for name, view := range ddoc.Views {
		view.Map = `function(doc,meta) {
	                    var sync = doc._sync;
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                      return;
	                    if ((sync.flags & 1) || sync.deleted)
	                      return;
	                    var channels = [];
	                    var channelMap = sync.channels;
						if (channelMap) {
							for (var name in channelMap) {
								removed = channelMap[name];
								if (!removed)
									channels.push(name);
							}
						}
	                    delete doc._sync;
	                    meta.rev = sync.rev;
	                    meta.channels = channels;

	                    var _emit = emit;
	                    (function(){
		                    var emit = function(key,value) {
		                    	_emit(key,[channels, value]);
		                    };
							(` + view.Map + `) (doc, meta);
						}());
						doc._sync = sync;
					}`
		ddoc.Views[name] = view // view is not a pointer, so have to copy it back
	}
}

func (db *Database) DeleteDesignDoc(ddocName string) (err error) {
	if err = db.checkDDocAccess(ddocName); err == nil {
		err = db.Bucket.DeleteDDoc(ddocName)
	}
	return
}

func (db *Database) QueryDesignDoc(ddocName string, viewName string, options map[string]interface{}) (*sgbucket.ViewResult, error) {
	// Query has slightly different access control than checkDDocAccess():
	// * Admins can query any design doc including the internal ones
	// * Regular users can query non-internal design docs
	// * Only admin can initiate an "into" query
	if db.user != nil && (isInternalDDoc(ddocName) || options["into"] != nil) {
		return nil, base.HTTPErrorf(http.StatusForbidden, "forbidden")
	}

	result, err := db.Bucket.View(ddocName, viewName, options)
	if err != nil {
		return nil, err
	}
	if isInternalDDoc(ddocName) {
		if options["include_docs"] == true {
			for _, row := range result.Rows {
				stripSyncProperty(row)
			}
		}
	} else {
		// don't filter when admin reduces
		if !(db.user == nil && options["reduce"] == true) {
			result = filterViewResult(result, db.user)
		}
	}
	// refactor inside SaveRowsIntoTarget
	if options["into"] != nil { // and user is admin
		level := 0
		if options["group"] == true {
			level = -1
		}
		if options["group_level"] != nil {
			level = options["group_level"].(int)
		}
		saveRowsIntoTarget(db, ddocName, viewName, level, result, options["into"].(string))
	}
	return &result, nil
}

func saveRowsIntoTarget(db *Database, ddocName string, viewName string, level int, result sgbucket.ViewResult, target string) {
	prefix := fmt.Sprintf("%s_%s_%d", ddocName, viewName, level)
	// targetDb =

	// ... load all docs with that prefix by docid, and remove them one by one as we work
	// through the set
	var allDocsOpts ForEachDocIDOptions
	allDocsOpts.Startkey = prefix
	allDocsOpts.Endkey = prefix + "*"
	docids := map[string]int{}
	db.ForEachDocID(func(doc IDAndRev, channels []string) bool {
		docids[doc.DocID] = 1
		return true
	}, allDocsOpts)

	for _, row := range result.Rows {
		jsonKey, _ := json.Marshal(row.Key)
		// error checking...
		docid := fmt.Sprintf("r.%s.%s", prefix, jsonKey)
		docids[docid] = 0
		body, err := db.GetRev(docid, "", false, nil)
		base.LogTo("HTTP", "View doc %q", docid)
		if err != nil {
			body = Body{}
		}
		// otherwise numbers are different types
		bodyV, _ := jsonRound(body["value"])
		viewV, _ := jsonRound(row.Value)
		if !reflect.DeepEqual(bodyV, viewV) {
			body["value"] = row.Value
			body["key"] = row.Key
			body["type"] = "reduction"
			body["query"] = prefix
			newRev, err2 := db.Put(docid, body)
			base.LogTo("HTTP", "Save into %q - %v - %v %v", docid, newRev, body["value"], err2)
		}
	}
	// removed rows weren't seen
	for id := range docids {
		if docids[id] == 1 {
			body, err := db.GetRev(id, "", false, nil)
			if err == nil {
				newRev, err2 := db.DeleteDoc(id, body["_rev"].(string))
				base.LogTo("HTTP", "Removed row %q %v %v", id, newRev, err2)
			} else {
				base.LogTo("HTTP", "Missing Removed row %q %v %v", id, err)
			}
		}
	}
}

func jsonRound(input interface{}) (result interface{}, err error) {
	toMarshal := make([]interface{}, 1)
	toMarshal[0] = input
	jsonValue, err := json.Marshal(toMarshal)
	if err == nil {
		var arrayResult []interface{}
		err = json.Unmarshal(jsonValue, &arrayResult)
		result = arrayResult[0]
	}
	return result, err
}

// Cleans up the Value property, and removes rows that aren't visible to the current user
func filterViewResult(input sgbucket.ViewResult, user auth.User) (result sgbucket.ViewResult) {
	checkChannels := false
	var visibleChannels ch.TimedSet
	if user != nil {
		visibleChannels = user.InheritedChannels()
		checkChannels = !visibleChannels.Contains("*")
	}
	result.TotalRows = input.TotalRows
	result.Rows = make([]*sgbucket.ViewRow, 0, len(input.Rows)/2)
	for _, row := range input.Rows {
		value := row.Value.([]interface{})
		// value[0] is the array of channels; value[1] is the actual value
		if !checkChannels || channelsIntersect(visibleChannels, value[0].([]interface{})) {
			// Add this row:
			stripSyncProperty(row)
			result.Rows = append(result.Rows, &sgbucket.ViewRow{
				Key:   row.Key,
				Value: value[1],
				ID:    row.ID,
				Doc:   row.Doc,
			})
		}
	}
	return
}

// Is any item of channels found in visibleChannels?
func channelsIntersect(visibleChannels ch.TimedSet, channels []interface{}) bool {
	for _, channel := range channels {
		if visibleChannels.Contains(channel.(string)) || channel == "*" {
			return true
		}
	}
	return false
}

func stripSyncProperty(row *sgbucket.ViewRow) {
	if doc := row.Doc; doc != nil {
		delete((*doc).(map[string]interface{}), "_sync")
	}
}
