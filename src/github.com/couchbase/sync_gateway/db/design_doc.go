package db

import (
	"net/http"
	"strings"
	"fmt"
	// "encoding/json"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	// "github.com/couchbase/gocb"
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
	if db.user != nil && isInternalDDoc(ddocName) {
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
		result = filterViewResult(result, db.user, options["reduce"] == true)
	}
	return &result, nil
}

// Result of a n1ql query.
type N1QLResult struct {
	Rows []interface{} `json:"results"`
}

func (db *Database) N1QLQuery(queryName string, params map[string]interface{}, args []interface{}) (*N1QLResult, error) {
	vres := N1QLResult{}

	if params == nil {
		params = make(map[string]interface{})
	}

	if queryString := db.N1QLQueries[queryName]; queryString != "" {
		// queries that don't include CHANNEL_FILTER() are restricted to users with * access
		queryDoesFilter := strings.Contains(queryString, "CHANNEL_FILTER()")
		query := db.N1QLStatements[queryName]

		checkChannels := false
		var visibleChannels ch.TimedSet
		var userChannels []string
		if db.user != nil {
			visibleChannels = db.user.InheritedChannels()
			userChannels = visibleChannels.AllChannels()
			checkChannels = !visibleChannels.Contains("*")
		} else {
			userChannels = []string{"*"}
		}
		if checkChannels && !queryDoesFilter {
			return nil, base.HTTPErrorf(http.StatusForbidden, "Only users with acccess to `*` channel can request queries without `CHANNEL_FILTER()` in the WHERE clause.")
		}

		var queryParams interface{};
		if len(args) > 0 {
			queryParams = args
		} else {
			if queryDoesFilter {
				params["_userChannels"] = userChannels
			}
			queryParams = params
		}
		fmt.Printf("query: %+v\n", queryString)
		fmt.Printf("queryParams: %+v\n", queryParams)

		rows, err := db.N1QLConnection.ExecuteN1qlQuery(query, queryParams)
		if err != nil {
			return nil, err
		}
		for {
			var row map[string]interface{}
			hasRow := rows.Next(&row)
			if !hasRow {
				break
			}
			fmt.Printf("Row: %+v\n", row)
			vres.Rows = append(vres.Rows, row)
		}
		rows.Close()
	}
	return &vres, nil
}

// Cleans up the Value property, and removes rows that aren't visible to the current user
func filterViewResult(input sgbucket.ViewResult, user auth.User, reduce bool) (result sgbucket.ViewResult) {
	checkChannels := false
	var visibleChannels ch.TimedSet
	if user != nil {
		visibleChannels = user.InheritedChannels()
		checkChannels = !visibleChannels.Contains("*")
		if reduce {
			return // this is an error
		}
	}
	result.TotalRows = input.TotalRows
	result.Rows = make([]*sgbucket.ViewRow, 0, len(input.Rows)/2)
	for _, row := range input.Rows {
		if reduce {
			// Add the raw row:
			result.Rows = append(result.Rows, &sgbucket.ViewRow{
				Key:   row.Key,
				Value: row.Value,
				ID:    row.ID,
			})
		} else {
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
