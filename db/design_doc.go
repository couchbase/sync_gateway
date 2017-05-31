package db

import (
	"fmt"
	"net/http"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
)

const (
	DesignDocSyncGateway      = "sync_gateway"
	DesignDocSyncHousekeeping = "sync_housekeeping"
	ViewPrincipals            = "principals"
	ViewChannels              = "channels"
	ViewAccess                = "access"
	ViewAccessVbSeq           = "access_vbseq"
	ViewRoleAccess            = "role_access"
	ViewRoleAccessVbSeq       = "role_access_vbseq"
	ViewAllBits               = "all_bits"
	ViewAllDocs               = "all_docs"
	ViewImport                = "import"
	ViewOldRevs               = "old_revs"
	ViewSessions              = "sessions"
	ViewTombstones            = "tombstones"
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

func (db *Database) PutDesignDoc(ddocName string, ddoc sgbucket.DesignDoc) (err error) {
	wrap := true
	if opts := ddoc.Options; opts != nil {
		if opts.Raw == true {
			wrap = false
		}
	}
	if wrap {
		wrapViews(&ddoc, db.GetUserViewsEnabled(), db.UseXattrs())
	}
	if err = db.checkDDocAccess(ddocName); err == nil {
		err = db.Bucket.PutDDoc(ddocName, ddoc)
	}
	return
}

const (
	// viewWrapper_adminViews adds the rev to metadata, and strips the _sync property from the view result
	viewWrapper_adminViews = `function(doc,meta) {
	                    var sync = doc._sync;
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                      return;
	                    if ((sync.flags & 1) || sync.deleted)
	                      return;
	                    delete doc._sync;
	                    meta.rev = sync.rev;
						(%s) (doc, meta);
						doc._sync = sync;}`
	// viewWrapper_userViews does the same work as viewWrapper_adminViews, and also includes channel information in the view results
	viewWrapper_userViews = `function(doc,meta) {
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
								(%s) (doc, meta);
							}());
							doc._sync = sync;
						}`
	viewWrapper_adminViews_xattr = `function(doc,meta) {
	                    var sync = meta.xattrs._sync;
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                      return;
	                    if ((sync.flags & 1) || sync.deleted)
	                      return;
	                    meta.rev = sync.rev;
						(%s) (doc, meta);}`
	viewWrapper_userViews_xattr = `function(doc,meta) {
		                    var sync = meta.xattrs._sync;
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
		                    meta.rev = sync.rev;
		                    meta.channels = channels;

		                    var _emit = emit;
		                    (function(){
			                    var emit = function(key,value) {
			                    	_emit(key,[channels, value]);
			                    };
								(%s) (doc, meta);
							}());
						}`
)

func getViewWrapper(enableUserViews bool, useXattrs bool) string {
	if enableUserViews {
		if useXattrs {
			return viewWrapper_userViews_xattr
		} else {
			return viewWrapper_userViews
		}
	} else {
		if useXattrs {
			return viewWrapper_adminViews_xattr
		} else {
			return viewWrapper_adminViews
		}
	}
}

func wrapViews(ddoc *sgbucket.DesignDoc, enableUserViews bool, useXattrs bool) {
	// Wrap the map functions to ignore special docs and strip _sync metadata.  If user views are enabled, also
	// add channel filtering.
	viewWrapper := getViewWrapper(enableUserViews, useXattrs)
	for name, view := range ddoc.Views {
		view.Map = fmt.Sprintf(viewWrapper, view.Map)
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

	// Regular users have limitations on what they can query
	if db.user != nil {
		// * Regular users can only query when user views are enabled
		if !db.GetUserViewsEnabled() {
			return nil, base.HTTPErrorf(http.StatusForbidden, "forbidden")
		}
		// * Admins can query any design doc including the internal ones
		// * Regular users can query non-internal design docs
		if isInternalDDoc(ddocName) {
			return nil, base.HTTPErrorf(http.StatusForbidden, "forbidden")
		}
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
		applyChannelFiltering := options["reduce"] != true && db.GetUserViewsEnabled()
		result = filterViewResult(result, db.user, applyChannelFiltering)
	}
	return &result, nil
}

// Cleans up the Value property, and removes rows that aren't visible to the current user
func filterViewResult(input sgbucket.ViewResult, user auth.User, applyChannelFiltering bool) (result sgbucket.ViewResult) {
	hasStarChannel := false
	var visibleChannels ch.TimedSet
	if user != nil {
		visibleChannels = user.InheritedChannels()
		hasStarChannel = !visibleChannels.Contains("*")
		if !applyChannelFiltering {
			return // this is an error
		}
	}
	result.TotalRows = input.TotalRows
	result.Rows = make([]*sgbucket.ViewRow, 0, len(input.Rows)/2)
	for _, row := range input.Rows {
		if applyChannelFiltering {
			value, ok := row.Value.([]interface{})
			if ok {
				// value[0] is the array of channels; value[1] is the actual value
				if !hasStarChannel || channelsIntersect(visibleChannels, value[0].([]interface{})) {
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
		} else {
			// Add the raw row:
			stripSyncProperty(row)
			result.Rows = append(result.Rows, &sgbucket.ViewRow{
				Key:   row.Key,
				Value: row.Value,
				ID:    row.ID,
				Doc:   row.Doc,
			})
		}

	}
	result.TotalRows = len(result.Rows)
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
