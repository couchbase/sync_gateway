/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	ch "github.com/couchbase/sync_gateway/channels"
	pkgerrors "github.com/pkg/errors"
)

// ViewVersion should be incremented every time any view definition changes.
// Currently both Sync Gateway design docs share the same view version, but this is
// subject to change if the update schedule diverges
const DesignDocVersion = "2.1"
const DesignDocFormat = "%s_%s" // Design doc prefix, view version

// DesignDocPreviousVersions defines the set of versions included during removal of obsolete
// design docs.  Must be updated whenever DesignDocVersion is incremented.
// Uses a hardcoded list instead of version comparison to simpify the processing
// (particularly since there aren't expected to be many view versions before moving to GSI).
var DesignDocPreviousVersions = []string{"", "2.0"}

const (
	DesignDocSyncGatewayPrefix      = "sync_gateway"
	DesignDocSyncHousekeepingPrefix = "sync_housekeeping"
	ViewPrincipals                  = "principals"
	ViewChannels                    = "channels"
	ViewAccess                      = "access"
	ViewAccessVbSeq                 = "access_vbseq"
	ViewRoleAccess                  = "role_access"
	ViewRoleAccessVbSeq             = "role_access_vbseq"
	ViewAllDocs                     = "all_docs"
	ViewImport                      = "import"
	ViewSessions                    = "sessions"
	ViewTombstones                  = "tombstones"
)

// Principals view result row
type principalsViewRow struct {
	Key   string // principal name
	Value bool   // 'isUser' flag
}

func isInternalDDoc(ddocName string) bool {
	return strings.HasPrefix(ddocName, "sync_")
}

func DesignDocSyncGateway() string {
	return fmt.Sprintf(DesignDocFormat, DesignDocSyncGatewayPrefix, DesignDocVersion)
}

func DesignDocSyncHousekeeping() string {
	return fmt.Sprintf(DesignDocFormat, DesignDocSyncHousekeepingPrefix, DesignDocVersion)
}

// Enforces access by admins only, and not to the built-in Sync Gateway design docs:
func (db *Database) checkDDocAccess(ddocName string) error {
	if db.user != nil || isInternalDDoc(ddocName) {
		return base.HTTPErrorf(http.StatusForbidden, "forbidden")
	}
	return nil
}

func (db *Database) GetDesignDoc(ddocName string) (ddoc sgbucket.DesignDoc, err error) {
	if err = db.checkDDocAccess(ddocName); err != nil {
		return ddoc, err
	}
	return db.Bucket.GetDDoc(ddocName)
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
		err = db.Bucket.PutDDoc(ddocName, &ddoc)
	}
	return
}

const (
	// viewWrapper_adminViews adds the rev to metadata, and strips the _sync property from the view result
	syncViewAdminWrapper = `
	function(doc, meta) {
	
		//Skip any internal sync documents
		if (meta.id.substring(0, 6) == "%[1]s") {
			return;
		}
		var sync;
		var isXattr;
	
		//Get sync data from xattrs or from the doc body
		if (meta.xattrs === undefined || meta.xattrs.%[2]s === undefined) {
			sync = doc.%[3]s;
			isXattr = false;
		} else {
			sync = meta.xattrs.%[2]s;
			isXattr = true;
		}
	
		//Skip if the document has been deleted or has no sync data defined
		if (sync === undefined || (sync.flags & 1) || sync.deleted)
			return;
	
		//If sync data is in body strip it from the view result
		if (!isXattr) {
			delete doc.%[3]s;
		}
	
		//Add rev to meta
		meta.rev = sync.rev;
	
		//Run view
		(%[4]s)(doc, meta);
	
		//Re-add sync data to body
		if (!isXattr) {
			doc.%[3]s = sync;
		}
	}`
	syncViewUserWrapper = `
	function(doc, meta) {
		var sync;
		var isXattr;
	
		//Skip any internal sync documents
		if (meta.id.substring(0, 6) == "%[1]s") {
			return;
		}
	
		//Get sync data from xattrs or from the doc body
		if (meta.xattrs === undefined || meta.xattrs.%[2]s === undefined) {
			sync = doc.%[3]s;
			isXattr = false;
		} else {
			sync = meta.xattrs.%[2]s;
			isXattr = true;
		}
	
		//Skip if the document has been deleted or has no sync data defined
		if (sync === undefined || (sync.flags & 1) || sync.deleted)
			return;
	
		//If sync data is in body strip it from the view result
		if (!isXattr) {
			delete doc.%[3]s;
		}
	
		//Update channels
		var channels = [];
		var channelMap = sync.channels;
		if (channelMap) {
			for (var name in channelMap) {
				removed = channelMap[name];
				if (!removed)
					channels.push(name);
			}
		}
		meta.channels = channels;
	
		//Add rev to meta
		meta.rev = sync.rev;
	
		//Run view
		var _emit = emit;
		(function() {
			var emit = function(key, value) {
				_emit(key, [channels, value]);
			};
			(%[4]s)(doc, meta);
		}());
	
		//Re-add sync data to body
		if (!isXattr) {
			doc.%[3]s = sync;
		}
	}`
)

func wrapViews(ddoc *sgbucket.DesignDoc, enableUserViews bool, useXattrs bool) {
	// Wrap the map functions to ignore special docs and strip _sync metadata.  If user views are enabled, also
	// add channel filtering.
	for name, view := range ddoc.Views {
		if enableUserViews {
			view.Map = fmt.Sprintf(syncViewUserWrapper, base.SyncPrefix, base.SyncXattrName, base.SyncPropertyName, view.Map)
		} else {
			view.Map = fmt.Sprintf(syncViewAdminWrapper, base.SyncPrefix, base.SyncXattrName, base.SyncPropertyName, view.Map)
		}
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
				// handle the case where walrus returns []string instead of []interface{}
				vi, ok := value[0].([]interface{})
				if !ok {
					vi = base.ToArrayOfInterface(value[0].([]string))
				}

				if !hasStarChannel || channelsIntersect(visibleChannels, vi) {
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
		delete((*doc).(map[string]interface{}), base.SyncPropertyName)
	}
}

func InitializeViews(bucket base.Bucket) error {

	// Check whether design docs are already present
	ddocsExist := checkExistingDDocs(bucket)

	// If not present, install design docs and views
	if !ddocsExist {
		base.InfofCtx(context.TODO(), base.KeyAll, "Design docs for current view version (%s) do not exist - creating...", DesignDocVersion)
		if err := installViews(bucket); err != nil {
			return err
		}
	}

	// Wait for views to be indexed and available
	return WaitForViews(bucket)
}

func checkExistingDDocs(bucket base.Bucket) bool {

	// Check whether design docs already exist
	_, getDDocErr := bucket.GetDDoc(DesignDocSyncGateway())
	sgDDocExists := getDDocErr == nil

	_, getDDocErr = bucket.GetDDoc(DesignDocSyncHousekeeping())
	sgHousekeepingDDocExists := getDDocErr == nil

	if sgDDocExists && sgHousekeepingDDocExists {
		base.InfofCtx(context.TODO(), base.KeyAll, "Design docs for current SG view version (%s) found.", DesignDocVersion)
		return true
	}

	return false
}

func installViews(bucket base.Bucket) error {

	// syncData specifies the path to Sync Gateway sync metadata used in the map function -
	// in the document body when xattrs available, in the mobile xattr when xattrs enabled.
	syncData := fmt.Sprintf(`var sync
							if (meta.xattrs === undefined || meta.xattrs.%[1]s === undefined) {
		                        sync = doc.%[2]s
		                  	} else {
		                       	sync = meta.xattrs.%[1]s
		                    }
		                     `, base.SyncXattrName, base.SyncPropertyName)

	// View for _all_docs
	// Key is docid; value is [revid, sequence]
	alldocs_map := `function (doc, meta) {
                     %[1]s
                     if (sync === undefined || meta.id.substring(0,6) == "%[2]s")
                       return;
                     if ((sync.flags & 1) || sync.deleted)
                       return;
                     var channels = sync.channels;
                     var channelNames = [];
                     for (ch in channels) {
                     	if (channels[ch] == null)
                     		channelNames.push(ch);
                     }
                     emit(meta.id, {r:sync.rev, s:sync.sequence, c:channelNames}); }`
	alldocs_map = fmt.Sprintf(alldocs_map, syncData, base.SyncPrefix)

	// View for importing unknown docs
	// Key is [existing?, docid] where 'existing?' is false for unknown docs
	import_map := `function (doc, meta) {
					 %s
                     if(meta.id.substring(0,6) != "%s") {
                       var exists = (sync !== undefined);
                       emit([exists, meta.id], null); } }`
	import_map = fmt.Sprintf(import_map, syncData, base.SyncPrefix)

	// Sessions view - used for session delete
	// Key is username; value is docid
	sessions_map := `function (doc, meta) {
                     	var prefix = meta.id.substring(0,%d);
                     	if (prefix == %q)
                     		emit(doc.username, meta.id);}`
	sessions_map = fmt.Sprintf(sessions_map, len(base.SessionPrefix), base.SessionPrefix)

	// Tombstones view - used for view tombstone compaction
	// Key is purge time; value is docid
	tombstones_map := `function (doc, meta) {
                     	%s
                     	if (sync !== undefined && sync.tombstoned_at !== undefined)
                     		emit(sync.tombstoned_at, meta.id);}`
	tombstones_map = fmt.Sprintf(tombstones_map, syncData)

	// All-principals view
	// Key is name; value is true for user, false for role
	principals_map := `function (doc, meta) {
							 var prefix = meta.id.substring(0,11);
							 var isUser = (prefix == %q);
							 if (isUser || prefix == %q)
			                     emit(meta.id.substring(%d), isUser); }`
	principals_map = fmt.Sprintf(principals_map, base.UserPrefix, base.RolePrefix,
		len(base.UserPrefix))

	// By-channels view.
	// Key is [channelname, sequence]; value is [docid, revid, flag?]
	// where flag is true for doc deletion, false for removed from channel, missing otherwise
	channels_map := `function (doc, meta) {
	                    %s
	                    if (sync === undefined || meta.id.substring(0,6) == "%s")
	                        return;
						var sequence = sync.sequence;
	                    if (sequence === undefined)
	                        return;
	                    var value = {rev:sync.rev};
	                    if (sync.flags) {
	                    	value.flags = sync.flags
	                    } else if (sync.deleted) {
	                    	value.flags = %d // channels.Deleted
	                    }
	                    if (%v) // EnableStarChannelLog
							emit(["*", sequence], value);
						var channels = sync.channels;
						if (channels) {
							for (var name in channels) {
								removed = channels[name];
								if (!removed)
									emit([name, sequence], value);
								else {
									var flags = removed.del ? %d : %d; // channels.Removed/Deleted
									emit([name, removed.seq], {rev:removed.rev, flags: flags});
								}
							}
						}
					}`

	channels_map = fmt.Sprintf(channels_map, syncData, base.SyncPrefix, ch.Deleted, EnableStarChannelLog,
		ch.Removed|ch.Deleted, ch.Removed)

	// Channel access view, used by ComputeChannelsForPrincipal()
	// Key is username; value is dictionary channelName->firstSequence (compatible with TimedSet)
	access_map := `function (doc, meta) {
	                    %s
	                    if (sync === undefined || meta.id.substring(0,6) == "%s")
	                        return;
	                    var access = sync.access;
	                    if (access) {
	                        for (var name in access) {
	                            emit(name, access[name]);
	                        }
	                    }
	               }`
	access_map = fmt.Sprintf(access_map, syncData, base.SyncPrefix)

	// Vbucket sequence version of channel access view, used by ComputeChannelsForPrincipal()
	// Key is username; value is dictionary channelName->firstSequence (compatible with TimedSet)

	access_vbSeq_map := `function (doc, meta) {
		                    %s
		                    if (sync === undefined || meta.id.substring(0,6) == "%s")
		                        return;
		                    var access = sync.access;
		                    if (access) {
		                        for (var name in access) {
		                        	// Build a timed set based on vb and vbseq of this revision
		                        	var value = {};
		                        	for (var channel in access[name]) {
		                        		var timedSetWithVbucket = {};
				                        timedSetWithVbucket["vb"] = parseInt(meta.vb, 10);
				                        timedSetWithVbucket["seq"] = parseInt(meta.seq, 10);
				                        value[channel] = timedSetWithVbucket;
			                        }
		                            emit(name, value)
		                        }

		                    }
		               }`
	access_vbSeq_map = fmt.Sprintf(access_vbSeq_map, syncData, base.SyncPrefix)

	// Role access view, used by ComputeRolesForUser()
	// Key is username; value is array of role names
	roleAccess_map := `function (doc, meta) {
	                    %s
	                    if (sync === undefined || meta.id.substring(0,6) == "%s")
	                        return;
	                    var access = sync.role_access;
	                    if (access) {
	                        for (var name in access) {
	                            emit(name, access[name]);
	                        }
	                    }
	               }`
	roleAccess_map = fmt.Sprintf(roleAccess_map, syncData, base.SyncPrefix)

	// Vbucket sequence version of role access view, used by ComputeRolesForUser()
	// Key is username; value is dictionary channelName->firstSequence (compatible with TimedSet)

	roleAccess_vbSeq_map := `function (doc, meta) {
		                    %s
		                    if (sync === undefined || meta.id.substring(0,6) == "%s")
		                        return;
		                    var access = sync.role_access;
		                    if (access) {
		                        for (var name in access) {
		                        	// Build a timed set based on vb and vbseq of this revision
		                        	var value = {};
		                        	for (var role in access[name]) {
		                        		var timedSetWithVbucket = {};
				                        timedSetWithVbucket["vb"] = parseInt(meta.vb, 10);
				                        timedSetWithVbucket["seq"] = parseInt(meta.seq, 10);
				                        value[role] = timedSetWithVbucket;
			                        }
		                            emit(name, value)
		                        }

		                    }
		               }`
	roleAccess_vbSeq_map = fmt.Sprintf(roleAccess_vbSeq_map, syncData, base.SyncPrefix)

	designDocMap := map[string]*sgbucket.DesignDoc{}
	designDocMap[DesignDocSyncGateway()] = &sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			ViewChannels:        sgbucket.ViewDef{Map: channels_map},
			ViewAccess:          sgbucket.ViewDef{Map: access_map},
			ViewRoleAccess:      sgbucket.ViewDef{Map: roleAccess_map},
			ViewAccessVbSeq:     sgbucket.ViewDef{Map: access_vbSeq_map},
			ViewRoleAccessVbSeq: sgbucket.ViewDef{Map: roleAccess_vbSeq_map},
			ViewPrincipals:      sgbucket.ViewDef{Map: principals_map},
		},
		Options: &sgbucket.DesignDocOptions{
			IndexXattrOnTombstones: true,
		},
	}

	designDocMap[DesignDocSyncHousekeeping()] = &sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			ViewAllDocs:    sgbucket.ViewDef{Map: alldocs_map, Reduce: "_count"},
			ViewImport:     sgbucket.ViewDef{Map: import_map, Reduce: "_count"},
			ViewSessions:   sgbucket.ViewDef{Map: sessions_map},
			ViewTombstones: sgbucket.ViewDef{Map: tombstones_map},
		},
		Options: &sgbucket.DesignDocOptions{
			IndexXattrOnTombstones: true, // For ViewTombstones
		},
	}

	sleeper := base.CreateDoublingSleeperFunc(
		11, //MaxNumRetries approx 10 seconds total retry duration
		5,  //InitialRetrySleepTimeMS
	)

	// add all design docs from map into bucket
	for designDocName, designDoc := range designDocMap {

		//start a retry loop to put design document backing off double the delay each time
		worker := func() (shouldRetry bool, err error, value interface{}) {
			err = bucket.PutDDoc(designDocName, designDoc)
			if err != nil {
				base.WarnfCtx(context.TODO(), "Error installing Couchbase design doc: %v", err)
			}
			return err != nil, err, nil
		}

		description := fmt.Sprintf("Attempt to install Couchbase design doc")
		err, _ := base.RetryLoop(description, worker, sleeper)

		if err != nil {
			return pkgerrors.WithStack(base.RedactErrorf("Error installing Couchbase Design doc: %v.  Error: %v", base.UD(designDocName), err))
		}
	}

	base.InfofCtx(context.TODO(), base.KeyAll, "Design docs successfully created for view version %s.", DesignDocVersion)

	return nil
}

// Issue a stale=false queries against critical views to guarantee indexing is complete and views are ready
func WaitForViews(bucket base.Bucket) error {
	var viewsWg sync.WaitGroup
	views := []string{ViewChannels, ViewAccess, ViewRoleAccess}
	viewErrors := make(chan error, len(views))

	base.InfofCtx(context.TODO(), base.KeyAll, "Verifying view availability for bucket %s...", base.UD(bucket.GetName()))

	for _, viewName := range views {
		viewsWg.Add(1)
		go func(view string) {
			defer viewsWg.Done()
			viewErr := waitForViewIndexing(bucket, DesignDocSyncGateway(), view)
			if viewErr != nil {
				viewErrors <- viewErr
			}
		}(viewName)
	}

	viewsWg.Wait()
	if len(viewErrors) > 0 {
		err := <-viewErrors
		close(viewErrors)
		return err
	}

	base.InfofCtx(context.TODO(), base.KeyAll, "Views ready for bucket %s.", base.UD(bucket.GetName()))
	return nil

}

// Issues stale=false view queries to determine when view indexing is complete.  Retries on timeout
func waitForViewIndexing(bucket base.Bucket, ddocName string, viewName string) error {
	opts := map[string]interface{}{"stale": false, "key": fmt.Sprintf("view_%s_ready_check", viewName), "limit": 1}

	// Not using standard retry loop here, because we want to retry indefinitely on view timeout (since view indexing could potentially take hours), and
	// we don't need to sleep between attempts.  Using manual exponential backoff retry processing for non-timeout related errors, waits up to ~5 min
	errRetryCount := 0
	retrySleep := float64(100)
	maxRetry := 18
	for {
		results, err := bucket.ViewQuery(ddocName, viewName, opts)
		if results != nil {
			_ = results.Close()
		}
		if err == nil {
			return nil
		}

		// Retry on timeout or undefined view errors , otherwise return the error
		if err == base.ErrViewTimeoutError {
			base.InfofCtx(context.TODO(), base.KeyAll, "Timeout waiting for view %q to be ready for bucket %q - retrying...", viewName, base.UD(bucket.GetName()))
		} else {
			// For any other error, retry up to maxRetry, to wait for view initialization on the server
			errRetryCount++
			if errRetryCount > maxRetry {
				return err
			}
			base.WarnfCtx(context.TODO(), "Error waiting for view %q to be ready for bucket %q - retrying...(%d/%d)", viewName, bucket.GetName(), errRetryCount, maxRetry)
			time.Sleep(time.Duration(retrySleep) * time.Millisecond)
			retrySleep *= float64(1.5)
		}
	}

}

func removeObsoleteDesignDocs(bucket base.Bucket, previewOnly bool, useViews bool) (removedDesignDocs []string, err error) {

	removedDesignDocs = make([]string, 0)
	designDocPrefixes := []string{DesignDocSyncGatewayPrefix, DesignDocSyncHousekeepingPrefix}

	versionsToRemove := DesignDocPreviousVersions

	if !useViews {
		versionsToRemove = append(versionsToRemove, DesignDocVersion)
	}

	for _, previousVersion := range versionsToRemove {
		for _, ddocPrefix := range designDocPrefixes {
			var ddocName string
			if previousVersion == "" {
				ddocName = ddocPrefix
			} else {
				ddocName = fmt.Sprintf(DesignDocFormat, ddocPrefix, previousVersion)
			}

			if !previewOnly {
				removeDDocErr := bucket.DeleteDDoc(ddocName)
				if removeDDocErr != nil && !IsMissingDDocError(removeDDocErr) {
					base.WarnfCtx(context.TODO(), "Unexpected error when removing design doc %q: %s", ddocName, removeDDocErr)
				}
				// Only include in list of removedDesignDocs if it was actually removed
				if removeDDocErr == nil {
					removedDesignDocs = append(removedDesignDocs, ddocName)
				}
			} else {
				_, existsDDocErr := bucket.GetDDoc(ddocName)
				if existsDDocErr != nil && !IsMissingDDocError(existsDDocErr) {
					base.WarnfCtx(context.TODO(), "Unexpected error when checking existence of design doc %q: %s", ddocName, existsDDocErr)
				}
				// Only include in list of removedDesignDocs if it exists
				if existsDDocErr == nil {
					removedDesignDocs = append(removedDesignDocs, ddocName)
				}

			}
		}

	}
	return removedDesignDocs, nil
}

// Similar to IsKeyNotFoundError(), but for the specific error returned by GetDDoc/DeleteDDoc
func IsMissingDDocError(err error) bool {
	if err == nil {
		return false
	}
	unwrappedErr := pkgerrors.Cause(err)

	// Walrus
	if _, ok := unwrappedErr.(sgbucket.MissingError); ok {
		return true
	}

	// gocb
	if strings.Contains(unwrappedErr.Error(), "not_found") {
		return true
	}

	// gocb v2
	if errors.Is(err, gocb.ErrDesignDocumentNotFound) {
		return true
	}

	return false

}
