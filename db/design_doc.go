package db

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

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
var DesignDocPreviousVersions = []string{""}

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
		delete((*doc).(map[string]interface{}), "_sync")
	}
}

func InitializeViews(bucket base.Bucket) error {

	// Check whether design docs are already present
	ddocsExist := checkExistingDDocs(bucket)

	// If not present, install design docs and views
	if !ddocsExist {
		base.Infof(base.KeyAll, "Design docs for current view version (%s) do not exist - creating...", DesignDocVersion)
		if err := installViews(bucket); err != nil {
			return err
		}
	}

	// Wait for views to be indexed and available
	return WaitForViews(bucket)
}

func checkExistingDDocs(bucket base.Bucket) bool {

	// Check whether design docs already exist
	var result interface{}
	getDDocErr := bucket.GetDDoc(DesignDocSyncGateway(), &result)
	sgDDocExists := getDDocErr == nil && result != nil

	getDDocErr = bucket.GetDDoc(DesignDocSyncHousekeeping(), &result)
	sgHousekeepingDDocExists := getDDocErr == nil && result != nil

	if sgDDocExists && sgHousekeepingDDocExists {
		base.Infof(base.KeyAll, "Design docs for current SG view version (%s) found.", DesignDocVersion)
		return true
	}

	return false
}

func installViews(bucket base.Bucket) error {

	// syncData specifies the path to Sync Gateway sync metadata used in the map function -
	// in the document body when xattrs available, in the mobile xattr when xattrs enabled.
	syncData := fmt.Sprintf(`var sync
							if (meta.xattrs === undefined || meta.xattrs.%s === undefined) {
		                        sync = doc._sync
		                  	} else {
		                       	sync = meta.xattrs.%s
		                    }
		                     `, KSyncXattrName, KSyncXattrName)

	// View for _all_docs
	// Key is docid; value is [revid, sequence]
	alldocs_map := `function (doc, meta) {
                     %s
                     if (sync === undefined || meta.id.substring(0,6) == "_sync:")
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
	alldocs_map = fmt.Sprintf(alldocs_map, syncData)

	// View for importing unknown docs
	// Key is [existing?, docid] where 'existing?' is false for unknown docs
	import_map := `function (doc, meta) {
					 %s
                     if(meta.id.substring(0,6) != "_sync:") {
                       var exists = (sync !== undefined);
                       emit([exists, meta.id], null); } }`
	import_map = fmt.Sprintf(import_map, syncData)

	// Sessions view - used for session delete
	// Key is username; value is docid
	sessions_map := `function (doc, meta) {
                     	var prefix = meta.id.substring(0,%d);
                     	if (prefix == %q)
                     		emit(doc.username, meta.id);}`
	sessions_map = fmt.Sprintf(sessions_map, len(auth.SessionKeyPrefix), auth.SessionKeyPrefix)

	// Tombstones view - used for view tombstone compaction
	// Key is purge time; value is docid
	tombstones_map := `function (doc, meta) {
                     	var sync = meta.xattrs._sync;
                     	if (sync !== undefined && sync.tombstoned_at !== undefined)
                     		emit(sync.tombstoned_at, meta.id);}`

	// All-principals view
	// Key is name; value is true for user, false for role
	principals_map := `function (doc, meta) {
							 var prefix = meta.id.substring(0,11);
							 var isUser = (prefix == %q);
							 if (isUser || prefix == %q)
			                     emit(meta.id.substring(%d), isUser); }`
	principals_map = fmt.Sprintf(principals_map, auth.UserKeyPrefix, auth.RoleKeyPrefix,
		len(auth.UserKeyPrefix))

	// By-channels view.
	// Key is [channelname, sequence]; value is [docid, revid, flag?]
	// where flag is true for doc deletion, false for removed from channel, missing otherwise
	channels_map := `function (doc, meta) {
	                    %s
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
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

	channels_map = fmt.Sprintf(channels_map, syncData, ch.Deleted, EnableStarChannelLog,
		ch.Removed|ch.Deleted, ch.Removed)

	// Channel access view, used by ComputeChannelsForPrincipal()
	// Key is username; value is dictionary channelName->firstSequence (compatible with TimedSet)
	access_map := `function (doc, meta) {
	                    %s
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                        return;
	                    var access = sync.access;
	                    if (access) {
	                        for (var name in access) {
	                            emit(name, access[name]);
	                        }
	                    }
	               }`
	access_map = fmt.Sprintf(access_map, syncData)

	// Vbucket sequence version of channel access view, used by ComputeChannelsForPrincipal()
	// Key is username; value is dictionary channelName->firstSequence (compatible with TimedSet)

	access_vbSeq_map := `function (doc, meta) {
		                    %s
		                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
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
	access_vbSeq_map = fmt.Sprintf(access_vbSeq_map, syncData)

	// Role access view, used by ComputeRolesForUser()
	// Key is username; value is array of role names
	roleAccess_map := `function (doc, meta) {
	                    %s
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                        return;
	                    var access = sync.role_access;
	                    if (access) {
	                        for (var name in access) {
	                            emit(name, access[name]);
	                        }
	                    }
	               }`
	roleAccess_map = fmt.Sprintf(roleAccess_map, syncData)

	// Vbucket sequence version of role access view, used by ComputeRolesForUser()
	// Key is username; value is dictionary channelName->firstSequence (compatible with TimedSet)

	roleAccess_vbSeq_map := `function (doc, meta) {
		                    %s
		                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
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
	roleAccess_vbSeq_map = fmt.Sprintf(roleAccess_vbSeq_map, syncData)

	designDocMap := map[string]sgbucket.DesignDoc{}
	designDocMap[DesignDocSyncGateway()] = sgbucket.DesignDoc{
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

	designDocMap[DesignDocSyncHousekeeping()] = sgbucket.DesignDoc{
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
				base.Warnf(base.KeyAll, "Error installing Couchbase design doc: %v", err)
			}
			return err != nil, err, nil
		}

		description := fmt.Sprintf("Attempt to install Couchbase design doc")
		err, _ := base.RetryLoop(description, worker, sleeper)

		if err != nil {
			return pkgerrors.WithStack(base.RedactErrorf("Error installing Couchbase Design doc: %v.  Error: %v", base.UD(designDocName), err))
		}
	}

	base.Infof(base.KeyAll, "Design docs successfully created for view version %s.", DesignDocVersion)

	return nil
}

// Issue a stale=false queries against critical views to guarantee indexing is complete and views are ready
func WaitForViews(bucket base.Bucket) error {
	var viewsWg sync.WaitGroup
	views := []string{ViewChannels, ViewAccess, ViewRoleAccess}
	viewErrors := make(chan error, len(views))

	base.Infof(base.KeyAll, "Verifying view availability for bucket %s...", base.UD(bucket.GetName()))

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

	base.Infof(base.KeyAll, "Views ready for bucket %s.", base.UD(bucket.GetName()))
	return nil

}

// Issues stale=false view queries to determine when view indexing is complete.  Retries on timeout
func waitForViewIndexing(bucket base.Bucket, ddocName string, viewName string) error {
	var vres interface{}
	opts := map[string]interface{}{"stale": false, "key": fmt.Sprintf("view_%s_ready_check", viewName), "limit": 1}

	// Not using standard retry loop here, because we want to retry indefinitely on view timeout (since view indexing could potentially take hours), and
	// we don't need to sleep between attempts.  Using manual exponential backoff retry processing for non-timeout related errors, waits up to ~5 min
	errRetryCount := 0
	retrySleep := float64(100)
	maxRetry := 18
	for {
		err := bucket.ViewCustom(ddocName, viewName, opts, &vres)
		if err == nil {
			return nil
		}

		// Retry on timeout or undefined view errors , otherwise return the error
		if err == base.ErrViewTimeoutError {
			base.Infof(base.KeyAll, "Timeout waiting for view %q to be ready for bucket %q - retrying...", viewName, base.UD(bucket.GetName()))
		} else {
			// For any other error, retry up to maxRetry, to wait for view initialization on the server
			errRetryCount++
			if errRetryCount > maxRetry {
				return err
			}
			base.Warnf(base.KeyAll, "Error waiting for view %q to be ready for bucket %q - retrying...(%d/%d)", viewName, bucket.GetName(), errRetryCount, maxRetry)
			time.Sleep(time.Duration(retrySleep) * time.Millisecond)
			retrySleep *= float64(1.5)
		}
	}

}

func removeObsoleteDesignDocs(bucket base.Bucket, previewOnly bool) (removedDesignDocs []string, err error) {

	removedDesignDocs = make([]string, 0)
	designDocPrefixes := []string{DesignDocSyncGatewayPrefix, DesignDocSyncHousekeepingPrefix}

	for _, previousVersion := range DesignDocPreviousVersions {
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
					base.Warnf(base.KeyAll, "Unexpected error when removing design doc %q: %s", ddocName, removeDDocErr)
					return removedDesignDocs, removeDDocErr
				}
				// Only include in list of removedDesignDocs if it was actually removed
				if removeDDocErr == nil {
					removedDesignDocs = append(removedDesignDocs, ddocName)
				}
			} else {
				var result interface{}
				existsDDocErr := bucket.GetDDoc(ddocName, &result)
				if existsDDocErr != nil && !IsMissingDDocError(existsDDocErr) {
					base.Warnf(base.KeyAll, "Unexpected error when checking existence of design doc %q: %s", ddocName, existsDDocErr)
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

	return false

}
