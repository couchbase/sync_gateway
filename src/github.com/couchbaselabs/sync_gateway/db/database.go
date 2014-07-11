//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"encoding/json"
	"expvar"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/walrus"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

// Basic description of a database. Shared between all Database objects on the same database.
// This object is thread-safe so it can be shared between HTTP handlers.
type DatabaseContext struct {
	Name               string                  // Database name
	Bucket             base.Bucket             // Storage
	tapListener        changeListener          // Listens on server Tap feed
	sequences          *sequenceAllocator      // Source of new sequence numbers
	ChannelMapper      *channels.ChannelMapper // Runs JS 'sync' function
	StartTime          time.Time               // Timestamp when context was instantiated
	ChangesClientStats Statistics              // Tracks stats of # of changes connections
	RevsLimit          uint32                  // Max depth a document's revision tree can grow to
	autoImport         bool                    // Add sync data to new untracked docs?
	Shadower           *Shadower               // Tracks an external Couchbase bucket
	revisionCache      *RevisionCache          // Cache of recently-accessed doc revisions
	changeCache        changeCache
}

const DefaultRevsLimit = 1000

// Number of recently-accessed doc revisions to cache in RAM
const RevisionCacheCapacity = 5000

// Represents a simulated CouchDB database. A new instance is created for each HTTP request,
// so this struct does not have to be thread-safe.
type Database struct {
	*DatabaseContext
	user auth.User
}

// All special/internal documents the gateway creates have this prefix in their keys.
const kSyncKeyPrefix = "_sync:"

var dbExpvars = expvar.NewMap("syncGateway_db")

func ValidateDatabaseName(dbName string) error {
	// http://wiki.apache.org/couchdb/HTTP_database_API#Naming_and_Addressing
	if match, _ := regexp.MatchString(`^[a-z][-a-z0-9_$()+/]*$`, dbName); !match {
		return base.HTTPErrorf(http.StatusBadRequest,
			"Illegal database name: %s", dbName)
	}
	return nil
}

// Helper function to open a Couchbase connection and return a specific bucket.
func ConnectToBucket(spec base.BucketSpec) (bucket base.Bucket, err error) {
	bucket, err = base.GetBucket(spec)
	if err != nil {
		err = base.HTTPErrorf(http.StatusBadGateway,
			"Unable to connect to server: %s", err)
	} else {
		err = installViews(bucket)
	}
	return
}

// Creates a new DatabaseContext on a bucket. The bucket will be closed when this context closes.
func NewDatabaseContext(dbName string, bucket base.Bucket, autoImport bool) (*DatabaseContext, error) {
	if err := ValidateDatabaseName(dbName); err != nil {
		return nil, err
	}
	context := &DatabaseContext{
		Name:       dbName,
		Bucket:     bucket,
		StartTime:  time.Now(),
		RevsLimit:  DefaultRevsLimit,
		autoImport: autoImport,
	}
	context.revisionCache = NewRevisionCache(RevisionCacheCapacity, context.revCacheLoader)
	var err error
	context.sequences, err = newSequenceAllocator(bucket)
	if err != nil {
		return nil, err
	}
	lastSeq, err := context.sequences.lastSequence()
	if err != nil {
		return nil, err
	}
	context.changeCache.Init(context, lastSeq, func(changedChannels base.Set) {
		context.tapListener.Notify(changedChannels)
	})
	context.tapListener.OnDocChanged = context.changeCache.DocChanged

	if err = context.tapListener.Start(bucket, true); err != nil {
		return nil, err
	}
	go context.watchDocChanges()
	return context, nil
}

func (context *DatabaseContext) Close() {
	context.tapListener.Stop()
	context.changeCache.Stop()
	context.Shadower.Stop()
	context.Bucket.Close()
	context.Bucket = nil
}

func (context *DatabaseContext) Authenticator() *auth.Authenticator {
	// Authenticators are lightweight & stateless, so it's OK to return a new one every time
	return auth.NewAuthenticator(context.Bucket, context)
}

// Makes a Database object given its name and bucket.
func GetDatabase(context *DatabaseContext, user auth.User) (*Database, error) {
	return &Database{context, user}, nil
}

func CreateDatabase(context *DatabaseContext) (*Database, error) {
	return &Database{context, nil}, nil
}

func (db *Database) SameAs(otherdb *Database) bool {
	return db != nil && otherdb != nil &&
		db.Bucket == otherdb.Bucket
}

// Reloads the database's User object, in case its persistent properties have been changed.
func (db *Database) ReloadUser() error {
	if db.user == nil {
		return nil
	}
	user, err := db.Authenticator().GetUser(db.user.Name())
	if err == nil {
		db.user = user
	}
	return err
}

//////// ALL DOCUMENTS:

// The number of documents in the database.
func (db *Database) DocCount() int {
	vres, err := db.queryAllDocs(true)
	if err != nil {
		return -1
	}
	if len(vres.Rows) == 0 {
		return 0
	}
	return int(vres.Rows[0].Value.(float64))
}

func installViews(bucket base.Bucket) error {
	// View for finding every Couchbase doc (used when deleting a database)
	// Key is docid; value is null
	allbits_map := `function (doc, meta) {
                      emit(meta.id, null); }`
	// View for _all_docs
	// Key is docid; value is [revid, sequence]
	alldocs_map := `function (doc, meta) {
                     var sync = doc._sync;
                     if (sync === undefined || meta.id.substring(0,6) == "_sync:")
                       return;
                     if ((sync.flags & 1) || sync.deleted)
                       return;
                     emit(meta.id, [sync.rev, sync.sequence]); }`
	// View for importing unknown docs
	// Key is [existing?, docid] where 'existing?' is false for unknown docs
	import_map := `function (doc, meta) {
                     if(meta.id.substring(0,6) != "_sync:") {
                       var exists = (doc["_sync"] !== undefined);
                       emit([exists, meta.id], null); } }`
	// View for compaction -- finds all revision docs
	// Key and value are ignored.
	oldrevs_map := `function (doc, meta) {
                     var sync = doc._sync;
                     if (meta.id.substring(0,10) == "_sync:rev:")
	                     emit("",null); }`
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
	                    var sync = doc._sync;
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
	channels_map = fmt.Sprintf(channels_map, channels.Deleted, EnableStarChannelLog,
		channels.Removed|channels.Deleted, channels.Removed)
	// Channel access view, used by ComputeChannelsForPrincipal()
	// Key is username; value is dictionary channelName->firstSequence (compatible with TimedSet)
	access_map := `function (doc, meta) {
	                    var sync = doc._sync;
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                        return;
	                    var access = sync.access;
	                    if (access) {
	                        for (var name in access) {
	                            emit(name, access[name]);
	                        }
	                    }
	               }`
	// Role access view, used by ComputeRolesForUser()
	// Key is username; value is array of role names
	roleAccess_map := `function (doc, meta) {
	                    var sync = doc._sync;
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                        return;
	                    var access = sync.role_access;
	                    if (access) {
	                        for (var name in access) {
	                            emit(name, access[name]);
	                        }
	                    }
	               }`

	ddoc := walrus.DesignDoc{
		Views: walrus.ViewMap{
			"principals":  walrus.ViewDef{Map: principals_map},
			"channels":    walrus.ViewDef{Map: channels_map},
			"access":      walrus.ViewDef{Map: access_map},
			"role_access": walrus.ViewDef{Map: roleAccess_map},
		},
	}
	err := bucket.PutDDoc("sync_gateway", ddoc)
	if err != nil {
		base.Warn("Error installing Couchbase design doc: %v", err)
	}

	ddoc = walrus.DesignDoc{
		Views: walrus.ViewMap{
			"all_bits": walrus.ViewDef{Map: allbits_map},
			"all_docs": walrus.ViewDef{Map: alldocs_map, Reduce: "_count"},
			"import":   walrus.ViewDef{Map: import_map, Reduce: "_count"},
			"old_revs": walrus.ViewDef{Map: oldrevs_map, Reduce: "_count"},
		},
	}
	err = bucket.PutDDoc("sync_housekeeping", ddoc)
	if err != nil {
		base.Warn("Error installing Couchbase design doc: %v", err)
	}

	return err
}

type IDAndRev struct {
	DocID    string
	RevID    string
	Sequence uint64
}

// Returns all document IDs as an array.
func (db *Database) AllDocIDs() ([]IDAndRev, error) {
	vres, err := db.queryAllDocs(false)
	if err != nil {
		return nil, err
	}

	rows := vres.Rows
	result := make([]IDAndRev, 0, len(rows))
	for _, row := range rows {
		value := row.Value.([]interface{})
		result = append(result, IDAndRev{
			DocID:    row.Key.(string),
			RevID:    value[0].(string),
			Sequence: uint64(value[1].(float64)),
		})
	}
	return result, nil
}

// Returns the IDs of all users and roles
func (db *DatabaseContext) AllPrincipalIDs() (users, roles []string, err error) {
	vres, err := db.Bucket.View("sync_gateway", "principals", Body{"stale": false})
	if err != nil {
		return
	}
	users = []string{}
	roles = []string{}
	for _, row := range vres.Rows {
		name := row.Key.(string)
		if name != "" {
			if row.Value.(bool) {
				users = append(users, name)
			} else {
				roles = append(roles, name)
			}
		}
	}
	return
}

func (db *Database) queryAllDocs(reduce bool) (walrus.ViewResult, error) {
	opts := Body{"stale": false, "reduce": reduce}
	vres, err := db.Bucket.View("sync_housekeeping", "all_docs", opts)
	if err != nil {
		base.Warn("all_docs got error: %v", err)
	}
	return vres, err
}

//////// HOUSEKEEPING:

// Deletes all documents in the database
func (db *Database) DeleteAllDocs(docType string) error {
	opts := Body{"stale": false}
	if docType != "" {
		opts["startkey"] = "_sync:" + docType + ":"
		opts["endkey"] = "_sync:" + docType + "~"
		opts["inclusive_end"] = false
	}
	vres, err := db.Bucket.View("sync_housekeeping", "all_bits", opts)
	if err != nil {
		base.Warn("all_bits view returned %v", err)
		return err
	}

	//FIX: Is there a way to do this in one operation?
	base.Log("Deleting %d %q documents of %q ...", len(vres.Rows), docType, db.Name)
	for _, row := range vres.Rows {
		base.LogTo("CRUD", "\tDeleting %q", row.ID)
		if err := db.Bucket.Delete(row.ID); err != nil {
			base.Warn("Error deleting %q: %v", row.ID, err)
		}
	}
	return nil
}

// Deletes old revisions that have been moved to individual docs
func (db *Database) Compact() (int, error) {
	opts := Body{"stale": false, "reduce": false}
	vres, err := db.Bucket.View("sync_housekeeping", "old_revs", opts)
	if err != nil {
		base.Warn("old_revs view returned %v", err)
		return 0, err
	}

	//FIX: Is there a way to do this in one operation?
	base.Log("Compacting away %d old revs of %q ...", len(vres.Rows), db.Name)
	count := 0
	for _, row := range vres.Rows {
		base.LogTo("CRUD", "\tDeleting %q", row.ID)
		if err := db.Bucket.Delete(row.ID); err != nil {
			base.Warn("Error deleting %q: %v", row.ID, err)
		} else {
			count++
		}
	}
	return count, nil
}

// Deletes all orphaned CouchDB attachments not used by any revisions.
func VacuumAttachments(bucket base.Bucket) (int, error) {
	return 0, base.HTTPErrorf(http.StatusNotImplemented, "Vacuum is temporarily out of order")
}

//////// SYNC FUNCTION:

const kSyncDataKey = "_sync:syncdata"

// Sets the database context's sync function based on the JS code from config.
// If the function is different from the prior one, all documents are run through it again to
// update their channel assignments and the access privileges they assign to users and roles.
// If importExistingDocs is true, documents in the bucket that are not known to Sync Gateway will
// be imported (have _sync data added) and run through the sync function.
func (context *DatabaseContext) ApplySyncFun(syncFun string, importExistingDocs bool) error {
	var err error
	if syncFun == "" {
		context.ChannelMapper = nil
	} else if context.ChannelMapper != nil {
		_, err = context.ChannelMapper.SetFunction(syncFun)
	} else {
		context.ChannelMapper = channels.NewChannelMapper(syncFun)
	}
	if err != nil {
		base.Warn("Error setting sync function: %s", err)
		return err
	}

	// Check whether the sync function is different from the previous one:
	var syncData struct {
		Sync string
	}
	err = context.Bucket.Get(kSyncDataKey, &syncData)
	syncDataMissing := base.IsDocNotFoundError(err)
	if err != nil && !syncDataMissing {
		return err
	} else if syncFun == syncData.Sync {
		// Sync function hasn't changed. But if importing, scan imported docs anyway:
		if importExistingDocs {
			db := &Database{context, nil}
			return db.UpdateAllDocChannels(false, importExistingDocs)
		}
		return nil
	} else {
		if !syncDataMissing {
			// It's changed, so re-run it on all docs:
			db := &Database{context, nil}
			if err = db.UpdateAllDocChannels(true, importExistingDocs); err != nil {
				return err
			}
		}

		// Finally save the new function source:
		syncData.Sync = syncFun
		return context.Bucket.Set(kSyncDataKey, 0, syncData)
	}
}

// Re-runs the sync function on every current document in the database (if doCurrentDocs==true)
// and/or imports docs in the bucket not known to the gateway (if doImportDocs==true).
// To be used when the JavaScript channelmap function changes.
func (db *Database) UpdateAllDocChannels(doCurrentDocs bool, doImportDocs bool) error {
	if doCurrentDocs {
		base.Log("Recomputing document channels...")
	}
	if doImportDocs {
		base.Log("Importing documents...")
	} else if !doCurrentDocs {
		return nil // no-op if neither option is set
	}
	options := Body{"stale": false, "reduce": false}
	if !doCurrentDocs {
		options["endkey"] = []interface{}{true}
		options["endkey_inclusive"] = false
	} else if !doImportDocs {
		options["startkey"] = []interface{}{true}
	}
	vres, err := db.Bucket.View("sync_housekeeping", "import", options)
	if err != nil {
		return err
	}

	// We are about to alter documents without updating their sequence numbers, which would
	// really confuse the changeCache, so turn it off until we're done:
	db.changeCache.EnableChannelLogs(false)
	defer db.changeCache.EnableChannelLogs(true)
	db.changeCache.ClearLogs()

	//base.Log("Re-running sync() function on all %d documents...", len(vres.Rows))
	changeCount := 0
	for _, row := range vres.Rows {
		rowKey := row.Key.([]interface{})
		docid := rowKey[1].(string)
		key := realDocID(docid)
		//base.Log("\tupdating %q", docid)
		err := db.Bucket.Update(key, 0, func(currentValue []byte) ([]byte, error) {
			// Be careful: this block can be invoked multiple times if there are races!
			if currentValue == nil {
				return nil, couchbase.UpdateCancel // someone deleted it?!
			}
			doc, err := unmarshalDocument(docid, currentValue)
			if err != nil {
				return nil, err
			}

			imported := false
			if !doc.hasValidSyncData() {
				// This is a document not known to the sync gateway. Ignore or import it:
				if !doImportDocs {
					return nil, couchbase.UpdateCancel
				}
				imported = true
				if err = db.initializeSyncData(doc); err != nil {
					return nil, err
				}
				base.LogTo("CRUD", "\tImporting document %q --> rev %q", docid, doc.CurrentRev)
			} else {
				if !doCurrentDocs {
					return nil, couchbase.UpdateCancel
				}
				base.LogTo("CRUD", "\tRe-syncing document %q", docid)
			}

			// Run the sync fn over each current/leaf revision, in case there are conflicts:
			changed := 0
			doc.History.forEachLeaf(func(rev *RevInfo) {
				body, _ := db.getRevFromDoc(doc, rev.ID, false)
				channels, access, roles, err := db.getChannelsAndAccess(doc, body, rev.ID)
				if err != nil {
					// Probably the validator rejected the doc
					base.Warn("Error calling sync() on doc %q: %v", docid, err)
					access = nil
					channels = nil
				}
				rev.Channels = channels

				if rev.ID == doc.CurrentRev {
					changed = len(doc.Access.updateAccess(doc, access)) +
						len(doc.RoleAccess.updateAccess(doc, roles)) +
						len(doc.updateChannels(channels))
				}
			})

			if changed > 0 || imported {
				base.LogTo("Access", "Saving updated channels and access grants of %q", docid)
				return json.Marshal(doc)
			} else {
				return nil, couchbase.UpdateCancel
			}
		})
		if err == nil {
			changeCount++
		} else if err != couchbase.UpdateCancel {
			base.Warn("Error updating doc %q: %v", docid, err)
		}
	}

	if changeCount > 0 {
		// Now invalidate channel cache of all users/roles:
		base.Log("Invalidating channel caches of users/roles...")
		users, roles, _ := db.AllPrincipalIDs()
		for _, name := range users {
			db.invalUserChannels(name)
		}
		for _, name := range roles {
			db.invalRoleChannels(name)
		}
	}
	return nil
}

func (db *Database) invalUserRoles(username string) {
	authr := db.Authenticator()
	if user, _ := authr.GetUser(username); user != nil {
		authr.InvalidateRoles(user)
	}
}

func (db *Database) invalUserChannels(username string) {
	authr := db.Authenticator()
	if user, _ := authr.GetUser(username); user != nil {
		authr.InvalidateChannels(user)
	}
}

func (db *Database) invalRoleChannels(rolename string) {
	authr := db.Authenticator()
	if role, _ := authr.GetRole(rolename); role != nil {
		authr.InvalidateChannels(role)
	}
}

func (db *Database) invalUserOrRoleChannels(name string) {
	if strings.HasPrefix(name, "role:") {
		db.invalRoleChannels(name[5:])
	} else {
		db.invalUserChannels(name)
	}
}

//////// SEQUENCE ALLOCATION:

func (context *DatabaseContext) LastSequence() (uint64, error) {
	return context.sequences.lastSequence()
}

func (context *DatabaseContext) ReserveSequences(numToReserve uint64) error {
	return context.sequences.reserveSequences(numToReserve)
}
