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
	"net/http"
	"regexp"
	"strings"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/couchbaselabs/walrus"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

var kDBNameMatch = regexp.MustCompile("[-%+()$_a-z0-9]+")

// Basic description of a database. Shared between all Database objects on the same database.
// This object is thread-safe so it can be shared between HTTP handlers.
type DatabaseContext struct {
	Name          string                  // Database name
	Bucket        base.Bucket             // Storage
	tapListener   changeListener          // Listens on server Tap feed
	sequences     *sequenceAllocator      // Source of new sequence numbers
	ChannelMapper *channels.ChannelMapper // Runs JS 'sync' function
}

// Represents a simulated CouchDB database. A new instance is created for each HTTP request,
// so this struct does not have to be thread-safe.
type Database struct {
	*DatabaseContext
	user auth.User
}

// Helper function to open a Couchbase connection and return a specific bucket.
func ConnectToBucket(couchbaseURL, poolName, bucketName string) (bucket base.Bucket, err error) {
	bucket, err = base.GetBucket(couchbaseURL, poolName, bucketName)
	if err != nil {
		return
	}
	base.Log("Connected to <%s>, pool %s, bucket %s", couchbaseURL, poolName, bucketName)
	err = installViews(bucket)
	return
}

// Creates a new DatabaseContext on a bucket. The bucket will be closed when this context closes.
func NewDatabaseContext(dbName string, bucket base.Bucket) (*DatabaseContext, error) {
	context := &DatabaseContext{
		Name:   dbName,
		Bucket: bucket,
	}
	var err error
	context.sequences, err = newSequenceAllocator(bucket)
	if err != nil {
		return nil, err
	}
	if err = context.tapListener.Start(bucket); err != nil {
		return nil, err
	}
	return context, nil
}

func (context *DatabaseContext) Close() {
	if context.ChannelMapper != nil {
		context.ChannelMapper.Stop()
	}
	context.tapListener.Stop()
	context.Bucket.Close()
	context.Bucket = nil
}

func (context *DatabaseContext) Authenticator() *auth.Authenticator {
	// Authenticators are lightweight & stateless, so it's OK to return a new one every time
	return auth.NewAuthenticator(context.Bucket, context)
}

// Sets the database context's channelMapper based on the JS code from config
func (context *DatabaseContext) ApplySyncFun(syncFun string) error {
	var err error
	if context.ChannelMapper != nil {
		_, err = context.ChannelMapper.SetFunction(syncFun)
	} else {
		context.ChannelMapper, err = channels.NewChannelMapper(syncFun)
	}
	if err != nil {
		base.Warn("Error setting sync function: %s", err)
		return err
	}
	return nil
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
	// Key is docid; value is revid
	alldocs_map := `function (doc, meta) {
                     var sync = doc._sync;
                     if (sync === undefined || meta.id.substring(0,6) == "_sync:")
                       return;
                     if (sync.deleted)
                       return;
                     emit(meta.id, sync.rev); }`
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
							 if (prefix == "_sync:user:" || prefix == "_sync:role:")
			                     emit(meta.id.substring(11), prefix == "_sync:user:"); }`
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
	                    var value = [meta.id, sync.rev];
	                    if (sync.deleted)
	                        value.push(true);
						emit(["*", sequence], value);
						var channels = sync.channels;
						if (channels) {
							for (var name in channels) {
								removed = channels[name];
								if (!removed)
									emit([name, sequence], value);
								else
									emit([name, removed.seq],
										 [meta.id, removed.rev, !!removed.del, true]);
							}
						}
					}`
	// Channel access view, used by ComputeChannelsForPrincipal()
	// Key is username; value is dictionary channelName->firstSequence (compatible with TimedSet)
	access_map := `function (doc, meta) {
	                    var sync = doc._sync;
	                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
	                        return;
	                    var sequence = sync.sequence;
	                    if (sync.deleted || sequence === undefined)
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
	                    var sequence = sync.sequence;
	                    if (sync.deleted || sequence === undefined)
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
	DocID string
	RevID string
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
		result = append(result, IDAndRev{DocID: row.Key.(string), RevID: row.Value.(string)})
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

// Deletes a database (and all documents)
func (db *Database) Delete() error {
	opts := Body{"stale": false}
	vres, err := db.Bucket.View("sync_housekeeping", "all_bits", opts)
	if err != nil {
		base.Warn("all_bits view returned %v", err)
		return err
	}

	//FIX: Is there a way to do this in one operation?
	base.Log("Deleting %d documents of %q ...", len(vres.Rows), db.Name)
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
	base.Log("Deleting %d old revs of %q ...", len(vres.Rows), db.Name)
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
	return 0, &base.HTTPError{http.StatusNotImplemented, "Vacuum is temporarily out of order"}
}

// Re-runs the channelMapper on every document in the database.
// To be used when the JavaScript channelmap function changes.
func (db *Database) UpdateAllDocChannels() error {
	base.Log("Recomputing document channels...")
	vres, err := db.Bucket.View("sync_gateway", "all_docs", Body{"stale": false, "reduce": false})
	if err != nil {
		return err
	}
	for _, row := range vres.Rows {
		docid := row.Key.(string)
		key := db.realDocID(docid)
		err := db.Bucket.Update(key, 0, func(currentValue []byte) ([]byte, error) {
			// Be careful: this block can be invoked multiple times if there are races!
			if currentValue == nil {
				return nil, couchbase.UpdateCancel // someone deleted it?!
			}
			doc, err := unmarshalDocument(docid, currentValue)
			if err != nil {
				return nil, err
			}
			body, err := db.getRevFromDoc(doc, "", false)
			if err != nil {
				return nil, err
			}
			parentRevID := doc.History[doc.CurrentRev].Parent
			channels, access, roles, err := db.getChannelsAndAccess(doc, body, parentRevID)
			if err != nil {
				// Probably the validator rejected the doc
				access = nil
				channels = nil
			}
			doc.Access.updateAccess(doc, access)
			doc.RoleAccess.updateAccess(doc, roles)
			doc.updateChannels(channels)
			base.Log("\tSaving updated channels and access grants of %q", docid)
			return json.Marshal(doc)
		})
		if err != nil && err != couchbase.UpdateCancel {
			base.Warn("Error updating doc %q: %v", docid, err)
		}
	}

	// Now invalidate channel cache of all users/roles:
	users, roles, _ := db.AllPrincipalIDs()
	for _, name := range users {
		db.invalUserChannels(name)
	}
	for _, name := range roles {
		db.invalRoleChannels(name)
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
