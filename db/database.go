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
	Name          string
	Bucket        base.Bucket
	sequences     *sequenceAllocator
	ChannelMapper *channels.ChannelMapper
	Validator     *Validator
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

func NewDatabaseContext(dbName string, bucket base.Bucket) (*DatabaseContext, error) {
	sequences, err := newSequenceAllocator(bucket)
	if err != nil {
		return nil, err
	}
	return &DatabaseContext{Name: dbName, Bucket: bucket, sequences: sequences}, nil
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
	allbits_map := `function (doc, meta) {
                      emit(meta.id, null); }`
	// View for _all_docs
	alldocs_map := `function (doc, meta) {
                     var sync = doc._sync;
                     if (sync === undefined || meta.id.substring(0,6) == "_sync:")
                       return;
                     if (sync.deleted)
                       return;
                     emit(meta.id, sync.rev); }`
	// View for _changes feed, i.e. a by-sequence index
	changes_map := `function (doc, meta) {
                    var sync = doc._sync;
                    if (sync === undefined || meta.id.substring(0,6) == "_sync:")
                        return;
                    if (sync.sequence === undefined)
                        return;
                    var value = [meta.id, sync.rev];
                    if (sync.deleted)
                        value.push(true);
                    emit(sync.sequence, value); }`
	// By-channels view
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
									emit([name, removed.seq], [meta.id, removed.rev, false]);
							}
						}
					}`
	// Channel access view, used by ComputeChannelsForPrincipal()
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

	ddoc := walrus.DesignDoc{
		Views: walrus.ViewMap{
			"all_bits": walrus.ViewDef{Map: allbits_map},
			"all_docs": walrus.ViewDef{Map: alldocs_map, Reduce: "_count"},
			"channels": walrus.ViewDef{Map: channels_map},
			"access":   walrus.ViewDef{Map: access_map},
			"changes":  walrus.ViewDef{Map: changes_map},
		},
	}
	err := bucket.PutDDoc("sync_gateway", ddoc)
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

func (db *Database) queryAllDocs(reduce bool) (walrus.ViewResult, error) {
	opts := Body{"stale": false, "reduce": reduce}
	vres, err := db.Bucket.View("sync_gateway", "all_docs", opts)
	if err != nil {
		base.Warn("all_docs got error: %v", err)
	}
	return vres, err
}

// Deletes a database (and all documents)
func (db *Database) Delete() error {
	opts := Body{"stale": false}
	vres, err := db.Bucket.View("sync_gateway", "all_bits", opts)
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
			channels, access, err := db.getChannelsAndAccess(doc, body, parentRevID)
			if err != nil {
				// Probably the validator rejected the doc
				access = nil
				channels = nil
			}
			db.updateDocAccess(doc, access)
			db.updateDocChannels(doc, channels)
			base.Log("\tSaving updated channels and access grants of %q", docid)
			return json.Marshal(doc)
		})
		if err != nil && err != couchbase.UpdateCancel {
			base.Warn("Error updating doc %q: %v", docid, err)
		}
	}
	return nil
}
