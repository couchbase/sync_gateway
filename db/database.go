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
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"regexp"

	"github.com/couchbaselabs/go-couchbase"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

var kDBNameMatch = regexp.MustCompile("[-%+()$_a-z0-9]+")

type DatabaseContext struct {
	Name          string
	Bucket        *couchbase.Bucket
	sequences     *sequenceAllocator
	ChannelMapper *channels.ChannelMapper
	Validator     *Validator
}

// Represents a simulated CouchDB database.
type Database struct {
	*DatabaseContext
	user *auth.User
}

// Helper function to open a Couchbase connection and return a specific bucket.
func ConnectToBucket(couchbaseURL, poolName, bucketName string) (bucket *couchbase.Bucket, err error) {
	bucket, err = couchbase.GetBucket(couchbaseURL, poolName, bucketName)
	if err != nil {
		return
	}
	log.Printf("Connected to <%s>, pool %s, bucket %s", couchbaseURL, poolName, bucketName)
	err = installViews(bucket)
	return
}

func NewDatabaseContext(dbName string, bucket *couchbase.Bucket) (*DatabaseContext, error) {
	sequences, err := newSequenceAllocator(bucket)
	if err != nil {
		return nil, err
	}
	return &DatabaseContext{Name: dbName, Bucket: bucket, sequences: sequences}, nil
}

// Makes a Database object given its name and bucket.
func GetDatabase(context *DatabaseContext, user *auth.User) (*Database, error) {
	return &Database{context, user}, nil
}

func CreateDatabase(context *DatabaseContext) (*Database, error) {
	return &Database{context, nil}, nil
}

func (db *Database) SameAs(otherdb *Database) bool {
	return db != nil && otherdb != nil &&
		db.Bucket == otherdb.Bucket
}

// Sets the database object's channelMapper and validator based on the JS code in _design/channels
func (db *Database) ReadDesignDocument() error {
	body, err := db.Get("_design/channels")
	if err != nil {
		if status, _ := base.ErrorAsHTTPStatus(err); status == http.StatusNotFound {
			err = nil // missing design document is not an error
		}
		return err
	}
	src, ok := body["channelmap"].(string)
	if ok {
		log.Printf("Channel mapper = %s", src)
		db.ChannelMapper, err = channels.NewChannelMapper(src)
		if err != nil {
			log.Printf("WARNING: Error loading channel mapper: %s", err)
			return err
		}
	}
	src, ok = body["validate_doc_update"].(string)
	if ok {
		log.Printf("Validator = %s", src)
		db.Validator, err = NewValidator(src)
		if err != nil {
			log.Printf("WARNING: Error loading validator: %s", err)
			return err
		}
	}
	return nil
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

func installViews(bucket *couchbase.Bucket) error {
	node := bucket.Nodes[rand.Intn(len(bucket.Nodes))]
	u, err := url.Parse(node.CouchAPIBase)
	if err != nil {
		fmt.Printf("Failed to parse %s", node.CouchAPIBase)
		return err
	}
	u.Path = fmt.Sprintf("/%s/_design/%s", bucket.Name, "sync_gateway")

	// View for finding every Couchbase doc used by a database (for cleanup upon deletion)
	allbits_map := `function (doc, meta) {
                     var pieces = meta.id.split(":", 2);
                     if (pieces.length < 2)
                       return;
					 var type = pieces[0];
					 if (type == "doc" || type == "ldoc" || type == "seq")
                       emit(meta.id, null); }`
	// View for _all_docs
	alldocs_map := `function (doc, meta) {
                     var pieces = meta.id.split(":", 2);
                     if (pieces.length < 2 || pieces[0] != "doc")
                       return;
                     if (doc.deleted || doc.id===undefined)
                       return;
                     emit(doc.id, doc.rev); }`
	// View for _changes feed, i.e. a by-sequence index
	changes_map := `function (doc, meta) {
                    if (doc.sequence === undefined)
                        return;
                    var pieces = meta.id.split(":", 2);
                    if (pieces.length < 2 || pieces[0] != "doc" || doc.id===undefined)
                        return;
                    var value = [doc.id, doc.rev];
                    if (doc.deleted)
                        value.push(true);
                    emit(doc.sequence, value); }`
	// By-channels view
	channels_map := `function (doc, meta) {
						var sequence = doc.sequence;
	                    if (sequence === undefined)
	                        return;
	                    var pieces = meta.id.split(":", 2);
	                    if (pieces.length < 2 || pieces[0] != "doc" || doc.id===undefined)
	                        return;
	                    var value = [doc.id, doc.rev];
	                    if (doc.deleted)
	                        value.push(true);
						emit(["*", sequence], value);
						var channels = doc["channels"];
						if (channels) {
							for (var name in channels) {
								removed = channels[name];
								if (!removed)
									emit([name, sequence], value);
								else
									emit([name, removed.seq], [doc.id, removed.rev, false]);
							}
						}
					}`
	// View for mapping revision IDs to documents (for vacuuming)
	revs_map := `function (doc, meta) {
					  var pieces = meta.id.split(":", 2);
					  if (pieces.length < 2)
					    return;
					  if (pieces[0] == "rev") {
					    emit(pieces[1], true);
					  } else if (pieces[0] == "doc") {
					    var keys = doc.history.keys;
					    for (var i = 0; i < keys.length; ++i) {
					      if (keys[i] != "")
					        emit(keys[i], false);
					    }
					  }
					}`
	// View for mapping attachment IDs to revisions (for vacuuming)
	atts_map := `function (doc, meta) {
				  var pieces = meta.id.split(":", 2);
				  if (pieces.length < 2)
				    return;
				  if (pieces[0] == "att") {
				    emit(pieces[1], true);
				  } else if (pieces[0] == "rev") {
				    var atts = doc._attachments;
				    if (atts) {
				      for (var i = 0; i < atts.length; ++i) {
				        var digest = atts[i].digest
				        if (digest)
				          emit(digest, false);
				      }
				    }
				  }
				}`
	// Reduce function for revs_map and atts_map
	revs_or_atts_reduce := `function(keys, values, rereduce) {
							  for (var i = 0; i < values.length; ++i) {
							    if (!values[i])
							      return false;
							  }
							  return true;
							}`

	ddoc := Body{
		"language": "javascript",
		"views": Body{
			"all_bits": Body{"map": allbits_map},
			"all_docs": Body{"map": alldocs_map, "reduce": "_count"},
			"channels": Body{"map": channels_map},
			"revs":     Body{"map": revs_map, "reduce": revs_or_atts_reduce},
			"atts":     Body{"map": atts_map, "reduce": revs_or_atts_reduce},
			"changes":  Body{"map": changes_map}}}
	payload, err := json.Marshal(ddoc)
	rq, err := http.NewRequest("PUT", u.String(), bytes.NewBuffer(payload))
	rq.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(rq)

	if err == nil && response.StatusCode > 299 {
		err = &base.HTTPError{Status: response.StatusCode, Message: response.Status}
	}
	if err != nil {
		log.Printf("WARNING: Error installing Couchbase design doc: %v", err)
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

func (db *Database) queryAllDocs(reduce bool) (couchbase.ViewResult, error) {
	opts := Body{"stale": false, "reduce": reduce}
	vres, err := db.Bucket.View("sync_gateway", "all_docs", opts)
	if err != nil {
		log.Printf("WARNING: all_docs got error: %v", err)
	}
	return vres, err
}

// Deletes a database (and all documents)
func (db *Database) Delete() error {
	opts := Body{"stale": false}
	vres, err := db.Bucket.View("sync_gateway", "all_bits", opts)
	if err != nil {
		log.Printf("WARNING: all_bits view returned %v", err)
		return err
	}

	//FIX: Is there a way to do this in one operation?
	log.Printf("Deleting %d documents of %q ...", len(vres.Rows), db.Name)
	for _, row := range vres.Rows {
		if base.Logging {
			log.Printf("\tDeleting %q", row.ID)
		}
		if err := db.Bucket.Delete(row.ID); err != nil {
			log.Printf("WARNING: Error deleting %q: %v", row.ID, err)
		}
	}
	return nil
}

func vacuumContent(bucket *couchbase.Bucket, viewName string, docPrefix string) (int, error) {
	vres, err := bucket.View("sync_gateway", viewName, Body{"stale": false, "group_level": 1})
	if err != nil {
		log.Printf("WARNING: %s view returned %v", viewName, err)
		return 0, err
	}
	deletions := 0
	for _, row := range vres.Rows {
		if row.Value.(bool) {
			deletions += vacIt(bucket, docPrefix+row.Key.(string))
		}
	}
	return deletions, nil
}

func vacIt(bucket *couchbase.Bucket, docid string) int {
	if err := bucket.Delete(docid); err != nil {
		log.Printf("\tError vacuuming %q: %v", docid, err)
		return 0
	}
	if base.Logging {
		log.Printf("\tVacuumed %q", docid)
	}
	return 1
}

// Deletes all orphaned CouchDB revisions not used by any documents.
func VacuumRevisions(bucket *couchbase.Bucket) (int, error) {
	return vacuumContent(bucket, "revs", "rev:")
}

// Deletes all orphaned CouchDB attachments not used by any revisions.
func VacuumAttachments(bucket *couchbase.Bucket) (int, error) {
	return vacuumContent(bucket, "atts", "att:")
}

// Re-runs the channelMapper on every document in the database.
// To be used when the JavaScript channelmap function changes.
func (db *Database) UpdateAllDocChannels() error {
	log.Printf("Recomputing document channels...")
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
			doc := newDocument()
			if err := json.Unmarshal(currentValue, doc); err != nil {
				return nil, err
			}
			body, err := db.getRevFromDoc(doc, "", false)
			if err != nil {
				return nil, err
			}
			channels, access := db.getChannelsAndAccess(body)
			if !db.updateDocChannels(doc, channels) {
				return nil, couchbase.UpdateCancel // unchanged
			}
			if !db.updateDocAccess(doc, access) {
				return nil, couchbase.UpdateCancel // unchanged
			}
			log.Printf("\tSaving updated channels of %q", docid)
			return json.Marshal(doc)
		})
		if err != nil && err != couchbase.UpdateCancel {
			log.Printf("WARNING: Error updating doc %q: %v", docid, err)
		}
	}
	return nil
}
