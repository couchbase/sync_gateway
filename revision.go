//  Copyright (c) 2011 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package basecouch

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"

	"github.com/couchbaselabs/go-couchbase"
)

// The body of a CouchDB document/revision as decoded from JSON.
type Body map[string]interface{}

func (db *Database) getRevision(docid, revid string, key RevKey) (Body, error) {
	var body Body
	err := db.bucket.Get(revKeyToString(key), &body)
	if err != nil {
		return nil, err
	}
	body["_id"] = docid
	body["_rev"] = revid
	return body, nil
}

func (db *Database) setRevision(body Body) (RevKey, error) {
	body = stripSpecialProperties(body)
	digester := sha1.New()
	digester.Write(canonicalEncoding(body))
	revKey := RevKey(base64.StdEncoding.EncodeToString(digester.Sum(nil)))
	_, err := db.bucket.Add(revKeyToString(revKey), 0, body)
	return revKey, err
}

// Deletes all orphaned CouchDB revisions not used by any documents.
func VacuumRevisions(bucket *couchbase.Bucket) (int, error) {
	vres, err := bucket.View("couchdb", "revs", nil)
	if err != nil {
		log.Printf("WARNING: revs view returned %v", err)
		return 0, err
	}
	deletions := 0
	curRev := ""
	hasDocument := false
	for _, row := range vres.Rows {
		key := row.Key.([]interface{})
		revid := key[0].(string)
		//log.Printf("\trev=%q, isRev=%v", revid, key[1].(bool))//TEMP
		if revid != curRev {
			if !hasDocument && curRev != "" {
				revdocid := revKeyToString(RevKey(curRev))
				if LogRequestsVerbose {
					log.Printf("\tDeleting %q", revdocid)
				}
				err = bucket.Delete(revdocid)
				if err == nil {
					deletions++
				}
			}
			curRev = revid
			hasDocument = false
		}
		if !hasDocument && !key[1].(bool) {
			hasDocument = true
		}
	}
	return deletions, nil
}

//////// HELPERS:

func revKeyToString(key RevKey) string {
	return "rev:" + string(key)
}

func createRevID(generation int, parentRevID string, body Body) string {
	// This should produce the same results as TouchDB.
	digester := md5.New()
	digester.Write([]byte{byte(len(parentRevID))})
	digester.Write([]byte(parentRevID))
	digester.Write(canonicalEncoding(stripSpecialProperties(body)))
	return fmt.Sprintf("%d-%x", generation, digester.Sum(nil))
}

func parseRevID(revid string) (int, string) {
	if revid == "" {
		return 0, ""
	}
	var generation int
	var id string
	n, _ := fmt.Sscanf(revid, "%d-%s", &generation, &id)
	if n < 1 || generation < 1 {
		log.Printf("WARNING: parseRevID failed on %q", revid)
		return -1, ""
	}
	return generation, id
}

func compareRevIDs(id1, id2 string) int {
	gen1, sha1 := parseRevID(id1)
	gen2, sha2 := parseRevID(id2)
	switch {
	case gen1 > gen2:
		return 1
	case gen1 < gen2:
		return -1
	case sha1 > sha2:
		return 1
	case sha1 < sha2:
		return -1
	}
	return 0
}

func stripSpecialProperties(body Body) Body {
	stripped := Body{}
	for key, value := range body {
		if key == "" || key[0] != '_' || key == "_attachments" || key == "_deleted" {
			stripped[key] = value
		}
	}
	return stripped
}

func canonicalEncoding(body Body) []byte {
	encoded, err := json.Marshal(body) //FIX: Use canonical JSON encoder
	if err != nil {
		panic(fmt.Sprintf("Couldn't encode body %v", body))
	}
	return encoded
}
