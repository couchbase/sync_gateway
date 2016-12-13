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
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// The body of a CouchDB document/revision as decoded from JSON.
type Body map[string]interface{}

func (body Body) ShallowCopy() Body {
	copied := make(Body, len(body))
	for key, value := range body {
		copied[key] = value
	}
	return copied
}

func (body Body) ImmutableAttachmentsCopy() Body {
	if body == nil {
		return nil
	}
	copied := make(Body, len(body))
	for k1, v1 := range body {
		if k1 == "_attachments" {
			atts := v1.(map[string]interface{})
			attscopy := make(map[string]interface{}, len(atts))
			for k2, v2 := range atts {
				attachment := v2.(map[string]interface{})
				attachmentcopy := make(map[string]interface{}, len(attachment))
				for k3, v3 := range attachment {
					attachmentcopy[k3] = v3
				}
				attscopy[k2] = attachmentcopy
			}
			v1 = attscopy
		}
		copied[k1] = v1
	}
	return copied
}

// Returns the expiry as uint32 (using getExpiry), and removes the _exp property from the body
func (body Body) extractExpiry() (uint32, error) {

	exp, err := body.getExpiry()
	if err != nil {
		return exp, err
	}
	delete(body, "_exp")

	return exp, nil
}

// Looks up the _exp property in the document, and turns it into a Couchbase Server expiry value, as:
//   1. Numeric JSON values are converted to uint32 and returned as-is
//   2. String JSON values that are numbers are converted to int32 and returned as-is
//   3. String JSON values that are ISO-8601 dates are converted to UNIX time and returned
//   4. Null JSON values return 0
func (body Body) getExpiry() (uint32, error) {
	rawExpiry, ok := body["_exp"]
	if !ok {
		return 0, nil
	}
	switch expiry := rawExpiry.(type) {
	case float64:
		return uint32(expiry), nil
	case string:
		// First check if it's a numeric string
		expInt, err := strconv.ParseInt(expiry, 10, 32)
		if err == nil {
			return uint32(expInt), nil
		}
		// Check if it's an ISO-8601 date
		expRFC3339, err := time.Parse(time.RFC3339, expiry)
		if err == nil {
			return uint32(expRFC3339.Unix()), nil
		} else {
			return 0, fmt.Errorf("Unable to parse expiry %s as either numeric or date expiry:%v", err)
		}
	case nil:
		// Leave as zero/empty expiry
		return 0, nil
	}

	return 0, nil
}

// Looks up the raw JSON data of a revision that's been archived to a separate doc.
// If the revision isn't found (e.g. has been deleted by compaction) returns 404 error.
func (db *DatabaseContext) getOldRevisionJSON(docid string, revid string) ([]byte, error) {
	data, _, err := db.Bucket.GetRaw(oldRevisionKey(docid, revid))
	if base.IsDocNotFoundError(err) {
		base.LogTo("CRUD+", "No old revision %q / %q", docid, revid)
		err = base.HTTPErrorf(404, "missing")
	}
	if data != nil {
		base.LogTo("CRUD+", "Got old revision %q / %q --> %d bytes", docid, revid, len(data))
	}
	return data, err
}

func (db *Database) setOldRevisionJSON(docid string, revid string, body []byte) error {
	base.LogTo("CRUD+", "Saving old revision %q / %q (%d bytes)", docid, revid, len(body))

	// Set old revisions to expire after 5 minutes.  Future enhancement to make this a config
	// setting might be appropriate.
	return db.Bucket.SetRaw(oldRevisionKey(docid, revid), 300, body)
}

//////// UTILITY FUNCTIONS:

func oldRevisionKey(docid string, revid string) string {
	return fmt.Sprintf("_sync:rev:%s:%d:%s", docid, len(revid), revid)
}

// Version of FixJSONNumbers (see base/util.go) that operates on a Body
func (body Body) FixJSONNumbers() {
	for k, v := range body {
		body[k] = base.FixJSONNumbers(v)
	}
}

func createRevID(generation int, parentRevID string, body Body) string {
	// This should produce the same results as TouchDB.
	digester := md5.New()
	digester.Write([]byte{byte(len(parentRevID))})
	digester.Write([]byte(parentRevID))
	digester.Write(canonicalEncoding(stripSpecialProperties(body)))
	return fmt.Sprintf("%d-%x", generation, digester.Sum(nil))
}

// Returns the generation number (numeric prefix) of a revision ID.
func genOfRevID(revid string) int {
	if revid == "" {
		return 0
	}
	var generation int
	n, _ := fmt.Sscanf(revid, "%d-", &generation)
	if n < 1 || generation < 1 {
		base.Warn("genOfRevID failed on %q", revid)
		return -1
	}
	return generation
}

// Splits a revision ID into generation number and hex digest.
func ParseRevID(revid string) (int, string) {
	if revid == "" {
		return 0, ""
	}
	var generation int
	var id string
	n, _ := fmt.Sscanf(revid, "%d-%s", &generation, &id)
	if n < 1 || generation < 1 {
		base.Warn("parseRevID failed on %q", revid)
		return -1, ""
	}
	return generation, id
}

func compareRevIDs(id1, id2 string) int {
	gen1, sha1 := ParseRevID(id1)
	gen2, sha2 := ParseRevID(id2)
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

func containsUserSpecialProperties(body Body) bool {
	for key := range body {
		if key != "" && key[0] == '_' && key != "_id" && key != "_rev" && key != "_deleted" && key != "_attachments" && key != "_revisions" {
			return true
		}
	}
	return false
}

func canonicalEncoding(body Body) []byte {
	encoded, err := json.Marshal(body) //FIX: Use canonical JSON encoder
	if err != nil {
		panic(fmt.Sprintf("Couldn't encode body %v", body))
	}
	return encoded
}

func GetStringArrayProperty(body Body, property string) ([]string, error) {
	if raw, exists := body[property]; !exists {
		return nil, nil
	} else if strings, ok := raw.([]string); ok {
		return strings, nil
	} else if items, ok := raw.([]interface{}); ok {
		strings := make([]string, len(items))
		for i := 0; i < len(items); i++ {
			strings[i], ok = items[i].(string)
			if !ok {
				return nil, base.HTTPErrorf(http.StatusBadRequest, property+" must be a string array")
			}
		}
		return strings, nil
	} else {
		return nil, base.HTTPErrorf(http.StatusBadRequest, property+" must be a string array")
	}
}
