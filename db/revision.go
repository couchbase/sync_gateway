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

	"github.com/couchbase/sync_gateway/base"
)

// The body of a CouchDB document/revision as decoded from JSON.
type Body map[string]interface{}

func (b *Body) Unmarshal(data []byte) error {
	if err := json.Unmarshal(data, &b); err != nil {
		return err
	}
	return nil
}

func (body Body) ShallowCopy() Body {
	copied := make(Body, len(body))
	for key, value := range body {
		copied[key] = value
	}
	return copied
}

// Creates a mutable copy that does a deep copy of the _attachments property in the body,
// suitable for modification by the caller
func (body Body) MutableAttachmentsCopy() Body {
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

	exp, present, err := body.getExpiry()
	if !present || err != nil {
		return exp, err
	}
	delete(body, "_exp")

	return exp, nil
}

// Looks up the _exp property in the document, and turns it into a Couchbase Server expiry value, as:
func (body Body) getExpiry() (uint32, bool, error) {
	rawExpiry, ok := body["_exp"]
	if !ok {
		return 0, false, nil //_exp not present
	}
	expiry, err := base.ReflectExpiry(rawExpiry)
	if err != nil || expiry == nil {
		return 0, false, err
	}
	return *expiry, true, err
}

// nonJSONPrefix is used to ensure old revision bodies aren't hidden from N1QL/Views.
const nonJSONPrefix = byte(1)

// Looks up the raw JSON data of a revision that's been archived to a separate doc.
// If the revision isn't found (e.g. has been deleted by compaction) returns 404 error.
func (db *DatabaseContext) getOldRevisionJSON(docid string, revid string) ([]byte, error) {
	data, _, err := db.Bucket.GetRaw(oldRevisionKey(docid, revid))
	if base.IsDocNotFoundError(err) {
		base.Debugf(base.KeyCRUD, "No old revision %q / %q", base.UD(docid), revid)
		err = base.HTTPErrorf(404, "missing")
	}
	if data != nil {
		// Strip out the non-JSON prefix
		if len(data) > 0 && data[0] == nonJSONPrefix {
			data = data[1:]
		}
		base.Debugf(base.KeyCRUD, "Got old revision %q / %q --> %d bytes", base.UD(docid), revid, len(data))
	}
	return data, err
}

func (db *Database) setOldRevisionJSON(docid string, revid string, body []byte) error {
	base.Debugf(base.KeyCRUD, "Saving old revision %q / %q (%d bytes)", base.UD(docid), revid, len(body))

	// Set old revisions to expire after Options.OldRevExpirySeconds.  Defaults to 5 minutes.

	// Setting the binary flag isn't sufficient to make N1QL ignore the doc - the binary flag is only used by the SDKs.
	// To ensure it's not available via N1QL, need to prefix the raw bytes with non-JSON data.
	// Prepending using append/shift/set to reduce garbage.
	body = append(body, byte(0))
	copy(body[1:], body[0:])
	body[0] = nonJSONPrefix
	return db.Bucket.SetRaw(oldRevisionKey(docid, revid), db.DatabaseContext.Options.OldRevExpirySeconds, base.BinaryDocument(body))
}

// Currently only used by unit tests - deletes an archived old revision from the database
func (db *Database) purgeOldRevisionJSON(docid string, revid string) error {
	base.Debugf(base.KeyCRUD, "Purging old revision backup %q / %q ", base.UD(docid), revid)
	return db.Bucket.Delete(oldRevisionKey(docid, revid))
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

// CreateRevIDForImport assumes docs being imported are already canonically encoded, and have special properties removed
func createRevIDForImport(generation int, parentRevID string, body []byte) string {
	digester := md5.New()
	digester.Write([]byte{byte(len(parentRevID))})
	digester.Write([]byte(parentRevID))
	digester.Write(body)
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
		base.Warnf(base.KeyAll, "genOfRevID unsuccessful for %q", revid)
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
		base.Warnf(base.KeyAll, "parseRevID unsuccessful for %q", revid)
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
