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
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
)

func (db *Database) GetSpecial(doctype string, docid string) (Body, error) {
	key := db.realSpecialDocID(doctype, docid)
	if key == "" {
		return nil, base.HTTPErrorf(400, "Invalid doc ID")
	}

	body := Body{}
	_, err := db.Bucket.Get(key, &body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Updates or deletes a special document.
func (db *Database) putSpecial(doctype string, docid string, matchRev string, body Body) (string, error) {
	key := db.realSpecialDocID(doctype, docid)
	if key == "" {
		return "", base.HTTPErrorf(400, "Invalid doc ID")
	}
	var revid string

	expiry, err := body.getExpiry()
	if err != nil {
		return "", base.HTTPErrorf(http.StatusBadRequest, "Invalid expiry: %v", err)
	}
	err = db.Bucket.Update(key, int(expiry), func(value []byte) ([]byte, error) {
		if len(value) == 0 {
			if matchRev != "" || body == nil {
				return nil, base.HTTPErrorf(http.StatusNotFound, "No previous revision to replace")
			}
		} else {
			prevBody := Body{}
			if err := json.Unmarshal(value, &prevBody); err != nil {
				return nil, err
			}
			if matchRev != prevBody["_rev"] {
				return nil, base.HTTPErrorf(http.StatusConflict, "Document update conflict")
			}
		}

		if body != nil {
			// Updating:
			var generation uint
			if matchRev != "" {
				fmt.Sscanf(matchRev, "0-%d", &generation)
			}
			revid = fmt.Sprintf("0-%d", generation+1)
			body["_rev"] = revid
			return json.Marshal(body)
		} else {
			// Deleting:
			return nil, nil
		}
	})

	return revid, err
}

func (db *Database) PutSpecial(doctype string, docid string, body Body) (string, error) {
	matchRev, _ := body["_rev"].(string)
	body = stripSpecialSpecialProperties(body)
	return db.putSpecial(doctype, docid, matchRev, body)
}

func (db *Database) DeleteSpecial(doctype string, docid string, revid string) error {
	_, err := db.putSpecial(doctype, docid, revid, nil)
	return err
}

func (db *Database) realSpecialDocID(doctype string, docid string) string {
	return "_sync:" + doctype + ":" + docid
}

func stripSpecialSpecialProperties(body Body) Body {
	stripped := Body{}
	for key, value := range body {
		if key == "" || key[0] != '_' {
			stripped[key] = value
		}
	}
	return stripped
}
