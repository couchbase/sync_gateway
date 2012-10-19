//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package basecouch

import (
	"fmt"
	"net/http"
)

// Gets a local document.
func (db *Database) GetLocal(docid string) (Body, error) {
	key := db.realLocalDocID(docid)
	if key == "" {
		return nil, &HTTPError{Status: 400, Message: "Invalid doc ID"}
	}

	body := Body{}
	err := db.bucket.Get(key, &body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// Updates a local document.
func (db *Database) PutLocal(docid string, body Body) (string, error) {
	key := db.realLocalDocID(docid)
	if key == "" {
		return "", &HTTPError{Status: 400, Message: "Invalid doc ID"}
	}

	// Verify that the _rev key in the body matches the current stored value:
	var matchRev string
	if body["_rev"] != nil {
		matchRev = body["_rev"].(string)
	}
	prevBody := Body{}
	err := db.bucket.Get(key, &prevBody)
	if err != nil {
		if !isMissingDocError(err) {
			return "", err
		}
		if matchRev != "" {
			return "", &HTTPError{Status: http.StatusNotFound,
				Message: "No previous revision to replace"}
		}
	} else {
		if matchRev != prevBody["_rev"] {
			return "", &HTTPError{Status: http.StatusConflict, Message: "Document update conflict"}
		}
	}

	var generation uint
	if matchRev != "" {
		fmt.Sscanf(matchRev, "0-%d", &generation)
	}
	revid := fmt.Sprintf("0-%d", generation+1)
	body["_rev"] = revid

	err = db.bucket.Set(db.realLocalDocID(docid), 0, body)
	if err != nil {
		return "", err
	}
	return revid, nil
}

// Deletes a local document.
func (db *Database) DeleteLocal(docid string) error {
	key := db.realLocalDocID(docid)
	if key == "" {
		return &HTTPError{Status: 400, Message: "Invalid doc ID"}
	}
	return db.bucket.Delete(db.realLocalDocID(docid))
}

func (db *Database) realLocalDocID(docid string) string {
	// Local doc ID prefix is "ldoc:"
	return "l" + db.realDocID(docid)
}
