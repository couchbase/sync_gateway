//  Copyright (c) 2011 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package basecouch

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
func (db *Database) PutLocal(docid string, body Body) error {
	key := db.realLocalDocID(docid)
	if key == "" {
		return &HTTPError{Status: 400, Message: "Invalid doc ID"}
	}

	return db.bucket.Set(db.realLocalDocID(docid), 0, body)
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
	return db.realDocID("_local/" + docid)
}
