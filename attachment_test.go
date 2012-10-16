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
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"github.com/sdegutis/go.assert"
)

func unjson(j string) Body {
	var body Body
	err := json.Unmarshal([]byte(j), &body)
	if err != nil {
		panic(fmt.Sprintf("Invalid JSON: %v", err))
	}
	return body
}

func TestAttachments(t *testing.T) {
	db, err := CreateDatabase(gTestBucket, "testdb")
	assertNoError(t, err, "Couldn't create database 'testdb'")
	defer func() {
		err = db.Delete()
		status, _ := ErrorAsHTTPStatus(err)
		if status != 200 && status != 404 {
			assertNoError(t, err, "Couldn't delete database 'testdb'")
		}
	}()

	// Test creating & updating a document:
	log.Printf("Create rev 1...")
	rev1input := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="},
                                    "bye.txt": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA=="}}}`
	var body Body
	json.Unmarshal([]byte(rev1input), &body)
	revid, err := db.Put("doc1", unjson(rev1input))
	assertNoError(t, err, "Couldn't create document")

	log.Printf("Retrieve doc...")
	rev1output := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ=", "length":11, "digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", "revpos":1},
                                  "bye.txt": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA==", "length":19, "digest":"sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=", "revpos":1}},
                    "_id":"doc1", "_rev":"1-54f3a105fb903018c160712ffddb74dc"}`
	gotbody, err := db.GetRev("doc1", "", false, []string{})
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, unjson(rev1output))

	log.Printf("Create rev 2...")
	rev2str := `{"_attachments": {"hello.txt": {}, "bye.txt": {"data": "YnllLXlh"}}}`
	var body2 Body
	json.Unmarshal([]byte(rev2str), &body2)
	body2["_rev"] = revid
	revid, err = db.Put("doc1", body2)
	assertNoError(t, err, "Couldn't update document")
	assert.Equals(t, revid, "2-08b42c51334c0469bd060e6d9e6d797b")

	log.Printf("Retrieve doc...")
	rev2output := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ=", "length":11, "digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", "revpos":1},
                                  "bye.txt": {"data": "YnllLXlh", "length":6, "digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=", "revpos":2}},
                    "_id":"doc1", "_rev":"2-08b42c51334c0469bd060e6d9e6d797b"}`
	gotbody, err = db.GetRev("doc1", "", false, []string{})
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, unjson(rev2output))

	log.Printf("Retrieve doc with atts_since...")
	rev2Aoutput := `{"_attachments": {"hello.txt": {"stub":true, "length":11, "digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", "revpos":1},
                                  "bye.txt": {"data": "YnllLXlh", "length":6, "digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=", "revpos":2}},
                    "_id":"doc1", "_rev":"2-08b42c51334c0469bd060e6d9e6d797b"}`
	gotbody, err = db.GetRev("doc1", "", false, []string{"1-54f3a105fb903018c160712ffddb74dc", "1-foo", "993-bar"})
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, unjson(rev2Aoutput))

	log.Printf("Create rev 3...")
	rev3str := `{"_attachments": {"bye.txt": {}}}`
	var body3 Body
	json.Unmarshal([]byte(rev3str), &body3)
	body3["_rev"] = revid
	revid, err = db.Put("doc1", body3)
	assertNoError(t, err, "Couldn't update document")
	assert.Equals(t, revid, "3-252b9fa1f306930bffc07e7d75b77faf")

	log.Printf("Retrieve doc...")
	rev3output := `{"_attachments": {"bye.txt": {"data": "YnllLXlh", "length":6, "digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=", "revpos":2}},
                    "_id":"doc1", "_rev":"3-252b9fa1f306930bffc07e7d75b77faf"}`
	gotbody, err = db.GetRev("doc1", "", false, []string{})
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, unjson(rev3output))
}
