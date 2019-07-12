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
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func unjson(j string) Body {
	var body Body
	err := json.Unmarshal([]byte(j), &body)
	if err != nil {
		panic(fmt.Sprintf("Invalid JSON: %v", err))
	}
	return body
}

func tojson(obj interface{}) string {
	j, _ := json.Marshal(obj)
	return string(j)
}

func TestBackupOldRevisionWithAttachments(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

	deltasEnabled := base.IsEnterpriseEdition()
	xattrsEnabled := base.TestUseXattrs()

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	bucket := testBucket.Bucket

	context, err := NewDatabaseContext(
		"db",
		bucket,
		false,
		DatabaseContextOptions{
			EnableXattr: xattrsEnabled,
			DeltaSyncOptions: DeltaSyncOptions{
				Enabled:          deltasEnabled,
				RevMaxAgeSeconds: DefaultDeltaSyncRevMaxAge,
			},
		},
	)
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")

	docID := "doc1"
	var rev1Body Body
	rev1Data := `{"test": true, "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`
	require.NoError(t, json.Unmarshal([]byte(rev1Data), &rev1Body))
	rev1ID, err := db.Put(docID, rev1Body)
	require.NoError(t, err)
	assert.Equal(t, "1-6e5a9ed9e2e8637d495ac5dd2fa90479", rev1ID)

	rev1OldBody, err := db.getOldRevisionJSON(docID, rev1ID)
	if deltasEnabled && xattrsEnabled {
		require.NoError(t, err)
		assert.Contains(t, string(rev1OldBody), "hello.txt")
	} else {
		// current revs aren't backed up unless both xattrs and deltas are enabled
		require.Error(t, err)
		assert.Equal(t, "404 missing", err.Error())
	}

	// create rev 2 and check backups for both revs
	var rev2Body Body
	rev2Data := `{"test": true, "updated": true, "_attachments": {"hello.txt": {"stub": true, "revpos": 1}}}`
	require.NoError(t, json.Unmarshal([]byte(rev2Data), &rev2Body))
	err = db.PutExistingRev(docID, rev2Body, []string{"2-abc", rev1ID}, true)
	require.NoError(t, err)
	rev2ID := "2-abc"

	// now in any case - we'll have rev 1 backed up
	rev1OldBody, err = db.getOldRevisionJSON(docID, rev1ID)
	require.NoError(t, err)
	assert.Contains(t, string(rev1OldBody), "hello.txt")

	// and rev 2 should be present only for the xattrs and deltas case
	rev2OldBody, err := db.getOldRevisionJSON(docID, rev2ID)
	if deltasEnabled && xattrsEnabled {
		require.NoError(t, err)
		assert.Contains(t, string(rev2OldBody), "hello.txt")
	} else {
		// current revs aren't backed up unless both xattrs and deltas are enabled
		require.Error(t, err)
		assert.Equal(t, "404 missing", err.Error())
	}
}

func TestAttachments(t *testing.T) {

	testBucket := base.GetTestBucketOrPanic()
	defer testBucket.Close()
	bucket := testBucket.Bucket

	context, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")

	// Test creating & updating a document:
	log.Printf("Create rev 1...")
	rev1input := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="},
                                    "bye.txt": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA=="}}}`
	var body Body
	json.Unmarshal([]byte(rev1input), &body)
	revid, err := db.Put("doc1", unjson(rev1input))
	rev1id := revid
	assert.NoError(t, err, "Couldn't create document")

	log.Printf("Retrieve doc...")
	rev1output := `{"_attachments":{"bye.txt":{"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA==","digest":"sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=","length":19,"revpos":1},"hello.txt":{"data":"aGVsbG8gd29ybGQ=","digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1}},"_id":"doc1","_rev":"1-54f3a105fb903018c160712ffddb74dc"}`
	gotbody, err := db.GetRev("doc1", "", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev1output, tojson(gotbody))

	log.Printf("Create rev 2...")
	rev2str := `{"_attachments": {"hello.txt": {"stub":true, "revpos":1}, "bye.txt": {"data": "YnllLXlh"}}}`
	var body2 Body
	json.Unmarshal([]byte(rev2str), &body2)
	body2[BodyRev] = revid
	revid, err = db.Put("doc1", body2)
	assert.NoError(t, err, "Couldn't update document")
	assert.Equal(t, "2-08b42c51334c0469bd060e6d9e6d797b", revid)

	log.Printf("Retrieve doc...")
	rev2output := `{"_attachments":{"bye.txt":{"data":"YnllLXlh","digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=","length":6,"revpos":2},"hello.txt":{"data":"aGVsbG8gd29ybGQ=","digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1}},"_id":"doc1","_rev":"2-08b42c51334c0469bd060e6d9e6d797b"}`
	gotbody, err = db.GetRev("doc1", "", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2output, tojson(gotbody))

	log.Printf("Retrieve doc with atts_since...")
	rev2Aoutput := `{"_attachments":{"bye.txt":{"data":"YnllLXlh","digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=","length":6,"revpos":2},"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1,"stub":true}},"_id":"doc1","_rev":"2-08b42c51334c0469bd060e6d9e6d797b"}`
	gotbody, err = db.GetRev("doc1", "", false, []string{"1-54f3a105fb903018c160712ffddb74dc", "1-foo", "993-bar"})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2Aoutput, tojson(gotbody))

	log.Printf("Create rev 3...")
	rev3str := `{"_attachments": {"bye.txt": {"stub":true,"revpos":2}}}`
	var body3 Body
	json.Unmarshal([]byte(rev3str), &body3)
	body3[BodyRev] = revid
	revid, err = db.Put("doc1", body3)
	assert.NoError(t, err, "Couldn't update document")
	assert.Equal(t, "3-252b9fa1f306930bffc07e7d75b77faf", revid)

	log.Printf("Retrieve doc...")
	rev3output := `{"_attachments":{"bye.txt":{"data":"YnllLXlh","digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=","length":6,"revpos":2}},"_id":"doc1","_rev":"3-252b9fa1f306930bffc07e7d75b77faf"}`
	gotbody, err = db.GetRev("doc1", "", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev3output, tojson(gotbody))

	log.Printf("Expire body of rev 1, then add a child...") // test fix of #498
	err = db.Bucket.Delete(oldRevisionKey("doc1", rev1id))
	assert.NoError(t, err, "Couldn't compact old revision")
	rev2Bstr := `{"_attachments": {"bye.txt": {"stub":true,"revpos":1,"digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}, "_rev": "2-f000"}`
	var body2B Body
	err = json.Unmarshal([]byte(rev2Bstr), &body2B)
	assert.NoError(t, err, "bad JSON")
	err = db.PutExistingRev("doc1", body2B, []string{"2-f000", rev1id}, false)
	assert.NoError(t, err, "Couldn't update document")
}

func TestAttachmentForRejectedDocument(t *testing.T) {

	testBucket := testBucket()
	defer testBucket.Close()
	bucket := testBucket.Bucket

	context, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
		throw({forbidden: "None shall pass!"});
	}`)

	docBody := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`
	var body Body
	json.Unmarshal([]byte(docBody), &body)
	_, err = db.Put("doc1", unjson(docBody))
	log.Printf("Got error on put doc:%v", err)
	db.Bucket.Dump()

	// Attempt to retrieve the attachment doc
	_, _, err = db.Bucket.GetRaw("_sync:att:sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=")

	assert.True(t, err != nil, "Expect error when attempting to retrieve attachment document after doc is rejected.")

}

func TestAttachmentRetrievalUsingRevCache(t *testing.T) {

	testBucket := base.GetTestBucketOrPanic()
	defer testBucket.Close()
	bucket := testBucket.Bucket

	context, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")

	// Test creating & updating a document:
	rev1input := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="},
                                    "bye.txt": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA=="}}}`
	_, err = db.Put("doc1", unjson(rev1input))
	assert.NoError(t, err, "Couldn't create document")

	initCount, countErr := base.GetExpvarAsInt("syncGateway_db", "document_gets")
	assert.NoError(t, countErr, "Couldn't retrieve document_gets expvar")
	rev1output := `{"_attachments":{"bye.txt":{"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA==","digest":"sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=","length":19,"revpos":1},"hello.txt":{"data":"aGVsbG8gd29ybGQ=","digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1}},"_id":"doc1","_rev":"1-54f3a105fb903018c160712ffddb74dc"}`
	gotbody, err := db.GetRev("doc1", "1-54f3a105fb903018c160712ffddb74dc", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev1output, tojson(gotbody))

	getCount, countErr := base.GetExpvarAsInt("syncGateway_db", "document_gets")
	assert.NoError(t, countErr, "Couldn't retrieve document_gets expvar")
	assert.Equal(t, initCount, getCount)

	// Repeat, validate no additional get operations
	gotbody, err = db.GetRev("doc1", "1-54f3a105fb903018c160712ffddb74dc", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev1output, tojson(gotbody))
	getCount, countErr = base.GetExpvarAsInt("syncGateway_db", "document_gets")
	assert.NoError(t, countErr, "Couldn't retrieve document_gets expvar")
	assert.Equal(t, initCount, getCount)
}
