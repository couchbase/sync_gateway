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
	incomingDocument := IncomingDocument{
		BodyBytes: []byte(`{"test":true}`),
		SpecialProperties: SpecialProperties{
			DocID: docID,
			Attachments: AttachmentsMeta{
				"hello.txt": map[string]interface{}{
					"data": "aGVsbG8gd29ybGQ=",
				},
			},
		},
	}
	rev1ID, _, err := db.Put(&incomingDocument)
	require.NoError(t, err)
	assert.Equal(t, "1-12ff9ce1dd501524378fe092ce9aee8f", rev1ID)

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
	_, _, err = db.PutExistingRevWithBody(docID, rev2Body, []string{"2-abc", rev1ID}, true)
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

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	bucket := testBucket.Bucket

	context, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")

	// Test creating & updating a document:
	log.Printf("Create rev 1...")
	rev1input := IncomingDocument{
		BodyBytes: []byte(`{}`),
		SpecialProperties: SpecialProperties{
			DocID: "doc1",
			Attachments: AttachmentsMeta{
				"hello.txt": map[string]interface{}{
					"data": "aGVsbG8gd29ybGQ=",
				},
				"bye.txt": map[string]interface{}{
					"data": "Z29vZGJ5ZSBjcnVlbCB3b3JsZA==",
				},
			},
		},
	}
	rev1id, _, err := db.Put(&rev1input)
	require.NoError(t, err, "Couldn't create document")

	log.Printf("Retrieve doc...")
	rev1output := `{"_attachments":{"bye.txt":{"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA==","digest":"sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=","length":19,"revpos":1},"hello.txt":{"data":"aGVsbG8gd29ybGQ=","digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1}},"_id":"doc1","_rev":"1-ca9ad22802b66f662ff171f226211d5c"}`
	gotbody, err := db.GetRev1xBody("doc1", "", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev1output, tojson(gotbody))

	log.Printf("Create rev 2...")
	rev2input := IncomingDocument{
		BodyBytes: []byte(`{}`),
		SpecialProperties: SpecialProperties{
			DocID: "doc1",
			RevID: rev1id,
			Attachments: AttachmentsMeta{
				"hello.txt": map[string]interface{}{
					"stub":   true,
					"revpos": 1,
				},
				"bye.txt": map[string]interface{}{
					"data": "YnllLXlh",
				},
			},
		},
	}
	rev2id, _, err := db.Put(&rev2input)
	require.NoError(t, err, "Couldn't update document")
	assert.Equal(t, "2-5d3308aae9930225ed7f6614cf115366", rev2id)

	log.Printf("Retrieve doc...")
	rev2output := `{"_attachments":{"bye.txt":{"data":"YnllLXlh","digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=","length":6,"revpos":2},"hello.txt":{"data":"aGVsbG8gd29ybGQ=","digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1}},"_id":"doc1","_rev":"2-5d3308aae9930225ed7f6614cf115366"}`
	gotbody, err = db.GetRev1xBody("doc1", "", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2output, tojson(gotbody))

	log.Printf("Retrieve doc with atts_since...")
	rev2Aoutput := `{"_attachments":{"bye.txt":{"data":"YnllLXlh","digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=","length":6,"revpos":2},"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1,"stub":true}},"_id":"doc1","_rev":"2-5d3308aae9930225ed7f6614cf115366"}`
	gotbody, err = db.GetRev1xBody("doc1", "", false, []string{"1-ca9ad22802b66f662ff171f226211d5c", "1-foo", "993-bar"})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2Aoutput, tojson(gotbody))

	log.Printf("Create rev 3...")
	rev3input := IncomingDocument{
		BodyBytes: []byte(`{}`),
		SpecialProperties: SpecialProperties{
			DocID: "doc1",
			RevID: rev2id,
			Attachments: AttachmentsMeta{
				"bye.txt": map[string]interface{}{
					"stub":   true,
					"revpos": 2,
				},
			},
		},
	}
	rev3id, _, err := db.Put(&rev3input)
	assert.NoError(t, err, "Couldn't update document")
	assert.Equal(t, "3-aa3ff4ca3aad12e1479b65cb1e602676", rev3id)

	log.Printf("Retrieve doc...")
	rev3output := `{"_attachments":{"bye.txt":{"data":"YnllLXlh","digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=","length":6,"revpos":2}},"_id":"doc1","_rev":"3-aa3ff4ca3aad12e1479b65cb1e602676"}`
	gotbody, err = db.GetRev1xBody("doc1", "", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev3output, tojson(gotbody))

	log.Printf("Expire body of rev 1, then add a child...") // test fix of #498
	err = db.Bucket.Delete(oldRevisionKey("doc1", rev1id))
	assert.NoError(t, err, "Couldn't compact old revision")
	rev2Bstr := `{"_attachments": {"bye.txt": {"stub":true,"revpos":1,"digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}, "_rev": "2-f000"}`
	var body2B Body
	err = json.Unmarshal([]byte(rev2Bstr), &body2B)
	assert.NoError(t, err, "bad JSON")
	_, _, err = db.PutExistingRevWithBody("doc1", body2B, []string{"2-f000", rev1id}, false)
	assert.NoError(t, err, "Couldn't update document")
}

func TestAttachmentForRejectedDocument(t *testing.T) {

	testBucket := testBucket(t)
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

	doc := IncomingDocument{
		BodyBytes: []byte(`{}`),
		SpecialProperties: SpecialProperties{
			DocID: "doc1",
			Attachments: AttachmentsMeta{
				"hello.txt": map[string]interface{}{
					"data": "aGVsbG8gd29ybGQ=",
				},
			},
		},
	}

	_, _, err = db.Put(&doc)
	require.NoError(t, err, "Got error on put doc")
	db.Bucket.Dump()

	// Attempt to retrieve the attachment doc
	_, _, err = db.Bucket.GetRaw(base.AttPrefix + "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=")
	assert.Error(t, err, "Expect error when attempting to retrieve attachment document after doc is rejected.")

}

func TestAttachmentRetrievalUsingRevCache(t *testing.T) {

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	bucket := testBucket.Bucket

	context, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")

	// Test creating & updating a document:
	doc := IncomingDocument{
		BodyBytes: []byte(`{}`),
		SpecialProperties: SpecialProperties{
			DocID: "doc1",
			Attachments: AttachmentsMeta{
				"hello.txt": map[string]interface{}{
					"data": "aGVsbG8gd29ybGQ=",
				},
				"bye.txt": map[string]interface{}{
					"data": "Z29vZGJ5ZSBjcnVlbCB3b3JsZA==",
				},
			},
		},
	}
	_, _, err = db.Put(&doc)
	require.NoError(t, err, "Couldn't create document")

	initCount, countErr := base.GetExpvarAsInt("syncGateway_db", "document_gets")
	assert.NoError(t, countErr, "Couldn't retrieve document_gets expvar")
	rev1output := `{"_attachments":{"bye.txt":{"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA==","digest":"sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=","length":19,"revpos":1},"hello.txt":{"data":"aGVsbG8gd29ybGQ=","digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1}},"_id":"doc1","_rev":"1-ca9ad22802b66f662ff171f226211d5c"}`
	gotbody, err := db.GetRev1xBody("doc1", "1-ca9ad22802b66f662ff171f226211d5c", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev1output, tojson(gotbody))

	getCount, countErr := base.GetExpvarAsInt("syncGateway_db", "document_gets")
	assert.NoError(t, countErr, "Couldn't retrieve document_gets expvar")
	assert.Equal(t, initCount, getCount)

	// Repeat, validate no additional get operations
	gotbody, err = db.GetRev1xBody("doc1", "1-ca9ad22802b66f662ff171f226211d5c", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev1output, tojson(gotbody))
	getCount, countErr = base.GetExpvarAsInt("syncGateway_db", "document_gets")
	assert.NoError(t, countErr, "Couldn't retrieve document_gets expvar")
	assert.Equal(t, initCount, getCount)
}
