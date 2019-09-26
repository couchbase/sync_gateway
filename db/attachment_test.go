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
	err := base.JSONUnmarshal([]byte(j), &body)
	if err != nil {
		panic(fmt.Sprintf("Invalid JSON: %v", err))
	}
	return body
}

func tojson(obj interface{}) string {
	j, _ := base.JSONMarshal(obj)
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
	require.NoError(t, base.JSONUnmarshal([]byte(rev1Data), &rev1Body))
	rev1ID, _, err := db.Put(docID, rev1Body)
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
	require.NoError(t, base.JSONUnmarshal([]byte(rev2Data), &rev2Body))
	_, err = db.PutExistingRevWithBody(docID, rev2Body, []string{"2-abc", rev1ID}, true)
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
	rev1input := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="},
                                    "bye.txt": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA=="}}}`
	var body Body
	assert.NoError(t, base.JSONUnmarshal([]byte(rev1input), &body))
	revid, _, err := db.Put("doc1", body)
	rev1id := revid
	assert.NoError(t, err, "Couldn't create document")

	log.Printf("Retrieve doc...")
	gotbody, err := db.GetRev1xBody("doc1", "", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	atts := gotbody[BodyAttachments].(AttachmentsMeta)

	hello := atts["hello.txt"].(map[string]interface{})
	assert.Equal(t, "hello world", string(hello["data"].([]byte)))
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, 11, hello["length"])
	assert.Equal(t, 1, hello["revpos"])

	bye := atts["bye.txt"].(map[string]interface{})
	assert.Equal(t, "goodbye cruel world", string(bye["data"].([]byte)))
	assert.Equal(t, "sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=", bye["digest"])
	assert.Equal(t, 19, bye["length"])
	assert.Equal(t, 1, bye["revpos"])

	log.Printf("Create rev 2...")
	rev2str := `{"_attachments": {"hello.txt": {"stub":true, "revpos":1}, "bye.txt": {"data": "YnllLXlh"}}}`
	var body2 Body
	assert.NoError(t, base.JSONUnmarshal([]byte(rev2str), &body2))
	body2[BodyRev] = revid
	revid, _, err = db.Put("doc1", body2)
	assert.NoError(t, err, "Couldn't update document")
	assert.Equal(t, "2-5d3308aae9930225ed7f6614cf115366", revid)

	log.Printf("Retrieve doc...")
	gotbody, err = db.GetRev1xBody("doc1", "", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	atts = gotbody[BodyAttachments].(AttachmentsMeta)

	hello = atts["hello.txt"].(map[string]interface{})
	assert.Equal(t, "hello world", string(hello["data"].([]byte)))
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(1), hello["revpos"])

	bye = atts["bye.txt"].(map[string]interface{})
	assert.Equal(t, "bye-ya", string(bye["data"].([]byte)))
	assert.Equal(t, "sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=", bye["digest"])
	assert.Equal(t, 6, bye["length"])
	assert.Equal(t, 2, bye["revpos"])

	log.Printf("Retrieve doc with atts_since...")
	gotbody, err = db.GetRev1xBody("doc1", "", false, []string{"1-ca9ad22802b66f662ff171f226211d5c", "1-foo", "993-bar"})
	assert.NoError(t, err, "Couldn't get document")
	atts = gotbody[BodyAttachments].(AttachmentsMeta)

	hello = atts["hello.txt"].(map[string]interface{})
	assert.Nil(t, hello["data"])

	bye = atts["bye.txt"].(map[string]interface{})
	require.NotNil(t, bye["data"])
	assert.Equal(t, "bye-ya", string(bye["data"].([]byte)))
	require.NotNil(t, bye["digest"])
	assert.Equal(t, "sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=", bye["digest"])
	require.NotNil(t, bye["length"])
	assert.Equal(t, 6, bye["length"])
	require.NotNil(t, bye["revpos"])
	assert.Equal(t, 2, bye["revpos"])

	log.Printf("Create rev 3...")
	rev3str := `{"_attachments": {"bye.txt": {"stub":true,"revpos":2}}}`
	var body3 Body
	assert.NoError(t, base.JSONUnmarshal([]byte(rev3str), &body3))
	body3[BodyRev] = revid
	revid, _, err = db.Put("doc1", body3)
	assert.NoError(t, err, "Couldn't update document")
	assert.Equal(t, "3-aa3ff4ca3aad12e1479b65cb1e602676", revid)

	log.Printf("Retrieve doc...")
	gotbody, err = db.GetRev1xBody("doc1", "", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	atts = gotbody[BodyAttachments].(AttachmentsMeta)

	assert.Nil(t, atts["hello.txt"])

	bye = atts["bye.txt"].(map[string]interface{})
	assert.Equal(t, "bye-ya", string(bye["data"].([]byte)))
	assert.Equal(t, "sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=", bye["digest"])
	assert.Equal(t, float64(6), bye["length"])
	assert.Equal(t, float64(2), bye["revpos"])

	log.Printf("Expire body of rev 1, then add a child...") // test fix of #498
	err = db.Bucket.Delete(oldRevisionKey("doc1", rev1id))
	assert.NoError(t, err, "Couldn't compact old revision")
	rev2Bstr := `{"_attachments": {"bye.txt": {"stub":true,"revpos":1,"digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}, "_rev": "2-f000"}`
	var body2B Body
	assert.NoError(t, base.JSONUnmarshal([]byte(rev2Bstr), &body2B))
	_, err = db.PutExistingRevWithBody("doc1", body2B, []string{"2-f000", rev1id}, false)
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

	docBody := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`
	var body Body
	base.JSONUnmarshal([]byte(docBody), &body)
	_, _, err = db.Put("doc1", unjson(docBody))
	log.Printf("Got error on put doc:%v", err)
	db.Bucket.Dump()

	// Attempt to retrieve the attachment doc
	_, _, err = db.Bucket.GetRaw(base.AttPrefix + "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=")

	assert.True(t, err != nil, "Expect error when attempting to retrieve attachment document after doc is rejected.")

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
	rev1input := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="},
                                    "bye.txt": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA=="}}}`
	_, _, err = db.Put("doc1", unjson(rev1input))
	assert.NoError(t, err, "Couldn't create document")

	initCount, countErr := base.GetExpvarAsInt("syncGateway_db", "document_gets")
	assert.NoError(t, countErr, "Couldn't retrieve document_gets expvar")
	gotbody, err := db.GetRev1xBody("doc1", "1-ca9ad22802b66f662ff171f226211d5c", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	atts := gotbody[BodyAttachments].(AttachmentsMeta)

	hello := atts["hello.txt"].(map[string]interface{})
	assert.Equal(t, "hello world", string(hello["data"].([]byte)))
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, 11, hello["length"])
	assert.Equal(t, 1, hello["revpos"])

	bye := atts["bye.txt"].(map[string]interface{})
	assert.Equal(t, "goodbye cruel world", string(bye["data"].([]byte)))
	assert.Equal(t, "sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=", bye["digest"])
	assert.Equal(t, 19, bye["length"])
	assert.Equal(t, 1, bye["revpos"])

	getCount, countErr := base.GetExpvarAsInt("syncGateway_db", "document_gets")
	assert.NoError(t, countErr, "Couldn't retrieve document_gets expvar")
	assert.Equal(t, initCount, getCount)

	// Repeat, validate no additional get operations
	gotbody, err = db.GetRev1xBody("doc1", "1-ca9ad22802b66f662ff171f226211d5c", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	atts = gotbody[BodyAttachments].(AttachmentsMeta)

	hello = atts["hello.txt"].(map[string]interface{})
	assert.Equal(t, "hello world", string(hello["data"].([]byte)))
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, 11, hello["length"])
	assert.Equal(t, 1, hello["revpos"])

	bye = atts["bye.txt"].(map[string]interface{})
	assert.Equal(t, "goodbye cruel world", string(bye["data"].([]byte)))
	assert.Equal(t, "sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=", bye["digest"])
	assert.Equal(t, 19, bye["length"])
	assert.Equal(t, 1, bye["revpos"])

	getCount, countErr = base.GetExpvarAsInt("syncGateway_db", "document_gets")
	assert.NoError(t, countErr, "Couldn't retrieve document_gets expvar")
	assert.Equal(t, initCount, getCount)
}

func TestAttachmentCASRetryAfterNewAttachment(t *testing.T) {

	var db *Database
	var enableCallback bool
	var rev1ID string

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			log.Printf("Creating rev 2 for key %s", key)
			var rev2Body Body
			rev2Data := `{"prop1":"value2", "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`
			require.NoError(t, base.JSONUnmarshal([]byte(rev2Data), &rev2Body))
			_, err := db.PutExistingRevWithBody("doc1", rev2Body, []string{"2-abc", rev1ID}, true)
			require.NoError(t, err)

			log.Printf("Done creating rev 2 for key %s", key)
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		WriteUpdateCallback: writeUpdateCallback,
	}

	db = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer tearDownTestDB(t, db)

	// Test creating & updating a document:

	// 1. Create a document with no attachment
	rev1Json := `{"prop1":"value1"}`
	rev1ID, _, err := db.Put("doc1", unjson(rev1Json))
	assert.NoError(t, err, "Couldn't create document")

	// 2. Create rev 2 with new attachment - done in callback
	enableCallback = true

	// 3. Create rev 3 with new attachment to same attachment
	log.Printf("starting create of rev 3")
	var rev3Body Body
	rev3Data := `{"prop1":"value3", "_attachments": {"hello.txt": {"revpos":2,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
	require.NoError(t, base.JSONUnmarshal([]byte(rev3Data), &rev3Body))
	_, err = db.PutExistingRevWithBody("doc1", rev3Body, []string{"3-abc", "2-abc", rev1ID}, true)
	require.NoError(t, err)

	log.Printf("rev 3 done")

	// 4. Get the document, check attachments
	finalDoc, err := db.Get("doc1")
	attachments := GetBodyAttachments(finalDoc)
	assert.True(t, attachments != nil, "_attachments should be present in GET response")
	attachment, attachmentOk := attachments["hello.txt"].(map[string]interface{})
	assert.True(t, attachmentOk, "hello.txt attachment should be present in GET response")
	_, digestOk := attachment["digest"]
	assert.True(t, digestOk, "digest should be set for attachment hello.txt in GET response")

}

func TestAttachmentCASRetryDuringNewAttachment(t *testing.T) {

	var db *Database
	var enableCallback bool
	var rev1ID string

	writeUpdateCallback := func(key string) {
		if enableCallback {
			enableCallback = false
			log.Printf("Creating rev 2 for key %s", key)
			var rev2Body Body
			rev2Data := `{"prop1":"value2"}`
			require.NoError(t, base.JSONUnmarshal([]byte(rev2Data), &rev2Body))
			_, err := db.PutExistingRevWithBody("doc1", rev2Body, []string{"2-abc", rev1ID}, true)
			require.NoError(t, err)

			log.Printf("Done creating rev 2 for key %s", key)
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		WriteUpdateCallback: writeUpdateCallback,
	}

	db = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer tearDownTestDB(t, db)

	// Test creating & updating a document:

	// 1. Create a document with no attachment
	rev1Json := `{"prop1":"value1"}`
	rev1ID, _, err := db.Put("doc1", unjson(rev1Json))
	assert.NoError(t, err, "Couldn't create document")

	// 2. Create rev 2 with no attachment
	enableCallback = true

	// 3. Create rev 3 with new attachment
	log.Printf("starting create of rev 3")
	var rev3Body Body
	rev3Data := `{"prop1":"value3", "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`
	require.NoError(t, base.JSONUnmarshal([]byte(rev3Data), &rev3Body))
	_, err = db.PutExistingRevWithBody("doc1", rev3Body, []string{"3-abc", "2-abc", rev1ID}, true)
	require.NoError(t, err)

	log.Printf("rev 3 done")

	// 4. Get the document, check attachments
	finalDoc, err := db.Get("doc1")
	log.Printf("get doc attachments: %v", finalDoc)

	attachments := GetBodyAttachments(finalDoc)
	assert.True(t, attachments != nil, "_attachments should be present in GET response")
	attachment, attachmentOk := attachments["hello.txt"].(map[string]interface{})
	assert.True(t, attachmentOk, "hello.txt attachment should be present in GET response")
	_, digestOk := attachment["digest"]
	assert.True(t, digestOk, "digest should be set for attachment hello.txt in GET response")

}
