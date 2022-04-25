//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
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
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	deltasEnabled := base.IsEnterpriseEdition()
	xattrsEnabled := base.TestUseXattrs()

	bucket := base.GetTestBucket(t)
	dbCtx, err := NewDatabaseContext(
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
	defer dbCtx.Close()
	db, err := CreateDatabase(dbCtx)
	assert.NoError(t, err, "Couldn't create database 'db'")

	docID := "doc1"
	var rev1Body Body
	rev1Data := `{"test": true, "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`
	require.NoError(t, base.JSONUnmarshal([]byte(rev1Data), &rev1Body))
	rev1ID, _, err := db.Put(docID, rev1Body)
	require.NoError(t, err)
	assert.Equal(t, "1-12ff9ce1dd501524378fe092ce9aee8f", rev1ID)

	rev1OldBody, err := db.getOldRevisionJSON(base.TestCtx(t), docID, rev1ID)
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
	_, _, err = db.PutExistingRevWithBody(docID, rev2Body, []string{"2-abc", rev1ID}, true)
	require.NoError(t, err)
	rev2ID := "2-abc"

	// now in any case - we'll have rev 1 backed up
	rev1OldBody, err = db.getOldRevisionJSON(base.TestCtx(t), docID, rev1ID)
	require.NoError(t, err)
	assert.Contains(t, string(rev1OldBody), "hello.txt")

	// and rev 2 should be present only for the xattrs and deltas case
	rev2OldBody, err := db.getOldRevisionJSON(base.TestCtx(t), docID, rev2ID)
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

	bucket := base.GetTestBucket(t)
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
	gotbody, err := db.Get1xRevBody("doc1", "", false, []string{})
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
	gotbody, err = db.Get1xRevBody("doc1", "", false, []string{})
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
	gotbody, err = db.Get1xRevBody("doc1", "", false, []string{"1-ca9ad22802b66f662ff171f226211d5c", "1-foo", "993-bar"})
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
	gotbody, err = db.Get1xRevBody("doc1", "", false, []string{})
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
	_, _, err = db.PutExistingRevWithBody("doc1", body2B, []string{"2-f000", rev1id}, false)
	assert.NoError(t, err, "Couldn't update document")
}

func TestAttachmentForRejectedDocument(t *testing.T) {

	bucket := base.GetTestBucket(t)
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
	require.NoError(t, base.JSONUnmarshal([]byte(docBody), &body))
	_, _, err = db.Put("doc1", unjson(docBody))
	log.Printf("Got error on put doc:%v", err)
	db.Bucket.Dump()

	// Attempt to retrieve the attachment doc
	_, _, err = db.Bucket.GetRaw(base.AttPrefix + "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=")

	assert.True(t, err != nil, "Expect error when attempting to retrieve attachment document after doc is rejected.")

}

func TestAttachmentRetrievalUsingRevCache(t *testing.T) {

	bucket := base.GetTestBucket(t)
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
	gotbody, err := db.Get1xRevBody("doc1", "1-ca9ad22802b66f662ff171f226211d5c", false, []string{})
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
	gotbody, err = db.Get1xRevBody("doc1", "1-ca9ad22802b66f662ff171f226211d5c", false, []string{})
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
			_, _, err := db.PutExistingRevWithBody("doc1", rev2Body, []string{"2-abc", rev1ID}, true)
			require.NoError(t, err)

			log.Printf("Done creating rev 2 for key %s", key)
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		UpdateCallback: writeUpdateCallback,
	}

	db = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close()

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
	_, _, err = db.PutExistingRevWithBody("doc1", rev3Body, []string{"3-abc", "2-abc", rev1ID}, true)
	require.NoError(t, err)

	log.Printf("rev 3 done")

	// 4. Get the document, check attachments
	finalDoc, err := db.Get1xBody("doc1")
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
			_, _, err := db.PutExistingRevWithBody("doc1", rev2Body, []string{"2-abc", rev1ID}, true)
			require.NoError(t, err)

			log.Printf("Done creating rev 2 for key %s", key)
		}
	}

	// Use leaky bucket to inject callback in query invocation
	queryCallbackConfig := base.LeakyBucketConfig{
		UpdateCallback: writeUpdateCallback,
	}

	db = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), queryCallbackConfig)
	defer db.Close()

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
	_, _, err = db.PutExistingRevWithBody("doc1", rev3Body, []string{"3-abc", "2-abc", rev1ID}, true)
	require.NoError(t, err)

	log.Printf("rev 3 done")

	// 4. Get the document, check attachments
	finalDoc, err := db.Get1xBody("doc1")
	log.Printf("get doc attachments: %v", finalDoc)

	attachments := GetBodyAttachments(finalDoc)
	assert.True(t, attachments != nil, "_attachments should be present in GET response")
	attachment, attachmentOk := attachments["hello.txt"].(map[string]interface{})
	assert.True(t, attachmentOk, "hello.txt attachment should be present in GET response")
	_, digestOk := attachment["digest"]
	assert.True(t, digestOk, "digest should be set for attachment hello.txt in GET response")

}

func TestForEachStubAttachmentErrors(t *testing.T) {
	bucket := base.GetTestBucket(t)
	context, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")

	var body Body
	callbackCount := 0
	callback := func(name string, digest string, knownData []byte, meta map[string]interface{}) ([]byte, error) {
		callbackCount++
		return []byte("data"), nil
	}

	// Call ForEachStubAttachment with invalid attachment; simulates the error scenario.
	doc := `{"_attachments": "No Attachment"}`
	docID := "foo"
	existingDigests := make(map[string]string)
	assert.NoError(t, base.JSONUnmarshal([]byte(doc), &body))
	err = db.ForEachStubAttachment(body, 1, docID, existingDigests, callback)
	assert.Error(t, err, "It should throw 400 Invalid _attachments")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	// Call ForEachStubAttachment with invalid attachment; simulates the error scenario.
	doc = `{"_attachments": {"image1.jpeg": "", "image2.jpeg": ""}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(doc), &body))
	err = db.ForEachStubAttachment(body, 1, docID, existingDigests, callback)
	assert.Error(t, err, "It should throw 400 Invalid _attachments")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	// Check whether the attachment iteration is getting skipped if revpos < minRevpos
	callbackCount = 0
	doc = `{"_attachments": {"image.jpg": {"stub":true, "revpos":1}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(doc), &body))
	err = db.ForEachStubAttachment(body, 2, docID, existingDigests, callback)
	assert.NoError(t, err, "It should not throw any error")
	assert.Equal(t, 0, callbackCount)

	// Verify the attachment is getting skipped if digest is in existing set
	callbackCount = 0
	existingDigests["image.jpg"] = "e1a1"
	doc = `{"_attachments": {"image.jpg": {"stub":true, "revpos":2, "digest":"e1a1"}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(doc), &body))
	err = db.ForEachStubAttachment(body, 2, docID, existingDigests, callback)
	assert.NoError(t, err, "It should not throw any error")
	assert.Equal(t, 0, callbackCount)

	// Verify the attachment is not getting skipped if digest doesn't match existing set
	callbackCount = 0
	doc = `{"_attachments": {"image.jpg": {"stub":true, "revpos":2, "digest":"e1a2"}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(doc), &body))
	err = db.ForEachStubAttachment(body, 2, docID, existingDigests, callback)
	assert.NoError(t, err, "It should not throw any error")
	assert.Equal(t, 1, callbackCount)

	// Check whether the attachment iteration is getting skipped if there is no revpos.
	doc = `{"_attachments": {"image.jpg": {"stub":true}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(doc), &body))
	err = db.ForEachStubAttachment(body, 2, docID, existingDigests, callback)
	assert.NoError(t, err, "It should not throw any error")

	// Should throw invalid attachment error is the digest is not valid string or empty.
	doc = `{"_attachments": {"image.jpg": {"stub":true, "revpos":1, "digest":true}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(doc), &body))
	err = db.ForEachStubAttachment(body, 1, docID, existingDigests, callback)
	assert.Error(t, err, "It should throw 400 Invalid attachments")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	// Call ForEachStubAttachment with some bad digest value. Internally it should throw a missing
	// document error and invoke the callback function.
	doc = `{"_attachments": {"image.jpg": {"stub":true, "revpos":1, "digest":"9304cdd066efa64f78387e9cc9240a70527271bc"}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(doc), &body))
	err = db.ForEachStubAttachment(body, 1, docID, existingDigests, callback)
	assert.NoError(t, err, "It should not throw any error")

	// Simulate an error from the callback function; it should return the same error from ForEachStubAttachment.
	doc = `{"_attachments": {"image.jpg": {"stub":true, "revpos":1, "digest":"9304cdd066efa64f78387e9cc9240a70527271bc"}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(doc), &body))
	callback = func(name string, digest string, knownData []byte, meta map[string]interface{}) ([]byte, error) {
		return nil, errors.New("Can't work with this digest value!")
	}
	err = db.ForEachStubAttachment(body, 1, docID, existingDigests, callback)
	assert.Error(t, err, "It should throw the actual error")
	assert.Contains(t, err.Error(), "Can't work with this digest value!")
}

func TestGenerateProofOfAttachment(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	attData := []byte(`hello world`)

	nonce, proof1, err := GenerateProofOfAttachment(attData)
	require.NoError(t, err)
	assert.True(t, len(nonce) >= 20, "nonce should be at least 20 bytes")
	assert.NotEmpty(t, proof1)
	assert.True(t, strings.HasPrefix(proof1, "sha1-"))

	proof2 := ProveAttachment(attData, nonce)
	assert.NotEmpty(t, proof1, "")
	assert.True(t, strings.HasPrefix(proof1, "sha1-"))

	assert.Equal(t, proof1, proof2, "GenerateProofOfAttachment and ProveAttachment produced different proofs.")
}

func TestDecodeAttachmentError(t *testing.T) {
	attr, err := DecodeAttachment(make([]int, 1))
	assert.Nil(t, attr, "Attachment of data (type []int) should not get decoded.")
	assert.Error(t, err, "It should throw 400 invalid attachment data (type []int)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	attr, err = DecodeAttachment(make([]float64, 1))
	assert.Nil(t, attr, "Attachment of data (type []float64) should not get decoded.")
	assert.Error(t, err, "It should throw 400 invalid attachment data (type []float64)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	attr, err = DecodeAttachment(make([]string, 1))
	assert.Nil(t, attr, "Attachment of data (type []string) should not get decoded.")
	assert.Error(t, err, "It should throw 400 invalid attachment data (type []string)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	attr, err = DecodeAttachment(make(map[string]int, 1))
	assert.Nil(t, attr, "Attachment of data (type map[string]int) should not get decoded.")
	assert.Error(t, err, "It should throw 400 invalid attachment data (type map[string]int)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	attr, err = DecodeAttachment(make(map[string]float64, 1))
	assert.Nil(t, attr, "Attachment of data (type map[string]float64) should not get decoded.")
	assert.Error(t, err, "It should throw 400 invalid attachment data (type map[string]float64)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	attr, err = DecodeAttachment(make(map[string]string, 1))
	assert.Error(t, err, "should throw 400 invalid attachment data (type map[string]float64)")
	assert.Error(t, err, "It 400 invalid attachment data (type map[string]string)")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	book := struct {
		author string
		title  string
		price  float64
	}{author: "William Shakespeare", title: "Hamlet", price: 7.99}
	attr, err = DecodeAttachment(book)
	assert.Nil(t, attr)
	assert.Error(t, err, "It should throw 400 invalid attachment data (type struct { author string; title string; price float64 })")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))
}

func TestSetAttachment(t *testing.T) {
	bucket := base.GetTestBucket(t)
	context, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
	assert.NoError(t, err, "The database context should be created for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "The database 'db' should be created")

	// Set attachment with a valid attachment
	att := `{"att1.txt": {"data": "YXR0MS50eHQ="}}}`
	key := Sha1DigestKey([]byte(att))
	err = db.setAttachment(key, []byte(att))
	assert.NoError(t, err, "Attachment should be saved in db and key should be returned")
	attBytes, err := db.GetAttachment(key)
	assert.NoError(t, err, "Attachment should be retrieved from the database")
	assert.Equal(t, att, string(attBytes))
}

func TestRetrieveAncestorAttachments(t *testing.T) {
	bucket := base.GetTestBucket(t)
	context, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
	assert.NoError(t, err, "The database context should be created for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "The database 'db' should be created")

	var body Body
	db.RevsLimit = 3

	// Create document (rev 1)
	text := `{"key": "value", "version": "1a"}`
	assert.NoError(t, base.JSONUnmarshal([]byte(text), &body))
	doc, revID, err := db.PutExistingRevWithBody("doc", body, []string{"1-a"}, false)
	assert.NoError(t, err, "Couldn't create document")
	log.Printf("doc: %v", doc)

	// Add an attachment to a document (rev 2)
	text = `{"key": "value", "version": "2a", "_attachments": {"att1.txt": {"data": "YXR0MS50eHQ="}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(text), &body))
	body[BodyRev] = revID
	doc, _, err = db.PutExistingRevWithBody("doc", body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "Couldn't create document")
	log.Printf("doc: %v", doc)

	text = `{"key": "value", "version": "3a", "_attachments": {"att1.txt": {"stub":true,"revpos":2,"digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(text), &body))
	body[BodyRev] = revID
	doc, _, err = db.PutExistingRevWithBody("doc", body, []string{"3-a", "2-a"}, false)
	assert.NoError(t, err, "Couldn't create document")
	log.Printf("doc: %v", doc)

	text = `{"key": "value", "version": "4a", "_attachments": {"att1.txt": {"stub":true,"revpos":2,"digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(text), &body))
	body[BodyRev] = revID
	doc, _, err = db.PutExistingRevWithBody("doc", body, []string{"4-a", "3-a"}, false)
	assert.NoError(t, err, "Couldn't create document")
	log.Printf("doc: %v", doc)

	text = `{"key": "value", "version": "5a", "_attachments": {"att1.txt": {"stub":true,"revpos":2,"digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(text), &body))
	body[BodyRev] = revID
	doc, _, err = db.PutExistingRevWithBody("doc", body, []string{"5-a", "4-a"}, false)
	assert.NoError(t, err, "Couldn't create document")
	log.Printf("doc: %v", doc)

	text = `{"key": "value", "version": "6a", "_attachments": {"att1.txt": {"stub":true,"revpos":2,"digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(text), &body))
	body[BodyRev] = revID
	doc, _, err = db.PutExistingRevWithBody("doc", body, []string{"6-a", "5-a"}, false)
	assert.NoError(t, err, "Couldn't create document")
	log.Printf("doc: %v", doc)

	text = `{"key": "value", "version": "3b", "type": "pruned"}`
	assert.NoError(t, base.JSONUnmarshal([]byte(text), &body))
	body[BodyRev] = revID
	doc, _, err = db.PutExistingRevWithBody("doc", body, []string{"3-b", "2-a"}, false)
	assert.NoError(t, err, "Couldn't create document")
	log.Printf("doc: %v", doc)

	text = `{"key": "value", "version": "3b", "_attachments": {"att1.txt": {"stub":true,"revpos":2,"digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(text), &body))
	body[BodyRev] = revID
	doc, _, err = db.PutExistingRevWithBody("doc", body, []string{"3-b", "2-a"}, false)
	assert.NoError(t, err, "Couldn't create document")
	log.Printf("doc: %v", doc)
}

func TestStoreAttachments(t *testing.T) {
	bucket := base.GetTestBucket(t)
	context, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
	assert.NoError(t, err, "The database context should be created for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "The database 'db' should be created")
	var revBody Body

	// Simulate Invalid _attachments scenario; try to put a document with bad
	// attachment metadata. It should throw "Invalid _attachments" error.
	revText := `{"key1": "value1", "_attachments": {"att1.txt": "YXR0MS50eHQ="}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(revText), &revBody))
	revId, doc, err := db.Put("doc1", revBody)
	assert.Empty(t, revId, "The revId should be empty since the request has attachment")
	assert.Empty(t, doc, "The doc should be empty since the request has attachment")
	assert.Error(t, err, "It should throw 400 Invalid _attachments")
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))

	// Simulate illegal base64 data error while storing attachments in Couchbase database.
	revText = `{"key1": "value1", "_attachments": {"att1.txt": {"data": "%$^&**"}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(revText), &revBody))
	revId, doc, err = db.Put("doc1", revBody)
	assert.Empty(t, revId, "The revId should be empty since illegal base64 data in attachment")
	assert.Empty(t, doc, "The doc should be empty since illegal base64 data in attachment")
	assert.Error(t, err, "It should throw illegal base64 data at input byte 0 error")
	assert.Contains(t, err.Error(), "illegal base64 data at input byte 0")

	// Simulate a valid scenario; attachment contains data, so store it in the database.
	// Include content type, encoding, attachment length  in attachment metadata.
	revText = `{"key1": "value1", "_attachments": {"att1.txt": {"data": "YXR0MS50eHQ=", "content_type": "text/plain", "encoding": "utf-8", "length": 12}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(revText), &revBody))
	revId, doc, err = db.Put("doc1", revBody)
	assert.NoError(t, err, "Couldn't update document")
	assert.NotEmpty(t, revId, "Document revision id should be generated")
	require.NotNil(t, doc)
	assert.NotEmpty(t, doc.Attachments, "Attachment metadata should be populated")
	attachment := doc.Attachments["att1.txt"].(map[string]interface{})
	assert.Equal(t, "text/plain", attachment["content_type"])
	assert.Equal(t, "sha1-crv3IVNxp3JXbP6bizTHt3GB3O0=", attachment["digest"])
	assert.Equal(t, 8, attachment["encoded_length"])
	assert.Equal(t, "utf-8", attachment["encoding"])
	assert.Equal(t, float64(12), attachment["length"])
	assert.NotEmpty(t, attachment["revpos"])
	assert.True(t, attachment["stub"].(bool))

	// Simulate a valid scenario; attachment contains data, so store it in the database.
	// Include content type, encoding in attachment metadata but no attachment length.
	revText = `{"key1": "value1", "_attachments": {"att1.txt": {"data": "YXR0MS50eHQ=", "content_type": "text/plain", "encoding": "utf-8"}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(revText), &revBody))
	revId, doc, err = db.Put("doc2", revBody)
	assert.NoError(t, err, "Couldn't update document")
	assert.NotEmpty(t, revId, "Document revision id should be generated")
	require.NotNil(t, doc)
	assert.NotEmpty(t, doc.Attachments, "Attachment metadata should be populated")
	attachment = doc.Attachments["att1.txt"].(map[string]interface{})
	assert.Equal(t, "text/plain", attachment["content_type"])
	assert.Equal(t, "sha1-crv3IVNxp3JXbP6bizTHt3GB3O0=", attachment["digest"])
	assert.Equal(t, 8, attachment["encoded_length"])
	assert.Equal(t, "utf-8", attachment["encoding"])
	assert.Empty(t, attachment["length"], "Attachment length should be empty")
	assert.NotEmpty(t, attachment["revpos"])
	assert.True(t, attachment["stub"].(bool))

	// Simulate a valid scenario; attachment contains data, so store it in the database.
	// Include only content type in attachment metadata but no encoding and attachment length.
	// Attachment length should be calculated automatically in this case.
	revText = `{"key1": "value1", "_attachments": {"att1.txt": {"data": "YXR0MS50eHQ=", "content_type": "text/plain"}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(revText), &revBody))
	revId, doc, err = db.Put("doc3", revBody)
	assert.NoError(t, err, "Couldn't update document")
	assert.NotEmpty(t, revId, "Document revision id should be generated")
	require.NotNil(t, doc)
	assert.NotEmpty(t, doc.Attachments, "Attachment metadata should be populated")
	attachment = doc.Attachments["att1.txt"].(map[string]interface{})
	assert.Equal(t, "text/plain", attachment["content_type"])
	assert.Equal(t, "sha1-crv3IVNxp3JXbP6bizTHt3GB3O0=", attachment["digest"])
	assert.Empty(t, attachment["encoded_length"])
	assert.Equal(t, int(8), attachment["length"])
	assert.Empty(t, attachment["encoding"])
	assert.NotEmpty(t, attachment["revpos"])
	assert.True(t, attachment["stub"].(bool))

	// Simulate error scenario for attachment without data; stub is not provided; If the data is
	// empty in attachment, the attachment must be a stub that repeats a parent attachment.
	revText = `{"key1": "value1", "_attachments": {"att1.txt": {"revpos": 2}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(revText), &revBody))
	revId, doc, err = db.Put("doc4", revBody)
	assert.Empty(t, revId, "The revId should be empty since stub is not included in attachment")
	assert.Empty(t, doc, "The doc should be empty since stub is not included in attachment")
	assert.Error(t, err, "It should throw 400 Missing data of attachment error")
	assert.Contains(t, err.Error(), "400 Missing data of attachment")

	// Simulate error scenario for attachment without data; revpos is not provided; If the data is
	// empty in attachment, the attachment must be a stub that repeats a parent attachment.
	revText = `{"key2": "value1", "_attachments": {"att1.txt": {"stub": true}}}`
	assert.NoError(t, base.JSONUnmarshal([]byte(revText), &revBody))
	revId, doc, err = db.Put("doc5", revBody)
	assert.Empty(t, revId, "The revId should be empty since revpos is not included in attachment")
	assert.Empty(t, doc, "The doc should be empty since revpos is not included in attachment")
	assert.Error(t, err, "It should throw 400 Missing/invalid revpos in stub attachment error")
	assert.Contains(t, err.Error(), "400 Missing/invalid revpos in stub attachment")
}

// TestMigrateBodyAttachments will set up a document with an attachment in pre-2.5 metadata format, and test various upgrade scenarios.
func TestMigrateBodyAttachments(t *testing.T) {

	if base.TestUseXattrs() && base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket when using xattrs")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	const docKey = "TestAttachmentMigrate"

	setupFn := func(t *testing.T) (db *Database, teardownFn func()) {
		bucket := base.GetTestBucket(t)
		dbCtx, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{
			EnableXattr: base.TestUseXattrs(),
		})

		assert.NoError(t, err, "The database context should be created for database 'db'")
		db, err = CreateDatabase(dbCtx)
		assert.NoError(t, err, "The database 'db' should be created")

		// Put a document with hello.txt attachment, to write attachment to the bucket
		rev1input := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`
		var body Body
		assert.NoError(t, base.JSONUnmarshal([]byte(rev1input), &body))
		_, _, err = db.Put("doc1", body)
		assert.NoError(t, err, "Couldn't create document")

		gotbody, err := db.Get1xRevBody("doc1", "", false, []string{})
		assert.NoError(t, err, "Couldn't get document")
		atts, ok := gotbody[BodyAttachments].(AttachmentsMeta)
		assert.True(t, ok)

		hello, ok := atts["hello.txt"].(map[string]interface{})
		assert.True(t, ok)

		helloData, ok := hello["data"].([]byte)
		assert.True(t, ok)

		assert.Equal(t, "hello world", string(helloData))
		assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
		assert.Equal(t, 11, hello["length"])
		assert.Equal(t, 1, hello["revpos"])

		// Create 2.1 style document - metadata in sync data, _attachments in body, referencing attachment created above
		bodyPre25 := `{
  "test":true,
  "_attachments": {
    "hello.txt": {
      "digest": "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
      "length": 11,
      "revpos": 1,
      "stub": true
    }
  }
}`

		syncData := `{
  "rev": "3-a",
  "sequence": 4,
  "recent_sequences": [
    4
  ],
  "history": {
    "revs": [
      "2-a",
      "3-a",
      "1-a"
    ],
    "parents": [
      2,
      0,
      -1
    ],
    "channels": [
      [
        "ABC"
      ],
      [
        "ABC"
      ],
      [
        "ABC"
      ]
    ]
  },
  "channels": {
    "ABC": null
  }
}`

		var bodyVal map[string]interface{}
		var xattrVal map[string]interface{}
		err = base.JSONUnmarshal([]byte(bodyPre25), &bodyVal)
		assert.NoError(t, err)
		err = base.JSONUnmarshal([]byte(syncData), &xattrVal)
		assert.NoError(t, err)

		if base.TestUseXattrs() {
			_, err = bucket.WriteCasWithXattr(docKey, base.SyncXattrName, 0, 0, nil, bodyVal, xattrVal)
			assert.NoError(t, err)
		} else {
			newBody, err := base.InjectJSONPropertiesFromBytes([]byte(bodyPre25), base.KVPairBytes{Key: base.SyncPropertyName, Val: []byte(syncData)})
			assert.NoError(t, err)
			ok, err := bucket.Add(docKey, 0, newBody)
			assert.NoError(t, err)
			assert.True(t, ok)
		}

		// Fetch the raw doc sync data from the bucket to make sure we didn't store pre-2.5 attachments in syncData.
		docSyncData, err := db.GetDocSyncData(base.TestCtx(t), docKey)
		assert.NoError(t, err)
		assert.Empty(t, docSyncData.Attachments)

		return db, func() {
			dbCtx.Close()
		}
	}

	// Reading the active rev of a doc containing pre 2.5 meta. Make sure the rev ID is not changed, and the metadata is appearing in syncData.
	t.Run("2.1 meta, read active rev", func(t *testing.T) {
		db, teardownFn := setupFn(t)
		defer teardownFn()

		rev, err := db.GetRev(docKey, "", true, nil)
		require.NoError(t, err)

		// latest rev was 3-a when we called GetActive, make sure that hasn't changed.
		gen, _ := ParseRevID(rev.RevID)
		assert.Equal(t, 3, gen)

		// read-only operations don't "upgrade" the metadata, but it should still transform it on-demand before returning.
		// Only write operations (tested down below) actually write an upgrade back to the bucket.
		assert.Len(t, rev.Attachments, 1, "expecting 1 attachment returned in rev")

		// _attachments shouldn't be present in the body at this point.
		// It will be stamped in for 1.x clients that require it further up the stack.
		body1, err := rev.Body()
		require.NoError(t, err)
		bodyAtts, foundBodyAtts := body1[BodyAttachments]
		assert.False(t, foundBodyAtts, "not expecting '_attachments' in body but found them: %v", bodyAtts)

		// Fetch the raw doc sync data from the bucket to see if this read-only op unintentionally persisted the migrated meta.
		syncData, err := db.GetDocSyncData(base.TestCtx(t), docKey)
		assert.NoError(t, err)
		assert.Empty(t, syncData.Attachments)
	})

	// Reading a non-active revision shouldn't perform an upgrade, but should transform the metadata in memory for the returned rev.
	t.Run("2.1 meta, read non-active rev", func(t *testing.T) {
		db, teardownFn := setupFn(t)
		defer teardownFn()

		rev, err := db.GetRev(docKey, "3-a", true, nil)
		require.NoError(t, err)

		// latest rev was 3-a when we called Get, make sure that hasn't changed.
		gen, _ := ParseRevID(rev.RevID)
		assert.Equal(t, 3, gen)

		// read-only operations don't "upgrade" the metadata, but it should still transform it on-demand before returning.
		// Only write operations (tested down below) actually write an upgrade back to the bucket.
		assert.Len(t, rev.Attachments, 1, "expecting 1 attachment returned in rev")

		// _attachments shouldn't be present in the body at this point.
		// It will be stamped in for 1.x clients that require it further up the stack.
		body1, err := rev.Body()
		require.NoError(t, err)
		bodyAtts, foundBodyAtts := body1[BodyAttachments]
		assert.False(t, foundBodyAtts, "not expecting '_attachments' in body but found them: %v", bodyAtts)

		// Fetch the raw doc sync data from the bucket to see if this read-only op unintentionally persisted the migrated meta.
		syncData, err := db.GetDocSyncData(base.TestCtx(t), docKey)
		assert.NoError(t, err)
		assert.Empty(t, syncData.Attachments)
	})

	// Writing a new rev should migrate the metadata and write that upgrade back to the bucket.
	t.Run("2.1 meta, write new rev", func(t *testing.T) {
		db, teardownFn := setupFn(t)
		defer teardownFn()

		// Update the doc with a the same body as rev 3-a, and make sure attachments are migrated.
		newBody := Body{
			"test":  true,
			BodyRev: "3-a",
			BodyAttachments: map[string]interface{}{
				"hello.txt": map[string]interface{}{
					"digest": "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
					"length": 11,
					"revpos": 1,
					"stub":   true,
				},
			},
		}
		newRevID, _, err := db.Put(docKey, newBody)
		require.NoError(t, err)

		gen, _ := ParseRevID(newRevID)
		assert.Equal(t, 4, gen)

		// Verify attachments are in syncData returned from GetRev
		rev, err := db.GetRev(docKey, newRevID, true, nil)
		require.NoError(t, err)

		assert.Len(t, rev.Attachments, 1, "expecting 1 attachment returned in rev")

		// _attachments shouldn't be present in the body at this point.
		// It will be stamped in for 1.x clients that require it further up the stack.
		body1, err := rev.Body()
		require.NoError(t, err)
		bodyAtts, foundBodyAtts := body1[BodyAttachments]
		assert.False(t, foundBodyAtts, "not expecting '_attachments' in body but found them: %v", bodyAtts)

		// Fetch the raw doc sync data from the bucket to make sure we actually moved attachments on write.
		syncData, err := db.GetDocSyncData(base.TestCtx(t), docKey)
		assert.NoError(t, err)
		assert.Len(t, syncData.Attachments, 1)
	})

	// Adding a new attachment should migrate existing attachments, without losing any.
	t.Run("2.1 meta, add new attachment", func(t *testing.T) {
		db, teardownFn := setupFn(t)
		defer teardownFn()

		rev, err := db.GetRev(docKey, "3-a", true, nil)
		require.NoError(t, err)

		// read-only in-memory transformation should've been applied here, so we can append the new attachment to the existing rev.Attachments map when we write.
		require.Len(t, rev.Attachments, 1)

		// Fetch the raw doc sync data from the bucket to see if this read-only op unintentionally persisted the migrated meta.
		syncData, err := db.GetDocSyncData(base.TestCtx(t), docKey)
		assert.NoError(t, err)
		assert.Empty(t, syncData.Attachments)

		byeTxtData, err := base64.StdEncoding.DecodeString("Z29vZGJ5ZSBjcnVlbCB3b3JsZA==")
		require.NoError(t, err)

		newAtts := rev.Attachments.ShallowCopy()
		newAtts["bye.txt"] = map[string]interface{}{
			"content_type": "text/plain",
			"stub":         false,
			"data":         byeTxtData,
		}

		// update the doc with a copy of the previous doc body
		newBody, err := rev.MutableBody()
		require.NoError(t, err)
		newBody[BodyRev] = "3-a"
		newBody[BodyAttachments] = newAtts
		newRevID, _, err := db.Put(docKey, newBody)
		require.NoError(t, err)

		gen, _ := ParseRevID(newRevID)
		assert.Equal(t, 4, gen)

		// Verify attachments are now present via GetRev
		rev, err = db.GetRev(docKey, newRevID, true, nil)
		require.NoError(t, err)

		gen, _ = ParseRevID(rev.RevID)
		assert.Equal(t, 4, gen)

		assert.Len(t, rev.Attachments, 2, "expecting 2 attachments returned in rev")

		// _attachments shouldn't be present in the body at this point.
		// It will be stamped in for 1.x clients that require it further up the stack.
		body1, err := rev.Body()
		require.NoError(t, err)
		bodyAtts, foundBodyAtts := body1[BodyAttachments]
		assert.False(t, foundBodyAtts, "not expecting '_attachments' in body but found them: %v", bodyAtts)

		// Fetch the raw doc sync data from the bucket to make sure we actually moved attachments on write.
		syncData, err = db.GetDocSyncData(base.TestCtx(t), docKey)
		assert.NoError(t, err)
		assert.Len(t, syncData.Attachments, 2)
	})
}

// TestMigrateBodyAttachmentsMerge will set up a document with attachments in both pre-2.5 and post-2.5 metadata, making sure that both attachments are preserved.
func TestMigrateBodyAttachmentsMerge(t *testing.T) {

	if base.TestUseXattrs() && base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket when using xattrs")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	const docKey = "TestAttachmentMigrate"

	bucket := base.GetTestBucket(t)
	dbCtx, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{
		EnableXattr: base.TestUseXattrs(),
	})
	require.NoError(t, err, "The database context should be created for database 'db'")
	defer dbCtx.Close()

	db, err := CreateDatabase(dbCtx)
	require.NoError(t, err, "The database 'db' should be created")

	// Put a document 2 attachments, to write attachment to the bucket
	rev1input := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="},"bye.txt": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA=="}}}`
	var body Body
	assert.NoError(t, base.JSONUnmarshal([]byte(rev1input), &body))
	_, _, err = db.Put("doc1", body)
	assert.NoError(t, err, "Couldn't create document")

	gotbody, err := db.Get1xRevBody("doc1", "", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	atts, ok := gotbody[BodyAttachments].(AttachmentsMeta)
	assert.True(t, ok)

	hello, ok := atts["hello.txt"].(map[string]interface{})
	assert.True(t, ok)

	helloData, ok := hello["data"].([]byte)
	assert.True(t, ok)

	assert.Equal(t, "hello world", string(helloData))
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, 11, hello["length"])
	assert.Equal(t, 1, hello["revpos"])

	bye, ok := atts["bye.txt"].(map[string]interface{})
	assert.True(t, ok)

	byeData, ok := bye["data"].([]byte)
	assert.True(t, ok)

	assert.Equal(t, "goodbye cruel world", string(byeData))
	assert.Equal(t, "sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=", bye["digest"])
	assert.Equal(t, 19, bye["length"])
	assert.Equal(t, 1, bye["revpos"])

	// Create 2.1 style document - metadata in sync data, _attachments in body, referencing attachments created above
	bodyPre25 := `{
  "test":true,
  "_attachments": {
    "hello.txt": {
      "digest": "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
      "length": 11,
      "revpos": 1,
      "stub": true
    }
  }
}`

	syncData := `{
  "rev": "3-a",
  "sequence": 4,
  "recent_sequences": [
    4
  ],
  "history": {
    "revs": [
      "2-a",
      "3-a",
      "1-a"
    ],
    "parents": [
      2,
      0,
      -1
    ],
    "channels": [
      [
        "ABC"
      ],
      [
        "ABC"
      ],
      [
        "ABC"
      ]
    ]
  },
  "channels": {
    "ABC": null
  },
  "attachments": {
    "bye.txt": {
      "digest": "sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=",
      "length": 19,
      "revpos": 1,
      "stub": true
    }
  }
}`

	var bodyVal map[string]interface{}
	var xattrVal map[string]interface{}
	err = base.JSONUnmarshal([]byte(bodyPre25), &bodyVal)
	assert.NoError(t, err)
	err = base.JSONUnmarshal([]byte(syncData), &xattrVal)
	assert.NoError(t, err)

	if base.TestUseXattrs() {
		_, err = bucket.WriteCasWithXattr(docKey, base.SyncXattrName, 0, 0, nil, bodyVal, xattrVal)
		assert.NoError(t, err)
	} else {
		newBody, err := base.InjectJSONPropertiesFromBytes([]byte(bodyPre25), base.KVPairBytes{Key: base.SyncPropertyName, Val: []byte(syncData)})
		assert.NoError(t, err)
		ok, err := bucket.Add(docKey, 0, newBody)
		assert.NoError(t, err)
		assert.True(t, ok)
	}

	// Fetch the raw doc sync data from the bucket to make sure we didn't store pre-2.5 attachments in syncData.
	docSyncData, err := db.GetDocSyncData(base.TestCtx(t), docKey)
	assert.NoError(t, err)
	assert.Len(t, docSyncData.Attachments, 1)
	_, ok = docSyncData.Attachments["hello.txt"]
	assert.False(t, ok)
	_, ok = docSyncData.Attachments["bye.txt"]
	assert.True(t, ok)

	rev, err := db.GetRev(docKey, "3-a", true, nil)
	require.NoError(t, err)

	// read-only in-memory transformation should've been applied here, both attachments should be present in rev.Attachments
	assert.Len(t, rev.Attachments, 2)
	_, ok = rev.Attachments["hello.txt"]
	assert.True(t, ok)
	_, ok = rev.Attachments["bye.txt"]
	assert.True(t, ok)

	// _attachments shouldn't be present in the body at this point.
	// It will be stamped in for 1.x clients that require it further up the stack.
	body1, err := rev.Body()
	require.NoError(t, err)
	bodyAtts, foundBodyAtts := body1[BodyAttachments]
	assert.False(t, foundBodyAtts, "not expecting '_attachments' in body but found them: %v", bodyAtts)

	// Fetch the raw doc sync data from the bucket to see if this read-only op unintentionally persisted the migrated meta.
	docSyncData, err = db.GetDocSyncData(base.TestCtx(t), docKey)
	assert.NoError(t, err)
	_, ok = docSyncData.Attachments["hello.txt"]
	assert.False(t, ok)
	_, ok = docSyncData.Attachments["bye.txt"]
	assert.True(t, ok)
}

// TestMigrateBodyAttachmentsMergeConflicting will set up a document with the same attachment name in both pre-2.5 and post-2.5 metadata, making sure that the metadata with the most recent revpos is chosen.
func TestMigrateBodyAttachmentsMergeConflicting(t *testing.T) {

	if base.TestUseXattrs() && base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server bucket when using xattrs")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	const docKey = "TestAttachmentMigrate"

	bucket := base.GetTestBucket(t)
	context, err := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{
		EnableXattr: base.TestUseXattrs(),
	})
	require.NoError(t, err, "The database context should be created for database 'db'")
	defer context.Close()

	db, err := CreateDatabase(context)
	require.NoError(t, err, "The database 'db' should be created")

	// Put a document with 3 attachments, to write attachments to the bucket
	rev1input := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="},"bye.txt": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA=="},"new.txt": {"data":"bmV3IGRhdGE="}}}`
	var body Body
	assert.NoError(t, base.JSONUnmarshal([]byte(rev1input), &body))
	_, _, err = db.Put("doc1", body)
	assert.NoError(t, err, "Couldn't create document")

	gotbody, err := db.Get1xRevBody("doc1", "", false, []string{})
	assert.NoError(t, err, "Couldn't get document")
	atts, ok := gotbody[BodyAttachments].(AttachmentsMeta)
	assert.True(t, ok)

	hello, ok := atts["hello.txt"].(map[string]interface{})
	assert.True(t, ok)

	helloData, ok := hello["data"].([]byte)
	assert.True(t, ok)

	assert.Equal(t, "hello world", string(helloData))
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, 11, hello["length"])
	assert.Equal(t, 1, hello["revpos"])

	bye, ok := atts["bye.txt"].(map[string]interface{})
	assert.True(t, ok)

	byeData, ok := bye["data"].([]byte)
	assert.True(t, ok)

	assert.Equal(t, "goodbye cruel world", string(byeData))
	assert.Equal(t, "sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=", bye["digest"])
	assert.Equal(t, 19, bye["length"])
	assert.Equal(t, 1, bye["revpos"])

	new, ok := atts["new.txt"].(map[string]interface{})
	assert.True(t, ok)

	newData, ok := new["data"].([]byte)
	assert.True(t, ok)

	assert.Equal(t, "new data", string(newData))
	assert.Equal(t, "sha1-AZffsEGpPp0Zn4jv1xFA8ydfxp0=", new["digest"])
	assert.Equal(t, 8, new["length"])
	assert.Equal(t, 1, new["revpos"])

	// Create 2.1 style document - metadata in sync data, _attachments in body, referencing attachments created above
	bodyPre25 := `{
  "test":true,
  "_attachments": {
    "hello.txt": {
      "digest": "sha1-AZffsEGpPp0Zn4jv1xFA8ydfxp0=",
      "length": 8,
      "revpos": 2,
      "stub": true
    },
    "bye.txt": {
      "digest": "sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=",
      "length": 19,
      "revpos": 1,
      "stub": true
    }
  }
}`

	syncData := `{
  "rev": "3-a",
  "sequence": 4,
  "recent_sequences": [
    4
  ],
  "history": {
    "revs": [
      "2-a",
      "3-a",
      "1-a"
    ],
    "parents": [
      2,
      0,
      -1
    ],
    "channels": [
      [
        "ABC"
      ],
      [
        "ABC"
      ],
      [
        "ABC"
      ]
    ]
  },
  "channels": {
    "ABC": null
  },
  "attachments": {
    "hello.txt": {
      "digest": "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
      "length": 11,
      "revpos": 1,
      "stub": true
    },
    "bye.txt": {
      "digest": "sha1-AZffsEGpPp0Zn4jv1xFA8ydfxp0=",
      "length": 8,
      "revpos": 2,
      "stub": true
    }
  }
}`

	var bodyVal map[string]interface{}
	var xattrVal map[string]interface{}
	err = base.JSONUnmarshal([]byte(bodyPre25), &bodyVal)
	assert.NoError(t, err)
	err = base.JSONUnmarshal([]byte(syncData), &xattrVal)
	assert.NoError(t, err)

	if base.TestUseXattrs() {
		_, err = bucket.WriteCasWithXattr(docKey, base.SyncXattrName, 0, 0, nil, bodyVal, xattrVal)
		assert.NoError(t, err)
	} else {
		newBody, err := base.InjectJSONPropertiesFromBytes([]byte(bodyPre25), base.KVPairBytes{Key: base.SyncPropertyName, Val: []byte(syncData)})
		assert.NoError(t, err)
		ok, err := bucket.Add(docKey, 0, newBody)
		assert.NoError(t, err)
		assert.True(t, ok)
	}

	rev, err := db.GetRev(docKey, "3-a", true, nil)
	require.NoError(t, err)

	// read-only in-memory transformation should've been applied here, both attachments should be present in rev.Attachments
	assert.Len(t, rev.Attachments, 2)

	// see if we got new.txt's meta for both attachments (because of the higher revpos)
	helloAtt, ok := rev.Attachments["hello.txt"]
	assert.True(t, ok)
	helloAttMeta, ok := helloAtt.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "sha1-AZffsEGpPp0Zn4jv1xFA8ydfxp0=", helloAttMeta["digest"])

	byeAtt, ok := rev.Attachments["bye.txt"]
	assert.True(t, ok)
	byeAttMeta, ok := byeAtt.(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "sha1-AZffsEGpPp0Zn4jv1xFA8ydfxp0=", byeAttMeta["digest"])

	// _attachments shouldn't be present in the body at this point.
	// It will be stamped in for 1.x clients that require it further up the stack.
	body1, err := rev.Body()
	require.NoError(t, err)
	bodyAtts, foundBodyAtts := body1[BodyAttachments]
	assert.False(t, foundBodyAtts, "not expecting '_attachments' in body but found them: %v", bodyAtts)
}

func TestAllowedAttachments(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeySync)

	var tests = []struct {
		name              string
		inputBlipProtocol string
		inputAttVersion   int
	}{
		{"TestAllowedAttachmentsCBMobile2AttVer1", BlipCBMobileReplicationV2, AttVersion1},
		{"TestAllowedAttachmentsCBMobile2AttVer2", BlipCBMobileReplicationV2, AttVersion2},
		{"TestAllowedAttachmentsCBMobile3AttVer1", BlipCBMobileReplicationV3, AttVersion1},
		{"TestAllowedAttachmentsCBMobile3AttVer2", BlipCBMobileReplicationV3, AttVersion2},
	}

	isAllowedAttachment := func(ctx *BlipSyncContext, key string) bool {
		return ctx.allowedAttachment(key).counter > 0
	}

	requireIsAttachmentAllowedTrue := func(t *testing.T, ctx *BlipSyncContext, docID string, meta []AttachmentStorageMeta, activeSubprotocol string) {
		docIDForAllowedAttKey := docID
		if activeSubprotocol == BlipCBMobileReplicationV2 {
			docIDForAllowedAttKey = ""
		}
		for _, att := range meta {
			key := allowedAttachmentKey(docIDForAllowedAttKey, att.digest, activeSubprotocol)
			require.True(t, isAllowedAttachment(ctx, key))
		}
	}

	requireIsAttachmentAllowedFalse := func(t *testing.T, ctx *BlipSyncContext, docID string, meta []AttachmentStorageMeta, activeSubprotocol string) {
		docIDForAllowedAttKey := docID
		if activeSubprotocol == BlipCBMobileReplicationV2 {
			docIDForAllowedAttKey = ""
		}
		for _, att := range meta {
			key := allowedAttachmentKey(docIDForAllowedAttKey, att.digest, activeSubprotocol)
			require.False(t, isAllowedAttachment(ctx, key))
		}
	}

	// Single document associated with multiple attachments of different digests.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &BlipSyncContext{}
			meta := []AttachmentStorageMeta{
				{digest: "digest1", version: tt.inputAttVersion},
				{digest: "digest2", version: tt.inputAttVersion},
			}
			docID := "doc1"

			ctx.addAllowedAttachments(docID, meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedTrue(t, ctx, docID, meta, tt.inputBlipProtocol)

			ctx.removeAllowedAttachments(docID, meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedFalse(t, ctx, docID, meta, tt.inputBlipProtocol)
		})
	}

	// Single document associated with multiple attachments with matching digests.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &BlipSyncContext{}
			meta := []AttachmentStorageMeta{
				{digest: "digest1", version: tt.inputAttVersion},
				{digest: "digest1", version: tt.inputAttVersion},
			}
			docID := "doc1"

			ctx.addAllowedAttachments(docID, meta, tt.inputBlipProtocol)
			key := allowedAttachmentKey(docID, meta[0].digest, tt.inputBlipProtocol)
			require.True(t, isAllowedAttachment(ctx, key))

			ctx.removeAllowedAttachments(docID, meta, tt.inputBlipProtocol)
			require.False(t, isAllowedAttachment(ctx, key))
		})
	}

	// Multiple documents associated with multiple attachments with different digests.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &BlipSyncContext{}
			meta := []AttachmentStorageMeta{
				{digest: "digest1", version: tt.inputAttVersion},
				{digest: "digest2", version: tt.inputAttVersion},
			}

			docID1 := "doc1"
			ctx.addAllowedAttachments(docID1, meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedTrue(t, ctx, docID1, meta, tt.inputBlipProtocol)

			docID2 := "doc2"
			ctx.addAllowedAttachments(docID2, meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedTrue(t, ctx, docID2, meta, tt.inputBlipProtocol)

			ctx.removeAllowedAttachments(docID1, meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedTrue(t, ctx, docID2, meta, tt.inputBlipProtocol)
			if tt.inputBlipProtocol == BlipCBMobileReplicationV2 {
				requireIsAttachmentAllowedTrue(t, ctx, docID1, meta, tt.inputBlipProtocol)
			} else if tt.inputBlipProtocol == BlipCBMobileReplicationV3 {
				requireIsAttachmentAllowedFalse(t, ctx, docID1, meta, tt.inputBlipProtocol)
			}

			ctx.removeAllowedAttachments(docID2, meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedFalse(t, ctx, docID2, meta, tt.inputBlipProtocol)
		})
	}

	// Multiple documents associated with multiple attachments with matching digests.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &BlipSyncContext{}
			meta := []AttachmentStorageMeta{
				{digest: "digest1", version: tt.inputAttVersion},
				{digest: "digest1", version: tt.inputAttVersion},
			}

			docID1 := "doc1"
			ctx.addAllowedAttachments(docID1, meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedTrue(t, ctx, docID1, meta, tt.inputBlipProtocol)

			docID2 := "doc2"
			ctx.addAllowedAttachments(docID2, meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedTrue(t, ctx, docID2, meta, tt.inputBlipProtocol)

			ctx.removeAllowedAttachments(docID1, meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedTrue(t, ctx, docID2, meta, tt.inputBlipProtocol)
			if tt.inputBlipProtocol == BlipCBMobileReplicationV2 {
				requireIsAttachmentAllowedTrue(t, ctx, docID1, meta, tt.inputBlipProtocol)
			} else if tt.inputBlipProtocol == BlipCBMobileReplicationV3 {
				requireIsAttachmentAllowedFalse(t, ctx, docID1, meta, tt.inputBlipProtocol)
			}

			ctx.removeAllowedAttachments(docID2, meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedFalse(t, ctx, docID2, meta, tt.inputBlipProtocol)
		})
	}

	// Two document with different attachments.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := &BlipSyncContext{}

			docID1 := "doc1"
			att1Meta := []AttachmentStorageMeta{{digest: "att1", version: tt.inputAttVersion}}
			ctx.addAllowedAttachments(docID1, att1Meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedTrue(t, ctx, docID1, att1Meta, tt.inputBlipProtocol)

			docID2 := "doc2"
			att2Meta := []AttachmentStorageMeta{{digest: "att2", version: tt.inputAttVersion}}
			ctx.addAllowedAttachments(docID2, att2Meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedTrue(t, ctx, docID2, att2Meta, tt.inputBlipProtocol)

			ctx.removeAllowedAttachments(docID1, att1Meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedFalse(t, ctx, docID1, att1Meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedTrue(t, ctx, docID2, att2Meta, tt.inputBlipProtocol)

			ctx.removeAllowedAttachments(docID2, att2Meta, tt.inputBlipProtocol)
			requireIsAttachmentAllowedFalse(t, ctx, docID2, att2Meta, tt.inputBlipProtocol)
		})
	}
}

func TestGetAttVersion(t *testing.T) {
	var tests = []struct {
		name                    string
		inputAttVersion         interface{}
		expectedValidAttVersion bool
		expectedAttVersion      int
	}{
		{"int attachment version", AttVersion2, true, AttVersion2},
		{"float64 attachment version", float64(AttVersion2), true, AttVersion2},
		{"invalid json.Number attachment version", json.Number("foo"), false, 0},
		{"valid json.Number attachment version", json.Number(strconv.Itoa(AttVersion2)), true, AttVersion2},
		{"invaid string attachment version", strconv.Itoa(AttVersion2), false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := map[string]interface{}{"ver": tt.inputAttVersion}
			version, ok := GetAttachmentVersion(meta)
			assert.Equal(t, tt.expectedValidAttVersion, ok)
			assert.Equal(t, tt.expectedAttVersion, version)
		})
	}
}

func TestLargeAttachments(t *testing.T) {
	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")

	normalAttachment := make([]byte, 15*1024*1024)   // permissible size
	oversizeAttachment := make([]byte, 25*1024*1024) // memcached would send an E2BIG
	hugeAttachment := make([]byte, 35*1024*1024)     // memcached would abruptly close our connection
	_, _ = rand.Read(normalAttachment)
	_, _ = rand.Read(oversizeAttachment)
	_, _ = rand.Read(hugeAttachment)

	_, _, err = db.Put("testdoc", Body{
		"_attachments": AttachmentsMeta{
			"foo.bin": map[string]interface{}{
				"data": base64.StdEncoding.EncodeToString(normalAttachment),
			},
		},
	})
	require.NoError(t, err, "Couldn't create appropriately sized attachment")

	_, _, err = db.Put("bigdoc", Body{
		"_attachments": AttachmentsMeta{
			"foo.bin": map[string]interface{}{
				"data": base64.StdEncoding.EncodeToString(oversizeAttachment),
			},
		},
	})
	var httpErr *base.HTTPError
	require.ErrorAs(t, err, &httpErr, "Created doc with oversize attachment")
	require.Equal(t, http.StatusRequestEntityTooLarge, httpErr.Status)

	_, _, err = db.Put("hugedoc", Body{
		"_attachments": AttachmentsMeta{
			"foo.bin": map[string]interface{}{
				"data": base64.StdEncoding.EncodeToString(hugeAttachment),
			},
		},
	})
	require.ErrorAs(t, err, &httpErr, "Created doc with huge attachment")
	require.Equal(t, http.StatusRequestEntityTooLarge, httpErr.Status)
}
