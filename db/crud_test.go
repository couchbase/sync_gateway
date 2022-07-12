/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type treeDoc struct {
	Meta treeMeta `json:"_sync"`
}

type treeMeta struct {
	RevTree revTreeList `json:"history"`
}

// Retrieve the raw doc from the bucket, and unmarshal sync history as revTreeList, to validate low-level  storage
func getRevTreeList(bucket base.Bucket, key string, useXattrs bool) (revTreeList, error) {
	switch useXattrs {
	case true:
		var rawDoc, rawXattr []byte
		_, getErr := bucket.GetWithXattr(key, base.SyncXattrName, "", &rawDoc, &rawXattr, nil)
		if getErr != nil {
			return revTreeList{}, getErr
		}

		var treeMeta treeMeta
		err := base.JSONUnmarshal(rawXattr, &treeMeta)
		if err != nil {
			return revTreeList{}, err
		}
		return treeMeta.RevTree, nil

	default:
		rawDoc, _, err := bucket.GetRaw(key)
		if err != nil {
			return revTreeList{}, err
		}
		var doc treeDoc
		err = base.JSONUnmarshal(rawDoc, &doc)
		return doc.Meta.RevTree, err
	}

}

// TestRevisionCacheLoad
// Tests simple retrieval of rev not resident in the cache
func TestRevisionCacheLoad(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	db := setupTestDBWithViewsEnabled(t)
	defer db.Close()

	base.TestExternalRevStorage = true

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Flush the cache
	db.FlushRevisionCacheForTest()

	// Retrieve the document:
	log.Printf("Retrieve doc 1-a...")
	_, err = db.Get1xRevBody("doc1", "1-a", false, nil)
	assert.NoError(t, err, "Couldn't get document")

	docRev, err := db.GetRev("doc1", "1-a", false, nil)
	assert.NoError(t, err)
	assert.Equal(t, "1-a", docRev.RevID)

	// Validate that mutations to the body don't affect the revcache value
	_, err = base.InjectJSONProperties(docRev.BodyBytes, base.KVPair{Key: "modified", Val: "property"})
	assert.NoError(t, err)

	docRevAgain, err := db.GetRev("doc1", "1-a", false, nil)
	assert.NoError(t, err)
	assert.Equal(t, "1-a", docRevAgain.RevID)

	body, err = docRevAgain.Body()
	assert.NoError(t, err)
	_, ok := body["modified"]
	assert.False(t, ok)
}

func TestHasAttachmentsFlag(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	db := setupTestDB(t)
	defer db.Close()

	base.TestExternalRevStorage = true
	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a with a large body")
	rev2a_body := unjson(`{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`)
	rev2a_body["key1"] = prop_1000_bytes
	rev2a_body["version"] = "2a"
	doc, newRev, err := db.PutExistingRevWithBody("doc1", rev2a_body, []string{"2-a", "1-a"}, false)
	rev2a_body[BodyId] = doc.ID
	rev2a_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotDoc, err := db.GetDocument(base.TestCtx(t), "doc1", DocUnmarshalSync)
	assert.NoError(t, err)
	require.Contains(t, gotDoc.Attachments, "hello.txt")
	attachmentData, ok := gotDoc.Attachments["hello.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", attachmentData["digest"])
	assert.Equal(t, float64(11), attachmentData["length"])
	assert.Equal(t, float64(2), attachmentData["revpos"])
	assert.True(t, attachmentData["stub"].(bool))
	assert.Equal(t, float64(2), attachmentData["ver"])

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	log.Printf("Create rev 2-b with a large body")
	rev2b_body := unjson(`{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`)
	rev2b_body["key1"] = prop_1000_bytes
	rev2b_body["version"] = "2b"
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev2b_body, []string{"2-b", "1-a"}, false)
	rev2b_body[BodyId] = doc.ID
	rev2b_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-b")
	gotDoc, err = db.GetDocument(base.TestCtx(t), "doc1", DocUnmarshalSync)
	assert.NoError(t, err)
	require.Contains(t, gotDoc.Attachments, "hello.txt")
	attachmentData, ok = gotDoc.Attachments["hello.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", attachmentData["digest"])
	assert.Equal(t, float64(11), attachmentData["length"])
	assert.Equal(t, float64(2), attachmentData["revpos"])
	assert.True(t, attachmentData["stub"].(bool))
	assert.Equal(t, float64(2), attachmentData["ver"])

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err := getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Equal(t, 0, len(revTree.BodyMap))
	assert.Equal(t, 1, len(revTree.BodyKeyMap))
	assert.Equal(t, 1, len(revTree.HasAttachments))
}

func TestHasAttachmentsFlagForLegacyAttachments(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
		t.Skip("Test only works with a Couchbase server and Xattrs")
	}
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	db := setupTestDB(t)
	defer db.Close()

	base.TestExternalRevStorage = true
	prop_1000_bytes := base.CreateProperty(1000)

	rawDocWithAttachmentAndSyncMeta := func() []byte {
		return []byte(`{
   "_sync": {
      "rev": "2-a",
      "sequence": 2,
      "recent_sequences": [
         2
      ],
      "history": {
         "revs": [
            "1-a",
            "2-a"
         ],
         "parents": [
            -1,
             0
         ],
         "channels": [
            null
         ]
      },
      "cas": "",
      "attachments": {
         "hi.txt": {
            "revpos": 2,
            "content_type": "text/plain",
            "length": 2,
            "stub": true,
            "digest": "sha1-witfkXg0JglCjW9RssWvTAveakI="
         }
      },
      "time_saved": "2021-09-01T17:33:03.054227821Z"
   },
  "key": "value"
}`)
	}

	createDocWithLegacyAttachment := func(docID string, rawDoc []byte, attKey string, attBody []byte) {
		// Write attachment directly to the bucket.
		_, err := db.Bucket.Add(attKey, 0, attBody)
		require.NoError(t, err)

		body := Body{}
		err = body.Unmarshal(rawDoc)
		require.NoError(t, err, "Error unmarshalling body")

		// Write raw document to the bucket.
		_, err = db.Bucket.Add(docID, 0, rawDoc)
		require.NoError(t, err)

		// Get the existing bucket doc
		_, existingBucketDoc, err := db.GetDocWithXattr(docID, DocUnmarshalAll)
		require.NoError(t, err)

		// Migrate document metadata from document body to system xattr.
		_, _, err = db.migrateMetadata(docID, body, existingBucketDoc, nil)
		require.NoError(t, err)
	}

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a with legacy attachment.
	// 1-a
	//  |
	// 2-a
	docID := "doc1"
	attBody := []byte(`hi`)
	digest := Sha1DigestKey(attBody)
	attKey := MakeAttachmentKey(AttVersion1, docID, digest)
	rawDoc := rawDocWithAttachmentAndSyncMeta()
	createDocWithLegacyAttachment(docID, rawDoc, attKey, attBody)

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, Body{"_id": "doc1", "_rev": "1-a", "key1": "value1", "version": "1a"}, gotbody)

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	log.Printf("Create rev 2-b with a large body")
	// rev2b_body := unjson(`{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`)
	rev2b_body := Body{}
	rev2b_body["key1"] = prop_1000_bytes
	rev2b_body["version"] = "2b"
	doc, newRev, err := db.PutExistingRevWithBody("doc1", rev2b_body, []string{"2-b", "1-a"}, false)
	rev2b_body[BodyId] = doc.ID
	rev2b_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-b")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2b_body, gotbody)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err := getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Equal(t, 0, len(revTree.HasAttachments))
}

// TestRevisionStorageConflictAndTombstones
// Tests permutations of inline and external storage of conflicts and tombstones
func TestRevisionStorageConflictAndTombstones(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	db := setupTestDB(t)
	defer db.Close()

	base.TestExternalRevStorage = true

	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a with a large body")
	rev2a_body := Body{}
	rev2a_body["key1"] = prop_1000_bytes
	rev2a_body["version"] = "2a"
	doc, newRev, err := db.PutExistingRevWithBody("doc1", rev2a_body, []string{"2-a", "1-a"}, false)
	rev2a_body[BodyId] = doc.ID
	rev2a_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2a_body, gotbody)

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	log.Printf("Create rev 2-b with a large body")
	rev2b_body := Body{}
	rev2b_body["key1"] = prop_1000_bytes
	rev2b_body["version"] = "2b"
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev2b_body, []string{"2-b", "1-a"}, false)
	rev2b_body[BodyId] = doc.ID
	rev2b_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-b")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2b_body, gotbody)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err := getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Equal(t, 0, len(revTree.BodyMap))
	assert.Equal(t, 1, len(revTree.BodyKeyMap))

	// Retrieve the raw revision body backup of 2-a, and verify it's intact
	log.Printf("Verify document storage of 2-a")
	var revisionBody Body
	rawRevision, _, err := db.Bucket.GetRaw(base.SyncPrefix + "rb:4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	assert.NoError(t, err, "Couldn't get raw backup revision")
	assert.NoError(t, base.JSONUnmarshal(rawRevision, &revisionBody))
	assert.Equal(t, rev2a_body["version"], revisionBody["version"])
	assert.Equal(t, rev2a_body["value"], revisionBody["value"])

	// Retrieve the non-inline revision
	db.FlushRevisionCacheForTest()
	rev2aGet, err := db.Get1xRevBody("doc1", "2-a", false, nil)
	assert.NoError(t, err, "Couldn't get rev 2-a")
	assert.Equal(t, rev2a_body, rev2aGet)

	// Tombstone 2-b (with rev 3-b, minimal tombstone)
	//    1-a
	//   /  \
	// 2-a  2-b
	//       |
	//      3-b(t)
	log.Printf("Create tombstone 3-b")
	rev3b_body := Body{}
	rev3b_body["version"] = "3b"
	rev3b_body[BodyDeleted] = true
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev3b_body, []string{"3-b", "2-b"}, false)
	rev3b_body[BodyId] = doc.ID
	rev3b_body[BodyRev] = newRev
	rev3b_body[BodyDeleted] = true
	assert.NoError(t, err, "add 3-b (tombstone)")

	// Retrieve tombstone
	rev3bGet, err := db.Get1xRevBody("doc1", "3-b", false, nil)
	assert.NoError(t, err, "Couldn't get rev 3-b")
	assert.Equal(t, rev3b_body, rev3bGet)

	// Retrieve the document, validate that we get 2-a
	log.Printf("Retrieve doc, expect 2-a")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2a_body, gotbody)

	// Ensure previous revision body backup has been removed
	_, _, err = db.Bucket.GetRaw(base.RevBodyPrefix + "4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	assert.True(t, base.IsKeyNotFoundError(db.Bucket, err), "Revision should be not found")

	// Validate the tombstone is stored inline (due to small size)
	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Equal(t, 1, len(revTree.BodyMap))
	assert.Equal(t, 0, len(revTree.BodyKeyMap))

	// Create another conflict (2-c)
	//      1-a
	//    /  |   \
	// 2-a  2-b  2-c
	//       |
	//      3-b(t)
	log.Printf("Create rev 2-c with a large body")
	rev2c_body := Body{}
	rev2c_body["key1"] = prop_1000_bytes
	rev2c_body["version"] = "2c"
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev2c_body, []string{"2-c", "1-a"}, false)
	rev2c_body[BodyId] = doc.ID
	rev2c_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-c")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-c")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2c_body, gotbody)

	// Tombstone with a large tombstone
	//      1-a
	//    /  |  \
	// 2-a  2-b  2-c
	//       |    \
	//     3-b(t) 3-c(t)
	log.Printf("Create tombstone 3-c")
	rev3c_body := Body{}
	rev3c_body["version"] = "3c"
	rev3c_body["key1"] = prop_1000_bytes
	rev3c_body[BodyDeleted] = true
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev3c_body, []string{"3-c", "2-c"}, false)
	rev3c_body[BodyId] = doc.ID
	rev3c_body[BodyRev] = newRev
	rev3c_body[BodyDeleted] = true
	assert.NoError(t, err, "add 3-c (large tombstone)")

	// Validate the tombstone is not stored inline (due to small size)
	log.Printf("Verify raw revtree w/ tombstone 3-c in key map")
	newRevTree, err := getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Equal(t, 1, len(newRevTree.BodyMap))    // tombstone 3-b
	assert.Equal(t, 1, len(newRevTree.BodyKeyMap)) // tombstone 3-c

	// Retrieve the non-inline tombstone revision
	db.FlushRevisionCacheForTest()
	rev3cGet, err := db.Get1xRevBody("doc1", "3-c", false, nil)
	assert.NoError(t, err, "Couldn't get rev 3-c")
	assert.Equal(t, rev3c_body, rev3cGet)

	log.Printf("Retrieve doc, verify active rev is 2-a")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2a_body, gotbody)

	// Add active revision, ensure all revisions remain intact
	log.Printf("Create rev 3-a with a large body")
	rev3a_body := Body{}
	rev3a_body["key1"] = prop_1000_bytes
	rev3a_body["version"] = "3a"
	_, _, err = db.PutExistingRevWithBody("doc1", rev2c_body, []string{"3-a", "2-a"}, false)
	assert.NoError(t, err, "add 3-a")

	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Equal(t, 1, len(revTree.BodyMap))    // tombstone 3-b
	assert.Equal(t, 1, len(revTree.BodyKeyMap)) // tombstone 3-c
}

// TestRevisionStoragePruneTombstone - tests cleanup of external tombstone bodies when pruned.
func TestRevisionStoragePruneTombstone(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	base.TestExternalRevStorage = true

	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 2-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a with a large body")
	rev2a_body := Body{}
	rev2a_body["key1"] = prop_1000_bytes
	rev2a_body["version"] = "2a"
	doc, newRev, err := db.PutExistingRevWithBody("doc1", rev2a_body, []string{"2-a", "1-a"}, false)
	rev2a_body[BodyId] = doc.ID
	rev2a_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2a_body, gotbody)

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	log.Printf("Create rev 2-b with a large body")
	rev2b_body := Body{}
	rev2b_body["key1"] = prop_1000_bytes
	rev2b_body["version"] = "2b"
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev2b_body, []string{"2-b", "1-a"}, false)
	rev2b_body[BodyId] = doc.ID
	rev2b_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-b")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2b_body, gotbody)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err := getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Equal(t, 0, len(revTree.BodyMap))
	assert.Equal(t, 1, len(revTree.BodyKeyMap))

	// Retrieve the raw revision body backup of 2-a, and verify it's intact
	log.Printf("Verify document storage of 2-a")
	var revisionBody Body
	rawRevision, _, err := db.Bucket.GetRaw(base.SyncPrefix + "rb:4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	assert.NoError(t, err, "Couldn't get raw backup revision")
	assert.NoError(t, base.JSONUnmarshal(rawRevision, &revisionBody))
	assert.Equal(t, rev2a_body["version"], revisionBody["version"])
	assert.Equal(t, rev2a_body["value"], revisionBody["value"])

	// Retrieve the non-inline revision
	db.FlushRevisionCacheForTest()
	rev2aGet, err := db.Get1xRevBody("doc1", "2-a", false, nil)
	assert.NoError(t, err, "Couldn't get rev 2-a")
	assert.Equal(t, rev2a_body, rev2aGet)

	// Tombstone 2-b (with rev 3-b, large tombstone)
	//    1-a
	//   /  \
	// 2-a  2-b
	//       |
	//      3-b(t)

	log.Printf("Create tombstone 3-b")
	rev3b_body := Body{}
	rev3b_body["version"] = "3b"
	rev3b_body["key1"] = prop_1000_bytes
	rev3b_body[BodyDeleted] = true
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev3b_body, []string{"3-b", "2-b"}, false)
	rev3b_body[BodyId] = doc.ID
	rev3b_body[BodyRev] = newRev
	rev3b_body[BodyDeleted] = true
	assert.NoError(t, err, "add 3-b (tombstone)")

	// Retrieve tombstone
	db.FlushRevisionCacheForTest()
	rev3bGet, err := db.Get1xRevBody("doc1", "3-b", false, nil)
	assert.NoError(t, err, "Couldn't get rev 3-b")
	assert.Equal(t, rev3b_body, rev3bGet)

	// Retrieve the document, validate that we get 2-a
	log.Printf("Retrieve doc, expect 2-a")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2a_body, gotbody)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Equal(t, 0, len(revTree.BodyMap))
	assert.Equal(t, 1, len(revTree.BodyKeyMap))
	log.Printf("revTree.BodyKeyMap:%v", revTree.BodyKeyMap)

	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	log.Printf("revtree before additional revisions: %v", revTree.BodyKeyMap)

	// Add revisions until 3-b is pruned
	db.RevsLimit = 5
	activeRevBody := Body{}
	activeRevBody["version"] = "...a"
	activeRevBody["key1"] = prop_1000_bytes
	_, _, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"3-a", "2-a"}, false)
	assert.NoError(t, err, "add 3-a")
	_, _, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"4-a", "3-a"}, false)
	assert.NoError(t, err, "add 4-a")
	_, _, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"5-a", "4-a"}, false)
	assert.NoError(t, err, "add 5-a")
	_, _, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"6-a", "5-a"}, false)
	assert.NoError(t, err, "add 6-a")
	_, _, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"7-a", "6-a"}, false)
	assert.NoError(t, err, "add 7-a")
	_, _, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"8-a", "7-a"}, false)
	assert.NoError(t, err, "add 8-a")

	// Verify that 3-b is still present at this point
	db.FlushRevisionCacheForTest()
	rev3bGet, err = db.Get1xRevBody("doc1", "3-b", false, nil)
	assert.NoError(t, err, "Rev 3-b should still exist")

	// Add one more rev that triggers pruning since gen(9-3) > revsLimit
	_, _, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"9-a", "8-a"}, false)
	assert.NoError(t, err, "add 9-a")

	// Verify that 3-b has been pruned
	log.Printf("Attempt to retrieve 3-b, expect pruned")
	db.FlushRevisionCacheForTest()
	rev3bGet, err = db.Get1xRevBody("doc1", "3-b", false, nil)
	assert.Equal(t, "404 missing", err.Error())

	// Ensure previous tombstone body backup has been removed
	log.Printf("Verify revision body doc has been removed from bucket")
	_, _, err = db.Bucket.GetRaw(base.SyncPrefix + "rb:ULDLuEgDoKFJeET2hojeFANXM8SrHdVfAGONki+kPxM=")
	assert.True(t, base.IsKeyNotFoundError(db.Bucket, err), "Revision should be not found")

}

// Checks for unwanted interaction between old revision body backups and revision cache
func TestOldRevisionStorage(t *testing.T) {

	db := setupTestDB(t)
	defer db.Close()

	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a", "large": prop_1000_bytes}
	_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	require.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a")
	rev2a_body := Body{"key1": "value2", "version": "2a", "large": prop_1000_bytes}
	doc, newRev, err := db.PutExistingRevWithBody("doc1", rev2a_body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")
	rev2a_body[BodyId] = doc.ID
	rev2a_body[BodyRev] = newRev

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := db.Get1xBody("doc1")
	require.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2a_body, gotbody)

	// Create rev 3-a

	// 1-a
	//  |
	// 2-a
	//  |
	// 3-a
	log.Printf("Create rev 3-a")
	rev3a_body := Body{"key1": "value2", "version": "3a", "large": prop_1000_bytes}
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev3a_body, []string{"3-a", "2-a", "1-a"}, false)
	require.NoError(t, err, "add 3-a")
	rev3a_body[BodyId] = doc.ID
	rev3a_body[BodyRev] = newRev

	// Retrieve the document:
	log.Printf("Retrieve doc 3-a...")
	gotbody, err = db.Get1xBody("doc1")
	require.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev3a_body, gotbody)

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	//  |
	// 3-a
	log.Printf("Create rev 2-b")
	rev2b_body := Body{"key1": "value2", "version": "2b", "large": prop_1000_bytes}
	_, _, err = db.PutExistingRevWithBody("doc1", rev2b_body, []string{"2-b", "1-a"}, false)
	require.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify still rev 3-a")
	gotbody, err = db.Get1xBody("doc1")
	require.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev3a_body, gotbody)

	// Create rev that hops a few generations
	//    1-a
	//   /  \
	// 2-a  2-b
	//  |
	// 3-a
	//  |
	// 4-a
	//  |
	// 5-a
	//  |
	// 6-a
	log.Printf("Create rev 6-a")
	rev6a_body := Body{"key1": "value2", "version": "6a", "large": prop_1000_bytes}
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev6a_body, []string{"6-a", "5-a", "4-a", "3-a"}, false)
	require.NoError(t, err, "add 6-a")
	rev6a_body[BodyId] = doc.ID
	rev6a_body[BodyRev] = newRev

	// Retrieve the document:
	log.Printf("Retrieve doc 6-a...")
	gotbody, err = db.Get1xBody("doc1")
	require.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev6a_body, gotbody)

	// Add child to non-winning revision w/ inline body
	//    1-a
	//   /  \
	// 2-a  2-b
	//  |    |
	// 3-a  3-b
	//  |
	// 4-a
	//  |
	// 5-a
	//  |
	// 6-a
	log.Printf("Create rev 3-b")
	rev3b_body := Body{"key1": "value2", "version": "3b", "large": prop_1000_bytes}
	_, _, err = db.PutExistingRevWithBody("doc1", rev3b_body, []string{"3-b", "2-b", "1-a"}, false)
	require.NoError(t, err, "add 3-b")

	// Same again and again
	// Add child to non-winning revision w/ inline body
	//    1-a
	//   /   \
	// 2-a    2-b
	//  |    / |  \
	// 3-a 3-b 3-c 3-d
	//  |
	// 4-a
	//  |
	// 5-a
	//  |
	// 6-a

	log.Printf("Create rev 3-c")
	rev3c_body := Body{"key1": "value2", "version": "3c", "large": prop_1000_bytes}
	_, _, err = db.PutExistingRevWithBody("doc1", rev3c_body, []string{"3-c", "2-b", "1-a"}, false)
	require.NoError(t, err, "add 3-c")

	log.Printf("Create rev 3-d")
	rev3d_body := Body{"key1": "value2", "version": "3d", "large": prop_1000_bytes}
	_, _, err = db.PutExistingRevWithBody("doc1", rev3d_body, []string{"3-d", "2-b", "1-a"}, false)
	require.NoError(t, err, "add 3-d")

	// Create new winning revision on 'b' branch.  Triggers movement of 6-a to inline storage.  Force cas retry, check document contents
	//    1-a
	//   /   \
	// 2-a    2-b
	//  |    / |  \
	// 3-a 3-b 3-c 3-d
	//  |   |
	// 4-a 4-b
	//  |   |
	// 5-a 5-b
	//  |   |
	// 6-a 6-b
	//      |
	//     7-b
	log.Printf("Create rev 7-b")
	rev7b_body := Body{"key1": "value2", "version": "7b", "large": prop_1000_bytes}
	_, _, err = db.PutExistingRevWithBody("doc1", rev7b_body, []string{"7-b", "6-b", "5-b", "4-b", "3-b"}, false)
	require.NoError(t, err, "add 7-b")

}

// Ensure safe handling when hitting a bucket error during backup of old revision bodies.
// https://github.com/couchbase/sync_gateway/issues/3692
func TestOldRevisionStorageError(t *testing.T) {

	// Use LeakyBucket to force a server error when persisting the old revision body for doc1, rev 2-b
	forceErrorKey := oldRevisionKey("doc1", "2-b")
	leakyConfig := base.LeakyBucketConfig{
		ForceErrorSetRawKeys: []string{forceErrorKey},
	}
	db := setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), leakyConfig)
	defer db.Close()

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {channel(doc.channels);}`)

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "v": "1a"}
	_, _, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a")
	rev2a_body := Body{"key1": "value2", "v": "2a"}
	doc, newRev, err := db.PutExistingRevWithBody("doc1", rev2a_body, []string{"2-a", "1-a"}, false)
	rev2a_body[BodyId] = doc.ID
	rev2a_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2a_body, gotbody)

	// Create rev 3-a, should re-attempt to write old revision body for 2-a
	// 1-a
	//  |
	// 2-a
	//  |
	// 3-a
	log.Printf("Create rev 3-a")
	rev3a_body := Body{"key1": "value2", "v": "3a"}
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev3a_body, []string{"3-a", "2-a", "1-a"}, false)
	rev3a_body[BodyId] = doc.ID
	rev3a_body[BodyRev] = newRev
	assert.NoError(t, err, "add 3-a")

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	//  |
	// 3-a
	log.Printf("Create rev 2-b")
	rev2b_body := Body{"key1": "value2", "v": "2b"}
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev2b_body, []string{"2-b", "1-a"}, false)
	rev2b_body[BodyId] = doc.ID
	rev2b_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify still rev 3-a")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev3a_body, gotbody)

	// Create rev that hops a few generations
	//    1-a
	//   /  \
	// 2-a  2-b
	//  |
	// 3-a
	//  |
	// 4-a
	//  |
	// 5-a
	//  |
	// 6-a
	log.Printf("Create rev 6-a")
	rev6a_body := Body{"key1": "value2", "v": "6a"}
	doc, newRev, err = db.PutExistingRevWithBody("doc1", rev6a_body, []string{"6-a", "5-a", "4-a", "3-a"}, false)
	rev6a_body[BodyId] = doc.ID
	rev6a_body[BodyRev] = newRev
	assert.NoError(t, err, "add 6-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 6-a...")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev6a_body, gotbody)

	// Add child to non-winning revision w/ inline body
	// Creation of 3-b will trigger leaky bucket handling when obsolete body of rev 2-b is persisted
	//    1-a
	//   /  \
	// 2-a  2-b
	//  |    |
	// 3-a  3-b
	//  |
	// 4-a
	//  |
	// 5-a
	//  |
	// 6-a
	log.Printf("Create rev 3-b")
	rev3b_body := Body{"key1": "value2", "v": "3b"}
	_, _, err = db.PutExistingRevWithBody("doc1", rev3b_body, []string{"3-b", "2-b", "1-a"}, false)
	assert.NoError(t, err, "add 3-b")

	// Same again
	// Add child to non-winning revision w/ inline body.
	// Prior to fix for https://github.com/couchbase/sync_gateway/issues/3692, this fails due to malformed oldDoc
	//    1-a
	//   /   \
	// 2-a    2-b
	//  |     / |
	// 3-a  3-b 3-c
	//  |
	// 4-a
	//  |
	// 5-a
	//  |
	// 6-a

	log.Printf("Create rev 3-c")
	rev3c_body := Body{"key1": "value2", "v": "3c"}
	_, _, err = db.PutExistingRevWithBody("doc1", rev3c_body, []string{"3-c", "2-b", "1-a"}, false)
	assert.NoError(t, err, "add 3-c")

}

// Validate JSON number handling for large sequence values
func TestLargeSequence(t *testing.T) {

	db := setupTestDBWithCustomSyncSeq(t, 9223372036854775807)
	defer db.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Write a doc via SG
	body := Body{"key1": "largeSeqTest"}
	_, _, err := db.PutExistingRevWithBody("largeSeqDoc", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add largeSeqDoc")

	syncData, err := db.GetDocSyncData(base.TestCtx(t), "largeSeqDoc")
	assert.NoError(t, err, "Error retrieving document sync data")
	assert.Equal(t, uint64(9223372036854775808), syncData.Sequence)
}

const rawDocMalformedRevisionStorage = `
	{
     "` + base.SyncPropertyName + `":
		{"rev":"6-a",
         "new_rev":"3-b",
         "flags":28,
         "sequence":6,
         "recent_sequences":[1,2,3,4,5,6],
         "history":{
              "revs":["5-a","6-a","3-b","2-b","2-a","1-a","3-a","4-a"],
              "parents":[7,0,3,5,5,-1,4,6],
              "bodymap":{
                 "2":"{\"key1\":\"value2\",\"v\":\"3b\"}",
                 "3":"\u0001{\"key1\":\"value2\",\"v\":\"2b\""},
              "channels":[null,null,null,null,null,null,null,null]},
         "cas":"",
         "value_crc32c":"",
         "time_saved":"2018-09-27T13:47:44.719971735-07:00"
     },
     "key1":"value2",
     "v":"6a"
    }`

func TestMalformedRevisionStorageRecovery(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {channel(doc.channels);}`)

	// Create a document with a malformed revision body (due to https://github.com/couchbase/sync_gateway/issues/3692) in the bucket
	// Document has the following rev tree, with a malformed body of revision 2-b remaining in the revision tree (same set of operations as
	// TestOldRevisionStorageError)
	//    1-a
	//   /  \
	// 2-a  2-b
	//  |    |
	// 3-a  3-b
	//  |
	// 4-a
	//  |
	// 5-a
	//  |
	// 6-a
	log.Printf("Add doc1 w/ malformed body for rev 2-b included in revision tree")
	ok, addErr := db.Bucket.Add("doc1", 0, []byte(rawDocMalformedRevisionStorage))
	assert.True(t, ok)
	assert.NoError(t, addErr, "Error writing raw document")

	// Increment _sync:seq to match sequences allocated by raw doc
	_, incrErr := db.Bucket.Incr(base.SyncSeqKey, 5, 0, 0)
	assert.NoError(t, incrErr, "Error incrementing sync:seq")

	// Add child to non-winning revision w/ malformed inline body.
	// Prior to fix for https://github.com/couchbase/sync_gateway/issues/3700, this fails
	//    1-a
	//   /   \
	// 2-a    2-b
	//  |     / |
	// 3-a  3-b 3-c
	//  |
	// 4-a
	//  |
	// 5-a
	//  |
	// 6-a
	log.Printf("Attempt to create rev 3-c")
	rev3c_body := Body{"key1": "value2", "v": "3c"}
	_, _, err := db.PutExistingRevWithBody("doc1", rev3c_body, []string{"3-c", "2-b", "1-a"}, false)
	assert.NoError(t, err, "add 3-c")
}

func BenchmarkDatabaseGet1xRev(b *testing.B) {
	base.DisableTestLogging(b)

	db := setupTestDB(b)
	defer db.Close()

	body := Body{"foo": "bar", "rev": "1-a"}
	_, _, _ = db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)

	largeDoc := make([]byte, 1000000)
	longBody := Body{"val": string(largeDoc), "rev": "1-a"}
	_, _, _ = db.PutExistingRevWithBody("doc2", longBody, []string{"1-a"}, false)

	var shortWithAttachmentsDataBody Body
	shortWithAttachmentsData := `{"test": true, "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}, "rev":"1-a"}`
	_ = base.JSONUnmarshal([]byte(shortWithAttachmentsData), &shortWithAttachmentsDataBody)
	_, _, _ = db.PutExistingRevWithBody("doc3", shortWithAttachmentsDataBody, []string{"1-a"}, false)

	b.Run("ShortLatest", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.Get1xRevBody("doc1", "", false, nil)
		}
	})
	b.Run("LongLatest", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.Get1xRevBody("doc2", "", false, nil)
		}
	})
	b.Run("ShortWithAttachmentsLatest", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.Get1xRevBody("doc3", "", false, nil)
		}
	})

	updateBody := Body{"rev": "2-a"}
	_, _, _ = db.PutExistingRevWithBody("doc1", updateBody, []string{"2-a", "1-a"}, false)
	_, _, _ = db.PutExistingRevWithBody("doc2", updateBody, []string{"2-a", "1-a"}, false)
	_, _, _ = db.PutExistingRevWithBody("doc3", updateBody, []string{"2-a", "1-a"}, false)

	b.Run("ShortOld", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.Get1xRevBody("doc1", "1-a", false, nil)
		}
	})
	b.Run("LongOld", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.Get1xRevBody("doc2", "1-a", false, nil)
		}
	})
	b.Run("ShortWithAttachmentsOld", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.Get1xRevBody("doc3", "1-a", false, nil)
		}
	})
}

func BenchmarkDatabaseGetRev(b *testing.B) {
	base.DisableTestLogging(b)

	db := setupTestDB(b)
	defer db.Close()

	body := Body{"foo": "bar", "rev": "1-a"}
	_, _, _ = db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)

	largeDoc := make([]byte, 1000000)
	longBody := Body{"val": string(largeDoc), "rev": "1-a"}
	_, _, _ = db.PutExistingRevWithBody("doc2", longBody, []string{"1-a"}, false)

	var shortWithAttachmentsDataBody Body
	shortWithAttachmentsData := `{"test": true, "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}, "rev":"1-a"}`
	_ = base.JSONUnmarshal([]byte(shortWithAttachmentsData), &shortWithAttachmentsDataBody)
	_, _, _ = db.PutExistingRevWithBody("doc3", shortWithAttachmentsDataBody, []string{"1-a"}, false)

	b.Run("ShortLatest", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.GetRev("doc1", "", false, nil)
		}
	})
	b.Run("LongLatest", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.GetRev("doc2", "", false, nil)
		}
	})
	b.Run("ShortWithAttachmentsLatest", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.GetRev("doc3", "", false, nil)
		}
	})

	updateBody := Body{"rev": "2-a"}
	_, _, _ = db.PutExistingRevWithBody("doc1", updateBody, []string{"2-a", "1-a"}, false)
	_, _, _ = db.PutExistingRevWithBody("doc2", updateBody, []string{"2-a", "1-a"}, false)
	_, _, _ = db.PutExistingRevWithBody("doc3", updateBody, []string{"2-a", "1-a"}, false)

	b.Run("ShortOld", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.GetRev("doc1", "1-a", false, nil)
		}
	})
	b.Run("LongOld", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.GetRev("doc2", "1-a", false, nil)
		}
	})
	b.Run("ShortWithAttachmentsOld", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			_, _ = db.GetRev("doc3", "1-a", false, nil)
		}
	})
}

// Replicates delta patching work carried out by handleRev
func BenchmarkHandleRevDelta(b *testing.B) {
	base.DisableTestLogging(b)

	db := setupTestDB(b)
	defer db.Close()

	body := Body{"foo": "bar"}
	_, _, _ = db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)

	getDelta := func(newDoc *Document) {
		deltaSrcRev, _ := db.GetRev("doc1", "1-a", false, nil)

		deltaSrcBody, _ := deltaSrcRev.MutableBody()

		// Stamp attachments so we can patch them
		if len(deltaSrcRev.Attachments) > 0 {
			deltaSrcBody[BodyAttachments] = map[string]interface{}(deltaSrcRev.Attachments)
		}

		deltaSrcMap := map[string]interface{}(deltaSrcBody)
		_ = base.Patch(&deltaSrcMap, newDoc.Body())
	}

	b.Run("SmallDiff", func(b *testing.B) {
		newDoc := &Document{
			ID:    "doc1",
			RevID: "1a",
		}
		newDoc.UpdateBodyBytes([]byte(`{"foo": "bart"}`))
		for n := 0; n < b.N; n++ {
			getDelta(newDoc)
		}
	})

	b.Run("Huge Diff", func(b *testing.B) {
		newDoc := &Document{
			ID:    "doc1",
			RevID: "1a",
		}
		largeDoc := make([]byte, 1000000)
		longBody := Body{"val": string(largeDoc)}
		bodyBytes, _ := base.JSONMarshal(longBody)
		newDoc.UpdateBodyBytes(bodyBytes)
		for n := 0; n < b.N; n++ {
			getDelta(newDoc)
		}
	})
}

func TestGetAvailableRevAttachments(t *testing.T) {
	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")

	// Create the very first revision of the document with attachment; let's call this as rev 1-a
	payload := `{"sku":"6213100","_attachments":{"camera.txt":{"data":"Q2Fub24gRU9TIDVEIE1hcmsgSVY="}}}`
	doc, rev, err := db.PutExistingRevWithBody("camera", unjson(payload), []string{"1-a"}, false)
	assert.NoError(t, err, "Couldn't create document")
	ancestor := rev // Ancestor revision

	// Create the second revision of the document with attachment reference;
	payload = `{"sku":"6213101","_attachments":{"camera.txt":{"stub":true,"revpos":1}}}`
	doc, rev, err = db.PutExistingRevWithBody("camera", unjson(payload), []string{"2-a", "1-a"}, false)
	parent := rev // Immediate ancestor or parent revision
	assert.NoError(t, err, "Couldn't create document")

	payload = `{"sku":"6213102","_attachments":{"camera.txt":{"stub":true,"revpos":1}}}`
	doc, rev, err = db.PutExistingRevWithBody("camera", unjson(payload), []string{"3-a", "2-a"}, false)
	assert.NoError(t, err, "Couldn't create document")

	// Get available attachments by immediate ancestor revision or parent revision
	meta, found := db.getAvailableRevAttachments(doc, parent)
	attachment := meta["camera.txt"].(map[string]interface{})
	assert.Equal(t, "sha1-VoSNiNQGHE1HirIS5HMxj6CrlHI=", attachment["digest"])
	assert.Equal(t, json.Number("20"), attachment["length"])
	assert.Equal(t, json.Number("1"), attachment["revpos"])
	assert.True(t, found, "Ancestor should exists")

	// Get available attachments by immediate ancestor revision
	meta, found = db.getAvailableRevAttachments(doc, ancestor)
	attachment = meta["camera.txt"].(map[string]interface{})
	assert.Equal(t, "sha1-VoSNiNQGHE1HirIS5HMxj6CrlHI=", attachment["digest"])
	assert.Equal(t, json.Number("20"), attachment["length"])
	assert.Equal(t, json.Number("1"), attachment["revpos"])
	assert.True(t, found, "Ancestor should exists")
}

func TestGet1xRevAndChannels(t *testing.T) {
	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")

	docId := "dd6d2dcc679d12b9430a9787bab45b33"
	payload := `{"sku":"6213100","_attachments":{"camera.txt":{"data":"Q2Fub24gRU9TIDVEIE1hcmsgSVY="}}}`
	doc1, rev1, err := db.PutExistingRevWithBody(docId, unjson(payload), []string{"1-a"}, false)
	assert.NoError(t, err, "Couldn't create document")

	payload = `{"sku":"6213101","_attachments":{"lens.txt":{"data":"Q2Fub24gRU9TIDVEIE1hcmsgSVY="}}}`
	doc2, rev2, err := db.PutExistingRevWithBody(docId, unjson(payload), []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "Couldn't create document")

	// Get the 1x revision from document with list revision enabled
	bodyBytes, removed, err := db.get1xRevFromDoc(doc2, rev2, true)
	assert.False(t, removed)
	assert.NoError(t, err, "It should not throw any error")
	assert.NotNil(t, bodyBytes, "Document body bytes should be received")

	var response = Body{}
	assert.NoError(t, response.Unmarshal(bodyBytes))

	// Get the 1x revision from document with list revision enabled. Also validate that the
	// BodyRevisions property is present and correct since listRevisions=true.
	bodyBytes, removed, err = db.get1xRevFromDoc(doc1, rev1, true)
	assert.False(t, removed)
	assert.NoError(t, err, "It should not throw any error")
	assert.NotNil(t, bodyBytes, "Document body bytes should be received")
	assert.NoError(t, response.Unmarshal(bodyBytes))
	assert.Equal(t, docId, response[BodyId])
	assert.Equal(t, "1-a", response[BodyRev])
	assert.Equal(t, "6213100", response["sku"])
	revisions, ok := response[BodyRevisions].(map[string]interface{})
	assert.True(t, ok, "revisions should be extracted from response body")
	assert.Equal(t, json.Number("1"), revisions[RevisionsStart])
	assert.Equal(t, []interface{}{"a"}, revisions[RevisionsIds])

	// Delete the document, creating tombstone revision rev3
	rev3, err := db.DeleteDoc(docId, rev2)
	bodyBytes, removed, err = db.get1xRevFromDoc(doc2, rev3, true)
	assert.False(t, removed)
	assert.Error(t, err, "It should throw 404 missing error")
	assert.Nil(t, bodyBytes, "Document body bytes should be empty")

	// get1xRevFromDoc for doc2 should be returning the current revision id (in this case, the tombstone revision rev3).
	// Also validate that the BodyRevisions property is present and correct since listRevisions=true.
	bodyBytes, removed, err = db.get1xRevFromDoc(doc2, "", true)
	assert.False(t, removed)
	assert.NoError(t, err, "It should not throw any error")
	assert.NotNil(t, bodyBytes, "Document body bytes should be received")
	assert.NoError(t, response.Unmarshal(bodyBytes))
	assert.Equal(t, docId, response[BodyId])
	assert.Equal(t, "2-a", response[BodyRev])
	assert.Equal(t, "6213101", response["sku"])
	revisions, ok = response[BodyRevisions].(map[string]interface{})
	assert.True(t, ok, "revisions should be extracted from response body")
	assert.Equal(t, json.Number("2"), revisions[RevisionsStart])
	assert.Equal(t, []interface{}{"a", "a"}, revisions[RevisionsIds])
}

func TestGet1xRevFromDoc(t *testing.T) {
	context, err := NewDatabaseContext("db", base.GetTestBucket(t), false, DatabaseContextOptions{})
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assert.NoError(t, err, "Couldn't create database 'db'")

	// Create the first revision of the document
	docId := "356779a9a1696714480f57fa3fb66d4c"
	payload := `{"city":"Los Angeles"}`
	doc, rev1, err := db.PutExistingRevWithBody(docId, unjson(payload), []string{"1-a"}, false)
	assert.NoError(t, err, "Couldn't create document")
	assert.NotEmpty(t, doc, "Document shouldn't be empty")
	assert.Equal(t, "1-a", rev1, "Provided input revision ID should be returned")

	// Get rev1 using get1xRevFromDoc. Also validate that the BodyRevisions property is present
	// and correct since listRevisions=true.
	bodyBytes, removed, err := db.get1xRevFromDoc(doc, rev1, true)
	assert.NotEmpty(t, bodyBytes, "Document body bytes should be returned")
	assert.False(t, removed, "This shouldn't be a removed document")
	var response = Body{}
	assert.NoError(t, response.Unmarshal(bodyBytes))
	assert.Equal(t, docId, response[BodyId])
	assert.Equal(t, "1-a", response[BodyRev])
	assert.Equal(t, "Los Angeles", response["city"])
	revisions, ok := response[BodyRevisions].(map[string]interface{})
	assert.True(t, ok, "revisions should be extracted from response body")
	assert.Equal(t, json.Number("1"), revisions[RevisionsStart])
	assert.Equal(t, []interface{}{"a"}, revisions[RevisionsIds])

	// Create the second revision of the document
	payload = `{"city":"Hollywood"}`
	doc, rev2, err := db.PutExistingRevWithBody(docId, unjson(payload), []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "Couldn't create document")
	assert.NotEmpty(t, doc, "Document shouldn't be empty")
	assert.Equal(t, "2-a", rev2, "Provided input revision ID should be returned")

	// Get rev2 using get1xRevFromDoc. Also validate that the BodyRevisions property is present
	// and correct since listRevisions=true.
	bodyBytes, removed, err = db.get1xRevFromDoc(doc, rev2, true)
	assert.NotEmpty(t, bodyBytes, "Document body bytes should be returned")
	assert.False(t, removed, "This shouldn't be a removed document")
	assert.NoError(t, response.Unmarshal(bodyBytes))
	assert.Equal(t, docId, response[BodyId])
	assert.Equal(t, "2-a", response[BodyRev])
	assert.Equal(t, "Hollywood", response["city"])
	revisions, ok = response[BodyRevisions].(map[string]interface{})
	assert.True(t, ok, "revisions should be extracted from response body")
	assert.Equal(t, json.Number("2"), revisions[RevisionsStart])
	assert.Equal(t, []interface{}{"a", "a"}, revisions[RevisionsIds])

	// Get body bytes from doc with unknown revision id; it simulates the error scenario.
	// A 404 missing error should be thrown when trying get the body bytes of the document
	// which doesn't exists in the revision tree. The revision "3-a" doesn't exists in database.
	bodyBytes, removed, err = db.get1xRevFromDoc(doc, "3-a", true)
	assert.Error(t, err, "It should throw 404 missing error")
	assert.Contains(t, err.Error(), "404 missing")
	assert.Empty(t, bodyBytes, "Provided revision doesn't exists")
	assert.False(t, removed, "This shouldn't be a removed revision")
	assert.Error(t, response.Unmarshal(bodyBytes), "Unexpected empty JSON input to body.Unmarshal")

	// Deletes the document, by adding a new revision whose _deleted property is true.
	body := Body{BodyDeleted: true, BodyRev: rev2}
	rev3, doc, err := db.Put(docId, body)
	assert.NoError(t, err, "Document should be deleted")
	assert.NotEmpty(t, rev3, "Document revision shouldn't be empty")

	// Get the document body bytes with the tombstone revision rev3, with listRevisions=true
	// Also validate that the BodyRevisions property is present and correct.
	bodyBytes, removed, err = db.get1xRevFromDoc(doc, rev3, true)
	assert.NotEmpty(t, bodyBytes, "Document body bytes should be returned")
	assert.False(t, removed, "This shouldn't be a removed document")
	assert.NoError(t, response.Unmarshal(bodyBytes))
	assert.Equal(t, docId, response[BodyId])
	assert.Equal(t, rev3, response[BodyRev])
	assert.Equal(t, "Hollywood", response["city"])
	revisions, ok = response[BodyRevisions].(map[string]interface{})
	assert.True(t, ok, "revisions should be extracted from response body")
	assert.Equal(t, json.Number("3"), revisions[RevisionsStart])
	assert.Equal(t, []interface{}{"5464898886a6c57cd648c659f0993bb3", "a", "a"}, revisions[RevisionsIds])

	// If the provided revision ID is blank and the current revision is already deleted
	// when checking document revision history, it should throw 404 deleted error.
	bodyBytes, removed, err = db.get1xRevFromDoc(doc, "", true)
	assert.Error(t, err, "404 deleted")
	assert.Contains(t, err.Error(), "404 deleted")
	assert.Empty(t, bodyBytes, "Document body bytes should be empty")
	assert.False(t, removed, "This shouldn't be a removed document")
	assert.Error(t, response.Unmarshal(bodyBytes), "Unexpected empty JSON input to body.Unmarshal")
}

func TestMergeAttachments(t *testing.T) {
	tests := []struct {
		name             string
		pre25Attachments AttachmentsMeta
		docAttachments   AttachmentsMeta
		wantMerged       AttachmentsMeta
	}{
		{
			"all nil",
			nil,
			nil,
			nil,
		},
		{
			"pre25Atts only",
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			nil,
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
		},
		{
			"docAtts only",
			nil,
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
		},
		{
			"disjoint set",
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att2": map[string]interface{}{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
				"att2": map[string]interface{}{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
		},
		{
			"25Atts wins",
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
		},
		{
			"docAtts wins",
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
		},
		{
			"invalid pre25 revpos",
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "def",
					"revpos": "6",
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]interface{}{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged := mergeAttachments(tt.pre25Attachments, tt.docAttachments)
			assert.Equal(t, tt.wantMerged, merged)
		})
	}
}
