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
	"context"
	"encoding/json"
	"log"
	"reflect"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
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
func getRevTreeList(ctx context.Context, dataStore sgbucket.DataStore, key string, useXattrs bool) (revTreeList, error) {
	switch useXattrs {
	case true:
		_, xattrs, _, getErr := dataStore.GetWithXattrs(ctx, key, []string{base.SyncXattrName})
		rawXattr, ok := xattrs[base.SyncXattrName]
		if !ok {
			return revTreeList{}, base.ErrXattrNotFound
		}
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
		rawDoc, _, err := dataStore.GetRaw(key)
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

	db, ctx := setupTestDBWithViewsEnabled(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	base.TestExternalRevStorage = true

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")

	// Flush the cache
	db.FlushRevisionCacheForTest()

	// Retrieve the document:
	log.Printf("Retrieve doc 1-a...")
	_, err = collection.Get1xRevBody(ctx, "doc1", "1-a", false, nil)
	assert.NoError(t, err, "Couldn't get document")

	docRev, err := collection.GetRev(ctx, "doc1", "1-a", false, nil)
	assert.NoError(t, err)
	assert.Equal(t, "1-a", docRev.RevID)

	// Validate that mutations to the body don't affect the revcache value
	_, err = base.InjectJSONProperties(docRev.BodyBytes, base.KVPair{Key: "modified", Val: "property"})
	assert.NoError(t, err)

	docRevAgain, err := collection.GetRev(ctx, "doc1", "1-a", false, nil)
	assert.NoError(t, err)
	assert.Equal(t, "1-a", docRevAgain.RevID)

	body, err = docRevAgain.Body()
	assert.NoError(t, err)
	_, ok := body["modified"]
	assert.False(t, ok)
}

func TestHasAttachmentsFlag(t *testing.T) {
	db, ctx := setupTestDBAllowConflicts(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	base.TestExternalRevStorage = true
	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a with a large body")
	rev2a_body := unmarshalBody(t, `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`)
	rev2a_body["key1"] = prop_1000_bytes
	rev2a_body["version"] = "2a"
	doc, newRev, err := collection.PutExistingRevWithBody(ctx, "doc1", rev2a_body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	rev2a_body[BodyId] = doc.ID
	rev2a_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotDoc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalSync)
	assert.NoError(t, err)
	require.Contains(t, gotDoc.Attachments(), "hello.txt")
	attachmentData, ok := gotDoc.Attachments()["hello.txt"].(map[string]any)
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
	rev2b_body := unmarshalBody(t, `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`)
	rev2b_body["key1"] = prop_1000_bytes
	rev2b_body["version"] = "2b"
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev2b_body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err)
	rev2b_body[BodyId] = doc.ID
	rev2b_body[BodyRev] = newRev

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-b")
	gotDoc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalSync)
	assert.NoError(t, err)
	require.Contains(t, gotDoc.Attachments(), "hello.txt")
	attachmentData, ok = gotDoc.Attachments()["hello.txt"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", attachmentData["digest"])
	assert.Equal(t, float64(11), attachmentData["length"])
	assert.Equal(t, float64(2), attachmentData["revpos"])
	assert.True(t, attachmentData["stub"].(bool))
	assert.Equal(t, float64(2), attachmentData["ver"])

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err := getRevTreeList(ctx, collection.dataStore, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Len(t, revTree.BodyMap, 0)
	assert.Len(t, revTree.BodyKeyMap, 1)
	assert.Len(t, revTree.HasAttachments, 1)
}

// TestRevisionStorageConflictAndTombstones
// Tests permutations of inline and external storage of conflicts and tombstones
func TestRevisionStorageConflictAndTombstones(t *testing.T) {

	db, ctx := setupTestDBAllowConflicts(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	base.TestExternalRevStorage = true

	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a with a large body")
	rev2a_body := Body{}
	rev2a_body["key1"] = prop_1000_bytes
	rev2a_body["version"] = "2a"
	doc, newRev, err := collection.PutExistingRevWithBody(ctx, "doc1", rev2a_body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	rev2a_body[BodyId] = doc.ID
	rev2a_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := collection.Get1xBody(ctx, "doc1")
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
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev2b_body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	rev2b_body[BodyId] = doc.ID
	rev2b_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-b")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2b_body, gotbody)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err := getRevTreeList(ctx, collection.dataStore, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Len(t, revTree.BodyMap, 0)
	assert.Len(t, revTree.BodyKeyMap, 1)

	// Retrieve the raw revision body backup of 2-a, and verify it's intact
	log.Printf("Verify document storage of 2-a")
	var revisionBody Body
	rawRevision, _, err := collection.dataStore.GetRaw(base.SyncDocPrefix + "rb:4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	assert.NoError(t, err, "Couldn't get raw backup revision")
	assert.NoError(t, base.JSONUnmarshal(rawRevision, &revisionBody))
	assert.Equal(t, rev2a_body["version"], revisionBody["version"])
	assert.Equal(t, rev2a_body["value"], revisionBody["value"])

	// Retrieve the non-inline revision
	db.FlushRevisionCacheForTest()
	rev2aGet, err := collection.Get1xRevBody(ctx, "doc1", "2-a", false, nil)
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
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev3b_body, []string{"3-b", "2-b"}, false, ExistingVersionWithUpdateToHLV)
	rev3b_body[BodyId] = doc.ID
	rev3b_body[BodyRev] = newRev
	rev3b_body[BodyDeleted] = true
	assert.NoError(t, err, "add 3-b (tombstone)")

	// Retrieve tombstone
	rev3bGet, err := collection.Get1xRevBody(ctx, "doc1", "3-b", false, nil)
	assert.NoError(t, err, "Couldn't get rev 3-b")
	assert.Equal(t, rev3b_body, rev3bGet)

	// Retrieve the document, validate that we get 2-a
	log.Printf("Retrieve doc, expect 2-a")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2a_body, gotbody)

	// Ensure previous revision body backup has been removed
	_, _, err = db.MetadataStore.GetRaw(base.RevBodyPrefix + "4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	base.RequireDocNotFoundError(t, err)

	// Validate the tombstone is stored inline (due to small size)
	revTree, err = getRevTreeList(ctx, collection.dataStore, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Len(t, revTree.BodyMap, 1)
	assert.Len(t, revTree.BodyKeyMap, 0)

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
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev2c_body, []string{"2-c", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	rev2c_body[BodyId] = doc.ID
	rev2c_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-c")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-c")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
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
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev3c_body, []string{"3-c", "2-c"}, false, ExistingVersionWithUpdateToHLV)
	rev3c_body[BodyId] = doc.ID
	rev3c_body[BodyRev] = newRev
	rev3c_body[BodyDeleted] = true
	assert.NoError(t, err, "add 3-c (large tombstone)")

	// Validate the tombstone is not stored inline (due to small size)
	log.Printf("Verify raw revtree w/ tombstone 3-c in key map")
	newRevTree, err := getRevTreeList(ctx, collection.dataStore, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Len(t, newRevTree.BodyMap, 1)    // tombstone 3-b
	assert.Len(t, newRevTree.BodyKeyMap, 1) // tombstone 3-c

	// Retrieve the non-inline tombstone revision
	db.FlushRevisionCacheForTest()
	rev3cGet, err := collection.Get1xRevBody(ctx, "doc1", "3-c", false, nil)
	assert.NoError(t, err, "Couldn't get rev 3-c")
	assert.Equal(t, rev3c_body, rev3cGet)

	log.Printf("Retrieve doc, verify active rev is 2-a")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2a_body, gotbody)

	// Add active revision, ensure all revisions remain intact
	log.Printf("Create rev 3-a with a large body")
	rev3a_body := Body{}
	rev3a_body["key1"] = prop_1000_bytes
	rev3a_body["version"] = "3a"
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", rev2c_body, []string{"3-a", "2-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 3-a")

	revTree, err = getRevTreeList(ctx, collection.dataStore, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Len(t, revTree.BodyMap, 1)    // tombstone 3-b
	assert.Len(t, revTree.BodyKeyMap, 1) // tombstone 3-c
}

// TestRevisionStoragePruneTombstone - tests cleanup of external tombstone bodies when pruned.
func TestRevisionStoragePruneTombstone(t *testing.T) {

	db, ctx := setupTestDBAllowConflicts(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	base.TestExternalRevStorage = true

	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 2-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a with a large body")
	rev2a_body := Body{}
	rev2a_body["key1"] = prop_1000_bytes
	rev2a_body["version"] = "2a"
	doc, newRev, err := collection.PutExistingRevWithBody(ctx, "doc1", rev2a_body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	rev2a_body[BodyId] = doc.ID
	rev2a_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := collection.Get1xBody(ctx, "doc1")
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
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev2b_body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err)
	rev2b_body[BodyId] = doc.ID
	rev2b_body[BodyRev] = newRev

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-b")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2b_body, gotbody)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err := getRevTreeList(ctx, collection.dataStore, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Len(t, revTree.BodyMap, 0)
	assert.Len(t, revTree.BodyKeyMap, 1)

	// Retrieve the raw revision body backup of 2-a, and verify it's intact
	log.Printf("Verify document storage of 2-a")
	var revisionBody Body
	rawRevision, _, err := collection.dataStore.GetRaw(base.SyncDocPrefix + "rb:4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	assert.NoError(t, err, "Couldn't get raw backup revision")
	assert.NoError(t, base.JSONUnmarshal(rawRevision, &revisionBody))
	assert.Equal(t, rev2a_body["version"], revisionBody["version"])
	assert.Equal(t, rev2a_body["value"], revisionBody["value"])

	// Retrieve the non-inline revision
	db.FlushRevisionCacheForTest()
	rev2aGet, err := collection.Get1xRevBody(ctx, "doc1", "2-a", false, nil)
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
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev3b_body, []string{"3-b", "2-b"}, false, ExistingVersionWithUpdateToHLV)
	rev3b_body[BodyId] = doc.ID
	rev3b_body[BodyRev] = newRev
	rev3b_body[BodyDeleted] = true
	assert.NoError(t, err, "add 3-b (tombstone)")

	// Retrieve tombstone
	db.FlushRevisionCacheForTest()
	rev3bGet, err := collection.Get1xRevBody(ctx, "doc1", "3-b", false, nil)
	assert.NoError(t, err, "Couldn't get rev 3-b")
	assert.Equal(t, rev3b_body, rev3bGet)

	// Retrieve the document, validate that we get 2-a
	log.Printf("Retrieve doc, expect 2-a")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
	assert.NoError(t, err, "Couldn't get document")
	assert.Equal(t, rev2a_body, gotbody)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err = getRevTreeList(ctx, collection.dataStore, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	assert.Len(t, revTree.BodyMap, 0)
	assert.Len(t, revTree.BodyKeyMap, 1)
	log.Printf("revTree.BodyKeyMap:%v", revTree.BodyKeyMap)

	revTree, err = getRevTreeList(ctx, collection.dataStore, "doc1", db.UseXattrs())
	require.NoError(t, err)
	log.Printf("revtree before additional revisions: %v", revTree.BodyKeyMap)

	// Add revisions until 3-b is pruned
	db.RevsLimit = 5
	activeRevBody := Body{}
	activeRevBody["version"] = "...a"
	activeRevBody["key1"] = prop_1000_bytes
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", activeRevBody, []string{"3-a", "2-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 3-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", activeRevBody, []string{"4-a", "3-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 4-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", activeRevBody, []string{"5-a", "4-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 5-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", activeRevBody, []string{"6-a", "5-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 6-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", activeRevBody, []string{"7-a", "6-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 7-a")
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", activeRevBody, []string{"8-a", "7-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 8-a")

	// Verify that 3-b is still present at this point
	db.FlushRevisionCacheForTest()
	_, err = collection.Get1xRevBody(ctx, "doc1", "3-b", false, nil)
	assert.NoError(t, err, "Rev 3-b should still exist")

	// Add one more rev that triggers pruning since gen(9-3) > revsLimit
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", activeRevBody, []string{"9-a", "8-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 9-a")

	// Verify that 3-b has been pruned
	log.Printf("Attempt to retrieve 3-b, expect pruned")
	db.FlushRevisionCacheForTest()
	_, err = collection.Get1xRevBody(ctx, "doc1", "3-b", false, nil)
	require.Error(t, err)
	assert.Equal(t, "404 missing", err.Error())

	// Ensure previous tombstone body backup has been removed
	log.Printf("Verify revision body doc has been removed from bucket")
	_, _, err = collection.dataStore.GetRaw(base.SyncDocPrefix + "rb:ULDLuEgDoKFJeET2hojeFANXM8SrHdVfAGONki+kPxM=")
	base.RequireDocNotFoundError(t, err)

}

// Checks for unwanted interaction between old revision body backups and revision cache
func TestOldRevisionStorage(t *testing.T) {

	db, ctx := setupTestDBAllowConflicts(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a", "large": prop_1000_bytes}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a")
	rev2a_body := Body{"key1": "value2", "version": "2a", "large": prop_1000_bytes}
	doc, newRev, err := collection.PutExistingRevWithBody(ctx, "doc1", rev2a_body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 2-a")
	rev2a_body[BodyId] = doc.ID
	rev2a_body[BodyRev] = newRev

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := collection.Get1xBody(ctx, "doc1")
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
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev3a_body, []string{"3-a", "2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err, "add 3-a")
	rev3a_body[BodyId] = doc.ID
	rev3a_body[BodyRev] = newRev

	// Retrieve the document:
	log.Printf("Retrieve doc 3-a...")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
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
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", rev2b_body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify still rev 3-a")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
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
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev6a_body, []string{"6-a", "5-a", "4-a", "3-a"}, false, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err, "add 6-a")
	rev6a_body[BodyId] = doc.ID
	rev6a_body[BodyRev] = newRev

	// Retrieve the document:
	log.Printf("Retrieve doc 6-a...")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
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
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", rev3b_body, []string{"3-b", "2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
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
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", rev3c_body, []string{"3-c", "2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err, "add 3-c")

	log.Printf("Create rev 3-d")
	rev3d_body := Body{"key1": "value2", "version": "3d", "large": prop_1000_bytes}
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", rev3d_body, []string{"3-d", "2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
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
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", rev7b_body, []string{"7-b", "6-b", "5-b", "4-b", "3-b"}, false, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err, "add 7-b")

}

// Ensure safe handling when hitting a bucket error during backup of old revision bodies.
// https://github.com/couchbase/sync_gateway/issues/3692
func TestOldRevisionStorageError(t *testing.T) {

	// Use LeakyBucket to force a server error when persisting the old revision body for doc1, rev 2-b
	forceErrorKey := oldRevisionKey("doc1", "2-b")
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	leakyBucket := base.NewLeakyBucket(bucket, base.LeakyBucketConfig{
		ForceErrorSetRawKeys: []string{forceErrorKey},
	})
	db, ctx := SetupTestDBForBucketWithOptions(t, leakyBucket, DatabaseContextOptions{AllowConflicts: base.Ptr(true)})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "v": "1a"}
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a")
	rev2a_body := Body{"key1": "value2", "v": "2a"}
	doc, newRev, err := collection.PutExistingRevWithBody(ctx, "doc1", rev2a_body, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	rev2a_body[BodyId] = doc.ID
	rev2a_body[BodyRev] = newRev
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := collection.Get1xBody(ctx, "doc1")
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
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev3a_body, []string{"3-a", "2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
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
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev2b_body, []string{"2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	require.NoError(t, err)
	rev2b_body[BodyId] = doc.ID
	rev2b_body[BodyRev] = newRev

	// Retrieve the document:
	log.Printf("Retrieve doc, verify still rev 3-a")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
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
	doc, newRev, err = collection.PutExistingRevWithBody(ctx, "doc1", rev6a_body, []string{"6-a", "5-a", "4-a", "3-a"}, false, ExistingVersionWithUpdateToHLV)
	rev6a_body[BodyId] = doc.ID
	rev6a_body[BodyRev] = newRev
	assert.NoError(t, err, "add 6-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 6-a...")
	gotbody, err = collection.Get1xBody(ctx, "doc1")
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
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", rev3b_body, []string{"3-b", "2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
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
	_, _, err = collection.PutExistingRevWithBody(ctx, "doc1", rev3c_body, []string{"3-c", "2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 3-c")

}

// Validate JSON number handling for large sequence values
func TestLargeSequence(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("rosmar does not support writing oversize ints")
	}

	db, ctx := setupTestDBWithCustomSyncSeq(t, 9223372036854775807)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Write a doc via SG
	body := Body{"key1": "largeSeqTest"}
	_, _, err := collection.PutExistingRevWithBody(ctx, "largeSeqDoc", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add largeSeqDoc")

	syncData, err := collection.GetDocSyncData(ctx, "largeSeqDoc")
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
	db, ctx := setupTestDBAllowConflicts(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

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
	ok, addErr := collection.dataStore.Add("doc1", 0, []byte(rawDocMalformedRevisionStorage))
	assert.True(t, ok)
	assert.NoError(t, addErr, "Error writing raw document")

	// Increment _sync:seq to match sequences allocated by raw doc
	_, incrErr := collection.dataStore.Incr(db.MetadataKeys.SyncSeqKey(), 5, 0, 0)
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
	_, _, err := collection.PutExistingRevWithBody(ctx, "doc1", rev3c_body, []string{"3-c", "2-b", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "add 3-c")
}

func BenchmarkDatabaseGet1xRev(b *testing.B) {
	base.DisableTestLogging(b)

	db, ctx := setupTestDB(b)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, b, db)

	body := Body{"foo": "bar", "rev": "1-a"}
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)

	largeDoc := make([]byte, 1000000)
	longBody := Body{"val": string(largeDoc), "rev": "1-a"}
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc2", longBody, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)

	var shortWithAttachmentsDataBody Body
	shortWithAttachmentsData := `{"test": true, "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}, "rev":"1-a"}`
	_ = base.JSONUnmarshal([]byte(shortWithAttachmentsData), &shortWithAttachmentsDataBody)
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc3", shortWithAttachmentsDataBody, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)

	b.Run("ShortLatest", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.Get1xRevBody(ctx, "doc1", "", false, nil)
		}
	})
	b.Run("LongLatest", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.Get1xRevBody(ctx, "doc2", "", false, nil)
		}
	})
	b.Run("ShortWithAttachmentsLatest", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.Get1xRevBody(ctx, "doc3", "", false, nil)
		}
	})

	updateBody := Body{"rev": "2-a"}
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc1", updateBody, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc2", updateBody, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc3", updateBody, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)

	b.Run("ShortOld", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.Get1xRevBody(ctx, "doc1", "1-a", false, nil)
		}
	})
	b.Run("LongOld", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.Get1xRevBody(ctx, "doc2", "1-a", false, nil)
		}
	})
	b.Run("ShortWithAttachmentsOld", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.Get1xRevBody(ctx, "doc3", "1-a", false, nil)
		}
	})
}

func BenchmarkDatabaseGetRev(b *testing.B) {
	base.DisableTestLogging(b)

	db, ctx := setupTestDB(b)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, b, db)

	body := Body{"foo": "bar", "rev": "1-a"}
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)

	largeDoc := make([]byte, 1000000)
	longBody := Body{"val": string(largeDoc), "rev": "1-a"}
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc2", longBody, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)

	var shortWithAttachmentsDataBody Body
	shortWithAttachmentsData := `{"test": true, "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}, "rev":"1-a"}`
	_ = base.JSONUnmarshal([]byte(shortWithAttachmentsData), &shortWithAttachmentsDataBody)
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc3", shortWithAttachmentsDataBody, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)

	b.Run("ShortLatest", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.GetRev(ctx, "doc1", "", false, nil)
		}
	})
	b.Run("LongLatest", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.GetRev(ctx, "doc2", "", false, nil)
		}
	})
	b.Run("ShortWithAttachmentsLatest", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.GetRev(ctx, "doc3", "", false, nil)
		}
	})

	updateBody := Body{"rev": "2-a"}
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc1", updateBody, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc2", updateBody, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc3", updateBody, []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)

	b.Run("ShortOld", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.GetRev(ctx, "doc1", "1-a", false, nil)
		}
	})
	b.Run("LongOld", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.GetRev(ctx, "doc2", "1-a", false, nil)
		}
	})
	b.Run("ShortWithAttachmentsOld", func(b *testing.B) {
		for b.Loop() {
			_, _ = collection.GetRev(ctx, "doc3", "1-a", false, nil)
		}
	})
}

// Replicates delta patching work carried out by handleRev
func BenchmarkHandleRevDelta(b *testing.B) {
	base.DisableTestLogging(b)

	db, ctx := setupTestDB(b)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, b, db)

	body := Body{"foo": "bar"}
	_, _, _ = collection.PutExistingRevWithBody(ctx, "doc1", body, []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)

	getDelta := func(newDoc *Document) {
		deltaSrcRev, _ := collection.GetRev(ctx, "doc1", "1-a", false, nil)

		deltaSrcBody, _ := deltaSrcRev.MutableBody()

		// Stamp attachments so we can patch them
		if len(deltaSrcRev.Attachments) > 0 {
			deltaSrcBody[BodyAttachments] = map[string]any(deltaSrcRev.Attachments)
		}

		deltaSrcMap := map[string]any(deltaSrcBody)
		_ = base.Patch(&deltaSrcMap, newDoc.Body(ctx))
	}

	b.Run("SmallDiff", func(b *testing.B) {
		newDoc := &Document{
			ID:    "doc1",
			RevID: "1a",
		}
		newDoc.UpdateBodyBytes([]byte(`{"foo": "bart"}`))
		for b.Loop() {
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
		for b.Loop() {
			getDelta(newDoc)
		}
	})
}

func TestGetAvailableRevAttachments(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Create the very first revision of the document with attachment; let's call this as rev 1-a
	payload := `{"sku":"6213100","_attachments":{"camera.txt":{"data":"Q2Fub24gRU9TIDVEIE1hcmsgSVY="}}}`
	_, rev, err := collection.PutExistingRevWithBody(ctx, "camera", unmarshalBody(t, payload), []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Couldn't create document")
	ancestor := rev // Ancestor revision

	// Create the second revision of the document with attachment reference;
	payload = `{"sku":"6213101","_attachments":{"camera.txt":{"stub":true,"revpos":1}}}`
	_, rev, err = collection.PutExistingRevWithBody(ctx, "camera", unmarshalBody(t, payload), []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	parent := rev // Immediate ancestor or parent revision
	assert.NoError(t, err, "Couldn't create document")

	payload = `{"sku":"6213102","_attachments":{"camera.txt":{"stub":true,"revpos":1}}}`
	doc, _, err := collection.PutExistingRevWithBody(ctx, "camera", unmarshalBody(t, payload), []string{"3-a", "2-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Couldn't create document")

	// Get available attachments by immediate ancestor revision or parent revision
	meta, found := collection.getAvailableRevAttachments(ctx, doc, parent)
	require.True(t, found, "Ancestor should exists")
	attachment := meta["camera.txt"].(map[string]any)
	assert.Equal(t, "sha1-VoSNiNQGHE1HirIS5HMxj6CrlHI=", attachment["digest"])
	assert.Equal(t, json.Number("20"), attachment["length"])
	assert.Equal(t, json.Number("1"), attachment["revpos"])

	// Get available attachments by immediate ancestor revision
	meta, found = collection.getAvailableRevAttachments(ctx, doc, ancestor)
	require.True(t, found, "Ancestor should exists")
	attachment = meta["camera.txt"].(map[string]any)
	assert.Equal(t, "sha1-VoSNiNQGHE1HirIS5HMxj6CrlHI=", attachment["digest"])
	assert.Equal(t, json.Number("20"), attachment["length"])
	assert.Equal(t, json.Number("1"), attachment["revpos"])
}

func TestGet1xRevAndChannels(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	docId := "dd6d2dcc679d12b9430a9787bab45b33"
	payload := `{"sku":"6213100","_attachments":{"camera.txt":{"data":"Q2Fub24gRU9TIDVEIE1hcmsgSVY="}}}`
	doc1, rev1, err := collection.PutExistingRevWithBody(ctx, docId, unmarshalBody(t, payload), []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Couldn't create document")

	payload = `{"sku":"6213101","_attachments":{"lens.txt":{"data":"Q2Fub24gRU9TIDVEIE1hcmsgSVY="}}}`
	doc2, rev2, err := collection.PutExistingRevWithBody(ctx, docId, unmarshalBody(t, payload), []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Couldn't create document")

	// Get the 1x revision from document with list revision enabled
	bodyBytes, removed, err := collection.get1xRevFromDoc(ctx, doc2, rev2, true)
	assert.False(t, removed)
	assert.NoError(t, err, "It should not throw any error")
	assert.NotNil(t, bodyBytes, "Document body bytes should be received")

	var response = Body{}
	assert.NoError(t, response.Unmarshal(bodyBytes))

	// Get the 1x revision from document with list revision enabled. Also validate that the
	// BodyRevisions property is present and correct since listRevisions=true.
	bodyBytes, removed, err = collection.get1xRevFromDoc(ctx, doc1, rev1, true)
	assert.False(t, removed)
	assert.NoError(t, err, "It should not throw any error")
	assert.NotNil(t, bodyBytes, "Document body bytes should be received")
	assert.NoError(t, response.Unmarshal(bodyBytes))
	assert.Equal(t, docId, response[BodyId])
	assert.Equal(t, "1-a", response[BodyRev])
	assert.Equal(t, "6213100", response["sku"])
	revisions, ok := response[BodyRevisions].(map[string]any)
	assert.True(t, ok, "revisions should be extracted from response body")
	assert.Equal(t, json.Number("1"), revisions[RevisionsStart])
	assert.Equal(t, []any{"a"}, revisions[RevisionsIds])

	// Delete the document, creating tombstone revision rev3
	rev3, _, err := collection.DeleteDoc(ctx, docId, DocVersion{RevTreeID: rev2})
	require.NoError(t, err)
	bodyBytes, removed, err = collection.get1xRevFromDoc(ctx, doc2, rev3, true)
	assert.False(t, removed)
	assert.Error(t, err, "It should throw 404 missing error")
	assert.Nil(t, bodyBytes, "Document body bytes should be empty")

	// get1xRevFromDoc for doc2 should be returning the current revision id (in this case, the tombstone revision rev3).
	// Also validate that the BodyRevisions property is present and correct since listRevisions=true.
	bodyBytes, removed, err = collection.get1xRevFromDoc(ctx, doc2, "", true)
	assert.False(t, removed)
	assert.NoError(t, err, "It should not throw any error")
	assert.NotNil(t, bodyBytes, "Document body bytes should be received")
	assert.NoError(t, response.Unmarshal(bodyBytes))
	assert.Equal(t, docId, response[BodyId])
	assert.Equal(t, "2-a", response[BodyRev])
	assert.Equal(t, "6213101", response["sku"])
	revisions, ok = response[BodyRevisions].(map[string]any)
	assert.True(t, ok, "revisions should be extracted from response body")
	assert.Equal(t, json.Number("2"), revisions[RevisionsStart])
	assert.Equal(t, []any{"a", "a"}, revisions[RevisionsIds])
}

func TestGet1xRevFromDoc(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// Create the first revision of the document
	docId := "356779a9a1696714480f57fa3fb66d4c"
	payload := `{"city":"Los Angeles"}`
	doc, rev1, err := collection.PutExistingRevWithBody(ctx, docId, unmarshalBody(t, payload), []string{"1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Couldn't create document")
	assert.NotEmpty(t, doc, "Document shouldn't be empty")
	assert.Equal(t, "1-a", rev1, "Provided input revision ID should be returned")

	// Get rev1 using get1xRevFromDoc. Also validate that the BodyRevisions property is present
	// and correct since listRevisions=true.
	bodyBytes, removed, err := collection.get1xRevFromDoc(ctx, doc, rev1, true)
	require.NoError(t, err)
	assert.NotEmpty(t, bodyBytes, "Document body bytes should be returned")
	assert.False(t, removed, "This shouldn't be a removed document")
	var response = Body{}
	assert.NoError(t, response.Unmarshal(bodyBytes))
	assert.Equal(t, docId, response[BodyId])
	assert.Equal(t, "1-a", response[BodyRev])
	assert.Equal(t, "Los Angeles", response["city"])
	revisions, ok := response[BodyRevisions].(map[string]any)
	assert.True(t, ok, "revisions should be extracted from response body")
	assert.Equal(t, json.Number("1"), revisions[RevisionsStart])
	assert.Equal(t, []any{"a"}, revisions[RevisionsIds])

	// Create the second revision of the document
	payload = `{"city":"Hollywood"}`
	doc, rev2, err := collection.PutExistingRevWithBody(ctx, docId, unmarshalBody(t, payload), []string{"2-a", "1-a"}, false, ExistingVersionWithUpdateToHLV)
	assert.NoError(t, err, "Couldn't create document")
	assert.NotEmpty(t, doc, "Document shouldn't be empty")
	assert.Equal(t, "2-a", rev2, "Provided input revision ID should be returned")

	// Get rev2 using get1xRevFromDoc. Also validate that the BodyRevisions property is present
	// and correct since listRevisions=true.
	bodyBytes, removed, err = collection.get1xRevFromDoc(ctx, doc, rev2, true)
	require.NoError(t, err)
	assert.NotEmpty(t, bodyBytes, "Document body bytes should be returned")
	assert.False(t, removed, "This shouldn't be a removed document")
	assert.NoError(t, response.Unmarshal(bodyBytes))
	assert.Equal(t, docId, response[BodyId])
	assert.Equal(t, "2-a", response[BodyRev])
	assert.Equal(t, "Hollywood", response["city"])
	revisions, ok = response[BodyRevisions].(map[string]any)
	assert.True(t, ok, "revisions should be extracted from response body")
	assert.Equal(t, json.Number("2"), revisions[RevisionsStart])
	assert.Equal(t, []any{"a", "a"}, revisions[RevisionsIds])

	// Get body bytes from doc with unknown revision id; it simulates the error scenario.
	// A 404 missing error should be thrown when trying get the body bytes of the document
	// which doesn't exists in the revision tree. The revision "3-a" doesn't exists in database.
	bodyBytes, removed, err = collection.get1xRevFromDoc(ctx, doc, "3-a", true)
	assert.Error(t, err, "It should throw 404 missing error")
	assert.Contains(t, err.Error(), "404 missing")
	assert.Empty(t, bodyBytes, "Provided revision doesn't exists")
	assert.False(t, removed, "This shouldn't be a removed revision")
	assert.Error(t, response.Unmarshal(bodyBytes), "Unexpected empty JSON input to body.Unmarshal")

	// Deletes the document, by adding a new revision whose _deleted property is true.
	body := Body{BodyDeleted: true, BodyRev: rev2}
	rev3, doc, err := collection.Put(ctx, docId, body)
	assert.NoError(t, err, "Document should be deleted")
	assert.NotEmpty(t, rev3, "Document revision shouldn't be empty")

	// Get the document body bytes with the tombstone revision rev3, with listRevisions=true
	// Also validate that the BodyRevisions property is present and correct.
	bodyBytes, removed, err = collection.get1xRevFromDoc(ctx, doc, rev3, true)
	require.NoError(t, err)
	assert.NotEmpty(t, bodyBytes, "Document body bytes should be returned")
	assert.False(t, removed, "This shouldn't be a removed document")
	assert.NoError(t, response.Unmarshal(bodyBytes))
	assert.Equal(t, docId, response[BodyId])
	assert.Equal(t, rev3, response[BodyRev])
	assert.Equal(t, "Hollywood", response["city"])
	revisions, ok = response[BodyRevisions].(map[string]any)
	assert.True(t, ok, "revisions should be extracted from response body")
	assert.Equal(t, json.Number("3"), revisions[RevisionsStart])
	assert.Equal(t, []any{"5464898886a6c57cd648c659f0993bb3", "a", "a"}, revisions[RevisionsIds])

	// If the provided revision ID is blank and the current revision is already deleted
	// when checking document revision history, it should throw 404 deleted error.
	bodyBytes, removed, err = collection.get1xRevFromDoc(ctx, doc, "", true)
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
				"att1": map[string]any{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			nil,
			AttachmentsMeta{
				"att1": map[string]any{
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
				"att1": map[string]any{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]any{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
		},
		{
			"disjoint set",
			AttachmentsMeta{
				"att1": map[string]any{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att2": map[string]any{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]any{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
				"att2": map[string]any{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
		},
		{
			"25Atts wins",
			AttachmentsMeta{
				"att1": map[string]any{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]any{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]any{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
		},
		{
			"docAtts wins",
			AttachmentsMeta{
				"att1": map[string]any{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]any{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]any{
					"digest": "def",
					"revpos": json.Number("6"),
					"stub":   true,
				},
			},
		},
		{
			"invalid pre25 revpos",
			AttachmentsMeta{
				"att1": map[string]any{
					"digest": "def",
					"revpos": "6",
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]any{
					"digest": "abc",
					"revpos": json.Number("4"),
					"stub":   true,
				},
			},
			AttachmentsMeta{
				"att1": map[string]any{
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

func TestGetChannelsAndAccess(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, _ := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	require.Nil(t, collection.ChannelMapper)

	doc := &Document{
		ID: "doc1",
	}

	testCases := []struct {
		body                      string
		defaultCollectionChannels base.Set
		name                      string
	}{
		{
			body:                      `{}`,
			defaultCollectionChannels: nil,
			name:                      "emptyDoc",
		},
		{
			body:                      `{"channels": "ABC"}`,
			defaultCollectionChannels: base.SetOf("ABC"),
			name:                      "ChannelsABCString",
		},
		{
			body:                      `{"channels": ["ABC"]}`,
			defaultCollectionChannels: base.SetOf("ABC"),
			name:                      "ChannelsABCArray",
		},
		{
			body:                      `{"channels": ["ABC", "DEF"]}`,
			defaultCollectionChannels: base.SetOf("ABC", "DEF"),
			name:                      "ChannelsABCDEF",
		},
		{
			body:                      `{"key": "value"}`,
			defaultCollectionChannels: nil,
			name:                      "NoChannelsInDoc",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			body := Body{}
			require.NoError(t, body.Unmarshal([]byte(test.body)))
			result, access, roles, expiry, oldJson, err := collection.getChannelsAndAccess(base.TestCtx(t), doc, body, nil, "")
			require.NoError(t, err)
			require.Equal(t, "", oldJson)
			require.Nil(t, expiry)
			require.Nil(t, expiry)
			require.Nil(t, access)
			require.Nil(t, roles)
			if collection.IsDefaultCollection() {
				require.Equal(t, test.defaultCollectionChannels, result)
			} else {
				require.Equal(t, base.SetOf(collection.Name), result)

			}
		})
	}
}

func TestKnownRevsForCheckChangeVersion(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// have three revisions on db for the doc
	revID, doc, err := collection.Put(ctx, t.Name(), Body{"some": "data"})
	require.NoError(t, err)
	for range 2 {
		revID, doc, err = collection.Put(ctx, t.Name(), Body{"some": "data", BodyRev: revID})
		require.NoError(t, err)
	}

	// call CheckChangeVersion with fake VV and the docID from above
	missing, possible := collection.CheckChangeVersion(ctx, t.Name(), "123@src")

	// assert that the missing revision is the CV and known revID is current revIDS of the document
	require.Len(t, missing, 1)
	require.Len(t, possible, 2)
	// db's CV and revID of the doc should be returned as known revs by CheckChangeVersion
	assert.Equal(t, "123@src", missing[0])
	assert.Equal(t, doc.HLV.GetCurrentVersionString(), possible[0])
	assert.Equal(t, doc.GetRevTreeID(), possible[1])
}

func TestPutStampClusterUUID(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
		t.Skip("This test only works on Couchbase Server and with XATTRS enabled")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	key := "doc1"

	body := Body{}
	err := body.Unmarshal([]byte(`{"field": "value"}`))
	require.NoError(t, err)

	_, doc, err := collection.Put(ctx, key, body)

	require.NoError(t, err)
	require.Len(t, doc.ClusterUUID, 32)

	_, xattrs, _, err := collection.dataStore.GetWithXattrs(ctx, key, []string{base.SyncXattrName})
	require.NoError(t, err)
	require.Contains(t, xattrs, base.SyncXattrName)
	var xattr map[string]any
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &xattr))
	require.Len(t, xattr["cluster_uuid"].(string), 32)
}

// TestAssignSequenceReleaseLoop repros conditions seen in CBG-3516 (where each sequence between nextSequence and docSequence has an unusedSeq doc)
func TestAssignSequenceReleaseLoop(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges, base.KeyCRUD, base.KeyDCP)

	// import disabled
	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{})
	defer db.Close(ctx)

	// positive sequence gap (other cluster's sequencing is higher)
	const otherClusterSequenceOffset = 10

	startReleasedSequenceCount := db.DbStats.Database().SequenceReleasedCount.Value()

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	rev, doc, err := collection.Put(ctx, "doc1", Body{"foo": "bar"})
	require.NoError(t, err)
	t.Logf("doc sequence: %d", doc.Sequence)

	// but we can fiddle with the sequence in the metadata of the doc write to simulate a doc from a different cluster (with a higher sequence)
	var newSyncData map[string]any
	sd, err := json.Marshal(doc.SyncData)
	require.NoError(t, err)
	err = json.Unmarshal(sd, &newSyncData)
	require.NoError(t, err)
	newSyncData["sequence"] = doc.SyncData.Sequence + otherClusterSequenceOffset
	_, err = collection.dataStore.UpdateXattrs(ctx, doc.ID, 0, doc.Cas, map[string][]byte{base.SyncXattrName: base.MustJSONMarshal(t, newSyncData)}, DefaultMutateInOpts())
	require.NoError(t, err)

	_, doc, err = collection.Put(ctx, "doc1", Body{"foo": "buzz", BodyRev: rev})
	require.NoError(t, err)
	require.Greaterf(t, doc.Sequence, uint64(otherClusterSequenceOffset), "Expected new doc sequence %d to be greater than other cluster's sequence %d", doc.Sequence, otherClusterSequenceOffset)

	// wait for the doc to be received
	err = db.changeCache.waitForSequence(ctx, doc.Sequence, time.Second*30)
	require.NoError(t, err)

	expectedReleasedSequenceCount := otherClusterSequenceOffset
	releasedSequenceCount := db.DbStats.Database().SequenceReleasedCount.Value() - startReleasedSequenceCount
	assert.Equal(t, uint64(expectedReleasedSequenceCount), releasedSequenceCount)
}

// TestReleaseSequenceOnDocWrite:
//   - Define a leaky bucket callback for a conflicting write + define key to return a timeout error for
//   - Setup db with leaky bucket config
//   - Init a channel cache by calling changes
//   - Write a doc that will return timeout error but will successfully persist
//   - Wait for it to arrive at change cache
//   - Assert we don't release a sequence for it + we have it in changes cache
//   - Write new doc with conflict error
//   - Assert we release a sequence for this
func TestReleaseSequenceOnDocWriteFailure(t *testing.T) {
	defer SuspendSequenceBatching()()

	var ctx context.Context
	var db *Database
	var forceDocConflict bool

	conflictDoc := t.Name() + "_conflict"
	timeoutDoc := t.Name() + "_timeout"

	// call back to create a conflict mid write and force a non timeout error upon write attempt
	writeUpdateCallback := func(key string) {
		if key == conflictDoc && forceDocConflict {
			forceDocConflict = false
			body := Body{"test": "doc"}
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
			_, _, err := collection.Put(ctx, conflictDoc, body)
			require.NoError(t, err)
		}
	}

	callbackConfig := base.LeakyBucketConfig{
		UpdateCallback:                writeUpdateCallback,
		ForceTimeoutErrorOnUpdateKeys: []string{timeoutDoc},
	}

	db, ctx = setupTestLeakyDBWithCacheOptions(t, DefaultCacheOptions(), callbackConfig)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// init channel cache, this will make changes call after timeout doc is written below fail pre changes made in CBG-4067,
	// due to duplicate sequence at the cache with an unused sequence. See steps in ticket CBG-4067 as example.
	_ = getChanges(t, collection, base.SetOf("*"), getChangesOptionsWithZeroSeq(t))

	assert.Equal(t, uint64(0), db.DbStats.Database().SequenceReleasedCount.Value())

	// write doc that will return timeout but will actually be persisted successfully on server
	// this mimics what was seen before
	_, _, err := collection.Put(ctx, timeoutDoc, Body{"test": "doc"})
	require.Error(t, err)

	// wait for changes
	require.NoError(t, collection.WaitForPendingChanges(ctx))

	// assert that no sequences were released + a sequence was cached
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		db.UpdateCalculatedStats(ctx)
		assert.Equal(t, uint64(0), db.DbStats.Database().SequenceReleasedCount.Value())
		assert.Equal(t, uint64(1), db.DbStats.Cache().HighSeqCached.Value())
	}, time.Second*10, time.Millisecond*100)

	// get cached changes + assert the document is present
	changes := getChanges(t, collection, base.SetOf("*"), getChangesOptionsWithZeroSeq(t))
	require.Len(t, changes, 1)
	assert.Equal(t, timeoutDoc, changes[0].ID)

	// write doc that will have a conflict error, we should expect the document sequence to be released
	forceDocConflict = true
	_, _, err = collection.Put(ctx, conflictDoc, Body{"test": "doc"})
	require.Error(t, err)

	// wait for changes
	require.NoError(t, collection.WaitForPendingChanges(ctx))

	// assert that a sequence was released after the above write error
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		db.UpdateCalculatedStats(ctx)
		assert.Equal(t, uint64(1), db.DbStats.Database().SequenceReleasedCount.Value())
	}, time.Second*10, time.Millisecond*100)
}

func TestDocUpdateCorruptSequence(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache, base.KeyChanges, base.KeyCRUD, base.KeyDCP)

	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{})
	defer db.Close(ctx)

	// create a sequence much higher than _syc:seqs value
	const corruptSequence = MaxSequencesToRelease + 1000

	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	rev, doc, err := collection.Put(ctx, "doc1", Body{"foo": "bar"})
	require.NoError(t, err)
	docRev := doc.RevID
	t.Logf("doc sequence: %d", doc.Sequence)

	// but we can fiddle with the sequence in the metadata of the doc write to simulate a doc from a different cluster (with a higher sequence)
	_, xattrs, _, err := collection.dataStore.GetWithXattrs(ctx, "doc1", []string{base.SyncXattrName})
	require.NoError(t, err)
	var newSyncData map[string]any
	err = json.Unmarshal(xattrs[base.SyncXattrName], &newSyncData)
	require.NoError(t, err)
	newSyncData["sequence"] = corruptSequence
	_, err = collection.dataStore.UpdateXattrs(ctx, doc.ID, 0, doc.Cas, map[string][]byte{base.SyncXattrName: base.MustJSONMarshal(t, newSyncData)}, DefaultMutateInOpts())
	require.NoError(t, err)

	_, _, err = collection.Put(ctx, "doc1", Body{"foo": "buzz", BodyRev: rev})
	require.Error(t, err)
	require.ErrorIs(t, err, base.ErrMaxSequenceReleasedExceeded)

	// assert update to doc was cancelled thus doc1 is its original version
	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, uint64(corruptSequence), doc.Sequence)
	assert.Equal(t, docRev, doc.RevID)

	base.RequireWaitForStat(t, func() int64 {
		return db.DbStats.Database().CorruptSequenceCount.Value()
	}, 1)
}

func TestPutResurrection(t *testing.T) {

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	startWarnCount := base.SyncGatewayStats.GlobalStats.ResourceUtilization.WarnCount.Value()
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	const docID = "doc1"
	_, _, err := collection.Put(ctx, docID, Body{"foo": "bar"})
	require.NoError(t, err)

	require.NoError(t, collection.Purge(ctx, docID, false))
	// assert no warnings when re-pushing a resurrection
	_, _, err = collection.Put(ctx, docID, Body{"resurrect": true})
	require.NoError(t, err)

	require.Equal(t, startWarnCount, base.SyncGatewayStats.GlobalStats.ResourceUtilization.WarnCount.Value())
}

// TestPutExistingCurrentVersion:
//   - Put a document in a db
//   - Assert on the update to HLV after that PUT
//   - Construct a HLV to represent the doc created locally being updated on a client
//   - Call PutExistingCurrentVersion simulating doc update arriving over replicator
//   - Assert that the doc's HLV in the bucket has been updated correctly with the CV, PV and cvCAS
func TestPutExistingCurrentVersion(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	bucketUUID := db.EncodedSourceID
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create a new doc
	key := "doc1"
	body := Body{"key1": "value1"}

	rev, _, err := collection.Put(ctx, key, body)
	require.NoError(t, err)

	// assert on HLV on that above PUT
	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalSync)
	assert.NoError(t, err)
	assert.Equal(t, bucketUUID, doc.HLV.SourceID)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.Version)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.CurrentVersionCAS)

	// store the cas version allocated to the above doc creation for creation of incoming HLV later in test
	originalDocVersion := doc.HLV.Version

	// PUT an update to the above doc
	body = Body{"key1": "value11"}
	body[BodyRev] = rev
	_, _, err = collection.Put(ctx, key, body)
	require.NoError(t, err)

	// grab the new version for the above update to assert against later in test
	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalSync)
	require.NoError(t, err)
	docUpdateVersion := doc.HLV.Version
	docUpdateVersionInt := docUpdateVersion
	currentSourceID := doc.HLV.SourceID

	// construct a mock doc update coming over a replicator
	body = Body{"key1": "value2"}
	newDoc := CreateTestDocument(key, "", body, false, 0)

	// Simulate a conflicting doc update happening from a client that
	// has only replicated the initial version of the document
	pv := make(HLVVersions)
	pv[currentSourceID] = originalDocVersion

	// create a version larger than the allocated version above
	incomingVersion := docUpdateVersionInt + 10
	incomingHLV := &HybridLogicalVector{
		SourceID:         "test",
		Version:          incomingVersion,
		PreviousVersions: pv,
	}
	opts := PutDocOptions{
		NewDoc:    newDoc,
		NewDocHLV: incomingHLV,
	}
	doc, cv, _, err := collection.PutExistingCurrentVersion(ctx, opts)
	assertHTTPError(t, err, 409)
	require.Nil(t, doc)
	require.Nil(t, cv)

	// Update the client's HLV to include the latest SGW version.
	incomingHLV.PreviousVersions[currentSourceID] = docUpdateVersion
	// TODO: because expectedCurrentRev isn't being updated, storeOldBodyInRevTreeAndUpdateCurrent isn't
	//  updating the document body.   Need to review whether it makes sense to keep using
	// storeOldBodyInRevTreeAndUpdateCurrent, or if this needs a larger overhaul to support VV
	doc, cv, _, err = collection.PutExistingCurrentVersion(ctx, opts)
	require.NoError(t, err)
	assert.Equal(t, "test", cv.SourceID)
	assert.Equal(t, incomingVersion, cv.Value)
	assert.Equal(t, []byte(`{"key1":"value2"}`), doc._rawBody)

	// assert on the sync data from the above update to the doc
	// CV should be equal to CV of update on client but the cvCAS should be updated with the new update and
	// PV should contain the old CV pair
	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalSync)
	assert.NoError(t, err)

	assert.Equal(t, "test", doc.HLV.SourceID)
	assert.Equal(t, incomingVersion, doc.HLV.Version)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.CurrentVersionCAS)
	// update the pv map so we can assert we have correct pv map in HLV
	pv[bucketUUID] = docUpdateVersion
	assert.True(t, reflect.DeepEqual(doc.HLV.PreviousVersions, pv))
	assert.Equal(t, "3-60b024c44c283b369116c2c2570e8088", doc.SyncData.GetRevTreeID())

	// Attempt to push the same client update, validate server rejects as an already known version and cancels the update.
	// This case doesn't return error, verify that SyncData hasn't been changed.
	_, _, _, err = collection.PutExistingCurrentVersion(ctx, opts)
	require.NoError(t, err)
	syncData2, err := collection.GetDocSyncData(ctx, "doc1")
	require.NoError(t, err)
	require.Equal(t, doc.SyncData.TimeSaved, syncData2.TimeSaved)
	require.Equal(t, doc.SyncData.GetRevTreeID(), syncData2.GetRevTreeID())

}

// TestPutExistingCurrentVersionWithConflict:
//   - Put a document in a db
//   - Assert on the update to HLV after that PUT
//   - Construct a HLV to represent the doc created locally being updated on a client
//   - Call PutExistingCurrentVersion simulating doc update arriving over replicator
//   - Assert conflict between the local HLV for the doc and the incoming mutation is correctly identified
//   - Assert that the doc's HLV in the bucket hasn't been updated
func TestPutExistingCurrentVersionWithConflict(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	bucketUUID := db.EncodedSourceID
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create a new doc
	key := "doc1"
	body := Body{"key1": "value1"}

	_, _, err := collection.Put(ctx, key, body)
	require.NoError(t, err)

	// assert on the HLV values after the above creation of the doc
	doc, err := collection.GetDocument(ctx, "doc1", DocUnmarshalSync)
	require.NoError(t, err)
	assert.Equal(t, bucketUUID, doc.HLV.SourceID)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.Version)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.CurrentVersionCAS)

	// create a new doc update to simulate a doc update arriving over replicator from, client
	body = Body{"key1": "value2"}
	newDoc := CreateTestDocument(key, "", body, false, 0)
	incomingHLV := &HybridLogicalVector{
		SourceID: "test",
		Version:  1234,
	}

	opts := PutDocOptions{
		NewDoc:    newDoc,
		NewDocHLV: incomingHLV,
	}
	// assert that a conflict is correctly identified and the doc and cv are nil
	doc, cv, _, err := collection.PutExistingCurrentVersion(ctx, opts)
	assertHTTPError(t, err, 409)
	require.Nil(t, doc)
	require.Nil(t, cv)

	// assert persisted doc hlv hasn't been updated
	doc, err = collection.GetDocument(ctx, "doc1", DocUnmarshalSync)
	assert.NoError(t, err)
	assert.Equal(t, bucketUUID, doc.HLV.SourceID)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.Version)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.CurrentVersionCAS)
}

// TestPutExistingCurrentVersionWithNoExistingDoc:
//   - Purpose of this test is to test PutExistingRevWithBody code pathway where an
//     existing doc is not provided from the bucket into the function simulating a new, not seen
//     before doc entering this code path
func TestPutExistingCurrentVersionWithNoExistingDoc(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	bucketUUID := db.BucketUUID
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// construct a mock doc update coming over a replicator
	body := Body{"key1": "value2"}
	newDoc := CreateTestDocument("doc2", "", body, false, 0)

	// construct a HLV that simulates a doc update happening on a client
	// this means moving the current source version pair to PV and adding new sourceID and version pair to CV
	pv := make(HLVVersions)
	pv[bucketUUID] = uint64(2)
	// create a version larger than the allocated version above
	incomingVersion := uint64(2 + 10)
	incomingHLV := &HybridLogicalVector{
		SourceID:         "test",
		Version:          incomingVersion,
		PreviousVersions: pv,
	}
	opts := PutDocOptions{
		NewDoc:      newDoc,
		NewDocHLV:   incomingHLV,
		ExistingDoc: &sgbucket.BucketDocument{},
	}
	// call PutExistingCurrentVersion with empty existing doc
	doc, cv, _, err := collection.PutExistingCurrentVersion(ctx, opts)
	require.NoError(t, err)
	assert.NotNil(t, doc)
	// assert on returned CV value
	assert.Equal(t, "test", cv.SourceID)
	assert.Equal(t, incomingVersion, cv.Value)
	assert.Equal(t, []byte(`{"key1":"value2"}`), doc._rawBody)

	// assert on the sync data from the above update to the doc
	// CV should be equal to CV of update on client but the cvCAS should be updated with the new update and
	// PV should contain the old CV pair
	doc, err = collection.GetDocument(ctx, "doc2", DocUnmarshalSync)
	assert.NoError(t, err)
	assert.Equal(t, "test", doc.HLV.SourceID)
	assert.Equal(t, incomingVersion, doc.HLV.Version)
	assert.Equal(t, base.HexCasToUint64(doc.SyncData.Cas), doc.HLV.CurrentVersionCAS)
	// update the pv map so we can assert we have correct pv map in HLV
	assert.True(t, reflect.DeepEqual(doc.HLV.PreviousVersions, pv))
	assert.Equal(t, "1-3a208ea66e84121b528f05b5457d1134", doc.SyncData.GetRevTreeID())
}

// TestGetCVWithDocResidentInCache:
//   - Two test cases, one with doc a user will have access to, one without
//   - Purpose is to have a doc that is resident in rev cache and use the GetCV function to retrieve these docs
//   - Assert that the doc the user has access to is corrected fetched
//   - Assert the doc the user doesn't have access to is fetched but correctly redacted
func TestGetCVWithDocResidentInCache(t *testing.T) {
	const docID = "doc1"

	testCases := []struct {
		name        string
		docChannels []string
		access      bool
	}{
		{
			name:        "getCVWithUserAccess",
			docChannels: []string{"A"},
			access:      true,
		},
		{
			name:        "getCVWithoutUserAccess",
			docChannels: []string{"B"},
			access:      false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db, ctx := setupTestDB(t)
			defer db.Close(ctx)
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
			collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

			// Create a user with access to channel A
			authenticator := db.Authenticator(base.TestCtx(t))
			user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "A"))
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(user))
			collection.user, err = authenticator.GetUser("alice")
			require.NoError(t, err)

			// create doc with the channels for the test case
			docBody := Body{"channels": testCase.docChannels}
			rev, doc, err := collection.Put(ctx, docID, docBody)
			require.NoError(t, err)

			vrs := doc.HLV.Version
			src := doc.HLV.SourceID
			sv := &Version{Value: vrs, SourceID: src}
			revision, err := collection.GetCV(ctx, docID, sv, false)
			require.NoError(t, err)
			if testCase.access {
				assert.Equal(t, rev, revision.RevID)
				assert.Equal(t, sv, revision.CV)
				assert.Equal(t, docID, revision.DocID)
				assert.Equal(t, []byte(`{"channels":["A"]}`), revision.BodyBytes)
			} else {
				assert.Equal(t, rev, revision.RevID)
				assert.Equal(t, sv, revision.CV)
				assert.Equal(t, docID, revision.DocID)
				assert.Equal(t, []byte(RemovedRedactedDocument), revision.BodyBytes)
			}
		})
	}
}

// TestGetByCVForDocNotResidentInCache:
//   - Setup db with rev cache size of 1
//   - Put two docs forcing eviction of the first doc
//   - Use GetCV function to fetch the first doc, forcing the rev cache to load the doc from bucket
//   - Assert the doc revision fetched is correct to the first doc we created
func TestGetByCVForDocNotResidentInCache(t *testing.T) {
	t.Skip("")

	db, ctx := SetupTestDBWithOptions(t, DatabaseContextOptions{
		RevisionCacheOptions: &RevisionCacheOptions{
			MaxItemCount: 1,
		},
	})
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

	// Create a user with access to channel A
	authenticator := db.Authenticator(base.TestCtx(t))
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "A"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))
	collection.user, err = authenticator.GetUser("alice")
	require.NoError(t, err)

	const (
		doc1ID = "doc1"
		doc2ID = "doc2"
	)

	revBody := Body{"channels": []string{"A"}}
	rev, doc, err := collection.Put(ctx, doc1ID, revBody)
	require.NoError(t, err)

	// put another doc that should evict first doc from cache
	_, _, err = collection.Put(ctx, doc2ID, revBody)
	require.NoError(t, err)

	// get by CV should force a load from bucket and have a cache miss
	vrs := doc.HLV.Version
	src := doc.HLV.SourceID
	sv := &Version{Value: vrs, SourceID: src}
	revision, err := collection.GetCV(ctx, doc1ID, sv, false)
	require.NoError(t, err)

	// assert the fetched doc is the first doc we added and assert that we did in fact get cache miss
	assert.Equal(t, int64(1), db.DbStats.Cache().RevisionCacheMisses.Value())
	assert.Equal(t, rev, revision.RevID)
	assert.Equal(t, sv, revision.CV)
	assert.Equal(t, doc1ID, revision.DocID)
	assert.Equal(t, []byte(`{"channels":["A"]}`), revision.BodyBytes)
}

// TestGetCVActivePathway:
//   - Two test cases, one with doc a user will have access to, one without
//   - Purpose is top specify nil CV to the GetCV function to force the GetActive code pathway
//   - Assert doc that is created is fetched correctly when user has access to doc
//   - Assert that correct error is returned when user has no access to the doc
func TestGetCVActivePathway(t *testing.T) {
	const docID = "doc1"

	testCases := []struct {
		name        string
		docChannels []string
		access      bool
	}{
		{
			name:        "activeFetchWithUserAccess",
			docChannels: []string{"A"},
			access:      true,
		},
		{
			name:        "activeFetchWithoutUserAccess",
			docChannels: []string{"B"},
			access:      false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			db, ctx := setupTestDB(t)
			defer db.Close(ctx)
			collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
			collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, db.Options.JavascriptTimeout)

			// Create a user with access to channel A
			authenticator := db.Authenticator(base.TestCtx(t))
			user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "A"))
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(user))
			collection.user, err = authenticator.GetUser("alice")
			require.NoError(t, err)

			// test get active path by specifying nil cv
			revBody := Body{"channels": testCase.docChannels}
			rev, doc, err := collection.Put(ctx, docID, revBody)
			require.NoError(t, err)
			revision, err := collection.GetCV(ctx, docID, nil, false)

			if testCase.access == true {
				require.NoError(t, err)
				vrs := doc.HLV.Version
				src := doc.HLV.SourceID
				sv := &Version{Value: vrs, SourceID: src}
				assert.Equal(t, rev, revision.RevID)
				assert.Equal(t, sv, revision.CV)
				assert.Equal(t, docID, revision.DocID)
				assert.Equal(t, []byte(`{"channels":["A"]}`), revision.BodyBytes)
			} else {
				require.Error(t, err)
				assert.ErrorContains(t, err, ErrForbidden.Error())
				assert.Equal(t, DocumentRevision{}, revision)
			}
		})
	}
}

// createNewTestDocument creates a valid document for testing.
func createNewTestDocument(t *testing.T, db *Database, body []byte) *Document {
	collection, ctx := GetSingleDatabaseCollectionWithUser(base.TestCtx(t), t, db)
	ctx = base.UserLogCtx(ctx, "gotest", base.UserDomainBuiltin, nil)
	ctx = base.DatabaseLogCtx(ctx, db.Name, nil)
	name := SafeDocumentName(t, t.Name())
	var b Body
	require.NoError(t, base.JSONUnmarshal(body, &b))
	_, _, err := collection.Put(ctx, name, b)
	require.NoError(t, err)
	doc, err := collection.GetDocument(ctx, name, DocUnmarshalAll)
	require.NoError(t, err)
	require.NotNil(t, doc.HLV)
	require.NotEmpty(t, doc.RevAndVersion.CurrentSource)
	require.NotEmpty(t, doc.RevAndVersion.CurrentVersion)
	return doc
}

func TestIsSGWrite(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport)
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	body := []byte(`{"some":"data"}`)
	testCases := []struct {
		name    string
		docBody []byte
	}{
		{
			name:    "normal body",
			docBody: body,
		},
		{
			name:    "nil body",
			docBody: nil,
		},
	}

	t.Run("standard Put", func(t *testing.T) {
		doc := createNewTestDocument(t, db, body)
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				isSGWrite, _, _ := doc.IsSGWrite(ctx, testCase.docBody)
				require.True(t, isSGWrite, "Expected doc to be identified as SG write for body %q", string(testCase.docBody))
			})
		}
	})
	t.Run("no HLV", func(t *testing.T) {
		// falls back to body crc32 comparison
		doc := createNewTestDocument(t, db, body)
		doc.HLV = nil
		doc.Cas = 1 // force mismatch cas
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				isSGWrite, _, _ := doc.IsSGWrite(ctx, testCase.docBody)
				require.True(t, isSGWrite, "Expected doc to be identified an SDK write for body %q", string(testCase.docBody))
			})
		}
	})
	t.Run("no _sync.rev.src", func(t *testing.T) {
		// this is a corrupt _sync.rev, so assume that it was a _vv at some point and import just in case
		doc := createNewTestDocument(t, db, body)
		doc.RevAndVersion.CurrentSource = ""
		doc.Cas = 1 // force mismatch cas
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				isSGWrite, _, _ := doc.IsSGWrite(ctx, testCase.docBody)
				require.False(t, isSGWrite, "Expected doc not to be identified as SG write for body %q since _sync.rev.src is empty", string(testCase.docBody))
			})
		}
	})
	t.Run("no _sync.rev.ver", func(t *testing.T) {
		doc := createNewTestDocument(t, db, body)
		doc.RevAndVersion.CurrentVersion = ""
		doc.Cas = 1 // force mismatch cas
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				isSGWrite, _, _ := doc.IsSGWrite(ctx, testCase.docBody)
				require.False(t, isSGWrite, "Expected doc not to be identified as SG write for body %q since _sync.rev.ver is empty", string(testCase.docBody))
			})
		}
	})
	t.Run("mismatch sync.rev.ver", func(t *testing.T) {
		doc := createNewTestDocument(t, db, body)
		doc.RevAndVersion.CurrentVersion = "0x1234"
		doc.Cas = 1 // force mismatch cas
		for _, testCase := range testCases {
			t.Run(testCase.name, func(t *testing.T) {
				isSGWrite, _, _ := doc.IsSGWrite(ctx, testCase.docBody)
				require.False(t, isSGWrite, "Expected doc to not be identified as SG write for body %q due to mismatched _sync.rev.ver", string(testCase.docBody))
			})
		}
	})

}

func TestSyncDataCVEqual(t *testing.T) {
	testCases := []struct {
		name       string
		syncData   SyncData
		cvSourceID string
		cvValue    uint64
		cvEqual    bool
	}{
		{
			name: "syncData CV matches",
			syncData: SyncData{
				RevAndVersion: channels.RevAndVersion{
					CurrentSource:  "testSourceID",
					CurrentVersion: "0x0100000000000000",
				},
			},
			cvSourceID: "testSourceID",
			cvValue:    1,
			cvEqual:    true,
		},
		{
			name: "syncData sourceID mismatch",
			syncData: SyncData{
				RevAndVersion: channels.RevAndVersion{
					CurrentSource:  "testSourceID",
					CurrentVersion: "0x1",
				},
			},
			cvSourceID: "testSourceID2",
			cvValue:    1,
			cvEqual:    false,
		},
		{
			name: "syncData sourceID mismatch",
			syncData: SyncData{
				RevAndVersion: channels.RevAndVersion{
					CurrentSource:  "testSourceID",
					CurrentVersion: "0x2",
				},
			},
			cvSourceID: "testSourceID",
			cvValue:    1,
			cvEqual:    false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cv := Version{
				SourceID: testCase.cvSourceID,
				Value:    testCase.cvValue,
			}
			require.Equal(t, testCase.cvEqual, testCase.syncData.CVEqual(cv))
		})
	}
}

func TestProposedRev(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)
	collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)

	// create 3 documents
	const (
		SingleRevDoc  = "SingleRevDoc"
		MultiRevDoc   = "MultiRevDoc"
		TombstonedDoc = "TombstonedDoc"
	)
	body := Body{"key1": "value1", "key2": 1234}
	_, doc1, err := collection.Put(ctx, SingleRevDoc, body)
	require.NoError(t, err)
	doc1Rev := doc1.GetRevTreeID()

	_, doc2, err := collection.Put(ctx, MultiRevDoc, body)
	require.NoError(t, err)
	doc2Rev1 := doc2.GetRevTreeID()
	_, doc2, err = collection.Put(ctx, MultiRevDoc, Body{"_rev": doc2Rev1, "key1": "value2", "key2": 5678})
	require.NoError(t, err)
	doc2Rev2 := doc2.GetRevTreeID()
	_, doc2, err = collection.Put(ctx, MultiRevDoc, Body{"_rev": doc2Rev2, "key1": "value3", "key2": 91011})
	require.NoError(t, err)
	doc2Rev3 := doc2.GetRevTreeID()

	_, doc3, err := collection.Put(ctx, TombstonedDoc, body)
	require.NoError(t, err)
	doc3Rev1 := doc3.GetRevTreeID()
	_, _, err = collection.Put(ctx, TombstonedDoc, Body{"_rev": doc3Rev1, "_deleted": true})
	require.NoError(t, err)

	testCases := []struct {
		name               string
		revID              string
		parentRevID        string
		expectedStatus     ProposedRevStatus
		expectedCurrentRev string
		docID              string
	}{
		{
			name:               "no_existing_document-curr_rev-no_parent",
			revID:              "1-abc",
			parentRevID:        "",
			expectedStatus:     ProposedRev_OK_IsNew,
			expectedCurrentRev: "",
			docID:              "doc",
		},
		{
			name:               "no_existing_document-curr_rev-with_parent",
			revID:              "2-def",
			parentRevID:        "1-abc",
			expectedStatus:     ProposedRev_OK_IsNew,
			expectedCurrentRev: "",
			docID:              "doc",
		},
		{
			name:               "one_rev_doc-curr_rev-without_parent",
			revID:              doc1Rev,
			parentRevID:        "",
			expectedStatus:     ProposedRev_Exists,
			expectedCurrentRev: "",
			docID:              SingleRevDoc,
		},
		{
			name:               "one_rev_doc-incorrect_curr_rev-without_parent",
			revID:              "1-abc",
			parentRevID:        "",
			expectedStatus:     ProposedRev_Conflict,
			expectedCurrentRev: doc1Rev,
			docID:              SingleRevDoc,
		},
		{
			name:               "one_rev_doc-new_curr_rev-without_parent",
			revID:              "2-abc",
			parentRevID:        doc1Rev,
			expectedStatus:     ProposedRev_OK,
			expectedCurrentRev: "",
			docID:              SingleRevDoc,
		},
		{
			name:               "multi_rev_doc-rev1-without_parent",
			revID:              doc2Rev1,
			parentRevID:        "",
			expectedStatus:     ProposedRev_Conflict,
			expectedCurrentRev: doc2Rev3,
			docID:              MultiRevDoc,
		},
		{
			name:               "multi_rev_doc-rev2-with_rev1_parent",
			revID:              doc2Rev2,
			parentRevID:        doc2Rev1,
			expectedStatus:     ProposedRev_Conflict,
			expectedCurrentRev: doc2Rev3,
			docID:              MultiRevDoc,
		},
		{
			name:               "multi_rev_doc-rev2-with_incorrect_parent",
			revID:              doc2Rev2,
			parentRevID:        "1-abc",
			expectedStatus:     ProposedRev_Conflict,
			expectedCurrentRev: doc2Rev3,
			docID:              MultiRevDoc,
		},
		{
			name:               "multi_rev_doc-rev2-without_parent",
			revID:              doc2Rev2,
			parentRevID:        "",
			expectedStatus:     ProposedRev_Conflict,
			expectedCurrentRev: doc2Rev3,
			docID:              MultiRevDoc,
		},
		{
			name:               "multi_rev_doc-conflicting_rev2-with_parent",
			revID:              "2-abc",
			parentRevID:        doc2Rev1,
			expectedStatus:     ProposedRev_Conflict,
			expectedCurrentRev: doc2Rev3,
			docID:              MultiRevDoc,
		},
		{
			name:               "multi_rev_doc-conflicting_rev3-without_parent",
			revID:              doc2Rev3,
			parentRevID:        "",
			expectedStatus:     ProposedRev_Exists,
			expectedCurrentRev: "",
			docID:              MultiRevDoc,
		},
		{
			name:               "multi_rev_doc-conflicting_rev3-with_parent",
			revID:              doc2Rev3,
			parentRevID:        doc2Rev2,
			expectedStatus:     ProposedRev_Exists,
			expectedCurrentRev: "",
			docID:              MultiRevDoc,
		},
		{
			name:               "multi_rev_doc-conflicting_rev3-with_incorrect_parent",
			revID:              doc2Rev3,
			parentRevID:        doc2Rev1,
			expectedStatus:     ProposedRev_Exists,
			expectedCurrentRev: "",
			docID:              MultiRevDoc,
		},
		{
			name:               "multi_rev_doc-conflicting_incorrect_rev3-with_parent",
			revID:              "3-abc",
			parentRevID:        doc2Rev2,
			expectedStatus:     ProposedRev_Conflict,
			expectedCurrentRev: doc2Rev3,
			docID:              MultiRevDoc,
		},
		{
			name:               "new revision with previous revision as tombstone",
			revID:              "1-abc",
			parentRevID:        "",
			expectedStatus:     ProposedRev_OK,
			expectedCurrentRev: "",
			docID:              TombstonedDoc,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			status, rev := collection.CheckProposedRev(ctx, tc.docID, tc.revID, tc.parentRevID)
			assert.Equal(t, tc.expectedStatus, status)
			assert.Equal(t, tc.expectedCurrentRev, rev)
		})
	}
}
