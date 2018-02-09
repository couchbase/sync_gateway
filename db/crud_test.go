package db

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
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
		_, getErr := bucket.GetWithXattr(key, KSyncXattrName, &rawDoc, &rawXattr)
		if getErr != nil {
			return revTreeList{}, getErr
		}

		var treeMeta treeMeta
		err := json.Unmarshal(rawXattr, &treeMeta)
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
		err = json.Unmarshal(rawDoc, &doc)
		return doc.Meta.RevTree, err
	}

}

// TestRevisionStorageConflictAndTombstones
// Tests permutations of inline and external storage of conflicts and tombstones
func TestRevisionStorageConflictAndTombstones(t *testing.T) {

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	base.TestExternalRevStorage = true

	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 2-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	assertNoError(t, db.PutExistingRev("doc1", body, []string{"1-a"}, false), "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a with a large body")
	rev2a_body := Body{}
	rev2a_body["key1"] = prop_1000_bytes
	rev2a_body["version"] = "2a"
	assertNoError(t, db.PutExistingRev("doc1", rev2a_body, []string{"2-a", "1-a"}, false), "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, rev2a_body)

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	log.Printf("Create rev 2-b with a large body")
	rev2b_body := Body{}
	rev2b_body["key1"] = prop_1000_bytes
	rev2b_body["version"] = "2b"
	assertNoError(t, db.PutExistingRev("doc1", rev2b_body, []string{"2-b", "1-a"}, false), "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-b")
	gotbody, err = db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, rev2b_body)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err := getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assertNoError(t, err, "Couldn't get revtree for raw document")
	assert.Equals(t, len(revTree.BodyMap), 0)
	assert.Equals(t, len(revTree.BodyKeyMap), 1)

	// Retrieve the raw revision body backup of 2-a, and verify it's intact
	log.Printf("Verify document storage of 2-a")
	var revisionBody Body
	rawRevision, _, err := db.Bucket.GetRaw("_sync:rb:4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	assertNoError(t, err, "Couldn't get raw backup revision")
	json.Unmarshal(rawRevision, &revisionBody)
	assert.Equals(t, revisionBody["version"], rev2a_body["version"])
	assert.Equals(t, revisionBody["value"], rev2a_body["value"])

	// Retrieve the non-inline revision
	db.FlushRevisionCache()
	rev2aGet, err := db.GetRev("doc1", "2-a", false, nil)
	assertNoError(t, err, "Couldn't get rev 2-a")
	assert.DeepEquals(t, rev2aGet, rev2a_body)

	// Tombstone 2-b (with rev 3-b, minimal tombstone)
	//    1-a
	//   /  \
	// 2-a  2-b
	//       |
	//      3-b(t)

	log.Printf("Create tombstone 3-b")
	rev3b_body := Body{}
	rev3b_body["version"] = "3b"
	rev3b_body["_deleted"] = true
	assertNoError(t, db.PutExistingRev("doc1", rev3b_body, []string{"3-b", "2-b"}, false), "add 3-b (tombstone)")

	// Retrieve tombstone
	rev3bGet, err := db.GetRev("doc1", "3-b", false, nil)
	assertNoError(t, err, "Couldn't get rev 3-b")
	assert.DeepEquals(t, rev3bGet, rev3b_body)

	// Retrieve the document, validate that we get 2-a
	log.Printf("Retrieve doc, expect 2-a")
	gotbody, err = db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, rev2a_body)

	// Ensure previous revision body backup has been removed
	_, _, err = db.Bucket.GetRaw("_sync:rb:4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	assertTrue(t, base.IsKeyNotFoundError(db.Bucket, err), "Revision should be not found")

	// Validate the tombstone is stored inline (due to small size)
	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assertNoError(t, err, "Couldn't get revtree for raw document")
	assert.Equals(t, len(revTree.BodyMap), 1)
	assert.Equals(t, len(revTree.BodyKeyMap), 0)

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
	assertNoError(t, db.PutExistingRev("doc1", rev2c_body, []string{"2-c", "1-a"}, false), "add 2-c")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-c")
	gotbody, err = db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, rev2c_body)

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
	rev3c_body["_deleted"] = true
	assertNoError(t, db.PutExistingRev("doc1", rev3c_body, []string{"3-c", "2-c"}, false), "add 3-c (large tombstone)")

	// Validate the tombstone is not stored inline (due to small size)
	log.Printf("Verify raw revtree w/ tombstone 3-c in key map")
	newRevTree, err := getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assertNoError(t, err, "Couldn't get revtree for raw document")
	assert.Equals(t, len(newRevTree.BodyMap), 1)    // tombstone 3-b
	assert.Equals(t, len(newRevTree.BodyKeyMap), 1) // tombstone 3-c

	// Retrieve the non-inline tombstone revision
	db.FlushRevisionCache()
	rev3cGet, err := db.GetRev("doc1", "3-c", false, nil)
	assertNoError(t, err, "Couldn't get rev 3-c")
	assert.DeepEquals(t, rev3cGet, rev3c_body)

	log.Printf("Retrieve doc, verify active rev is 2-a")
	gotbody, err = db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, rev2a_body)

	// Add active revision, ensure all revisions remain intact
	log.Printf("Create rev 3-a with a large body")
	rev3a_body := Body{}
	rev3a_body["key1"] = prop_1000_bytes
	rev3a_body["version"] = "3a"
	assertNoError(t, db.PutExistingRev("doc1", rev2c_body, []string{"3-a", "2-a"}, false), "add 3-a")

	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assertNoError(t, err, "Couldn't get revtree for raw document")
	assert.Equals(t, len(revTree.BodyMap), 1)    // tombstone 3-b
	assert.Equals(t, len(revTree.BodyKeyMap), 1) // tombstone 3-c
}

// TestRevisionStoragePruneTombstone - tests cleanup of external tombstone bodies when pruned.
func TestRevisionStoragePruneTombstone(t *testing.T) {

	db, testBucket := setupTestDBWithCacheOptions(t, CacheOptions{})
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	base.TestExternalRevStorage = true

	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 2-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	assertNoError(t, db.PutExistingRev("doc1", body, []string{"1-a"}, false), "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a with a large body")
	rev2a_body := Body{}
	rev2a_body["key1"] = prop_1000_bytes
	rev2a_body["version"] = "2a"
	assertNoError(t, db.PutExistingRev("doc1", rev2a_body, []string{"2-a", "1-a"}, false), "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, rev2a_body)

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	log.Printf("Create rev 2-b with a large body")
	rev2b_body := Body{}
	rev2b_body["key1"] = prop_1000_bytes
	rev2b_body["version"] = "2b"
	assertNoError(t, db.PutExistingRev("doc1", rev2b_body, []string{"2-b", "1-a"}, false), "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-b")
	gotbody, err = db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, rev2b_body)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err := getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assertNoError(t, err, "Couldn't get revtree for raw document")
	assert.Equals(t, len(revTree.BodyMap), 0)
	assert.Equals(t, len(revTree.BodyKeyMap), 1)

	// Retrieve the raw revision body backup of 2-a, and verify it's intact
	log.Printf("Verify document storage of 2-a")
	var revisionBody Body
	rawRevision, _, err := db.Bucket.GetRaw("_sync:rb:4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	assertNoError(t, err, "Couldn't get raw backup revision")
	json.Unmarshal(rawRevision, &revisionBody)
	assert.Equals(t, revisionBody["version"], rev2a_body["version"])
	assert.Equals(t, revisionBody["value"], rev2a_body["value"])

	// Retrieve the non-inline revision
	db.FlushRevisionCache()
	rev2aGet, err := db.GetRev("doc1", "2-a", false, nil)
	assertNoError(t, err, "Couldn't get rev 2-a")
	assert.DeepEquals(t, rev2aGet, rev2a_body)

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
	rev3b_body["_deleted"] = true
	assertNoError(t, db.PutExistingRev("doc1", rev3b_body, []string{"3-b", "2-b"}, false), "add 3-b (tombstone)")

	// Retrieve tombstone
	db.FlushRevisionCache()
	rev3bGet, err := db.GetRev("doc1", "3-b", false, nil)
	assertNoError(t, err, "Couldn't get rev 3-b")
	assert.DeepEquals(t, rev3bGet, rev3b_body)

	// Retrieve the document, validate that we get 2-a
	log.Printf("Retrieve doc, expect 2-a")
	gotbody, err = db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, rev2a_body)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assertNoError(t, err, "Couldn't get revtree for raw document")
	assert.Equals(t, len(revTree.BodyMap), 0)
	assert.Equals(t, len(revTree.BodyKeyMap), 1)
	log.Printf("revTree.BodyKeyMap:%v", revTree.BodyKeyMap)

	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	log.Printf("revtree before additional revisions: %v", revTree.BodyKeyMap)

	// Add revisions until 3-b is pruned
	db.RevsLimit = 5
	activeRevBody := Body{}
	activeRevBody["version"] = "...a"
	activeRevBody["key1"] = prop_1000_bytes
	assertNoError(t, db.PutExistingRev("doc1", activeRevBody, []string{"3-a", "2-a"}, false), "add 3-a")
	assertNoError(t, db.PutExistingRev("doc1", activeRevBody, []string{"4-a", "3-a"}, false), "add 4-a")
	assertNoError(t, db.PutExistingRev("doc1", activeRevBody, []string{"5-a", "4-a"}, false), "add 5-a")
	assertNoError(t, db.PutExistingRev("doc1", activeRevBody, []string{"6-a", "5-a"}, false), "add 6-a")
	assertNoError(t, db.PutExistingRev("doc1", activeRevBody, []string{"7-a", "6-a"}, false), "add 7-a")
	assertNoError(t, db.PutExistingRev("doc1", activeRevBody, []string{"8-a", "7-a"}, false), "add 8-a")

	// Verify that 3-b is still present at this point
	db.FlushRevisionCache()
	rev3bGet, err = db.GetRev("doc1", "3-b", false, nil)
	assertNoError(t, err, "Rev 3-b should still exist")

	// Add one more rev that triggers pruning since gen(9-3) > revsLimit
	assertNoError(t, db.PutExistingRev("doc1", activeRevBody, []string{"9-a", "8-a"}, false), "add 9-a")

	// Verify that 3-b has been pruned
	log.Printf("Attempt to retrieve 3-b, expect pruned")
	db.FlushRevisionCache()
	rev3bGet, err = db.GetRev("doc1", "3-b", false, nil)
	assert.Equals(t, err.Error(), "404 missing")

	// Ensure previous tombstone body backup has been removed
	log.Printf("Verify revision body doc has been removed from bucket")
	_, _, err = db.Bucket.GetRaw("_sync:rb:ULDLuEgDoKFJeET2hojeFANXM8SrHdVfAGONki+kPxM=")
	assertTrue(t, base.IsKeyNotFoundError(db.Bucket, err), "Revision should be not found")

}
