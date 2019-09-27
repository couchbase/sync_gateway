package db

import (
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
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
		_, getErr := bucket.GetWithXattr(key, base.SyncXattrName, &rawDoc, &rawXattr)
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

// TestRevisionStorageConflictAndTombstones
// Tests permutations of inline and external storage of conflicts and tombstones
func TestRevisionStorageConflictAndTombstones(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	base.TestExternalRevStorage = true

	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	_, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a with a large body")
	rev2a_body := Body{}
	rev2a_body["key1"] = prop_1000_bytes
	rev2a_body["version"] = "2a"
	_, err = db.PutExistingRevWithBody("doc1", rev2a_body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev2a_body)

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	log.Printf("Create rev 2-b with a large body")
	rev2b_body := Body{}
	rev2b_body["key1"] = prop_1000_bytes
	rev2b_body["version"] = "2b"
	_, err = db.PutExistingRevWithBody("doc1", rev2b_body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-b")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev2b_body)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err := getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	goassert.Equals(t, len(revTree.BodyMap), 0)
	goassert.Equals(t, len(revTree.BodyKeyMap), 1)

	// Retrieve the raw revision body backup of 2-a, and verify it's intact
	log.Printf("Verify document storage of 2-a")
	var revisionBody Body
	rawRevision, _, err := db.Bucket.GetRaw("_sync:rb:4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	assert.NoError(t, err, "Couldn't get raw backup revision")
	assert.NoError(t, base.JSONUnmarshal(rawRevision, &revisionBody))
	goassert.Equals(t, revisionBody["version"], rev2a_body["version"])
	goassert.Equals(t, revisionBody["value"], rev2a_body["value"])

	// Retrieve the non-inline revision
	db.FlushRevisionCacheForTest()
	rev2aGet, err := db.Get1xRevBody("doc1", "2-a", false, nil)
	assert.NoError(t, err, "Couldn't get rev 2-a")
	goassert.DeepEquals(t, rev2aGet, rev2a_body)

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
	_, err = db.PutExistingRevWithBody("doc1", rev3b_body, []string{"3-b", "2-b"}, false)
	assert.NoError(t, err, "add 3-b (tombstone)")

	// Retrieve tombstone
	rev3bGet, err := db.Get1xRevBody("doc1", "3-b", false, nil)
	assert.NoError(t, err, "Couldn't get rev 3-b")
	goassert.DeepEquals(t, rev3bGet, rev3b_body)

	// Retrieve the document, validate that we get 2-a
	log.Printf("Retrieve doc, expect 2-a")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev2a_body)

	// Ensure previous revision body backup has been removed
	_, _, err = db.Bucket.GetRaw("_sync:rb:4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	assert.True(t, base.IsKeyNotFoundError(db.Bucket, err), "Revision should be not found")

	// Validate the tombstone is stored inline (due to small size)
	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	goassert.Equals(t, len(revTree.BodyMap), 1)
	goassert.Equals(t, len(revTree.BodyKeyMap), 0)

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
	_, err = db.PutExistingRevWithBody("doc1", rev2c_body, []string{"2-c", "1-a"}, false)
	assert.NoError(t, err, "add 2-c")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-c")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev2c_body)

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
	_, err = db.PutExistingRevWithBody("doc1", rev3c_body, []string{"3-c", "2-c"}, false)
	assert.NoError(t, err, "add 3-c (large tombstone)")

	// Validate the tombstone is not stored inline (due to small size)
	log.Printf("Verify raw revtree w/ tombstone 3-c in key map")
	newRevTree, err := getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	goassert.Equals(t, len(newRevTree.BodyMap), 1)    // tombstone 3-b
	goassert.Equals(t, len(newRevTree.BodyKeyMap), 1) // tombstone 3-c

	// Retrieve the non-inline tombstone revision
	db.FlushRevisionCacheForTest()
	rev3cGet, err := db.Get1xRevBody("doc1", "3-c", false, nil)
	assert.NoError(t, err, "Couldn't get rev 3-c")
	goassert.DeepEquals(t, rev3cGet, rev3c_body)

	log.Printf("Retrieve doc, verify active rev is 2-a")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev2a_body)

	// Add active revision, ensure all revisions remain intact
	log.Printf("Create rev 3-a with a large body")
	rev3a_body := Body{}
	rev3a_body["key1"] = prop_1000_bytes
	rev3a_body["version"] = "3a"
	_, err = db.PutExistingRevWithBody("doc1", rev2c_body, []string{"3-a", "2-a"}, false)
	assert.NoError(t, err, "add 3-a")

	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	goassert.Equals(t, len(revTree.BodyMap), 1)    // tombstone 3-b
	goassert.Equals(t, len(revTree.BodyKeyMap), 1) // tombstone 3-c
}

// TestRevisionStoragePruneTombstone - tests cleanup of external tombstone bodies when pruned.
func TestRevisionStoragePruneTombstone(t *testing.T) {

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	base.TestExternalRevStorage = true

	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 2-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a"}
	_, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a with a large body")
	rev2a_body := Body{}
	rev2a_body["key1"] = prop_1000_bytes
	rev2a_body["version"] = "2a"
	_, err = db.PutExistingRevWithBody("doc1", rev2a_body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev2a_body)

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	log.Printf("Create rev 2-b with a large body")
	rev2b_body := Body{}
	rev2b_body["key1"] = prop_1000_bytes
	rev2b_body["version"] = "2b"
	_, err = db.PutExistingRevWithBody("doc1", rev2b_body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify rev 2-b")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev2b_body)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err := getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	goassert.Equals(t, len(revTree.BodyMap), 0)
	goassert.Equals(t, len(revTree.BodyKeyMap), 1)

	// Retrieve the raw revision body backup of 2-a, and verify it's intact
	log.Printf("Verify document storage of 2-a")
	var revisionBody Body
	rawRevision, _, err := db.Bucket.GetRaw("_sync:rb:4GctXhLVg13d59D0PUTPRD0i58Hbe1d0djgo1qOEpfI=")
	assert.NoError(t, err, "Couldn't get raw backup revision")
	base.JSONUnmarshal(rawRevision, &revisionBody)
	goassert.Equals(t, revisionBody["version"], rev2a_body["version"])
	goassert.Equals(t, revisionBody["value"], rev2a_body["value"])

	// Retrieve the non-inline revision
	db.FlushRevisionCacheForTest()
	rev2aGet, err := db.Get1xRevBody("doc1", "2-a", false, nil)
	assert.NoError(t, err, "Couldn't get rev 2-a")
	goassert.DeepEquals(t, rev2aGet, rev2a_body)

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
	_, err = db.PutExistingRevWithBody("doc1", rev3b_body, []string{"3-b", "2-b"}, false)
	assert.NoError(t, err, "add 3-b (tombstone)")

	// Retrieve tombstone
	db.FlushRevisionCacheForTest()
	rev3bGet, err := db.Get1xRevBody("doc1", "3-b", false, nil)
	assert.NoError(t, err, "Couldn't get rev 3-b")
	goassert.DeepEquals(t, rev3bGet, rev3b_body)

	// Retrieve the document, validate that we get 2-a
	log.Printf("Retrieve doc, expect 2-a")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev2a_body)

	// Retrieve the raw document, and verify 2-a isn't stored inline
	log.Printf("Retrieve doc, verify rev 2-a not inline")
	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	assert.NoError(t, err, "Couldn't get revtree for raw document")
	goassert.Equals(t, len(revTree.BodyMap), 0)
	goassert.Equals(t, len(revTree.BodyKeyMap), 1)
	log.Printf("revTree.BodyKeyMap:%v", revTree.BodyKeyMap)

	revTree, err = getRevTreeList(db.Bucket, "doc1", db.UseXattrs())
	log.Printf("revtree before additional revisions: %v", revTree.BodyKeyMap)

	// Add revisions until 3-b is pruned
	db.RevsLimit = 5
	activeRevBody := Body{}
	activeRevBody["version"] = "...a"
	activeRevBody["key1"] = prop_1000_bytes
	_, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"3-a", "2-a"}, false)
	assert.NoError(t, err, "add 3-a")
	_, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"4-a", "3-a"}, false)
	assert.NoError(t, err, "add 4-a")
	_, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"5-a", "4-a"}, false)
	assert.NoError(t, err, "add 5-a")
	_, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"6-a", "5-a"}, false)
	assert.NoError(t, err, "add 6-a")
	_, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"7-a", "6-a"}, false)
	assert.NoError(t, err, "add 7-a")
	_, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"8-a", "7-a"}, false)
	assert.NoError(t, err, "add 8-a")

	// Verify that 3-b is still present at this point
	db.FlushRevisionCacheForTest()
	rev3bGet, err = db.Get1xRevBody("doc1", "3-b", false, nil)
	assert.NoError(t, err, "Rev 3-b should still exist")

	// Add one more rev that triggers pruning since gen(9-3) > revsLimit
	_, err = db.PutExistingRevWithBody("doc1", activeRevBody, []string{"9-a", "8-a"}, false)
	assert.NoError(t, err, "add 9-a")

	// Verify that 3-b has been pruned
	log.Printf("Attempt to retrieve 3-b, expect pruned")
	db.FlushRevisionCacheForTest()
	rev3bGet, err = db.Get1xRevBody("doc1", "3-b", false, nil)
	goassert.Equals(t, err.Error(), "404 missing")

	// Ensure previous tombstone body backup has been removed
	log.Printf("Verify revision body doc has been removed from bucket")
	_, _, err = db.Bucket.GetRaw("_sync:rb:ULDLuEgDoKFJeET2hojeFANXM8SrHdVfAGONki+kPxM=")
	assert.True(t, base.IsKeyNotFoundError(db.Bucket, err), "Revision should be not found")

}

// Checks for unwanted interaction between old revision body backups and revision cache
func TestOldRevisionStorage(t *testing.T) {

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	prop_1000_bytes := base.CreateProperty(1000)

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "version": "1a", "large": prop_1000_bytes}
	_, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a")
	rev2a_body := Body{"key1": "value2", "version": "2a", "large": prop_1000_bytes}
	_, err = db.PutExistingRevWithBody("doc1", rev2a_body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev2a_body)

	// Create rev 3-a

	// 1-a
	//  |
	// 2-a
	//  |
	// 3-a
	log.Printf("Create rev 3-a")
	rev3a_body := Body{"key1": "value2", "version": "3a", "large": prop_1000_bytes}
	_, err = db.PutExistingRevWithBody("doc1", rev3a_body, []string{"3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "add 3-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 3-a...")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev3a_body)

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	//  |
	// 3-a
	log.Printf("Create rev 2-b")
	rev2b_body := Body{"key1": "value2", "version": "2b", "large": prop_1000_bytes}
	_, err = db.PutExistingRevWithBody("doc1", rev2b_body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify still rev 3-a")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev3a_body)

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
	_, err = db.PutExistingRevWithBody("doc1", rev6a_body, []string{"6-a", "5-a", "4-a", "3-a"}, false)
	assert.NoError(t, err, "add 6-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 6-a...")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev6a_body)

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
	_, err = db.PutExistingRevWithBody("doc1", rev3b_body, []string{"3-b", "2-b", "1-a"}, false)
	assert.NoError(t, err, "add 3-b")

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
	_, err = db.PutExistingRevWithBody("doc1", rev3c_body, []string{"3-c", "2-b", "1-a"}, false)
	assert.NoError(t, err, "add 3-c")

	log.Printf("Create rev 3-d")
	rev3d_body := Body{"key1": "value2", "version": "3d", "large": prop_1000_bytes}
	_, err = db.PutExistingRevWithBody("doc1", rev3d_body, []string{"3-d", "2-b", "1-a"}, false)
	assert.NoError(t, err, "add 3-d")

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
	_, err = db.PutExistingRevWithBody("doc1", rev7b_body, []string{"7-b", "6-b", "5-b", "4-b", "3-b"}, false)
	assert.NoError(t, err, "add 7-b")

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
	defer tearDownTestDB(t, db)

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {channel(doc.channels);}`)

	// Create rev 1-a
	log.Printf("Create rev 1-a")
	body := Body{"key1": "value1", "v": "1a"}
	_, err := db.PutExistingRevWithBody("doc1", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add 1-a")

	// Create rev 2-a
	// 1-a
	//  |
	// 2-a
	log.Printf("Create rev 2-a")
	rev2a_body := Body{"key1": "value2", "v": "2a"}
	_, err = db.PutExistingRevWithBody("doc1", rev2a_body, []string{"2-a", "1-a"}, false)
	assert.NoError(t, err, "add 2-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 2-a...")
	gotbody, err := db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev2a_body)

	// Create rev 3-a, should re-attempt to write old revision body for 2-a
	// 1-a
	//  |
	// 2-a
	//  |
	// 3-a
	log.Printf("Create rev 3-a")
	rev3a_body := Body{"key1": "value2", "v": "3a"}
	_, err = db.PutExistingRevWithBody("doc1", rev3a_body, []string{"3-a", "2-a", "1-a"}, false)
	assert.NoError(t, err, "add 3-a")

	// Create rev 2-b
	//    1-a
	//   /  \
	// 2-a  2-b
	//  |
	// 3-a
	log.Printf("Create rev 2-b")
	rev2b_body := Body{"key1": "value2", "v": "2b"}
	_, err = db.PutExistingRevWithBody("doc1", rev2b_body, []string{"2-b", "1-a"}, false)
	assert.NoError(t, err, "add 2-b")

	// Retrieve the document:
	log.Printf("Retrieve doc, verify still rev 3-a")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev3a_body)

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
	_, err = db.PutExistingRevWithBody("doc1", rev6a_body, []string{"6-a", "5-a", "4-a", "3-a"}, false)
	assert.NoError(t, err, "add 6-a")

	// Retrieve the document:
	log.Printf("Retrieve doc 6-a...")
	gotbody, err = db.Get1xBody("doc1")
	assert.NoError(t, err, "Couldn't get document")
	goassert.DeepEquals(t, gotbody, rev6a_body)

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
	_, err = db.PutExistingRevWithBody("doc1", rev3b_body, []string{"3-b", "2-b", "1-a"}, false)
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
	_, err = db.PutExistingRevWithBody("doc1", rev3c_body, []string{"3-c", "2-b", "1-a"}, false)
	assert.NoError(t, err, "add 3-c")

}

// Validate JSON number handling for large sequence values
func TestLargeSequence(t *testing.T) {

	db, testBucket := setupTestDBWithCustomSyncSeq(t, 9223372036854775807)
	defer tearDownTestDB(t, db)
	defer testBucket.Close()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Write a doc via SG
	body := Body{"key1": "largeSeqTest"}
	_, err := db.PutExistingRevWithBody("largeSeqDoc", body, []string{"1-a"}, false)
	assert.NoError(t, err, "add largeSeqDoc")

	syncData, err := db.GetDocSyncData("largeSeqDoc")
	assert.NoError(t, err, "Error retrieving document sync data")
	goassert.Equals(t, syncData.Sequence, uint64(9223372036854775808))
}

const rawDocMalformedRevisionStorage = `
	{
     "_sync":
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
	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

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
	ok, addErr := db.Bucket.AddRaw("doc1", 0, []byte(rawDocMalformedRevisionStorage))
	goassert.True(t, ok)
	assert.NoError(t, addErr, "Error writing raw document")

	// Increment _sync:seq to match sequences allocated by raw doc
	_, incrErr := db.Bucket.Incr("_sync:seq", 5, 0, 0)
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
	_, err := db.PutExistingRevWithBody("doc1", rev3c_body, []string{"3-c", "2-b", "1-a"}, false)
	assert.NoError(t, err, "add 3-c")
}
