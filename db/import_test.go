package db

import (
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

// There are additional tests that exercise the import functionality in rest/import_test.go

// 1. Write a doc to the bucket
// 2. Build params to migrateMeta (existing doc, raw doc, cas.. sgbucket docs)
// 3. Update doc in the bucket with new expiry
// 4. Call migrateMeta with stale args that have old expiry
// 5. Assert that migrateMeta does the right thing and respects the new expiry value
//
// See SG PR #3109 for more details on motivation for this test
func TestMigrateMetadata(t *testing.T) {

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	base.EnableLogKey("Migrate+")
	base.EnableLogKey("Import+")

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	key := "TestMigrateMetadata"
	bodyBytes := rawDocWithSyncMeta()
	body := Body{}
	err := body.Unmarshal(bodyBytes)
	assertNoError(t, err, "Error unmarshalling body")

	// Create via the SDK with sync metadata intact
	expirySeconds := time.Second * 30
	syncMetaExpiry := time.Now().Add(expirySeconds)
	_, err = testBucket.Bucket.Add(key, uint32(syncMetaExpiry.Unix()), bodyBytes)
	assertNoError(t, err, "Error writing doc w/ expiry")

	// Get the existing bucket doc
	_, existingBucketDoc, err := db.GetDocWithXattr(key, DocUnmarshalAll)

	// Set the expiry value to a stale value (it's about to be stale, since below it will get updated to a later value)
	existingBucketDoc.Expiry = uint32(syncMetaExpiry.Unix())

	// Update doc in the bucket with new expiry
	laterExpirySeconds := time.Second * 60
	laterSyncMetaExpiry := time.Now().Add(laterExpirySeconds)
	updateCallbackFn := func(current []byte) (updated []byte, expiry *uint32, err error) {
		// This update function will not be "competing" with other updates, so it doesn't need
		// to handle being called back multiple times or performing any merging with existing values.
		exp := uint32(laterSyncMetaExpiry.Unix())
		return bodyBytes, &exp, nil
	}
	testBucket.Bucket.Update(
		key,
		uint32(laterSyncMetaExpiry.Unix()), // it's a bit confusing why the expiry needs to be passed here AND via the callback fn
		updateCallbackFn,
	)

	// Call migrateMeta with stale args that have old stale expiry
	docOut, _, err := db.migrateMetadata(
		key,
		body,
		existingBucketDoc,
	)
	assertNoError(t, err, "Error calling migrateMetadata()")

	//// Get the doc expiry
	gocbBucket := db.Bucket.(*base.CouchbaseBucketGoCB)
	expiry, getExpiryErr := gocbBucket.GetExpiry(key)
	assertNoError(t, getExpiryErr, "Error getting expiry")

	// This should equal the docOut expiry
	assert.Equals(t, expiry, uint32(docOut.Expiry.Unix()))

	// Assert that the expiry is the _later_ expiry, otherwise it means that later expiry was discarded and clobbered with
	// the original expiry that was passed via
	if expiry != uint32(laterSyncMetaExpiry.Unix()) {
		t.Errorf("Expected expiry to be %v but was %v", laterSyncMetaExpiry.Unix(), expiry)
	}

	// Try to call migrateMetadata again, even though the doc has already been migrated.
	// This should essentially abort the migration and return the latest doc
	docOutAlreadyMigrated, _, err := db.migrateMetadata(
		key,
		body,
		existingBucketDoc,
	)
	assertNoError(t, err, "Error calling migrateMetadata()")

	// Assert that the returned doc has the latest doc expiry
	assert.True(t, docOutAlreadyMigrated.Expiry != nil)
	if uint32(docOutAlreadyMigrated.Expiry.Unix()) != uint32(laterSyncMetaExpiry.Unix()) {
		t.Errorf("Expected expiry to be %v but was %v", laterSyncMetaExpiry.Unix(), uint32(docOutAlreadyMigrated.Expiry.Unix()))
	}

}

func rawDocWithSyncMeta() []byte {

	return []byte(`
{
    "_sync": {
        "rev": "1-ca9ad22802b66f662ff171f226211d5c",
        "sequence": 1,
        "recent_sequences": [
            1
        ],
        "history": {
            "revs": [
                "1-ca9ad22802b66f662ff171f226211d5c"
            ],
            "parents": [
                -1
            ],
            "channels": [
                null
            ]
        },
        "cas": "",
        "time_saved": "2017-11-29T12:46:13.456631-08:00"
    }
}
`)

}
