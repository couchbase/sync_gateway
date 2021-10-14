package db

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttachmentMark(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Requires CBS")
	}

	testDb := setupTestDB(t)
	defer testDb.Close()

	body := map[string]interface{}{"foo": "bar"}
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%s_%d", t.Name(), i)
		_, _, err := testDb.Put(key, body)
		assert.NoError(t, err)
	}

	attKeys := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		docID := fmt.Sprintf("testDoc-%d", i)
		attKey := fmt.Sprintf("att-%d", i)
		attBody := map[string]interface{}{"value": strconv.Itoa(i)}
		attJSONBody, err := base.JSONMarshal(attBody)
		assert.NoError(t, err)
		attKeys = append(attKeys, CreateLegacyAttachmentDoc(t, testDb, docID, []byte("{}"), attKey, attJSONBody))
	}

	err := testDb.Bucket.SetRaw("testDocx", 0, []byte("{}"))
	assert.NoError(t, err)

	terminator := make(chan struct{})
	attachmentsMarked, err := Mark(testDb, t.Name(), terminator, func(markedAttachments *int) {})
	assert.NoError(t, err)
	assert.Equal(t, 10, attachmentsMarked)

	for _, attDocKey := range attKeys {
		var attachmentData Body
		_, err = testDb.Bucket.GetXattr(attDocKey, base.AttachmentCompactionXattrName, &attachmentData)
		assert.NoError(t, err)

		compactID, ok := attachmentData[CompactionIDKey]
		assert.True(t, ok)
		assert.Equal(t, t.Name(), compactID)
	}
}

func TestAttachmentSweep(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Requires CBS")
	}

	testDb := setupTestDB(t)
	defer testDb.Close()

	makeMarkedDoc := func(docid string, compactID string) {
		err := testDb.Bucket.SetRaw(docid, 0, []byte("{}"))
		assert.NoError(t, err)
		_, err = testDb.Bucket.SetXattr(docid, base.AttachmentCompactionXattrName, []byte(`{"`+CompactionIDKey+`": "`+compactID+`"}`))
		assert.NoError(t, err)
	}

	makeUnmarkedDoc := func(docid string) {
		err := testDb.Bucket.SetRaw(docid, 0, []byte("{}"))
		assert.NoError(t, err)
	}

	// Make docs that are marked - ie. They have a doc referencing them so don't purge
	for i := 0; i < 4; i++ {
		docID := fmt.Sprintf("%s%s%d", base.AttPrefix, "marked", i)
		makeMarkedDoc(docID, t.Name())
	}

	// Make docs that are marked but with an old compact ID - ie. They have lost the doc that was referencing them so purge
	for i := 0; i < 5; i++ {
		docID := fmt.Sprintf("%s%s%d", base.AttPrefix, "oldMarked", i)
		makeMarkedDoc(docID, "old")
	}

	// Make docs that are unmarked - ie. Never been marked so purge
	for i := 0; i < 6; i++ {
		docID := fmt.Sprintf("%s%s%d", base.AttPrefix, "unmarked", i)
		makeUnmarkedDoc(docID)
	}

	terminator := make(chan struct{})
	purged, err := Sweep(testDb, t.Name(), terminator, func(purgedAttachments *int) {})
	assert.NoError(t, err)

	assert.Equal(t, 11, purged)
}

func TestAttachmentMarkAndSweep(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Requires CBS")
	}

	testDb := setupTestDB(t)
	defer testDb.Close()

	attKeys := make([]string, 0, 15)
	for i := 0; i < 10; i++ {
		docID := fmt.Sprintf("testDoc-%d", i)
		attKey := fmt.Sprintf("att-%d", i)
		attBody := map[string]interface{}{"value": strconv.Itoa(i)}
		attJSONBody, err := base.JSONMarshal(attBody)
		assert.NoError(t, err)
		attKeys = append(attKeys, CreateLegacyAttachmentDoc(t, testDb, docID, []byte("{}"), attKey, attJSONBody))
	}

	makeUnmarkedDoc := func(docid string) {
		err := testDb.Bucket.SetRaw(docid, 0, []byte("{}"))
		assert.NoError(t, err)
		attKeys = append(attKeys, docid)
	}

	for i := 0; i < 5; i++ {
		docID := fmt.Sprintf("%s%s%d", base.AttPrefix, "unmarked", i)
		makeUnmarkedDoc(docID)
	}

	terminator := make(chan struct{})
	attachmentsMarked, err := Mark(testDb, t.Name(), terminator, func(markedAttachments *int) {})
	assert.NoError(t, err)
	assert.Equal(t, 10, attachmentsMarked)

	terminator = make(chan struct{})
	attachmentsPurged, err := Sweep(testDb, t.Name(), terminator, func(markedAttachments *int) {})
	assert.NoError(t, err)
	assert.Equal(t, 5, attachmentsPurged)

	for _, attDocKey := range attKeys {
		var back interface{}
		_, err = testDb.Bucket.Get(attDocKey, &back)
		if strings.Contains(attDocKey, "unmarked") {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}
	}

}

func CreateLegacyAttachmentDoc(t *testing.T, db *Database, docID string, body []byte, attID string, attBody []byte) string {
	attDigest := Sha1DigestKey(attBody)

	attDocID := MakeAttachmentKey(AttVersion1, docID, attDigest)
	_, err := db.Bucket.AddRaw(attDocID, 0, attBody)
	require.NoError(t, err)

	var unmarshalledBody Body
	err = base.JSONUnmarshal(body, &unmarshalledBody)
	require.NoError(t, err)

	_, _, err = db.Put(docID, unmarshalledBody)
	require.NoError(t, err)

	_, err = db.Bucket.WriteUpdateWithXattr(docID, base.SyncXattrName, "", 0, nil, func(doc []byte, xattr []byte, userXattr []byte, cas uint64) (updatedDoc []byte, updatedXattr []byte, deletedDoc bool, expiry *uint32, err error) {
		attachmentSyncData := map[string]interface{}{
			attID: map[string]interface{}{
				"content_type": "application/json",
				"digest":       attDigest,
				"length":       2,
				"revpos":       2,
				"stub":         true,
			},
		}

		attachmentSyncDataBytes, err := base.JSONMarshal(attachmentSyncData)
		require.NoError(t, err)

		xattr, err = base.InjectJSONPropertiesFromBytes(xattr, base.KVPairBytes{
			Key: "attachments",
			Val: attachmentSyncDataBytes,
		})
		require.NoError(t, err)

		return doc, xattr, false, nil, nil
	})
	require.NoError(t, err)

	return attDocID
}
