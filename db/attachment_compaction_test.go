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

	attKeys := make([]string, 0, 11)
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

	attKeys = append(attKeys, createConflictingDocOneLeafHasAttachmentBodyMap(t, "conflictAtt", "attForConflict", []byte(`{"value": "att"}`), testDb))
	attKeys = append(attKeys, createConflictingDocOneLeafHasAttachmentBodyKey(t, "conflictAttBodyKey", "attForConflict2", []byte(`{"val": "bodyKeyAtt"}`), testDb))
	attKeys = append(attKeys, createDocWithInBodyAttachment(t, "inBodyDoc", []byte(`{}`), "attForInBodyRef", []byte(`{"val": "inBodyAtt"}`), testDb))

	terminator := make(chan struct{})
	attachmentsMarked, err := Mark(testDb, t.Name(), terminator, func(markedAttachments *int) {})
	assert.NoError(t, err)
	assert.Equal(t, 13, attachmentsMarked)

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
				"length":       len(attBody),
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

func createConflictingDocOneLeafHasAttachmentBodyMap(t *testing.T, docID string, attID string, attBody []byte, db *Database) string {
	attDigest := Sha1DigestKey(attBody)
	attLength := len(attBody)

	syncData := `{
      "rev": "2-b",
      "flags": 24,
      "sequence": 3,
      "recent_sequences": [
        1,
        2,
        3
      ],
      "history": {
        "revs": [
          "2-5d3308aae9930225ed7f6614cf115366",
          "2-b",
		  "1-ca9ad22802b66f662ff171f226211d5c"
        ],
        "parents": [
          2,
          2,
          -1
        ],
        "bodymap": {
          "0": "{\"_attachments\":{\"` + attID + `\":{\"digest\":\"` + attDigest + `\",\"length\":` + strconv.Itoa(attLength) + `,\"revpos\":1,\"stub\":true}}}"
        },
        "channels": [
          null,
          null,
          null
        ]
      },
      "cas": "0x0000502dcaefad16",
      "value_crc32c": "0x5a0a8886",
      "time_saved": "2021-10-14T16:38:11.359443+01:00"
    }`

	_, err := db.Bucket.WriteWithXattr(docID, base.SyncXattrName, 0, 0, []byte(`{"Winning Rev": true}`), []byte(syncData), false, false)
	assert.NoError(t, err)

	attDocID := MakeAttachmentKey(AttVersion1, docID, attDigest)
	_, err = db.Bucket.AddRaw(attDocID, 0, attBody)
	require.NoError(t, err)

	return attDocID
}

func createConflictingDocOneLeafHasAttachmentBodyKey(t *testing.T, docID string, attID string, attBody []byte, db *Database) string {
	attDigest := Sha1DigestKey(attBody)

	syncData := `{
      "rev": "2-b",
      "flags": 24,
      "sequence": 3,
      "recent_sequences": [
        1,
        2,
        3
      ],
      "history": {
        "revs": [
          "1-ca9ad22802b66f662ff171f226211d5c",
          "2-01b555fc7738f62c68dd16da92009740",
          "2-b"
        ],
        "parents": [
          -1,
          0,
          0
        ],
        "bodyKeyMap": {
          "1": "_sync:rb:PVLZc9dMcSQWX9uiA9tvlMDcs/PXoFIowRvfjcSurvU="
        },
        "channels": [
          null,
          null,
          null
        ]
      },
      "cas": "0x000013a35309b016",
      "value_crc32c": "0x5a0a8886",
      "channel_set": null,
      "channel_set_history": null,
      "time_saved": "2021-10-21T12:48:39.549095+01:00"
    }`

	_, err := db.Bucket.WriteWithXattr(docID, base.SyncXattrName, 0, 0, []byte(`{"Winning Rev": true}`), []byte(syncData), false, false)
	assert.NoError(t, err)

	attDocID := MakeAttachmentKey(AttVersion1, docID, attDigest)
	_, err = db.Bucket.AddRaw(attDocID, 0, attBody)
	require.NoError(t, err)

	backupKey := "_sync:rb:PVLZc9dMcSQWX9uiA9tvlMDcs/PXoFIowRvfjcSurvU="
	bodyBackup := `
	{
	  "_attachments": {
		"` + attID + `": {
		  "revpos": 1,
		  "stub": true,
		  "digest": "` + attDigest + `"
		}
	  },
	  "testval": "val"
	}`

	err = db.Bucket.SetRaw(backupKey, 0, []byte(bodyBackup))
	assert.NoError(t, err)

	return attDocID
}

func createDocWithInBodyAttachment(t *testing.T, docID string, docBody []byte, attID string, attBody []byte, db *Database) string {
	var body Body
	err := base.JSONUnmarshal(docBody, &body)
	assert.NoError(t, err)

	_, _, err = db.Put(docID, body)
	assert.NoError(t, err)

	attDigest := Sha1DigestKey(attBody)
	attDocID := MakeAttachmentKey(AttVersion1, docID, attDigest)
	_, err = db.Bucket.AddRaw(attDocID, 0, attBody)
	require.NoError(t, err)

	_, err = db.Bucket.Update(docID, 0, func(current []byte) (updated []byte, expiry *uint32, delete bool, err error) {
		attachmentSyncData := map[string]interface{}{
			attID: map[string]interface{}{
				"content_type": "application/json",
				"digest":       attDigest,
				"length":       len(attBody),
				"revpos":       2,
				"stub":         true,
			},
		}

		attachmentSyncDataBytes, err := base.JSONMarshal(attachmentSyncData)
		if err != nil {
			return nil, base.Uint32Ptr(0), false, err
		}

		updated, err = base.InjectJSONPropertiesFromBytes(current, base.KVPairBytes{
			Key: "_attachments",
			Val: attachmentSyncDataBytes,
		})
		if err != nil {
			return nil, base.Uint32Ptr(0), false, err
		}

		return updated, base.Uint32Ptr(0), false, nil
	})
	require.NoError(t, err)

	return attDocID
}
