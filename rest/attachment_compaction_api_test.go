package rest

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttachmentCompactionAPI(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Perform GET before compact has been ran, ensure it starts in valid 'stopped' state
	resp := rt.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)

	var response db.AttachmentManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateStopped, response.State)
	assert.Equal(t, int64(0), response.MarkedAttachments)
	assert.Equal(t, int64(0), response.PurgedAttachments)
	assert.Empty(t, response.LastErrorMessage)

	// Kick off compact
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)

	// Attempt to kick off again and validate it correctly errors
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusServiceUnavailable)

	// Wait for run to complete
	err = rt.WaitForCondition(func() bool {
		time.Sleep(1 * time.Second)

		resp := rt.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
		assertStatus(t, resp, http.StatusOK)

		var response db.AttachmentManagerResponse
		err = base.JSONUnmarshal(resp.BodyBytes(), &response)
		assert.NoError(t, err)

		return response.State == db.BackgroundProcessStateStopped
	})
	assert.NoError(t, err)

	// Create some legacy attachments to be marked but not compacted
	for i := 0; i < 20; i++ {
		docID := fmt.Sprintf("testDoc-%d", i)
		attID := fmt.Sprintf("testAtt-%d", i)
		attBody := map[string]interface{}{"value": strconv.Itoa(i)}
		attJSONBody, err := base.JSONMarshal(attBody)
		assert.NoError(t, err)
		CreateLegacyAttachmentDoc(t, &db.Database{DatabaseContext: rt.GetDatabase()}, docID, []byte("{}"), attID, attJSONBody)
	}

	// Create some 'unmarked' attachments
	makeUnmarkedDoc := func(docid string) {
		err := rt.GetDatabase().Bucket.SetRaw(docid, 0, []byte("{}"))
		assert.NoError(t, err)
	}

	for i := 0; i < 5; i++ {
		docID := fmt.Sprintf("%s%s%d", base.AttPrefix, "unmarked", i)
		makeUnmarkedDoc(docID)
	}

	// Start attachment compaction run
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)

	// Wait for run to complete
	err = rt.WaitForCondition(func() bool {
		time.Sleep(1 * time.Second)

		resp := rt.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
		assertStatus(t, resp, http.StatusOK)

		var response db.AttachmentManagerResponse
		err = base.JSONUnmarshal(resp.BodyBytes(), &response)
		assert.NoError(t, err)

		return response.State == db.BackgroundProcessStateStopped
	})
	assert.NoError(t, err)

	// Validate results of GET
	resp = rt.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)

	err = base.JSONUnmarshal(resp.BodyBytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateStopped, response.State)
	assert.Equal(t, int64(20), response.MarkedAttachments)
	assert.Equal(t, int64(5), response.PurgedAttachments)
	assert.Empty(t, response.LastErrorMessage)

	// Start another run
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)

	// Attempt to terminate that run
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment&action=stop", "")
	assertStatus(t, resp, http.StatusOK)

	// Verify it has been marked as 'stopping'
	resp = rt.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)

	err = base.JSONUnmarshal(resp.BodyBytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateStopping, response.State)

	// Wait for run to complete
	err = rt.WaitForCondition(func() bool {
		time.Sleep(1 * time.Second)

		resp := rt.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
		assertStatus(t, resp, http.StatusOK)

		var response db.AttachmentManagerResponse
		err = base.JSONUnmarshal(resp.BodyBytes(), &response)
		assert.NoError(t, err)

		return response.State == db.BackgroundProcessStateStopped
	})
	assert.NoError(t, err)

}

func CreateLegacyAttachmentDoc(t *testing.T, testDB *db.Database, docID string, body []byte, attID string, attBody []byte) string {
	attDigest := db.Sha1DigestKey(attBody)

	attDocID := db.MakeAttachmentKey(db.AttVersion1, docID, attDigest)
	_, err := testDB.Bucket.AddRaw(attDocID, 0, attBody)
	require.NoError(t, err)

	var unmarshalledBody db.Body
	err = base.JSONUnmarshal(body, &unmarshalledBody)
	require.NoError(t, err)

	_, _, err = testDB.Put(docID, unmarshalledBody)
	require.NoError(t, err)

	_, err = testDB.Bucket.WriteUpdateWithXattr(docID, base.SyncXattrName, "", 0, nil, func(doc []byte, xattr []byte, userXattr []byte, cas uint64) (updatedDoc []byte, updatedXattr []byte, deletedDoc bool, expiry *uint32, err error) {
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
