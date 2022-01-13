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
	assert.Equal(t, db.BackgroundProcessStateCompleted, response.State)
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

		return response.State == db.BackgroundProcessStateCompleted
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
		err := rt.GetDatabase().Bucket.SetRaw(docid, 0, nil, []byte("{}"))
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

		return response.State == db.BackgroundProcessStateCompleted
	})
	assert.NoError(t, err)

	// Validate results of GET
	resp = rt.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)

	err = base.JSONUnmarshal(resp.BodyBytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateCompleted, response.State)
	assert.Equal(t, int64(20), response.MarkedAttachments)
	assert.Equal(t, int64(5), response.PurgedAttachments)
	assert.Empty(t, response.LastErrorMessage)

	// Start another run
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)

	// Attempt to terminate that run
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment&action=stop", "")
	assertStatus(t, resp, http.StatusOK)

	// Verify it has been marked as 'stopping' --> its possible we'll get stopped instead based on timing of persisted doc update
	err = rt.WaitForCondition(func() bool {
		time.Sleep(1 * time.Second)

		resp := rt.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
		assertStatus(t, resp, http.StatusOK)

		var response db.AttachmentManagerResponse
		err = base.JSONUnmarshal(resp.BodyBytes(), &response)
		assert.NoError(t, err)

		return response.State == db.BackgroundProcessStateStopping || response.State == db.BackgroundProcessStateStopped
	})
	assert.NoError(t, err)

	// Wait for run to complete
	_ = rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateStopped)
}

func TestAttachmentCompactionPersistence(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	tb := base.GetTestBucket(t)
	noCloseTB := tb.NoCloseClone()

	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: noCloseTB,
	})
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb,
	})

	defer rt2.Close()
	defer rt1.Close()

	// Start attachment compaction on one SGW
	resp := rt1.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)

	_ = rt1.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateCompleted)

	// Ensure compaction is marked complete on the other node too
	var rt2AttachmentStatus db.AttachmentManagerResponse
	resp = rt2.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)
	err := base.JSONUnmarshal(resp.BodyBytes(), &rt2AttachmentStatus)
	assert.NoError(t, err)
	assert.Equal(t, rt2AttachmentStatus.State, db.BackgroundProcessStateCompleted)

	// Start compaction again
	resp = rt1.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)
	status := rt1.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateRunning)
	compactID := status.CompactID

	// Abort process early from rt1
	resp = rt1.SendAdminRequest("POST", "/db/_compact?type=attachment&action=stop", "")
	assertStatus(t, resp, http.StatusOK)
	status = rt2.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateStopped)

	// Ensure aborted status is present on rt2
	resp = rt2.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)
	err = base.JSONUnmarshal(resp.BodyBytes(), &rt2AttachmentStatus)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateStopped, rt2AttachmentStatus.State)

	// Attempt to start again from rt2 --> Should resume based on aborted state (same compactionID)
	resp = rt2.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)
	status = rt2.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateRunning)
	assert.Equal(t, compactID, status.CompactID)

	// Wait for compaction to complete
	_ = rt1.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateCompleted)
}

func TestAttachmentCompactionDryRun(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create some 'unmarked' attachments
	makeUnmarkedDoc := func(docid string) {
		err := rt.GetDatabase().Bucket.SetRaw(docid, 0, nil, []byte("{}"))
		assert.NoError(t, err)
	}

	attachmentKeys := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		docID := fmt.Sprintf("%s%s%d", base.AttPrefix, "unmarked", i)
		makeUnmarkedDoc(docID)
		attachmentKeys = append(attachmentKeys, docID)
	}

	resp := rt.SendAdminRequest("POST", "/db/_compact?type=attachment&dry_run=true", "")
	assertStatus(t, resp, http.StatusOK)
	status := rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateCompleted)
	assert.True(t, status.DryRun)
	assert.Equal(t, int64(5), status.PurgedAttachments)

	for _, docID := range attachmentKeys {
		_, _, err := rt.GetDatabase().Bucket.GetRaw(docID)
		assert.NoError(t, err)
	}

	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)
	status = rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateCompleted)
	assert.False(t, status.DryRun)
	assert.Equal(t, int64(5), status.PurgedAttachments)

	for _, docID := range attachmentKeys {
		_, _, err := rt.GetDatabase().Bucket.GetRaw(docID)
		assert.Error(t, err)
		assert.True(t, base.IsDocNotFoundError(err))
	}
}

func TestAttachmentCompactionReset(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Start compaction
	resp := rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)
	status := rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateRunning)
	compactID := status.CompactID

	// Stop compaction before complete -- enters aborted state
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment&action=stop", "")
	assertStatus(t, resp, http.StatusOK)
	status = rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateStopped)

	// Ensure status is aborted
	resp = rt.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)
	var attachmentStatus db.AttachmentManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &attachmentStatus)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateStopped, attachmentStatus.State)

	// Start compaction again but with reset=true --> meaning it shouldn't try to resume
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment&reset=true", "")
	assertStatus(t, resp, http.StatusOK)
	status = rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateRunning)
	assert.NotEqual(t, compactID, status.CompactID)

	// Wait for completion before closing test
	_ = rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateCompleted)
}

func TestAttachmentCompactionInvalidDocs(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create a raw binary doc
	_, err := rt.Bucket().AddRaw("binary", 0, []byte("binary doc"))
	assert.NoError(t, err)

	// Create a CBS tombstone
	_, err = rt.Bucket().AddRaw("deleted", 0, []byte("{}"))
	assert.NoError(t, err)
	err = rt.Bucket().Delete("deleted")
	assert.NoError(t, err)

	// Also create an actual legacy attachment to ensure they are still processed
	CreateLegacyAttachmentDoc(t, &db.Database{DatabaseContext: rt.GetDatabase()}, "docID", []byte("{}"), "attKey", []byte("{}"))

	// Create attachment with no doc reference
	err = rt.GetDatabase().Bucket.SetRaw(base.AttPrefix+"test", 0, nil, []byte("{}"))
	assert.NoError(t, err)
	err = rt.GetDatabase().Bucket.SetRaw(base.AttPrefix+"test2", 0, nil, []byte("{}"))
	assert.NoError(t, err)

	// Write a normal doc to ensure this passes through fine
	resp := rt.SendAdminRequest("PUT", "/db/normal-doc", "{}")
	assertStatus(t, resp, http.StatusCreated)

	// Start compaction
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)
	status := rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateCompleted)

	assert.Equal(t, int64(2), status.PurgedAttachments)
	assert.Equal(t, int64(1), status.MarkedAttachments)
	assert.Equal(t, db.BackgroundProcessStateCompleted, status.State)
}

func TestAttachmentCompactionStartTimeAndStats(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create attachment with no doc reference
	err := rt.GetDatabase().Bucket.SetRaw(base.AttPrefix+"test", 0, nil, []byte("{}"))
	assert.NoError(t, err)

	databaseStats := rt.GetDatabase().DbStats.Database()

	// Start compaction
	resp := rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)
	status := rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateCompleted)

	// Check stats and start time response is correct
	firstStartTime := status.StartTime
	firstStartTimeStat := databaseStats.CompactionAttachmentStartTime.Value()
	assert.False(t, firstStartTime.IsZero())
	assert.NotEqual(t, 0, firstStartTimeStat)
	assert.Equal(t, int64(1), databaseStats.NumAttachmentsCompacted.Value())

	// Start compaction again
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)
	status = rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateCompleted)

	// Check that stats have been updated to new run and previous attachment count stat remains
	assert.True(t, status.StartTime.After(firstStartTime))
	assert.True(t, databaseStats.CompactionAttachmentStartTime.Value() > firstStartTimeStat)
	assert.Equal(t, int64(1), databaseStats.NumAttachmentsCompacted.Value())
}

func TestAttachmentCompactionAbort(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	for i := 0; i < 1000; i++ {
		docID := fmt.Sprintf("testDoc-%d", i)
		attID := fmt.Sprintf("testAtt-%d", i)
		attBody := map[string]interface{}{"value": strconv.Itoa(i)}
		attJSONBody, err := base.JSONMarshal(attBody)
		assert.NoError(t, err)
		CreateLegacyAttachmentDoc(t, &db.Database{DatabaseContext: rt.GetDatabase()}, docID, []byte("{}"), attID, attJSONBody)
	}

	resp := rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	assertStatus(t, resp, http.StatusOK)

	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment&action=stop", "")
	assertStatus(t, resp, http.StatusOK)

	status := rt.WaitForAttachmentCompactionStatus(t, db.BackgroundProcessStateStopped)
	assert.Equal(t, int64(0), status.PurgedAttachments)
}

func (rt *RestTester) WaitForAttachmentCompactionStatus(t *testing.T, state db.BackgroundProcessState) db.AttachmentManagerResponse {
	var response db.AttachmentManagerResponse
	err := rt.WaitForConditionWithOptions(func() bool {
		resp := rt.SendAdminRequest("GET", "/db/_compact?type=attachment", "")
		assertStatus(t, resp, http.StatusOK)

		err := base.JSONUnmarshal(resp.BodyBytes(), &response)
		assert.NoError(t, err)

		return response.State == state
	}, 90, 1000)
	assert.NoError(t, err)

	return response
}

func CreateLegacyAttachmentDoc(t *testing.T, testDB *db.Database, docID string, body []byte, attID string, attBody []byte) string {
	if !base.TestUseXattrs() {
		t.Skip("Requires xattrs")
	}

	attDigest := db.Sha1DigestKey(attBody)

	attDocID := db.MakeAttachmentKey(db.AttVersion1, docID, attDigest)
	_, err := testDB.Bucket.AddRaw(attDocID, 0, attBody)
	require.NoError(t, err)

	var unmarshalledBody db.Body
	err = base.JSONUnmarshal(body, &unmarshalledBody)
	require.NoError(t, err)

	_, _, err = testDB.Put(docID, unmarshalledBody)
	require.NoError(t, err)

	_, err = testDB.Bucket.WriteUpdateWithXattr(docID, base.SyncXattrName, "", 0, nil, nil, func(doc []byte, xattr []byte, userXattr []byte, cas uint64) (updatedDoc []byte, updatedXattr []byte, deletedDoc bool, expiry *uint32, err error) {
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
