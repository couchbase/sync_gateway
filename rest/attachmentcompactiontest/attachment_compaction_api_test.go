// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package attachmentcompactiontest

import (
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/sgtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAttachmentCompactionAPI(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	// attachment compaction has to run on default collection, we can't run on multiple scopes right now for SG_TEST_USE_DEFAULT_COLLECTION = false
	rt := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{
		LeakyBucketConfig: &base.LeakyBucketConfig{},
	})
	defer rt.Close()

	// Avoid racing the automatic startup migration against the mark phase below.
	_ = rt.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateCompleted)

	// cleanup attachments left behind
	defer func() {
		resp := rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment&reset=true", "")
		rest.RequireStatus(t, resp, http.StatusOK)
		_ = rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateCompleted)
	}()

	// Perform GET before compact has been run — verify initial state.
	resp := rt.SendAdminRequest("GET", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	var response db.AttachmentManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &response)
	require.NoError(t, err)
	require.Equal(t, db.BackgroundProcessStateCompleted, response.State)
	require.Equal(t, int64(0), response.MarkedAttachments)
	require.Equal(t, int64(0), response.PurgedAttachments)
	require.Empty(t, response.LastErrorMessage)

	dataStore := rt.GetSingleDataStore()
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	// Create some legacy attachments to be marked but not compacted. Both doc keys and attachment
	// bodies land on vBucket 0 so the mark phase stays serial on a single DCP worker — otherwise
	// concurrent SetXattrs calls race to close the pauser channel.
	docIDs := base.VBucket0DocIDs(t, rt.Bucket(), 3)
	attBodies := base.VBucket0AttachmentBodies(t, rt.Bucket(), 3)
	for i, attBody := range attBodies {
		attID := fmt.Sprintf("testAtt-%d", i)
		rest.CreateLegacyAttachmentDoc(t, ctx, collection, docIDs[i], []byte("{}"), attID, attBody)
	}

	// Create some 'unmarked' attachments
	makeUnmarkedDoc := func(docid string) {
		err := dataStore.SetRaw(ctx, docid, 0, nil, []byte("{}"))
		require.NoError(t, err)
	}

	for i := range 2 {
		docID := fmt.Sprintf("%s%s%d", base.AttPrefix, "unmarked", i)
		makeUnmarkedDoc(docID)
	}

	// Pause at the first attachment mark so the concurrent-start 503 check arrives while running.
	pauser := newCompactionPauser(rt)
	defer pauser.Close()
	pauser.Pause()

	// Start attachment compaction run
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.WaitUntilBlocked()

	// Attempt to kick off again and validate it correctly errors
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusServiceUnavailable)

	pauser.Release()
	rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateCompleted)

	// Validate results of GET
	resp = rt.SendAdminRequest("GET", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	err = base.JSONUnmarshal(resp.BodyBytes(), &response)
	require.NoError(t, err)
	require.Equal(t, db.BackgroundProcessStateCompleted, response.State)
	require.Equal(t, int64(3), response.MarkedAttachments)
	require.Equal(t, int64(2), response.PurgedAttachments)
	require.Empty(t, response.LastErrorMessage)

	// Start another run and stop it mid-flight.
	pauser.Pause()
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.WaitUntilBlocked()

	resp = rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment&action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.Release()

	_ = rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateStopped)
}

func TestAttachmentCompactionPersistence(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	tb := base.GetTestBucket(t)
	noCloseTB := tb.NoCloseClone()

	// Attachment Compaction only runs on _default._default
	rt1 := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{
		CustomTestBucket: noCloseTB,
	})
	rt2 := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.LeakyBucketClone(base.LeakyBucketConfig{}),
	})
	defer rt2.Close()
	defer rt1.Close()

	// Start attachment compaction on one SGW
	resp := rt1.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	_ = rt1.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateCompleted)

	// Ensure compaction is marked complete on the other node too
	var rt2AttachmentStatus db.AttachmentManagerResponse
	resp = rt2.SendAdminRequest("GET", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err := base.JSONUnmarshal(resp.BodyBytes(), &rt2AttachmentStatus)
	assert.NoError(t, err)
	assert.Equal(t, rt2AttachmentStatus.State, db.BackgroundProcessStateCompleted)

	// Add a legacy attachment so the mark phase has something to block on.
	collection, ctx := rt1.GetSingleTestDatabaseCollectionWithUser()
	rest.CreateLegacyAttachmentDoc(t, ctx, collection, t.Name(), []byte("{}"), "att", []byte("att body"))

	// Pause at the mark phase so stop arrives while genuinely in-flight.
	pauser := newCompactionPauser(rt1)
	defer pauser.Close()
	pauser.Pause()

	// Start compaction again
	resp = rt1.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.WaitUntilBlocked()
	compactID := rt1.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateRunning).CompactID

	// Abort process early from rt1
	resp = rt1.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment&action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.Release()
	rt2.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateStopped)

	// Ensure aborted status is present on rt2
	resp = rt2.SendAdminRequest("GET", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	err = base.JSONUnmarshal(resp.BodyBytes(), &rt2AttachmentStatus)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateStopped, rt2AttachmentStatus.State)

	// Add a second legacy attachment now that the run is genuinely stopped. On a real cluster,
	// stopping isn't instant, so the first attachment may already be marked by this point (the
	// blocked mark call completes once Release lets it through) -- a fresh doc created after
	// Stopped is confirmed is guaranteed unmarked, giving the resumed run below something to
	// genuinely block on.
	rest.CreateLegacyAttachmentDoc(t, ctx, collection, t.Name()+"_resume", []byte("{}"), "att-resume", []byte("att body 2"))

	// Pause again, bound to rt2's own leaky datastore this time -- the resumed run below performs
	// its writes through rt2, so a pauser bound to rt1 wouldn't intercept them.
	pauser2 := newCompactionPauser(rt2)
	defer pauser2.Close()
	pauser2.Pause()

	// Attempt to start again from rt2 --> Should resume based on aborted state (same compactionID)
	resp = rt2.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser2.WaitUntilBlocked()
	status := rt2.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateRunning)
	assert.Equal(t, compactID, status.CompactID)
	pauser2.Release()

	// Wait for compaction to complete
	status = rt1.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateCompleted)
	assert.Equal(t, compactID, status.CompactID)
}

func TestAttachmentCompactionDryRun(t *testing.T) {
	ctx := base.TestCtx(t)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	// attachment compaction has to run on default collection, we can't run on multiple scopes right now for SG_TEST_USE_DEFAULT_COLLECTION = false
	rt := rest.NewRestTesterDefaultCollection(t, nil)
	defer rt.Close()

	dataStore := rt.GetSingleDataStore()
	// Create some 'unmarked' attachments
	makeUnmarkedDoc := func(docid string) {
		err := dataStore.SetRaw(ctx, docid, 0, nil, []byte("{}"))
		assert.NoError(t, err)
	}

	attachmentKeys := make([]string, 0, 5)
	for i := range 5 {
		docID := fmt.Sprintf("%s%s%d", base.AttPrefix, "unmarked", i)
		makeUnmarkedDoc(docID)
		attachmentKeys = append(attachmentKeys, docID)
	}

	resp := rt.SendAdminRequest("POST", "/db/_compact?type=attachment&dry_run=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status := rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateCompleted)
	assert.True(t, status.DryRun)
	if !base.UnitTestUrlIsWalrus() && !base.TestsDisableGSI() {
		// It is possible for Couchbase Server GSI runs which use DCP purge to two DCP events from a previous
		// test.
		// 1. attachment mutation
		// 2. attachment deletion
		//
		// In a non dry run test, these will not be counted since removing the attachment will not fail. Relax
		// the assertion to greater than the number of documents.
		assert.GreaterOrEqual(t, status.PurgedAttachments, int64(5))

	} else {
		assert.Equal(t, int64(5), status.PurgedAttachments)
	}

	for _, docID := range attachmentKeys {
		_, _, err := dataStore.GetRaw(ctx, docID)
		assert.NoError(t, err)
	}

	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status = rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateCompleted)
	assert.False(t, status.DryRun)
	assert.Equal(t, int64(5), status.PurgedAttachments)

	for _, docID := range attachmentKeys {
		_, _, err := dataStore.GetRaw(ctx, docID)
		assert.Error(t, err)
		assert.True(t, base.IsDocNotFoundError(err))
	}
}

func TestAttachmentCompactionReset(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	// Attachment Compaction only runs on _default._default
	rt := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{
		LeakyBucketConfig: &base.LeakyBucketConfig{},
	})
	defer rt.Close()

	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	rest.CreateLegacyAttachmentDoc(t, ctx, collection, t.Name(), []byte("{}"), "att", []byte("att body"))

	// Pause compaction at the mark of the first attachment so stop arrives while genuinely in-flight.
	pauser := newCompactionPauser(rt)
	defer pauser.Close()
	pauser.Pause()

	// Start compaction
	resp := rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.WaitUntilBlocked()
	compactID := rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateRunning).CompactID

	// Stop compaction before complete -- enters aborted state
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment&action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.Release()
	rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateStopped)

	// Ensure status is aborted
	resp = rt.SendAdminRequest("GET", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var attachmentStatus db.AttachmentManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &attachmentStatus)
	assert.NoError(t, err)
	assert.Equal(t, db.BackgroundProcessStateStopped, attachmentStatus.State)

	// Start compaction again but with reset=true --> meaning it shouldn't try to resume
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment&reset=true", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status := rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateRunning)
	assert.NotEqual(t, compactID, status.CompactID)

	// Wait for completion and verify the completed run also carries a different compactID
	status = rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateCompleted)
	assert.NotEqual(t, compactID, status.CompactID)
}

func TestAttachmentCompactionInvalidDocs(t *testing.T) {
	ctx := base.TestCtx(t)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	// attachment compaction has to run on default collection, we can't run on multiple scopes right now for SG_TEST_USE_DEFAULT_COLLECTION = false
	rt := rest.NewRestTesterDefaultCollection(t, nil)
	defer rt.Close()

	// Avoid racing the automatic startup migration against the mark phase below.
	_ = rt.WaitForAttachmentMigrationStatus(db.BackgroundProcessStateCompleted)

	dataStore := rt.GetSingleDataStore()
	// Create a raw binary doc
	_, err := dataStore.AddRaw(ctx, "binary", 0, []byte("binary doc"))
	assert.NoError(t, err)

	// Create a CBS tombstone
	_, err = dataStore.AddRaw(ctx, "deleted", 0, []byte("{}"))
	assert.NoError(t, err)
	err = dataStore.Delete(ctx, "deleted")
	assert.NoError(t, err)

	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	// Also create an actual legacy attachment to ensure they are still processed
	rest.CreateLegacyAttachmentDoc(t, ctx, collection, "docID", []byte("{}"), "attKey", []byte("{}"))

	// Create attachment with no doc reference
	err = dataStore.SetRaw(ctx, base.AttPrefix+"test", 0, nil, []byte("{}"))
	assert.NoError(t, err)
	err = dataStore.SetRaw(ctx, base.AttPrefix+"test2", 0, nil, []byte("{}"))
	assert.NoError(t, err)

	// Write a normal doc to ensure this passes through fine
	resp := rt.SendAdminRequest("PUT", "/db/normal-doc", "{}")
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Start compaction
	resp = rt.SendAdminRequest("POST", "/db/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status := rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateCompleted)

	assert.Equal(t, int64(2), status.PurgedAttachments)
	assert.Equal(t, int64(1), status.MarkedAttachments)
	assert.Equal(t, db.BackgroundProcessStateCompleted, status.State)
}

func TestAttachmentCompactionStartTimeAndStats(t *testing.T) {
	ctx := base.TestCtx(t)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	rt := rest.NewRestTesterDefaultCollection(t, nil)
	defer rt.Close()

	// Create attachment with no doc reference
	err := rt.GetDatabase().Bucket.DefaultDataStore(ctx).SetRaw(ctx, base.AttPrefix+"test", 0, nil, []byte("{}"))
	assert.NoError(t, err)

	databaseStats := rt.GetDatabase().DbStats.Database()

	// Start compaction
	resp := rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status := rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateCompleted)

	// Check stats and start time response is correct
	firstStartTime := status.StartTime
	firstStartTimeStat := databaseStats.CompactionAttachmentStartTime.Value()
	assert.False(t, firstStartTime.IsZero())
	assert.NotEqual(t, 0, firstStartTimeStat)
	assert.Equal(t, int64(1), databaseStats.NumAttachmentsCompacted.Value())

	// CompactionAttachmentStartTime has second granularity; sleep to ensure the second run starts
	// in a different second so the stat value strictly increases.
	time.Sleep(time.Second)

	// Start compaction again
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status = rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateCompleted)

	// Check that stats have been updated to new run and previous attachment count stat remains
	assert.True(t, status.StartTime.After(firstStartTime))
	assert.True(t, databaseStats.CompactionAttachmentStartTime.Value() > firstStartTimeStat)
	assert.Equal(t, int64(1), databaseStats.NumAttachmentsCompacted.Value())
}

func TestAttachmentCompactionAbort(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	// Attachment Compaction only runs on _default._default
	rt := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{
		LeakyBucketConfig: &base.LeakyBucketConfig{},
	})
	defer rt.Close()

	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	rest.CreateLegacyAttachmentDoc(t, ctx, collection, t.Name(), []byte("{}"), "att", []byte("att body"))

	pauser := newCompactionPauser(rt)
	defer pauser.Close()
	pauser.Pause()

	resp := rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.WaitUntilBlocked()

	resp = rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment&action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	pauser.Release()

	status := rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateStopped)
	assert.Equal(t, int64(0), status.PurgedAttachments)
}

func TestAttachmentCompactionMarkPhaseRollback(t *testing.T) {
	ctx := base.TestCtx(t)
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	var garbageVBUUID gocbcore.VbUUID = 1234

	rt := rest.NewRestTesterDefaultCollection(t, nil)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	// Create some 'unmarked' attachments
	makeUnmarkedDoc := func(docid string) {
		err := dataStore.SetRaw(ctx, docid, 0, nil, []byte("{}"))
		require.NoError(t, err)
	}

	for i := range 1000 {
		docID := fmt.Sprintf("%s%s%d", base.AttPrefix, "unmarked", i)
		makeUnmarkedDoc(docID)
	}

	// kick off compaction and wait for "mark" phase to begin
	resp := rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	_ = rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateRunning)

	// immediately stop the compaction process (we just need the status data to be persisted to the bucket)
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment&action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	stat := rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateStopped)
	require.Equal(t, string(db.MarkPhase), stat.Phase)

	// alter persisted dcp metadata from the first run to force a rollback
	checkpointPrefix := db.GetAttachmentCompactionDCPCheckpointPrefix(rt.GetDatabase(), stat.CompactID, "mark")

	meta := base.NewDCPMetadataCS(rt.Context(), dataStore, 1024, 8, checkpointPrefix)
	vbMeta := meta.GetMeta(0)
	vbMeta.VbUUID = garbageVBUUID
	meta.SetMeta(0, vbMeta)
	meta.Persist(rt.Context(), 0, []uint16{0})

	// kick off a new run attempting to start it again (should force into rollback handling)
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_compact?type=attachment&action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	_ = rt.WaitForAttachmentCompactionStatus(db.BackgroundProcessStateCompleted)

	// Validate results of recovered attachment compaction process
	resp = rt.SendAdminRequest("GET", "/{{.db}}/_compact?type=attachment", "")
	rest.RequireStatus(t, resp, http.StatusOK)

	// validate that the compaction process actually recovered from rollback by checking stats
	var response db.AttachmentManagerResponse
	err := base.JSONUnmarshal(resp.BodyBytes(), &response)
	require.NoError(t, err)
	require.Equal(t, db.BackgroundProcessStateCompleted, response.State)
	require.Equal(t, int64(0), response.MarkedAttachments)
	require.Equal(t, int64(1000), response.PurgedAttachments)

}

// compactionPauser blocks the compaction mark phase at the first attachment it encounters. Can be
// Paused and Released multiple times across a test.
// With more than one legacy attachment doc, both the parent doc keys (base.VBucket0DocIDs) and the
// attachment bodies (base.VBucket0AttachmentBodies) must land on vBucket 0: the mark phase's
// SetXattrs calls run on whichever goroutine processes the parent doc's mutation, not one keyed
// off the attachment's own vBucket, so constraining only the bodies still allows concurrent calls.
type compactionPauser struct {
	t           testing.TB
	blocked     chan struct{}
	blockCh     chan struct{}
	ds          *base.LeakyDataStore
	callbackSet atomic.Bool
}

func newCompactionPauser(rt *rest.RestTester) *compactionPauser {
	leakyDS, ok := base.AsLeakyDataStore(rt.Bucket().DefaultDataStore(rt.Context()))
	require.True(rt.TB(), ok, "datastore must be a LeakyDataStore")
	return &compactionPauser{
		t:  rt.TB(),
		ds: leakyDS,
	}
}

// Pause arms the pauser to block the mark phase at the first attachment it encounters. Call
// Release before pausing again.
func (p *compactionPauser) Pause() {
	if !p.callbackSet.CompareAndSwap(false, true) {
		require.FailNow(p.t, "compactionPauser.Pause called while already paused; call Release first")
	}
	p.blocked = make(chan struct{})
	p.blockCh = make(chan struct{})
	p.ds.SetXattrCallback(func(key string) error {
		if !strings.HasPrefix(key, base.AttPrefix) {
			return nil
		}
		close(p.blocked)
		// Runs on the mark phase's goroutine, so use the goroutine-safe wait.
		sgtest.RequireChanClosedFromCallback(p.t, p.blockCh)
		return nil
	})
}

// WaitUntilBlocked blocks until compaction is paused at an attachment doc.
func (p *compactionPauser) WaitUntilBlocked() {
	p.t.Helper()
	base.RequireChanClosed(p.t, p.blocked)
}

// Release clears the callback and unblocks the paused doc. Fails the test if not currently paused.
func (p *compactionPauser) Release() {
	if !p.release() {
		require.FailNow(p.t, "compactionPauser.Release called while not paused")
	}
}

// Close releases the pauser and resets the LeakyBucket callback.
func (p *compactionPauser) Close() {
	p.release()
}

// release clears the callback and unblocks the paused doc if currently paused, reporting whether
// it was paused.
func (p *compactionPauser) release() bool {
	if !p.callbackSet.CompareAndSwap(true, false) {
		return false
	}
	p.ds.SetXattrCallback(nil)
	close(p.blockCh)
	return true
}
