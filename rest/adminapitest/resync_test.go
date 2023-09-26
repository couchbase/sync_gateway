package adminapitest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResyncRollback ensures that we allow rollback of
func TestResyncRollback(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test doesn't works with walrus")
	}
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: `function(doc) { channel("x") }`, // use custom sync function to increment sync function counter
	})

	defer rt.Close()

	numDocs := 10
	for i := 0; i < numDocs; i++ {
		rt.CreateDoc(t, fmt.Sprintf("doc%v", i))
	}
	assert.Equal(t, int64(numDocs), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	response := rt.SendAdminRequest("POST", "/{{.db}}/_offline", "")
	rest.RequireStatus(t, response, http.StatusOK)
	require.NoError(t, rt.WaitForDBState(db.RunStateString[db.DBOffline]))

	// we need to wait for the resync to start and not finish so we get a partial completion
	resp := rt.SendAdminRequest("POST", "/{{.db}}/_resync", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	_ = rt.WaitForResyncDCPStatus(db.BackgroundProcessStateRunning)

	// immediately stop the resync process (we just need the status data to be persisted to the bucket), we are looking for partial completion
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_resync?action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status := rt.WaitForResyncDCPStatus(db.BackgroundProcessStateStopped)
	// make sure this hasn't accidentally completed, and we have one document
	require.Equal(t, db.BackgroundProcessStateStopped, status.State)
	require.Greater(t, status.DocsProcessed, int64(1))

	// alter persisted dcp metadata from the first run to force a rollback
	name := db.GenerateResyncDCPStreamName(status.ResyncID)
	checkpointPrefix := fmt.Sprintf("%s:%v", rt.GetDatabase().MetadataKeys.DCPCheckpointPrefix(rt.GetDatabase().Options.GroupID), name)
	meta := base.NewDCPMetadataCS(rt.Context(), rt.Bucket().DefaultDataStore(), 1024, 8, checkpointPrefix)
	vbMeta := meta.GetMeta(0)
	var garbageVBUUID gocbcore.VbUUID = 1234
	vbMeta.VbUUID = garbageVBUUID
	meta.SetMeta(0, vbMeta)
	meta.Persist(rt.Context(), 0, []uint16{0})

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)
	status = rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)

	require.Greater(t, rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value(), int64(numDocs*2))
	require.Greater(t, status.DocsProcessed, int64(numDocs))
}
