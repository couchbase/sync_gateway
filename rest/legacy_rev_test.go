package rest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

// TestResyncLegacyRev makes sure that running resync on a legacy rev will send future blip messages as a legacy rev and not as an HLV
func TestResyncLegacyRev(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	const (
		alice = "alice"
	)
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	rt.CreateUser(alice, []string{collection.Name})

	docID := db.SafeDocumentName(t, t.Name())
	doc := rt.CreateDocNoHLV(docID, db.Body{"foo": "bar"})

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true // requires hlv
	btcRunner.Run(func(t *testing.T) {
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{Username: alice})
		defer btc.Close()

		btcRunner.StartOneshotPull(btc.id)

		msg := btcRunner.WaitForPullRevMessage(btc.id, docID, DocVersion{RevTreeID: doc.GetRevTreeID()})
		require.Equal(t, msg.Properties[db.RevMessageRev], doc.GetRevTreeID())
	})
	_, err := collection.UpdateSyncFun(ctx, fmt.Sprintf(`function() {channel("A", "%s")}`, collection.Name))
	require.NoError(t, err)

	// use ResyncDocument and TakeDbOffline/Online instead of /ks/_config/sync && /db/_resync to work under rosmar which
	// doesn't yet support DCP resync or updating config on an existing bucket.
	regenerateSequences := false
	var unusedSequences []uint64
	_, _, err = collection.ResyncDocument(ctx, docID, docID, regenerateSequences, unusedSequences)
	require.NoError(t, err)

	rt.TakeDbOffline()
	rt.TakeDbOnline()

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/"+docID, "")
	RequireStatus(t, resp, http.StatusOK)

	t.Logf("=== After resync ===")
	btcRunner = NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true // r
	btcRunner.Run(func(t *testing.T) {
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{Username: alice})
		defer btc.Close()

		btcRunner.StartOneshotPull(btc.id)

		msg := btcRunner.WaitForPullRevMessage(btc.id, docID, DocVersion{RevTreeID: doc.GetRevTreeID()})
		require.Equal(t, msg.Properties[db.RevMessageRev], doc.GetRevTreeID())
	})
}
