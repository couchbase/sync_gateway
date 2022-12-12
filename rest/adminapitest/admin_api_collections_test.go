package adminapitest

import (
	"fmt"
	"net/http"
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/require"
)

func TestCollectionsSetFunctions(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	// Start SG with bootstrap credentials filled
	ctx := base.TestCtx(t)
	config := rest.BootstrapStartupConfigForTest(t)

	// Get a test bucket, and use it to create the database.
	tb := base.GetTestBucket(t)

	numCollections := 2
	scopesConfig := rest.GetCollectionsConfig(t, tb, numCollections)
	dataStoreNames := rest.GetDataStoreNamesFromScopesConfig(scopesConfig)

	importFilter1 := "function(doc) { console.log('importfilter1'); return true; }"
	syncFunc1 := "function(doc){ console.log('syncFunc1'); channel(doc.channels); }"

	importFilter2 := "function(doc) { console.log('importfilter2'); return true; }"
	syncFunc2 := "function(doc) { console.log('syncFunc2'); channel(doc.channels); }"

	scopesConfig[dataStoreNames[0].ScopeName()].Collections[dataStoreNames[0].CollectionName()] = rest.CollectionConfig{
		ImportFilter: &importFilter1,
		SyncFn:       &syncFunc1,
	}
	scopesConfig[dataStoreNames[1].ScopeName()].Collections[dataStoreNames[1].CollectionName()] = rest.CollectionConfig{
		ImportFilter: &importFilter2,
		SyncFn:       &syncFunc2,
	}

	sc, err := rest.SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	serverErr := make(chan error)
	go func() {
		serverErr <- rest.StartServer(ctx, &config, sc)
	}()
	require.NoError(t, sc.WaitForRESTAPIs())
	defer func() {
		sc.Close(ctx)
		require.NoError(t, <-serverErr)
	}()

	defer tb.Close()

	// Initial DB config
	dbConfig := `{
	"bucket": "` + tb.GetName() + `",
	"name": "db",
	"offline": false,
	"enable_shared_bucket_access": ` + strconv.FormatBool(base.TestUseXattrs()) + `,
	"use_views": ` + strconv.FormatBool(base.TestsDisableGSI()) + `,
	"num_index_replicas": 0,
	}`

	// Create initial database
	resp := rest.BootstrapAdminRequest(t, http.MethodPut, "/db/", dbConfig)
	resp.RequireStatus(http.StatusCreated)

	// Take DB offline
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_offline", "")
	resp.RequireStatus(http.StatusOK)

	// Persist configs
	for _, keyspace := range []string{"db", "db._default", "db._default._default"} {
		resp = rest.BootstrapAdminRequest(t, http.MethodPut, fmt.Sprintf("/%s/_config/import_filter", keyspace), importFilter)
		resp.RequireStatus(http.StatusOK)

		resp = rest.BootstrapAdminRequest(t, http.MethodPut, fmt.Sprintf("/%s/_config/sync", keyspace), syncFunc)
		resp.RequireStatus(http.StatusOK)
	}
	// Take DB online
	resp = rest.BootstrapAdminRequest(t, http.MethodPost, "/db/_online", "")
	resp.RequireStatus(http.StatusOK)

	// Check configs match
	for _, keyspace := range []string{"db", "db._default", "db._default._default"} {
		resp = rest.BootstrapAdminRequest(t, http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace), "")
		resp.RequireResponse(http.StatusOK, importFilter)

		resp = rest.BootstrapAdminRequest(t, http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace), "")
		resp.RequireResponse(http.StatusOK, syncFunc)
	}
}
