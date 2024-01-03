package importuserxattrtest

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserXattrsRawGet(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("Test requires xattrs to be enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	docKey := t.Name()
	xattrKey := "xattrKey"

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				AutoImport:   true,
				UserXattrKey: &xattrKey,
			},
		},
	})
	defer rt.Close()

	userXattrStore, ok := base.AsUserXattrStore(rt.GetSingleDataStore())
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, "{}")
	rest.RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt.WaitForPendingChanges())

	_, err := userXattrStore.WriteUserXattr(docKey, xattrKey, "val")
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value() == 1
	})
	require.NoError(t, err)

	resp = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docKey, "")
	rest.RequireStatus(t, resp, http.StatusOK)

	var RawReturn struct {
		Meta struct {
			Xattrs map[string]interface{} `json:"xattrs"`
		} `json:"_meta"`
	}

	err = json.Unmarshal(resp.BodyBytes(), &RawReturn)
	require.NoError(t, err)

	assert.Equal(t, "val", RawReturn.Meta.Xattrs[xattrKey])
}
