package rest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminAPIAuth(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	endPoints := []struct {
		Method          string
		DBScoped        bool
		Endpoint        string
		SkipSuccessTest bool
	}{
		{
			"POST",
			true,
			"/_session",
			false,
		},
		{
			"GET",
			true,
			"/_session/id",
			false,
		},
		{
			"DELETE",
			true,
			"/_session/id",
			false,
		},
		{
			"GET",
			true,
			"/_raw/doc",
			false,
		},
		{
			"GET",
			true,
			"/_user/",
			false,
		}, {
			"POST",
			true,
			"/_user/",
			false,
		}, {
			"GET",
			true,
			"/_user/user",
			false,
		}, {
			"PUT",
			true,
			"/_user/user",
			false,
		}, {
			"DELETE",
			true,
			"/_user/user",
			false,
		}, {
			"DELETE",
			true,
			"/_user/user/_session",
			false,
		}, {
			"DELETE",
			true,
			"/_user/user/_session/id",
			false,
		},
		{
			"GET",
			true,
			"/_role/",
			false,
		}, {
			"POST",
			true,
			"/_role/",
			false,
		},
		{
			"GET",
			true,
			"/_role/role",
			false,
		}, {
			"PUT",
			true,
			"/_role/role",
			false,
		}, {
			"DELETE",
			true,
			"/_role/role",
			false,
		}, {
			"GET",
			true,
			"/_replication/",
			false,
		}, {
			"POST",
			true,
			"/_replication/",
			false,
		}, {
			"GET",
			true,
			"/_replication/id",
			false,
		},
		{
			"PUT",
			true,
			"/_replication/id",
			false,
		},
		{
			"DELETE",
			true,
			"/_replication/id",
			false,
		},
		{
			"GET",
			true,
			"/_replicationStatus/",
			false,
		},
		{
			"GET",
			true,
			"/_replicationStatus/id",
			false,
		},
		{
			"PUT",
			true,
			"/_replicationStatus/id",
			false,
		},
		{
			"GET",
			false,
			"/_logging",
			false,
		},
		{
			"PUT",
			false,
			"/_logging",
			false,
		},
		{
			"POST",
			false,
			"/_logging",
			false,
		},
		{
			"POST",
			false,
			"/_profile/name",
			false,
		},
		{
			"POST",
			false,
			"/_profile",
			false,
		},
		{
			"POST",
			false,
			"/_heap",
			false,
		},
		{
			"GET",
			false,
			"/_stats",
			false,
		},
		{
			"GET",
			false,
			"/_expvar",
			false,
		},
		{
			"GET",
			false,
			"/_config",
			false,
		},
		{
			"GET",
			false,
			"/_status",
			false,
		},
		{
			"GET",
			false,
			"/_sgcollect_info",
			false,
		},
		{
			"DELETE",
			false,
			"/_sgcollect_info",
			false,
		},
		{
			"POST",
			false,
			"/_sgcollect_info",
			false,
		},
		{
			"GET",
			false,
			"/_debug/pprof/goroutine",
			false,
		},
		{
			"GET",
			false,
			"/_debug/pprof/cmdline",
			false,
		},
		{
			"GET",
			false,
			"/_debug/pprof/symbol",
			false,
		},
		{
			"GET",
			false,
			"/_debug/pprof/heap",
			false,
		},
		{
			"GET",
			false,
			"/_debug/pprof/profile",
			false,
		},
		{
			"GET",
			false,
			"/_debug/pprof/block",
			false,
		},
		{
			"GET",
			false,
			"/_debug/pprof/threadcreate",
			false,
		},
		{
			"GET",
			false,
			"/_debug/pprof/mutex",
			false,
		},
		{
			"GET",
			false,
			"/_debug/pprof/trace",
			false,
		},
		{
			"GET",
			false,
			"/_debug/fgprof",
			false,
		},
		{
			"POST",
			false,
			"/_post_upgrade",
			false,
		},
		{
			"GET",
			true,
			"/_config",
			false,
		},
		{
			"PUT",
			true,
			"/_config",
			false,
		},
		{
			"GET",
			true,
			"/_resync",
			false,
		},
		{
			"POST",
			true,
			"/_resync",
			false,
		},
		{
			"POST",
			true,
			"/_purge",
			false,
		},
		{
			"POST",
			true,
			"/_flush",
			false,
		},
		{
			"POST",
			true,
			"/_offline",
			true,
		},
		{
			"POST",
			true,
			"/_online",
			false,
		},
		{
			"GET",
			true,
			"/_dump/view",
			false,
		},
		{
			"GET",
			true,
			"/_view/view",
			false,
		},
		{
			"GET",
			true,
			"/_dumpchannel/channel",
			false,
		},
		{
			"POST",
			true,
			"/_repair",
			false,
		},
		{
			"PUT",
			true,
			"/db",
			false,
		},
		{
			"DELETE",
			true,
			"/",
			true,
		},
		{
			"GET",
			false,
			"/_all_dbs",
			false,
		},
		{
			"POST",
			true,
			"/_compact",
			false,
		},
		{
			"GET",
			true,
			"/",
			true,
		},
		{
			"POST",
			true,
			"/",
			true,
		},
		{
			"GET",
			true,
			"/_all_docs",
			false,
		},
		{
			"POST",
			true,
			"/_bulk_docs",
			false,
		},
		{
			"POST",
			true,
			"/_bulk_get",
			false,
		},
		{
			"GET",
			true,
			"/_changes",
			false,
		},
		{
			"GET",
			true,
			"/_design/ddoc",
			false,
		},
		{
			"PUT",
			true,
			"/_design/ddoc",
			false,
		},
		{
			"DELETE",
			true,
			"/_design/ddoc",
			false,
		},
		{
			"GET",
			true,
			"/_design/ddoc/_view/view",
			false,
		},
		{
			"POST",
			true,
			"/_ensure_full_commit",
			false,
		},
		{
			"POST",
			true,
			"/_revs_diff",
			false,
		},
		{
			"GET",
			true,
			"/_local/docid",
			false,
		},
		{
			"PUT",
			true,
			"/_local/docid",
			false,
		},
		{
			"DELETE",
			true,
			"/_local/docid",
			false,
		},
		{
			"GET",
			true,
			"/docid",
			false,
		},
		{
			"PUT",
			true,
			"/docid",
			false,
		},
		{
			"DELETE",
			true,
			"/docid",
			false,
		},
		{
			"GET",
			true,
			"/docid/attachid",
			false,
		},
		{
			"PUT",
			true,
			"/docid/attachid",
			false,
		},
		{
			"GET",
			true,
			"/_blipsync",
			false,
		},
	}

	rt := NewRestTester(t, &RestTesterConfig{
		adminInterfaceAuthentication:   true,
		metricsInterfaceAuthentication: true,
	})
	defer rt.Close()

	eps, _, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	MakeUser(t, eps[0], "noaccess", "password", []string{})
	defer DeleteUser(t, eps[0], "noaccess")

	MakeUser(t, eps[0], "MobileSyncGatewayUser", "password", []string{fmt.Sprintf("%s[%s]", MobileSyncGatewayRole.RoleName, rt.Bucket().GetName())})
	defer DeleteUser(t, eps[0], "MobileSyncGatewayUser")

	MakeUser(t, eps[0], "ROAdminUser", "password", []string{ReadOnlyAdminRole.RoleName})
	defer DeleteUser(t, eps[0], "ROAdminUser")

	for _, endPoint := range endPoints {
		t.Run(endPoint.Method+endPoint.Endpoint, func(t *testing.T) {
			formattedEndpoint := endPoint.Endpoint
			if endPoint.DBScoped {
				formattedEndpoint = "/db" + formattedEndpoint
			}
			resp := rt.SendAdminRequest(endPoint.Method, formattedEndpoint, `{}`)
			assertStatus(t, resp, http.StatusUnauthorized)

			resp = rt.SendAdminRequestWithAuth(endPoint.Method, formattedEndpoint, `{}`, "noaccess", "password")
			assertStatus(t, resp, http.StatusForbidden)

			if !endPoint.SkipSuccessTest {
				if endPoint.DBScoped {
					resp = rt.SendAdminRequestWithAuth(endPoint.Method, formattedEndpoint, `{}`, "MobileSyncGatewayUser", "password")
				} else {
					resp = rt.SendAdminRequestWithAuth(endPoint.Method, formattedEndpoint, `{}`, "ROAdminUser", "password")
				}

				// For some of the endpoints they have other requirements, such as setting up users and others require
				// bodies. Rather than doing a full test of the endpoint itself this will at least confirm that they pass
				// the auth stage.
				fmt.Println(resp.Code)
				assert.True(t, resp.Code != http.StatusUnauthorized && resp.Code != http.StatusForbidden)
			}
		})
	}

}
