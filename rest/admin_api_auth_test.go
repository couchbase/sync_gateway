package rest

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func MakeUser(t *testing.T, serverURL, username, password string, roles []string) {
	httpClient := http.DefaultClient

	form := url.Values{}
	form.Add("password", password)
	form.Add("roles", strings.Join(roles, ","))

	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/settings/rbac/users/local/%s", serverURL, username), strings.NewReader(form.Encode()))
	require.NoError(t, err)

	req.SetBasicAuth(base.TestClusterUsername(), base.TestClusterPassword())
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := httpClient.Do(req)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func DeleteUser(t *testing.T, serverURL, username string) {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/settings/rbac/users/local/%s", serverURL, username), nil)
	require.NoError(t, err)

	req.SetBasicAuth(base.TestClusterUsername(), base.TestClusterPassword())

	httpClient := http.DefaultClient
	resp, err := httpClient.Do(req)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestCheckPermissions(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	clusterAdminPermission := Permission{"!admin", false}
	clusterReadOnlyAdminPermission := Permission{"!ro_admin", false}

	testCases := []struct {
		Name                      string
		Username                  string
		Password                  string
		RequestPermissions        []Permission
		ResponsePermissions       []Permission
		ExpectedStatusCode        int
		ExpectedPermissionResults map[string]bool
		CreateUser                string
		CreatePassword            string
		CreateRoles               []string
	}{
		{
			Name:                      "ClusterAdminTest",
			Username:                  base.TestClusterUsername(),
			Password:                  base.TestClusterPassword(),
			RequestPermissions:        []Permission{clusterAdminPermission},
			ExpectedStatusCode:        http.StatusOK,
			ExpectedPermissionResults: nil,
		},
		{
			Name:                      "CreatedAdmin",
			Username:                  "CreatedAdmin",
			Password:                  "password",
			RequestPermissions:        []Permission{clusterAdminPermission},
			ExpectedStatusCode:        http.StatusOK,
			ExpectedPermissionResults: nil,
			CreateUser:                "CreatedAdmin",
			CreatePassword:            "password",
			CreateRoles:               []string{"admin"},
		},
		{
			Name:                      "Non-Existent User",
			Username:                  "NonExistent",
			Password:                  "",
			RequestPermissions:        []Permission{clusterAdminPermission},
			ExpectedStatusCode:        http.StatusUnauthorized,
			ExpectedPermissionResults: nil,
		},
		{
			Name:                      "Wrong Password",
			Username:                  "WrongPassUser",
			Password:                  "incorrectPass",
			RequestPermissions:        nil,
			ExpectedStatusCode:        http.StatusUnauthorized,
			ExpectedPermissionResults: nil,
			CreateUser:                "WrongPassUser",
			CreatePassword:            "password",
			CreateRoles:               nil,
		},
		{
			Name:                      "Missing Permission",
			Username:                  "NoPermUser",
			Password:                  "password",
			RequestPermissions:        []Permission{clusterAdminPermission},
			ExpectedStatusCode:        http.StatusForbidden,
			ExpectedPermissionResults: nil,
			CreateUser:                "NoPermUser",
			CreatePassword:            "password",
			CreateRoles:               []string{"ro_admin"},
		},
		{
			Name:                      "HasResponsePermissionWithoutAccessPermission",
			Username:                  "HasResponsePermissionWithoutAccessPermission",
			Password:                  "password",
			RequestPermissions:        []Permission{clusterAdminPermission},
			ResponsePermissions:       []Permission{clusterReadOnlyAdminPermission},
			ExpectedStatusCode:        http.StatusForbidden,
			ExpectedPermissionResults: nil,
			CreateUser:                "HasResponsePermissionWithoutAccessPermission",
			CreatePassword:            "password",
			CreateRoles:               []string{"ro_admin"},
		},
		{
			Name:                      "ValidateResponsePermission",
			Username:                  "ValidateResponsePermission",
			Password:                  "password",
			RequestPermissions:        []Permission{clusterAdminPermission},
			ResponsePermissions:       []Permission{clusterReadOnlyAdminPermission},
			ExpectedStatusCode:        http.StatusOK,
			ExpectedPermissionResults: map[string]bool{"!ro_admin": true},
			CreateUser:                "ValidateResponsePermission",
			CreatePassword:            "password",
			CreateRoles:               []string{"admin"},
		},
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			if testCase.CreateUser != "" {
				MakeUser(t, eps[0], testCase.CreateUser, testCase.CreatePassword, testCase.CreateRoles)
				defer DeleteUser(t, eps[0], testCase.CreateUser)
			}

			statusCode, permResults, err := CheckPermissions(httpClient, eps, "", testCase.Username, testCase.Password, testCase.RequestPermissions, testCase.ResponsePermissions)
			require.NoError(t, err)
			assert.Equal(t, testCase.ExpectedStatusCode, statusCode)
			assert.True(t, reflect.DeepEqual(testCase.ExpectedPermissionResults, permResults))
		})
	}
}

func TestCheckPermissionsWithX509(t *testing.T) {
	tb, teardownFn, caCertPath, certPath, keyPath := setupX509Tests(t, true)
	defer tb.Close()
	defer teardownFn()

	ctx := NewServerContext(&StartupConfig{
		Bootstrap: BootstrapConfig{
			Server:       base.UnitTestUrl(),
			X509CertPath: certPath,
			X509KeyPath:  keyPath,
			CACertPath:   caCertPath,
		},
	}, false)
	defer ctx.Close()

	eps, httpClient, err := ctx.ObtainManagementEndpointsAndHTTPClient()
	assert.NoError(t, err)

	statusCode, _, err := CheckPermissions(httpClient, eps, "", base.TestClusterUsername(), base.TestClusterPassword(), []Permission{Permission{"!admin", false}}, nil)
	assert.NoError(t, err)

	assert.Equal(t, http.StatusOK, statusCode)
}

func TestCheckRoles(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	testCases := []struct {
		Name               string
		Username           string
		Password           string
		BucketName         string
		RequestRoles       []RouteRole
		ExpectedStatusCode int
		CreateUser         string
		CreatePassword     string
		CreateRoles        []string
	}{
		{
			Name:               "ClusterAdmin",
			Username:           base.TestClusterUsername(),
			Password:           base.TestClusterPassword(),
			BucketName:         "",
			RequestRoles:       []RouteRole{FullAdminRole},
			ExpectedStatusCode: http.StatusOK,
		},
		{
			Name:               "CreatedAdmin",
			Username:           "CreatedAdmin",
			Password:           "password",
			BucketName:         "",
			RequestRoles:       []RouteRole{FullAdminRole},
			ExpectedStatusCode: http.StatusOK,
			CreateUser:         "CreatedAdmin",
			CreatePassword:     "password",
			CreateRoles:        []string{"admin"},
		},
		{
			Name:               "ReadOnlyAdmin",
			Username:           "ReadOnlyAdmin",
			Password:           "password",
			BucketName:         "",
			RequestRoles:       []RouteRole{ReadOnlyAdminRole},
			ExpectedStatusCode: http.StatusOK,
			CreateUser:         "ReadOnlyAdmin",
			CreatePassword:     "password",
			CreateRoles:        []string{"ro_admin"},
		},
		{
			Name:               "CreatedBucketAdmin",
			Username:           "CreatedBucketAdmin",
			Password:           "password",
			BucketName:         rt.Bucket().GetName(),
			RequestRoles:       []RouteRole{MobileSyncGatewayRole},
			ExpectedStatusCode: http.StatusOK,
			CreateUser:         "CreatedBucketAdmin",
			CreatePassword:     "password",
			CreateRoles:        []string{fmt.Sprintf("mobile_sync_gateway[%s]", rt.Bucket().GetName())},
		},
		{
			Name:               "CreatedBucketAdminWildcard",
			Username:           "CreatedBucketAdminWildcard",
			Password:           "password",
			BucketName:         rt.Bucket().GetName(),
			RequestRoles:       []RouteRole{MobileSyncGatewayRole},
			ExpectedStatusCode: http.StatusOK,
			CreateUser:         "CreatedBucketAdminWildcard",
			CreatePassword:     "password",
			CreateRoles:        []string{"mobile_sync_gateway[*]"},
		},
		{
			Name:               "CreateUserNoRole",
			Username:           "CreateUserNoRole",
			Password:           "password",
			BucketName:         "",
			RequestRoles:       []RouteRole{ReadOnlyAdminRole},
			ExpectedStatusCode: http.StatusForbidden,
			CreateUser:         "CreateUserNoRole",
			CreatePassword:     "password",
			CreateRoles:        []string{""},
		},
		{
			Name:               "CreateUserInsufficientRole",
			Username:           "CreateUserInsufficientRole",
			Password:           "password",
			BucketName:         "",
			RequestRoles:       []RouteRole{MobileSyncGatewayRole},
			ExpectedStatusCode: http.StatusForbidden,
			CreateUser:         "CreateUserInsufficientRole",
			CreatePassword:     "password",
			CreateRoles:        []string{fmt.Sprintf("bucket_full_access[%s]", rt.Bucket().GetName())},
		},
	}

	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			if testCase.CreateUser != "" {
				MakeUser(t, eps[0], testCase.CreateUser, testCase.CreatePassword, testCase.CreateRoles)
				defer DeleteUser(t, eps[0], testCase.CreateUser)
			}

			statusCode, err := CheckRoles(httpClient, eps, testCase.Username, testCase.Password, testCase.RequestRoles, testCase.BucketName)
			require.NoError(t, err)
			assert.Equal(t, testCase.ExpectedStatusCode, statusCode)
		})
	}
}

func TestAdminAuth(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	rt := NewRestTester(t, nil)
	defer rt.Close()

	BucketFullAccessRoleTest := fmt.Sprintf("bucket_full_access[%s]", rt.Bucket().GetName())
	clusterAdminPermission := Permission{"!admin", false}
	bucketWritePermission := Permission{"!write", true}

	testCases := []struct {
		Name                string
		Username            string
		Password            string
		CheckPermissions    []Permission
		ResponsePermissions []Permission
		ExpectedStatusCode  int
		ExpectedPermResults map[string]bool
		CreateUser          string
		CreatePassword      string
		CreateRoles         []string
		BucketName          string
	}{
		{
			Name:               "ClusterAdmin",
			Username:           base.TestClusterUsername(),
			Password:           base.TestClusterPassword(),
			CheckPermissions:   []Permission{clusterAdminPermission},
			ExpectedStatusCode: http.StatusOK,
			BucketName:         "",
		},
		{
			Name:               "ClusterAdminWrongPassword",
			Username:           "ClusterAdminWrongPassword",
			Password:           "wrongpassword",
			CheckPermissions:   []Permission{clusterAdminPermission},
			ExpectedStatusCode: http.StatusUnauthorized,
			CreateUser:         "ClusterAdminWrongPassword",
			CreatePassword:     "password",
			CreateRoles:        []string{"admin"},
			BucketName:         "",
		},
		{
			Name:               "NoUser",
			Username:           "IDontExist",
			Password:           "password",
			CheckPermissions:   []Permission{clusterAdminPermission},
			ExpectedStatusCode: http.StatusUnauthorized,
			BucketName:         "",
		},
		{
			Name:               "MissingPermissionAndRole",
			Username:           "MissingPermissionAndRole",
			Password:           "password",
			CheckPermissions:   []Permission{clusterAdminPermission},
			ExpectedStatusCode: http.StatusForbidden,
			CreateUser:         "MissingPermissionAndRole",
			CreatePassword:     "password",
			CreateRoles:        []string{""},
			BucketName:         "",
		},
		{
			Name:               "MissingPermissionAndRoleDBScoped",
			Username:           "MissingPermissionAndRoleDBScoped",
			Password:           "password",
			CheckPermissions:   []Permission{clusterAdminPermission},
			ExpectedStatusCode: http.StatusForbidden,
			CreateUser:         "MissingPermissionAndRoleDBScoped",
			CreatePassword:     "password",
			CreateRoles:        []string{""},
			BucketName:         rt.Bucket().GetName(),
		},
		{
			Name:               "MissingPermissionHasRole",
			Username:           "MissingPermissionHasRole",
			Password:           "password",
			CheckPermissions:   []Permission{clusterAdminPermission},
			ExpectedStatusCode: http.StatusOK,
			CreateUser:         "MissingPermissionHasRole",
			CreatePassword:     "password",
			CreateRoles:        []string{"ro_admin"},
			BucketName:         "",
		},
		{
			Name:               "MissingPermissionHasDBScoped",
			Username:           "MissingPermissionHasDBScoped",
			Password:           "password",
			CheckPermissions:   []Permission{clusterAdminPermission},
			ExpectedStatusCode: http.StatusOK,
			CreateUser:         "MissingPermissionHasDBScoped",
			CreatePassword:     "password",
			CreateRoles:        []string{BucketFullAccessRoleTest},
			BucketName:         rt.Bucket().GetName(),
		},
		{
			Name:                "HasOneAccessPermissionButHasRole",
			Username:            "HasOneAccessPermissionButHasRole",
			Password:            "password",
			CheckPermissions:    []Permission{bucketWritePermission},
			ResponsePermissions: []Permission{clusterAdminPermission},
			ExpectedStatusCode:  http.StatusOK,
			ExpectedPermResults: map[string]bool{"!admin": true},
			CreateUser:          "HasOneAccessPermissionButHasRole",
			CreatePassword:      "password",
			CreateRoles:         []string{BucketFullAccessRoleTest},
			BucketName:          rt.Bucket().GetName(),
		},
	}

	for _, testCase := range testCases {
		var managementEndpoints []string
		var httpClient *http.Client
		var err error

		if testCase.BucketName != "" {
			managementEndpoints, httpClient, err = rt.GetDatabase().ObtainManagementEndpointsAndHTTPClient()
		} else {
			managementEndpoints, httpClient, err = rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
		}
		require.NoError(t, err)

		t.Run(testCase.Name, func(t *testing.T) {
			if testCase.CreateUser != "" {
				MakeUser(t, managementEndpoints[0], testCase.CreateUser, testCase.CreatePassword, testCase.CreateRoles)
				defer DeleteUser(t, managementEndpoints[0], testCase.CreateUser)
			}

			permResults, statusCode, err := checkAdminAuth(testCase.BucketName, testCase.Username, testCase.Password, "", httpClient, managementEndpoints, true, testCase.CheckPermissions, testCase.ResponsePermissions)

			assert.NoError(t, err)
			assert.Equal(t, testCase.ExpectedStatusCode, statusCode)

			if testCase.ExpectedPermResults != nil {
				assert.True(t, reflect.DeepEqual(testCase.ExpectedPermResults, permResults))
			}

		})

	}
}

func TestAdminAuthWithX509(t *testing.T) {
	tb, teardownFn, caCertPath, certPath, keyPath := setupX509Tests(t, true)
	defer tb.Close()
	defer teardownFn()

	ctx := NewServerContext(&StartupConfig{
		Bootstrap: BootstrapConfig{
			Server:       base.UnitTestUrl(),
			X509CertPath: certPath,
			X509KeyPath:  keyPath,
			CACertPath:   caCertPath,
		},
	}, false)
	defer ctx.Close()

	managementEndpoints, httpClient, err := ctx.ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	_, _, err = checkAdminAuth("", base.TestClusterUsername(), base.TestClusterPassword(), "", httpClient, managementEndpoints, true, []Permission{{"!admin", false}}, nil)
	assert.NoError(t, err)
}

func TestAdminAPIAuth(t *testing.T) {

	// Don't really care about the log level but this test hits the logging endpoint so this is used to reset the logging
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyNone)()

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

	MakeUser(t, eps[0], "ClusterAdminUser", "password", []string{ClusterAdminRole.RoleName})
	defer DeleteUser(t, eps[0], "ClusterAdminUser")

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

				// For some of the endpoints they have other requirements, such as setting up users and others require
				// bodies. Rather than doing a full test of the endpoint itself this will at least confirm that they pass
				// the auth stage.
				if endPoint.DBScoped {
					resp = rt.SendAdminRequestWithAuth(endPoint.Method, formattedEndpoint, `{}`, "MobileSyncGatewayUser", "password")
					assert.True(t, resp.Code != http.StatusUnauthorized && resp.Code != http.StatusForbidden)
				} else {
					resp = rt.SendAdminRequestWithAuth(endPoint.Method, formattedEndpoint, `{}`, "ROAdminUser", "password")
					if endPoint.Method == http.MethodPut || endPoint.Method == http.MethodPost {
						assertStatus(t, resp, http.StatusForbidden)
					} else {
						assert.True(t, resp.Code != http.StatusUnauthorized && resp.Code != http.StatusForbidden)
					}

					resp = rt.SendAdminRequestWithAuth(endPoint.Method, formattedEndpoint, `{}`, "ClusterAdminUser", "password")
					assert.True(t, resp.Code != http.StatusUnauthorized && resp.Code != http.StatusForbidden)

				}
			}
		})
	}

}

func TestDisablePermissionCheck(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	clusterAdminPermission := Permission{"!admin", false}

	// Some random role and perm
	viewsAdminRole := RouteRole{RoleName: "views_admin", DatabaseScoped: true}
	statsReadPermission := Permission{".stats!read", true}

	testCases := []struct {
		Name               string
		CreateUser         string
		CreateUserRole     *RouteRole
		RequirePerms       []Permission
		DoPermissionCheck  bool
		ExpectedStatusCode int
	}{
		{
			Name:               "AccessViaRole",
			CreateUser:         "SGWUser",
			CreateUserRole:     &MobileSyncGatewayRole,
			RequirePerms:       []Permission{clusterAdminPermission},
			DoPermissionCheck:  false,
			ExpectedStatusCode: http.StatusOK,
		},
		{
			Name:               "AccessViaPermission",
			CreateUser:         "PermUser",
			CreateUserRole:     &viewsAdminRole,
			RequirePerms:       []Permission{statsReadPermission},
			DoPermissionCheck:  true,
			ExpectedStatusCode: http.StatusOK,
		},
		{
			Name:               "AccessViaPermissionButDisabled",
			CreateUser:         "SGWUser",
			CreateUserRole:     &viewsAdminRole,
			RequirePerms:       []Permission{statsReadPermission},
			DoPermissionCheck:  false,
			ExpectedStatusCode: http.StatusForbidden,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{
				enableAdminAuthPermissionsCheck: testCase.DoPermissionCheck,
			})
			defer rt.Close()

			eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
			require.NoError(t, err)

			MakeUser(t, eps[0], testCase.CreateUser, "password", []string{fmt.Sprintf("%s[%s]", testCase.CreateUserRole.RoleName, rt.Bucket().GetName())})
			defer DeleteUser(t, eps[0], testCase.CreateUser)

			_, statusCode, err := checkAdminAuth(rt.Bucket().GetName(), testCase.CreateUser, "password", "", httpClient, eps, testCase.DoPermissionCheck, testCase.RequirePerms, nil)
			assert.NoError(t, err)

			assert.Equal(t, testCase.ExpectedStatusCode, statusCode)
		})
	}
}
