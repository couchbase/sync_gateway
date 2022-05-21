package rest

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func MakeUser(t *testing.T, httpClient *http.Client, serverURL, username, password string, roles []string) {
	form := url.Values{}
	form.Add("password", password)
	form.Add("roles", strings.Join(roles, ","))

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {
		req, err := http.NewRequest("PUT", fmt.Sprintf("%s/settings/rbac/users/local/%s", serverURL, username), strings.NewReader(form.Encode()))
		require.NoError(t, err)

		req.SetBasicAuth(base.TestClusterUsername(), base.TestClusterPassword())
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

		resp, err := httpClient.Do(req)
		if err != nil {
			return true, err, resp
		}
		return false, err, resp
	}

	err, resp := base.RetryLoop("Admin Auth testing MakeUser", retryWorker, base.CreateSleeperFunc(10, 100))
	require.NoError(t, err)

	if resp.(*http.Response).StatusCode != http.StatusOK {
		bodyResp, err := ioutil.ReadAll(resp.(*http.Response).Body)
		assert.NoError(t, err)
		fmt.Println(string(bodyResp))
	}
	require.Equal(t, http.StatusOK, resp.(*http.Response).StatusCode)

	require.NoError(t, resp.(*http.Response).Body.Close(), "Error closing response body")
}

func DeleteUser(t *testing.T, httpClient *http.Client, serverURL, username string) {
	retryWorker := func() (shouldRetry bool, err error, value interface{}) {
		req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/settings/rbac/users/local/%s", serverURL, username), nil)
		require.NoError(t, err)

		req.SetBasicAuth(base.TestClusterUsername(), base.TestClusterPassword())

		resp, err := httpClient.Do(req)
		if err != nil {
			return true, err, resp
		}
		return false, err, resp
	}

	err, resp := base.RetryLoop("Admin Auth testing DeleteUser", retryWorker, base.CreateSleeperFunc(10, 100))
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.(*http.Response).StatusCode)

	require.NoError(t, resp.(*http.Response).Body.Close(), "Error closing response body")
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
				MakeUser(t, httpClient, eps[0], testCase.CreateUser, testCase.CreatePassword, testCase.CreateRoles)
				defer DeleteUser(t, httpClient, eps[0], testCase.CreateUser)
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

	goCBAgent, err := ctx.initializeGoCBAgent()
	require.NoError(t, err)
	ctx.GoCBAgent = goCBAgent

	noX509HttpClient, err := ctx.initializeNoX509HttpClient()
	require.NoError(t, err)
	ctx.NoX509HTTPClient = noX509HttpClient

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
				MakeUser(t, httpClient, eps[0], testCase.CreateUser, testCase.CreatePassword, testCase.CreateRoles)
				defer DeleteUser(t, httpClient, eps[0], testCase.CreateUser)
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
		Operation           string
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
			Operation:          "GET",
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
				MakeUser(t, httpClient, managementEndpoints[0], testCase.CreateUser, testCase.CreatePassword, testCase.CreateRoles)
				defer DeleteUser(t, httpClient, managementEndpoints[0], testCase.CreateUser)
			}

			permResults, statusCode, err := checkAdminAuth(testCase.BucketName, testCase.Username, testCase.Password, testCase.Operation, httpClient, managementEndpoints, true, testCase.CheckPermissions, testCase.ResponsePermissions)

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

	goCBAgent, err := ctx.initializeGoCBAgent()
	require.NoError(t, err)
	ctx.GoCBAgent = goCBAgent

	noX509HttpClient, err := ctx.initializeNoX509HttpClient()
	require.NoError(t, err)
	ctx.NoX509HTTPClient = noX509HttpClient

	managementEndpoints, httpClient, err := ctx.ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	var statusCode int
	_, statusCode, err = checkAdminAuth("", base.TestClusterUsername(), base.TestClusterPassword(), "", httpClient, managementEndpoints, true, []Permission{{"!admin", false}}, nil)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, statusCode)

	_, statusCode, err = checkAdminAuth("", "invalidUser", "invalidPassword", "", httpClient, managementEndpoints, true, []Permission{{"!admin", false}}, nil)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, statusCode)
}

func TestAdminAPIAuth(t *testing.T) {

	// Don't really care about the log level but this test hits the logging endpoint so this is used to reset the logging
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyNone)

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	rt := NewRestTester(t, &RestTesterConfig{
		adminInterfaceAuthentication:   true,
		metricsInterfaceAuthentication: true,
	})
	defer rt.Close()

	// init RT bucket so we can get the config
	_ = rt.Bucket()
	dbConfig := rt.DatabaseConfig
	dbConfigRaw, err := base.JSONMarshal(dbConfig)
	require.NoError(t, err)

	endPoints := []struct {
		Method          string
		DBScoped        bool
		Endpoint        string
		SkipSuccessTest bool
		body            string // The body to use in requests. Default: `{}`
	}{
		{
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_session",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_session/id",
		},
		{
			Method:   "DELETE",
			DBScoped: true,
			Endpoint: "/_session/id",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_raw/doc",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_user/",
		}, {
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_user/",
		}, {
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_user/user",
		}, {
			Method:   "PUT",
			DBScoped: true,
			Endpoint: "/_user/user",
		}, {
			Method:   "DELETE",
			DBScoped: true,
			Endpoint: "/_user/user",
		}, {
			Method:   "DELETE",
			DBScoped: true,
			Endpoint: "/_user/user/_session",
		}, {
			Method:   "DELETE",
			DBScoped: true,
			Endpoint: "/_user/user/_session/id",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_role/",
		}, {
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_role/",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_role/role",
		}, {
			Method:   "PUT",
			DBScoped: true,
			Endpoint: "/_role/role",
		}, {
			Method:   "DELETE",
			DBScoped: true,
			Endpoint: "/_role/role",
		}, {
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_replication/",
		}, {
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_replication/",
		}, {
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_replication/id",
		},
		{
			Method:   "PUT",
			DBScoped: true,
			Endpoint: "/_replication/id",
		},
		{
			Method:   "DELETE",
			DBScoped: true,
			Endpoint: "/_replication/id",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_replicationStatus/",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_replicationStatus/id",
		},
		{
			Method:   "PUT",
			DBScoped: true,
			Endpoint: "/_replicationStatus/id",
		},
		{
			Method:   "GET",
			Endpoint: "/_logging",
		},
		{
			Method:   "PUT",
			Endpoint: "/_logging",
		},
		{
			Method:   "POST",
			Endpoint: "/_logging",
		},
		{
			Method:   "POST",
			Endpoint: "/_profile/name",
		},
		{
			Method:   "POST",
			Endpoint: "/_profile",
		},
		{
			Method:   "POST",
			Endpoint: "/_heap",
		},
		{
			Method:   "GET",
			Endpoint: "/_stats",
		},
		{
			Method:   "GET",
			Endpoint: "/_expvar",
		},
		{
			Method:   "GET",
			Endpoint: "/_config",
		},
		{
			Method:   "GET",
			Endpoint: "/_status",
		},
		{
			Method:   "GET",
			Endpoint: "/_sgcollect_info",
		},
		{
			Method:   "DELETE",
			Endpoint: "/_sgcollect_info",
		},
		{
			Method:   "POST",
			Endpoint: "/_sgcollect_info",
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/goroutine",
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/cmdline",
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/symbol",
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/heap",
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/profile?seconds=1",
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/block?seconds=1",
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/threadcreate",
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/mutex?seconds=1",
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/trace",
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/fgprof?seconds=1",
		},
		{
			Method:   "POST",
			Endpoint: "/_post_upgrade",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_config",
		},
		{
			Method:   "PUT",
			DBScoped: true,
			Endpoint: "/_config",
			body:     string(dbConfigRaw),
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_resync",
		},
		{
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_resync",
		},
		{
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_purge",
		},
		{
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_flush",
		},
		{
			Method:          "POST",
			DBScoped:        true,
			Endpoint:        "/_offline",
			SkipSuccessTest: true,
		},
		{
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_online",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_dump/view",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_view/view",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_dumpchannel/channel",
		},
		{
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_repair",
		},
		{
			Method:   "PUT",
			DBScoped: true,
			Endpoint: "/db",
		},
		{
			Method:          "DELETE",
			DBScoped:        true,
			Endpoint:        "/",
			SkipSuccessTest: true,
		},
		{
			Method:   "GET",
			Endpoint: "/_all_dbs",
		},
		{
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_compact",
		},
		{
			Method:          "GET",
			DBScoped:        true,
			Endpoint:        "/",
			SkipSuccessTest: true,
		},
		{
			Method:          "POST",
			DBScoped:        true,
			Endpoint:        "/",
			SkipSuccessTest: true,
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_all_docs",
		},
		{
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_bulk_docs",
		},
		{
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_bulk_get",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_changes",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_design/ddoc",
		},
		{
			Method:   "PUT",
			DBScoped: true,
			Endpoint: "/_design/ddoc",
		},
		{
			Method:   "DELETE",
			DBScoped: true,
			Endpoint: "/_design/ddoc",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_design/ddoc/_view/view",
		},
		{
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_ensure_full_commit",
		},
		{
			Method:   "POST",
			DBScoped: true,
			Endpoint: "/_revs_diff",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_local/docid",
		},
		{
			Method:   "PUT",
			DBScoped: true,
			Endpoint: "/_local/docid",
		},
		{
			Method:   "DELETE",
			DBScoped: true,
			Endpoint: "/_local/docid",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/docid",
		},
		{
			Method:   "PUT",
			DBScoped: true,
			Endpoint: "/docid",
		},
		{
			Method:   "DELETE",
			DBScoped: true,
			Endpoint: "/docid",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/docid/attachid",
		},
		{
			Method:   "PUT",
			DBScoped: true,
			Endpoint: "/docid/attachid",
		},
		{
			Method:   "GET",
			DBScoped: true,
			Endpoint: "/_blipsync",
		},
	}

	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	MakeUser(t, httpClient, eps[0], "noaccess", "password", []string{})
	defer DeleteUser(t, httpClient, eps[0], "noaccess")

	MakeUser(t, httpClient, eps[0], "MobileSyncGatewayUser", "password", []string{fmt.Sprintf("%s[%s]", MobileSyncGatewayRole.RoleName, rt.Bucket().GetName())})
	defer DeleteUser(t, httpClient, eps[0], "MobileSyncGatewayUser")

	MakeUser(t, httpClient, eps[0], "ROAdminUser", "password", []string{ReadOnlyAdminRole.RoleName})
	defer DeleteUser(t, httpClient, eps[0], "ROAdminUser")

	MakeUser(t, httpClient, eps[0], "ClusterAdminUser", "password", []string{ClusterAdminRole.RoleName})
	defer DeleteUser(t, httpClient, eps[0], "ClusterAdminUser")

	for _, endPoint := range endPoints {
		body := `{}`
		if endPoint.body != "" {
			body = endPoint.body
		}
		formattedEndpoint := endPoint.Endpoint
		if endPoint.DBScoped {
			formattedEndpoint = "/db" + formattedEndpoint
		}
		t.Run(endPoint.Method+formattedEndpoint, func(t *testing.T) {
			resp := rt.SendAdminRequest(endPoint.Method, formattedEndpoint, body)
			assertStatus(t, resp, http.StatusUnauthorized)

			resp = rt.SendAdminRequestWithAuth(endPoint.Method, formattedEndpoint, body, "noaccess", "password")
			assertStatus(t, resp, http.StatusForbidden)

			if !endPoint.SkipSuccessTest {

				// For some of the endpoints they have other requirements, such as setting up users and others require
				// bodies. Rather than doing a full test of the endpoint itself this will at least confirm that they pass
				// the auth stage.
				if endPoint.DBScoped {
					resp = rt.SendAdminRequestWithAuth(endPoint.Method, formattedEndpoint, body, "MobileSyncGatewayUser", "password")
					assert.True(t, resp.Code != http.StatusUnauthorized && resp.Code != http.StatusForbidden)
				} else {
					resp = rt.SendAdminRequestWithAuth(endPoint.Method, formattedEndpoint, body, "ROAdminUser", "password")
					if endPoint.Method == http.MethodGet || endPoint.Method == http.MethodHead || endPoint.Method == http.MethodOptions {
						assert.True(t, resp.Code != http.StatusUnauthorized && resp.Code != http.StatusForbidden)
					} else {
						assertStatus(t, resp, http.StatusForbidden)
					}

					resp = rt.SendAdminRequestWithAuth(endPoint.Method, formattedEndpoint, body, "ClusterAdminUser", "password")
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

			if testCase.CreateUserRole.DatabaseScoped {
				MakeUser(t, httpClient, eps[0], testCase.CreateUser, "password", []string{fmt.Sprintf("%s[%s]", testCase.CreateUserRole.RoleName, rt.Bucket().GetName())})
			} else {
				MakeUser(t, httpClient, eps[0], testCase.CreateUser, "password", []string{fmt.Sprintf("%s", testCase.CreateUserRole.RoleName)})
			}
			defer DeleteUser(t, httpClient, eps[0], testCase.CreateUser)

			_, statusCode, err := checkAdminAuth(rt.Bucket().GetName(), testCase.CreateUser, "password", "", httpClient, eps, testCase.DoPermissionCheck, testCase.RequirePerms, nil)
			assert.NoError(t, err)

			assert.Equal(t, testCase.ExpectedStatusCode, statusCode)
		})
	}
}

func TestNewlyCreateSGWPermissions(t *testing.T) {
	t.Skip("Requires DP 7.0.1")

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	mobileSyncGateway := "mobile_sync_gateway"
	syncGatewayDevOps := "sync_gateway_dev_ops"
	syncGatewayApp := "sync_gateway_app"
	syncGatewayAppRo := "sync_gateway_app_ro"
	syncGatewayConfigurator := "sync_gateway_configurator"
	syncGatewayReplicator := "sync_gateway_replicator"

	rt := NewRestTester(t, &RestTesterConfig{
		adminInterfaceAuthentication:    true,
		enableAdminAuthPermissionsCheck: true,
	})
	defer rt.Close()

	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	MakeUser(t, httpClient, eps[0], mobileSyncGateway, "password", []string{fmt.Sprintf("%s[*]", mobileSyncGateway)})
	defer DeleteUser(t, httpClient, eps[0], mobileSyncGateway)
	MakeUser(t, httpClient, eps[0], syncGatewayDevOps, "password", []string{fmt.Sprintf("%s", syncGatewayDevOps)})
	defer DeleteUser(t, httpClient, eps[0], syncGatewayDevOps)
	MakeUser(t, httpClient, eps[0], syncGatewayApp, "password", []string{fmt.Sprintf("%s[*]", syncGatewayApp)})
	defer DeleteUser(t, httpClient, eps[0], syncGatewayApp)
	MakeUser(t, httpClient, eps[0], syncGatewayAppRo, "password", []string{fmt.Sprintf("%s[*]", syncGatewayAppRo)})
	defer DeleteUser(t, httpClient, eps[0], syncGatewayAppRo)
	MakeUser(t, httpClient, eps[0], syncGatewayConfigurator, "password", []string{fmt.Sprintf("%s[*]", syncGatewayConfigurator)})
	defer DeleteUser(t, httpClient, eps[0], syncGatewayConfigurator)
	MakeUser(t, httpClient, eps[0], syncGatewayReplicator, "password", []string{fmt.Sprintf("%s[*]", syncGatewayReplicator)})
	defer DeleteUser(t, httpClient, eps[0], syncGatewayReplicator)

	testUsers := []string{syncGatewayDevOps, syncGatewayApp, syncGatewayAppRo, syncGatewayReplicator, syncGatewayConfigurator}

	endPoints := []struct {
		Method   string
		Endpoint string
		Users    []string
	}{
		{
			Method:   "GET",
			Endpoint: "/db/",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "POST",
			Endpoint: "/db/",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_all_docs",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_bulk_docs",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/bulk_get",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_changes",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_design/ddoc",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "PUT",
			Endpoint: "/db/_design/ddoc",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "DELETE",
			Endpoint: "/db/_design/ddoc",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_design/ddoc/_view/view",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_ensure_full_commit",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_revs_diff",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_local/doc",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "PUT",
			Endpoint: "/db/_local/doc",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "DELETE",
			Endpoint: "/db/_local/doc",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/doc",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "PUT",
			Endpoint: "/db/doc",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "DELETE",
			Endpoint: "/db/doc",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/doc/attach",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "PUT",
			Endpoint: "/db/doc/attach",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_blipsync",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_session",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_session/session",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "DELETE",
			Endpoint: "/db/_session/session",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp},
		},
		{
			Method:   "DELETE",
			Endpoint: "/db/_session/session",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_raw/doc",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_revtree/doc",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_user/",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_user/",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_user/user",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "PUT",
			Endpoint: "/db/_user/user",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp},
		},
		{
			Method:   "DELETE",
			Endpoint: "/db/_user/user",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp},
		},
		{
			Method:   "DELETE",
			Endpoint: "/db/_user/user/_session",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp},
		},
		{
			Method:   "DELETE",
			Endpoint: "/db/_user/user/_session/session",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_role/",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_role/",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_role/role",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "PUT",
			Endpoint: "/db/_role/role",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp},
		},
		{
			Method:   "DELETE",
			Endpoint: "/db/_role/role",
			Users:    []string{syncGatewayConfigurator, syncGatewayApp},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_replicationStatus/",
			Users:    []string{syncGatewayReplicator},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_replicationStatus/repl",
			Users:    []string{syncGatewayReplicator},
		},
		{
			Method:   "PUT",
			Endpoint: "/db/_replicationStatus/repl",
			Users:    []string{syncGatewayReplicator},
		},
		{
			Method:   "GET",
			Endpoint: "/_logging",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "PUT",
			Endpoint: "/_logging",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "POST",
			Endpoint: "/_profile/profile",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "POST",
			Endpoint: "/_profile",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "POST",
			Endpoint: "/_heap",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_stats",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_expvar",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_status",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_sgcollect_info",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "DELETE",
			Endpoint: "/_sgcollect_info",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "POST",
			Endpoint: "/_sgcollect_info",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/goroutine",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/cmdline",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/symbol",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/heap",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/profile?seconds=1",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/block?seconds=1",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/threadcreate",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/mutex?seconds=1",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/pprof/trace",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/_debug/fgprof?seconds=1",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "POST",
			Endpoint: "/_post_upgrade",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_config",
			Users:    []string{syncGatewayConfigurator},
		},
		{
			Method:   "PUT",
			Endpoint: "/db/_config",
			Users:    []string{syncGatewayApp, syncGatewayConfigurator},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_resync",
			Users:    []string{syncGatewayConfigurator},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_resync",
			Users:    []string{syncGatewayConfigurator},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_purge",
			Users:    []string{syncGatewayApp},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_flush",
			Users:    []string{syncGatewayDevOps},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_offline",
			Users:    []string{syncGatewayConfigurator},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_online",
			Users:    []string{syncGatewayConfigurator},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_dump/view",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_view/view",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "GET",
			Endpoint: "/db/_dumpchannel/channel",
			Users:    []string{syncGatewayApp, syncGatewayAppRo},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_repair",
			Users:    []string{syncGatewayConfigurator},
		},
		{
			Method:   "PUT",
			Endpoint: "/db/",
			Users:    []string{syncGatewayConfigurator},
		},
		{
			Method:   "POST",
			Endpoint: "/db/_compact",
			Users:    []string{syncGatewayConfigurator},
		},
		{
			Method:   "DELETE",
			Endpoint: "/db/",
			Users:    []string{syncGatewayConfigurator},
		},
		{
			Method:   "GET",
			Endpoint: "/_all_dbs",
			Users:    []string{syncGatewayDevOps},
		},
	}

	for _, endpoint := range endPoints {
		endpoint.Users = append(endpoint.Users, mobileSyncGateway)

		for _, testUser := range testUsers {
			testName := fmt.Sprintf("%s-%s-%s", testUser, endpoint.Method, endpoint.Endpoint)
			t.Run(testName, func(t *testing.T) {
				isAllowedUser := false
				for _, user := range endpoint.Users {
					if user == testUser {
						resp := rt.SendAdminRequestWithAuth(endpoint.Method, endpoint.Endpoint, "", user, "password")
						assert.True(t, resp.Code != http.StatusUnauthorized && resp.Code != http.StatusForbidden)
						isAllowedUser = true
					}
				}

				if !isAllowedUser {
					resp := rt.SendAdminRequestWithAuth(endpoint.Method, endpoint.Endpoint, "", testUser, "password")
					assertStatus(t, resp, http.StatusForbidden)
				}
			})
		}

	}

}

func TestCreateDBSpecificBucketPerm(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.RequireNumTestBuckets(t, 2)

	rt := NewRestTester(t, &RestTesterConfig{
		adminInterfaceAuthentication: true,
	})
	defer rt.Close()

	tb := base.GetTestBucket(t)
	defer tb.Close()

	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	mobileSyncGateway := "mobile_sync_gateway"
	MakeUser(t, httpClient, eps[0], mobileSyncGateway, "password", []string{fmt.Sprintf("%s[%s]", mobileSyncGateway, tb.GetName())})
	defer DeleteUser(t, httpClient, eps[0], mobileSyncGateway)

	resp := rt.SendAdminRequestWithAuth("PUT", "/db2/", `{"bucket": "`+tb.GetName()+`", "username": "`+base.TestClusterUsername()+`", "password": "`+base.TestClusterPassword()+`", "num_index_replicas": 0, "use_views": `+strconv.FormatBool(base.TestsDisableGSI())+`}`, mobileSyncGateway, "password")
	assertStatus(t, resp, http.StatusCreated)
}
