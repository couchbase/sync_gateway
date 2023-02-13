// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	serverURL := base.UnitTestUrl()
	if !base.ServerIsTLS(serverURL) {
		t.Skipf("URI %s needs to start with couchbases://", serverURL)
	}
	tb, caCertPath, certPath, keyPath := setupX509Tests(t, true)
	defer tb.Close()

	ctx := base.TestCtx(t)
	svrctx := NewServerContext(ctx, &StartupConfig{
		Bootstrap: BootstrapConfig{
			Server:       serverURL,
			X509CertPath: certPath,
			X509KeyPath:  keyPath,
			CACertPath:   caCertPath,
		},
	}, false)
	defer svrctx.Close(ctx)

	goCBAgent, err := svrctx.initializeGoCBAgent(ctx)
	require.NoError(t, err)
	svrctx.GoCBAgent = goCBAgent

	noX509HttpClient, err := svrctx.initializeNoX509HttpClient()
	require.NoError(t, err)
	svrctx.NoX509HTTPClient = noX509HttpClient

	eps, httpClient, err := svrctx.ObtainManagementEndpointsAndHTTPClient()
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
	SGWorBFArole := RouteRole{MobileSyncGatewayRole.RoleName, true}
	if base.TestsUseServerCE() {
		SGWorBFArole = RouteRole{BucketFullAccessRole.RoleName, true}
	}

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
			RequestRoles:       []RouteRole{SGWorBFArole},
			ExpectedStatusCode: http.StatusOK,
			CreateUser:         "CreatedBucketAdmin",
			CreatePassword:     "password",
			CreateRoles:        []string{fmt.Sprintf("%s[%s]", SGWorBFArole.RoleName, rt.Bucket().GetName())},
		},
		{
			Name:               "CreatedBucketAdminWildcard",
			Username:           "CreatedBucketAdminWildcard",
			Password:           "password",
			BucketName:         rt.Bucket().GetName(),
			RequestRoles:       []RouteRole{SGWorBFArole},
			ExpectedStatusCode: http.StatusOK,
			CreateUser:         "CreatedBucketAdminWildcard",
			CreatePassword:     "password",
			CreateRoles:        []string{fmt.Sprintf("%s[*]", SGWorBFArole.RoleName)},
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
			RequestRoles:       []RouteRole{SGWorBFArole},
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

		ctx := rt.Context()
		if testCase.BucketName != "" {
			managementEndpoints, httpClient, err = rt.GetDatabase().ObtainManagementEndpointsAndHTTPClient(ctx)
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
	serverURL := base.UnitTestUrl()
	if !base.ServerIsTLS(serverURL) {
		t.Skipf("URI %s needs to start with couchbases://", serverURL)
	}
	tb, caCertPath, certPath, keyPath := setupX509Tests(t, true)
	defer tb.Close()

	ctx := base.TestCtx(t)
	svrctx := NewServerContext(ctx, &StartupConfig{
		Bootstrap: BootstrapConfig{
			Server:       serverURL,
			X509CertPath: certPath,
			X509KeyPath:  keyPath,
			CACertPath:   caCertPath,
		},
	}, false)
	defer svrctx.Close(ctx)

	goCBAgent, err := svrctx.initializeGoCBAgent(ctx)
	require.NoError(t, err)
	svrctx.GoCBAgent = goCBAgent

	noX509HttpClient, err := svrctx.initializeNoX509HttpClient()
	require.NoError(t, err)
	svrctx.NoX509HTTPClient = noX509HttpClient

	managementEndpoints, httpClient, err := svrctx.ObtainManagementEndpointsAndHTTPClient()
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
	base.LongRunningTest(t)

	// Don't really care about the log level but this test hits the logging endpoint so this is used to reset the logging
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyNone)

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}
	serverURL := base.UnitTestUrl()
	if base.ServerIsTLS(serverURL) {
		t.Skipf("URI %s can not start with couchbases://", serverURL)
	}

	rt := NewRestTester(t, &RestTesterConfig{
		AdminInterfaceAuthentication:   true,
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
		Endpoint        string
		SkipSuccessTest bool
		body            string // The body to use in requests. Default: `{}`
	}{
		{
			Method:   "POST",
			Endpoint: "/{{.db}}/_session",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_session/id",
		},
		{
			Method:   "DELETE",
			Endpoint: "/{{.db}}/_session/id",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.keyspace}}/_raw/doc",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_user/",
		},
		{
			Method:   "POST",
			Endpoint: "/{{.db}}/_user/",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_user/user",
		}, {
			Method:   "PUT",
			Endpoint: "/{{.db}}/_user/user",
		}, {
			Method:   "DELETE",
			Endpoint: "/{{.db}}/_user/user",
		}, {
			Method:   "DELETE",
			Endpoint: "/{{.db}}/_user/user/_session",
		}, {
			Method:   "DELETE",
			Endpoint: "/{{.db}}/_user/user/_session/id",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_role/",
		}, {
			Method:   "POST",
			Endpoint: "/{{.db}}/_role/",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_role/role",
		}, {
			Method:   "PUT",
			Endpoint: "/{{.db}}/_role/role",
		}, {
			Method:   "DELETE",
			Endpoint: "/{{.db}}/_role/role",
		}, {
			Method:   "GET",
			Endpoint: "/{{.db}}/_replication/",
		}, {
			Method:   "POST",
			Endpoint: "/{{.db}}/_replication/",
		}, {
			Method:   "GET",
			Endpoint: "/{{.db}}/_replication/id",
		},
		{
			Method:   "PUT",
			Endpoint: "/{{.db}}/_replication/id",
		},
		{
			Method:   "DELETE",
			Endpoint: "/{{.db}}/_replication/id",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_replicationStatus/",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_replicationStatus/id",
		},
		{
			Method:   "PUT",
			Endpoint: "/{{.db}}/_replicationStatus/id",
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
			Endpoint: "/{{.db}}/_config",
		},
		{
			Method:   "PUT",
			Endpoint: "/{{.db}}/_config",
			body:     string(dbConfigRaw),
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_resync",
		},
		{
			Method:   "POST",
			Endpoint: "/{{.db}}/_resync",
		},
		{
			Method:   "POST",
			Endpoint: "/{{.keyspace}}/_purge",
		},
		{
			Method:   "POST",
			Endpoint: "/{{.db}}/_flush",
		},
		{
			Method:          "POST",
			Endpoint:        "/{{.db}}/_offline",
			SkipSuccessTest: true,
		},
		{
			Method:   "POST",
			Endpoint: "/{{.db}}/_online",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_dump/view",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_view/view",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.keyspace}}/_dumpchannel/channel",
		},
		{
			Method:   "POST",
			Endpoint: "/{{.db}}/_repair",
		},
		{
			Method:   "PUT",
			Endpoint: "/{{.keyspace}}/db",
		},
		{
			Method:          "DELETE",
			Endpoint:        "/{{.db}}/",
			SkipSuccessTest: true,
		},
		{
			Method:   "GET",
			Endpoint: "/_all_dbs",
		},
		{
			Method:   "POST",
			Endpoint: "/{{.db}}/_compact",
		},
		{
			Method:          "GET",
			Endpoint:        "/{{.db}}/",
			SkipSuccessTest: true,
		},
		{
			Method:          "POST",
			Endpoint:        "/{{.db}}/",
			SkipSuccessTest: true,
		},
		{
			Method:   "GET",
			Endpoint: "/{{.keyspace}}/_all_docs",
		},
		{
			Method:   "POST",
			Endpoint: "/{{.keyspace}}/_bulk_docs",
		},
		{
			Method:   "POST",
			Endpoint: "/{{.keyspace}}/_bulk_get",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.keyspace}}/_changes",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_design/ddoc",
		},
		{
			Method:   "PUT",
			Endpoint: "/{{.db}}/_design/ddoc",
		},
		{
			Method:   "DELETE",
			Endpoint: "/{{.db}}/_design/ddoc",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_design/ddoc/_view/view",
		},
		{
			Method:   "POST",
			Endpoint: "/{{.db}}/_ensure_full_commit",
		},
		{
			Method:   "POST",
			Endpoint: "/{{.keyspace}}/_revs_diff",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.keyspace}}/_local/docid",
		},
		{
			Method:   "PUT",
			Endpoint: "/{{.keyspace}}/_local/docid",
		},
		{
			Method:   "DELETE",
			Endpoint: "/{{.keyspace}}/_local/docid",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.keyspace}}/docid",
		},
		{
			Method:   "PUT",
			Endpoint: "/{{.keyspace}}/docid",
		},
		{
			Method:   "DELETE",
			Endpoint: "/{{.keyspace}}/docid",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.keyspace}}/docid/attachid",
		},
		{
			Method:   "PUT",
			Endpoint: "/{{.keyspace}}/docid/attachid",
		},
		{
			Method:   "GET",
			Endpoint: "/{{.db}}/_blipsync",
		},
	}

	SGWorBFArole := MobileSyncGatewayRole.RoleName
	if base.TestsUseServerCE() {
		SGWorBFArole = BucketFullAccessRole.RoleName
	}
	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	MakeUser(t, httpClient, eps[0], "noaccess", "password", []string{})
	defer DeleteUser(t, httpClient, eps[0], "noaccess")

	MakeUser(t, httpClient, eps[0], "MobileSyncGatewayUser", "password", []string{fmt.Sprintf("%s[%s]", SGWorBFArole, rt.Bucket().GetName())})
	defer DeleteUser(t, httpClient, eps[0], "MobileSyncGatewayUser")

	MakeUser(t, httpClient, eps[0], "ROAdminUser", "password", []string{ReadOnlyAdminRole.RoleName})
	defer DeleteUser(t, httpClient, eps[0], "ROAdminUser")

	if base.TestsUseServerCE() {
		MakeUser(t, httpClient, eps[0], "ClusterAdminUser", "password", []string{ClusterAdminRole.RoleName})
		defer DeleteUser(t, httpClient, eps[0], "ClusterAdminUser")
	}

	for _, endPoint := range endPoints {
		body := `{}`
		if endPoint.body != "" {
			body = endPoint.body
		}

		t.Run(endPoint.Method+endPoint.Endpoint, func(t *testing.T) {
			resp := rt.SendAdminRequest(endPoint.Method, endPoint.Endpoint, body)
			RequireStatus(t, resp, http.StatusUnauthorized)

			resp = rt.SendAdminRequestWithAuth(endPoint.Method, endPoint.Endpoint, body, "noaccess", "password")
			RequireStatus(t, resp, http.StatusForbidden)

			if !endPoint.SkipSuccessTest {

				// For some of the endpoints they have other requirements, such as setting up users and others require
				// bodies. Rather than doing a full test of the endpoint itself this will at least confirm that they pass
				// the auth stage.
				if strings.HasPrefix(endPoint.Endpoint, "/{{.") {
					resp = rt.SendAdminRequestWithAuth(endPoint.Method, endPoint.Endpoint, body, "MobileSyncGatewayUser", "password")
					assert.True(t, resp.Code != http.StatusUnauthorized && resp.Code != http.StatusForbidden)
				} else {
					resp = rt.SendAdminRequestWithAuth(endPoint.Method, endPoint.Endpoint, body, "ROAdminUser", "password")
					if endPoint.Method == http.MethodGet || endPoint.Method == http.MethodHead || endPoint.Method == http.MethodOptions {
						assert.True(t, resp.Code != http.StatusUnauthorized && resp.Code != http.StatusForbidden)
					} else {
						RequireStatus(t, resp, http.StatusForbidden)
					}

					if base.TestsUseServerCE() {
						resp = rt.SendAdminRequestWithAuth(endPoint.Method, endPoint.Endpoint, body, "ClusterAdminUser", "password")
						assert.True(t, resp.Code != http.StatusUnauthorized && resp.Code != http.StatusForbidden)
					}
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

	rt := NewRestTester(t, nil)
	SGWorBFArole := RouteRole{MobileSyncGatewayRole.RoleName, true}
	if base.TestsUseServerCE() {
		SGWorBFArole = RouteRole{BucketFullAccessRole.RoleName, true}
	}
	rt.Close()

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
			CreateUserRole:     &SGWorBFArole,
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
		AdminInterfaceAuthentication:    true,
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
					RequireStatus(t, resp, http.StatusForbidden)
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

	tb := base.GetTestBucket(t)
	defer tb.Close()

	rt := NewRestTester(t, &RestTesterConfig{
		AdminInterfaceAuthentication: true,
	})
	defer rt.Close()

	SGWorBFArole := RouteRole{MobileSyncGatewayRole.RoleName, true}
	if base.TestsUseServerCE() {
		SGWorBFArole = RouteRole{BucketFullAccessRole.RoleName, true}
	}

	eps, httpClient, err := rt.ServerContext().ObtainManagementEndpointsAndHTTPClient()
	require.NoError(t, err)

	MakeUser(t, httpClient, eps[0], SGWorBFArole.RoleName, "password", []string{fmt.Sprintf("%s[%s]", SGWorBFArole.RoleName, tb.GetName())})
	defer DeleteUser(t, httpClient, eps[0], SGWorBFArole.RoleName)

	resp := rt.SendAdminRequestWithAuth("PUT", "/db2/", `{"bucket": "`+tb.GetName()+`", "username": "`+base.TestClusterUsername()+`", "password": "`+base.TestClusterPassword()+`", "num_index_replicas": 0, "use_views": `+strconv.FormatBool(base.TestsDisableGSI())+`}`, SGWorBFArole.RoleName, "password")
	RequireStatus(t, resp, http.StatusCreated)
}
