/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package indextest

import (
	"fmt"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRoleQuery(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	testCases := []struct {
		isServerless bool
	}{
		{
			isServerless: false,
		},
		{
			isServerless: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("TestRoleQuery in Serverless=%t", testCase.isServerless), func(t *testing.T) {
			dbContextConfig := getDatabaseContextOptions(testCase.isServerless)

			tBucket := base.GetTestBucketDefaultCollection(t)
			database, ctx := db.SetupTestDBForDataStoreWithOptions(t, tBucket, dbContextConfig)
			defer database.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				require.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)

			authenticator := database.Authenticator(ctx)
			require.NotNil(t, authenticator, "database.Authenticator(ctx) returned nil")

			// Add roles
			for i := 1; i <= 5; i++ {
				role, err := authenticator.NewRole(fmt.Sprintf("role%d", i), base.SetOf("ABC"))
				require.NoError(t, err, "Error creating new role")
				require.NoError(t, authenticator.Save(role))
			}

			// Delete 1 role
			role1, err := authenticator.NewRole("role1", base.SetOf("ABC"))
			require.NoError(t, err)
			err = authenticator.DeleteRole(role1, false, 0)
			require.NoError(t, err)

			// Standard query
			results, queryErr := database.QueryRoles(ctx, "", 0)
			require.NoError(t, queryErr, "Query error")
			defer func() {
				require.NoError(t, results.Close())
			}()
			var row map[string]interface{}
			rowCount := 0
			for results.Next(&row) {
				rowCount++
			}
			require.Equal(t, 4, rowCount)
		})
	}

}

func TestBuildRolesQuery(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	testCases := []struct {
		isServerless bool
	}{
		{
			isServerless: false,
		},
		{
			isServerless: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("BuildRolesQuery in Serverless=%t", testCase.isServerless), func(t *testing.T) {
			dbContextConfig := getDatabaseContextOptions(testCase.isServerless)

			database, ctx := db.SetupTestDBWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				require.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)

			// roles
			roleStatement, _ := database.BuildRolesQuery("", 0)
			plan, explainErr := n1QLStore.ExplainQuery(roleStatement, nil)
			require.NoError(t, explainErr, "Error generating explain for roleAccess query")

			covered := db.IsCovered(plan)
			planJSON, err := base.JSONMarshal(plan)
			require.NoError(t, err)
			require.Equal(t, testCase.isServerless, covered, "Roles query covered by index; expectedToBeCovered: %t, Plan: %s", testCase.isServerless, planJSON)
		})
	}
}

func TestBuildSessionsQuery(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	testCases := []struct {
		isServerless bool
	}{
		{
			isServerless: false,
		},
		{
			isServerless: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("BuildSessionsQuery in Serverless=%t", testCase.isServerless), func(t *testing.T) {
			dbContextConfig := getDatabaseContextOptions(testCase.isServerless)

			database, ctx := db.SetupTestDBWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				require.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)

			// Sessions
			roleStatement, _ := database.BuildSessionsQuery("user1")
			plan, explainErr := n1QLStore.ExplainQuery(roleStatement, nil)
			require.NoError(t, explainErr, "Error generating explain for roleAccess query")

			covered := db.IsCovered(plan)
			planJSON, err := base.JSONMarshal(plan)
			require.NoError(t, err)
			require.Equal(t, testCase.isServerless, covered, "Session query covered by index; expectedToBeCovered: %t, Plan: %s", testCase.isServerless, planJSON)
		})
	}
}

func TestBuildUsersQuery(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	testCases := []struct {
		isServerless bool
	}{
		{
			isServerless: false,
		},
		{
			isServerless: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("TestBuildUsersQuery in Serverless=%t", testCase.isServerless), func(t *testing.T) {
			dbContextConfig := getDatabaseContextOptions(testCase.isServerless)

			database, ctx := db.SetupTestDBWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				require.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)

			// Sessions
			roleStatement, _ := database.BuildUsersQuery("", 0)
			plan, explainErr := n1QLStore.ExplainQuery(roleStatement, nil)
			require.NoError(t, explainErr, "Error generating explain for roleAccess query")

			covered := db.IsCovered(plan)
			planJSON, err := base.JSONMarshal(plan)
			require.NoError(t, err)
			require.Equal(t, testCase.isServerless, covered, "Users query covered by index; expectedToBeCovered: %t, Plan: %s", testCase.isServerless, planJSON)
		})
	}
}

func TestQueryAllRoles(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	testCases := []struct {
		isServerless bool
	}{
		{
			isServerless: false,
		},
		{
			isServerless: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("TestQueryAllRoles in Serverless=%t", testCase.isServerless), func(t *testing.T) {
			dbContextConfig := getDatabaseContextOptions(testCase.isServerless)

			database, ctx := db.SetupTestDBWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				require.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)

			authenticator := database.Authenticator(ctx)
			require.NotNil(t, authenticator, "db.Authenticator(ctx) returned nil")

			// Add roles
			for i := 1; i <= 5; i++ {
				role, err := authenticator.NewRole(fmt.Sprintf("role%d", i), base.SetOf("ABC"))
				require.NoError(t, err, "Error creating new role")
				require.NoError(t, authenticator.Save(role))
			}

			// Delete 1 role
			role1, err := authenticator.NewRole("role1", base.SetOf("ABC"))
			require.NoError(t, err)
			err = authenticator.DeleteRole(role1, false, 0)
			require.NoError(t, err)

			// Standard query
			results, queryErr := database.QueryAllRoles(ctx, "", 0)
			require.NoError(t, queryErr, "Query error")
			defer func() {
				require.NoError(t, results.Close())
			}()

			var row map[string]interface{}
			rowCount := 0
			for results.Next(&row) {
				rowCount++
			}
			require.Equal(t, 5, rowCount)
		})
	}
}

func TestAllPrincipalIDs(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	testCases := []struct {
		isServerless bool
	}{
		{
			isServerless: false,
		},
		{
			isServerless: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("TestAllPrincipalIDs in Serverless=%t", testCase.isServerless), func(t *testing.T) {
			dbContextConfig := getDatabaseContextOptions(testCase.isServerless)

			db, ctx := db.SetupTestDBWithOptions(t, dbContextConfig)
			defer db.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(db.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				require.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

			db.Options.QueryPaginationLimit = 100
			db.ChannelMapper = channels.NewDefaultChannelMapper()
			authenticator := db.Authenticator(ctx)

			rolename1 := uuid.NewString()
			rolename2 := uuid.NewString()
			username := uuid.NewString()

			user1, err := authenticator.NewUser(username, "letmein", nil)
			require.NoError(t, err)
			user1.SetExplicitRoles(channels.TimedSet{rolename1: channels.NewVbSimpleSequence(1), rolename2: channels.NewVbSimpleSequence(1)}, 1)
			require.NoError(t, authenticator.Save(user1))

			role1, err := authenticator.NewRole(rolename1, nil)
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(role1))

			role2, err := authenticator.NewRole(rolename2, nil)
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(role2))

			err = db.DeleteRole(ctx, role2.Name(), false)
			require.NoError(t, err)
			roleGet, err := authenticator.GetRoleIncDeleted(role2.Name())
			require.NoError(t, err)
			require.True(t, roleGet.IsDeleted())

			t.Log("user1:", user1.Name())
			t.Log("role1:", role1.Name())
			t.Log("role2:", role2.Name())

			// require allprincipals still returns users and deleted roles
			users, roles, err := db.AllPrincipalIDs(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{user1.Name()}, users)
			require.ElementsMatch(t, []string{role1.Name(), role2.Name()}, roles)

			roles, err = db.GetRoleIDs(ctx, db.UseViews(), false)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{role1.Name()}, roles)
		})
	}
}

func TestGetRoleIDs(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	testCases := []struct {
		isServerless   bool
		includeDeleted bool
	}{
		{
			isServerless:   false,
			includeDeleted: false,
		},
		{
			isServerless:   false,
			includeDeleted: true,
		},
		{
			isServerless:   true,
			includeDeleted: false,
		},
		{
			isServerless:   true,
			includeDeleted: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("TestGetRoleIDs {Serverless=%t; includeDelete=%t}", testCase.isServerless, testCase.includeDeleted), func(t *testing.T) {
			dbContextConfig := getDatabaseContextOptions(testCase.isServerless)

			database, ctx := db.SetupTestDBWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				require.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

			database.Options.QueryPaginationLimit = 100
			database.ChannelMapper = channels.NewDefaultChannelMapper()
			authenticator := database.Authenticator(ctx)

			rolename1 := uuid.NewString()
			rolename2 := uuid.NewString()

			role1, err := authenticator.NewRole(rolename1, nil)
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(role1))

			role2, err := authenticator.NewRole(rolename2, nil)
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(role2))

			err = database.DeleteRole(ctx, role2.Name(), false)
			require.NoError(t, err)
			roleGet, err := authenticator.GetRoleIncDeleted(role2.Name())
			require.NoError(t, err)
			require.True(t, roleGet.IsDeleted())

			t.Log("role1:", role1.Name())
			t.Log("role2:", role2.Name())

			// require roles
			roles, err := database.GetRoleIDs(ctx, database.UseViews(), testCase.includeDeleted)
			expectedRoles := []string{role1.Name()}
			if testCase.includeDeleted {
				expectedRoles = append(expectedRoles, role2.Name())
			}

			require.NoError(t, err)
			require.ElementsMatch(t, expectedRoles, roles)
		})
	}
}

func getDatabaseContextOptions(isServerless bool) db.DatabaseContextOptions {
	defaultCacheOptions := db.DefaultCacheOptions()

	return db.DatabaseContextOptions{
		CacheOptions: &defaultCacheOptions,
		Serverless:   isServerless,
	}
}

type resetN1QLStoreFn func(n1QLStore base.N1QLStore, isServerless bool) error

func setupN1QLStore(bucket base.Bucket, isServerless bool) (base.N1QLStore, resetN1QLStoreFn, error) {
	n1QLStore, ok := base.AsN1QLStore(bucket)
	if !ok {
		return nil, nil, fmt.Errorf("Unable to get n1QLStore for testBucket")
	}

	if err := db.InitializeIndexes(n1QLStore, base.TestUseXattrs(), 0, false, isServerless); err != nil {
		return nil, nil, err
	}

	return n1QLStore, clearIndexes, nil
}

var clearIndexes resetN1QLStoreFn = func(n1QLStore base.N1QLStore, isServerless bool) error {
	indexes := db.GetIndexesName(isServerless, base.TestUseXattrs())
	for _, index := range indexes {
		err := n1QLStore.DropIndex(index)
		if err != nil && strings.Contains(err.Error(), "Index not exist") {
			return err
		}
	}
	return nil
}
