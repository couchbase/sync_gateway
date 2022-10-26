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
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
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

			db, ctx := setupTestDBForBucketWithOptions(t, dbContextConfig)
			defer db.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(db.Bucket, testCase.isServerless)
			assert.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				assert.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)

			authenticator := db.Authenticator(ctx)
			assert.NotNil(t, authenticator, "db.Authenticator(ctx) returned nil")

			// Add roles
			for i := 1; i <= 5; i++ {
				role, err := authenticator.NewRole(fmt.Sprintf("role%d", i), base.SetOf("ABC"))
				assert.NoError(t, err, "Error creating new role")
				assert.NoError(t, authenticator.Save(role))
			}

			// Delete 1 role
			role1, err := authenticator.NewRole("role1", base.SetOf("ABC"))
			assert.NoError(t, err)
			err = authenticator.DeleteRole(role1, false, 0)
			assert.NoError(t, err)

			// Standard query
			results, queryErr := db.QueryRoles(ctx, "", 0)
			assert.NoError(t, queryErr, "Query error")
			var row map[string]interface{}
			rowCount := 0
			for results.Next(&row) {
				rowCount++
			}
			assert.Equal(t, 4, rowCount)
			assert.NoError(t, results.Close())
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

			database, ctx := setupTestDBForBucketWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(database.Bucket, testCase.isServerless)
			assert.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				assert.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)

			// roles
			roleStatement, _ := database.BuildRolesQuery("", 0)
			plan, explainErr := n1QLStore.ExplainQuery(roleStatement, nil)
			assert.NoError(t, explainErr, "Error generating explain for roleAccess query")

			covered := db.IsCovered(plan)
			planJSON, err := base.JSONMarshal(plan)
			assert.NoError(t, err)
			assert.Equal(t, testCase.isServerless, covered, "Roles query covered by index; expectedToBeCovered: %t, Plan: %s", testCase.isServerless, planJSON)
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

			database, ctx := setupTestDBForBucketWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(database.Bucket, testCase.isServerless)
			assert.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				assert.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)

			// Sessions
			roleStatement, _ := database.BuildSessionsQuery("user1")
			plan, explainErr := n1QLStore.ExplainQuery(roleStatement, nil)
			assert.NoError(t, explainErr, "Error generating explain for roleAccess query")

			covered := db.IsCovered(plan)
			planJSON, err := base.JSONMarshal(plan)
			assert.NoError(t, err)
			assert.Equal(t, testCase.isServerless, covered, "Session query covered by index; expectedToBeCovered: %t, Plan: %s", testCase.isServerless, planJSON)
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

			database, ctx := setupTestDBForBucketWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(database.Bucket, testCase.isServerless)
			assert.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				assert.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)

			// Sessions
			roleStatement, _ := database.BuildUsersQuery("", 0)
			plan, explainErr := n1QLStore.ExplainQuery(roleStatement, nil)
			assert.NoError(t, explainErr, "Error generating explain for roleAccess query")

			covered := db.IsCovered(plan)
			planJSON, err := base.JSONMarshal(plan)
			assert.NoError(t, err)
			assert.Equal(t, testCase.isServerless, covered, "Users query covered by index; expectedToBeCovered: %t, Plan: %s", testCase.isServerless, planJSON)
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

			database, ctx := setupTestDBForBucketWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(database.Bucket, testCase.isServerless)
			assert.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				assert.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)

			authenticator := database.Authenticator(ctx)
			assert.NotNil(t, authenticator, "db.Authenticator(ctx) returned nil")

			// Add roles
			for i := 1; i <= 5; i++ {
				role, err := authenticator.NewRole(fmt.Sprintf("role%d", i), base.SetOf("ABC"))
				assert.NoError(t, err, "Error creating new role")
				assert.NoError(t, authenticator.Save(role))
			}

			// Delete 1 role
			role1, err := authenticator.NewRole("role1", base.SetOf("ABC"))
			assert.NoError(t, err)
			err = authenticator.DeleteRole(role1, false, 0)
			assert.NoError(t, err)

			// Standard query
			results, queryErr := database.QueryAllRoles(ctx, "", 0)
			assert.NoError(t, queryErr, "Query error")

			var row map[string]interface{}
			rowCount := 0
			for results.Next(&row) {
				rowCount++
			}
			assert.Equal(t, 5, rowCount)
			assert.NoError(t, results.Close())
		})
	}
}
