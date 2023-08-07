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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoleQuery(t *testing.T) {
	if base.TestsDisableGSI() {
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

			database, ctx := db.SetupTestDBWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			n1QLStores, reset, err := setupN1QLStore(ctx, database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func() {
				assert.NoError(t, reset(ctx, n1QLStores, testCase.isServerless))
			}()

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
	if base.TestsDisableGSI() {
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

			n1QLStores, reset, err := setupN1QLStore(ctx, database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func() {
				assert.NoError(t, reset(ctx, n1QLStores, testCase.isServerless))
			}()

			// roles
			n1QLStore, ok := base.AsN1QLStore(database.MetadataStore)
			require.True(t, ok)

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

func TestBuildUsersQuery(t *testing.T) {
	if base.TestsDisableGSI() {
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

			n1QLStores, reset, err := setupN1QLStore(ctx, database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func() {
				assert.NoError(t, reset(ctx, n1QLStores, testCase.isServerless))
			}()

			// Sessions
			n1QLStore, ok := base.AsN1QLStore(database.MetadataStore)
			require.True(t, ok)
			userStatement, _ := database.BuildUsersQuery("", 0)
			plan, explainErr := n1QLStore.ExplainQuery(userStatement, nil)
			require.NoError(t, explainErr)

			covered := db.IsCovered(plan)
			planJSON, err := base.JSONMarshal(plan)
			require.NoError(t, err)
			require.Equal(t, testCase.isServerless, covered, "Users query covered by index; expectedToBeCovered: %t, Plan: %s", testCase.isServerless, planJSON)
		})
	}
}

func TestQueryAllRoles(t *testing.T) {
	if base.TestsDisableGSI() {
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

			n1QLStores, reset, err := setupN1QLStore(ctx, database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func() {
				assert.NoError(t, reset(ctx, n1QLStores, testCase.isServerless))
			}()

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
	if base.TestsDisableGSI() {
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
			database, ctx := db.SetupTestDBWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			n1QLStores, reset, err := setupN1QLStore(ctx, database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func() {
				assert.NoError(t, reset(ctx, n1QLStores, testCase.isServerless))
			}()

			base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

			database.Options.QueryPaginationLimit = 100
			authenticator := database.Authenticator(ctx)

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

			err = database.DeleteRole(ctx, role2.Name(), false)
			require.NoError(t, err)
			roleGet, err := authenticator.GetRoleIncDeleted(role2.Name())
			require.NoError(t, err)
			require.True(t, roleGet.IsDeleted())

			t.Log("user1:", user1.Name())
			t.Log("role1:", role1.Name())
			t.Log("role2:", role2.Name())

			// require allprincipals still returns users and deleted roles
			users, roles, err := database.AllPrincipalIDs(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{user1.Name()}, users)
			require.ElementsMatch(t, []string{role1.Name(), role2.Name()}, roles)

			roles, err = database.GetRoleIDs(ctx, database.UseViews(), false)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{role1.Name()}, roles)
		})
	}
}

func TestGetRoleIDs(t *testing.T) {
	if base.TestsDisableGSI() {
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

			n1QLStores, reset, err := setupN1QLStore(ctx, database.Bucket, testCase.isServerless)
			require.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func() {
				assert.NoError(t, reset(ctx, n1QLStores, testCase.isServerless))
			}()

			base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

			database.Options.QueryPaginationLimit = 100
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

type resetN1QLStoreFn func(ctx context.Context, n1QLStores []base.N1QLStore, isServerless bool) error

func setupN1QLStore(ctx context.Context, bucket base.Bucket, isServerless bool) ([]base.N1QLStore, resetN1QLStoreFn, error) {

	dataStoreNames, err := bucket.ListDataStores()
	if err != nil {
		return nil, nil, err
	}

	outN1QLStores := make([]base.N1QLStore, 0)
	for _, dataStoreName := range dataStoreNames {
		ctx = base.CollectionLogCtx(ctx, dataStoreName.CollectionName())
		dataStore, err := bucket.NamedDataStore(dataStoreName)
		if err != nil {
			return nil, nil, err
		}
		n1QLStore, ok := base.AsN1QLStore(dataStore)
		if !ok {
			return nil, nil, fmt.Errorf("Unable to get n1QLStore for testBucket")
		}

		options := db.InitializeIndexOptions{
			FailFast:    false,
			NumReplicas: 0,
			Serverless:  isServerless,
			UseXattrs:   base.TestUseXattrs(),
		}
		if base.IsDefaultCollection(dataStoreName.ScopeName(), dataStoreName.CollectionName()) {
			options.MetadataIndexes = db.IndexesAll
		}
		if err := db.InitializeIndexes(ctx, n1QLStore, options); err != nil {
			return nil, nil, err
		}
		outN1QLStores = append(outN1QLStores, n1QLStore)
	}

	return outN1QLStores, clearIndexes, nil
}

// resetN1QLStores restores the set of indexes to the starting state
var clearIndexes resetN1QLStoreFn = func(ctx context.Context, n1QLStores []base.N1QLStore, isServerless bool) error {
	options := db.InitializeIndexOptions{
		UseXattrs:       base.TestUseXattrs(),
		NumReplicas:     0,
		FailFast:        false,
		Serverless:      isServerless,
		MetadataIndexes: db.IndexesAll,
	}

	indexes := db.GetIndexesName(options)
	var err error
	for _, n1QLStore := range n1QLStores {
		for _, index := range indexes {
			newErr := n1QLStore.DropIndex(ctx, index)
			if newErr != nil && strings.Contains(newErr.Error(), "Index not exist") {
				err = errors.Wrap(err, newErr.Error())
			}
		}
	}
	return err
}
