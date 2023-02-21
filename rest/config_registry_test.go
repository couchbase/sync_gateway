// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

// TestRegistryHelpers unmarshals a registry and performs upserts and removals
func TestRegistryHelpers(t *testing.T) {

	registryJSON :=
		`{
		"config_groups": {
			"default": {
				"databases": {
	       			"db1": {
						"version": "1-abc",
						"scopes": {
	               			"_default": {
								"collections": ["c1", "c2"]
							}
						}
					},
	       			"db2": {
						"version": "1-abc",
						"scopes": {
	               			"_default": {
								"collections": ["c3", "c4"]
							}
						}
					}
				}
			},
			"cg1": {
				"databases": {
	       			"db1": {
						"version": "1-abc",
						"scopes": {
	               			"_default": {
								"collections": ["c2"]
							}
						}
					},
					"db3": {
						"version": "1-def",
						"scopes": {
	               			"s1": {
								"collections": ["c1", "c2"]
							}
						}
					}
				}
			}
		}
	}`
	ctx := base.TestCtx(t)

	var registry *GatewayRegistry
	err := base.JSONUnmarshal([]byte(registryJSON), &registry)
	require.NoError(t, err)
	configGroup, ok := registry.ConfigGroups["default"]
	require.True(t, ok)
	registryDb, ok := configGroup.Databases["db1"]
	require.True(t, ok)
	assert.Equal(t, "1-abc", registryDb.Version)

	// Request db for collection that only exists in one config group
	dbName, ok := registry.getDbForCollection(ctx, "_default", "c1")
	require.True(t, ok)
	assert.Equal(t, "db1", dbName)

	// Request db for collection that exists in multiple config groups (same db)
	dbName, ok = registry.getDbForCollection(ctx, "_default", "c2")
	require.True(t, ok)
	assert.Equal(t, "db1", dbName)

	// Request db for unused collection
	_, ok = registry.getDbForCollection(ctx, "_default", "c5")
	require.False(t, ok)

	dbName, ok = registry.getDbForCollection(ctx, "s1", "c1")
	require.True(t, ok)
	assert.Equal(t, "db3", dbName)

	require.True(t, registry.removeDatabase("cg1", "db3"))
	require.False(t, registry.removeDatabase("cg1", "db3"))

	// verify removal from collection map
	_, ok = registry.getDbForCollection(ctx, "s1", "c1")
	assert.False(t, ok)

	dbConfig := &DatabaseConfig{
		Version: "1-ghi",
		DbConfig: DbConfig{
			Name: "defaultDb",
		},
	}
	registry.upsertDatabaseConfig(ctx, "cg1", dbConfig)
	addedDefaultDb, ok := registry.getDbForCollection(ctx, "_default", "_default")
	require.True(t, ok)
	assert.Equal(t, "defaultDb", addedDefaultDb)
}

// TestUpsertDatabaseConfig tests registry.upsertDatabaseConfig
func TestUpsertDatabaseConfig(t *testing.T) {

	ctx := base.TestCtx(t)
	registry := NewGatewayRegistry()
	dbConfig := makeDatabaseConfig("db1", "scope1", []string{"c1", "c2", "c3"}) // in cg1
	dbConfig.Version = "1"
	_, err := registry.upsertDatabaseConfig(ctx, "cg1", dbConfig)
	require.NoError(t, err)
	registryDatabase, ok := registry.ConfigGroups["cg1"].Databases["db1"]
	require.True(t, ok)
	require.Equal(t, "1", registryDatabase.Version)
	require.Nil(t, registryDatabase.PreviousVersion)
	registryScope, ok := registryDatabase.Scopes["scope1"]
	require.True(t, ok)
	registryCollections := registryScope.Collections
	require.True(t, base.StringSliceContains(registryCollections, "c1"))
	require.True(t, base.StringSliceContains(registryCollections, "c2"))
	require.True(t, base.StringSliceContains(registryCollections, "c3"))

	dbConfigUpdate := makeDatabaseConfig("db1", "scope1", []string{"c3", "c4", "c5"})
	dbConfigUpdate.Version = "2"
	_, err = registry.upsertDatabaseConfig(ctx, "cg1", dbConfigUpdate)
	require.NoError(t, err)
	registryDatabase, ok = registry.ConfigGroups["cg1"].Databases["db1"]
	require.True(t, ok)
	require.Equal(t, "2", registryDatabase.Version)
	registryScope, ok = registryDatabase.Scopes["scope1"]
	require.True(t, ok)
	registryCollections = registryScope.Collections
	require.True(t, base.StringSliceContains(registryCollections, "c3"))
	require.True(t, base.StringSliceContains(registryCollections, "c4"))
	require.True(t, base.StringSliceContains(registryCollections, "c5"))

	require.NotNil(t, registryDatabase.PreviousVersion)
	require.Equal(t, "1", registryDatabase.PreviousVersion.Version)
	previousScope, ok := registryDatabase.PreviousVersion.Scopes["scope1"]
	require.True(t, ok)
	previousCollections := previousScope.Collections
	require.True(t, base.StringSliceContains(previousCollections, "c1"))
	require.True(t, base.StringSliceContains(previousCollections, "c2"))
	require.True(t, base.StringSliceContains(previousCollections, "c3"))

	// Test previous version removal
	err = registry.removePreviousVersion("cg2", "db1", "1") // config group mismatch
	require.Equal(t, base.ErrNotFound, err)
	err = registry.removePreviousVersion("cg1", "db2", "1") // db name mismatch
	require.Equal(t, base.ErrNotFound, err)
	err = registry.removePreviousVersion("cg1", "db1", "2") // config version mismatch
	require.Equal(t, base.ErrConfigVersionMismatch, err)
	err = registry.removePreviousVersion("cg1", "db1", "1") // config version mismatch
	require.NoError(t, err)
	registryDatabase, ok = registry.ConfigGroups["cg1"].Databases["db1"]
	require.True(t, ok)
	require.Nil(t, registryDatabase.PreviousVersion)

}

// TestRegistryConflicts tests registry.findCollectionConflicts
func TestRegistryConflicts(t *testing.T) {

	// Initialize registry with databases
	dbConfig1 := makeDatabaseConfig("db1", "scope1", []string{"c1", "c2", "c3"}) // in cg1
	dbConfig2 := makeDatabaseConfig("db1", "scope1", []string{"c1", "c3"})       // in cg2
	dbConfig3 := makeDatabaseConfig("db2", "scope1", []string{"c4", "c5", "c6"}) // in cg1
	dbConfig4 := makeDatabaseConfig("db3", "scope2", []string{"c1", "c2", "c3"}) // in cg1
	defaultConfig := &DatabaseConfig{DbConfig: DbConfig{Name: "defaultDb"}}

	ctx := base.TestCtx(t)
	registry := NewGatewayRegistry()
	_, err := registry.upsertDatabaseConfig(ctx, "cg1", dbConfig1)
	require.NoError(t, err)
	_, err = registry.upsertDatabaseConfig(ctx, "cg2", dbConfig2)
	require.NoError(t, err)
	_, err = registry.upsertDatabaseConfig(ctx, "cg1", dbConfig3)
	require.NoError(t, err)
	_, err = registry.upsertDatabaseConfig(ctx, "cg1", dbConfig4)
	require.NoError(t, err)
	_, err = registry.upsertDatabaseConfig(ctx, "cg1", defaultConfig)
	require.NoError(t, err)
	testCases := []struct {
		name              string
		dbConfig          *DatabaseConfig
		expectedConflicts []base.ScopeAndCollectionName
	}{
		{
			name:              "Different scope, different collections",
			dbConfig:          makeDatabaseConfig("newDb", "scope3", []string{"c5", "c6"}),
			expectedConflicts: []base.ScopeAndCollectionName{},
		},
		{
			name:              "Same scope, different collections",
			dbConfig:          makeDatabaseConfig("newDb", "scope1", []string{"c7", "c8"}),
			expectedConflicts: []base.ScopeAndCollectionName{},
		},
		{
			name:              "Different scope, Same collections",
			dbConfig:          makeDatabaseConfig("newDb", "scope3", []string{"c1", "c2"}),
			expectedConflicts: []base.ScopeAndCollectionName{},
		},
		{
			name:              "Same scope, same collections, same dbName",
			dbConfig:          makeDatabaseConfig("db1", "scope1", []string{"c2", "c3"}),
			expectedConflicts: []base.ScopeAndCollectionName{},
		},
		{
			name:              "Same scope, one conflicting collection",
			dbConfig:          makeDatabaseConfig("newDb", "scope1", []string{"c1", "c7"}),
			expectedConflicts: []base.ScopeAndCollectionName{{"scope1", "c1"}},
		},
		{
			name:              "Same scope, two conflicting collections",
			dbConfig:          makeDatabaseConfig("newDb", "scope1", []string{"c7", "c1", "c2"}),
			expectedConflicts: []base.ScopeAndCollectionName{{"scope1", "c1"}, {"scope1", "c2"}},
		},
		{
			name:              "Same scope, two conflicting collections",
			dbConfig:          makeDatabaseConfig("newDb", "scope1", []string{"c7", "c1", "c2"}),
			expectedConflicts: []base.ScopeAndCollectionName{{"scope1", "c1"}, {"scope1", "c2"}},
		},
		{
			name:              "Non-conflict with default collection, same dbName",
			dbConfig:          &DatabaseConfig{DbConfig: DbConfig{Name: "defaultDb"}},
			expectedConflicts: []base.ScopeAndCollectionName{},
		},
		{
			name:              "Non-conflict with default scope, different collection",
			dbConfig:          makeDatabaseConfig("newDb", base.DefaultScope, []string{"c1"}),
			expectedConflicts: []base.ScopeAndCollectionName{},
		},
		{
			name:              "Conflict with legacy default collection",
			dbConfig:          &DatabaseConfig{DbConfig: DbConfig{Name: "newDb"}},
			expectedConflicts: []base.ScopeAndCollectionName{{base.DefaultScope, base.DefaultCollection}},
		},
		{
			name:              "Conflict with explicit default collection",
			dbConfig:          makeDatabaseConfig("newDb", base.DefaultScope, []string{base.DefaultCollection}),
			expectedConflicts: []base.ScopeAndCollectionName{{base.DefaultScope, base.DefaultCollection}},
		},
		{
			name:              "Conflict with explicit default collection among others",
			dbConfig:          makeDatabaseConfig("newDb", base.DefaultScope, []string{base.DefaultCollection, "c1"}),
			expectedConflicts: []base.ScopeAndCollectionName{{base.DefaultScope, base.DefaultCollection}},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			conflicts := registry.getCollectionConflicts(ctx, testCase.dbConfig.Name, testCase.dbConfig.Scopes)
			assert.Equal(t, len(testCase.expectedConflicts), len(conflicts))
			// no ordering guarantee on returned conflicts, need to compare slices
			for _, expectedConflict := range testCase.expectedConflicts {
				found := false
				for conflict, _ := range conflicts {
					if conflict == expectedConflict {
						found = true
					}
				}
				assert.True(t, found, fmt.Sprintf("Not found: %v", expectedConflict))
			}
		})
	}

}

func makeDatabaseConfig(dbName string, scopeName string, collectionNames []string) *DatabaseConfig {

	var scopesConfig ScopesConfig
	scopesConfig = ScopesConfig{
		scopeName: ScopeConfig{
			map[string]CollectionConfig{},
		},
	}
	for _, collectionName := range collectionNames {
		scopesConfig[scopeName].Collections[collectionName] = CollectionConfig{}
	}

	return &DatabaseConfig{
		DbConfig: DbConfig{
			Name:   dbName,
			Scopes: scopesConfig,
		},
	}
}
