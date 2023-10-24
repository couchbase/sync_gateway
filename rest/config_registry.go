// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"context"
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
)

// GatewayRegistry lists all databases defined for the bucket, across config groups.  Used to fetch the full set of databases for a config group (for config polling),
// and to prevent assignment of a collection to multiple databases.  Note that the same database may exist in different config groups,
// and matching database names in different config groups are allowed to target the same collection.
// Storage format:
//
//		configGroups: {
//		   	default: {
//		   		databases: {
//		       		db1: {
//		           		version: 2-abc,
//		           		scopes: {
//		               		_default:
//								collections: [_default, c1, c2]
//							}
//						},
//	                 previous_version: {
//		           			version: 1-abc,
//		           			scopes: {
//		               			_default:
//									collections: [_default, c2]
//								}
//						},
//					}
//				}
//			}
//		}
type GatewayRegistry struct {
	cas          uint64
	Version      string                          `json:"version"`       // Registry version
	ConfigGroups map[string]*RegistryConfigGroup `json:"config_groups"` // Map of config groups, keyed by config group ID
	SGVersion    base.ComparableVersion          `json:"sg_version"`    // Latest patch version of Sync Gateway that touched the registry
}

const GatewayRegistryVersion = "1.0"

const deletedDatabaseVersion = "0-0"

// RegistryConfigGroup stores the set of databases for a given config group
type RegistryConfigGroup struct {
	Databases map[string]*RegistryDatabase `json:"databases"`
}

// RegistryDatabase stores the version and set of RegistryScopes for a database
type RegistryDatabase struct {
	RegistryDatabaseVersion        // current version
	MetadataID              string `json:"metadata_id"`    // Metadata ID
	UUID                    string `json:"uuid,omitempty"` // Database UUID
	// PreviousVersion stores the previous database version while an update is in progress, in case update of the config
	// fails and rollback is required.  Required to avoid cross-database collection conflicts during rollback.
	PreviousVersion *RegistryDatabaseVersion `json:"previous_version,omitempty"`
}

// DatabaseVersion stores the version and collection set for a database.  Used for storing current or previous version.
type RegistryDatabaseVersion struct {
	Version string         `json:"version,omitempty"` // Database Version
	Scopes  RegistryScopes `json:"scopes,omitempty"`  // Scopes and collections for this version
}

type RegistryScopes map[string]RegistryScope

// RegistryScope stores the list of collections for the scope as a slice
type RegistryScope struct {
	Collections []string `json:"collections,omitempty"`
}

var defaultOnlyRegistryScopes = map[string]RegistryScope{base.DefaultScope: {Collections: []string{base.DefaultCollection}}}
var DefaultOnlyScopesConfig = ScopesConfig{base.DefaultScope: {Collections: map[string]*CollectionConfig{base.DefaultCollection: {}}}}

func NewGatewayRegistry(syncGatewayVersion base.ComparableVersion) *GatewayRegistry {
	return &GatewayRegistry{
		ConfigGroups: make(map[string]*RegistryConfigGroup),
		Version:      GatewayRegistryVersion,
		SGVersion:    syncGatewayVersion,
	}
}

// getCollectionsByDatabase returns all in-use collections as a map from collection name to the associated dbname
func (r *GatewayRegistry) getCollectionsByDatabase(ctx context.Context) map[base.ScopeAndCollectionName]string {
	collectionsByDatabase := make(map[base.ScopeAndCollectionName]string, 0)
	for _, configGroup := range r.ConfigGroups {
		for dbName, database := range configGroup.Databases {
			for scopeName, scope := range database.Scopes {
				for _, collectionName := range scope.Collections {
					scName := base.ScopeAndCollectionName{Scope: scopeName, Collection: collectionName}
					// If duplicate found with different db name, log info
					if existingName, ok := collectionsByDatabase[scName]; ok && existingName != dbName {
						base.InfofCtx(ctx, base.KeyAll, "Collection %s associated with multiple databases in registry: [%s, %s]", scName, existingName, dbName)
					}
					collectionsByDatabase[scName] = dbName
				}
			}
		}
	}
	return collectionsByDatabase
}

// deleteDatabase deletes the current version of the db from the registry, updating previous version
func (r *GatewayRegistry) deleteDatabase(configGroupID, dbName string) error {
	configGroup, ok := r.ConfigGroups[configGroupID]
	if !ok {
		return base.ErrNotFound
	}
	database, ok := configGroup.Databases[dbName]
	if !ok {
		return base.ErrNotFound
	}
	database.PreviousVersion = &RegistryDatabaseVersion{
		Version: database.Version,
	}
	// Scopes are not copied to PreviousVersion, as previous collections for in-flight deletes are not considered conflicts, since
	// an interrupted delete is still processed as a delete (since we have enough information to identify intent).  Mark the
	// version to identify this state.
	database.Version = deletedDatabaseVersion
	database.Scopes = nil
	return nil
}

// removeDatabase removes the specified db from the registry entirely
func (r *GatewayRegistry) removeDatabase(configGroupID, dbName string) bool {
	configGroup, ok := r.ConfigGroups[configGroupID]
	if !ok {
		return false
	}
	_, ok = configGroup.Databases[dbName]
	if !ok {
		return false
	}
	delete(configGroup.Databases, dbName)
	if len(configGroup.Databases) == 0 {
		delete(r.ConfigGroups, configGroupID)
	}
	return true
}

// upsertDatabaseConfig upserts the registry entry for the specified db to match the config.  If there is
// an existing entry for the database, it will be replaced - otherwise a new entry for the database is added.
// Returns error if database config is in conflict with the active version of another database.
// If in-conflict with an in-flight update to other database(s), returns that set as previousVersionConflicts so that callers
// can wait and retry.
func (r *GatewayRegistry) upsertDatabaseConfig(ctx context.Context, configGroupID string, config *DatabaseConfig) (previousVersionConflicts []configGroupAndDatabase, err error) {

	if config == nil {
		return nil, fmt.Errorf("attempted to upsertDatabaseConfig with nil config")
	}
	collectionConflicts := r.getCollectionConflicts(ctx, config.Name, config.Scopes)
	if len(collectionConflicts) > 0 {
		return nil, base.HTTPErrorf(http.StatusConflict, "Cannot update config for database %s - collections are in use by another database: %v", base.UD(config.Name), collectionConflicts)
	}

	// This is just a defensive check - any potential races should have already collided on collections first
	if r.hasMetadataIDConflict(config.Name, config.MetadataID) {
		base.WarnfCtx(ctx, "Unexpected conflict on metadataID while upserting databaseConfig in registry for metadataID %s", base.UD(config.MetadataID))
		return nil, base.HTTPErrorf(http.StatusConflict, "Cannot update config for database %s - metadataID is in use by another database: %v", base.UD(config.Name), collectionConflicts)
	}

	// For conflicts with in-flight updates, call getRegistryAndDatabase to block until those updates complete or rollback
	previousVersionConflicts = r.getPreviousConflicts(ctx, config.Name, config.Scopes)
	if len(previousVersionConflicts) > 0 {
		return previousVersionConflicts, base.HTTPErrorf(http.StatusConflict, "Cannot update config, collections are in use by another database with an update in progress")
	}

	configGroup, ok := r.ConfigGroups[configGroupID]
	if !ok {
		configGroup = NewRegistryConfigGroup()
		r.ConfigGroups[configGroupID] = configGroup
	}

	newRegistryDatabase := registryDatabaseFromConfig(config)
	previousRegistryDatabase, ok := configGroup.Databases[config.Name]
	if ok {
		newRegistryDatabase.PreviousVersion = &RegistryDatabaseVersion{
			Version: previousRegistryDatabase.Version,
			Scopes:  previousRegistryDatabase.Scopes,
		}
	}
	configGroup.Databases[config.Name] = newRegistryDatabase
	return nil, nil
}

// rollbackDatabaseConfig reverts the registry entry to the previous version, and removes the previous version
func (r *GatewayRegistry) rollbackDatabaseConfig(ctx context.Context, configGroupID string, dbName string) (err error) {

	configGroup, ok := r.ConfigGroups[configGroupID]
	if !ok {
		return base.ErrNotFound
	}
	registryDatabase, ok := configGroup.Databases[dbName]
	if !ok {
		return base.ErrNotFound
	}

	if registryDatabase.PreviousVersion == nil {
		return fmt.Errorf("Rollback requested but registry did not include previous version for db %s", base.MD(dbName))
	}

	registryDatabase.Version = registryDatabase.PreviousVersion.Version
	registryDatabase.Scopes = registryDatabase.PreviousVersion.Scopes
	registryDatabase.PreviousVersion = nil
	return nil
}

func (r *GatewayRegistry) removePreviousVersion(configGroupID string, dbName string, version string) error {
	registryDatabase, ok := r.getRegistryDatabase(configGroupID, dbName)
	if !ok {
		return base.ErrNotFound
	}
	if registryDatabase.PreviousVersion == nil || registryDatabase.PreviousVersion.Version != version {
		return base.ErrConfigVersionMismatch
	}
	registryDatabase.PreviousVersion = nil
	return nil
}

func (r *GatewayRegistry) getRegistryDatabase(configGroupID, dbName string) (*RegistryDatabase, bool) {
	cg, ok := r.ConfigGroups[configGroupID]
	if !ok {
		return nil, false
	}
	db, found := cg.Databases[dbName]
	return db, found
}

// getDbForCollection returns the database associated with the specified scope and collection
func (r *GatewayRegistry) getDbForCollection(ctx context.Context, scopeName string, collectionName string) (string, bool) {
	collectionsByDatabase := r.getCollectionsByDatabase(ctx)
	dbName, ok := collectionsByDatabase[base.ScopeAndCollectionName{Scope: scopeName, Collection: collectionName}]
	return dbName, ok
}

// configGroupAndDatabase stores the [configGroup, databaseName] tuple.
type configGroupAndDatabase struct {
	configGroup  string
	databaseName string
}

// getCollectionConflicts returns a map of registry collections that are in conflict with the provided scopesConfig.  Map values
// are the dbName for the conflicting collection.  Matching collections for the same dbname is not a conflicts (even across config groups).
func (r *GatewayRegistry) getCollectionConflicts(ctx context.Context, dbName string, scopes ScopesConfig) (activeConflicts map[base.ScopeAndCollectionName]string) {

	if len(scopes) == 0 {
		return r.getCollectionConflicts(ctx, dbName, DefaultOnlyScopesConfig)
	}
	// activeConflicts is a map from conflicting collection names to db name
	activeConflicts = make(map[base.ScopeAndCollectionName]string, 0)

	for _, configGroup := range r.ConfigGroups {
		for registryDbName, database := range configGroup.Databases {
			if registryDbName != dbName {
				registryScopes := database.Scopes
				if len(registryScopes) == 0 {
					registryScopes = defaultOnlyRegistryScopes
				}
				for _, scName := range findCollectionConflicts(scopes, registryScopes) {
					activeConflicts[scName] = registryDbName
				}
			}
		}
	}
	return activeConflicts
}

// getPreviousConflicts returns the set of unique [configGroupAndDatabase] where the previousVersion for that database has a conflicting collection
// with the provided ScopesConfig.  previousVersion indicates an in-flight update of the registry for that database.
// Matching collections for the same dbname is not a conflicts (even across config groups).
func (r *GatewayRegistry) getPreviousConflicts(ctx context.Context, dbName string, scopes ScopesConfig) (previousConflicts []configGroupAndDatabase) {

	if len(scopes) == 0 {
		return r.getPreviousConflicts(ctx, dbName, DefaultOnlyScopesConfig)
	}
	conflictingDbs := make(map[configGroupAndDatabase]struct{}, 0)
	for cgName, configGroup := range r.ConfigGroups {
		for registryDbName, database := range configGroup.Databases {
			if registryDbName != dbName && database.PreviousVersion != nil {
				previousScopes := database.PreviousVersion.Scopes
				if len(previousScopes) == 0 {
					previousScopes = defaultOnlyRegistryScopes
				}
				previousConflicts := findCollectionConflicts(scopes, previousScopes)
				if len(previousConflicts) > 0 {
					conflictingDb := configGroupAndDatabase{cgName, registryDbName}
					conflictingDbs[conflictingDb] = struct{}{}
				}
			}
		}
	}

	previousConflicts = make([]configGroupAndDatabase, 0, len(conflictingDbs))
	for key, _ := range conflictingDbs {
		previousConflicts = append(previousConflicts, key)
	}
	return previousConflicts
}

// findCollectionConflicts is a utility method to find the set of conflicting collections between a database config (stored in ScopesConfig)
// and a database version in the registry (stored in a map of scopeName to RegistryScope).
func findCollectionConflicts(scopes ScopesConfig, registryScopes map[string]RegistryScope) []base.ScopeAndCollectionName {
	conflicts := make([]base.ScopeAndCollectionName, 0)
	for scopeName, scope := range scopes {
		registryScope, ok := registryScopes[scopeName]
		if ok {
			for collectionName, _ := range scope.Collections {
				for _, registryCollectionName := range registryScope.Collections {
					if collectionName == registryCollectionName {
						conflicts = append(conflicts, base.ScopeAndCollectionName{Scope: scopeName, Collection: collectionName})
					}
				}
			}
		}
	}
	return conflicts
}

// hasMetadataIDConflict checks whether the specified metadataID is already in use by a different db in the registry
func (r *GatewayRegistry) hasMetadataIDConflict(dbName string, metadataID string) bool {
	for _, configGroup := range r.ConfigGroups {
		for registryDbName, database := range configGroup.Databases {
			if registryDbName != dbName && database.MetadataID == metadataID {
				return true
			}
		}
	}
	return false
}

// registryDatabaseFromConfig creates a RegistryDatabase based on the specified config
func registryDatabaseFromConfig(config *DatabaseConfig) *RegistryDatabase {
	rdb := &RegistryDatabase{}
	rdb.Version = config.Version
	rdb.MetadataID = config.MetadataID
	if len(config.Scopes) == 0 {
		rdb.Scopes = defaultOnlyRegistryScopes
		return rdb
	}

	rdb.Scopes = make(map[string]RegistryScope)
	for scopeName, scope := range config.Scopes {
		registryScope := RegistryScope{
			Collections: make([]string, 0),
		}
		for collectionName, _ := range scope.Collections {
			registryScope.Collections = append(registryScope.Collections, collectionName)
		}
		rdb.Scopes[scopeName] = registryScope
	}
	return rdb
}

// NewRegistryConfigGroup initializes an empty RegistryConfigGroup
func NewRegistryConfigGroup() *RegistryConfigGroup {
	return &RegistryConfigGroup{
		Databases: make(map[string]*RegistryDatabase),
	}
}
