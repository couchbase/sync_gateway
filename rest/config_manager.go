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

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// ConfigManager should be used for any read/write of persisted database configuration files
type ConfigManager interface {
	// GetConfig fetches a database config for a given bucket and config group ID, along with the CAS of the config document. Does not enforce version match with registry.
	GetConfig(ctx context.Context, bucket, groupID, dbName string, config *DatabaseConfig) (cas uint64, err error)
	// GetDatabaseConfigs returns all configs for the bucket and config group.  Enforces version match with registry.
	GetDatabaseConfigs(ctx context.Context, bucketName, groupID string) ([]*DatabaseConfig, error)
	// InsertConfig saves a new database config for a given bucket and config group ID.
	InsertConfig(ctx context.Context, bucket, groupID string, config *DatabaseConfig) (newCAS uint64, err error)
	// UpdateConfig updates an existing database config for a given bucket and config group ID. updateCallback can return nil to remove the config.
	UpdateConfig(ctx context.Context, bucket, groupID, dbName string, updateCallback func(bucketConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error)) (newCAS uint64, err error)
	// DeleteConfig removes a database config for a given bucket, config group ID and database name.
	DeleteConfig(ctx context.Context, bucket, dbName, groupID string) (err error)

	// CheckMinorDowngrade returns an error the sgVersion represents at least minor version downgrade from the version in the bucket.
	CheckMinorDowngrade(ctx context.Context, bucketName string, sgVersion base.ComparableBuildVersion) error

	// SetSGVersion updates the Sync Gateway version in the bucket registry
	SetSGVersion(ctx context.Context, bucketName string, sgVersion base.ComparableBuildVersion) error
}

type dbConfigNameOnly struct {
	Name string `json:"name"`
}

var _ ConfigManager = &bootstrapContext{}

const configUpdateMaxRetryAttempts = 5 // Maximum number of retries due to conflicting updates or rollback
const configFetchMaxRetryAttempts = 5  // Maximum number of retries due to registry rollback

const defaultMetadataID = "_default"

// GetConfig fetches a database name for a given bucket and config group ID.
func (b *bootstrapContext) GetConfigName(ctx context.Context, bucketName, groupID, dbName string, configName *dbConfigNameOnly) (cas uint64, err error) {
	return b.Connection.GetMetadataDocument(ctx, bucketName, PersistentConfigKey(ctx, groupID, dbName), configName)
}

// GetConfig fetches a database config for a given bucket and config group ID, along with the CAS of the config document.
// GetConfig does *not* validate that config version matches registry version - operations requiring synchronization
// with registry should use getRegistryAndDatabase, or getConfig with the required version
func (b *bootstrapContext) GetConfig(ctx context.Context, bucketName, groupID, dbName string, config *DatabaseConfig) (cas uint64, err error) {
	return b.Connection.GetMetadataDocument(ctx, bucketName, PersistentConfigKey(ctx, groupID, dbName), config)
}

// InsertConfig saves a new database config for a given bucket and config group ID. This is a three-step process:
//  1. getRegistryAndDatabase to enforce synchronization pre-update
//  2. Update the registry to add the config
//  3. Write the config
func (b *bootstrapContext) InsertConfig(ctx context.Context, bucketName, groupID string, config *DatabaseConfig) (newCAS uint64, err error) {
	dbName := config.Name
	attempts := 0
	ctx = b.addDatabaseLogContext(ctx, &config.DbConfig)
	for attempts < configUpdateMaxRetryAttempts {
		attempts++
		base.InfofCtx(ctx, base.KeyConfig, "InsertConfig into bucket %s starting (attempt %d/%d)", bucketName, attempts, configUpdateMaxRetryAttempts)

		// Step 1. Fetch registry and databases - enforces registry/config synchronization
		registry, existingConfig, err := b.getRegistryAndDatabase(ctx, bucketName, groupID, dbName)
		if err != nil {
			base.InfofCtx(ctx, base.KeyConfig, "InsertConfig unable to retrieve registry and database: %v", err)
			return 0, err
		}
		if existingConfig != nil {
			return 0, base.ErrAlreadyExists
		}

		// If metadataID is not set on the config, compute
		if config.MetadataID == "" {
			config.MetadataID = b.computeMetadataID(ctx, registry, &config.DbConfig)
		}

		// Step 2. Update the registry to add the config
		// Add database to registry
		previousVersionConflicts, upsertErr := registry.upsertDatabaseConfig(ctx, groupID, config)
		if upsertErr != nil {
			base.InfofCtx(ctx, base.KeyConfig, "InsertConfig unable to update registry: %v", upsertErr)
			return 0, upsertErr
		}

		// If there are conflicts with previous versions of in-progress database updates, wait for those to complete
		if len(previousVersionConflicts) > 0 {
			err := b.WaitForConflictingUpdates(ctx, bucketName, previousVersionConflicts)
			if err != nil {
				return 0, err
			}
			continue
		}

		// Persist registry
		writeErr := b.setGatewayRegistry(ctx, bucketName, registry)
		if writeErr == nil {
			break
		}
		// retry on cas mismatch, otherwise return error
		if !base.IsCasMismatch(writeErr) {
			base.InfofCtx(ctx, base.KeyConfig, "InsertConfig unable to persist registry: %v", writeErr)
			return 0, writeErr
		}
		base.DebugfCtx(ctx, base.KeyConfig, "Registry updated successfully")

		// Check for context cancel before retrying
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("Exiting InsertConfig - context cancelled")
		default:
		}
	}
	// Step 3. Write the database config
	cas, configErr := b.Connection.InsertMetadataDocument(ctx, bucketName, PersistentConfigKey(ctx, groupID, dbName), config)
	if configErr != nil {
		base.InfofCtx(ctx, base.KeyConfig, "Insert for database config returned error %v", configErr)
	} else {
		base.DebugfCtx(ctx, base.KeyConfig, "Insert for database config was successful")
	}
	return cas, configErr
}

// UpdateConfig updates an existing database config for a given bucket and config group ID. This is a four-step process:
//  1. getRegistryAndDatabase to enforce synchronization pre-update
//  2. Update the registry to update the database definition including previous version (to support recovery in case step 2 fails)
//  3. Update the config
//  4. Update the registry to remove the previous version
func (b *bootstrapContext) UpdateConfig(ctx context.Context, bucketName, groupID, dbName string, updateCallback func(bucketConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error)) (newCAS uint64, err error) {
	var updatedConfig *DatabaseConfig
	var registry *GatewayRegistry
	var previousVersion string
	attempts := 0

outer:
	for attempts < configUpdateMaxRetryAttempts {
		attempts++
		base.InfofCtx(ctx, base.KeyConfig, "UpdateConfig starting (attempt %d/%d)", attempts, configUpdateMaxRetryAttempts)
		// Step 1. Fetch registry and databases - enforces registry/config synchronization
		var existingConfig *DatabaseConfig
		registry, existingConfig, err = b.getRegistryAndDatabase(ctx, bucketName, groupID, dbName)
		if existingConfig != nil {
			ctx = b.addDatabaseLogContext(ctx, &existingConfig.DbConfig)
		}
		if err != nil {
			base.InfofCtx(ctx, base.KeyConfig, "UpdateConfig unable to retrieve registry and database: %v", err)
			return 0, err
		}
		if existingConfig == nil {
			return 0, base.ErrNotFound
		}

		base.DebugfCtx(ctx, base.KeyConfig, "UpdateConfig fetched registry and database successfully")
		// Step 2. Update registry to update registry entry, and move previous registry entry to PreviousVersion
		previousVersion = existingConfig.Version
		var callbackErr error
		updatedConfig, callbackErr = updateCallback(existingConfig)
		if callbackErr != nil {
			return 0, callbackErr
		}

		// Update database in registry
		previousVersionConflicts, err := registry.upsertDatabaseConfig(ctx, groupID, updatedConfig)
		if err != nil {
			base.InfofCtx(ctx, base.KeyConfig, "UpdateConfig encountered error while upserting database config for conflicting updates: %v", err)
			return 0, err
		}
		// If there are conflicts with previous versions of in-progress database updates, wait for those to complete
		if len(previousVersionConflicts) > 0 {
			err := b.WaitForConflictingUpdates(ctx, bucketName, previousVersionConflicts)
			if err != nil {
				base.InfofCtx(ctx, base.KeyConfig, "UpdateConfig encountered error while waiting for conflicting updates: %v", err)
				return 0, err
			}
			continue
		}

		// Persist registry
		writeErr := b.setGatewayRegistry(ctx, bucketName, registry)
		if writeErr == nil {
			break
		}
		// retry on cas mismatch, otherwise return error
		if !base.IsCasMismatch(writeErr) {
			base.InfofCtx(ctx, base.KeyConfig, "UpdateConfig encountered error while persisting updated registry: %v", writeErr)
			return 0, writeErr
		}
		base.DebugfCtx(ctx, base.KeyConfig, "UpdateConfig persisted updated registry successfully")

		// Check for context cancel before retrying
		select {
		case <-ctx.Done():
			break outer
		default:
		}
	}

	// Step 2. Update the config document
	docID := PersistentConfigKey(ctx, groupID, dbName)
	fmt.Println("updatedConfig.cfgCas=", updatedConfig.cfgCas)
	casOut, err := b.Connection.WriteMetadataDocument(ctx, bucketName, docID, updatedConfig.cfgCas, updatedConfig)
	if err != nil {
		base.InfofCtx(ctx, base.KeyConfig, "Write for database config %q returned error %v", base.MD(docID), err)
		return 0, base.RedactErrorf("Error writing %q: %w", base.MD(docID), err)
	}
	base.DebugfCtx(ctx, base.KeyConfig, "Write for database config was successful")

	// Step 3. After config is successfully updated, finalize the update by removing the previous version from the registry
	err = registry.removePreviousVersion(groupID, dbName, previousVersion)
	if err != nil {
		return 0, fmt.Errorf("Error removing previous version of config group: %s, database: %s from registry after successful update: %w", base.MD(groupID), base.MD(dbName), err)
	}
	writeErr := b.setGatewayRegistry(ctx, bucketName, registry)
	if writeErr != nil {
		return 0, fmt.Errorf("Error persisting removal of previous version of config group: %s, database: %s from registry after successful update: %w", base.MD(groupID), base.MD(dbName), writeErr)
	}

	return casOut, nil
}

// DeleteConfig deletes a database config
//  1. getRegistryAndDatabase to enforce synchronization pre-update
//  2. Update the registry to delete the database definition and store the previous version (to support recovery in case step 2 fails)
//  3. Delete the config document
//  4. Update the registry to remove the database definition altogether
func (b *bootstrapContext) DeleteConfig(ctx context.Context, bucketName, groupID, dbName string) (err error) {
	var existingCas uint64
	var registry *GatewayRegistry
	attempts := 0
outer:
	for attempts < configUpdateMaxRetryAttempts {
		attempts++
		base.InfofCtx(ctx, base.KeyConfig, "DeleteConfig starting (attempt %d/%d)", attempts, configUpdateMaxRetryAttempts)
		var existingConfig *DatabaseConfig
		// Step 1. Fetch registry and databases - enforces registry/config synchronization
		registry, existingConfig, err = b.getRegistryAndDatabase(ctx, bucketName, groupID, dbName)
		if err != nil {
			base.InfofCtx(ctx, base.KeyConfig, "DeleteConfig unable to retrieve registry and database: %v", err)
			return err
		}
		if existingConfig == nil {
			return base.ErrNotFound
		}
		existingCas = existingConfig.cfgCas

		// Step 2. Update registry, mark database deleted in registry
		err = registry.deleteDatabase(groupID, dbName)
		if err != nil {
			base.InfofCtx(ctx, base.KeyConfig, "DeleteConfig unable to delete database from registry: %v", err)
			return err
		}

		// Persist registry
		writeErr := b.setGatewayRegistry(ctx, bucketName, registry)
		if writeErr == nil {
			break
		}
		// retry on cas mismatch, otherwise return error
		if !base.IsCasMismatch(writeErr) {
			base.InfofCtx(ctx, base.KeyConfig, "DeleteConfig failed to write updated registry: %v", writeErr)
			return writeErr
		}

		// Check for context cancel before retrying
		select {
		case <-ctx.Done():
			break outer
		default:
		}
	}

	err = b.Connection.DeleteMetadataDocument(ctx, bucketName, PersistentConfigKey(ctx, groupID, dbName), existingCas)
	if err != nil {
		base.InfofCtx(ctx, base.KeyConfig, "Delete for database config returned error %v", err)
		return err
	}
	base.DebugfCtx(ctx, base.KeyConfig, "Delete for database config was successful")

	// Step 3. After config is successfully deleted, finalize the delete by removing the previous version from the registry
	found := registry.removeDatabase(groupID, dbName)
	if !found {
		base.InfofCtx(ctx, base.KeyConfig, "Database not found in registry during finalization")
	} else {
		writeErr := b.setGatewayRegistry(ctx, bucketName, registry)
		if writeErr != nil {
			return fmt.Errorf("Error persisting removal of previous version of config group: %s, database: %s from registry after successful delete: %w", base.MD(groupID), base.MD(dbName), writeErr)
		}
	}

	return nil

}

// WaitForConflictingUpdates is called when an upsert is in conflict with previous versions found in the registry.  Previous
// versions indicate an in-progress update - WaitForConflictingUpdates uses getRegistryAndDatabase to wait for these
// updates to either successfully complete or be rolled back.
func (b *bootstrapContext) WaitForConflictingUpdates(ctx context.Context, bucketName string, databases []configGroupAndDatabase) error {
	for _, db := range databases {
		_, _, err := b.getRegistryAndDatabase(ctx, bucketName, db.configGroup, db.databaseName)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("Exiting WaitForConflictingUpdates - context cancelled")
		default:
		}
	}
	return nil
}

// GetDatabaseConfigs returns all configs for the bucket and config group.
func (b *bootstrapContext) GetDatabaseConfigs(ctx context.Context, bucketName, groupID string) ([]*DatabaseConfig, error) {

	attempts := 0
	for attempts < configFetchMaxRetryAttempts {
		attempts++

		registry, err := b.getGatewayRegistry(ctx, bucketName)
		if err != nil {
			return nil, err
		}

		// Check for legacy config file
		var legacyConfig DatabaseConfig
		var legacyDbName string
		cas, legacyErr := b.Connection.GetMetadataDocument(ctx, bucketName, PersistentConfigKey(ctx, groupID, ""), &legacyConfig)
		if legacyErr != nil && !base.IsDocNotFoundError(legacyErr) {
			return nil, fmt.Errorf("Error checking for legacy config for %s, %s: %w", base.MD(bucketName), base.MD(groupID), legacyErr)
		}
		if legacyErr == nil {
			legacyConfig.cfgCas = cas
			legacyDbName = legacyConfig.Name
		}

		configGroup, ok := registry.ConfigGroups[groupID]
		if !ok {
			// no configs defined for this config group
			return nil, nil
		}

		dbConfigs := make([]*DatabaseConfig, 0)
		legacyFoundInRegistry := false
		reloadRequired := false
		for dbName, registryDb := range configGroup.Databases {
			// Ignore databases with deleted version - represents an in-progress delete
			if registryDb.Version == deletedDatabaseVersion {
				continue
			}
			dbConfig, err := b.getDatabaseConfig(ctx, bucketName, groupID, dbName, registryDb.Version, registry)
			if err == base.ErrConfigRegistryReloadRequired {
				reloadRequired = true
				break
			} else if err != nil {
				return nil, err
			}
			dbConfigs = append(dbConfigs, dbConfig)
			if dbConfig.Name == legacyDbName {
				legacyFoundInRegistry = true
			}
		}

		// If we don't need to reload, append any legacy config found and return
		if !reloadRequired {
			if legacyDbName != "" && !legacyFoundInRegistry {
				dbConfigs = append(dbConfigs, &legacyConfig)
			}
			return dbConfigs, nil
		}
	}
	base.WarnfCtx(ctx, "Unable to successfully retrieve GetDatabaseConfigs for groupID: %s after %d attempts", base.MD(groupID), attempts)
	return nil, base.ErrConfigRegistryReloadRequired
}

// getConfigVersionWithRetry attempts to retrieve the specified config file.
// On file not found, will perform backoff retry up to specified timeout.  On timeout, returns the nil and rollback error.
// On mismatched version when registry version is newer, will perform backoff retry up to timeout.  On timeout, returns the config and rollback error.
// On mismatched version when config version is newer, returns the config and ErrConfigVersionMismatch.
func (b *bootstrapContext) getConfigVersionWithRetry(ctx context.Context, bucketName, groupID, dbName, version string) (*DatabaseConfig, error) {

	timeout := defaultConfigRetryTimeout
	if b.configRetryTimeout != 0 {
		timeout = b.configRetryTimeout
	}

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {
		config := &DatabaseConfig{}
		metadataKey := PersistentConfigKey(ctx, groupID, dbName)
		cas, err := b.Connection.GetMetadataDocument(ctx, bucketName, metadataKey, config)
		if base.IsDocNotFoundError(err) {
			return true, base.ErrConfigRegistryRollback, nil
		}
		if err != nil {
			return false, err, nil
		}

		config.cfgCas = cas
		// If version matches, success!
		if config.Version == version {
			return false, nil, config
		}

		// For version mismatch, handling depends on whether config has newer or older version than requested
		requestedGen, _ := db.ParseRevID(ctx, version)
		currentGen, _ := db.ParseRevID(ctx, config.Version)
		if currentGen > requestedGen {
			// If the config has a newer version than requested, return the config but alert caller that they have
			// requested a stale version.
			return false, base.ErrConfigVersionMismatch, config
		} else {
			base.InfofCtx(ctx, base.KeyConfig, "getConfigVersionWithRetry for key %s found version mismatch, retrying.  Requested: %s, Found: %s", metadataKey, version, config.Version)
			return true, base.ErrConfigRegistryRollback, config
		}
	}

	// Kick off the retry loop
	err, retryResult := base.RetryLoop(
		ctx,
		"Wait for config version match",
		retryWorker,
		base.CreateDoublingSleeperDurationFunc(50, timeout),
	)

	// Return the config with rollback error, to support rollback by the caller if appropriate
	if err != nil && err != base.ErrConfigRegistryRollback {
		return nil, err
	}

	if retryResult != nil {
		config, ok := retryResult.(*DatabaseConfig)
		if !ok {
			return nil, fmt.Errorf("Unable to convert returned config of type %T to *DatabaseConfig", retryResult)
		}
		return config, err
	}

	return nil, err
}

// getDatabaseConfig retrieves the database config, and enforces version match.  On config not found or mismatched
// version, will retry with backoff (to wait for in-flight updates to complete).  After retry timeout,
// triggers registry rollback and returns rollback error
func (b *bootstrapContext) getDatabaseConfig(ctx context.Context, bucketName, groupID, dbName string, version string, registry *GatewayRegistry) (*DatabaseConfig, error) {

	ctx = b.addDatabaseLogContext(ctx, &DbConfig{Name: dbName})
	config, err := b.getConfigVersionWithRetry(ctx, bucketName, groupID, dbName, version)
	if err != nil {
		if err == base.ErrConfigRegistryRollback {
			base.InfofCtx(ctx, base.KeyConfig, "Registry rollback required for bucket: %s, groupID: %s, dbName:%s", bucketName, groupID, dbName)
			rollbackErr := b.rollbackRegistry(ctx, bucketName, groupID, dbName, config, registry)
			// On successful registry rollback, caller needs reload registry
			if rollbackErr == nil {
				return nil, base.ErrConfigRegistryReloadRequired
			}
			// On unsuccessful rollback due to CAS failure, caller also needs to reload registry
			if base.IsCasMismatch(rollbackErr) {
				base.InfofCtx(ctx, base.KeyConfig, "Unsuccessful rollback for db:%s - CAS mismatch", dbName)
				return nil, base.ErrConfigRegistryReloadRequired
			}
			return nil, rollbackErr
		}
		return nil, err
	}
	return config, nil
}

// waitForConfigDelete waits for a config file to be deleted.  After retry timeout, returns latest cas
// and version, will backoff retry (to wait for in-flight updates to complete).  After retry timeout,
// triggers registry cleanup and returns returns rollback error.
// If version is not empty, waitForConfigDelete will attempt to remove the previousVersion from the registry
// on rollback.
func (b *bootstrapContext) waitForConfigDelete(ctx context.Context, bucketName, groupID, dbName string, version string, registry *GatewayRegistry) error {
	timeout := defaultConfigRetryTimeout
	if b.configRetryTimeout != 0 {
		timeout = b.configRetryTimeout
	}

	retryWorker := func() (shouldRetry bool, err error, value interface{}) {
		config := &DatabaseConfig{}
		cas, getErr := b.Connection.GetMetadataDocument(ctx, bucketName, PersistentConfigKey(ctx, groupID, dbName), config)
		// Success case - delete has been completed
		if base.IsDocNotFoundError(getErr) {
			return false, nil, nil
		}
		// For non-recoverable errors, return the error
		if getErr != nil {
			return false, getErr, nil
		}
		// On version mismatch, abandon attempts to delete
		if version != "" && version != config.Version {
			return false, base.ErrConfigRegistryReloadRequired, nil
		}

		return true, base.ErrAlreadyExists, cas
	}

	// Kick off the retry loop
	err, retryResult := base.RetryLoop(
		ctx,
		"Wait for config version match",
		retryWorker,
		base.CreateDoublingSleeperDurationFunc(50, timeout),
	)

	// If still exists after retry, re-attempt the delete
	if err == base.ErrAlreadyExists {
		existingCas, ok := retryResult.(uint64)
		if !ok {
			return fmt.Errorf("Unable to convert returned cas of type %T to uint64", retryResult)
		}

		err = b.Connection.DeleteMetadataDocument(ctx, bucketName, PersistentConfigKey(ctx, groupID, dbName), existingCas)
		if err != nil {
			return err
		}
		// delete successful, call rollback to remove entry from registry
		if version != "" {
			rollbackErr := b.rollbackRegistry(ctx, bucketName, groupID, dbName, nil, registry)
			if rollbackErr != nil {
				return rollbackErr
			}
		}

		return base.ErrConfigRegistryRollback
	}
	return err
}

// rollbackRegistry updates the registry entry to match the provided config
func (b *bootstrapContext) rollbackRegistry(ctx context.Context, bucketName, groupID, dbName string, config *DatabaseConfig, registry *GatewayRegistry) error {

	if config == nil {
		// nil config indicates dbName should be removed from registry
		base.InfofCtx(ctx, base.KeyConfig, "Rolling back config registry to remove db %s - config does not exist. bucketName:%s groupID:%s - config has been deleted", base.MD(dbName), base.MD(bucketName), base.MD(groupID))
		found := registry.removeDatabase(groupID, dbName)
		if !found {
			// The registry hasn't been reloaded since we tried to find this config, failing to find it now is unexpected
			return fmt.Errorf("Attempted to remove database %s (%s) from registry, was not found", base.MD(dbName), base.MD(groupID))
		}
	} else {
		// Mark the database config being rolled back first to update CAS, to ensure a slow writer doesn't succeed while we're rolling back.
		// Use the database name property for the update, as this is otherwise immutable.
		casOut, err := b.Connection.TouchMetadataDocument(ctx, bucketName, PersistentConfigKey(ctx, groupID, dbName), "name", dbName, config.cfgCas)
		if err != nil {
			return fmt.Errorf("Rollback cancelled - document has been updated")
		}
		config.cfgCas = casOut

		// non-nil config indicates database version in registry should be updated to match config
		base.InfofCtx(ctx, base.KeyConfig, "Rolling back config registry to align with db config version %s for db: %s, bucket:%s configGroup:%s", config.Version, base.MD(dbName), base.MD(bucketName), base.MD(groupID))
		registryErr := registry.rollbackDatabaseConfig(ctx, groupID, dbName)
		if registryErr != nil {
			// There shouldn't be a case where rollback introduces a collection conflict - it
			// shouldn't be possible to add a conflicting collection to the registry while a previous
			// config persistence is in-flight
			return fmt.Errorf("Unable to roll back registry to match existing config for database %s(%s): %w", base.MD(dbName), base.MD(groupID), registryErr)
		}
	}

	// Attempt to persist the registry
	casOut, err := b.Connection.WriteMetadataDocument(ctx, bucketName, base.SGRegistryKey, registry.cas, registry)
	if err == nil {
		registry.cas = casOut
		base.InfofCtx(ctx, base.KeyConfig, "Successful config registry rollback for bucket: %s, configGroup: %s, db: %s", base.MD(bucketName), base.MD(groupID), base.MD(dbName))
	}
	return err
}

// getGatewayRegistry returns the database registry document for the bucket
func (b *bootstrapContext) getGatewayRegistry(ctx context.Context, bucketName string) (result *GatewayRegistry, err error) {

	registry := &GatewayRegistry{}
	cas, getErr := b.Connection.GetMetadataDocument(ctx, bucketName, base.SGRegistryKey, registry)
	if getErr != nil {
		if base.IsDocNotFoundError(getErr) {
			return NewGatewayRegistry(b.sgVersion), nil
		}
		return nil, getErr
	}
	if registry.SGVersion.String() == "" {
		// 3.1.0 and 3.1.1 don't write a SGVersion, but everything else will
		configSGVersionStr := "3.1.0"
		v, err := base.NewComparableBuildVersionFromString(configSGVersionStr)
		if err != nil {
			return nil, err
		}
		registry.SGVersion = *v

	}

	registry.cas = cas

	return registry, nil
}

// getGatewayRegistry returns the database registry document for the bucket
func (b *bootstrapContext) setGatewayRegistry(ctx context.Context, bucketName string, registry *GatewayRegistry) (err error) {

	cas := uint64(0)
	if registry != nil {
		cas = registry.cas
	}

	var casOut uint64
	var writeErr error
	if cas == 0 {
		casOut, writeErr = b.Connection.InsertMetadataDocument(ctx, bucketName, base.SGRegistryKey, registry)
	} else {
		casOut, writeErr = b.Connection.WriteMetadataDocument(ctx, bucketName, base.SGRegistryKey, cas, registry)
	}

	if writeErr != nil {
		return writeErr
	}
	registry.cas = casOut
	return nil
}

// getRegistryAndDatabase retrieves both the gateway registry and database config for the specified dbName and groupID.
// If registry is not found, returns ErrNotFound
// If registry exists but database is not found, returns err=nil and config=nil
//
// This function manages synchronization between the registry and config by only returning successfully when the database
// versions align.  If the versions do not match, it will retry up to the maxRegistryRetryInterval.  If the maxInterval is
// reached, it triggers rollback of the registry to the config version - this covers cases where another node has failed
// between persisting registry and config.
// If the dbName does not exist in the registry but a config file does, similar retry is performed to wait for in-flight
// database deletion.
// If config rollback is triggered, the process is restarted up to the maximum registry reload count
func (b *bootstrapContext) getRegistryAndDatabase(ctx context.Context, bucketName, groupID, dbName string) (registry *GatewayRegistry, config *DatabaseConfig, err error) {

	base.DebugfCtx(ctx, base.KeyConfig, "getRegistryAndDatabase for bucket: %s, groupID: %s, db: %s", bucketName, groupID, dbName)
	registryLoadCount := 0
	// Registry reloads should only happen in the event of node failure, or due to concurrent registry operations from multiple requests.
	// Given this, we can abandon the current operation and return error after a small number of retry attempts
	maxRegistryLoadCount := 5

	// Outer loop performs a full retry (restarting from registry retrieval
	for registryLoadCount < maxRegistryLoadCount {
		registryLoadCount++
		if registryLoadCount > 1 {
			base.InfofCtx(ctx, base.KeyConfig, "Reloading registry for bucket: %s, groupID: %s, db: %s, attempt (%d/%d)", bucketName, groupID, dbName, registryLoadCount, maxRegistryLoadCount)
		}

		registry, err = b.getGatewayRegistry(ctx, bucketName)
		if err != nil {
			return nil, nil, err
		}

		var registryDb *RegistryDatabase
		configGroup, exists := registry.ConfigGroups[groupID]
		if exists {
			registryDb, exists = configGroup.Databases[dbName]
		}

		if !exists {
			// Use waitForConfigDelete to confirm/clean up any unexpected config files.  Recovers from slow updates
			// racing with rollback.
			err := b.waitForConfigDelete(ctx, bucketName, groupID, dbName, "", registry)
			if err == base.ErrConfigRegistryReloadRequired {
				continue
			}
			// On rollback (delete) of config for non-existent registry entry, log a warning but continue.
			if err == base.ErrConfigRegistryRollback {
				base.WarnfCtx(ctx, "Removed existing config for groupID: %v, dbName: %v that was not found in the registry", base.MD(groupID), base.MD(dbName))
				return registry, nil, nil
			}
			return registry, nil, err
		} else {
			if registryDb.Version != "" && registryDb.Version != deletedDatabaseVersion {
				// Database exists in registry, go fetch the config
				config, err = b.getDatabaseConfig(ctx, bucketName, groupID, dbName, registryDb.Version, registry)
				if err == base.ErrConfigRegistryReloadRequired {
					// ReloadRegistry is returned by getDatabaseConfig immediately if the config version is greater than version found in the registry.
					// We want to restart to pick up the latest registry
					continue
				}
			} else if registryDb.PreviousVersion != nil {
				// Previous Version without current version represents in-progress delete.  Wait for delete to complete
				err := b.waitForConfigDelete(ctx, bucketName, groupID, dbName, registryDb.PreviousVersion.Version, registry)
				if err == base.ErrConfigRegistryReloadRequired {
					// ReloadRegistry is returned by waitForConfigDelete immediately if the config exists but the
					// version does not match the previous version. Indicates a concurrent author has recreated the
					// database - continue to reload the registry.
					continue
				}
			}

			// Otherwise we're done (either success, or return unrecoverable error)
			return registry, config, err
		}
	}

	return nil, nil, fmt.Errorf("Unable to retrieve config - registry reload limit reached(%v)", maxRegistryLoadCount)

}

func (b *bootstrapContext) addDatabaseLogContext(ctx context.Context, config *DbConfig) context.Context {
	return base.DatabaseLogCtx(ctx, config.Name, config.toDbConsoleLogConfig(ctx))
}

func (b *bootstrapContext) ComputeMetadataIDForDbConfig(ctx context.Context, config *DbConfig) (string, error) {
	registry, err := b.getGatewayRegistry(ctx, config.GetBucketName())
	if err != nil {
		return "", err
	}
	metadataID := b.computeMetadataID(ctx, registry, config)
	return metadataID, nil
}

// computeMetadataID determines whether the database should use the default metadata storage location (to support configurations upgrading with
// existing sync metadata in the default collection).  The default metadataID is only used when all of the following
// conditions are met:
//  1. There isn't already a metadataID defined for the database name  in another config group
//  2. The default metadataID isn't already in use by another database in the registry
//  3. The database includes _default._default
//  4. The _default._default collection isn't already associated with a different metadata ID (syncInfo document is not present, or has a value of defaultMetadataID)
func (b *bootstrapContext) computeMetadataID(ctx context.Context, registry *GatewayRegistry, config *DbConfig) string {

	standardMetadataID := b.standardMetadataID(config.Name)

	// If there's already a metadataID assigned to this database in the registry (including other config groups), use that
	defaultMetadataIDInUse := false
	for _, cg := range registry.ConfigGroups {
		for dbName, db := range cg.Databases {
			if dbName == config.Name {
				return db.MetadataID
			}
			if db.MetadataID == defaultMetadataID {
				defaultMetadataIDInUse = true
			}
		}
	}

	// If the default metadata ID is already in use in the registry by a different database, use standard ID.
	if defaultMetadataIDInUse {
		return standardMetadataID
	}

	// If the database config doesn't include _default._default, use standard ID
	if config.Scopes != nil {
		defaultFound := false
		for scopeName, scope := range config.Scopes {
			for collectionName := range scope.Collections {
				if base.IsDefaultCollection(scopeName, collectionName) {
					defaultFound = true
				}
			}
		}
		if !defaultFound {
			return standardMetadataID
		}
	}

	// If _default._default is already associated with a non-default metadataID, use the standard ID
	bucketName := config.GetBucketName()
	var syncInfo base.SyncInfo
	exists, err := b.Connection.GetDocument(ctx, bucketName, base.SGSyncInfo, &syncInfo)
	if err != nil {
		base.WarnfCtx(ctx, "Error checking syncInfo metadataID in default collection - using standard metadataID.  Error: %v", err)
		return standardMetadataID
	}

	if exists && syncInfo.MetadataID != defaultMetadataID {
		return standardMetadataID
	}

	return defaultMetadataID

}

// standardMetadataID returns either the dbName or a base64 encoded SHA256 hash of the dbName, whichever is shorter.
func (b *bootstrapContext) standardMetadataID(dbName string) string {
	return base.SerializeIfLonger(dbName, 40)
}

// CheckMinorDowngrade returns an error the sgVersion represents at least minor version downgrade from the version in the bucket.
func (b *bootstrapContext) CheckMinorDowngrade(ctx context.Context, bucketName string, sgVersion base.ComparableBuildVersion) error {
	registry, err := b.getGatewayRegistry(ctx, bucketName)
	if err != nil {
		return err
	}

	if registry.SGVersion.AtLeastMinorDowngrade(&sgVersion) {
		err := base.RedactErrorf("Bucket %q has metadata from a newer Sync Gateway %s. Current version of Sync Gateway is %s.", base.MD(bucketName), registry.SGVersion, sgVersion)
		return err
	}

	return nil
}

// SetSGVersion will update the registry in a bucket with a version of Sync Gateway. This will not perform a write if the version is already up to date.
func (b *bootstrapContext) SetSGVersion(ctx context.Context, bucketName string, sgVersion base.ComparableBuildVersion) error {
	registry, err := b.getGatewayRegistry(ctx, bucketName)
	if err != nil {
		return err
	}
	if !registry.SGVersion.Less(&sgVersion) {
		return nil
	}
	originalRegistryVersion := registry.SGVersion
	registry.SGVersion = sgVersion
	err = b.setGatewayRegistry(ctx, bucketName, registry)
	if err != nil {
		base.WarnfCtx(ctx, "Error setting gateway registry in bucket %q: %q", base.MD(bucketName), err)
		return err
	}
	base.InfofCtx(ctx, base.KeyConfig, "Updated Sync Gateway version number in bucket %q from %s to %s", base.MD(bucketName), originalRegistryVersion, sgVersion)
	return nil
}
