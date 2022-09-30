// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	pkgerrors "github.com/pkg/errors"
)

// ServerMain is the main entry point of launching the Sync Gateway server; the main
// function directly calls this. It registers both signal and fatal panic handlers,
// does the initial setup and finally starts the server.
func ServerMain() {
	if err := serverMain(context.Background(), os.Args); err != nil {
		base.FatalfCtx(context.TODO(), "Couldn't start Sync Gateway: %v", err)
	}
}

// TODO: Pass ctx down into HTTP servers so that serverMain can be stopped.
func serverMain(ctx context.Context, osArgs []string) error {
	RegisterSignalHandler(ctx)
	defer base.FatalPanicHandler()

	base.InitializeMemoryLoggers()
	base.LogSyncGatewayVersion()

	flagStartupConfig, fs, disablePersistentConfig, err := parseFlags(osArgs)
	if err != nil {
		// Return nil for ErrHelp so the shell exit code is 0
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}

	if *disablePersistentConfig {
		return legacyServerMain(ctx, osArgs, flagStartupConfig)
	}

	disablePersistentConfigFallback, err := serverMainPersistentConfig(ctx, fs, flagStartupConfig)
	if disablePersistentConfigFallback {
		return legacyServerMain(ctx, osArgs, flagStartupConfig)
	}

	return err
}

// serverMainPersistentConfig runs the Sync Gateway server with persistent config.
func serverMainPersistentConfig(ctx context.Context, fs *flag.FlagSet, flagStartupConfig *StartupConfig) (disablePersistentConfigFallback bool, err error) {

	sc := DefaultStartupConfig(defaultLogFilePath)
	base.TracefCtx(ctx, base.KeyAll, "default config: %#v", sc)

	configPath := fs.Args()
	if len(configPath) > 1 {
		return false, fmt.Errorf("%d startup configs defined. Must be at most one startup config: %v", len(configPath), configPath)
	}

	var fileStartupConfig *StartupConfig
	var legacyDbUsers map[string]map[string]*auth.PrincipalConfig // [db][user]PrincipleConfig
	var legacyDbRoles map[string]map[string]*auth.PrincipalConfig // [db][roles]PrincipleConfig
	if len(configPath) == 1 {
		fileStartupConfig, err = LoadStartupConfigFromPath(configPath[0])
		if pkgerrors.Cause(err) == base.ErrUnknownField {
			// If we have an unknown field error processing config its possible that the config is a 2.x config
			// requiring automatic upgrade. We should attempt to perform this upgrade

			base.InfofCtx(ctx, base.KeyAll, "Found unknown fields in startup config. Attempting to read as legacy config.")

			var upgradeError error
			fileStartupConfig, disablePersistentConfigFallback, legacyDbUsers, legacyDbRoles, upgradeError = automaticConfigUpgrade(configPath[0])
			if upgradeError != nil {

				// We need to validate if the error was again, an unknown field error. If this is the case its possible
				// the config is actually a 3.x config but with a genuine unknown field, therefore we should  return the
				// original error from LoadStartupConfigFromPath.
				if pkgerrors.Cause(upgradeError) == base.ErrUnknownField {
					base.WarnfCtx(ctx, "Automatic upgrade attempt failed, %s not recognized as legacy config format: %v", base.MD(configPath[0]), upgradeError)
					base.WarnfCtx(ctx, "Provided config %s not recognized as bootstrap config format: %v", base.MD(configPath[0]), err)
					return false, fmt.Errorf("unknown config fields supplied. Unable to continue")
				}

				return false, upgradeError
			}

			if disablePersistentConfigFallback {
				return true, nil
			}

		} else if err != nil {
			return false, fmt.Errorf("Couldn't open config file: %w", err)
		}
		if fileStartupConfig != nil {
			redactedConfig, err := sc.Redacted()
			if err != nil {
				return false, err
			}
			base.TracefCtx(ctx, base.KeyAll, "got config from file: %#v", redactedConfig)
			err = sc.Merge(fileStartupConfig)
			if err != nil {
				return false, err
			}
		}
	}

	// merge flagStartupConfig on top of fileStartupConfig, because flags take precedence over config files.
	if flagStartupConfig != nil {
		base.TracefCtx(ctx, base.KeyAll, "got config from flags: %#v", flagStartupConfig)
		err := sc.Merge(flagStartupConfig)
		if err != nil {
			return false, err
		}
	}

	redactedConfig, err := sc.Redacted()
	if err != nil {
		return false, err
	}
	base.TracefCtx(ctx, base.KeyAll, "final config: %#v", redactedConfig)

	initialStartupConfig, err := getInitialStartupConfig(fileStartupConfig, flagStartupConfig)
	if err != nil {
		return false, err
	}

	base.InfofCtx(ctx, base.KeyAll, "Config: Starting in persistent mode using config group %q", sc.Bootstrap.ConfigGroupID)
	svrctx, err := SetupServerContext(ctx, &sc, true)
	if err != nil {
		return false, err
	}

	svrctx.initialStartupConfig = initialStartupConfig

	svrctx.addLegacyPrincipals(ctx, legacyDbUsers, legacyDbRoles)

	return false, StartServer(ctx, &sc, svrctx)
}

func getInitialStartupConfig(fileStartupConfig *StartupConfig, flagStartupConfig *StartupConfig) (*StartupConfig, error) {
	initialStartupConfigTemp := StartupConfig{}

	if fileStartupConfig != nil {
		if err := initialStartupConfigTemp.Merge(fileStartupConfig); err != nil {
			return nil, err
		}
	}

	if flagStartupConfig != nil {
		if err := initialStartupConfigTemp.Merge(flagStartupConfig); err != nil {
			return nil, err
		}
	}

	// Requires a deep copy of final input as passed in values are pointers.
	// Need to ensure runtime changes don't affect initialStartupConfig.
	var initialStartupConfig StartupConfig
	if err := base.DeepCopyInefficient(&initialStartupConfig, initialStartupConfigTemp); err != nil {
		return nil, err
	}

	return &initialStartupConfig, nil
}

// automaticConfigUpgrade takes the config path of the current 2.x config and attempts to perform the update steps to
// update it to a 3.x config
// Returns the new startup config, a bool of whether to fallback to legacy config, map of users per database, map of roles per database, and an error
func automaticConfigUpgrade(configPath string) (sc *StartupConfig, disablePersistentConfig bool, users map[string]map[string]*auth.PrincipalConfig, roles map[string]map[string]*auth.PrincipalConfig, err error) {
	legacyServerConfig, err := LoadLegacyServerConfig(configPath)
	if err != nil {
		return nil, false, nil, nil, err
	}

	if legacyServerConfig.DisablePersistentConfig != nil && *legacyServerConfig.DisablePersistentConfig {
		return nil, true, users, roles, nil
	}

	base.InfofCtx(context.Background(), base.KeyAll, "Config is a legacy config, and disable_persistent_config was not requested. Attempting automatic config upgrade.")

	startupConfig, dbConfigs, err := legacyServerConfig.ToStartupConfig()
	if err != nil {
		return nil, false, nil, nil, err
	}

	dbConfigs, err = sanitizeDbConfigs(dbConfigs)
	if err != nil {
		return nil, false, nil, nil, err
	}

	// Attempt to establish connection to server
	cluster, err := CreateCouchbaseClusterFromStartupConfig(startupConfig)
	if err != nil {
		return nil, false, nil, nil, err
	}

	// Write database configs to CBS with groupID "default"
	for _, dbConfig := range dbConfigs {
		dbc := dbConfig.ToDatabaseConfig()

		dbc.Version, err = GenerateDatabaseConfigVersionID("", &dbc.DbConfig)
		if err != nil {
			return nil, false, nil, nil, err
		}

		dbc.SGVersion = base.ProductVersion.String()

		// Return users and roles separate from config
		users = make(map[string]map[string]*auth.PrincipalConfig)
		roles = make(map[string]map[string]*auth.PrincipalConfig)
		users[dbc.Name] = dbc.Users
		roles[dbc.Name] = dbc.Roles
		dbc.Roles = nil
		dbc.Users = nil

		configGroupID := PersistentConfigDefaultGroupID
		if startupConfig.Bootstrap.ConfigGroupID != "" {
			configGroupID = startupConfig.Bootstrap.ConfigGroupID
		}

		_, err = cluster.InsertConfig(*dbc.Bucket, configGroupID, dbc)
		if err != nil {
			// If key already exists just continue
			if errors.Is(err, base.ErrAlreadyExists) {
				base.InfofCtx(context.Background(), base.KeyAll, "Skipping Couchbase Server persistence for config group %q in %s. Already exists.", configGroupID, base.UD(dbc.Name))
				continue
			}
			return nil, false, nil, nil, err
		}
		base.InfofCtx(context.Background(), base.KeyAll, "Persisted database %s config for group %q to Couchbase Server bucket: %s", base.UD(dbc.Name), configGroupID, base.MD(*dbc.Bucket))
	}

	// Attempt to backup current config
	// If we are able to write the config continue with the upgrade process writing
	// Otherwise continue with startup but don't attempt to write migrated config and log warning
	backupLocation, err := backupCurrentConfigFile(configPath)
	if err != nil {
		base.WarnfCtx(context.Background(), "Unable to write config file backup: %v. Won't write backup or updated config but will continue with startup.", err)
		return startupConfig, false, users, roles, nil
	}

	base.InfofCtx(context.Background(), base.KeyAll, "Current config backed up to %s", base.MD(backupLocation))

	// Overwrite old config with new migrated startup config
	jsonStartupConfig, err := json.MarshalIndent(startupConfig, "", "  ")
	if err != nil {
		return nil, false, nil, nil, err
	}

	// Attempt to write over the old config with the new migrated config
	// If we are able to write the config log success and continue
	// Otherwise continue with startup but log warning
	err = os.WriteFile(configPath, jsonStartupConfig, 0644)
	if err != nil {
		base.WarnfCtx(context.Background(), "Unable to write updated config file: %v -  but will continue with startup.", err)
		return startupConfig, false, users, roles, nil
	}

	base.InfofCtx(context.Background(), base.KeyAll, "Current config file overwritten by upgraded config at %s", base.MD(configPath))
	return startupConfig, false, users, roles, nil
}

// validate / sanitize db configs
// - remove fields no longer valid for persisted db configs
// - ensure matching servers are provided in all db configs
func sanitizeDbConfigs(configMap DbConfigMap) (DbConfigMap, error) {
	var databaseServerAddress string

	processedBucketDbNames := make(map[string]string, len(configMap))

	for dbName, dbConfig := range configMap {
		if dbConfig.Server == nil || *dbConfig.Server == "" {
			return nil, fmt.Errorf("automatic upgrade to persistent config requires each database config to have a server " +
				"address specified that are all matching in the 2.x config")
		}

		if databaseServerAddress == "" {
			databaseServerAddress = *dbConfig.Server
		}

		if *dbConfig.Server != databaseServerAddress {
			return nil, fmt.Errorf("automatic upgrade to persistent config requires matching server addresses in " +
				"2.x config")
		}

		if dbConfig.Bucket == nil || *dbConfig.Bucket == "" {
			dbNameCopy := dbName
			dbConfig.Bucket = &dbNameCopy
		}

		dbConfig.Name = dbName

		// strip now disallowed options (these are inherited from the bootstrap config on db load)
		dbConfig.Server = nil
		dbConfig.Username = ""
		dbConfig.Password = ""
		dbConfig.CertPath = ""
		dbConfig.KeyPath = ""
		dbConfig.CACertPath = ""

		if dbNameConflicting, ok := processedBucketDbNames[*dbConfig.Bucket]; ok {
			return nil, fmt.Errorf("automatic upgrade to persistent config failed. Only one database can "+
				"target any given bucket. %s used by %s and %s", *dbConfig.Bucket, dbName, dbNameConflicting)
		}

		processedBucketDbNames[*dbConfig.Bucket] = dbName

		// Make sure any updates are written back to the config
		configMap[dbName] = dbConfig
	}
	return configMap, nil
}

// backupCurrentConfigFile takes the original config path and copies this to a file with -bk appended with a timestamp
func backupCurrentConfigFile(sourcePath string) (string, error) {
	source, err := os.Open(sourcePath)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = source.Close()
	}()

	// Grab file extension
	fileExtension := filepath.Ext(sourcePath)

	// Remove file extension so we can easily modify filename
	fileNameWithoutExtension := strings.TrimSuffix(filepath.Base(sourcePath), fileExtension)

	// Modify file name and append file extension back onto it
	fileName := fmt.Sprintf("%s-backup-%d%s", fileNameWithoutExtension, time.Now().Unix(), fileExtension)

	backupPath := filepath.Join(filepath.Dir(sourcePath), fileName)

	backup, err := os.Create(backupPath)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = backup.Close()
	}()

	_, err = io.Copy(backup, source)
	if err != nil {
		return "", err
	}

	return backupPath, nil
}

func CreateCouchbaseClusterFromStartupConfig(config *StartupConfig) (*base.CouchbaseCluster, error) {
	cluster, err := base.NewCouchbaseCluster(config.Bootstrap.Server, config.Bootstrap.Username, config.Bootstrap.Password,
		config.Bootstrap.X509CertPath, config.Bootstrap.X509KeyPath, config.Bootstrap.CACertPath,
		config.IsServerless(), config.BucketCredentials, config.Bootstrap.ServerTLSSkipVerify)
	if err != nil {
		base.InfofCtx(context.Background(), base.KeyConfig, "Couldn't create couchbase cluster instance: %v", err)
		return nil, err
	}

	return cluster, nil
}

// parseFlags handles the parsing of legacy and persistent config flags.
func parseFlags(args []string) (flagStartupConfig *StartupConfig, fs *flag.FlagSet, disablePersistentConfig *bool, err error) {
	fs = flag.NewFlagSet(args[0], flag.ContinueOnError)

	// used by service scripts as a way to specify a per-distro defaultLogFilePath
	defaultLogFilePathFlag := fs.String("defaultLogFilePath", "", "Path to log files, if not overridden by --logFilePath, or the config")

	disablePersistentConfig = fs.Bool("disable_persistent_config", false, "Can be set to false to disable persistent config handling, and read all configuration from a legacy config file.")

	// register config property flags
	startupConfig := NewEmptyStartupConfig()
	legacyStartupConfig := NewEmptyStartupConfig()

	configFlags := registerConfigFlags(&startupConfig, fs)
	legacyConfigFlags := registerLegacyFlags(&legacyStartupConfig, fs)

	if err = fs.Parse(args[1:]); err != nil {
		return nil, nil, nil, err
	}

	err = fillConfigWithFlags(fs, configFlags)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error merging flags on to config: %w", err)
	}

	err = fillConfigWithLegacyFlags(legacyConfigFlags, fs, startupConfig.Logging.Console.LogLevel != nil)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error merging legacy flags on to config: %w", err)
	}
	// Merge persistent config on to legacy startup config to give priority to persistent config flags
	err = legacyStartupConfig.Merge(&startupConfig)
	if err != nil {
		return nil, nil, nil, err
	}

	defaultLogFilePath = *defaultLogFilePathFlag

	return &legacyStartupConfig, fs, disablePersistentConfig, nil
}
