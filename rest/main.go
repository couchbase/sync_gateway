package rest

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
	pkgerrors "github.com/pkg/errors"
)

// ServerMain is the main entry point of launching the Sync Gateway server; the main
// function directly calls this. It registers both signal and fatal panic handlers,
// does the initial setup and finally starts the server.
func ServerMain() {
	if err := serverMain(context.Background(), os.Args); err != nil {
		base.Fatalf("Couldn't start Sync Gateway: %v", err)
	}
}

// TODO: Pass ctx down into HTTP servers so that serverMain can be stopped.
func serverMain(ctx context.Context, osArgs []string) error {
	RegisterSignalHandler()
	defer base.FatalPanicHandler()

	base.InitializeMemoryLoggers()
	base.LogSyncGatewayVersion()

	fs := flag.NewFlagSet(osArgs[0], flag.ContinueOnError)

	// used by service scripts as a way to specify a per-distro defaultLogFilePath
	defaultLogFilePath = *fs.String("defaultLogFilePath", "", "Path to log files, if not overridden by --logFilePath, or the config")

	disablePersistentConfigFlag := fs.Bool("disable_persistent_config", false, "Can be set to false to disable persistent config handling, and read all configuration from a legacy config file.")

	// TODO: CBG-1542 Merge legacyFlagStartupConfig onto default config before merging others.
	legacyFlagStartupConfig := registerLegacyFlags(fs)
	_ = legacyFlagStartupConfig

	// register config property flags
	var flagStartupConfig StartupConfig
	// TODO: CBG-1542 Revisit config cli flags after initial persistent config implementation
	// if err := clistruct.RegisterJSONFlags(fs, &flagStartupConfig); err != nil {
	// 	return err
	// }

	// TODO: Be removed in a future commit once flags are sorted
	adminInterfaceAuthFlag := fs.Bool("api.admin_interface_authentication", true, "")
	metricsInterfaceAuthFlag := fs.Bool("api.metrics_interface_authentication", true, "")

	useTLSServer := fs.Bool("bootstrap.use_tls_server", true, "")

	if err := fs.Parse(osArgs[1:]); err != nil {
		// Return nil for ErrHelp so the shell exit code is 0
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}

	// TODO: Be removed in a future commit once flags are sorted
	// Only override config value if user explicitly set flag
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "api.admin_interface_authentication":
			flagStartupConfig.API.AdminInterfaceAuthentication = adminInterfaceAuthFlag
		case "api.metrics_interface_authentication":
			flagStartupConfig.API.MetricsInterfaceAuthentication = metricsInterfaceAuthFlag
		case "bootstrap.use_tls_server":
			flagStartupConfig.Bootstrap.UseTLSServer = useTLSServer
		}
	})

	if *disablePersistentConfigFlag {
		return legacyServerMain(osArgs, &flagStartupConfig)
	}

	disablePersistentConfigFallback, err := serverMainPersistentConfig(fs, &flagStartupConfig)
	if disablePersistentConfigFallback {
		base.Infof(base.KeyAll, "Falling back to disabled persistent config...")
		return legacyServerMain(osArgs, &flagStartupConfig)
	}

	return err
}

// serverMainPersistentConfig runs the Sync Gateway server with persistent config.
func serverMainPersistentConfig(fs *flag.FlagSet, flagStartupConfig *StartupConfig) (disablePersistentConfigFallback bool, err error) {

	sc := DefaultStartupConfig(defaultLogFilePath)
	base.Tracef(base.KeyAll, "default config: %#v", sc)

	configPath := fs.Args()
	if len(configPath) > 1 {
		return false, fmt.Errorf("%d startup configs defined. Must be at most one startup config: %v", len(configPath), configPath)
	}

	var fileStartupConfig *StartupConfig
	if len(configPath) == 1 {
		fileStartupConfig, err = LoadStartupConfigFromPath(configPath[0])
		if pkgerrors.Cause(err) == base.ErrUnknownField {
			// If we have an unknown field error processing config its possible that the config is a 2.x config
			// requiring automatic upgrade. We should attempt to perform this upgrade

			base.Infof(base.KeyAll, "Found unknown fields in startup config. Attempting automatic config upgrade.")

			var upgradeError error
			fileStartupConfig, disablePersistentConfigFallback, upgradeError = automaticConfigUpgrade(configPath[0])
			if upgradeError != nil {

				// We need to validate if the error was again, an unknown field error. If this is the case its possible
				// the config is actually a 3.x config but with a genuine unknown field, therefore we should  return the
				// original error from LoadStartupConfigFromPath.
				if pkgerrors.Cause(upgradeError) == base.ErrUnknownField {
					base.Warnf("Automatic upgrade attempt failed, %s not recognized as legacy config format: %v", base.MD(configPath[0]), upgradeError)
					base.Warnf("Provided config %s not recognized as boostrap startup config format: %v", base.MD(configPath[0]), err)
					return false, fmt.Errorf("unknown config fields supplied. Unable to continue")
				}

				return false, upgradeError
			}

			if disablePersistentConfigFallback {
				base.Infof(base.KeyAll, "Startup config specified disable_persistent_config. Falling back to legacy config.")
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
			base.Tracef(base.KeyAll, "got config from file: %#v", redactedConfig)
			err = sc.Merge(fileStartupConfig)
			if err != nil {
				return false, err
			}
		}
	}

	// merge flagStartupConfig on top of fileStartupConfig, because flags take precedence over config files.
	if flagStartupConfig != nil {
		base.Tracef(base.KeyAll, "got config from flags: %#v", flagStartupConfig)
		err := sc.Merge(flagStartupConfig)
		if err != nil {
			return false, err
		}
	}

	redactedConfig, err := sc.Redacted()
	if err != nil {
		return false, err
	}
	base.Tracef(base.KeyAll, "final config: %#v", redactedConfig)

	initialStartupConfig, err := getInitialStartupConfig(fileStartupConfig, flagStartupConfig)
	if err != nil {
		return false, err
	}

	base.Infof(base.KeyAll, "Config: Starting in persistent mode using config group %q", sc.Bootstrap.ConfigGroupID)
	ctx, err := setupServerContext(&sc, true)
	if err != nil {
		return false, err
	}

	ctx.initialStartupConfig = initialStartupConfig

	return false, startServer(&sc, ctx)
}

func getInitialStartupConfig(sc *StartupConfig, flagStartupConfig *StartupConfig) (*StartupConfig, error) {
	initialStartupConfigTemp := StartupConfig{}
	err := initialStartupConfigTemp.Merge(sc)
	if err != nil {
		return nil, err
	}

	err = initialStartupConfigTemp.Merge(flagStartupConfig)
	if err != nil {
		return nil, err
	}

	// Requires a deep copy of final input as passed in values are pointers. Need to ensure runtime changes don't
	// affect this
	var initialStartupConfig StartupConfig
	err = base.DeepCopyInefficient(&initialStartupConfig, initialStartupConfigTemp)
	if err != nil {
		return nil, err
	}

	return &initialStartupConfig, nil
}

// automaticConfigUpgrade takes the config path of the current 2.x config and attempts to perform the update steps to
// update it to a 3.x config
// Returns the new startup config, a bool of whether to fallback to legacy config and an error
func automaticConfigUpgrade(configPath string) (*StartupConfig, bool, error) {
	legacyServerConfig, err := LoadServerConfig(configPath)
	if err != nil {
		return nil, false, err
	}

	if legacyServerConfig.DisablePersistentConfig != nil && *legacyServerConfig.DisablePersistentConfig {
		return nil, true, nil
	}

	startupConfig, dbConfigs, err := legacyServerConfig.ToStartupConfig()
	if err != nil {
		return nil, false, err
	}

	dbConfigs, err = sanitizeDbConfigs(dbConfigs)
	if err != nil {
		return nil, false, err
	}

	// Attempt to establish connection to server
	cluster, err := establishCouchbaseClusterConnection(startupConfig)
	if err != nil {
		return nil, false, err
	}

	defer func() {
		_ = cluster.Close()
	}()

	// Write database configs to CBS with groupID "default"
	for _, dbConfig := range dbConfigs {
		_, err = cluster.PutConfig(*dbConfig.Bucket, persistentConfigDefaultGroupID, base.Uint64Ptr(0), dbConfig)
		if err != nil {
			// If key already exists just continue
			if errors.Is(err, base.ErrAlreadyExists) {
				base.Infof(base.KeyAll, "Skipping Couchbase Server persistence for %s. Already exists.", base.UD(dbConfig.Name))
				continue
			}
			return nil, false, err
		}
		base.Infof(base.KeyAll, "Persisted database %s config to Couchbase Server bucket: %s", base.UD(dbConfig.Name), base.MD(*dbConfig.Bucket))
	}

	// Attempt to backup current config
	backupLocation, err := backupCurrentConfigFile(configPath)
	if err != nil {
		return nil, false, err
	}

	base.Infof(base.KeyAll, "Current config backed up to %s", base.MD(backupLocation))

	// Overwrite old config with new migrated startup config
	jsonStartupConfig, err := json.MarshalIndent(startupConfig, "", "  ")
	if err != nil {
		return nil, false, err
	}

	err = ioutil.WriteFile(configPath, jsonStartupConfig, 0644)
	if err != nil {
		return nil, false, err
	}

	base.Infof(base.KeyAll, "Current config file overwritten by upgraded config at %s", base.MD(configPath))

	return startupConfig, false, nil
}

// validate / sanitize db configs
// - remove servers
// - remove users
// - ensure servers are the same
func sanitizeDbConfigs(configMap DbConfigMap) (DbConfigMap, error) {
	var databaseServerAddress string

	for dbName, dbConfig := range configMap {
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

		dbConfig.Server = nil
		dbConfig.Users = nil
		dbConfig.Roles = nil

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

func establishCouchbaseClusterConnection(config *StartupConfig) (*base.CouchbaseCluster, error) {
	err, c := base.RetryLoop("Cluster Bootstrap", func() (shouldRetry bool, err error, value interface{}) {
		cluster, err := base.NewCouchbaseCluster(config.Bootstrap.Server, config.Bootstrap.Username,
			config.Bootstrap.Password, config.Bootstrap.X509CertPath, config.Bootstrap.X509KeyPath,
			config.Bootstrap.CACertPath, config.Bootstrap.ServerTLSSkipVerify)
		if err != nil {
			base.Infof(base.KeyConfig, "Couldn't connect to bootstrap cluster: %v - will retry...", err)
			return true, err, nil
		}

		return false, nil, cluster
	}, base.CreateSleeperFunc(27, 1000)) // ~2 mins total - 5 second gocb WaitForReady timeout and 1 second interval
	if err != nil {
		return nil, err
	}

	base.Infof(base.KeyConfig, "Successfully connected to cluster")
	clusterConnection := c.(*base.CouchbaseCluster)

	return clusterConnection, nil
}
