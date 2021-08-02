package rest

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
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
	useTLSClient := fs.Bool("api.https.use_tls_client", true, "")

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
		case "api.https.use_tls_client":
			flagStartupConfig.API.HTTPS.UseTLSClient = useTLSClient
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

	if len(configPath) == 1 {
		fileStartupConfig, err := LoadStartupConfigFromPath(configPath[0])
		if pkgerrors.Cause(err) == base.ErrUnknownField {

			// Attempt to perform automatic config upgrade
			fileStartupConfig, err = automaticConfigUpgrade(configPath[0])
			if err != nil {
				return false, err
			}

		}
		if err != nil {
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

	base.Infof(base.KeyAll, "Config: Starting in persistent mode using config group %q", sc.Bootstrap.ConfigGroupID)
	ctx, err := setupServerContext(&sc, true)
	if err != nil {
		return false, err
	}

	return false, startServer(&sc, ctx)
}

func automaticConfigUpgrade(configPath string) (*StartupConfig, error) {
	legacyServerConfig, err := LoadServerConfig(configPath)
	if err != nil {
		return nil, err
	}

	startupConfig, dbConfigs, err := legacyServerConfig.ToStartupConfig()
	if err != nil {
		return nil, err
	}

	dbConfigs, err = sanitizeDbConfigs(dbConfigs)
	if err != nil {
		return nil, err
	}

	// Attempt to establish connection to server, add retry like its other use-case
	cluster, err := EstablishCouchbaseClusterConnection(startupConfig)
	if err != nil {
		return nil, err
	}

	defer cluster.Close()

	// Write database configs to CBS with groupID "default"
	for _, dbConfig := range dbConfigs {
		_, err = cluster.PutConfig(*dbConfig.Bucket, "default", base.Uint64Ptr(0), dbConfig)
		if err != nil {
			// TODO: if exists skip, else error
			return nil, err
		}
	}

	// Attempt to backup current config
	err = backupCurrentConfigFile(configPath)
	if err != nil {
		return nil, err
	}

	// Overwrite old config with new migrated startup config
	jsonStartupConfig, err := json.Marshal(startupConfig)
	err = ioutil.WriteFile(configPath, jsonStartupConfig, 0644)
	if err != nil {
		return nil, err
	}

	return startupConfig, nil
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
			return nil, fmt.Errorf("server addresses specified in dbConfig do not match. This is required for " +
				"persistent config")
		}

		if dbConfig.Bucket == nil || *dbConfig.Bucket == "" {
			*dbConfig.Bucket = dbName
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
func backupCurrentConfigFile(sourcePath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()

	backupDirPath := filepath.Dir(sourcePath)
	backupFileName := filepath.Base(sourcePath) + fmt.Sprintf("-bk-%s", time.Now().Format(base.ISO8601Format))

	backupPath := filepath.Join(backupDirPath, backupFileName)

	backup, err := os.Create(backupPath)
	if err != nil {
		return err
	}
	defer backup.Close()

	_, err = io.Copy(backup, source)
	if err != nil {
		return err
	}

	return nil
}

func EstablishCouchbaseClusterConnection(config *StartupConfig) (*base.CouchbaseCluster, error) {
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
