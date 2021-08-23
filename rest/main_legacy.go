package rest

import (
	"flag"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

// legacyServerMain runs the pre-3.0 Sync Gateway server.
func legacyServerMain(osArgs []string, flagStartupConfig *StartupConfig) error {
	base.Warnf("Running in legacy config mode")

	lc, err := setupServerConfig(osArgs)
	if err != nil {
		return err
	}

	sc := DefaultStartupConfig(defaultLogFilePath)

	lc.DisablePersistentConfig = base.BoolPtr(true)

	migratedStartupConfig, databases, err := lc.ToStartupConfig()
	if err != nil {
		return err
	}

	err = sc.Merge(migratedStartupConfig)
	if err != nil {
		return err
	}

	if flagStartupConfig != nil {
		base.Tracef(base.KeyAll, "got config from flags: %#v", flagStartupConfig)
		err := sc.Merge(flagStartupConfig)
		if err != nil {
			return err
		}
	}

	initialStartupConfig, err := getInitialStartupConfig(migratedStartupConfig, flagStartupConfig)
	if err != nil {
		return err
	}

	ctx, err := setupServerContext(&sc, false)
	if err != nil {
		return err
	}

	ctx.initialStartupConfig = initialStartupConfig

	err = ctx.CreateLocalDatabase(databases)
	if err != nil {
		return err
	}

	return startServer(&sc, ctx)
}

func registerLegacyFlags(fs *flag.FlagSet) *StartupConfig {
	publicInterface := fs.String("interface", DefaultPublicInterface, "Address to bind to")
	adminInterface := fs.String("adminInterface", DefaultAdminInterface, "Address to bind admin interface to")
	profileInterface := fs.String("profileInterface", "", "Address to bind profile interface to")
	pretty := fs.Bool("pretty", false, "Pretty-print JSON responses")
	verbose := fs.Bool("verbose", false, "Log more info about requests")

	url := fs.String("url", "", "Address of Couchbase server")
	certPath := fs.String("certpath", "", "Client certificate path")
	keyPath := fs.String("keypath", "", "Client certificate key path")
	caCertPath := fs.String("cacertpath", "", "Root CA certificate path")

	log := fs.String("log", "", "Log keys, comma separated")
	logFilePath := fs.String("logFilePath", "", "Path to log files")

	sc := StartupConfig{
		Bootstrap: BootstrapConfig{
			Server:       *url,
			CACertPath:   *caCertPath,
			X509CertPath: *certPath,
			X509KeyPath:  *keyPath,
		},
		API: APIConfig{
			ProfileInterface: *profileInterface,
		},
		Logging: base.LoggingConfig{
			LogFilePath: *logFilePath,
			Console:     &base.ConsoleLoggerConfig{},
		},
	}

	// Set if user modified default value
	if *publicInterface != DefaultPublicInterface {
		sc.API.PublicInterface = *publicInterface
	}
	if *adminInterface != DefaultAdminInterface {
		sc.API.AdminInterface = *adminInterface
	}
	if !*pretty {
		sc.API.Pretty = pretty
	}
	if *verbose {
		sc.Logging.Console.LogLevel = base.LogLevelPtr(base.LevelInfo)
	}
	if *log != "" {
		sc.Logging.Console.LogKeys = strings.Split(*log, ",")
	}

	// removed options
	dbname := fs.String("dbname", "", "Name of Couchbase Server database (defaults to name of bucket)")
	if *dbname != "" {
		//
	}
	configServer := fs.String("configServer", "", "URL of server that can return database configs")
	if *configServer != "" {
		//
	}
	deploymentID := fs.String("deploymentID", "", "Customer/project identifier for stats reporting")
	if *deploymentID != "" {
		//
	}

	return &sc
}
