package rest

import (
	"flag"

	"github.com/couchbase/sync_gateway/base"
)

// legacyServerMain runs the pre-3.0 Sync Gateway server.
func legacyServerMain(osArgs []string) error {
	base.Warnf("Running in legacy config mode")

	config, err := setupServerConfig(osArgs)
	if err != nil {
		return err
	}

	lc := LegacyConfig{
		PersistentConfig:   base.BoolPtr(false),
		LegacyServerConfig: *config,
	}

	sc, databases, err := lc.ToStartupConfig()
	if err != nil {
		return err
	}

	ctx, err := setupServerContext(sc, false)
	if err != nil {
		return err
	}

	err = ctx.CreateLocalDatabase(databases)
	if err != nil {
		return err
	}

	return startServer(sc, ctx)
}

func registerLegacyFlags(fs *flag.FlagSet) *StartupConfig {
	sc := StartupConfig{}

	fs.String("interface", DefaultPublicInterface, "Address to bind to")
	fs.String("adminInterface", DefaultAdminInterface, "Address to bind admin interface to")
	fs.String("profileInterface", "", "Address to bind profile interface to")
	fs.String("configServer", "", "URL of server that can return database configs")
	fs.String("deploymentID", "", "Customer/project identifier for stats reporting")
	fs.String("url", "", "Address of Couchbase server")
	fs.String("dbname", "", "Name of Couchbase Server database (defaults to name of bucket)")
	fs.Bool("pretty", false, "Pretty-print JSON responses")
	fs.Bool("verbose", false, "Log more info about requests")
	fs.String("log", "", "Log keys, comma separated")
	fs.String("logFilePath", "", "Path to log files")
	fs.String("certpath", "", "Client certificate path")
	fs.String("cacertpath", "", "Root CA certificate path")
	fs.String("keypath", "", "Client certificate key path")

	return &sc
}
