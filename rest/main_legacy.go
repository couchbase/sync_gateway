package rest

import (
	"context"
	"flag"
	"fmt"
	"reflect"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

const flagDeprecated = `Flag "%s" is deprecated. Please use "%s" in future.`

// legacyServerMain runs the pre-3.0 Sync Gateway server.
func legacyServerMain(osArgs []string, flagStartupConfig *StartupConfig) error {
	base.WarnfCtx(context.Background(), "Running in legacy config mode")

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
		base.TracefCtx(context.Background(), base.KeyAll, "got config from flags: %#v", flagStartupConfig)
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

type legacyConfigFlag struct {
	config         interface{}
	supersededFlag string
	flagValue      interface{}
}

func registerLegacyFlags(config *StartupConfig, fs *flag.FlagSet) map[string]legacyConfigFlag {
	return map[string]legacyConfigFlag{
		"interface":        {&config.API.PublicInterface, "api.public_interface", fs.String("interface", DefaultPublicInterface, "DEPRECATED: Address to bind to")},
		"adminInterface":   {&config.API.AdminInterface, "api.admin_interface", fs.String("adminInterface", DefaultAdminInterface, "DEPRECATED: Address to bind admin interface to")},
		"profileInterface": {&config.API.ProfileInterface, "api.profile_interface", fs.String("profileInterface", "", "DEPRECATED: Address to bind profile interface to")},
		"pretty":           {&config.API.Pretty, "api.pretty", fs.Bool("pretty", false, "DEPRECATED: Pretty-print JSON responses")},
		"verbose":          {&config.Logging.Console.LogLevel, "", fs.Bool("verbose", false, "DEPRECATED: Log more info about requests")},
		"url":              {&config.Bootstrap.Server, "bootstrap.server", fs.String("url", "", "DEPRECATED: Address of Couchbase server")},
		"certpath":         {&config.API.HTTPS.TLSCertPath, "api.https.tls_cert_path", fs.String("certpath", "", "DEPRECATED: Client certificate path")},
		"keypath":          {&config.API.HTTPS.TLSKeyPath, "api.https.tls_key_path", fs.String("keypath", "", "DEPRECATED: Client certificate key path")},
		"cacertpath":       {&config.Bootstrap.CACertPath, "bootstrap.ca_cert_path", fs.String("cacertpath", "", "DEPRECATED: Root CA certificate path")},
		"log":              {&config.Logging.Console.LogKeys, "logging.console.log_keys", fs.String("log", "", "DEPRECATED: Log keys, comma separated")},
		"logFilePath":      {&config.Logging.LogFilePath, "logging.log_file_path", fs.String("logFilePath", "", "DEPRECATED: Path to log files")},

		// Removed options
		"dbname":       {nil, "", fs.String("dbname", "", "REMOVED: Name of Couchbase Server database (defaults to name of bucket)")},
		"configServer": {nil, "", fs.String("configServer", "", "REMOVED: URL of server that can return database configs")},
		"deploymentID": {nil, "", fs.String("deploymentID", "", "REMOVED: Customer/project identifier for stats reporting")},
	}
}

func fillConfigWithLegacyFlags(flags map[string]legacyConfigFlag, fs *flag.FlagSet, consoleLogLevelSet bool) error {
	var errors *base.MultiError
	fs.Visit(func(f *flag.Flag) {
		cfgFlag, legacyFlag := flags[f.Name]
		if !legacyFlag {
			return
		}
		switch f.Name {
		case "interface", "adminInterface", "profileInterface", "url", "certpath", "keypath", "cacertpath", "logFilePath":
			*cfgFlag.config.(*string) = *cfgFlag.flagValue.(*string)
			base.WarnfCtx(context.Background(), flagDeprecated, "-"+f.Name, "-"+cfgFlag.supersededFlag)
		case "pretty":
			rCfg := reflect.ValueOf(cfgFlag.config).Elem()
			rFlag := reflect.ValueOf(cfgFlag.flagValue)
			rCfg.Set(rFlag)
			base.WarnfCtx(context.Background(), flagDeprecated, "-"+f.Name, "-"+cfgFlag.supersededFlag)
		case "verbose":
			if *cfgFlag.flagValue.(*bool) {
				if consoleLogLevelSet {
					base.WarnfCtx(context.Background(), `Cannot use deprecated flag "-verbose" with flag "-logging.console.log_level". To set Sync Gateway to be verbose, please use flag "-logging.console.log_level info". Ignoring flag...`)
				} else {
					*cfgFlag.config.(**base.LogLevel) = base.LogLevelPtr(base.LevelInfo)
					base.WarnfCtx(context.Background(), flagDeprecated, "-"+f.Name, "-logging.console.log_level info")
				}
			}
		case "log":
			list := strings.Split(*cfgFlag.flagValue.(*string), ",")
			*cfgFlag.config.(*[]string) = list
			base.WarnfCtx(context.Background(), flagDeprecated, "-"+f.Name, "-"+cfgFlag.supersededFlag)
		case "configServer":
			err := fmt.Errorf(`flag "-%s" is no longer supported and has been removed`, f.Name)
			errors = errors.Append(err)
		case "dbname", "deploymentID":
			base.WarnfCtx(context.Background(), `Flag "-%s" is no longer supported and has been removed.`, f.Name)
		}
	})
	return errors.ErrorOrNil()
}
