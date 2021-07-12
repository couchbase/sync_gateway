package rest

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/couchbase/sync_gateway/base"
	"github.com/imdario/mergo"
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

	// TODO: CBG-1461 Change default when we're ready to enable 3.0/bootstrap/persistent config by default (once QE's existing tests are ready to handle it)
	disablePersistentConfigFlag := fs.Bool("disable_persistent_config", true, "Can be set to false to disable persistent config handling, and read all configuration from a legacy config file.")

	// TODO: CBG-1542 Merge legacyFlagStartupConfig onto default config before merging others.
	legacyFlagStartupConfig := registerLegacyFlags(fs)
	_ = legacyFlagStartupConfig

	// register config property flags
	var flagStartupConfig StartupConfig
	// TODO: CBG-1542 Revisit config cli flags after initial persistent config implementation
	// if err := clistruct.RegisterJSONFlags(fs, &flagStartupConfig); err != nil {
	// 	return err
	// }

	if err := fs.Parse(osArgs[1:]); err != nil {
		// Return nil for ErrHelp so the shell exit code is 0
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}

	if *disablePersistentConfigFlag {
		return legacyServerMain(osArgs)
	}

	return serverMainPersistentConfig(fs, &flagStartupConfig)
}

// serverMainPersistentConfig runs the Sync Gateway server with persistent config.
func serverMainPersistentConfig(fs *flag.FlagSet, flagStartupConfig *StartupConfig) error {

	sc := DefaultStartupConfig(defaultLogFilePath)
	base.Tracef(base.KeyAll, "default config: %#v", sc)

	configPath := fs.Args()
	if len(configPath) > 1 {
		return fmt.Errorf("%d startup configs defined. Must be at most one startup config: %v", len(configPath), configPath)
	}

	if len(configPath) == 1 {
		fileStartupConfig, err := LoadStartupConfigFromPath(configPath[0])
		if pkgerrors.Cause(err) == base.ErrUnknownField {
			// TODO: CBG-1399 Do automatic legacy config upgrade here
			return fmt.Errorf("Couldn't parse config file: %w (legacy config upgrade not yet implemented)", err)
		}
		if err != nil {
			return fmt.Errorf("Couldn't open config file: %w", err)
		}
		if fileStartupConfig != nil {
			redactedConfig, err := sc.Redacted()
			if err != nil {
				return err
			}
			base.Tracef(base.KeyAll, "got config from file: %#v", redactedConfig)
			err = mergo.Merge(&sc, fileStartupConfig, mergo.WithOverride)
			if err != nil {
				return err
			}
		}
	}

	// merge flagStartupConfig on top of fileStartupConfig, because flags take precedence over config files.
	if flagStartupConfig != nil {
		base.Tracef(base.KeyAll, "got config from flags: %#v", flagStartupConfig)
		err := mergo.Merge(&sc, flagStartupConfig, mergo.WithOverride)
		if err != nil {
			return err
		}
	}

	redactedConfig, err := sc.Redacted()
	if err != nil {
		return err
	}
	base.Tracef(base.KeyAll, "final config: %#v", redactedConfig)

	base.Infof(base.KeyAll, "Config: Starting in persistent mode using config group %q", sc.Bootstrap.ConfigGroupID)
	ctx, err := setupServerContext(&sc, true)
	if err != nil {
		return err
	}

	return startServer(&sc, ctx)
}
