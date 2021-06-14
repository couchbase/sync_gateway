package rest

import (
	"context"
	"flag"
	"os"

	"github.com/couchbase/sync_gateway/base"
)

// ServerMain is the main entry point of launching the Sync Gateway server; the main
// function directly calls this. It registers both signal and fatal panic handlers,
// does the initial setup and finally starts the server.
func ServerMain() {
	if err := serverMain(context.Background(), os.Args); err != nil {
		base.Fatalf(err.Error())
	}
}

// TODO: Pass ctx down into HTTP servers so that serverMain can be stopped.
func serverMain(ctx context.Context, osArgs []string) error {
	RegisterSignalHandler()
	defer base.FatalPanicHandler()

	base.InitializeLoggers()

	// We can log version here because for console we have initialized an early logger in init() and for file loggers we
	// have the memory buffers.
	base.LogSyncGatewayVersion()

	fs := flag.NewFlagSet(osArgs[0], flag.ContinueOnError)

	// used by service scripts as a way to specify a per-distro defaultLogFilePath
	defaultLogFilePath = *fs.String("defaultLogFilePath", "", "Path to log files, if not overridden by --logFilePath, or the config")

	// TODO: Change default when we're ready to enable 3.0/bootstrap/persistent config by default (once QE's existing tests are ready to handle it)
	persistentConfigFlag := fs.Bool("persistent_config", false, "Can be set to false to disable persistent config handling, and read all configuration from a legacy config file.")

	// TODO: Merge legacyFlagStartupConfig onto default config before merging others.
	legacyFlagStartupConfig := registerLegacyFlags(fs)
	_ = legacyFlagStartupConfig

	// register config property flags
	var flagStartupConfig StartupConfig
	// TODO: Revisit config cli flags after initial persistent config implementation
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

	if !*persistentConfigFlag {
		return legacyServerMain(osArgs)
	}

	return serverMainPersistentConfig(osArgs, fs, &flagStartupConfig)
}
