package rest

import (
	"flag"
	"fmt"

	"github.com/couchbase/sync_gateway/base"
	"github.com/imdario/mergo"
	pkgerrors "github.com/pkg/errors"
)

// serverMainPersistentConfig runs the Sync Gateway server with persistent config.
func serverMainPersistentConfig(osArgs []string, fs *flag.FlagSet, flagStartupConfig *StartupConfig) error {

	// 3.0 config - bootstrap and pull configs from server buckets.
	base.Infof(base.KeyAll, "Running in persistent config mode")

	sc := DefaultStartupConfig(defaultLogFilePath)
	base.Tracef(base.KeyAll, "default config: %#v", sc)

	fileStartupConfig, err := LoadStartupConfigFromPaths(fs.Args()...)
	if pkgerrors.Cause(err) == base.ErrUnknownField {
		base.Warnf("Couldn't parse config file as startup config - trying as legacy config: %v", err)
		if legacyErr := legacyServerMain(osArgs); legacyErr != nil {
			base.Warnf("Couldn't parse config file as legacy config: %v", legacyErr)

			// return the original 3.0 config load error if we still can't do that
			return fmt.Errorf("Couldn't load config file: %w", err)
		}
		// TODO: CBG-1399 Do automatic config upograde
	} else if err != nil {
		return fmt.Errorf("Couldn't open config file: %w", err)
	}

	if fileStartupConfig != nil {
		base.Tracef(base.KeyAll, "got config from file: %#v", fileStartupConfig)
		err := mergo.Merge(&sc, fileStartupConfig, mergo.WithOverride)
		if err != nil {
			return err
		}
	}

	if flagStartupConfig != nil {
		base.Tracef(base.KeyAll, "got config from flags: %#v", flagStartupConfig)
		err := mergo.Merge(&sc, flagStartupConfig, mergo.WithOverride)
		if err != nil {
			return err
		}
	}

	base.Debugf(base.KeyAll, "final config: %#v", sc)

	ctx, err := setupServerContext(&sc, true)
	if err != nil {
		return err
	}

	return startServer(&sc, ctx)
}
