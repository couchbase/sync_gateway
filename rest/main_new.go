package rest

import (
	"flag"
	"fmt"

	"github.com/couchbase/sync_gateway/base"
	"github.com/imdario/mergo"
	pkgerrors "github.com/pkg/errors"
)

// serverMainPersistentConfig runs the Sync Gateway server with persistent config.
func serverMainPersistentConfig(fs *flag.FlagSet, flagStartupConfig *StartupConfig) error {

	// 3.0 config - bootstrap and pull configs from server buckets.
	base.Infof(base.KeyAll, "Running in persistent config mode")

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

	ctx, err := setupServerContext(&sc, true)
	if err != nil {
		return err
	}

	return startServer(&sc, ctx)
}
