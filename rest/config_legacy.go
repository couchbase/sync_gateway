package rest

import (
	"errors"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

type LegacyConfig struct {
	PersistentConfig *bool `json:"persistent_config,omitempty" help:"Can be set to false to disable 3.0/persistent config handling."`
	LegacyServerConfig
}

// ToStartupConfig returns the given LegacyConfig as a StartupConfig and a set of DBConfigs.
// The returned configs do not contain any default values - only a direct mapping of legacy config options as they were given.
func (lc *LegacyConfig) ToStartupConfig() (*StartupConfig, DbConfigMap, error) {

	// find a database's credentials for bootstrap (this isn't the first database config entry due to map iteration)
	var bsc *BootstrapConfig
	for _, dbConfig := range lc.Databases {
		if dbConfig.Server == nil || *dbConfig.Server == "" {
			continue
		}
		bsc = &BootstrapConfig{
			Server:     *dbConfig.Server,
			Username:   dbConfig.Username,
			Password:   dbConfig.Password,
			CertPath:   dbConfig.CertPath,
			KeyPath:    dbConfig.KeyPath,
			CACertPath: dbConfig.CACertPath,
		}
		break
	}

	if bsc == nil {
		return nil, nil, errors.New("couldn't upgrade legacy config without at least one server")
	}

	sc := StartupConfig{
		Bootstrap: *bsc,
		API: APIConfig{
			Pretty:             lc.Pretty,
			CompressResponses:  lc.CompressResponses,
			HideProductVersion: lc.HideProductVersion,
		},
		Logging: LoggingConfig2{},
		Replicator: ReplicatorConfig{
			MaxHeartbeat:    time.Second * time.Duration(lc.MaxHeartbeat),
			BLIPCompression: lc.ReplicatorCompression,
		},
		BcryptCost: lc.BcryptCost,
	}

	if lc.Unsupported != nil {
		if lc.Unsupported.Http2Config != nil {
			sc.Unsupported.HTTP2 = &HTTP2Config{
				Enabled: lc.Unsupported.Http2Config.Enabled,
			}
		}
	}

	if lc.Logging != nil {
		sc.Logging.LogFilePath = lc.Logging.LogFilePath
		sc.Logging.RedactionLevel = lc.Logging.RedactionLevel
		sc.Logging.Console = &lc.Logging.Console
		sc.Logging.Error = &lc.Logging.Error
		sc.Logging.Warn = &lc.Logging.Warn
		sc.Logging.Info = &lc.Logging.Info
		sc.Logging.Debug = &lc.Logging.Debug
		sc.Logging.Trace = &lc.Logging.Trace
		sc.Logging.Stats = &lc.Logging.Stats
	}

	if lc.Facebook != nil {
		sc.Auth.Facebook = &FacebookConfig2{
			Register: lc.Facebook.Register,
		}
	}

	if lc.Google != nil {
		sc.Auth.Google = &GoogleConfig2{
			Register:    lc.Google.Register,
			AppClientID: lc.Google.AppClientID,
		}
	}

	if lc.CORS != nil {
		sc.API.CORS = lc.CORS
	}

	if lc.Interface != nil {
		sc.API.PublicInterface = *lc.Interface
	}
	if lc.AdminInterface != nil {
		sc.API.AdminInterface = *lc.AdminInterface
	}
	if lc.MetricsInterface != nil {
		sc.API.MetricsInterface = *lc.MetricsInterface
	}
	if lc.ProfileInterface != nil {
		sc.API.ProfileInterface = *lc.ProfileInterface
	}
	if lc.ServerReadTimeout != nil {
		sc.API.ServerReadTimeout = time.Duration(*lc.ServerReadTimeout) * time.Second
	}
	if lc.ServerWriteTimeout != nil {
		sc.API.ServerWriteTimeout = time.Duration(*lc.ServerWriteTimeout) * time.Second
	}
	if lc.ReadHeaderTimeout != nil {
		sc.API.ReadHeaderTimeout = time.Duration(*lc.ReadHeaderTimeout) * time.Second
	}
	if lc.IdleTimeout != nil {
		sc.API.IdleTimeout = time.Duration(*lc.IdleTimeout) * time.Second
	}
	if lc.MaxIncomingConnections != nil {
		sc.API.MaximumConnections = uint(*lc.MaxIncomingConnections)
	}
	if lc.TLSMinVersion != nil {
		sc.API.TLS.MinimumVersion = *lc.TLSMinVersion
	}
	if lc.SSLCert != nil {
		sc.API.TLS.CertPath = *lc.SSLCert
	}
	if lc.SSLKey != nil {
		sc.API.TLS.KeyPath = *lc.SSLKey
	}
	if lc.Unsupported.StatsLogFrequencySecs != nil {
		sc.Unsupported.StatsLogFrequency = base.DurationPtr(time.Second * time.Duration(*lc.Unsupported.StatsLogFrequencySecs))
	}
	if lc.Unsupported.UseStdlibJSON != nil {
		sc.Unsupported.UseStdlibJSON = *lc.Unsupported.UseStdlibJSON
	}
	if lc.MaxFileDescriptors != nil {
		sc.MaxFileDescriptors = *lc.MaxFileDescriptors
	}

	// TODO: Translate database configs too?
	dbs := lc.Databases

	return &sc, dbs, nil
}
