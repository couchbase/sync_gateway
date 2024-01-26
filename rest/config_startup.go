// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"context"
	"os"
	"runtime"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
)

const (
	// PersistentConfigDefaultGroupID is used when no explicit config Group ID is defined.
	PersistentConfigDefaultGroupID   = "default"
	persistentConfigGroupIDMaxLength = 100
	// persistentConfigDefaultUpdateFrequency is a duration that defines how frequent configs are refreshed from Couchbase Server.
	persistentConfigDefaultUpdateFrequency = time.Second * 10
)

// DefaultStartupConfig returns a StartupConfig with values populated with defaults.
func DefaultStartupConfig(defaultLogFilePath string) StartupConfig {
	return StartupConfig{
		Bootstrap: BootstrapConfig{
			ConfigGroupID:         PersistentConfigDefaultGroupID,
			ConfigUpdateFrequency: base.NewConfigDuration(persistentConfigDefaultUpdateFrequency),
			ServerTLSSkipVerify:   base.BoolPtr(false),
			UseTLSServer:          base.BoolPtr(DefaultUseTLSServer),
		},
		API: APIConfig{
			PublicInterface:    DefaultPublicInterface,
			AdminInterface:     DefaultAdminInterface,
			MetricsInterface:   DefaultMetricsInterface,
			MaximumConnections: DefaultMaxIncomingConnections,
			CompressResponses:  base.BoolPtr(true),
			HTTPS: HTTPSConfig{
				TLSMinimumVersion: "tlsv1.2",
			},
			ReadHeaderTimeout:                         base.NewConfigDuration(base.DefaultReadHeaderTimeout),
			IdleTimeout:                               base.NewConfigDuration(base.DefaultIdleTimeout),
			AdminInterfaceAuthentication:              base.BoolPtr(true),
			MetricsInterfaceAuthentication:            base.BoolPtr(true),
			EnableAdminAuthenticationPermissionsCheck: base.BoolPtr(base.IsEnterpriseEdition()),
		},
		Logging: base.LoggingConfig{
			LogFilePath:    defaultLogFilePath,
			RedactionLevel: base.DefaultRedactionLevel,
		},
		Auth: AuthConfig{
			BcryptCost: auth.DefaultBcryptCost,
		},
		Unsupported: UnsupportedConfig{
			StatsLogFrequency: base.NewConfigDuration(time.Minute),
			Serverless: ServerlessConfig{
				Enabled:                base.BoolPtr(false),
				MinConfigFetchInterval: base.NewConfigDuration(DefaultMinConfigFetchInterval),
			},
			AllowDbConfigEnvVars: base.BoolPtr(true),
			DiagnosticInterface:  DefaultDiagnosticInterface, // Disabled by default
		},
		MaxFileDescriptors: DefaultMaxFileDescriptors,
	}
}

// StartupConfig is the config file used by Sync Gateway in 3.0+ to start up with node-specific settings, and then bootstrap databases via Couchbase Server.
type StartupConfig struct {
	Bootstrap   BootstrapConfig    `json:"bootstrap,omitempty"`
	API         APIConfig          `json:"api,omitempty"`
	Logging     base.LoggingConfig `json:"logging,omitempty"`
	Auth        AuthConfig         `json:"auth,omitempty"`
	Replicator  ReplicatorConfig   `json:"replicator,omitempty"`
	Unsupported UnsupportedConfig  `json:"unsupported,omitempty"`

	DatabaseCredentials PerDatabaseCredentialsConfig    `json:"database_credentials,omitempty" help:"A map of database name to credentials, that can be used instead of the bootstrap ones. This will override bucket_credentials that target the bucket that the database is in."`
	BucketCredentials   base.PerBucketCredentialsConfig `json:"bucket_credentials,omitempty" help:"A map of bucket names to credentials, that can be used instead of the bootstrap ones."`

	MaxFileDescriptors         uint64 `json:"max_file_descriptors,omitempty" help:"Max # of open file descriptors (RLIMIT_NOFILE)"`
	CouchbaseKeepaliveInterval *int   `json:"couchbase_keepalive_interval,omitempty" help:"TCP keep-alive interval between SG and Couchbase server"`

	DeprecatedConfig *DeprecatedConfig `json:"-,omitempty" help:"Deprecated options that can be set from a legacy config upgrade, but cannot be set from a 3.0 config."`
}

// BootstrapConfig describes the set of properties required in order to bootstrap config from Couchbase Server.
type BootstrapConfig struct {
	ConfigGroupID         string               `json:"group_id,omitempty"                help:"The config group ID to use when discovering databases. Allows for non-homogenous configuration"`
	ConfigUpdateFrequency *base.ConfigDuration `json:"config_update_frequency,omitempty" help:"How often to poll Couchbase Server for new config changes. Default: 10s"`
	Server                string               `json:"server,omitempty"                  help:"Couchbase Server connection string/URL"`
	Username              string               `json:"username,omitempty"                help:"Username for authenticating to server"`
	Password              string               `json:"password,omitempty"                help:"Password for authenticating to server"`
	CACertPath            string               `json:"ca_cert_path,omitempty"            help:"Root CA cert path for TLS connection"`
	ServerTLSSkipVerify   *bool                `json:"server_tls_skip_verify,omitempty"  help:"Allow empty server CA Cert Path without attempting to use system root pool"`
	X509CertPath          string               `json:"x509_cert_path,omitempty"          help:"Cert path (public key) for X.509 bucket auth"`
	X509KeyPath           string               `json:"x509_key_path,omitempty"           help:"Key path (private key) for X.509 bucket auth"`
	UseTLSServer          *bool                `json:"use_tls_server,omitempty"          help:"Enforces a secure or non-secure server scheme"`
}

type APIConfig struct {
	PublicInterface  string `json:"public_interface,omitempty"  help:"Network interface to bind public API to"`
	AdminInterface   string `json:"admin_interface,omitempty"   help:"Network interface to bind admin API to"`
	MetricsInterface string `json:"metrics_interface,omitempty" help:"Network interface to bind metrics API to"`
	ProfileInterface string `json:"profile_interface,omitempty" help:"Network interface to bind profiling API to"`

	AdminInterfaceAuthentication   *bool `json:"admin_interface_authentication,omitempty" help:"Whether the admin API requires authentication"`
	MetricsInterfaceAuthentication *bool `json:"metrics_interface_authentication,omitempty" help:"Whether the metrics API requires authentication"`

	EnableAdminAuthenticationPermissionsCheck *bool `json:"enable_advanced_auth_dp,omitempty" help:"Whether to enable the DP permissions check feature of admin auth"`

	ServerReadTimeout  *base.ConfigDuration `json:"server_read_timeout,omitempty"  help:"Maximum duration.Second before timing out read of the HTTP(S) request"`
	ServerWriteTimeout *base.ConfigDuration `json:"server_write_timeout,omitempty" help:"Maximum duration.Second before timing out write of the HTTP(S) response"`
	ReadHeaderTimeout  *base.ConfigDuration `json:"read_header_timeout,omitempty"  help:"The amount of time allowed to read request headers"`
	IdleTimeout        *base.ConfigDuration `json:"idle_timeout,omitempty"         help:"The maximum amount of time to wait for the next request when keep-alives are enabled"`

	Pretty             *bool `json:"pretty,omitempty"               help:"Pretty-print JSON responses"`
	MaximumConnections uint  `json:"max_connections,omitempty"      help:"Max # of incoming HTTP connections to accept"`
	CompressResponses  *bool `json:"compress_responses,omitempty"   help:"If false, disables compression of HTTP responses"`
	HideProductVersion *bool `json:"hide_product_version,omitempty" help:"Whether product versions removed from Server headers and REST API responses"`

	HTTPS HTTPSConfig      `json:"https,omitempty"`
	CORS  *auth.CORSConfig `json:"cors,omitempty"`
}

type HTTPSConfig struct {
	TLSMinimumVersion string `json:"tls_minimum_version,omitempty" help:"The minimum allowable TLS version for the REST APIs"`
	TLSCertPath       string `json:"tls_cert_path,omitempty"       help:"The TLS cert file to use for the REST APIs"`
	TLSKeyPath        string `json:"tls_key_path,omitempty"        help:"The TLS key file to use for the REST APIs"`
}

type AuthConfig struct {
	BcryptCost int `json:"bcrypt_cost,omitempty"          help:"Cost to use for bcrypt password hashes"`
}

type ReplicatorConfig struct {
	MaxHeartbeat              *base.ConfigDuration `json:"max_heartbeat,omitempty"    help:"Max heartbeat value for _changes request"`
	BLIPCompression           *int                 `json:"blip_compression,omitempty" help:"BLIP data compression level (0-9)"`
	MaxConcurrentReplications int                  `json:"max_concurrent_replications,omitempty" help:"Maximum number of replication connections to the node"`
}

type UnsupportedConfig struct {
	StatsLogFrequency    *base.ConfigDuration `json:"stats_log_frequency,omitempty"    help:"How often should stats be written to stats logs"`
	UseStdlibJSON        *bool                `json:"use_stdlib_json,omitempty"        help:"Bypass the jsoniter package and use Go's stdlib instead"`
	Serverless           ServerlessConfig     `json:"serverless,omitempty"`
	HTTP2                *HTTP2Config         `json:"http2,omitempty"`
	UserQueries          *bool                `json:"user_queries,omitempty"            help:"Feature flag for user N1QL/JS/GraphQL queries"`
	UseXattrConfig       *bool                `json:"use_xattr_config,omitempty"        help:"Store database configurations in system xattrs"`
	AllowDbConfigEnvVars *bool                `json:"allow_dbconfig_env_vars,omitempty" help:"Can be set to false to skip environment variable expansion in database configs"`
	DiagnosticInterface  string               `json:"diagnostic_interface,omitempty" help:"Network interface to bind diagnostic API to"`
}

type ServerlessConfig struct {
	Enabled                *bool                `json:"enabled,omitempty" help:"Enable Sync Gateway serverless mode."`
	MinConfigFetchInterval *base.ConfigDuration `json:"min_config_fetch_interval,omitempty" help:"How long to cache configs fetched from the buckets for. This cache is used for requested databases that SG does not know about."`
}

type HTTP2Config struct {
	Enabled *bool `json:"enabled,omitempty" help:"Whether HTTP2 support is enabled"`
}

type PerDatabaseCredentialsConfig map[string]*base.CredentialsConfig

type DeprecatedConfig struct {
	Facebook *FacebookConfigLegacy `json:"-" help:""`
	Google   *GoogleConfigLegacy   `json:"-" help:""`
}

func (sc *StartupConfig) Redacted() (*StartupConfig, error) {
	var config StartupConfig

	err := base.DeepCopyInefficient(&config, sc)
	if err != nil {
		return nil, err
	}

	if config.Bootstrap.Password != "" {
		config.Bootstrap.Password = base.RedactedStr
	}

	for _, credentialsConfig := range config.DatabaseCredentials {
		if credentialsConfig != nil && credentialsConfig.Password != "" {
			credentialsConfig.Password = base.RedactedStr
		}
	}

	for _, credentialsConfig := range config.BucketCredentials {
		if credentialsConfig != nil && credentialsConfig.Password != "" {
			credentialsConfig.Password = base.RedactedStr
		}
	}

	return &config, nil
}

func (sc *StartupConfig) IsServerless() bool {
	return base.BoolDefault(sc.Unsupported.Serverless.Enabled, false)
}

func LoadStartupConfigFromPath(ctx context.Context, path string) (*StartupConfig, error) {
	rc, err := readFromPath(ctx, path, false)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rc.Close() }()

	var sc StartupConfig
	err = DecodeAndSanitiseConfig(ctx, rc, &sc, true)
	return &sc, err
}

// NewEmptyStartupConfig initialises an empty StartupConfig with all *struct fields empty
func NewEmptyStartupConfig() StartupConfig {
	return StartupConfig{
		API: APIConfig{
			CORS: &auth.CORSConfig{},
		},
		Logging: base.LoggingConfig{
			Console: &base.ConsoleLoggerConfig{},
			Error:   &base.FileLoggerConfig{},
			Warn:    &base.FileLoggerConfig{},
			Info:    &base.FileLoggerConfig{},
			Debug:   &base.FileLoggerConfig{},
			Trace:   &base.FileLoggerConfig{},
			Stats:   &base.FileLoggerConfig{},
		},
		Unsupported: UnsupportedConfig{
			HTTP2: &HTTP2Config{},
		},
	}
}

// setGlobalConfig will set global variables and other settings based on the given StartupConfig.
// We should try to keep these minimal where possible, and favour ServerContext-scoped values.
func setGlobalConfig(ctx context.Context, sc *StartupConfig) error {

	// Per-process limits, can't be scoped any narrower.
	if os.Getenv("GOMAXPROCS") == "" && runtime.GOMAXPROCS(0) == 1 {
		// TODO: As of Go 1.5, the runtime automatically increases GOMAXPROCS to match the number of CPUs, all of this seems unnecessary.
		cpus := runtime.NumCPU()
		if cpus > 1 {
			runtime.GOMAXPROCS(cpus)
			base.InfofCtx(ctx, base.KeyAll, "Configured Go to use all %d CPUs; setenv GOMAXPROCS to override this", cpus)
		}
	}

	if _, err := base.SetMaxFileDescriptors(ctx, sc.MaxFileDescriptors); err != nil {
		base.ErrorfCtx(ctx, "Error setting MaxFileDescriptors to %d: %v", sc.MaxFileDescriptors, err)
	}

	// Given unscoped usage of base.JSON functions, this can't be scoped.
	if base.BoolDefault(sc.Unsupported.UseStdlibJSON, false) {
		base.InfofCtx(ctx, base.KeyAll, "Using the stdlib JSON package")
		base.UseStdlibJSON = true
	}

	return nil
}

// Merge applies non-empty fields from new onto non-empty fields on sc
func (sc *StartupConfig) Merge(new *StartupConfig) error {
	return base.ConfigMerge(sc, new)
}
