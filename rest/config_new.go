package rest

import (
	"os"
	"runtime"
	"time"

	"github.com/imdario/mergo"
	"golang.org/x/crypto/bcrypt"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
)

const (
	// persistentConfigDefaultGroupID is used when no explicit config Group ID is defined.
	persistentConfigDefaultGroupID = "default"
	// persistentConfigDefaultUpdateFrequency is a duration that defines how frequent configs are refreshed from Couchbase Server.
	persistentConfigDefaultUpdateFrequency = time.Second * 10

	// persistentConfigDocIDPrefix is the prefix used to store a persistent config. The rest of the doc ID is the group name.
	persistentConfigDocIDPrefix = base.SyncPrefix + "cnf:"
)

// DefaultStartupConfig returns a StartupConfig with values populated with defaults.
func DefaultStartupConfig(defaultLogFilePath string) StartupConfig {
	return StartupConfig{
		Bootstrap: BootstrapConfig{
			ConfigGroupID:         persistentConfigDefaultGroupID,
			ConfigUpdateFrequency: persistentConfigDefaultUpdateFrequency,
		},
		API: APIConfig{
			PublicInterface:    DefaultPublicInterface,
			AdminInterface:     DefaultAdminInterface,
			MetricsInterface:   DefaultMetricsInterface,
			MaximumConnections: DefaultMaxIncomingConnections,
			CompressResponses:  base.BoolPtr(true),
			TLS: TLSConfig{
				MinimumVersion: "tlsv1.2",
			},
			ReadHeaderTimeout: base.DefaultReadHeaderTimeout,
			IdleTimeout:       base.DefaultIdleTimeout,
		},
		// TODO: logging defaults
		Logging: LoggingConfig{
			LogFilePath:    defaultLogFilePath,
			RedactionLevel: base.DefaultRedactionLevel,
			Console: &base.ConsoleLoggerConfig{
				LogLevel: base.LogLevelPtr(base.LevelNone),
			},
			Error: &base.FileLoggerConfig{},
			Warn:  &base.FileLoggerConfig{},
			Info:  &base.FileLoggerConfig{},
			Debug: &base.FileLoggerConfig{},
			Trace: &base.FileLoggerConfig{},
			Stats: &base.FileLoggerConfig{},
		},
		Unsupported: UnsupportedConfig{
			StatsLogFrequency: base.DurationPtr(time.Minute),
		},
		BcryptCost:         bcrypt.DefaultCost,
		MaxFileDescriptors: DefaultMaxFileDescriptors,
	}
}

func (sc *StartupConfig) Redacted() (*StartupConfig, error) {
	var config StartupConfig

	err := base.DeepCopyInefficient(&config, sc)
	if err != nil {
		return nil, err
	}

	config.Bootstrap.Password = "xxxxx"

	return &config, nil
}

// StartupConfig is the config file used by Sync Gateway in 3.0+ to start up with node-specific settings, and then bootstrap databases via Couchbase Server.
type StartupConfig struct {
	Bootstrap   BootstrapConfig   `json:"bootstrap,omitempty"`
	API         APIConfig         `json:"api,omitempty"`
	Logging     LoggingConfig     `json:"logging,omitempty"`
	Auth        AuthConfig        `json:"auth,omitempty"`
	Replicator  ReplicatorConfig  `json:"replicator,omitempty"`
	Unsupported UnsupportedConfig `json:"unsupported,omitempty"`

	BcryptCost         int    `json:"bcrypt_cost,omitempty"          help:"Cost to use for bcrypt password hashes"`
	MaxFileDescriptors uint64 `json:"max_file_descriptors,omitempty" help:"Max # of open file descriptors (RLIMIT_NOFILE)"`
}

// BootstrapConfig describes the set of properties required in order to bootstrap config from Couchbase Server.
type BootstrapConfig struct {
	ConfigGroupID         string        `json:"group_id,omitempty"                help:"The config group ID to use when discovering databases. Allows for non-homogenous configuration"`
	ConfigUpdateFrequency time.Duration `json:"config_update_frequency,omitempty" help:"How often to poll Couchbase Server for new config changes. Default: 10s"`
	Server                string        `json:"server,omitempty"                  help:"Couchbase Server connection string/URL"`
	Username              string        `json:"username,omitempty"                help:"Username for authenticating to server"`
	Password              string        `json:"password,omitempty"                help:"Password for authenticating to server"`
	CertPath              string        `json:"cert_path,omitempty"               help:"Cert path (public key) for X.509 bucket auth"`
	KeyPath               string        `json:"key_path,omitempty"                help:"Key path (private key) for X.509 bucket auth"`
	CACertPath            string        `json:"ca_cert_path,omitempty"            help:"Root CA cert path for X.509 bucket auth"`
}

type APIConfig struct {
	PublicInterface  string `json:"public_interface,omitempty"  help:"Network interface to bind public API to"`
	AdminInterface   string `json:"admin_interface,omitempty"   help:"Network interface to bind admin API to"`
	MetricsInterface string `json:"metrics_interface,omitempty" help:"Network interface to bind metrics API to"`
	ProfileInterface string `json:"profile_interface,omitempty" help:"Network interface to bind profiling API to"`

	ServerReadTimeout  time.Duration `json:"server_read_timeout,omitempty"  help:"maximum duration.Second before timing out read of the HTTP(S) request"`
	ServerWriteTimeout time.Duration `json:"server_write_timeout,omitempty" help:"maximum duration.Second before timing out write of the HTTP(S) response"`
	ReadHeaderTimeout  time.Duration `json:"read_header_timeout,omitempty"  help:"The amount of time allowed to read request headers"`
	IdleTimeout        time.Duration `json:"idle_timeout,omitempty"         help:"The maximum amount of time to wait for the next request when keep-alives are enabled"`

	Pretty             bool  `json:"pretty,omitempty"               help:"Pretty-print JSON responses"`
	MaximumConnections uint  `json:"max_connections,omitempty"      help:"Max # of incoming HTTP connections to accept"`
	CompressResponses  *bool `json:"compress_responses,omitempty"   help:"If false, disables compression of HTTP responses"`
	HideProductVersion bool  `json:"hide_product_version,omitempty" help:"Whether product versions removed from Server headers and REST API responses"`

	TLS  TLSConfig   `json:"tls,omitempty"`
	CORS *CORSConfig `json:"cors,omitempty"`
}

type TLSConfig struct {
	MinimumVersion string `json:"minimum_version,omitempty"     help:"The minimum allowable TLS version for the REST APIs"`
	CertPath       string `json:"cert_path,omitempty" help:"The TLS cert file to use for the REST APIs"`
	KeyPath        string `json:"key_path,omitempty"  help:"The TLS key file to use for the REST APIs"`
}

type CORSConfig struct {
	Origin      []string `json:"origin,omitempty"       help:"List of allowed origins, use ['*'] to allow access from everywhere"`
	LoginOrigin []string `json:"login_origin,omitempty" help:"List of allowed login origins"`
	Headers     []string `json:"headers,omitempty"      help:"List of allowed headers"`
	MaxAge      int      `json:"max_age,omitempty"      help:"Maximum age of the CORS Options request"`
}

type LoggingConfig struct {
	LogFilePath    string                    `json:"log_file_path,omitempty"   help:"Absolute or relative path on the filesystem to the log file directory. A relative path is from the directory that contains the Sync Gateway executable file"`
	RedactionLevel base.RedactionLevel       `json:"redaction_level,omitempty" help:"Redaction level to apply to log output"`
	Console        *base.ConsoleLoggerConfig `json:"console,omitempty"`
	Error          *base.FileLoggerConfig    `json:"error,omitempty"`
	Warn           *base.FileLoggerConfig    `json:"warn,omitempty"`
	Info           *base.FileLoggerConfig    `json:"info,omitempty"`
	Debug          *base.FileLoggerConfig    `json:"debug,omitempty"`
	Trace          *base.FileLoggerConfig    `json:"trace,omitempty"`
	Stats          *base.FileLoggerConfig    `json:"stats,omitempty"`
}

type AuthConfig struct {
	Facebook *FacebookConfig `json:"facebook,omitempty"`
	Google   *GoogleConfig   `json:"google,omitempty"`
}

type FacebookConfig struct {
	Register bool `json:"register,omitempty" help:"If true, server will register new user accounts"`
}

type GoogleConfig struct {
	Register    bool     `json:"register,omitempty" help:"If true, server will register new user accounts"`
	AppClientID []string `json:"app_client_id,omitempty" help:"List of enabled client ids"`
}

type ReplicatorConfig struct {
	MaxHeartbeat    time.Duration `json:"max_heartbeat,omitempty"    help:"Max heartbeat value for _changes request"`
	BLIPCompression *int          `json:"blip_compression,omitempty" help:"BLIP data compression level (0-9)"`
}

type UnsupportedConfig struct {
	StatsLogFrequency *time.Duration `json:"stats_log_frequency,omitempty" help:"How often should stats be written to stats logs"`
	UseStdlibJSON     bool           `json:"use_stdlib_json,omitempty"     help:"Bypass the jsoniter package and use Go's stdlib instead"`

	HTTP2 *HTTP2Config `json:"http2,omitempty"`
}

type HTTP2Config struct {
	Enabled *bool `json:"enabled,omitempty" help:"Whether HTTP2 support is enabled"`
}

func LoadStartupConfigFromPath(path string) (*StartupConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var sc StartupConfig
	err = decodeAndSanitiseConfig(f, &sc)
	return &sc, err
}

func LoadStartupConfigFromPaths(paths ...string) (*StartupConfig, error) {
	if len(paths) == 0 {
		return nil, nil
	}

	sc := StartupConfig{}
	for _, path := range paths {
		pathSc, err := LoadStartupConfigFromPath(path)
		if err != nil {
			return nil, err
		}
		err = mergo.Merge(&sc, pathSc, mergo.WithOverride)
		if err != nil {
			return nil, err
		}
	}

	return &sc, nil
}

// setGlobalConfig will set global variables and other settings based on the given StartupConfig.
// We should try to keep these minimal where possible, and favour ServerContext-scoped values.
func setGlobalConfig(sc *StartupConfig) error {

	// Per-process limits, can't be scoped any narrower.
	if os.Getenv("GOMAXPROCS") == "" && runtime.GOMAXPROCS(0) == 1 {
		// TODO: As of Go 1.5, the runtime automatically increases GOMAXPROCS to match the number of CPUs, all of this seems unnecessary.
		cpus := runtime.NumCPU()
		if cpus > 1 {
			runtime.GOMAXPROCS(cpus)
			base.Infof(base.KeyAll, "Configured Go to use all %d CPUs; setenv GOMAXPROCS to override this", cpus)
		}
	}
	if _, err := base.SetMaxFileDescriptors(sc.MaxFileDescriptors); err != nil {
		base.Errorf("Error setting MaxFileDescriptors to %d: %v", sc.MaxFileDescriptors, err)
	}

	// Given unscoped usage of base.JSON functions, this can't be scoped.
	if sc.Unsupported.UseStdlibJSON {
		base.Infof(base.KeyAll, "Using the stdlib JSON package")
		base.UseStdlibJSON = true
	}

	// TODO: Move to be scoped under auth.Authenticator ?
	if err := auth.SetBcryptCost(sc.BcryptCost); err != nil {
		return err
	}

	return nil
}
