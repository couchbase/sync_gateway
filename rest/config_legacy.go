package rest

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/hashicorp/go-multierror"
	pkgerrors "github.com/pkg/errors"
)

// JSON object that defines the server configuration.
type LegacyServerConfig struct {
	TLSMinVersion              *string                        `json:"tls_minimum_version,omitempty"`    // Set TLS Version
	Interface                  *string                        `json:",omitempty"`                       // Interface to bind REST API to, default ":4984"
	SSLCert                    *string                        `json:",omitempty"`                       // Path to SSL cert file, or nil
	SSLKey                     *string                        `json:",omitempty"`                       // Path to SSL private key file, or nil
	ServerReadTimeout          *int                           `json:",omitempty"`                       // maximum duration.Second before timing out read of the HTTP(S) request
	ServerWriteTimeout         *int                           `json:",omitempty"`                       // maximum duration.Second before timing out write of the HTTP(S) response
	ReadHeaderTimeout          *int                           `json:",omitempty"`                       // The amount of time allowed to read request headers.
	IdleTimeout                *int                           `json:",omitempty"`                       // The maximum amount of time to wait for the next request when keep-alives are enabled.
	AdminInterface             *string                        `json:",omitempty"`                       // Interface to bind admin API to, default "localhost:4985"
	AdminUI                    *string                        `json:",omitempty"`                       // Path to Admin HTML page, if omitted uses bundled HTML
	ProfileInterface           *string                        `json:",omitempty"`                       // Interface to bind Go profile API to (no default)
	ConfigServer               *string                        `json:",omitempty"`                       // URL of config server (for dynamic db discovery)
	Facebook                   *FacebookConfigLegacy          `json:",omitempty"`                       // Configuration for Facebook validation
	Google                     *GoogleConfigLegacy            `json:",omitempty"`                       // Configuration for Google validation
	CORS                       *CORSConfigLegacy              `json:",omitempty"`                       // Configuration for allowing CORS
	DeprecatedLog              []string                       `json:"log,omitempty"`                    // Log keywords to enable
	DeprecatedLogFilePath      *string                        `json:"logFilePath,omitempty"`            // Path to log file, if missing write to stderr
	Logging                    *base.LegacyLoggingConfig      `json:",omitempty"`                       // Configuration for logging with optional log file rotation
	Pretty                     bool                           `json:",omitempty"`                       // Pretty-print JSON responses?
	DeploymentID               *string                        `json:",omitempty"`                       // Optional customer/deployment ID for stats reporting
	StatsReportInterval        *float64                       `json:",omitempty"`                       // Optional stats report interval (0 to disable)
	CouchbaseKeepaliveInterval *int                           `json:",omitempty"`                       // TCP keep-alive interval between SG and Couchbase server
	SlowQueryWarningThreshold  *int                           `json:",omitempty"`                       // Log warnings if N1QL queries take this many ms
	MaxIncomingConnections     *int                           `json:",omitempty"`                       // Max # of incoming HTTP connections to accept
	MaxFileDescriptors         *uint64                        `json:",omitempty"`                       // Max # of open file descriptors (RLIMIT_NOFILE)
	CompressResponses          *bool                          `json:",omitempty"`                       // If false, disables compression of HTTP responses
	Databases                  DbConfigMap                    `json:",omitempty"`                       // Pre-configured databases, mapped by name
	Replications               []*ReplicateV1ConfigLegacy     `json:",omitempty"`                       // sg-replicate replication definitions
	MaxHeartbeat               uint64                         `json:",omitempty"`                       // Max heartbeat value for _changes request (seconds)
	ClusterConfig              *ClusterConfigLegacy           `json:"cluster_config,omitempty"`         // Bucket and other config related to CBGT
	Unsupported                *UnsupportedServerConfigLegacy `json:"unsupported,omitempty"`            // Config for unsupported features
	ReplicatorCompression      *int                           `json:"replicator_compression,omitempty"` // BLIP data compression level (0-9)
	BcryptCost                 int                            `json:"bcrypt_cost,omitempty"`            // bcrypt cost to use for password hashes - Default: bcrypt.DefaultCost
	MetricsInterface           *string                        `json:"metricsInterface,omitempty"`       // Interface to bind metrics to. If not set then metrics isn't accessible
	HideProductVersion         bool                           `json:"hide_product_version,omitempty"`   // Determines whether product versions removed from Server headers and REST API responses. This setting does not apply to the Admin REST API.
	DisablePersistentConfig    *bool                          `json:"disable_persistent_config,omitempty" help:"Can be set to true to disable 3.0/persistent config handling."`
}

type FacebookConfigLegacy struct {
	Register bool // If true, server will register new user accounts
}

type GoogleConfigLegacy struct {
	Register    bool     // If true, server will register new user accounts
	AppClientID []string `json:"app_client_id"` // list of enabled client ids
}

type CORSConfigLegacy struct {
	Origin      []string // List of allowed origins, use ["*"] to allow access from everywhere
	LoginOrigin []string // List of allowed login origins
	Headers     []string // List of allowed headers
	MaxAge      int      // Maximum age of the CORS Options request
}

type ClusterConfigLegacy struct {
	BucketConfig
	DataDir                  string  `json:"data_dir,omitempty"`
	HeartbeatIntervalSeconds *uint16 `json:"heartbeat_interval_seconds,omitempty"`
}

func (c ClusterConfigLegacy) CBGTEnabled() bool {
	// if we have a non-empty server field, then assume CBGT is enabled.
	return c.Server != nil && *c.Server != ""
}

type UnsupportedServerConfigLegacy struct {
	Http2Config           *HTTP2Config `json:"http2,omitempty"`               // Config settings for HTTP2
	StatsLogFrequencySecs *uint        `json:"stats_log_freq_secs,omitempty"` // How often should stats be written to stats logs
	UseStdlibJSON         *bool        `json:"use_stdlib_json,omitempty"`     // Bypass the jsoniter package and use Go's stdlib instead
}

type ReplicateV1ConfigLegacy struct {
	Source           string      `json:"source"`
	Target           string      `json:"target"`
	Continuous       bool        `json:"continuous"`
	CreateTarget     bool        `json:"create_target"`
	DocIds           []string    `json:"doc_ids"`
	Filter           string      `json:"filter"`
	Proxy            string      `json:"proxy"`
	QueryParams      interface{} `json:"query_params"`
	Cancel           bool        `json:"cancel"`
	Async            bool        `json:"async"`
	ChangesFeedLimit *int        `json:"changes_feed_limit"`
	ReplicationId    string      `json:"replication_id"`
	upgradedToSGR2   bool        // upgradedToSGR2 is set to true when an equivalent SGR2 replication is found, which prevents this v1 replication from starting.
}

type ReplConfigMapLegacy map[string]*ReplicateV1ConfigLegacy

// ToStartupConfig returns the given LegacyServerConfig as a StartupConfig and a set of DBConfigs.
// The returned configs do not contain any default values - only a direct mapping of legacy config options as they were given.
func (lc *LegacyServerConfig) ToStartupConfig() (*StartupConfig, DbConfigMap, error) {

	// find a database's credentials for bootstrap (this isn't the first database config entry due to map iteration)
	bsc := &BootstrapConfig{}
	for _, dbConfig := range lc.Databases {
		if dbConfig.Server == nil || *dbConfig.Server == "" {
			continue
		}
		bsc = &BootstrapConfig{
			Server:       *dbConfig.Server,
			Username:     dbConfig.Username,
			Password:     dbConfig.Password,
			CACertPath:   dbConfig.CACertPath,
			X509CertPath: dbConfig.CertPath,
			X509KeyPath:  dbConfig.KeyPath,
		}
		break
	}

	sc := StartupConfig{
		Bootstrap: *bsc,
		API: APIConfig{
			Pretty:             lc.Pretty,
			CompressResponses:  lc.CompressResponses,
			HideProductVersion: lc.HideProductVersion,
		},
		Logging: LoggingConfig{},
		Auth: AuthConfig{
			BcryptCost: lc.BcryptCost,
		},
		Replicator: ReplicatorConfig{
			MaxHeartbeat:    base.ConfigDuration{Duration: time.Second * time.Duration(lc.MaxHeartbeat)},
			BLIPCompression: lc.ReplicatorCompression,
		},
	}

	if lc.Facebook != nil || lc.Google != nil {
		sc.DeprecatedConfig = &DeprecatedConfig{
			Facebook: lc.Facebook,
			Google:   lc.Google,
		}
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

	if lc.CORS != nil {
		sc.API.CORS = &CORSConfig{
			Origin:      lc.CORS.Origin,
			LoginOrigin: lc.CORS.LoginOrigin,
			Headers:     lc.CORS.Headers,
			MaxAge:      lc.CORS.MaxAge,
		}
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
		sc.API.ServerReadTimeout = base.NewConfigDuration(time.Duration(*lc.ServerReadTimeout) * time.Second)
	}
	if lc.ServerWriteTimeout != nil {
		sc.API.ServerWriteTimeout = base.NewConfigDuration(time.Duration(*lc.ServerWriteTimeout) * time.Second)
	}
	if lc.ReadHeaderTimeout != nil {
		sc.API.ReadHeaderTimeout = base.NewConfigDuration(time.Duration(*lc.ReadHeaderTimeout) * time.Second)
	}
	if lc.IdleTimeout != nil {
		sc.API.IdleTimeout = base.NewConfigDuration(time.Duration(*lc.IdleTimeout) * time.Second)
	}
	if lc.MaxIncomingConnections != nil {
		sc.API.MaximumConnections = uint(*lc.MaxIncomingConnections)
	}
	if lc.TLSMinVersion != nil {
		sc.API.HTTPS.TLSMinimumVersion = *lc.TLSMinVersion
	}
	if lc.SSLCert != nil {
		sc.API.HTTPS.TLSCertPath = *lc.SSLCert
	}
	if lc.SSLKey != nil {
		sc.API.HTTPS.TLSKeyPath = *lc.SSLKey
	}
	if lc.Unsupported != nil {
		if lc.Unsupported.StatsLogFrequencySecs != nil {
			sc.Unsupported.StatsLogFrequency = base.NewConfigDuration(time.Second * time.Duration(*lc.Unsupported.StatsLogFrequencySecs))
		}
		if lc.Unsupported.UseStdlibJSON != nil {
			sc.Unsupported.UseStdlibJSON = *lc.Unsupported.UseStdlibJSON
		}
	}
	if lc.MaxFileDescriptors != nil {
		sc.MaxFileDescriptors = *lc.MaxFileDescriptors
	}

	// TODO: Translate database configs too?
	dbs := lc.Databases

	return &sc, dbs, nil
}

// Implementation of AuthHandler interface for ClusterConfigLegacy
func (clusterConfig *ClusterConfigLegacy) GetCredentials() (string, string, string) {
	return base.TransformBucketCredentials(clusterConfig.Username, clusterConfig.Password, *clusterConfig.Bucket)
}

// LoadServerConfig loads a LegacyServerConfig from either a JSON file or from a URL
func LoadServerConfig(path string) (config *LegacyServerConfig, err error) {
	rc, err := readFromPath(path, false)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rc.Close() }()
	return readServerConfig(rc)
}

// readServerConfig returns a validated LegacyServerConfig from an io.Reader
func readServerConfig(r io.Reader) (config *LegacyServerConfig, err error) {
	err = decodeAndSanitiseConfig(r, &config)
	return config, err
}

// setupServerConfig parses command-line flags, reads the optional configuration file,
// performs the config validation and database setup.
func setupServerConfig(args []string) (config *LegacyServerConfig, err error) {
	var unknownFieldsErr error

	config, err = ParseCommandLine(args, flag.ExitOnError)
	if pkgerrors.Cause(err) == base.ErrUnknownField {
		unknownFieldsErr = err
	} else if err != nil {
		return nil, fmt.Errorf(err.Error())
	}

	// If we got an unknownFields error when reading the config
	// log and exit now we've tried setting up the logging.
	if unknownFieldsErr != nil {
		return nil, fmt.Errorf(unknownFieldsErr.Error())
	}

	// Validation
	var multiError *multierror.Error
	multiError = multierror.Append(multiError, config.validate())
	multiError = multierror.Append(multiError, config.setupAndValidateDatabases())
	if multiError.ErrorOrNil() != nil {
		base.Errorf("Error during config validation: %v", multiError)
		return nil, fmt.Errorf("error(s) during config validation: %v", multiError)
	}

	return config, nil
}

func (config *LegacyServerConfig) setupAndValidateDatabases() error {
	if config == nil {
		return nil
	}
	return config.Databases.SetupAndValidate()
}

// validate validates the given server config and returns all invalid options as a slice of errors
func (config *LegacyServerConfig) validate() (errorMessages error) {
	if config.Unsupported != nil && config.Unsupported.StatsLogFrequencySecs != nil {
		if *config.Unsupported.StatsLogFrequencySecs == 0 {
			// explicitly disabled
		} else if *config.Unsupported.StatsLogFrequencySecs < 10 {
			errorMessages = multierror.Append(errorMessages, fmt.Errorf(minValueErrorMsg,
				"unsupported.stats_log_freq_secs", 10))
		}
	}

	return errorMessages
}

// deprecatedConfigLoggingFallback will parse the LegacyServerConfig and try to
// use older logging config options for backwards compatibility.
// It will return a slice of deferred warnings to log at a later time.
func (config *LegacyServerConfig) deprecatedConfigLoggingFallback() {

	warningMsgFmt := "Using deprecated config option: %q. Use %q instead."

	if config.Logging.DeprecatedDefaultLog != nil {
		// Fall back to the old logging.["default"].LogFilePath option
		if config.Logging.LogFilePath == "" && config.Logging.DeprecatedDefaultLog.LogFilePath != nil {
			base.Warnf(warningMsgFmt, `logging.["default"].LogFilePath`, "logging.log_file_path")

			// Set the new LogFilePath to be the directory containing the old logfile, instead of the full path.
			// SGCollect relies on this path to pick up the standard and rotated log files.
			info, err := os.Stat(*config.Logging.DeprecatedDefaultLog.LogFilePath)
			if err == nil && info.IsDir() {
				config.Logging.LogFilePath = *config.Logging.DeprecatedDefaultLog.LogFilePath
			} else {
				config.Logging.LogFilePath = filepath.Dir(*config.Logging.DeprecatedDefaultLog.LogFilePath)
				base.Infof(base.KeyAll, "Using %v as log file path (parent directory of deprecated logging."+
					"[\"default\"].LogFilePath)", config.Logging.LogFilePath)
			}
		}

		// Fall back to the old logging.["default"].LogKeys option
		if len(config.Logging.Console.LogKeys) == 0 && len(config.Logging.DeprecatedDefaultLog.LogKeys) > 0 {
			base.Warnf(warningMsgFmt, `logging.["default"].LogKeys`, "logging.console.log_keys")
			config.Logging.Console.LogKeys = config.Logging.DeprecatedDefaultLog.LogKeys
		}

		// Fall back to the old logging.["default"].LogLevel option
		if config.Logging.Console.LogLevel == nil && config.Logging.DeprecatedDefaultLog.LogLevel != 0 {
			base.Warnf(warningMsgFmt, `logging.["default"].LogLevel`, "logging.console.log_level")
			config.Logging.Console.LogLevel = base.ToLogLevel(config.Logging.DeprecatedDefaultLog.LogLevel)
		}
	}

	// Fall back to the old LogFilePath option
	if config.Logging.LogFilePath == "" && config.DeprecatedLogFilePath != nil {
		base.Warnf(warningMsgFmt, "logFilePath", "logging.log_file_path")
		config.Logging.LogFilePath = *config.DeprecatedLogFilePath
	}

	// Fall back to the old Log option
	if config.Logging.Console.LogKeys == nil && len(config.DeprecatedLog) > 0 {
		base.Warnf(warningMsgFmt, "log", "logging.console.log_keys")
		config.Logging.Console.LogKeys = config.DeprecatedLog
	}
}

func (self *LegacyServerConfig) MergeWith(other *LegacyServerConfig) error {
	if self.Interface == nil {
		self.Interface = other.Interface
	}
	if self.AdminInterface == nil {
		self.AdminInterface = other.AdminInterface
	}
	if self.ProfileInterface == nil {
		self.ProfileInterface = other.ProfileInterface
	}
	if self.ConfigServer == nil {
		self.ConfigServer = other.ConfigServer
	}
	if self.DeploymentID == nil {
		self.DeploymentID = other.DeploymentID
	}
	if self.Facebook == nil {
		self.Facebook = other.Facebook
	}
	if self.CORS == nil {
		self.CORS = other.CORS
	}
	for _, flag := range other.DeprecatedLog {
		self.DeprecatedLog = append(self.DeprecatedLog, flag)
	}
	if self.Logging == nil {
		self.Logging = other.Logging
	}
	if other.Pretty {
		self.Pretty = true
	}
	for name, db := range other.Databases {
		if self.Databases[name] != nil {
			return base.RedactErrorf("Database %q already specified earlier", base.UD(name))
		}
		if self.Databases == nil {
			self.Databases = make(DbConfigMap)
		}
		self.Databases[name] = db
	}
	return nil
}

func (sc *LegacyServerConfig) Redacted() (*LegacyServerConfig, error) {
	var config LegacyServerConfig

	err := base.DeepCopyInefficient(&config, sc)
	if err != nil {
		return nil, err
	}

	for i := range config.Databases {
		config.Databases[i], err = config.Databases[i].Redacted()
		if err != nil {
			return nil, err
		}
	}

	return &config, nil
}

// Reads the command line flags and the optional config file.
func ParseCommandLine(args []string, handling flag.ErrorHandling) (*LegacyServerConfig, error) {
	flagSet := flag.NewFlagSet(args[0], handling)

	_ = flagSet.Bool("disable_persistent_config", false, "")

	_ = flagSet.Bool("api.admin_interface_authentication", true, "")
	_ = flagSet.Bool("api.metrics_interface_authentication", true, "")
	_ = flagSet.Bool("bootstrap.allow_insecure_server_connections", false, "")
	_ = flagSet.Bool("api.https.allow_insecure_tls_connections", false, "")

	addr := flagSet.String("interface", DefaultPublicInterface, "Address to bind to")
	authAddr := flagSet.String("adminInterface", DefaultAdminInterface, "Address to bind admin interface to")
	profAddr := flagSet.String("profileInterface", "", "Address to bind profile interface to")
	configServer := flagSet.String("configServer", "", "URL of server that can return database configs")
	deploymentID := flagSet.String("deploymentID", "", "Customer/project identifier for stats reporting")
	couchbaseURL := flagSet.String("url", "walrus:", "Address of Couchbase server")
	dbName := flagSet.String("dbname", "", "Name of Couchbase Server database (defaults to name of bucket)")
	pretty := flagSet.Bool("pretty", false, "Pretty-print JSON responses")
	verbose := flagSet.Bool("verbose", false, "Log more info about requests")
	logKeys := flagSet.String("log", "", "Log keys, comma separated")
	logFilePath := flagSet.String("logFilePath", "", "Path to log files")
	certpath := flagSet.String("certpath", "", "Client certificate path")
	cacertpath := flagSet.String("cacertpath", "", "Root CA certificate path")
	keypath := flagSet.String("keypath", "", "Client certificate key path")

	// used by service scripts as a way to specify a per-distro defaultLogFilePath
	defaultLogFilePathFlag := flagSet.String("defaultLogFilePath", "", "Path to log files, if not overridden by --logFilePath, or the config")

	_ = flagSet.Parse(args[1:])
	var config *LegacyServerConfig
	var err error

	if defaultLogFilePathFlag != nil {
		defaultLogFilePath = *defaultLogFilePathFlag
	}

	if flagSet.NArg() > 0 {
		// Read the configuration file(s), if any:
		for _, filename := range flagSet.Args() {
			newConfig, newConfigErr := LoadServerConfig(filename)

			if pkgerrors.Cause(newConfigErr) == base.ErrUnknownField {
				// Delay returning this error so we can continue with other setup
				err = pkgerrors.WithMessage(newConfigErr, fmt.Sprintf("Error reading config file %s", filename))
			} else if newConfigErr != nil {
				return config, pkgerrors.WithMessage(newConfigErr, fmt.Sprintf("Error reading config file %s", filename))
			}

			if config == nil {
				config = newConfig
			} else {
				if err := config.MergeWith(newConfig); err != nil {
					return config, pkgerrors.WithMessage(err, fmt.Sprintf("Error reading config file %s", filename))
				}
			}
		}

		// Override the config file with global settings from command line flags:
		if *addr != DefaultPublicInterface {
			config.Interface = addr
		}
		if *authAddr != DefaultAdminInterface {
			config.AdminInterface = authAddr
		}
		if *profAddr != "" {
			config.ProfileInterface = profAddr
		}
		if *configServer != "" {
			config.ConfigServer = configServer
		}
		if *deploymentID != "" {
			config.DeploymentID = deploymentID
		}
		if *pretty {
			config.Pretty = *pretty
		}

		// If the interfaces were not specified in either the config file or
		// on the command line, set them to the default values
		if config.Interface == nil {
			config.Interface = &DefaultPublicInterface
		}
		if config.AdminInterface == nil {
			config.AdminInterface = &DefaultAdminInterface
		}

		if *logFilePath != "" {
			config.Logging.LogFilePath = *logFilePath
		}

		if *logKeys != "" {
			config.Logging.Console.LogKeys = strings.Split(*logKeys, ",")
		}

		// Log HTTP Responses if verbose is enabled.
		if verbose != nil && *verbose {
			config.Logging.Console.LogKeys = append(config.Logging.Console.LogKeys, "HTTP+")
		}

	} else {
		// If no config file is given, create a default config, filled in from command line flags:
		var defaultBucketName = "sync_gateway"
		if *dbName == "" {
			*dbName = defaultBucketName
		}

		// At this point the addr is either:
		//   - A value provided by the user, in which case we want to leave it as is
		//   - The default value (":4984"), which is actually _not_ the default value we
		//     want for this case, since we are enabling insecure mode.  We want "localhost:4984" instead.
		// See #708 for more details
		if *addr == DefaultPublicInterface {
			*addr = "localhost:4984"
		}

		config = &LegacyServerConfig{
			Interface:        addr,
			AdminInterface:   authAddr,
			ProfileInterface: profAddr,
			Pretty:           *pretty,
			ConfigServer:     configServer,
			Logging: &base.LegacyLoggingConfig{
				Console: base.ConsoleLoggerConfig{
					// Enable the logger only when log keys have explicitly been set on the command line
					FileLoggerConfig: base.FileLoggerConfig{Enabled: base.BoolPtr(*logKeys != "")},
					LogKeys:          strings.Split(*logKeys, ","),
				},
				LogFilePath: *logFilePath,
			},
			Databases: map[string]*DbConfig{
				*dbName: {
					Name: *dbName,
					BucketConfig: BucketConfig{
						Server:     couchbaseURL,
						Bucket:     &defaultBucketName,
						CertPath:   *certpath,
						CACertPath: *cacertpath,
						KeyPath:    *keypath,
					},
					Users: map[string]*db.PrincipalConfig{
						base.GuestUsername: {
							Disabled:         false,
							ExplicitChannels: base.SetFromArray([]string{"*"}),
						},
					},
				},
			},
		}
	}

	if config.MetricsInterface == nil {
		config.MetricsInterface = &DefaultMetricsInterface
	}

	return config, err
}
