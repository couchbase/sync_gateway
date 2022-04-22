package rest

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/couchbase/gocbcore/v10/connstr"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	pkgerrors "github.com/pkg/errors"
)

// JSON object that defines the server configuration.
type LegacyServerConfig struct {
	TLSMinVersion                             *string                        `json:"tls_minimum_version,omitempty"`    // Set TLS Version
	UseTLSServer                              *bool                          `json:"use_tls_server,omitempty"`         // Use TLS for CBS <> SGW communications
	Interface                                 *string                        `json:",omitempty"`                       // Interface to bind REST API to, default ":4984"
	ServerTLSSkipVerify                       *bool                          `json:"server_tls_skip_verify,omitempty"` // Allow empty server CA Cert Path without attempting to use system root pool
	SSLCert                                   *string                        `json:",omitempty"`                       // Path to SSL cert file, or nil
	SSLKey                                    *string                        `json:",omitempty"`                       // Path to SSL private key file, or nil
	ServerReadTimeout                         *int                           `json:",omitempty"`                       // maximum duration.Second before timing out read of the HTTP(S) request
	ServerWriteTimeout                        *int                           `json:",omitempty"`                       // maximum duration.Second before timing out write of the HTTP(S) response
	ReadHeaderTimeout                         *int                           `json:",omitempty"`                       // The amount of time allowed to read request headers.
	IdleTimeout                               *int                           `json:",omitempty"`                       // The maximum amount of time to wait for the next request when keep-alives are enabled.
	AdminInterface                            *string                        `json:",omitempty"`                       // Interface to bind admin API to, default "localhost:4985"
	AdminUI                                   *string                        `json:",omitempty"`                       // Path to Admin HTML page, if omitted uses bundled HTML
	ProfileInterface                          *string                        `json:",omitempty"`                       // Interface to bind Go profile API to (no default)
	ConfigServer                              *string                        `json:",omitempty"`                       // URL of config server (for dynamic db discovery)
	Facebook                                  *FacebookConfigLegacy          `json:",omitempty"`                       // Configuration for Facebook validation
	Google                                    *GoogleConfigLegacy            `json:",omitempty"`                       // Configuration for Google validation
	CORS                                      *CORSConfigLegacy              `json:",omitempty"`                       // Configuration for allowing CORS
	DeprecatedLog                             []string                       `json:"log,omitempty"`                    // Log keywords to enable
	DeprecatedLogFilePath                     *string                        `json:"logFilePath,omitempty"`            // Path to log file, if missing write to stderr
	Logging                                   *base.LegacyLoggingConfig      `json:",omitempty"`                       // Configuration for logging with optional log file rotation
	Pretty                                    bool                           `json:",omitempty"`                       // Pretty-print JSON responses?
	DeploymentID                              *string                        `json:",omitempty"`                       // Optional customer/deployment ID for stats reporting
	StatsReportInterval                       *float64                       `json:",omitempty"`                       // Optional stats report interval (0 to disable)
	CouchbaseKeepaliveInterval                *int                           `json:",omitempty"`                       // TCP keep-alive interval between SG and Couchbase server
	SlowQueryWarningThreshold                 *int                           `json:",omitempty"`                       // Log warnings if N1QL queries take this many ms
	MaxIncomingConnections                    *int                           `json:",omitempty"`                       // Max # of incoming HTTP connections to accept
	MaxFileDescriptors                        *uint64                        `json:",omitempty"`                       // Max # of open file descriptors (RLIMIT_NOFILE)
	CompressResponses                         *bool                          `json:",omitempty"`                       // If false, disables compression of HTTP responses
	Databases                                 DbConfigMap                    `json:",omitempty"`                       // Pre-configured databases, mapped by name
	MaxHeartbeat                              *uint64                        `json:",omitempty"`                       // Max heartbeat value for _changes request (seconds)
	ClusterConfig                             *ClusterConfigLegacy           `json:"cluster_config,omitempty"`         // Bucket and other config related to CBGT
	Unsupported                               *UnsupportedServerConfigLegacy `json:"unsupported,omitempty"`            // Config for unsupported features
	ReplicatorCompression                     *int                           `json:"replicator_compression,omitempty"` // BLIP data compression level (0-9)
	BcryptCost                                int                            `json:"bcrypt_cost,omitempty"`            // bcrypt cost to use for password hashes - Default: bcrypt.DefaultCost
	MetricsInterface                          *string                        `json:"metricsInterface,omitempty"`       // Interface to bind metrics to. If not set then metrics isn't accessible
	HideProductVersion                        bool                           `json:"hide_product_version,omitempty"`   // Determines whether product versions removed from Server headers and REST API responses. This setting does not apply to the Admin REST API.
	DisablePersistentConfig                   *bool                          `json:"disable_persistent_config,omitempty" help:"Can be set to true to disable 3.0/persistent config handling."`
	AdminInterfaceAuthentication              *bool                          `json:"admin_interface_authentication,omitempty" help:"Whether the admin API requires authentication"`
	MetricsInterfaceAuthentication            *bool                          `json:"metrics_interface_authentication,omitempty" help:"Whether the metrics API requires authentication"`
	EnableAdminAuthenticationPermissionsCheck *bool                          `json:"enable_advanced_auth_dp,omitempty" help:"Whether to enable the permissions check feature of admin auth"`
	ConfigUpgradeGroupID                      string                         `json:"config_upgrade_group_id,omitempty"` // If set, determines the config group ID used when this legacy config is upgraded to a persistent config.
	RemovedLegacyServerConfig
}

// RemovedLegacyServerConfig are fields that used to be deprecated in a legacy config, but are now only present to log information about their removal.
type RemovedLegacyServerConfig struct {
	Replications interface{} `json:"replications,omitempty"` // Functionality removed. Used to log message to user to switch to ISGR
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

// ToStartupConfig returns the given LegacyServerConfig as a StartupConfig and a set of DBConfigs.
// The returned configs do not contain any default values - only a direct mapping of legacy config options as they were given.
func (lc *LegacyServerConfig) ToStartupConfig() (*StartupConfig, DbConfigMap, error) {
	// find a database's credentials for bootstrap (this isn't the first database config entry due to map iteration)
	bootstrapConfigIsSet := false
	bsc := &BootstrapConfig{}
	for _, dbConfig := range lc.Databases {
		// Move non-db scoped LegacyServerConfig properties to each DbConfig
		if lc.SlowQueryWarningThreshold != nil {
			dbConfig.SlowQueryWarningThresholdMs = base.Uint32Ptr(uint32(*lc.SlowQueryWarningThreshold))
		}

		// Validate replication configs
		for replicationID, replicationConfig := range dbConfig.Replications {
			if replicationConfig.ID != "" && replicationConfig.ID != replicationID {
				return nil, nil, fmt.Errorf("replication_id %q does not match replications key %q in replication config", replicationConfig.ID, replicationID)
			}

			err := replicationConfig.ValidateReplication(true)
			if err != nil {
				return nil, nil, err
			}
		}

		if dbConfig.Server == nil || *dbConfig.Server == "" {
			continue
		}

		server, username, password, err := legacyServerAddressUpgrade(*dbConfig.Server)
		if err != nil {
			server = *dbConfig.Server
			base.ErrorfCtx(context.Background(), "Error upgrading server address: %v", err)
		}

		dbConfig.Server = base.StringPtr(server)

		if !bootstrapConfigIsSet {
			// Prioritise config fields over credentials in host
			if dbConfig.Username != "" || dbConfig.Password != "" {
				username = dbConfig.Username
				password = dbConfig.Password
			}

			bsc = &BootstrapConfig{
				Server:              server,
				Username:            username,
				Password:            password,
				CACertPath:          dbConfig.CACertPath,
				X509CertPath:        dbConfig.CertPath,
				X509KeyPath:         dbConfig.KeyPath,
				ServerTLSSkipVerify: lc.ServerTLSSkipVerify,
				UseTLSServer:        lc.UseTLSServer,
			}
			bootstrapConfigIsSet = true
		}
	}

	if lc.ConfigUpgradeGroupID != "" {
		if !base.IsEnterpriseEdition() {
			return nil, nil, errors.New("customization of config_upgrade_group_id is only supported in enterprise edition")
		}
		bsc.ConfigGroupID = lc.ConfigUpgradeGroupID
	}

	sc := StartupConfig{
		Bootstrap: *bsc,
		API: APIConfig{
			CompressResponses:                         lc.CompressResponses,
			AdminInterfaceAuthentication:              lc.AdminInterfaceAuthentication,
			MetricsInterfaceAuthentication:            lc.MetricsInterfaceAuthentication,
			EnableAdminAuthenticationPermissionsCheck: lc.EnableAdminAuthenticationPermissionsCheck,
		},
		Logging: base.LoggingConfig{},
		Auth: AuthConfig{
			BcryptCost: lc.BcryptCost,
		},
		Replicator: ReplicatorConfig{
			BLIPCompression: lc.ReplicatorCompression,
		},
		CouchbaseKeepaliveInterval: lc.CouchbaseKeepaliveInterval,
	}

	if lc.Pretty {
		sc.API.Pretty = &lc.Pretty
	}

	if lc.HideProductVersion {
		sc.API.HideProductVersion = &lc.HideProductVersion
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

	if lc.MaxHeartbeat != nil {
		sc.Replicator.MaxHeartbeat = base.NewConfigDuration(time.Second * time.Duration(*lc.MaxHeartbeat))
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
			sc.Unsupported.UseStdlibJSON = lc.Unsupported.UseStdlibJSON
		}
	}
	if lc.MaxFileDescriptors != nil {
		sc.MaxFileDescriptors = *lc.MaxFileDescriptors
	}

	return &sc, lc.Databases, nil
}

// ToDatabaseConfig "upgrades" a DbConfig to be compatible with 3.x
func (dbc *DbConfig) ToDatabaseConfig() *DatabaseConfig {
	if dbc == nil {
		return nil
	}

	// Backwards compatibility: Continue defaulting to xattrs=false for upgraded 2.x configs (3.0+ default xattrs=true)
	if dbc.EnableXattrs == nil {
		dbc.EnableXattrs = base.BoolPtr(false)
	}

	// Move guest out of the Users section and into its own promoted field
	if guest, ok := dbc.Users[base.GuestUsername]; ok {
		dbc.Guest = guest
		delete(dbc.Users, base.GuestUsername)
	}

	return &DatabaseConfig{
		cas:      0,
		DbConfig: *dbc,
	}
}

func legacyServerAddressUpgrade(server string) (newServer, username, password string, err error) {
	if base.ServerIsWalrus(server) {
		return server, "", "", nil
	}

	connSpec, err := connstr.Parse(server)
	if err != nil {
		return "", "", "", err
	}

	// Get credentials from first host (if exists)
	splitServers := strings.Split(server, ",")
	u, err := url.Parse(splitServers[0])
	if err != nil {
		return "", "", "", err
	}
	if u.User != nil {
		urlUsername := u.User.Username()
		urlPassword, urlPasswordSet := u.User.Password()
		if urlUsername != "" && urlPasswordSet {
			username = urlUsername
			password = urlPassword
		}
	}

	if connSpec.Scheme == "http" {
		connSpec.Scheme = "couchbase"
		for i, addr := range connSpec.Addresses {
			if addr.Port != 8091 && addr.Port > 0 {
				return "", "", "", fmt.Errorf("automatic migration of connection string from http:// to couchbase:// scheme doesn't support non-default ports. " +
					"Please change the server field to use the couchbase(s):// scheme")
			}
			connSpec.Addresses[i].Port = -1
		}
	}

	return connSpec.String(), username, password, nil
}

// Implementation of AuthHandler interface for ClusterConfigLegacy
func (clusterConfig *ClusterConfigLegacy) GetCredentials() (string, string, string) {
	return base.TransformBucketCredentials(clusterConfig.Username, clusterConfig.Password, *clusterConfig.Bucket)
}

// LoadLegacyServerConfig loads a LegacyServerConfig from either a JSON file or from a URL
func LoadLegacyServerConfig(path string) (config *LegacyServerConfig, err error) {
	rc, err := readFromPath(path, false)
	if err != nil {
		return nil, err
	}

	defer func() { _ = rc.Close() }()
	return readLegacyServerConfig(rc)
}

// readLegacyServerConfig returns a validated LegacyServerConfig from an io.Reader
func readLegacyServerConfig(r io.Reader) (config *LegacyServerConfig, err error) {
	err = decodeAndSanitiseConfig(r, &config)
	if err != nil {
		return config, err
	}

	if config.Replications != nil {
		return config, fmt.Errorf("cannot use SG replicate as it has been removed. Please use Inter-Sync Gateway Replication instead")
	}
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
	var multiError *base.MultiError
	multiError = multiError.Append(config.validate())
	multiError = multiError.Append(config.setupAndValidateDatabases())
	if multiError.ErrorOrNil() != nil {
		base.ErrorfCtx(context.Background(), "Error during config validation: %v", multiError)
		return nil, fmt.Errorf("error(s) during config validation: %v", multiError)
	}

	return config, nil
}

func setupAndValidateDatabases(databases DbConfigMap) error {
	for name, dbConfig := range databases {
		if err := dbConfig.setup(name, BootstrapConfig{}, nil); err != nil {
			return err
		}
		if err := dbConfig.validate(context.Background(), false); err != nil {
			return err
		}
	}
	return nil
}

func (config *LegacyServerConfig) setupAndValidateDatabases() error {
	if config == nil {
		return nil
	}
	return setupAndValidateDatabases(config.Databases)
}

// validate validates the given server config and returns all invalid options as a slice of errors
func (config *LegacyServerConfig) validate() (errorMessages error) {
	var multiError *base.MultiError
	if config.Unsupported != nil && config.Unsupported.StatsLogFrequencySecs != nil {
		if *config.Unsupported.StatsLogFrequencySecs == 0 {
			// explicitly disabled
		} else if *config.Unsupported.StatsLogFrequencySecs < 10 {
			multiError = multiError.Append(fmt.Errorf(minValueErrorMsg,
				"unsupported.stats_log_freq_secs", 10))
		}
	}

	return multiError.ErrorOrNil()
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
	_ = flagSet.Bool("bootstrap.use_tls_server", true, "")

	addr := flagSet.String("interface", DefaultPublicInterface, "Address to bind to")
	authAddr := flagSet.String("adminInterface", DefaultAdminInterface, "Address to bind admin interface to")
	profAddr := flagSet.String("profileInterface", "", "Address to bind profile interface to")
	configServer := flagSet.String("configServer", "", "URL of server that can return database configs")
	deploymentID := flagSet.String("deploymentID", "", "Customer/project identifier for stats reporting")
	couchbaseURL := flagSet.String("url", "", "Address of Couchbase server")
	dbName := flagSet.String("dbname", "", "Name of Couchbase Server database (defaults to name of bucket)")
	pretty := flagSet.Bool("pretty", false, "Pretty-print JSON responses")
	verbose := flagSet.Bool("verbose", false, "Log more info about requests")
	logKeys := flagSet.String("log", "", "Log keys, comma separated")
	logFilePath := flagSet.String("logFilePath", "", "Path to log files")
	certpath := flagSet.String("certpath", "", "Client certificate path")
	cacertpath := flagSet.String("cacertpath", "", "Root CA certificate path")
	keypath := flagSet.String("keypath", "", "Client certificate key path")

	// used by service scripts as a way to specify a per-distro defaultLogFilePath
	flagSet.String("defaultLogFilePath", "", "Path to log files, if not overridden by --logFilePath, or the config")

	_ = flagSet.Parse(args[1:])
	var config *LegacyServerConfig
	var err error

	if flagSet.NArg() > 0 {
		// Read the configuration file(s), if any:
		for _, filename := range flagSet.Args() {
			newConfig, newConfigErr := LoadLegacyServerConfig(filename)

			if newConfigErr != nil {
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
			UseTLSServer:     base.BoolPtr(true),
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
							Disabled:         base.BoolPtr(false),
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
