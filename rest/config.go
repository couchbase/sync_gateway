//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// Register profiling handlers (see Go docs)
import _ "net/http/pprof"

var DefaultInterface = ":4984"
var DefaultAdminInterface = "127.0.0.1:4985" // Only accessible on localhost!
var DefaultServer = "walrus:"
var DefaultPool = "default"

var config *ServerConfig

const (
	DefaultMaxCouchbaseConnections         = 16
	DefaultMaxCouchbaseOverflowConnections = 0

	// Default value of ServerConfig.MaxIncomingConnections
	DefaultMaxIncomingConnections = 0

	// Default value of ServerConfig.MaxFileDescriptors
	DefaultMaxFileDescriptors uint64 = 5000
)

type SyncGatewayRunMode uint8

const (
	SyncGatewayRunModeNormal SyncGatewayRunMode = iota
	SyncGatewayRunModeAccel
)

// JSON object that defines the server configuration.
type ServerConfig struct {
	Interface                      *string                  `json:",omitempty"`            // Interface to bind REST API to, default ":4984"
	SSLCert                        *string                  `json:",omitempty"`            // Path to SSL cert file, or nil
	SSLKey                         *string                  `json:",omitempty"`            // Path to SSL private key file, or nil
	ServerReadTimeout              *int                     `json:",omitempty"`            // maximum duration.Second before timing out read of the HTTP(S) request
	ServerWriteTimeout             *int                     `json:",omitempty"`            // maximum duration.Second before timing out write of the HTTP(S) response
	AdminInterface                 *string                  `json:",omitempty"`            // Interface to bind admin API to, default ":4985"
	AdminUI                        *string                  `json:",omitempty"`            // Path to Admin HTML page, if omitted uses bundled HTML
	ProfileInterface               *string                  `json:",omitempty"`            // Interface to bind Go profile API to (no default)
	ConfigServer                   *string                  `json:",omitempty"`            // URL of config server (for dynamic db discovery)
	Facebook                       *FacebookConfig          `json:",omitempty"`            // Configuration for Facebook validation
	Google                         *GoogleConfig            `json:",omitempty"`            // Configuration for Google validation
	CORS                           *CORSConfig              `json:",omitempty"`            // Configuration for allowing CORS
	DeprecatedLog                  []string                 `json:"log,omitempty"`         // Log keywords to enable
	DeprecatedLogFilePath          *string                  `json:"logFilePath,omitempty"` // Path to log file, if missing write to stderr
	Logging                        *base.LoggingConfigMap   `json:",omitempty"`            // Configuration for logging with optional log file rotation
	Pretty                         bool                     `json:",omitempty"`            // Pretty-print JSON responses?
	DeploymentID                   *string                  `json:",omitempty"`            // Optional customer/deployment ID for stats reporting
	StatsReportInterval            *float64                 `json:",omitempty"`            // Optional stats report interval (0 to disable)
	MaxCouchbaseConnections        *int                     `json:",omitempty"`            // Max # of sockets to open to a Couchbase Server node
	MaxCouchbaseOverflow           *int                     `json:",omitempty"`            // Max # of overflow sockets to open
	CouchbaseKeepaliveInterval     *int                     `json:",omitempty"`            // TCP keep-alive interval between SG and Couchbase server
	SlowServerCallWarningThreshold *int                     `json:",omitempty"`            // Log warnings if database calls take this many ms
	MaxIncomingConnections         *int                     `json:",omitempty"`            // Max # of incoming HTTP connections to accept
	MaxFileDescriptors             *uint64                  `json:",omitempty"`            // Max # of open file descriptors (RLIMIT_NOFILE)
	CompressResponses              *bool                    `json:",omitempty"`            // If false, disables compression of HTTP responses
	Databases                      DbConfigMap              `json:",omitempty"`            // Pre-configured databases, mapped by name
	Replications                   []*ReplicationConfig     `json:",omitempty"`
	MaxHeartbeat                   uint64                   `json:",omitempty"`                        // Max heartbeat value for _changes request (seconds)
	ClusterConfig                  *ClusterConfig           `json:"cluster_config,omitempty"`          // Bucket and other config related to CBGT
	SkipRunmodeValidation          bool                     `json:"skip_runmode_validation,omitempty"` // If this is true, skips any config validation regarding accel vs normal mode
	Unsupported                    *UnsupportedServerConfig `json:"unsupported,omitempty"`             // Config for unsupported features
	RunMode                        SyncGatewayRunMode       `json:"runmode,omitempty"`                 // Whether this is an SG reader or an SG Accelerator
}

// Bucket configuration elements - used by db, shadow, index
type BucketConfig struct {
	Server   *string `json:"server,omitempty"`   // Couchbase server URL
	Pool     *string `json:"pool,omitempty"`     // Couchbase pool name, default "default"
	Bucket   *string `json:"bucket,omitempty"`   // Bucket name
	Username string  `json:"username,omitempty"` // Username for authenticating to server
	Password string  `json:"password,omitempty"` // Password for authenticating to server
}

type ClusterConfig struct {
	BucketConfig
	DataDir                  string  `json:"data_dir,omitempty"`
	HeartbeatIntervalSeconds *uint16 `json:"heartbeat_interval_seconds,omitempty"`
}

func (c ClusterConfig) CBGTEnabled() bool {
	// if we have a non-empty server field, then assume CBGT is enabled.
	return c.Server != nil && *c.Server != ""
}

// JSON object that defines a database configuration within the ServerConfig.
type DbConfig struct {
	BucketConfig
	Name               string                         `json:"name,omitempty"`                 // Database name in REST API (stored as key in JSON)
	Sync               *string                        `json:"sync,omitempty"`                 // Sync function defines which users can see which data
	Users              map[string]*db.PrincipalConfig `json:"users,omitempty"`                // Initial user accounts
	Roles              map[string]*db.PrincipalConfig `json:"roles,omitempty"`                // Initial roles
	RevsLimit          *uint32                        `json:"revs_limit,omitempty"`           // Max depth a document's revision tree can grow to
	ImportDocs         interface{}                    `json:"import_docs,omitempty"`          // false, true, or "continuous"
	Shadow             *ShadowConfig                  `json:"shadow,omitempty"`               // External bucket to shadow
	EventHandlers      interface{}                    `json:"event_handlers,omitempty"`       // Event handlers (webhook)
	FeedType           string                         `json:"feed_type,omitempty"`            // Feed type - "DCP" or "TAP"; defaults based on Couchbase server version
	AllowEmptyPassword bool                           `json:"allow_empty_password,omitempty"` // Allow empty passwords?  Defaults to false
	CacheConfig        *CacheConfig                   `json:"cache,omitempty"`                // Cache settings
	ChannelIndex       *ChannelIndexConfig            `json:"channel_index,omitempty"`        // Channel index settings
	RevCacheSize       *uint32                        `json:"rev_cache_size,omitempty"`       // Maximum number of revisions to store in the revision cache
	StartOffline       bool                           `json:"offline,omitempty"`              // start the DB in the offline state, defaults to false
	Unsupported        *UnsupportedConfig             `json:"unsupported,omitempty"`          // Config for unsupported features
	OIDCConfig         *auth.OIDCOptions              `json:"oidc,omitempty"`                 // Config properties for OpenID Connect authentication
}

type DbConfigMap map[string]*DbConfig

type ReplConfigMap map[string]*ReplicationConfig

type FacebookConfig struct {
	Register bool // If true, server will register new user accounts
}

type GoogleConfig struct {
	Register    bool     // If true, server will register new user accounts
	AppClientID []string `json:"app_client_id"` // list of enabled client ids
}

type CORSConfig struct {
	Origin      []string // List of allowed origins, use ["*"] to allow access from everywhere
	LoginOrigin []string // List of allowed login origins
	Headers     []string // List of allowed headers
	MaxAge      int      // Maximum age of the CORS Options request
}

type ShadowConfig struct {
	BucketConfig
	Doc_id_regex *string `json:"doc_id_regex,omitempty"` // Optional regex that doc IDs must match
	FeedType     string  `json:"feed_type,omitempty"`    // Feed type - "DCP" or "TAP"; defaults to TAP
}

type EventHandlerConfig struct {
	MaxEventProc    uint           `json:"max_processes,omitempty"`    // Max concurrent event handling goroutines
	WaitForProcess  string         `json:"wait_for_process,omitempty"` // Max wait time when event queue is full (ms)
	DocumentChanged []*EventConfig `json:"document_changed,omitempty"` // Document Commit
	DBStateChanged  []*EventConfig `json:"db_state_changed,omitempty"` // DB state change
}

type EventConfig struct {
	HandlerType string  `json:"handler"`           // Handler type
	Url         string  `json:"url,omitempty"`     // Url (webhook)
	Filter      string  `json:"filter,omitempty"`  // Filter function (webhook)
	Timeout     *uint64 `json:"timeout,omitempty"` // Timeout (webhook)
}

type CacheConfig struct {
	CachePendingSeqMaxWait *uint32 `json:"max_wait_pending,omitempty"` // Max wait for pending sequence before skipping
	CachePendingSeqMaxNum  *int    `json:"max_num_pending,omitempty"`  // Max number of pending sequences before skipping
	CacheSkippedSeqMaxWait *uint32 `json:"max_wait_skipped,omitempty"` // Max wait for skipped sequence before abandoning
	EnableStarChannel      *bool   `json:"enable_star_channel"`        // Enable star channel
	ChannelCacheMaxLength  *int    `json:"channel_cache_max_length"`   // Maximum number of entries maintained in cache per channel
	ChannelCacheMinLength  *int    `json:"channel_cache_min_length"`   // Minimum number of entries maintained in cache per channel
	ChannelCacheAge        *int    `json:"channel_cache_expiry"`       // Time (seconds) to keep entries in cache beyond the minimum retained
}

type ChannelIndexConfig struct {
	BucketConfig
	IndexWriter        bool                `json:"writer,omitempty"`      // Whether SG node is a channel index writer
	NumShards          uint16              `json:"num_shards,omitempty"`  // Number of partitions in the channel index
	SequenceHashConfig *SequenceHashConfig `json:"seq_hashing,omitempty"` // Sequence hash configuration
}

type SequenceHashConfig struct {
	BucketConfig         // Bucket used for Sequence hashing
	Expiry       *uint32 `json:"expiry,omitempty"`         // Expiry set for hash values on latest use
	Frequency    *int    `json:"hash_frequency,omitempty"` // Frequency of sequence hashing in changes feeds
}

type UnsupportedConfig struct {
	UserViews        *UserViewsConfig            `json:"user_views,omitempty"`         // Config settings for user views
	OidcTestProvider *db.OidcTestProviderOptions `json:"oidc_test_provider,omitempty"` // Config settings for OIDC Provider
}

type UnsupportedServerConfig struct {
	Http2Config *Http2Config `json:"http2,omitempty"` // Config settings for HTTP2
}

type UserViewsConfig struct {
	Enabled *bool `json:"enabled,omitempty"` // Whether pass-through view query is supported through public API
}

type Http2Config struct {
	Enabled *bool `json:"enabled,omitempty"` // Whether HTTP2 support is enabled
}

func (dbConfig *DbConfig) setup(name string) error {
	dbConfig.Name = name
	if dbConfig.Bucket == nil {
		dbConfig.Bucket = &dbConfig.Name
	}
	if dbConfig.Server == nil {
		dbConfig.Server = &DefaultServer
	}
	if dbConfig.Pool == nil {
		dbConfig.Pool = &DefaultPool
	}

	url, err := url.Parse(*dbConfig.Server)
	if err == nil && url.User != nil {
		// Remove credentials from URL and put them into the DbConfig.Username and .Password:
		if dbConfig.Username == "" {
			dbConfig.Username = url.User.Username()
		}
		if dbConfig.Password == "" {
			if password, exists := url.User.Password(); exists {
				dbConfig.Password = password
			}
		}
		url.User = nil
		urlStr := url.String()
		dbConfig.Server = &urlStr
	}

	if dbConfig.Shadow != nil {
		url, err = url.Parse(*dbConfig.Shadow.Server)
		if err == nil && url.User != nil {
			// Remove credentials from shadow URL and put them into the DbConfig.Shadow.Username and .Password:
			if dbConfig.Shadow.Username == "" {
				dbConfig.Shadow.Username = url.User.Username()
			}
			if dbConfig.Shadow.Password == "" {
				if password, exists := url.User.Password(); exists {
					dbConfig.Shadow.Password = password
				}
			}
			url.User = nil
			urlStr := url.String()
			dbConfig.Shadow.Server = &urlStr
		}
	}

	if dbConfig.ChannelIndex != nil {
		url, err = url.Parse(*dbConfig.ChannelIndex.Server)
		if err == nil && url.User != nil {
			// Remove credentials from shadow URL and put them into the DbConfig.ChannelIndex.Username and .Password:
			if dbConfig.ChannelIndex.Username == "" {
				dbConfig.ChannelIndex.Username = url.User.Username()
			}
			if dbConfig.ChannelIndex.Password == "" {
				if password, exists := url.User.Password(); exists {
					dbConfig.ChannelIndex.Password = password
				}
			}
			url.User = nil
			urlStr := url.String()
			dbConfig.ChannelIndex.Server = &urlStr
		}
	}

	return err
}

func (dbConfig DbConfig) validate() error {

	// if there is a ChannelIndex being used, then the only valid feed type is DCPSHARD
	if dbConfig.ChannelIndex != nil {
		if strings.ToLower(dbConfig.FeedType) != strings.ToLower(base.DcpShardFeedType) {
			msg := "ChannelIndex declared in config, but the FeedType is %v " +
				"rather than expected value of DCPSHARD"
			return fmt.Errorf(msg, dbConfig.FeedType)
		}
	}

	// if the feed type is DCPSHARD, then there must be a ChannelIndex
	if strings.ToLower(dbConfig.FeedType) == strings.ToLower(base.DcpShardFeedType) {
		if dbConfig.ChannelIndex == nil {
			msg := "FeedType is DCPSHARD, but no ChannelIndex declared in config"
			return fmt.Errorf(msg)
		}
	}

	return nil

}

func (dbConfig *DbConfig) validateSgDbConfig() error {

	if err := dbConfig.validate(); err != nil {
		return err
	}

	if dbConfig.ChannelIndex != nil && dbConfig.ChannelIndex.IndexWriter == true {
		return fmt.Errorf("Invalid configuration for Sync Gw.  Must not be configured as an IndexWriter")
	}

	// Don't allow Distributed Index and Bucket Shadowing to co-exist
	if err := dbConfig.verifyNoDistributedIndexAndBucketShadowing(); err != nil {
		return err
	}

	return nil

}

func (dbConfig *DbConfig) validateSgAccelDbConfig() error {

	if err := dbConfig.validate(); err != nil {
		return err
	}

	if dbConfig.ChannelIndex == nil {
		return fmt.Errorf("Invalid configuration for Sync Gw Accel.  Must have a ChannelIndex defined")
	}

	if dbConfig.ChannelIndex.IndexWriter == false {
		return fmt.Errorf("Invalid configuration for Sync Gw Accel.  Must be configured as an IndexWriter")
	}

	if strings.ToLower(dbConfig.FeedType) != strings.ToLower(base.DcpShardFeedType) {
		return fmt.Errorf("Invalid configuration for Sync Gw Accel.  Must be configured for DCPSHARD feedtype")

	}

	// Don't allow Distributed Index and Bucket Shadowing to co-exist
	if err := dbConfig.verifyNoDistributedIndexAndBucketShadowing(); err != nil {
		return err
	}

	return nil

}

func (dbConfig *DbConfig) verifyNoDistributedIndexAndBucketShadowing() error {
	// Don't allow Distributed Index and Bucket Shadowing to co-exist
	if dbConfig.ChannelIndex != nil && dbConfig.Shadow != nil {
		return fmt.Errorf("Using Sync Gateway Accel with Bucket Shadowing is not supported")
	}
	return nil
}

func (dbConfig *DbConfig) modifyConfig() {
	if dbConfig.ChannelIndex != nil {
		// if there is NO feed type, set to DCPSHARD, since that's the only
		// valid config when a Channel Index is specified
		if dbConfig.FeedType == "" {
			dbConfig.FeedType = base.DcpShardFeedType
		}
	}
}

// Implementation of AuthHandler interface for DbConfig
func (dbConfig *DbConfig) GetCredentials() (string, string, string) {
	return base.TransformBucketCredentials(dbConfig.Username, dbConfig.Password, *dbConfig.Bucket)
}

// Implementation of AuthHandler interface for ShadowConfig
func (shadowConfig *ShadowConfig) GetCredentials() (string, string, string) {
	return base.TransformBucketCredentials(shadowConfig.Username, shadowConfig.Password, *shadowConfig.Bucket)
}

// Implementation of AuthHandler interface for ChannelIndexConfig
func (channelIndexConfig *ChannelIndexConfig) GetCredentials() (string, string, string) {
	return base.TransformBucketCredentials(channelIndexConfig.Username, channelIndexConfig.Password, *channelIndexConfig.Bucket)
}

// Reads a ServerConfig from raw data
func ReadServerConfigFromData(runMode SyncGatewayRunMode, data []byte) (*ServerConfig, error) {

	data = base.ConvertBackQuotedStrings(data)
	var config *ServerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	config.RunMode = runMode

	// Validation:
	if err := config.setupAndValidateDatabases(); err != nil {
		return nil, err
	}

	return config, nil
}

// Reads a ServerConfig from a URL.
func ReadServerConfigFromUrl(runMode SyncGatewayRunMode, url string) (*ServerConfig, error) {

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return ReadServerConfigFromData(runMode, responseBody)

}

// Reads a ServerConfig from either a JSON file or from a URL.
func ReadServerConfig(runMode SyncGatewayRunMode, path string) (*ServerConfig, error) {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return ReadServerConfigFromUrl(runMode, path)
	} else {
		return ReadServerConfigFromFile(runMode, path)
	}
}

// Reads a ServerConfig from a JSON file.
func ReadServerConfigFromFile(runMode SyncGatewayRunMode, path string) (*ServerConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	data = base.ConvertBackQuotedStrings(data)
	var config *ServerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	config.RunMode = runMode

	// Validation:
	if err := config.setupAndValidateDatabases(); err != nil {
		return nil, err
	}

	return config, nil

}

func (config *ServerConfig) setupAndValidateDatabases() error {
	for name, dbConfig := range config.Databases {
		dbConfig.setup(name)
		if err := config.validateDbConfig(dbConfig); err != nil {
			return err
		}
	}
	return nil
}

func (config *ServerConfig) setupAndValidateLogging(verbose bool) error {
	//If a logging config exists, it must contain a single
	// appender named "default"
	if config.Logging == nil {
		if config.DeprecatedLogFilePath != nil {
			base.UpdateLogger(*config.DeprecatedLogFilePath)
		}
	} else {
		if len(*config.Logging) != 1 || ((*config.Logging)["default"] == nil) {
			return fmt.Errorf("The logging section must define a single \"default\" appender")
		}
		// Validate the default appender configuration
		if defaultLogger := (*config.Logging)["default"]; defaultLogger != nil {
			if err := defaultLogger.ValidateLogAppender(); err != nil {
				return err
			}
			base.CreateRollingLogger(defaultLogger)
		}
	}

	base.EnableLogKey("HTTP")
	if verbose {
		base.EnableLogKey("HTTP+")
	}

	return nil
}

func (config *ServerConfig) validateDbConfig(dbConfig *DbConfig) error {

	dbConfig.modifyConfig()

	switch config.RunMode {
	case SyncGatewayRunModeNormal:
		return dbConfig.validateSgDbConfig()
	case SyncGatewayRunModeAccel:
		return dbConfig.validateSgAccelDbConfig()
	}

	return fmt.Errorf("Unexpected RunMode: %v", config.RunMode)

}

func (self *ServerConfig) MergeWith(other *ServerConfig) error {
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
	if other.Pretty {
		self.Pretty = true
	}
	for name, db := range other.Databases {
		if self.Databases[name] != nil {
			return fmt.Errorf("Database %q already specified earlier", name)
		}
		self.Databases[name] = db
	}
	return nil
}

// Reads the command line flags and the optional config file.
func ParseCommandLine(runMode SyncGatewayRunMode) {
	addr := flag.String("interface", DefaultInterface, "Address to bind to")
	authAddr := flag.String("adminInterface", DefaultAdminInterface, "Address to bind admin interface to")
	profAddr := flag.String("profileInterface", "", "Address to bind profile interface to")
	configServer := flag.String("configServer", "", "URL of server that can return database configs")
	deploymentID := flag.String("deploymentID", "", "Customer/project identifier for stats reporting")
	couchbaseURL := flag.String("url", DefaultServer, "Address of Couchbase server")
	poolName := flag.String("pool", DefaultPool, "Name of pool")
	bucketName := flag.String("bucket", "sync_gateway", "Name of bucket")
	dbName := flag.String("dbname", "", "Name of Couchbase Server database (defaults to name of bucket)")
	pretty := flag.Bool("pretty", false, "Pretty-print JSON responses")
	verbose := flag.Bool("verbose", false, "Log more info about requests")
	logKeys := flag.String("log", "", "Log keywords, comma separated")
	logFilePath := flag.String("logFilePath", "", "Path to log file")
	skipRunModeValidation := flag.Bool("skipRunModeValidation", false, "Skip config validation for runmode (accel vs normal sg)")

	flag.Parse()

	if flag.NArg() > 0 {
		// Read the configuration file(s), if any:
		for i := 0; i < flag.NArg(); i++ {
			filename := flag.Arg(i)
			c, err := ReadServerConfig(runMode, filename)
			if err != nil {
				base.LogFatal("Error reading config file %s: %v", filename, err)
			}
			if config == nil {
				config = c
			} else {
				if err := config.MergeWith(c); err != nil {
					base.LogFatal("Error reading config file %s: %v", filename, err)
				}
			}
		}

		// Override the config file with global settings from command line flags:
		if *addr != DefaultInterface {
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
		if config.DeprecatedLog != nil {
			base.ParseLogFlags(config.DeprecatedLog)
		}

		// If the interfaces were not specified in either the config file or
		// on the command line, set them to the default values
		if config.Interface == nil {
			config.Interface = &DefaultInterface
		}
		if config.AdminInterface == nil {
			config.AdminInterface = &DefaultAdminInterface
		}

		if *logFilePath != "" {
			config.DeprecatedLogFilePath = logFilePath
		}

		if *skipRunModeValidation == true {
			config.SkipRunmodeValidation = *skipRunModeValidation
		}

	} else {
		// If no config file is given, create a default config, filled in from command line flags:
		if *dbName == "" {
			*dbName = *bucketName
		}

		// At this point the addr is either:
		//   - A value provided by the user, in which case we want to leave it as is
		//   - The default value (":4984"), which is actually _not_ the default value we
		//     want for this case, since we are enabling insecure mode.  We want "localhost:4984" instead.
		// See #708 for more details
		if *addr == DefaultInterface {
			*addr = "localhost:4984"
		}

		config = &ServerConfig{
			Interface:        addr,
			AdminInterface:   authAddr,
			ProfileInterface: profAddr,
			Pretty:           *pretty,
			Databases: map[string]*DbConfig{
				*dbName: {
					Name: *dbName,
					BucketConfig: BucketConfig{
						Server: couchbaseURL,
						Bucket: bucketName,
						Pool:   poolName,
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

	base.ParseLogFlag(*logKeys)

	// Logging config will now have been loaded from command line
	// or from a sync_gateway config file so we can validate the
	// configuration and setup logging now
	if err := config.setupAndValidateLogging(*verbose); err != nil {
		base.LogFatal("Error setting up logging %v", err)
	}

	//return config
}

func SetMaxFileDescriptors(maxP *uint64) {
	maxFDs := DefaultMaxFileDescriptors
	if maxP != nil {
		maxFDs = *maxP
	}
	_, err := base.SetMaxFileDescriptors(maxFDs)
	if err != nil {
		base.Warn("Error setting MaxFileDescriptors to %d: %v", maxFDs, err)
	}
}

func (config *ServerConfig) Serve(addr string, handler http.Handler) {
	maxConns := DefaultMaxIncomingConnections
	if config.MaxIncomingConnections != nil {
		maxConns = *config.MaxIncomingConnections
	}

	http2Enabled := false
	if config.Unsupported != nil && config.Unsupported.Http2Config != nil {
		http2Enabled = *config.Unsupported.Http2Config.Enabled
	}
	err := base.ListenAndServeHTTP(
		addr,
		maxConns,
		config.SSLCert,
		config.SSLKey,
		handler,
		config.ServerReadTimeout,
		config.ServerWriteTimeout,
		http2Enabled,
	)
	if err != nil {
		base.LogFatal("Failed to start HTTP server on %s: %v", addr, err)
	}
}

func (config *ServerConfig) HasAnyIndexReaderConfiguredDatabases() bool {
	numIndexReaders := config.NumIndexReaders()
	return numIndexReaders > 0
}

func (config *ServerConfig) HasAnyIndexWriterConfiguredDatabases() bool {
	numIndexWriters := config.NumIndexWriters()
	return numIndexWriters > 0
}

func (config *ServerConfig) NumIndexReaders() int {
	n := 0
	for _, dbConfig := range config.Databases {
		if dbConfig.ChannelIndex == nil || dbConfig.ChannelIndex.IndexWriter == false {
			n += 1
		}
	}
	return n
}

func (config *ServerConfig) NumIndexWriters() int {
	n := 0
	for _, dbConfig := range config.Databases {
		if dbConfig.ChannelIndex != nil && dbConfig.ChannelIndex.IndexWriter == true {
			n += 1
		}
	}
	return n
}

// Starts and runs the server given its configuration. (This function never returns.)
func RunServer(config *ServerConfig) {
	PrettyPrint = config.Pretty

	base.Logf("==== %s ====", LongVersionString)

	if os.Getenv("GOMAXPROCS") == "" && runtime.GOMAXPROCS(0) == 1 {
		cpus := runtime.NumCPU()
		if cpus > 1 {
			runtime.GOMAXPROCS(cpus)
			base.Logf("Configured Go to use all %d CPUs; setenv GOMAXPROCS to override this", cpus)
		}
	}

	SetMaxFileDescriptors(config.MaxFileDescriptors)

	sc := NewServerContext(config)
	for _, dbConfig := range config.Databases {
		if _, err := sc.AddDatabaseFromConfig(dbConfig); err != nil {
			base.LogFatal("Error opening database: %v", err)
		}
	}

	if config.ProfileInterface != nil {
		//runtime.MemProfileRate = 10 * 1024
		base.Logf("Starting profile server on %s", *config.ProfileInterface)
		go func() {
			http.ListenAndServe(*config.ProfileInterface, nil)
		}()
	}

	base.Logf("Starting admin server on %s", *config.AdminInterface)
	go config.Serve(*config.AdminInterface, CreateAdminHandler(sc))
	base.Logf("Starting server on %s ...", *config.Interface)
	config.Serve(*config.Interface, CreatePublicHandler(sc))
}

// for now  just cycle the logger to allow for log file rotation
func HandleSighup() {
	if config.DeprecatedLogFilePath != nil {
		base.UpdateLogger(*config.DeprecatedLogFilePath)
	}
}

func GetConfig() *ServerConfig {
	return config
}

func ValidateConfigOrPanic(runMode SyncGatewayRunMode) {

	// if the user passes -skipRunModeValidation on the command line, then skip validation
	if config.SkipRunmodeValidation == true {
		base.Logf("Skipping runmode (accel vs normal) config validation")
		return
	}

	switch runMode {
	case SyncGatewayRunModeNormal:
		// if index writer == true for any databases, panic
		if config.HasAnyIndexWriterConfiguredDatabases() {
			base.LogPanic("SG is running in normal mode but there are databases configured as index writers")
		}
	case SyncGatewayRunModeAccel:
		// if index writer != true for any databases, panic
		if config.HasAnyIndexReaderConfiguredDatabases() {
			base.LogPanic("SG is running in sg-accelerator mode but there are databases configured as index readers")
		}
	}

}

// Main entry point for a simple server; you can have your main() function just call this.
// It parses command-line flags, reads the optional configuration file, then starts the server.
func ServerMain(runMode SyncGatewayRunMode) {
	ParseCommandLine(runMode)
	ValidateConfigOrPanic(runMode)
	RunServer(config)
}
