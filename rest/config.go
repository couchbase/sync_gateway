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
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/pkg/errors"

	// Register profiling handlers (see Go docs)
	_ "net/http/pprof"
)

var (
	DefaultInterface              = ":4984"
	DefaultAdminInterface         = "127.0.0.1:4985" // Only accessible on localhost!
	DefaultMetricsInterface       = "127.0.0.1:4986" // Only accessible on localhost!
	DefaultServer                 = "walrus:"
	DefaultPool                   = "default"
	DefaultMinimumTLSVersionConst = tls.VersionTLS10

	// The value of defaultLogFilePath is populated by --defaultLogFilePath in ParseCommandLine()
	defaultLogFilePath string
)

var config *ServerConfig

const (
	eeOnlyWarningMsg   = "EE only configuration option %s=%v - Reverting to default value for CE: %v"
	minValueErrorMsg   = "minimum value for %s is: %v"
	rangeValueErrorMsg = "valid range for %s is: %s"

	// Default value of ServerConfig.MaxIncomingConnections
	DefaultMaxIncomingConnections = 0

	// Default value of ServerConfig.MaxFileDescriptors
	DefaultMaxFileDescriptors uint64 = 5000

	// Default number of index replicas
	DefaultNumIndexReplicas = uint(1)
)

// JSON object that defines the server configuration.
type ServerConfig struct {
	TLSMinVersion              *string                  `json:"tls_minimum_version,omitempty"`    // Set TLS Version
	Interface                  *string                  `json:",omitempty"`                       // Interface to bind REST API to, default ":4984"
	SSLCert                    *string                  `json:",omitempty"`                       // Path to SSL cert file, or nil
	SSLKey                     *string                  `json:",omitempty"`                       // Path to SSL private key file, or nil
	ServerReadTimeout          *int                     `json:",omitempty"`                       // maximum duration.Second before timing out read of the HTTP(S) request
	ServerWriteTimeout         *int                     `json:",omitempty"`                       // maximum duration.Second before timing out write of the HTTP(S) response
	ReadHeaderTimeout          *int                     `json:",omitempty"`                       // The amount of time allowed to read request headers.
	IdleTimeout                *int                     `json:",omitempty"`                       // The maximum amount of time to wait for the next request when keep-alives are enabled.
	AdminInterface             *string                  `json:",omitempty"`                       // Interface to bind admin API to, default "localhost:4985"
	AdminUI                    *string                  `json:",omitempty"`                       // Path to Admin HTML page, if omitted uses bundled HTML
	ProfileInterface           *string                  `json:",omitempty"`                       // Interface to bind Go profile API to (no default)
	ConfigServer               *string                  `json:",omitempty"`                       // URL of config server (for dynamic db discovery)
	Facebook                   *FacebookConfig          `json:",omitempty"`                       // Configuration for Facebook validation
	Google                     *GoogleConfig            `json:",omitempty"`                       // Configuration for Google validation
	CORS                       *CORSConfig              `json:",omitempty"`                       // Configuration for allowing CORS
	DeprecatedLog              []string                 `json:"log,omitempty"`                    // Log keywords to enable
	DeprecatedLogFilePath      *string                  `json:"logFilePath,omitempty"`            // Path to log file, if missing write to stderr
	Logging                    *base.LoggingConfig      `json:",omitempty"`                       // Configuration for logging with optional log file rotation
	Pretty                     bool                     `json:",omitempty"`                       // Pretty-print JSON responses?
	DeploymentID               *string                  `json:",omitempty"`                       // Optional customer/deployment ID for stats reporting
	StatsReportInterval        *float64                 `json:",omitempty"`                       // Optional stats report interval (0 to disable)
	CouchbaseKeepaliveInterval *int                     `json:",omitempty"`                       // TCP keep-alive interval between SG and Couchbase server
	SlowQueryWarningThreshold  *int                     `json:",omitempty"`                       // Log warnings if N1QL queries take this many ms
	MaxIncomingConnections     *int                     `json:",omitempty"`                       // Max # of incoming HTTP connections to accept
	MaxFileDescriptors         *uint64                  `json:",omitempty"`                       // Max # of open file descriptors (RLIMIT_NOFILE)
	CompressResponses          *bool                    `json:",omitempty"`                       // If false, disables compression of HTTP responses
	Databases                  DbConfigMap              `json:",omitempty"`                       // Pre-configured databases, mapped by name
	Replications               []*ReplicateV1Config     `json:",omitempty"`                       // sg-replicate replication definitions
	MaxHeartbeat               uint64                   `json:",omitempty"`                       // Max heartbeat value for _changes request (seconds)
	ClusterConfig              *ClusterConfig           `json:"cluster_config,omitempty"`         // Bucket and other config related to CBGT
	Unsupported                *UnsupportedServerConfig `json:"unsupported,omitempty"`            // Config for unsupported features
	ReplicatorCompression      *int                     `json:"replicator_compression,omitempty"` // BLIP data compression level (0-9)
	BcryptCost                 int                      `json:"bcrypt_cost,omitempty"`            // bcrypt cost to use for password hashes - Default: bcrypt.DefaultCost
	MetricsInterface           *string                  `json:"metrics_interface,omitempty"`      // Interface to bind metrics to, default ":4986"
}

// Bucket configuration elements - used by db, index
type BucketConfig struct {
	Server     *string `json:"server,omitempty"`      // Couchbase server URL
	Pool       *string `json:"pool,omitempty"`        // Couchbase pool name, default "default"
	Bucket     *string `json:"bucket,omitempty"`      // Bucket name
	Username   string  `json:"username,omitempty"`    // Username for authenticating to server
	Password   string  `json:"password,omitempty"`    // Password for authenticating to server
	CertPath   string  `json:"certpath,omitempty"`    // Cert path (public key) for X.509 bucket auth
	KeyPath    string  `json:"keypath,omitempty"`     // Key path (private key) for X.509 bucket auth
	CACertPath string  `json:"cacertpath,omitempty"`  // Root CA cert path for X.509 bucket auth
	KvTLSPort  int     `json:"kv_tls_port,omitempty"` // Memcached TLS port, if not default (11207)
}

func (bc *BucketConfig) MakeBucketSpec() base.BucketSpec {

	server := "http://localhost:8091"
	pool := "default"
	bucketName := ""
	tlsPort := 11207

	if bc.Server != nil {
		server = *bc.Server
	}
	if bc.Pool != nil {
		pool = *bc.Pool
	}
	if bc.Bucket != nil {
		bucketName = *bc.Bucket
	}

	if bc.KvTLSPort != 0 {
		tlsPort = bc.KvTLSPort
	}

	return base.BucketSpec{
		Server:     server,
		PoolName:   pool,
		BucketName: bucketName,
		Keypath:    bc.KeyPath,
		Certpath:   bc.CertPath,
		CACertPath: bc.CACertPath,
		KvTLSPort:  tlsPort,
		Auth:       bc,
	}
}

// Implementation of AuthHandler interface for BucketConfig
func (bucketConfig *BucketConfig) GetCredentials() (username string, password string, bucketname string) {
	return base.TransformBucketCredentials(bucketConfig.Username, bucketConfig.Password, *bucketConfig.Bucket)
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
	Name                             string                           `json:"name,omitempty"`                                 // Database name in REST API (stored as key in JSON)
	Sync                             *string                          `json:"sync,omitempty"`                                 // Sync function defines which users can see which data
	Users                            map[string]*db.PrincipalConfig   `json:"users,omitempty"`                                // Initial user accounts
	Roles                            map[string]*db.PrincipalConfig   `json:"roles,omitempty"`                                // Initial roles
	RevsLimit                        *uint32                          `json:"revs_limit,omitempty"`                           // Max depth a document's revision tree can grow to
	AutoImport                       interface{}                      `json:"import_docs,omitempty"`                          // Whether to automatically import Couchbase Server docs into SG.  Xattrs must be enabled.  true or "continuous" both enable this.
	ImportPartitions                 *uint16                          `json:"import_partitions,omitempty"`                    // Number of partitions for import sharding.  Impacts the total DCP concurrency for import
	ImportFilter                     *string                          `json:"import_filter,omitempty"`                        // Filter function (import)
	ImportBackupOldRev               bool                             `json:"import_backup_old_rev"`                          // Whether import should attempt to create a temporary backup of the previous revision body, when available.
	EventHandlers                    interface{}                      `json:"event_handlers,omitempty"`                       // Event handlers (webhook)
	FeedType                         string                           `json:"feed_type,omitempty"`                            // Feed type - "DCP" or "TAP"; defaults based on Couchbase server version
	AllowEmptyPassword               bool                             `json:"allow_empty_password,omitempty"`                 // Allow empty passwords?  Defaults to false
	CacheConfig                      *CacheConfig                     `json:"cache,omitempty"`                                // Cache settings
	DeprecatedRevCacheSize           *uint32                          `json:"rev_cache_size,omitempty"`                       // Maximum number of revisions to store in the revision cache (deprecated, CBG-356)
	StartOffline                     bool                             `json:"offline,omitempty"`                              // start the DB in the offline state, defaults to false
	Unsupported                      db.UnsupportedOptions            `json:"unsupported,omitempty"`                          // Config for unsupported features
	Deprecated                       DeprecatedOptions                `json:"deprecated,omitempty"`                           // Config for Deprecated features
	OIDCConfig                       *auth.OIDCOptions                `json:"oidc,omitempty"`                                 // Config properties for OpenID Connect authentication
	OldRevExpirySeconds              *uint32                          `json:"old_rev_expiry_seconds,omitempty"`               // The number of seconds before old revs are removed from CBS bucket
	ViewQueryTimeoutSecs             *uint32                          `json:"view_query_timeout_secs,omitempty"`              // The view query timeout in seconds
	LocalDocExpirySecs               *uint32                          `json:"local_doc_expiry_secs,omitempty"`                // The _local doc expiry time in seconds
	EnableXattrs                     *bool                            `json:"enable_shared_bucket_access,omitempty"`          // Whether to use extended attributes to store _sync metadata
	SecureCookieOverride             *bool                            `json:"session_cookie_secure,omitempty"`                // Override cookie secure flag
	SessionCookieName                string                           `json:"session_cookie_name"`                            // Custom per-database session cookie name
	SessionCookieHTTPOnly            bool                             `json:"session_cookie_http_only"`                       // HTTP only cookies
	AllowConflicts                   *bool                            `json:"allow_conflicts,omitempty"`                      // False forbids creating conflicts
	NumIndexReplicas                 *uint                            `json:"num_index_replicas"`                             // Number of GSI index replicas used for core indexes
	UseViews                         bool                             `json:"use_views"`                                      // Force use of views instead of GSI
	SendWWWAuthenticateHeader        *bool                            `json:"send_www_authenticate_header,omitempty"`         // If false, disables setting of 'WWW-Authenticate' header in 401 responses
	BucketOpTimeoutMs                *uint32                          `json:"bucket_op_timeout_ms,omitempty"`                 // How long bucket ops should block returning "operation timed out". If nil, uses GoCB default.  GoCB buckets only.
	DeltaSync                        *DeltaSyncConfig                 `json:"delta_sync,omitempty"`                           // Config for delta sync
	CompactIntervalDays              *float32                         `json:"compact_interval_days,omitempty"`                // Interval between scheduled compaction runs (in days) - 0 means don't run
	SGReplicateEnabled               *bool                            `json:"sgreplicate_enabled,omitempty"`                  // When false, node will not be assigned replications
	SGReplicateWebsocketPingInterval *int                             `json:"sgreplicate_websocket_heartbeat_secs,omitempty"` // If set, uses this duration as a custom heartbeat interval for websocket ping frames
	Replications                     map[string]*db.ReplicationConfig `json:"replications,omitempty"`                         // sg-replicate replication definitions
	ServeInsecureAttachmentTypes     bool                             `json:"serve_insecure_attachment_types,omitempty"`      // Attachment content type will bypass the content-disposition handling, default false
}

type DeltaSyncConfig struct {
	Enabled          *bool   `json:"enabled,omitempty"`             // Whether delta sync is enabled (requires EE)
	RevMaxAgeSeconds *uint32 `json:"rev_max_age_seconds,omitempty"` // The number of seconds deltas for old revs are available for
}

type DeprecatedOptions struct {
}

type DbConfigMap map[string]*DbConfig

type ReplConfigMap map[string]*ReplicateV1Config

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
	RevCacheConfig     *RevCacheConfig     `json:"rev_cache"`     // Revision Cache Config Settings
	ChannelCacheConfig *ChannelCacheConfig `json:"channel_cache"` // Channel Cache Config Settings
	DeprecatedCacheConfig
}

// ***************************************************************
//	Kept around for CBG-356 backwards compatability
// ***************************************************************
type DeprecatedCacheConfig struct {
	DeprecatedCachePendingSeqMaxWait *uint32 `json:"max_wait_pending,omitempty"`         // Max wait for pending sequence before skipping
	DeprecatedCachePendingSeqMaxNum  *int    `json:"max_num_pending,omitempty"`          // Max number of pending sequences before skipping
	DeprecatedCacheSkippedSeqMaxWait *uint32 `json:"max_wait_skipped,omitempty"`         // Max wait for skipped sequence before abandoning
	DeprecatedEnableStarChannel      *bool   `json:"enable_star_channel,omitempty"`      // Enable star channel
	DeprecatedChannelCacheMaxLength  *int    `json:"channel_cache_max_length,omitempty"` // Maximum number of entries maintained in cache per channel
	DeprecatedChannelCacheMinLength  *int    `json:"channel_cache_min_length,omitempty"` // Minimum number of entries maintained in cache per channel
	DeprecatedChannelCacheAge        *int    `json:"channel_cache_expiry,omitempty"`     // Time (seconds) to keep entries in cache beyond the minimum retained
}

type RevCacheConfig struct {
	Size       *uint32 `json:"size,omitempty"`        // Maximum number of revisions to store in the revision cache
	ShardCount *uint16 `json:"shard_count,omitempty"` // Number of shards the rev cache should be split into
}

type ChannelCacheConfig struct {
	MaxNumber            *int    `json:"max_number,omitempty"`                 // Maximum number of channel caches which will exist at any one point
	HighWatermarkPercent *int    `json:"compact_high_watermark_pct,omitempty"` // High watermark for channel cache eviction (percent)
	LowWatermarkPercent  *int    `json:"compact_low_watermark_pct,omitempty"`  // Low watermark for channel cache eviction (percent)
	MaxWaitPending       *uint32 `json:"max_wait_pending,omitempty"`           // Max wait for pending sequence before skipping
	MaxNumPending        *int    `json:"max_num_pending,omitempty"`            // Max number of pending sequences before skipping
	MaxWaitSkipped       *uint32 `json:"max_wait_skipped,omitempty"`           // Max wait for skipped sequence before abandoning
	EnableStarChannel    *bool   `json:"enable_star_channel,omitempty"`        // Enable star channel
	MaxLength            *int    `json:"max_length,omitempty"`                 // Maximum number of entries maintained in cache per channel
	MinLength            *int    `json:"min_length,omitempty"`                 // Minimum number of entries maintained in cache per channel
	ExpirySeconds        *int    `json:"expiry_seconds,omitempty"`             // Time (seconds) to keep entries in cache beyond the minimum retained
	QueryLimit           *int    `json:"query_limit,omitempty"`                // Limit used for channel queries, if not specified by client
}

type UnsupportedServerConfig struct {
	Http2Config           *Http2Config `json:"http2,omitempty"`               // Config settings for HTTP2
	StatsLogFrequencySecs *uint        `json:"stats_log_freq_secs,omitempty"` // How often should stats be written to stats logs
	UseStdlibJSON         *bool        `json:"use_stdlib_json,omitempty"`     // Bypass the jsoniter package and use Go's stdlib instead
}

type Http2Config struct {
	Enabled *bool `json:"enabled,omitempty"` // Whether HTTP2 support is enabled
}

func GetTLSVersionFromString(stringV *string) uint16 {
	if stringV != nil {
		switch *stringV {
		case "tlsv1":
			return tls.VersionTLS10
		case "tlsv1.1":
			return tls.VersionTLS11
		case "tlsv1.2":
			return tls.VersionTLS12
		case "tlsv1.3":
			return tls.VersionTLS13
		}
	}
	return uint16(DefaultMinimumTLSVersionConst)
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

	return err
}

func (dbConfig *DbConfig) AutoImportEnabled() (bool, error) {
	if dbConfig.AutoImport == nil {
		return base.DefaultAutoImport, nil
	}

	if b, ok := dbConfig.AutoImport.(bool); ok {
		return b, nil
	}

	str, ok := dbConfig.AutoImport.(string)
	if ok && str == "continuous" {
		base.Warnf(`Using deprecated config value for "import_docs": "continuous". Use "import_docs": true instead.`)
		return true, nil
	}

	return false, fmt.Errorf("Unrecognized value for import_docs: %#v. Valid values are true and false.", dbConfig.AutoImport)
}

func (dbConfig *DbConfig) validate() []error {
	return dbConfig.validateVersion(base.IsEnterpriseEdition())
}

func (dbConfig *DbConfig) validateVersion(isEnterpriseEdition bool) []error {

	errorMessages := make([]error, 0)

	// Make sure a non-zero compact_interval_days config is within the valid range
	if val := dbConfig.CompactIntervalDays; val != nil && *val != 0 &&
		(*val < db.CompactIntervalMinDays || *val > db.CompactIntervalMaxDays) {
		errorMessages = append(errorMessages, fmt.Errorf(rangeValueErrorMsg, "compact_interval_days", fmt.Sprintf("%g-%g", db.CompactIntervalMinDays, db.CompactIntervalMaxDays)))
	}

	if dbConfig.CacheConfig != nil {

		if dbConfig.CacheConfig.ChannelCacheConfig != nil {

			// EE: channel cache
			if !isEnterpriseEdition {
				if val := dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber; val != nil {
					base.Warnf(eeOnlyWarningMsg, "cache.channel_cache.max_number", *val, db.DefaultChannelCacheMaxNumber)
					dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber = nil
				}
				if val := dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent; val != nil {
					base.Warnf(eeOnlyWarningMsg, "cache.channel_cache.compact_high_watermark_pct", *val, db.DefaultCompactHighWatermarkPercent)
					dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent = nil
				}
				if val := dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent; val != nil {
					base.Warnf(eeOnlyWarningMsg, "cache.channel_cache.compact_low_watermark_pct", *val, db.DefaultCompactLowWatermarkPercent)
					dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent = nil
				}
			}

			if dbConfig.CacheConfig.ChannelCacheConfig.MaxNumPending != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxNumPending < 1 {
				errorMessages = append(errorMessages, fmt.Errorf(minValueErrorMsg, "cache.channel_cache.max_num_pending", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitPending != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitPending < 1 {
				errorMessages = append(errorMessages, fmt.Errorf(minValueErrorMsg, "cache.channel_cache.max_wait_pending", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitSkipped != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitSkipped < 1 {
				errorMessages = append(errorMessages, fmt.Errorf(minValueErrorMsg, "cache.channel_cache.max_wait_skipped", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxLength != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxLength < 1 {
				errorMessages = append(errorMessages, fmt.Errorf(minValueErrorMsg, "cache.channel_cache.max_length", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MinLength != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MinLength < 1 {
				errorMessages = append(errorMessages, fmt.Errorf(minValueErrorMsg, "cache.channel_cache.min_length", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.ExpirySeconds != nil && *dbConfig.CacheConfig.ChannelCacheConfig.ExpirySeconds < 1 {
				errorMessages = append(errorMessages, fmt.Errorf(minValueErrorMsg, "cache.channel_cache.expiry_seconds", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber < db.MinimumChannelCacheMaxNumber {
				errorMessages = append(errorMessages, fmt.Errorf(minValueErrorMsg, "cache.channel_cache.max_number", db.MinimumChannelCacheMaxNumber))
			}

			// Compact watermark validation
			hwm := db.DefaultCompactHighWatermarkPercent
			lwm := db.DefaultCompactLowWatermarkPercent
			if dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent != nil {
				if *dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent < 1 || *dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent > 100 {
					errorMessages = append(errorMessages, fmt.Errorf(rangeValueErrorMsg, "cache.channel_cache.compact_high_watermark_pct", "0-100"))
				}
				hwm = *dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent != nil {
				if *dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent < 1 || *dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent > 100 {
					errorMessages = append(errorMessages, fmt.Errorf(rangeValueErrorMsg, "cache.channel_cache.compact_low_watermark_pct", "0-100"))
				}
				lwm = *dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent
			}
			if lwm >= hwm {
				errorMessages = append(errorMessages, fmt.Errorf("cache.channel_cache.compact_high_watermark_pct (%v) must be greater than cache.channel_cache.compact_low_watermark_pct (%v)", hwm, lwm))
			}

		}

		if dbConfig.CacheConfig.RevCacheConfig != nil {
			// EE: disable revcache
			revCacheSize := dbConfig.CacheConfig.RevCacheConfig.Size
			if !isEnterpriseEdition && revCacheSize != nil && *revCacheSize == 0 {
				base.Warnf(eeOnlyWarningMsg, "cache.rev_cache.size", *revCacheSize, db.DefaultRevisionCacheSize)
				dbConfig.CacheConfig.RevCacheConfig.Size = nil
			}

			if dbConfig.CacheConfig.RevCacheConfig.ShardCount != nil {
				if *dbConfig.CacheConfig.RevCacheConfig.ShardCount < 1 {
					errorMessages = append(errorMessages, fmt.Errorf(minValueErrorMsg, "cache.rev_cache.shard_count", 1))
				}
			}
		}
	}

	// EE: delta sync
	if !isEnterpriseEdition && dbConfig.DeltaSync != nil && dbConfig.DeltaSync.Enabled != nil {
		base.Warnf(eeOnlyWarningMsg, "delta_sync.enabled", *dbConfig.DeltaSync.Enabled, false)
		dbConfig.DeltaSync.Enabled = nil
	}

	// Import validation
	autoImportEnabled, err := dbConfig.AutoImportEnabled()
	if err != nil {
		errorMessages = append(errorMessages, err)
	}
	if dbConfig.FeedType == base.TapFeedType && autoImportEnabled == true {
		errorMessages = append(errorMessages, fmt.Errorf("Invalid configuration for Sync Gw. TAP feed type can not be used with auto-import"))
	}

	if dbConfig.AutoImport != nil && autoImportEnabled && !dbConfig.UseXattrs() {
		errorMessages = append(errorMessages, fmt.Errorf("Invalid configuration - import_docs enabled, but enable_shared_bucket_access not enabled"))
	}

	if dbConfig.ImportPartitions != nil {
		if !isEnterpriseEdition {
			base.Warnf(eeOnlyWarningMsg, "import_partitions", *dbConfig.ImportPartitions, nil)
			dbConfig.ImportPartitions = nil
		} else if !dbConfig.UseXattrs() {
			errorMessages = append(errorMessages, fmt.Errorf("Invalid configuration - import_partitions set, but enable_shared_bucket_access not enabled"))
		} else if !autoImportEnabled {
			errorMessages = append(errorMessages, fmt.Errorf("Invalid configuration - import_partitions set, but import_docs disabled"))
		} else if *dbConfig.ImportPartitions < 1 || *dbConfig.ImportPartitions > 1024 {
			errorMessages = append(errorMessages, fmt.Errorf(rangeValueErrorMsg, "import_partitions", "1-1024"))
		}
	}

	return errorMessages

}

func (dbConfig *DbConfig) validateSgDbConfig() []error {

	errorMessages := make([]error, 0)

	if err := dbConfig.validate(); err != nil {
		errorMessages = append(errorMessages, err...)
	}

	return errorMessages

}

// Checks for deprecated cache config options and if they are set it will return a warning. If the old one is set and
// the new one is not set it will set the new to the old value. If they are both set it will still give the warning but
// will choose the new value.
func (dbConfig *DbConfig) deprecatedConfigCacheFallback() (warnings []string) {

	warningMsgFmt := "Using deprecated config option: %q. Use %q instead."

	if dbConfig.CacheConfig == nil {
		dbConfig.CacheConfig = &CacheConfig{}
	}

	if dbConfig.CacheConfig.RevCacheConfig == nil {
		dbConfig.CacheConfig.RevCacheConfig = &RevCacheConfig{}
	}

	if dbConfig.CacheConfig.ChannelCacheConfig == nil {
		dbConfig.CacheConfig.ChannelCacheConfig = &ChannelCacheConfig{}
	}

	if dbConfig.DeprecatedRevCacheSize != nil {
		if dbConfig.CacheConfig.RevCacheConfig.Size == nil {
			dbConfig.CacheConfig.RevCacheConfig.Size = dbConfig.DeprecatedRevCacheSize
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "rev_cache_size", "cache.rev_cache.size"))
	}

	if dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxWait != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitPending == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitPending = dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxWait
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "max_wait_pending", "cache.channel_cache.max_wait_pending"))
	}

	if dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxNum != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.MaxNumPending == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.MaxNumPending = dbConfig.CacheConfig.DeprecatedCachePendingSeqMaxNum
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "max_num_pending", "cache.channel_cache.max_num_pending"))
	}

	if dbConfig.CacheConfig.DeprecatedCacheSkippedSeqMaxWait != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitSkipped == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitSkipped = dbConfig.CacheConfig.DeprecatedCacheSkippedSeqMaxWait
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "max_wait_skipped", "cache.channel_cache.max_wait_skipped"))
	}

	if dbConfig.CacheConfig.DeprecatedEnableStarChannel != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.EnableStarChannel == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.EnableStarChannel = dbConfig.CacheConfig.DeprecatedEnableStarChannel
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "enable_star_channel", "cache.channel_cache.enable_star_channel"))
	}

	if dbConfig.CacheConfig.DeprecatedChannelCacheMaxLength != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.MaxLength == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.MaxLength = dbConfig.CacheConfig.DeprecatedChannelCacheMaxLength
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "channel_cache_max_length", "cache.channel_cache.max_length"))
	}

	if dbConfig.CacheConfig.DeprecatedChannelCacheMinLength != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.MinLength == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.MinLength = dbConfig.CacheConfig.DeprecatedChannelCacheMinLength
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "channel_cache_min_length", "cache.channel_cache.min_length"))
	}

	if dbConfig.CacheConfig.DeprecatedChannelCacheAge != nil {
		if dbConfig.CacheConfig.ChannelCacheConfig.ExpirySeconds == nil {
			dbConfig.CacheConfig.ChannelCacheConfig.ExpirySeconds = dbConfig.CacheConfig.DeprecatedChannelCacheAge
		}
		warnings = append(warnings, fmt.Sprintf(warningMsgFmt, "channel_cache_expiry", "cache.channel_cache.expiry_seconds"))
	}

	return warnings

}

// Implementation of AuthHandler interface for DbConfig
func (dbConfig *DbConfig) GetCredentials() (string, string, string) {
	return base.TransformBucketCredentials(dbConfig.Username, dbConfig.Password, *dbConfig.Bucket)
}

func (dbConfig *DbConfig) ConflictsAllowed() *bool {
	if dbConfig.AllowConflicts != nil {
		return dbConfig.AllowConflicts
	}
	return base.BoolPtr(base.DefaultAllowConflicts)
}

func (dbConfig *DbConfig) UseXattrs() bool {
	if dbConfig.EnableXattrs != nil {
		return *dbConfig.EnableXattrs
	}
	return base.DefaultUseXattrs
}

// Implementation of AuthHandler interface for ClusterConfig
func (clusterConfig *ClusterConfig) GetCredentials() (string, string, string) {
	return base.TransformBucketCredentials(clusterConfig.Username, clusterConfig.Password, *clusterConfig.Bucket)
}

// LoadServerConfig loads a ServerConfig from either a JSON file or from a URL
func LoadServerConfig(path string) (config *ServerConfig, err error) {
	var dataReadCloser io.ReadCloser

	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		resp, err := http.Get(path)
		if err != nil {
			return nil, err
		} else if resp.StatusCode >= 300 {
			return nil, base.HTTPErrorf(resp.StatusCode, http.StatusText(resp.StatusCode))
		}
		dataReadCloser = resp.Body
	} else {
		dataReadCloser, err = os.Open(path)
		if err != nil {
			return nil, err
		}
	}

	defer func() { _ = dataReadCloser.Close() }()
	return readServerConfig(dataReadCloser)
}

// readServerConfig returns a validated ServerConfig from an io.Reader
func readServerConfig(r io.Reader) (config *ServerConfig, err error) {
	err = decodeAndSanitiseConfig(r, &config)
	return config, err
}

// decodeAndSanitiseConfig will sanitise a ServerConfig or dbConfig from an io.Reader and unmarshal it into the given config parameter.
func decodeAndSanitiseConfig(r io.Reader, config interface{}) (err error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	b = base.ConvertBackQuotedStrings(b)

	d := base.JSONDecoder(bytes.NewBuffer(b))
	d.DisallowUnknownFields()
	err = d.Decode(config)
	return base.WrapJSONUnknownFieldErr(err)
}

func (config *ServerConfig) setupAndValidateDatabases() []error {
	if config == nil {
		return nil
	}

	for name, dbConfig := range config.Databases {

		if err := dbConfig.setup(name); err != nil {
			return []error{err}
		}

		if err := dbConfig.validateSgDbConfig(); err != nil && len(err) > 0 {
			return err
		}
	}
	return nil
}

// validate validates the given server config and returns all invalid options as a slice of errors
func (config *ServerConfig) validate() []error {
	errorMessages := make([]error, 0)

	if config.Unsupported != nil && config.Unsupported.StatsLogFrequencySecs != nil {
		if *config.Unsupported.StatsLogFrequencySecs == 0 {
			// explicitly disabled
		} else if *config.Unsupported.StatsLogFrequencySecs < 10 {
			errorMessages = append(errorMessages, fmt.Errorf(minValueErrorMsg, "unsupported.stats_log_freq_secs", 10))
		}
	}

	return errorMessages
}

// setupAndValidateLogging sets up and validates logging,
// and returns a slice of defferred logs to execute later.
func (config *ServerConfig) SetupAndValidateLogging() (warnings []base.DeferredLogFn, err error) {

	if config.Logging == nil {
		config.Logging = &base.LoggingConfig{}
	}

	// populate values from deprecated logging config options if not set
	warnings = config.deprecatedConfigLoggingFallback()

	base.SetRedaction(config.Logging.RedactionLevel)

	warningsInit, err := config.Logging.Init(defaultLogFilePath)
	warnings = append(warnings, warningsInit...)
	if err != nil {
		return warnings, err
	}

	if config.Logging.DeprecatedDefaultLog == nil {
		config.Logging.DeprecatedDefaultLog = &base.LogAppenderConfig{}
	}

	return warnings, nil
}

// deprecatedConfigLoggingFallback will parse the ServerConfig and try to
// use older logging config options for backwards compatibility.
// It will return a slice of deferred warnings to log at a later time.
func (config *ServerConfig) deprecatedConfigLoggingFallback() (warnings []base.DeferredLogFn) {

	warningMsgFmt := "Using deprecated config option: %q. Use %q instead."

	if config.Logging.DeprecatedDefaultLog != nil {
		// Fall back to the old logging.["default"].LogFilePath option
		if config.Logging.LogFilePath == "" && config.Logging.DeprecatedDefaultLog.LogFilePath != nil {
			warnings = append(warnings, func() {
				base.Warnf(warningMsgFmt, `logging.["default"].LogFilePath`, "logging.log_file_path")
			})
			// Set the new LogFilePath to be the directory containing the old logfile, instead of the full path.
			config.Logging.LogFilePath = filepath.Dir(*config.Logging.DeprecatedDefaultLog.LogFilePath)
		}

		// Fall back to the old logging.["default"].LogKeys option
		if len(config.Logging.Console.LogKeys) == 0 && len(config.Logging.DeprecatedDefaultLog.LogKeys) > 0 {
			warnings = append(warnings, func() {
				base.Warnf(warningMsgFmt, `logging.["default"].LogKeys`, "logging.console.log_keys")
			})
			config.Logging.Console.LogKeys = config.Logging.DeprecatedDefaultLog.LogKeys
		}

		// Fall back to the old logging.["default"].LogLevel option
		if config.Logging.Console.LogLevel == nil && config.Logging.DeprecatedDefaultLog.LogLevel != 0 {
			warnings = append(warnings, func() {
				base.Warnf(warningMsgFmt, `logging.["default"].LogLevel`, "logging.console.log_level")
			})
			config.Logging.Console.LogLevel = base.ToLogLevel(config.Logging.DeprecatedDefaultLog.LogLevel)
		}
	}

	// Fall back to the old LogFilePath option
	if config.Logging.LogFilePath == "" && config.DeprecatedLogFilePath != nil {
		warnings = append(warnings, func() {
			base.Warnf(warningMsgFmt, "logFilePath", "logging.log_file_path")
		})
		config.Logging.LogFilePath = *config.DeprecatedLogFilePath
	}

	// Fall back to the old Log option
	if config.Logging.Console.LogKeys == nil && len(config.DeprecatedLog) > 0 {
		warnings = append(warnings, func() {
			base.Warnf(warningMsgFmt, "log", "logging.console.log_keys")
		})
		config.Logging.Console.LogKeys = config.DeprecatedLog
	}

	return warnings
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

// Reads the command line flags and the optional config file.
func ParseCommandLine(args []string, handling flag.ErrorHandling) (*ServerConfig, error) {
	flagSet := flag.NewFlagSet(args[0], handling)
	addr := flagSet.String("interface", DefaultInterface, "Address to bind to")
	authAddr := flagSet.String("adminInterface", DefaultAdminInterface, "Address to bind admin interface to")
	profAddr := flagSet.String("profileInterface", "", "Address to bind profile interface to")
	configServer := flagSet.String("configServer", "", "URL of server that can return database configs")
	deploymentID := flagSet.String("deploymentID", "", "Customer/project identifier for stats reporting")
	couchbaseURL := flagSet.String("url", DefaultServer, "Address of Couchbase server")
	poolName := flagSet.String("pool", DefaultPool, "Name of pool")
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
	var config *ServerConfig
	var err error

	if defaultLogFilePathFlag != nil {
		defaultLogFilePath = *defaultLogFilePathFlag
	}

	if flagSet.NArg() > 0 {
		// Read the configuration file(s), if any:
		for _, filename := range flagSet.Args() {
			newConfig, newConfigErr := LoadServerConfig(filename)

			if errors.Cause(newConfigErr) == base.ErrUnknownField {
				// Delay returning this error so we can continue with other setup
				err = errors.WithMessage(newConfigErr, fmt.Sprintf("Error reading config file %s", filename))
			} else if newConfigErr != nil {
				return config, errors.WithMessage(newConfigErr, fmt.Sprintf("Error reading config file %s", filename))
			}

			if config == nil {
				config = newConfig
			} else {
				if err := config.MergeWith(newConfig); err != nil {
					return config, errors.WithMessage(err, fmt.Sprintf("Error reading config file %s", filename))
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

		// If the interfaces were not specified in either the config file or
		// on the command line, set them to the default values
		if config.Interface == nil {
			config.Interface = &DefaultInterface
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
		if *addr == DefaultInterface {
			*addr = "localhost:4984"
		}

		config = &ServerConfig{
			Interface:        addr,
			AdminInterface:   authAddr,
			ProfileInterface: profAddr,
			Pretty:           *pretty,
			ConfigServer:     configServer,
			Logging: &base.LoggingConfig{
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
						Pool:       poolName,
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

	return config, err
}

func SetMaxFileDescriptors(maxP *uint64) error {
	maxFDs := DefaultMaxFileDescriptors
	if maxP != nil {
		maxFDs = *maxP
	}
	_, err := base.SetMaxFileDescriptors(maxFDs)
	if err != nil {
		base.Errorf("Error setting MaxFileDescriptors to %d: %v", maxFDs, err)
		return err
	}
	return nil
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

	tlsMinVersion := GetTLSVersionFromString(config.TLSMinVersion)

	err := base.ListenAndServeHTTP(
		addr,
		maxConns,
		config.SSLCert,
		config.SSLKey,
		handler,
		config.ServerReadTimeout,
		config.ServerWriteTimeout,
		config.ReadHeaderTimeout,
		config.IdleTimeout,
		http2Enabled,
		tlsMinVersion,
	)
	if err != nil {
		base.Fatalf("Failed to start HTTP server on %s: %v", base.UD(addr), err)
	}
}

// Starts and runs the server given its configuration. (This function never returns.)
func RunServer(config *ServerConfig) {
	PrettyPrint = config.Pretty

	base.Infof(base.KeyAll, "Logging: Console level: %v", base.ConsoleLogLevel())
	base.Infof(base.KeyAll, "Logging: Console keys: %v", base.ConsoleLogKey().EnabledLogKeys())
	base.Infof(base.KeyAll, "Logging: Redaction level: %s", config.Logging.RedactionLevel)

	if os.Getenv("GOMAXPROCS") == "" && runtime.GOMAXPROCS(0) == 1 {
		cpus := runtime.NumCPU()
		if cpus > 1 {
			runtime.GOMAXPROCS(cpus)
			base.Infof(base.KeyAll, "Configured Go to use all %d CPUs; setenv GOMAXPROCS to override this", cpus)
		}
	}

	_ = SetMaxFileDescriptors(config.MaxFileDescriptors)

	// Use the stdlib JSON package, if configured to do so
	if config.Unsupported != nil && config.Unsupported.UseStdlibJSON != nil && *config.Unsupported.UseStdlibJSON {
		base.Infof(base.KeyAll, "Using the stdlib JSON package")
		base.UseStdlibJSON = true
	}

	// Set global bcrypt cost if configured
	if config.BcryptCost > 0 {
		if err := auth.SetBcryptCost(config.BcryptCost); err != nil {
			base.Fatalf("Configuration error: %v", err)
		}
	}

	if config.MetricsInterface == nil {
		config.MetricsInterface = &DefaultMetricsInterface
	}

	sc := NewServerContext(config)
	for _, dbConfig := range config.Databases {
		if _, err := sc.AddDatabaseFromConfig(dbConfig); err != nil {
			base.Fatalf("Error opening database %s: %v", base.MD(dbConfig.Name), err)
		}
	}
	_ = validateServerContext(sc)

	if config.ProfileInterface != nil {
		//runtime.MemProfileRate = 10 * 1024
		base.Infof(base.KeyAll, "Starting profile server on %s", base.UD(*config.ProfileInterface))
		go func() {
			_ = http.ListenAndServe(*config.ProfileInterface, nil)
		}()
	}

	go sc.PostStartup()

	base.Consolef(base.LevelInfo, base.KeyAll, "Starting metrics server on %s")
	go config.Serve(*config.MetricsInterface, CreateMetricHandler(sc))

	base.Consolef(base.LevelInfo, base.KeyAll, "Starting admin server on %s", *config.AdminInterface)
	go config.Serve(*config.AdminInterface, CreateAdminHandler(sc))

	base.Consolef(base.LevelInfo, base.KeyAll, "Starting server on %s ...", *config.Interface)
	config.Serve(*config.Interface, CreatePublicHandler(sc))
}

func validateServerContext(sc *ServerContext) (errors []error) {
	bucketUUIDToDBContext := make(map[string][]*db.DatabaseContext, len(sc.databases_))
	for _, dbContext := range sc.databases_ {
		if uuid, err := dbContext.Bucket.UUID(); err == nil {
			bucketUUIDToDBContext[uuid] = append(bucketUUIDToDBContext[uuid], dbContext)
		}
	}
	sharedBuckets := sharedBuckets(bucketUUIDToDBContext)
	for _, sharedBucket := range sharedBuckets {
		sharedBucketError := &SharedBucketError{sharedBucket}
		errors = append(errors, sharedBucketError)
		messageFormat := "Bucket %q is shared among databases %s. " +
			"This may result in unexpected behaviour if security is not defined consistently."
		base.Warnf(messageFormat, base.MD(sharedBucket.bucketName), base.MD(sharedBucket.dbNames))
	}
	return errors
}

type sharedBucket struct {
	bucketName string
	dbNames    []string
}

type SharedBucketError struct {
	sharedBucket sharedBucket
}

func (e *SharedBucketError) Error() string {
	messageFormat := "Bucket %q is shared among databases %v. " +
		"This may result in unexpected behaviour if security is not defined consistently."
	return fmt.Sprintf(messageFormat, e.sharedBucket.bucketName, e.sharedBucket.dbNames)
}

func (e *SharedBucketError) GetSharedBucket() sharedBucket {
	return e.sharedBucket
}

// Returns a list of buckets that are being shared by multiple databases.
func sharedBuckets(dbContextMap map[string][]*db.DatabaseContext) (sharedBuckets []sharedBucket) {
	for _, dbContexts := range dbContextMap {
		if len(dbContexts) > 1 {
			var dbNames []string
			for _, dbContext := range dbContexts {
				dbNames = append(dbNames, dbContext.Name)
			}
			sharedBuckets = append(sharedBuckets, sharedBucket{dbContexts[0].Bucket.GetName(), dbNames})
		}
	}
	return sharedBuckets
}

func HandleSighup() {
	for logger, err := range base.RotateLogfiles() {
		if err != nil {
			base.Warnf("Error rotating %v: %v", logger, err)
		}
	}
}

func GetConfig() *ServerConfig {
	return config
}

func RegisterSignalHandler() {
	signalchannel := make(chan os.Signal, 1)
	signal.Notify(signalchannel, syscall.SIGHUP, os.Interrupt, os.Kill)

	go func() {
		for sig := range signalchannel {
			base.Infof(base.KeyAll, "Handling signal: %v", sig)
			switch sig {
			case syscall.SIGHUP:
				HandleSighup()
			case os.Interrupt, os.Kill:
				// Ensure log buffers are flushed before exiting.
				base.FlushLogBuffers()
				os.Exit(130) // 130 == exit code 128 + 2 (interrupt)
			}
		}
	}()
}

// Main entry point for a simple server; you can have your main() function just call this.
// It parses command-line flags, reads the optional configuration file, then starts the server.
func ServerMain() {
	RegisterSignalHandler()
	defer base.FatalPanicHandler()

	var unknownFieldsErr error

	var err error
	config, err = ParseCommandLine(os.Args, flag.ExitOnError)
	if errors.Cause(err) == base.ErrUnknownField {
		unknownFieldsErr = err
	} else if err != nil {
		base.Fatalf(err.Error())
	}

	// Logging config will now have been loaded from command line
	// or from a sync_gateway config file so we can validate the
	// configuration and setup logging now
	warnings, err := config.SetupAndValidateLogging()
	if err != nil {
		// If we didn't set up logging correctly, we *probably* can't log via normal means...
		// as a best-effort, last-ditch attempt, we'll log to stderr as well.
		log.Printf("[ERR] Error setting up logging: %v", err)
		base.Fatalf("Error setting up logging: %v", err)
	}

	// This is the earliest opportunity to log a startup indicator
	// that will be persisted in all log files.
	base.LogSyncGatewayVersion()

	// If we got an unknownFields error when reading the config
	// log and exit now we've tried setting up the logging.
	if unknownFieldsErr != nil {
		base.Fatalf(unknownFieldsErr.Error())
	}

	// Execute any deferred warnings from setup.
	for _, logFn := range warnings {
		logFn()
	}

	// Validation
	var errorMsgs = make([]error, 0)
	errorMsgs = append(errorMsgs, config.validate()...)
	errorMsgs = append(errorMsgs, config.setupAndValidateDatabases()...)
	if len(errorMsgs) > 0 {
		for _, err := range errorMsgs {
			base.Errorf("Error during config validation: %v", err)
		}
		base.Fatalf("Error(s) during config validation")
	}

	RunServer(config)
}
