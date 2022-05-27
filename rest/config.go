//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"golang.org/x/crypto/bcrypt"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

var (
	DefaultPublicInterface        = ":4984"
	DefaultAdminInterface         = "127.0.0.1:4985" // Only accessible on localhost!
	DefaultMetricsInterface       = "127.0.0.1:4986" // Only accessible on localhost!
	DefaultMinimumTLSVersionConst = tls.VersionTLS12

	// The value of defaultLogFilePath is populated by -defaultLogFilePath by command line flag from service scripts.
	defaultLogFilePath string
)

const (
	eeOnlyWarningMsg   = "EE only configuration option %s=%v - Reverting to default value for CE: %v"
	minValueErrorMsg   = "minimum value for %s is: %v"
	rangeValueErrorMsg = "valid range for %s is: %s"

	// Default value of LegacyServerConfig.MaxIncomingConnections
	DefaultMaxIncomingConnections = 0

	// Default value of LegacyServerConfig.MaxFileDescriptors
	DefaultMaxFileDescriptors uint64 = 5000

	// Default number of index replicas
	DefaultNumIndexReplicas = uint(1)

	DefaultUseTLSServer = true
)

// Bucket configuration elements - used by db, index
type BucketConfig struct {
	Server                *string `json:"server,omitempty"`                   // Couchbase server URL
	DeprecatedPool        *string `json:"pool,omitempty"`                     // Couchbase pool name - This is now deprecated and forced to be "default"
	Bucket                *string `json:"bucket,omitempty"`                   // Bucket name
	Username              string  `json:"username,omitempty"`                 // Username for authenticating to server
	Password              string  `json:"password,omitempty"`                 // Password for authenticating to server
	CertPath              string  `json:"certpath,omitempty"`                 // Cert path (public key) for X.509 bucket auth
	KeyPath               string  `json:"keypath,omitempty"`                  // Key path (private key) for X.509 bucket auth
	CACertPath            string  `json:"cacertpath,omitempty"`               // Root CA cert path for X.509 bucket auth
	KvTLSPort             int     `json:"kv_tls_port,omitempty"`              // Memcached TLS port, if not default (11207)
	MaxConcurrentQueryOps *int    `json:"max_concurrent_query_ops,omitempty"` // Max concurrent  query ops
}

func (dc *DbConfig) MakeBucketSpec() base.BucketSpec {
	bc := &dc.BucketConfig

	server := ""
	bucketName := ""
	tlsPort := 11207

	if bc.Server != nil {
		server = *bc.Server
	}
	if bc.Bucket != nil {
		bucketName = *bc.Bucket
	}

	if bc.KvTLSPort != 0 {
		tlsPort = bc.KvTLSPort
	}

	return base.BucketSpec{
		Server:                server,
		BucketName:            bucketName,
		Keypath:               bc.KeyPath,
		Certpath:              bc.CertPath,
		CACertPath:            bc.CACertPath,
		KvTLSPort:             tlsPort,
		Auth:                  bc,
		MaxConcurrentQueryOps: bc.MaxConcurrentQueryOps,
	}
}

// Implementation of AuthHandler interface for BucketConfig
func (bucketConfig *BucketConfig) GetCredentials() (username string, password string, bucketname string) {
	return base.TransformBucketCredentials(bucketConfig.Username, bucketConfig.Password, *bucketConfig.Bucket)
}

// DbConfig defines a database configuration used in a config file or the REST API.
type DbConfig struct {
	BucketConfig
	Scopes                           ScopesConfig                     `json:"scopes,omitempty"`                               // Scopes and collection specific config
	Name                             string                           `json:"name,omitempty"`                                 // Database name in REST API (stored as key in JSON)
	Sync                             *string                          `json:"sync,omitempty"`                                 // The sync function applied to write operations in the _default scope and collection
	Users                            map[string]*db.PrincipalConfig   `json:"users,omitempty"`                                // Initial user accounts
	Roles                            map[string]*db.PrincipalConfig   `json:"roles,omitempty"`                                // Initial roles
	RevsLimit                        *uint32                          `json:"revs_limit,omitempty"`                           // Max depth a document's revision tree can grow to
	AutoImport                       interface{}                      `json:"import_docs,omitempty"`                          // Whether to automatically import Couchbase Server docs into SG.  Xattrs must be enabled.  true or "continuous" both enable this.
	ImportPartitions                 *uint16                          `json:"import_partitions,omitempty"`                    // Number of partitions for import sharding.  Impacts the total DCP concurrency for import
	ImportFilter                     *string                          `json:"import_filter,omitempty"`                        // The import filter applied to import operations in the _default scope and collection
	ImportBackupOldRev               *bool                            `json:"import_backup_old_rev,omitempty"`                // Whether import should attempt to create a temporary backup of the previous revision body, when available.
	EventHandlers                    *EventHandlerConfig              `json:"event_handlers,omitempty"`                       // Event handlers (webhook)
	FeedType                         string                           `json:"feed_type,omitempty"`                            // Feed type - "DCP" or "TAP"; defaults based on Couchbase server version
	AllowEmptyPassword               *bool                            `json:"allow_empty_password,omitempty"`                 // Allow empty passwords?  Defaults to false
	CacheConfig                      *CacheConfig                     `json:"cache,omitempty"`                                // Cache settings
	DeprecatedRevCacheSize           *uint32                          `json:"rev_cache_size,omitempty"`                       // Maximum number of revisions to store in the revision cache (deprecated, CBG-356)
	StartOffline                     *bool                            `json:"offline,omitempty"`                              // start the DB in the offline state, defaults to false
	Unsupported                      *db.UnsupportedOptions           `json:"unsupported,omitempty"`                          // Config for unsupported features
	OIDCConfig                       *auth.OIDCOptions                `json:"oidc,omitempty"`                                 // Config properties for OpenID Connect authentication
	OldRevExpirySeconds              *uint32                          `json:"old_rev_expiry_seconds,omitempty"`               // The number of seconds before old revs are removed from CBS bucket
	ViewQueryTimeoutSecs             *uint32                          `json:"view_query_timeout_secs,omitempty"`              // The view query timeout in seconds
	LocalDocExpirySecs               *uint32                          `json:"local_doc_expiry_secs,omitempty"`                // The _local doc expiry time in seconds
	EnableXattrs                     *bool                            `json:"enable_shared_bucket_access,omitempty"`          // Whether to use extended attributes to store _sync metadata
	SecureCookieOverride             *bool                            `json:"session_cookie_secure,omitempty"`                // Override cookie secure flag
	SessionCookieName                string                           `json:"session_cookie_name,omitempty"`                  // Custom per-database session cookie name
	SessionCookieHTTPOnly            *bool                            `json:"session_cookie_http_only,omitempty"`             // HTTP only cookies
	AllowConflicts                   *bool                            `json:"allow_conflicts,omitempty"`                      // Deprecated: False forbids creating conflicts
	NumIndexReplicas                 *uint                            `json:"num_index_replicas,omitempty"`                   // Number of GSI index replicas used for core indexes
	UseViews                         *bool                            `json:"use_views,omitempty"`                            // Force use of views instead of GSI
	SendWWWAuthenticateHeader        *bool                            `json:"send_www_authenticate_header,omitempty"`         // If false, disables setting of 'WWW-Authenticate' header in 401 responses. Implicitly false if disable_password_auth is true.
	DisablePasswordAuth              bool                             `json:"disable_password_auth,omitempty"`                // If true, disables user/pass authentication, only permitting OIDC or guest access
	BucketOpTimeoutMs                *uint32                          `json:"bucket_op_timeout_ms,omitempty"`                 // How long bucket ops should block returning "operation timed out". If nil, uses GoCB default.  GoCB buckets only.
	SlowQueryWarningThresholdMs      *uint32                          `json:"slow_query_warning_threshold,omitempty"`         // Log warnings if N1QL queries take this many ms
	DeltaSync                        *DeltaSyncConfig                 `json:"delta_sync,omitempty"`                           // Config for delta sync
	CompactIntervalDays              *float32                         `json:"compact_interval_days,omitempty"`                // Interval between scheduled compaction runs (in days) - 0 means don't run
	SGReplicateEnabled               *bool                            `json:"sgreplicate_enabled,omitempty"`                  // When false, node will not be assigned replications
	SGReplicateWebsocketPingInterval *int                             `json:"sgreplicate_websocket_heartbeat_secs,omitempty"` // If set, uses this duration as a custom heartbeat interval for websocket ping frames
	Replications                     map[string]*db.ReplicationConfig `json:"replications,omitempty"`                         // sg-replicate replication definitions
	ServeInsecureAttachmentTypes     *bool                            `json:"serve_insecure_attachment_types,omitempty"`      // Attachment content type will bypass the content-disposition handling, default false
	QueryPaginationLimit             *int                             `json:"query_pagination_limit,omitempty"`               // Query limit to be used during pagination of large queries
	UserXattrKey                     string                           `json:"user_xattr_key,omitempty"`                       // Key of user xattr that will be accessible from the Sync Function. If empty the feature will be disabled.
	ClientPartitionWindowSecs        *int                             `json:"client_partition_window_secs,omitempty"`         // How long clients can remain offline for without losing replication metadata. Default 30 days (in seconds)
	Guest                            *db.PrincipalConfig              `json:"guest,omitempty"`                                // Guest user settings
}

type ScopesConfig map[string]ScopeConfig
type ScopeConfig struct {
	Collections CollectionsConfig `json:"collections,omitempty"` // Collection-specific config options.
}

type CollectionsConfig map[string]CollectionConfig
type CollectionConfig struct {
	SyncFn       *string `json:"sync,omitempty"`          // The sync function applied to write operations in this collection.
	ImportFilter *string `json:"import_filter,omitempty"` // The import filter applied to import operations in this collection.
}

type DeltaSyncConfig struct {
	Enabled          *bool   `json:"enabled,omitempty"`             // Whether delta sync is enabled (requires EE)
	RevMaxAgeSeconds *uint32 `json:"rev_max_age_seconds,omitempty"` // The number of seconds deltas for old revs are available for
}

type DbConfigMap map[string]*DbConfig

type EventHandlerConfig struct {
	MaxEventProc    uint           `json:"max_processes,omitempty"`    // Max concurrent event handling goroutines
	WaitForProcess  string         `json:"wait_for_process,omitempty"` // Max wait time when event queue is full (ms)
	DocumentChanged []*EventConfig `json:"document_changed,omitempty"` // Document changed
	DBStateChanged  []*EventConfig `json:"db_state_changed,omitempty"` // DB state change
}

type EventConfig struct {
	HandlerType string                 `json:"handler,omitempty"` // Handler type
	Url         string                 `json:"url,omitempty"`     // Url (webhook)
	Filter      string                 `json:"filter,omitempty"`  // Filter function (webhook)
	Timeout     *uint64                `json:"timeout,omitempty"` // Timeout (webhook)
	Options     map[string]interface{} `json:"options,omitempty"` // Options can be specified per-handler, and are specific to each type.
}

type CacheConfig struct {
	RevCacheConfig     *RevCacheConfig     `json:"rev_cache,omitempty"`     // Revision Cache Config Settings
	ChannelCacheConfig *ChannelCacheConfig `json:"channel_cache,omitempty"` // Channel Cache Config Settings
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
	DeprecatedQueryLimit *int    `json:"query_limit,omitempty"`                // Limit used for channel queries, if not specified by client DEPRECATED in favour of db.QueryPaginationLimit
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

// inheritFromBootstrap sets any empty Couchbase Server values from the given bootstrap config.
func (dbc *DbConfig) inheritFromBootstrap(b BootstrapConfig) {
	if dbc.Username == "" {
		dbc.Username = b.Username
	}
	if dbc.Password == "" {
		dbc.Password = b.Password
	}
	if dbc.CACertPath == "" {
		dbc.CACertPath = b.CACertPath
	}
	if dbc.CertPath == "" {
		dbc.CertPath = b.X509CertPath
	}
	if dbc.KeyPath == "" {
		dbc.KeyPath = b.X509KeyPath
	}
	if dbc.Server == nil || *dbc.Server == "" {
		dbc.Server = &b.Server
	}
}

func (dbConfig *DbConfig) setPerDatabaseCredentials(dbCredentials DatabaseCredentialsConfig) {
	// X.509 overrides username/password
	if dbCredentials.X509CertPath != "" || dbCredentials.X509KeyPath != "" {
		dbConfig.CertPath = dbCredentials.X509CertPath
		dbConfig.KeyPath = dbCredentials.X509KeyPath
		dbConfig.Username = ""
		dbConfig.Password = ""
	} else {
		dbConfig.Username = dbCredentials.Username
		dbConfig.Password = dbCredentials.Password
		dbConfig.CertPath = ""
		dbConfig.KeyPath = ""
	}
}

// setup populates fields in the dbConfig
func (dbConfig *DbConfig) setup(dbName string, bootstrapConfig BootstrapConfig, dbCredentials *DatabaseCredentialsConfig) error {

	dbConfig.inheritFromBootstrap(bootstrapConfig)
	if dbCredentials != nil {
		dbConfig.setPerDatabaseCredentials(*dbCredentials)
	}

	dbConfig.Name = dbName
	if dbConfig.Bucket == nil {
		dbConfig.Bucket = &dbConfig.Name
	}

	if dbConfig.Server != nil {
		url, err := url.Parse(*dbConfig.Server)
		if err != nil {
			return err
		}
		if url.User != nil {
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
	}

	insecureSkipVerify := false
	if dbConfig.Unsupported != nil {
		insecureSkipVerify = dbConfig.Unsupported.RemoteConfigTlsSkipVerify
	}

	// Load Sync Function.
	if dbConfig.Sync != nil {
		sync, err := loadJavaScript(*dbConfig.Sync, insecureSkipVerify)
		if err != nil {
			return &JavaScriptLoadError{
				JSLoadType: SyncFunction,
				Path:       *dbConfig.Sync,
				Err:        err,
			}
		}
		dbConfig.Sync = &sync
	}

	// Load Import Filter Function.
	if dbConfig.ImportFilter != nil {
		importFilter, err := loadJavaScript(*dbConfig.ImportFilter, insecureSkipVerify)
		if err != nil {
			return &JavaScriptLoadError{
				JSLoadType: ImportFilter,
				Path:       *dbConfig.ImportFilter,
				Err:        err,
			}
		}
		dbConfig.ImportFilter = &importFilter
	}

	// Load Conflict Resolution Function.
	for _, rc := range dbConfig.Replications {
		if rc.ConflictResolutionFn != "" {
			conflictResolutionFn, err := loadJavaScript(rc.ConflictResolutionFn, insecureSkipVerify)
			if err != nil {
				return &JavaScriptLoadError{
					JSLoadType: ConflictResolver,
					Path:       rc.ConflictResolutionFn,
					Err:        err,
				}
			}
			rc.ConflictResolutionFn = conflictResolutionFn
		}
	}

	return nil
}

// loadJavaScript loads the JavaScript source from an external file or and HTTP/HTTPS endpoint.
// If the specified path does not qualify for a valid file or an URI, it returns the input path
// as-is with the assumption that it is an inline JavaScript source. Returns error if there is
// any failure in reading the JavaScript file or URI.
func loadJavaScript(path string, insecureSkipVerify bool) (js string, err error) {
	rc, err := readFromPath(path, insecureSkipVerify)
	if errors.Is(err, ErrPathNotFound) {
		// If rc is nil and readFromPath returns no error, treat the
		// the given path as an inline JavaScript and return it as-is.
		return path, nil
	}
	if err != nil {
		if !insecureSkipVerify {
			var unkAuthErr x509.UnknownAuthorityError
			if errors.As(err, &unkAuthErr) {
				return "", fmt.Errorf("%w. TLS certificate failed verification. TLS verification "+
					"can be disabled using the unsupported \"remote_config_tls_skip_verify\" option", err)
			}
			return "", err
		}
		return "", err
	}
	defer func() { _ = rc.Close() }()
	src, err := ioutil.ReadAll(rc)
	if err != nil {
		return "", err
	}
	return string(src), nil
}

// JSLoadType represents a specific JavaScript load type.
// It is used to uniquely identify any potential errors during JavaScript load.
type JSLoadType int

const (
	SyncFunction     JSLoadType = iota // Sync Function JavaScript load.
	ImportFilter                       // Import filter JavaScript load.
	ConflictResolver                   // Conflict Resolver JavaScript load.
	WebhookFilter                      // Webhook filter JavaScript load.
	jsLoadTypeCount                    // Number of JSLoadType constants.
)

// jsLoadTypes represents the list of different possible JSLoadType.
var jsLoadTypes = []string{"SyncFunction", "ImportFilter", "ConflictResolver", "WebhookFilter"}

// String returns the string representation of a specific JSLoadType.
func (t JSLoadType) String() string {
	if len(jsLoadTypes) < int(t) {
		return fmt.Sprintf("JSLoadType(%d)", t)
	}
	return jsLoadTypes[t]
}

// JavaScriptLoadError is returned if there is any failure in loading JavaScript
// source from an external file or URL (HTTP/HTTPS endpoint).
type JavaScriptLoadError struct {
	JSLoadType JSLoadType // A specific JavaScript load type.
	Path       string     // Path of the JavaScript source.
	Err        error      // Underlying error.
}

// Error returns string representation of the JavaScriptLoadError.
func (e *JavaScriptLoadError) Error() string {
	return fmt.Sprintf("Error loading JavaScript (%s) from %q, Err: %v", e.JSLoadType, e.Path, e.Err)
}

// ErrPathNotFound means that the specified path or URL (HTTP/HTTPS endpoint)
// doesn't exist to construct a ReadCloser to read the bytes later on.
var ErrPathNotFound = errors.New("path not found")

// readFromPath creates a ReadCloser from the given path. The path must be either a valid file
// or an HTTP/HTTPS endpoint. Returns an error if there is any failure in building ReadCloser.
func readFromPath(path string, insecureSkipVerify bool) (rc io.ReadCloser, err error) {
	messageFormat := "Loading content from [%s] ..."
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		base.InfofCtx(context.Background(), base.KeyAll, messageFormat, path)
		client := base.GetHttpClient(insecureSkipVerify)
		resp, err := client.Get(path)
		if err != nil {
			return nil, err
		} else if resp.StatusCode >= 300 {
			_ = resp.Body.Close()
			return nil, base.HTTPErrorf(resp.StatusCode, http.StatusText(resp.StatusCode))
		}
		rc = resp.Body
	} else if base.FileExists(path) {
		base.InfofCtx(context.Background(), base.KeyAll, messageFormat, path)
		rc, err = os.Open(path)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, ErrPathNotFound
	}
	return rc, nil
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
		base.WarnfCtx(context.Background(), `Using deprecated config value for "import_docs": "continuous". Use "import_docs": true instead.`)
		return true, nil
	}

	return false, fmt.Errorf("Unrecognized value for import_docs: %#v. Valid values are true and false.", dbConfig.AutoImport)
}

const dbConfigFieldNotAllowedErrorMsg = "Persisted database config does not support customization of the %q field"

// validatePersistentDbConfig checks for fields that are only allowed in non-persistent mode.
func (dbConfig *DbConfig) validatePersistentDbConfig() (errorMessages error) {
	var multiError *base.MultiError
	if dbConfig.Server != nil {
		multiError = multiError.Append(fmt.Errorf(dbConfigFieldNotAllowedErrorMsg, "server"))
	}
	if dbConfig.Username != "" {
		multiError = multiError.Append(fmt.Errorf(dbConfigFieldNotAllowedErrorMsg, "username"))
	}
	if dbConfig.Password != "" {
		multiError = multiError.Append(fmt.Errorf(dbConfigFieldNotAllowedErrorMsg, "password"))
	}
	if dbConfig.CertPath != "" {
		multiError = multiError.Append(fmt.Errorf(dbConfigFieldNotAllowedErrorMsg, "certpath"))
	}
	if dbConfig.KeyPath != "" {
		multiError = multiError.Append(fmt.Errorf(dbConfigFieldNotAllowedErrorMsg, "keypath"))
	}
	if dbConfig.CACertPath != "" {
		multiError = multiError.Append(fmt.Errorf(dbConfigFieldNotAllowedErrorMsg, "cacertpath"))
	}
	if dbConfig.Users != nil {
		multiError = multiError.Append(fmt.Errorf(dbConfigFieldNotAllowedErrorMsg, "users"))
	}
	if dbConfig.Roles != nil {
		multiError = multiError.Append(fmt.Errorf(dbConfigFieldNotAllowedErrorMsg, "roles"))
	}
	return multiError.ErrorOrNil()
}

func (dbConfig *DbConfig) validate(ctx context.Context, validateOIDCConfig bool) error {
	return dbConfig.validateVersion(ctx, base.IsEnterpriseEdition(), validateOIDCConfig)
}

func (dbConfig *DbConfig) validateVersion(ctx context.Context, isEnterpriseEdition, validateOIDCConfig bool) error {

	var multiError *base.MultiError
	// Make sure a non-zero compact_interval_days config is within the valid range
	if val := dbConfig.CompactIntervalDays; val != nil && *val != 0 &&
		(*val < db.CompactIntervalMinDays || *val > db.CompactIntervalMaxDays) {
		multiError = multiError.Append(fmt.Errorf(rangeValueErrorMsg, "compact_interval_days",
			fmt.Sprintf("%g-%g", db.CompactIntervalMinDays, db.CompactIntervalMaxDays)))
	}

	if dbConfig.CacheConfig != nil {

		if dbConfig.CacheConfig.ChannelCacheConfig != nil {

			// EE: channel cache
			if !isEnterpriseEdition {
				if val := dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber; val != nil {
					base.WarnfCtx(ctx, eeOnlyWarningMsg, "cache.channel_cache.max_number", *val, db.DefaultChannelCacheMaxNumber)
					dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber = nil
				}
				if val := dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent; val != nil {
					base.WarnfCtx(ctx, eeOnlyWarningMsg, "cache.channel_cache.compact_high_watermark_pct", *val, db.DefaultCompactHighWatermarkPercent)
					dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent = nil
				}
				if val := dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent; val != nil {
					base.WarnfCtx(ctx, eeOnlyWarningMsg, "cache.channel_cache.compact_low_watermark_pct", *val, db.DefaultCompactLowWatermarkPercent)
					dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent = nil
				}
			}

			if dbConfig.CacheConfig.ChannelCacheConfig.MaxNumPending != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxNumPending < 1 {
				multiError = multiError.Append(fmt.Errorf(minValueErrorMsg, "cache.channel_cache.max_num_pending", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitPending != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitPending < 1 {
				multiError = multiError.Append(fmt.Errorf(minValueErrorMsg, "cache.channel_cache.max_wait_pending", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitSkipped != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxWaitSkipped < 1 {
				multiError = multiError.Append(fmt.Errorf(minValueErrorMsg, "cache.channel_cache.max_wait_skipped", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxLength != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxLength < 1 {
				multiError = multiError.Append(fmt.Errorf(minValueErrorMsg, "cache.channel_cache.max_length", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MinLength != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MinLength < 1 {
				multiError = multiError.Append(fmt.Errorf(minValueErrorMsg, "cache.channel_cache.min_length", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.ExpirySeconds != nil && *dbConfig.CacheConfig.ChannelCacheConfig.ExpirySeconds < 1 {
				multiError = multiError.Append(fmt.Errorf(minValueErrorMsg, "cache.channel_cache.expiry_seconds", 1))
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber != nil && *dbConfig.CacheConfig.ChannelCacheConfig.MaxNumber < db.MinimumChannelCacheMaxNumber {
				multiError = multiError.Append(fmt.Errorf(minValueErrorMsg, "cache.channel_cache.max_number", db.MinimumChannelCacheMaxNumber))
			}

			// Compact watermark validation
			hwm := db.DefaultCompactHighWatermarkPercent
			lwm := db.DefaultCompactLowWatermarkPercent
			if dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent != nil {
				if *dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent < 1 || *dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent > 100 {
					multiError = multiError.Append(fmt.Errorf(rangeValueErrorMsg, "cache.channel_cache.compact_high_watermark_pct", "0-100"))
				}
				hwm = *dbConfig.CacheConfig.ChannelCacheConfig.HighWatermarkPercent
			}
			if dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent != nil {
				if *dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent < 1 || *dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent > 100 {
					multiError = multiError.Append(fmt.Errorf(rangeValueErrorMsg, "cache.channel_cache.compact_low_watermark_pct", "0-100"))
				}
				lwm = *dbConfig.CacheConfig.ChannelCacheConfig.LowWatermarkPercent
			}
			if lwm >= hwm {
				multiError = multiError.Append(fmt.Errorf("cache.channel_cache.compact_high_watermark_pct (%v) must be greater than cache.channel_cache.compact_low_watermark_pct (%v)", hwm, lwm))
			}

		}

		if dbConfig.CacheConfig.RevCacheConfig != nil {
			// EE: disable revcache
			revCacheSize := dbConfig.CacheConfig.RevCacheConfig.Size
			if !isEnterpriseEdition && revCacheSize != nil && *revCacheSize == 0 {
				base.WarnfCtx(ctx, eeOnlyWarningMsg, "cache.rev_cache.size", *revCacheSize, db.DefaultRevisionCacheSize)
				dbConfig.CacheConfig.RevCacheConfig.Size = nil
			}

			if dbConfig.CacheConfig.RevCacheConfig.ShardCount != nil {
				if *dbConfig.CacheConfig.RevCacheConfig.ShardCount < 1 {
					multiError = multiError.Append(fmt.Errorf(minValueErrorMsg, "cache.rev_cache.shard_count", 1))
				}
			}
		}
	}

	// EE: delta sync
	if !isEnterpriseEdition && dbConfig.DeltaSync != nil && dbConfig.DeltaSync.Enabled != nil {
		base.WarnfCtx(ctx, eeOnlyWarningMsg, "delta_sync.enabled", *dbConfig.DeltaSync.Enabled, false)
		dbConfig.DeltaSync.Enabled = nil
	}

	// Import validation
	autoImportEnabled, err := dbConfig.AutoImportEnabled()
	if err != nil {
		multiError = multiError.Append(err)
	}
	if dbConfig.FeedType == base.TapFeedType && autoImportEnabled == true {
		multiError = multiError.Append(fmt.Errorf("Invalid configuration for Sync Gw. TAP feed type can not be used with auto-import"))
	}

	if dbConfig.AutoImport != nil && autoImportEnabled && !dbConfig.UseXattrs() {
		multiError = multiError.Append(fmt.Errorf("Invalid configuration - import_docs enabled, but enable_shared_bucket_access not enabled"))
	}

	if dbConfig.ImportPartitions != nil {
		if !isEnterpriseEdition {
			base.WarnfCtx(ctx, eeOnlyWarningMsg, "import_partitions", *dbConfig.ImportPartitions, nil)
			dbConfig.ImportPartitions = nil
		} else if !dbConfig.UseXattrs() {
			multiError = multiError.Append(fmt.Errorf("Invalid configuration - import_partitions set, but enable_shared_bucket_access not enabled"))
		} else if !autoImportEnabled {
			multiError = multiError.Append(fmt.Errorf("Invalid configuration - import_partitions set, but import_docs disabled"))
		} else if *dbConfig.ImportPartitions < 1 || *dbConfig.ImportPartitions > 1024 {
			multiError = multiError.Append(fmt.Errorf(rangeValueErrorMsg, "import_partitions", "1-1024"))
		}
	}

	if dbConfig.DeprecatedPool != nil {
		base.WarnfCtx(ctx, `"pool" config option is not supported. The pool will be set to "default". The option should be removed from config file.`)
	}

	if isEmpty, err := validateJavascriptFunction(dbConfig.Sync); err != nil {
		multiError = multiError.Append(fmt.Errorf("sync function error: %w", err))
	} else if isEmpty {
		dbConfig.Sync = nil
	}

	if isEmpty, err := validateJavascriptFunction(dbConfig.ImportFilter); err != nil {
		multiError = multiError.Append(fmt.Errorf("import filter error: %w", err))
	} else if isEmpty {
		dbConfig.ImportFilter = nil
	}

	if err := db.ValidateDatabaseName(dbConfig.Name); err != nil {
		multiError = multiError.Append(err)
	}

	if dbConfig.Unsupported != nil && dbConfig.Unsupported.WarningThresholds != nil {
		warningThresholdXattrSize := dbConfig.Unsupported.WarningThresholds.XattrSize
		if warningThresholdXattrSize != nil {
			lowerLimit := 0.1 * 1024 * 1024 // 0.1 MB
			upperLimit := 1 * 1024 * 1024   // 1 MB
			if *warningThresholdXattrSize < uint32(lowerLimit) {
				multiError = multiError.Append(fmt.Errorf("xattr_size warning threshold cannot be lower than %d bytes", uint32(lowerLimit)))
			} else if *warningThresholdXattrSize > uint32(upperLimit) {
				multiError = multiError.Append(fmt.Errorf("xattr_size warning threshold cannot be higher than %d bytes", uint32(upperLimit)))
			}
		}

		warningThresholdChannelsPerDoc := dbConfig.Unsupported.WarningThresholds.ChannelsPerDoc
		if warningThresholdChannelsPerDoc != nil {
			lowerLimit := 5
			if *warningThresholdChannelsPerDoc < uint32(lowerLimit) {
				multiError = multiError.Append(fmt.Errorf("channels_per_doc warning threshold cannot be lower than %d", lowerLimit))
			}
		}

		warningThresholdGrantsPerDoc := dbConfig.Unsupported.WarningThresholds.GrantsPerDoc
		if warningThresholdGrantsPerDoc != nil {
			lowerLimit := 5
			if *warningThresholdGrantsPerDoc < uint32(lowerLimit) {
				multiError = multiError.Append(fmt.Errorf("access_and_role_grants_per_doc warning threshold cannot be lower than %d", lowerLimit))
			}
		}
	}

	revsLimit := dbConfig.RevsLimit
	if revsLimit != nil {
		if *dbConfig.ConflictsAllowed() {
			if *revsLimit < 20 {
				multiError = multiError.Append(fmt.Errorf("The revs_limit (%v) value in your Sync Gateway configuration cannot be set lower than 20.", *revsLimit))
			}
		} else {
			if *revsLimit <= 0 {
				multiError = multiError.Append(fmt.Errorf("The revs_limit (%v) value in your Sync Gateway configuration must be greater than zero.", *revsLimit))
			}
		}
	}

	if validateOIDCConfig && dbConfig.OIDCConfig != nil {
		for name, provider := range dbConfig.OIDCConfig.Providers {
			_, _, err := provider.DiscoverConfig(ctx)
			if err != nil {
				multiError = multiError.Append(fmt.Errorf("failed to validate OIDC configuration for %s: %w", name, err))
			}
		}
	}

	// scopes and collections validation
	if len(dbConfig.Scopes) > 1 {
		multiError = multiError.Append(fmt.Errorf("only one named scope is supported, but had %d (%v)", len(dbConfig.Scopes), dbConfig.Scopes))
	} else {
		for scopeName, scopeConfig := range dbConfig.Scopes {
			if len(scopeConfig.Collections) == 0 {
				multiError = multiError.Append(fmt.Errorf("must specify at least one collection in scope %v", scopeName))
				continue
			}

			if dbConfig.Sync != nil {
				multiError = multiError.Append(errors.New("cannot specify a database-level sync function with named scopes and collections"))
			}
			if dbConfig.ImportFilter != nil {
				multiError = multiError.Append(errors.New("cannot specify a database-level import filter with named scopes and collections"))
			}

			// validate each collection's config
			for collectionName, collectionConfig := range scopeConfig.Collections {
				if isEmpty, err := validateJavascriptFunction(collectionConfig.SyncFn); err != nil {
					multiError = multiError.Append(fmt.Errorf("collection %q sync function error: %w", collectionName, err))
				} else if isEmpty {
					collectionConfig.SyncFn = nil
				}

				if isEmpty, err := validateJavascriptFunction(collectionConfig.ImportFilter); err != nil {
					multiError = multiError.Append(fmt.Errorf("collection %q import filter error: %w", collectionName, err))
				} else if isEmpty {
					collectionConfig.ImportFilter = nil
				}
			}
		}
	}

	return multiError.ErrorOrNil()
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

// validateJavascriptFunction returns an error if the javascript function was invalid, if set.
func validateJavascriptFunction(jsFunc *string) (isEmpty bool, err error) {
	if jsFunc != nil && strings.TrimSpace(*jsFunc) != "" {
		if _, err := sgbucket.NewJSRunner(*jsFunc); err != nil {
			return false, fmt.Errorf("invalid javascript syntax: %w", err)
		}
		return false, nil
	}
	return true, nil
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

func (dbConfig *DbConfig) Redacted() (*DbConfig, error) {
	var config DbConfig

	err := base.DeepCopyInefficient(&config, dbConfig)
	if err != nil {
		return nil, err
	}

	err = config.redactInPlace()
	return &config, err
}

// redactInPlace modifies the given config to redact the fields inside it.
func (config *DbConfig) redactInPlace() error {

	if config.Password != "" {
		config.Password = base.RedactedStr
	}

	for i := range config.Users {
		if config.Users[i].Password != nil && *config.Users[i].Password != "" {
			config.Users[i].Password = base.StringPtr(base.RedactedStr)
		}
	}

	for i, _ := range config.Replications {
		config.Replications[i] = config.Replications[i].Redacted()
	}

	return nil
}

// decodeAndSanitiseConfig will sanitise a config from an io.Reader and unmarshal it into the given config parameter.
func decodeAndSanitiseConfig(r io.Reader, config interface{}) (err error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	// Expand environment variables.
	b, err = expandEnv(b)
	if err != nil {
		return err
	}
	b = base.ConvertBackQuotedStrings(b)

	d := base.JSONDecoder(bytes.NewBuffer(b))
	d.DisallowUnknownFields()
	err = d.Decode(config)
	return base.WrapJSONUnknownFieldErr(err)
}

// expandEnv replaces $var or ${var} in config according to the values of the
// current environment variables. The replacement is case-sensitive. References
// to undefined variables will result in an error. A default value can
// be given by using the form ${var:-default value}.
func expandEnv(config []byte) (value []byte, err error) {
	var multiError *base.MultiError
	val := []byte(os.Expand(string(config), func(key string) string {
		if key == "$" {
			base.DebugfCtx(context.Background(), base.KeyConfig, "Skipping environment variable expansion: %s", key)
			return key
		}
		val, err := envDefaultExpansion(key, os.Getenv)
		if err != nil {
			multiError = multiError.Append(err)
		}
		return val
	}))
	return val, multiError.ErrorOrNil()
}

// ErrEnvVarUndefined is returned when a specified variable can’t be resolved from
// the system environment and no default value is supplied in the configuration.
type ErrEnvVarUndefined struct {
	key string // Environment variable identifier.
}

func (e ErrEnvVarUndefined) Error() string {
	return fmt.Sprintf("undefined environment variable '${%s}' is specified in the config without default value", e.key)
}

// envDefaultExpansion implements the ${foo:-bar} parameter expansion from
// https://pubs.opengroup.org/onlinepubs/009695399/utilities/xcu_chap02.html#tag_02_06_02
func envDefaultExpansion(key string, getEnvFn func(string) string) (value string, err error) {
	kvPair := strings.SplitN(key, ":-", 2)
	key = kvPair[0]
	value = getEnvFn(key)
	if value == "" && len(kvPair) == 2 {
		// Set value to the default.
		value = kvPair[1]
		base.DebugfCtx(context.Background(), base.KeyConfig, "Replacing config environment variable '${%s}' with "+
			"default value specified", key)
	} else if value == "" && len(kvPair) != 2 {
		return "", ErrEnvVarUndefined{key: key}
	} else {
		base.DebugfCtx(context.Background(), base.KeyConfig, "Replacing config environment variable '${%s}'", key)
	}
	return value, nil
}

// SetupAndValidateLogging validates logging config and initializes all logging.
func (sc *StartupConfig) SetupAndValidateLogging() (err error) {

	base.SetRedaction(sc.Logging.RedactionLevel)

	if sc.Logging.LogFilePath == "" {
		sc.Logging.LogFilePath = defaultLogFilePath
	}

	return base.InitLogging(
		sc.Logging.LogFilePath,
		sc.Logging.Console,
		sc.Logging.Error,
		sc.Logging.Warn,
		sc.Logging.Info,
		sc.Logging.Debug,
		sc.Logging.Trace,
		sc.Logging.Stats,
	)
}

func SetMaxFileDescriptors(maxP *uint64) error {
	maxFDs := DefaultMaxFileDescriptors
	if maxP != nil {
		maxFDs = *maxP
	}
	_, err := base.SetMaxFileDescriptors(maxFDs)
	if err != nil {
		base.ErrorfCtx(context.Background(), "Error setting MaxFileDescriptors to %d: %v", maxFDs, err)
		return err
	}
	return nil
}

func (sc *ServerContext) Serve(config *StartupConfig, addr string, handler http.Handler) error {
	http2Enabled := false
	if config.Unsupported.HTTP2 != nil && config.Unsupported.HTTP2.Enabled != nil {
		http2Enabled = *config.Unsupported.HTTP2.Enabled
	}

	tlsMinVersion := GetTLSVersionFromString(&config.API.HTTPS.TLSMinimumVersion)

	serveFn, server, err := base.ListenAndServeHTTP(
		addr,
		config.API.MaximumConnections,
		config.API.HTTPS.TLSCertPath,
		config.API.HTTPS.TLSKeyPath,
		handler,
		config.API.ServerReadTimeout.Value(),
		config.API.ServerWriteTimeout.Value(),
		config.API.ReadHeaderTimeout.Value(),
		config.API.IdleTimeout.Value(),
		http2Enabled,
		tlsMinVersion,
	)
	if err != nil {
		return err
	}

	sc.addHTTPServer(server)

	return serveFn()
}

func (sc *ServerContext) addHTTPServer(s *http.Server) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	sc._httpServers = append(sc._httpServers, s)
}

func (sc *StartupConfig) validate(isEnterpriseEdition bool) (errorMessages error) {
	var multiError *base.MultiError
	if sc.Bootstrap.Server == "" {
		multiError = multiError.Append(fmt.Errorf("a server must be provided in the Bootstrap configuration"))
	}

	secureServer := base.ServerIsTLS(sc.Bootstrap.Server)
	if base.BoolDefault(sc.Bootstrap.UseTLSServer, DefaultUseTLSServer) {
		if !secureServer && !base.ServerIsWalrus(sc.Bootstrap.Server) {
			multiError = multiError.Append(fmt.Errorf("Must use secure scheme in Couchbase Server URL, or opt out by setting bootstrap.use_tls_server to false. Current URL: %s", base.SD(sc.Bootstrap.Server)))
		}
	} else {
		if secureServer {
			multiError = multiError.Append(fmt.Errorf("Couchbase server URL cannot use secure protocol when bootstrap.use_tls_server is false. Current URL: %s", base.SD(sc.Bootstrap.Server)))
		}
	}

	if sc.Bootstrap.ServerTLSSkipVerify != nil && *sc.Bootstrap.ServerTLSSkipVerify && sc.Bootstrap.CACertPath != "" {
		multiError = multiError.Append(fmt.Errorf("cannot skip server TLS validation and use CA Cert"))
	}

	// Make sure if a SSL key or cert is provided, they are both provided
	if (sc.API.HTTPS.TLSKeyPath != "" || sc.API.HTTPS.TLSCertPath != "") && (sc.API.HTTPS.TLSKeyPath == "" || sc.API.HTTPS.TLSCertPath == "") {
		multiError = multiError.Append(fmt.Errorf("both TLS Key Path and TLS Cert Path must be provided when using client TLS. Disable client TLS by not providing either of these options"))
	}

	if sc.Auth.BcryptCost > 0 && (sc.Auth.BcryptCost < auth.DefaultBcryptCost || sc.Auth.BcryptCost > bcrypt.MaxCost) {
		multiError = multiError.Append(fmt.Errorf("%v: %d outside allowed range: %d-%d", auth.ErrInvalidBcryptCost, sc.Auth.BcryptCost, auth.DefaultBcryptCost, bcrypt.MaxCost))
	}

	// EE only features
	if !isEnterpriseEdition {
		if sc.API.EnableAdminAuthenticationPermissionsCheck != nil && *sc.API.EnableAdminAuthenticationPermissionsCheck {
			multiError = multiError.Append(fmt.Errorf("enable_advanced_auth_dp is only supported in enterprise edition"))
		}

		if sc.Bootstrap.ConfigGroupID != persistentConfigDefaultGroupID {
			multiError = multiError.Append(fmt.Errorf("customization of group_id is only supported in enterprise edition"))
		}
	}

	if len(sc.Bootstrap.ConfigGroupID) > persistentConfigGroupIDMaxLength {
		multiError = multiError.Append(fmt.Errorf("group_id must be at most %d characters in length", persistentConfigGroupIDMaxLength))
	}

	return multiError.ErrorOrNil()
}

// setupServerContext creates a new ServerContext given its configuration and performs the context validation.
func setupServerContext(config *StartupConfig, persistentConfig bool) (*ServerContext, error) {
	// Logging config will now have been loaded from command line
	// or from a sync_gateway config file so we can validate the
	// configuration and setup logging now
	if err := config.SetupAndValidateLogging(); err != nil {
		// If we didn't set up logging correctly, we *probably* can't log via normal means...
		// as a best-effort, last-ditch attempt, we'll log to stderr as well.
		log.Printf("[ERR] Error setting up logging: %v", err)
		return nil, fmt.Errorf("error setting up logging: %v", err)
	}

	base.FlushLoggerBuffers()

	base.InfofCtx(context.Background(), base.KeyAll, "Logging: Console level: %v", base.ConsoleLogLevel())
	base.InfofCtx(context.Background(), base.KeyAll, "Logging: Console keys: %v", base.ConsoleLogKey().EnabledLogKeys())
	base.InfofCtx(context.Background(), base.KeyAll, "Logging: Redaction level: %s", config.Logging.RedactionLevel)

	if err := setGlobalConfig(config); err != nil {
		return nil, err
	}

	if err := config.validate(base.IsEnterpriseEdition()); err != nil {
		return nil, err
	}

	sc := NewServerContext(config, persistentConfig)
	if !base.ServerIsWalrus(config.Bootstrap.Server) {
		if err := sc.initializeCouchbaseServerConnections(); err != nil {
			return nil, err
		}
	}
	return sc, nil
}

// fetchAndLoadConfigs retrieves all database configs from the ServerContext's bootstrapConnection, and loads them into the ServerContext.
// It will remove any databases currently running that are not found in the bucket.
func (sc *ServerContext) fetchAndLoadConfigs(isInitialStartup bool) (count int, err error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	fetchedConfigs, err := sc.fetchConfigs(isInitialStartup)
	if err != nil {
		return 0, err
	}

	for _, dbName := range sc.bucketDbName {
		if _, foundMatchingDb := fetchedConfigs[dbName]; !foundMatchingDb {
			base.InfofCtx(context.TODO(), base.KeyConfig, "Database %q was running on this node, but config was not found on the server - removing database", base.MD(dbName))
			sc._removeDatabase(dbName)
		}
	}

	return sc._applyConfigs(fetchedConfigs), nil
}

func (sc *ServerContext) fetchAndLoadDatabase(dbName string) (found bool, err error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	return sc._fetchAndLoadDatabase(dbName)
}

// _fetchAndLoadDatabase will attempt to find the given database name first in a matching bucket name,
// but then fall back to searching through configs in each bucket to try and find a config.
func (sc *ServerContext) _fetchAndLoadDatabase(dbName string) (found bool, err error) {
	found, dbConfig, err := sc.fetchDatabase(dbName)
	if err != nil || !found {
		return false, err
	}
	sc._applyConfigs(map[string]DatabaseConfig{dbName: *dbConfig})

	return true, nil
}

func (sc *ServerContext) fetchDatabase(dbName string) (found bool, dbConfig *DatabaseConfig, err error) {
	buckets, err := sc.bootstrapContext.connection.GetConfigBuckets()
	if err != nil {
		return false, nil, fmt.Errorf("couldn't get buckets from cluster: %w", err)
	}
	logCtx := context.TODO()

	// move bucket matching dbName to the front so it's searched first
	for i, bucket := range buckets {
		if bucket == dbName {
			buckets = append(buckets[i:], buckets[:i]...)
		}
	}

	for _, bucket := range buckets {
		var cnf DatabaseConfig
		cas, err := sc.bootstrapContext.connection.GetConfig(bucket, sc.config.Bootstrap.ConfigGroupID, &cnf)
		if err == base.ErrNotFound {
			base.DebugfCtx(logCtx, base.KeyConfig, "%q did not contain config in group %q", bucket, sc.config.Bootstrap.ConfigGroupID)
			continue
		}
		if err != nil {
			base.DebugfCtx(logCtx, base.KeyConfig, "unable to fetch config in group %q from bucket %q: %v", sc.config.Bootstrap.ConfigGroupID, bucket, err)
			continue
		}

		if cnf.Name == "" {
			cnf.Name = bucket
		}

		if cnf.Name != dbName {
			base.TracefCtx(logCtx, base.KeyConfig, "%q did not contain config in group %q for db %q", bucket, sc.config.Bootstrap.ConfigGroupID, dbName)
			continue
		}

		cnf.cas = cas

		// TODO: This code is mostly copied from fetchConfigs, move into shared function with DbConfig REST API work?

		// inherit properties the bootstrap config
		cnf.CACertPath = sc.config.Bootstrap.CACertPath

		bucketCopy := bucket
		cnf.Bucket = &bucketCopy

		// any authentication fields defined on the dbconfig take precedence over any in the bootstrap config
		if cnf.Username == "" && cnf.Password == "" && cnf.CertPath == "" && cnf.KeyPath == "" {
			cnf.Username = sc.config.Bootstrap.Username
			cnf.Password = sc.config.Bootstrap.Password
			cnf.CertPath = sc.config.Bootstrap.X509CertPath
			cnf.KeyPath = sc.config.Bootstrap.X509KeyPath
		}
		base.TracefCtx(logCtx, base.KeyConfig, "Got config for bucket %q with cas %d", bucket, cas)
		return true, &cnf, nil
	}

	return false, nil, nil
}

// fetchConfigs retrieves all database configs from the ServerContext's bootstrapConnection.
func (sc *ServerContext) fetchConfigs(isInitialStartup bool) (dbNameConfigs map[string]DatabaseConfig, err error) {
	buckets, err := sc.bootstrapContext.connection.GetConfigBuckets()
	if err != nil {
		return nil, fmt.Errorf("couldn't get buckets from cluster: %w", err)
	}

	logCtx := context.TODO()
	fetchedConfigs := make(map[string]DatabaseConfig, len(buckets))

	for _, bucket := range buckets {
		base.TracefCtx(logCtx, base.KeyConfig, "Checking for config for group %q from bucket %q", sc.config.Bootstrap.ConfigGroupID, bucket)
		var cnf DatabaseConfig
		cas, err := sc.bootstrapContext.connection.GetConfig(bucket, sc.config.Bootstrap.ConfigGroupID, &cnf)
		if err == base.ErrNotFound {
			base.DebugfCtx(logCtx, base.KeyConfig, "Bucket %q did not contain config for group %q", bucket, sc.config.Bootstrap.ConfigGroupID)
			continue
		}
		if err != nil {
			// Unexpected error fetching config - SDK has already performed retries, so we'll treat it as a database removal
			// this could be due to invalid JSON or some other non-recoverable error.
			if isInitialStartup {
				base.WarnfCtx(logCtx, "Unable to fetch config for group %q from bucket %q on startup: %v", sc.config.Bootstrap.ConfigGroupID, bucket, err)
			} else {
				base.DebugfCtx(logCtx, base.KeyConfig, "Unable to fetch config for group %q from bucket %q: %v", sc.config.Bootstrap.ConfigGroupID, bucket, err)
			}
			continue
		}

		cnf.cas = cas

		// inherit properties the bootstrap config
		cnf.CACertPath = sc.config.Bootstrap.CACertPath

		bucketCopy := bucket
		cnf.Bucket = &bucketCopy

		// stamp per-database credentials if set
		if dbCredentials, ok := sc.config.DatabaseCredentials[cnf.Name]; ok && dbCredentials != nil {
			cnf.setPerDatabaseCredentials(*dbCredentials)
		}

		// any authentication fields defined on the dbconfig take precedence over any in the bootstrap config
		if cnf.Username == "" && cnf.Password == "" && cnf.CertPath == "" && cnf.KeyPath == "" {
			cnf.Username = sc.config.Bootstrap.Username
			cnf.Password = sc.config.Bootstrap.Password
			cnf.CertPath = sc.config.Bootstrap.X509CertPath
			cnf.KeyPath = sc.config.Bootstrap.X509KeyPath
		}

		base.DebugfCtx(logCtx, base.KeyConfig, "Got config for group %q from bucket %q with cas %d", sc.config.Bootstrap.ConfigGroupID, bucket, cas)
		fetchedConfigs[cnf.Name] = cnf
	}

	return fetchedConfigs, nil
}

// _applyConfigs takes a map of dbName->DatabaseConfig and loads them into the ServerContext where necessary.
func (sc *ServerContext) _applyConfigs(dbNameConfigs map[string]DatabaseConfig) (count int) {
	for dbName, cnf := range dbNameConfigs {
		applied, err := sc._applyConfig(cnf, false)
		if err != nil {
			base.ErrorfCtx(context.Background(), "Couldn't apply config for database %q: %v", base.MD(dbName), err)
			continue
		}
		if applied {
			count++
		}
	}

	return count
}

func (sc *ServerContext) applyConfigs(dbNameConfigs map[string]DatabaseConfig) (count int) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	return sc._applyConfigs(dbNameConfigs)
}

// _applyConfig loads the given database, failFast=true will not attempt to retry connecting/loading
func (sc *ServerContext) _applyConfig(cnf DatabaseConfig, failFast bool) (applied bool, err error) {
	// skip if we already have this config loaded, and we've got a cas value to compare with
	foundDbName, exists := sc.bucketDbName[*cnf.Bucket]
	if exists {
		// Somebody is trying to create a new database with a duplicate bucket. Changing db name is not supported and is rejected earlier in the update handler.
		if foundDbName != cnf.Name {
			return false, fmt.Errorf("%w: Bucket %q already in use by database %q", base.ErrAlreadyExists, *cnf.Bucket, foundDbName)
		}

		if cnf.cas == 0 {
			// force an update when the new config's cas was set to zero prior to load
			base.InfofCtx(context.TODO(), base.KeyConfig, "Forcing update of config for database %q bucket %q", cnf.Name, *cnf.Bucket)
		} else {
			if sc.dbConfigs[foundDbName].cas >= cnf.cas {
				base.DebugfCtx(context.TODO(), base.KeyConfig, "Database %q bucket %q config has not changed since last update", cnf.Name, *cnf.Bucket)
				return false, nil
			}
			base.InfofCtx(context.TODO(), base.KeyConfig, "Updating database %q for bucket %q with new config from bucket", cnf.Name, *cnf.Bucket)
		}
	}

	// ensure we're not loading a database from multiple buckets
	if dbc := sc.databases_[cnf.Name]; dbc != nil {
		runningBucket := dbc.Bucket.GetName()
		if runningBucket != *cnf.Bucket {
			return false, fmt.Errorf("database %q bucket %q cannot be added - already running %q using bucket %q", cnf.Name, *cnf.Bucket, cnf.Name, runningBucket)
		}
	}

	// Strip out version as we have no use for this locally and we want to prevent it being stored and being returned
	// by any output
	cnf.Version = ""

	// TODO: Dynamic update instead of reload
	if err := sc._reloadDatabaseWithConfig(cnf, failFast); err != nil {
		// remove these entries we just created above if the database hasn't loaded properly
		return false, fmt.Errorf("couldn't reload database: %w", err)
	}

	return true, nil
}

// applyConfigs takes a map of bucket->DatabaseConfig and loads them into the ServerContext where necessary.
func (sc *ServerContext) applyConfig(cnf DatabaseConfig) (applied bool, err error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	return sc._applyConfig(cnf, false)
}

// addLegacyPrincipals takes a map of databases that each have a map of names with principle configs.
// Call this function to install the legacy principles to the upgraded database that use a persistent config.
// Only call this function after the databases have been initalised via setupServerContext.
func (sc *ServerContext) addLegacyPrincipals(legacyDbUsers, legacyDbRoles map[string]map[string]*db.PrincipalConfig) {
	for dbName, dbUser := range legacyDbUsers {
		dbCtx, err := sc.GetDatabase(dbName)
		if err != nil {
			base.ErrorfCtx(context.Background(), "Couldn't get database context to install user principles: %v", err)
			continue
		}
		err = sc.installPrincipals(dbCtx, dbUser, "user")
		if err != nil {
			base.ErrorfCtx(context.Background(), "Couldn't install user principles: %v", err)
		}
	}

	for dbName, dbRole := range legacyDbRoles {
		dbCtx, err := sc.GetDatabase(dbName)
		if err != nil {
			base.ErrorfCtx(context.Background(), "Couldn't get database context to install role principles: %v", err)
			continue
		}
		err = sc.installPrincipals(dbCtx, dbRole, "role")
		if err != nil {
			base.ErrorfCtx(context.Background(), "Couldn't install role principles: %v", err)
		}
	}
}

// startServer starts and runs the server with the given configuration. (This function never returns.)
func startServer(config *StartupConfig, sc *ServerContext) error {
	if config.API.ProfileInterface != "" {
		// runtime.MemProfileRate = 10 * 1024
		base.InfofCtx(context.TODO(), base.KeyAll, "Starting profile server on %s", base.UD(config.API.ProfileInterface))
		go func() {
			_ = http.ListenAndServe(config.API.ProfileInterface, nil)
		}()
	}

	go sc.PostStartup()

	base.Consolef(base.LevelInfo, base.KeyAll, "Starting metrics server on %s", config.API.MetricsInterface)
	go func() {
		if err := sc.Serve(config, config.API.MetricsInterface, CreateMetricHandler(sc)); err != nil {
			base.ErrorfCtx(context.TODO(), "Error serving the Metrics API: %v", err)
		}
	}()

	base.Consolef(base.LevelInfo, base.KeyAll, "Starting admin server on %s", config.API.AdminInterface)
	go func() {
		if err := sc.Serve(config, config.API.AdminInterface, CreateAdminHandler(sc)); err != nil {
			base.ErrorfCtx(context.TODO(), "Error serving the Admin API: %v", err)
		}
	}()

	base.Consolef(base.LevelInfo, base.KeyAll, "Starting server on %s ...", config.API.PublicInterface)
	return sc.Serve(config, config.API.PublicInterface, CreatePublicHandler(sc))
}

func sharedBucketDatabaseCheck(sc *ServerContext) (errors error) {
	bucketUUIDToDBContext := make(map[string][]*db.DatabaseContext, len(sc.databases_))
	for _, dbContext := range sc.databases_ {
		if uuid, err := dbContext.Bucket.UUID(); err == nil {
			bucketUUIDToDBContext[uuid] = append(bucketUUIDToDBContext[uuid], dbContext)
		}
	}
	sharedBuckets := sharedBuckets(bucketUUIDToDBContext)

	var multiError *base.MultiError
	for _, sharedBucket := range sharedBuckets {
		sharedBucketError := &SharedBucketError{sharedBucket}
		multiError = multiError.Append(sharedBucketError)
		messageFormat := "Bucket %q is shared among databases %s. " +
			"This may result in unexpected behaviour if security is not defined consistently."
		base.WarnfCtx(context.Background(), messageFormat, base.MD(sharedBucket.bucketName), base.MD(sharedBucket.dbNames))
	}
	return multiError.ErrorOrNil()
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
			base.WarnfCtx(context.Background(), "Error rotating %v: %v", logger, err)
		}
	}
}

// RegisterSignalHandler invokes functions based on the given signals:
// - SIGHUP causes Sync Gateway to rotate log files.
// - SIGINT or SIGTERM causes Sync Gateway to exit cleanly.
// - SIGKILL cannot be handled by the application.
func RegisterSignalHandler() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGHUP, os.Interrupt, syscall.SIGTERM)

	go func() {
		for sig := range signalChannel {
			base.InfofCtx(context.TODO(), base.KeyAll, "Handling signal: %v", sig)
			switch sig {
			case syscall.SIGHUP:
				HandleSighup()
			default:
				// Ensure log buffers are flushed before exiting.
				base.FlushLogBuffers()
				os.Exit(130) // 130 == exit code 128 + 2 (interrupt)
			}
		}
	}()
}
