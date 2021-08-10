package rest

import (
	"github.com/couchbase/sync_gateway/db"
)

type DatabaseConfig struct {
	// cas is the Couchbase Server CAS of the database config in the bucket
	cas uint64

	Guest *db.PrincipalConfig `json:"guest,omitempty"`

	DbConfig

	// TODO: Review these (should we populate `server` with bootstrap server? Or just drop it altogether?)
	// Server                           string              `json:"server,omitempty"`                               // Couchbase server URL
	// Name                             string              `json:"name,omitempty"`                                 // Database name in REST API (stored as key in JSON)
	// Bucket                           string              `json:"bucket,omitempty"`                               // Bucket name
	// Username                         string              `json:"username,omitempty"`                             // Username for authenticating to server
	// Password                         string              `json:"password,omitempty"`                             // Password for authenticating to server
	// CACertPath                       string              `json:"cacertpath,omitempty"`                           // Root CA cert path for X.509 bucket auth
	// X509CertPath                     string              `json:"x509_cert_path,omitempty"`                       // Cert path (public key) for X.509 bucket auth
	// X509KeyPath                      string              `json:"x509_key_path,omitempty"`                        // Key path (private key) for X.509 bucket auth
	// Sync                             string              `json:"sync,omitempty"`                                 // Sync function defines which users can see which data
	// RevsLimit                        uint32              `json:"revs_limit,omitempty"`                           // Max depth a document's revision tree can grow to
	// AutoImport                       *bool               `json:"import_docs,omitempty"`                          // Whether to automatically import Couchbase Server docs into SG.  Xattrs must be enabled.  true or "continuous" both enable this.
	// ImportPartitions                 uint16              `json:"import_partitions,omitempty"`                    // Number of partitions for import sharding.  Impacts the total DCP concurrency for import
	// ImportFilter                     string              `json:"import_filter,omitempty"`                        // Filter function (import)
	// ImportBackupOldRev               bool                `json:"import_backup_old_rev"`                          // Whether import should attempt to create a temporary backup of the previous revision body, when available.
	// EventHandlers                    *EventHandlerConfig `json:"event_handlers,omitempty"`                       // Event handlers (webhook)
	// AllowEmptyPassword               bool                `json:"allow_empty_password,omitempty"`                 // Allow empty passwords?  Defaults to false
	// CacheConfig                      *CacheConfig        `json:"cache,omitempty"`                                // Cache settings
	// StartOffline                     bool                `json:"offline,omitempty"`                              // start the DB in the offline state, defaults to false
	// OIDCConfig                       *auth.OIDCOptions   `json:"oidc,omitempty"`                                 // Config properties for OpenID Connect authentication
	// OldRevExpirySeconds              *uint32             `json:"old_rev_expiry_seconds,omitempty"`               // The number of seconds before old revs are removed from CBS bucket
	// ViewQueryTimeoutSecs             *uint32             `json:"view_query_timeout_secs,omitempty"`              // The view query timeout in seconds
	// LocalDocExpirySecs               *uint32             `json:"local_doc_expiry_secs,omitempty"`                // The _local doc expiry time in seconds
	// EnableXattrs                     *bool               `json:"enable_shared_bucket_access,omitempty"`          // Whether to use extended attributes to store _sync metadata
	// SecureCookieOverride             *bool               `json:"session_cookie_secure,omitempty"`                // Override cookie secure flag
	// SessionCookieName                string              `json:"session_cookie_name"`                            // Custom per-database session cookie name
	// SessionCookieHTTPOnly            bool                `json:"session_cookie_http_only"`                       // HTTP only cookies
	// AllowConflicts                   *bool               `json:"allow_conflicts,omitempty"`                      // False forbids creating conflicts
	// NumIndexReplicas                 *uint               `json:"num_index_replicas"`                             // Number of GSI index replicas used for core indexes
	// UseViews                         bool                `json:"use_views"`                                      // Force use of views instead of GSI
	// SendWWWAuthenticateHeader        *bool               `json:"send_www_authenticate_header,omitempty"`         // If false, disables setting of 'WWW-Authenticate' header in 401 responses
	// BucketOpTimeoutMs                *uint32             `json:"bucket_op_timeout_ms,omitempty"`                 // How long bucket ops should block returning "operation timed out". If nil, uses GoCB default.  GoCB buckets only.
	// DeltaSync                        *DeltaSyncConfig    `json:"delta_sync,omitempty"`                           // Config for delta sync
	// CompactIntervalDays              *float32            `json:"compact_interval_days,omitempty"`                // Interval between scheduled compaction runs (in days) - 0 means don't run
	// SGReplicateEnabled               *bool               `json:"sgreplicate_enabled,omitempty"`                  // When false, node will not be assigned replications
	// SGReplicateWebsocketPingInterval *int                `json:"sgreplicate_websocket_heartbeat_secs,omitempty"` // If set, uses this duration as a custom heartbeat interval for websocket ping frames
	// ServeInsecureAttachmentTypes     bool                `json:"serve_insecure_attachment_types,omitempty"`      // Attachment content type will bypass the content-disposition handling, default false
	// QueryPaginationLimit             int                 `json:"query_pagination_limit,omitempty"`               // Query limit to be used during pagination of large queries
	// UserXattrKey                     string              `json:"user_xattr_key,omitempty"`                       // Key of user xattr that will be accessible from the Sync Function. If empty the feature will be disabled.
	// ClientPartitionWindowSecs        int                 `json:"client_partition_window_secs,omitempty"`         // How long clients can remain offline for without losing replication metadata. Default 30 days (in seconds)
	// Unsupported db.UnsupportedOptions `json:"unsupported,omitempty"` // Config for unsupported features
}

// DefaultDatabaseConfig returns a default database config
func DefaultDatabaseConfig() DatabaseConfig {
	return DatabaseConfig{
		cas: 0,
	}
}
