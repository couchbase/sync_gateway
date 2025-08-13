// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"flag"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// configFlag stores the config value, and the corresponding flag value
type configFlag struct {
	config               interface{}
	flagValue            interface{}
	disabled             bool   // disabled can be true to disable the flag - if set this will error and force users to stop using the flag.
	disabledErrorMessage string // disabledErrorMessage can be set to provide additional error message information
}

// registerConfigFlags holds a map of the flag values with the configFlag it goes with
// (which stores the corresponding config field pointer and flag value).
func registerConfigFlags(config *StartupConfig, fs *flag.FlagSet) map[string]configFlag {
	return map[string]configFlag{
		"bootstrap.group_id":                {config: &config.Bootstrap.ConfigGroupID, flagValue: fs.String("bootstrap.group_id", "", "The config group ID to use when discovering databases. Allows for non-homogenous configuration")},
		"bootstrap.config_update_frequency": {config: &config.Bootstrap.ConfigUpdateFrequency, flagValue: fs.String("bootstrap.config_update_frequency", persistentConfigDefaultUpdateFrequency.String(), "How often to poll Couchbase Server for new config changes")},
		"bootstrap.server":                  {config: &config.Bootstrap.Server, flagValue: fs.String("bootstrap.server", "", "Couchbase Server connection string/URL")},
		"bootstrap.username":                {config: &config.Bootstrap.Username, flagValue: fs.String("bootstrap.username", "", "Username for authenticating to server")},
		"bootstrap.password":                {config: nil, disabled: true, disabledErrorMessage: "Use config file to specify bootstrap password, or use X.509 cert/key path flags instead.", flagValue: fs.String("bootstrap.password", "", "Deprecated and disabled. Do not use. Use config file or X.509 auth.")},
		"bootstrap.ca_cert_path":            {config: &config.Bootstrap.CACertPath, flagValue: fs.String("bootstrap.ca_cert_path", "", "Root CA cert path for TLS connection")},
		"bootstrap.server_tls_skip_verify":  {config: &config.Bootstrap.ServerTLSSkipVerify, flagValue: fs.Bool("bootstrap.server_tls_skip_verify", false, "Allow empty server CA Cert Path without attempting to use system root pool")},
		"bootstrap.x509_cert_path":          {config: &config.Bootstrap.X509CertPath, flagValue: fs.String("bootstrap.x509_cert_path", "", "Cert path (public key) for X.509 bucket auth")},
		"bootstrap.x509_key_path":           {config: &config.Bootstrap.X509KeyPath, flagValue: fs.String("bootstrap.x509_key_path", "", "Key path (private key) for X.509 bucket auth")},
		"bootstrap.use_tls_server":          {config: &config.Bootstrap.UseTLSServer, flagValue: fs.Bool("bootstrap.use_tls_server", false, "Forces the connection to Couchbase Server to use TLS")},

		"api.public_interface":                              {config: &config.API.PublicInterface, flagValue: fs.String("api.public_interface", "", "Network interface to bind public API to")},
		"api.admin_interface":                               {config: &config.API.AdminInterface, flagValue: fs.String("api.admin_interface", "", "Network interface to bind admin API to")},
		"api.metrics_interface":                             {config: &config.API.MetricsInterface, flagValue: fs.String("api.metrics_interface", "", "Network interface to bind metrics API to")},
		"api.profile_interface":                             {config: &config.API.ProfileInterface, flagValue: fs.String("api.profile_interface", "", "Network interface to bind profiling API to")},
		"api.admin_interface_authentication":                {config: &config.API.AdminInterfaceAuthentication, flagValue: fs.Bool("api.admin_interface_authentication", false, "Whether the admin API requires authentication")},
		"api.metrics_interface_authentication":              {config: &config.API.MetricsInterfaceAuthentication, flagValue: fs.Bool("api.metrics_interface_authentication", false, "Whether the metrics API requires authentication")},
		"api.enable_admin_authentication_permissions_check": {config: &config.API.EnableAdminAuthenticationPermissionsCheck, flagValue: fs.Bool("api.enable_admin_authentication_permissions_check", false, "Whether to enable the DP permissions check feature of admin auth")},
		"api.server_read_timeout":                           {config: &config.API.ServerReadTimeout, flagValue: fs.String("api.server_read_timeout", "", "Maximum duration.Second before timing out read of the HTTP(S) request")},
		"api.server_write_timeout":                          {config: &config.API.ServerWriteTimeout, flagValue: fs.String("api.server_write_timeout", "", "Maximum duration.Second before timing out write of the HTTP(S) response")},
		"api.read_header_timeout":                           {config: &config.API.ReadHeaderTimeout, flagValue: fs.String("api.read_header_timeout", "", "The amount of time allowed to read request headers")},
		"api.idle_timeout":                                  {config: &config.API.IdleTimeout, flagValue: fs.String("api.idle_timeout", "", "The maximum amount of time to wait for the next request when keep-alives are enabled")},
		"api.pretty":                                        {config: &config.API.Pretty, flagValue: fs.Bool("api.pretty", false, "Pretty-print JSON responses")},
		"api.max_connections":                               {config: &config.API.MaximumConnections, flagValue: fs.Uint("api.max_connections", 0, "Max # of incoming HTTP connections to accept")},
		"api.compress_responses":                            {config: &config.API.CompressResponses, flagValue: fs.Bool("api.compress_responses", false, "If false, disables compression of HTTP responses")},
		"api.hide_product_version":                          {config: &config.API.CompressResponses, flagValue: fs.Bool("api.hide_product_version", false, "Whether product versions removed from Server headers and REST API responses")},

		"api.https.tls_minimum_version": {config: &config.API.HTTPS.TLSMinimumVersion, flagValue: fs.String("api.https.tls_minimum_version", "", "The minimum allowable TLS version for the REST APIs")},
		"api.https.tls_cert_path":       {config: &config.API.HTTPS.TLSCertPath, flagValue: fs.String("api.https.tls_cert_path", "", "The TLS cert file to use for the REST APIs")},
		"api.https.tls_key_path":        {config: &config.API.HTTPS.TLSKeyPath, flagValue: fs.String("api.https.tls_key_path", "", "The TLS key file to use for the REST APIs")},

		"api.cors.origin":       {config: &config.API.CORS.Origin, flagValue: fs.String("api.cors.origin", "", "List of comma separated allowed origins. Use '*' to allow access from everywhere")},
		"api.cors.login_origin": {config: &config.API.CORS.LoginOrigin, flagValue: fs.String("api.cors.login_origin", "", "List of comma separated allowed login origins")},
		"api.cors.headers":      {config: &config.API.CORS.Headers, flagValue: fs.String("api.cors.headers", "", "List of comma separated allowed headers")},
		"api.cors.max_age":      {config: &config.API.CORS.MaxAge, flagValue: fs.Int("api.cors.max_age", 0, "Maximum age of the CORS Options request")},

		"logging.log_file_path":   {config: &config.Logging.LogFilePath, flagValue: fs.String("logging.log_file_path", "", "Absolute or relative path on the filesystem to the log file directory. A relative path is from the directory that contains the Sync Gateway executable file")},
		"logging.redaction_level": {config: &config.Logging.RedactionLevel, flagValue: fs.String("logging.redaction_level", "", "Redaction level to apply to log output. Options: none, partial, full, unset")},

		"logging.console.enabled":                          {config: &config.Logging.Console.Enabled, flagValue: fs.Bool("logging.console.enabled", false, "")},
		"logging.console.rotation.max_size":                {config: &config.Logging.Console.Rotation.MaxSize, flagValue: fs.Int("logging.console.rotation.max_size", 0, "")},
		"logging.console.rotation.max_age":                 {config: &config.Logging.Console.Rotation.MaxAge, flagValue: fs.Int("logging.console.rotation.max_age", 0, "")},
		"logging.console.rotation.localtime":               {config: &config.Logging.Console.Rotation.LocalTime, flagValue: fs.Bool("logging.console.rotation.localtime", false, "")},
		"logging.console.rotation.rotated_logs_size_limit": {config: &config.Logging.Console.Rotation.RotatedLogsSizeLimit, flagValue: fs.Int("logging.console.rotation.rotated_logs_size_limit", 0, "")},
		"logging.console.rotation.rotation_interval":       {config: &config.Logging.Console.Rotation.RotationInterval, flagValue: fs.String("logging.console.rotation.rotation_interval", "", "")},
		"logging.console.collation_buffer_size":            {config: &config.Logging.Console.CollationBufferSize, flagValue: fs.Int("logging.console.collation_buffer_size", 0, "")},
		"logging.console.log_level":                        {config: &config.Logging.Console.LogLevel, flagValue: fs.String("logging.console.log_level", "", "Options: none, error, warn, info, debug, trace")},
		"logging.console.log_keys":                         {config: &config.Logging.Console.LogKeys, flagValue: fs.String("logging.console.log_keys", "", "Comma separated log keys")},
		"logging.console.color_enabled":                    {config: &config.Logging.Console.ColorEnabled, flagValue: fs.Bool("logging.console.color_enabled", false, "")},
		"logging.console.file_output":                      {config: &config.Logging.Console.FileOutput, flagValue: fs.String("logging.console.file_output", "", "")},

		"logging.error.enabled":                          {config: &config.Logging.Error.Enabled, flagValue: fs.Bool("logging.error.enabled", false, "")},
		"logging.error.rotation.max_size":                {config: &config.Logging.Error.Rotation.MaxSize, flagValue: fs.Int("logging.error.rotation.max_size", 0, "")},
		"logging.error.rotation.max_age":                 {config: &config.Logging.Error.Rotation.MaxAge, flagValue: fs.Int("logging.error.rotation.max_age", 0, "")},
		"logging.error.rotation.localtime":               {config: &config.Logging.Error.Rotation.LocalTime, flagValue: fs.Bool("logging.error.rotation.localtime", false, "")},
		"logging.error.rotation.rotated_logs_size_limit": {config: &config.Logging.Error.Rotation.RotatedLogsSizeLimit, flagValue: fs.Int("logging.error.rotation.rotated_logs_size_limit", 0, "")},
		"logging.error.rotation.rotation_interval":       {config: &config.Logging.Error.Rotation.RotationInterval, flagValue: fs.String("logging.error.rotation.rotation_interval", "", "")},
		"logging.error.collation_buffer_size":            {config: &config.Logging.Error.CollationBufferSize, flagValue: fs.Int("logging.error.collation_buffer_size", 0, "")},

		"logging.warn.enabled":                          {config: &config.Logging.Warn.Enabled, flagValue: fs.Bool("logging.warn.enabled", false, "")},
		"logging.warn.rotation.max_size":                {config: &config.Logging.Warn.Rotation.MaxSize, flagValue: fs.Int("logging.warn.rotation.max_size", 0, "")},
		"logging.warn.rotation.max_age":                 {config: &config.Logging.Warn.Rotation.MaxAge, flagValue: fs.Int("logging.warn.rotation.max_age", 0, "")},
		"logging.warn.rotation.localtime":               {config: &config.Logging.Warn.Rotation.LocalTime, flagValue: fs.Bool("logging.warn.rotation.localtime", false, "")},
		"logging.warn.rotation.rotated_logs_size_limit": {config: &config.Logging.Warn.Rotation.RotatedLogsSizeLimit, flagValue: fs.Int("logging.warn.rotation.rotated_logs_size_limit", 0, "")},
		"logging.warn.rotation.rotation_interval":       {config: &config.Logging.Warn.Rotation.RotationInterval, flagValue: fs.String("logging.warn.rotation.rotation_interval", "", "")},
		"logging.warn.collation_buffer_size":            {config: &config.Logging.Warn.CollationBufferSize, flagValue: fs.Int("logging.warn.collation_buffer_size", 0, "")},

		"logging.info.enabled":                          {config: &config.Logging.Info.Enabled, flagValue: fs.Bool("logging.info.enabled", false, "")},
		"logging.info.rotation.max_size":                {config: &config.Logging.Info.Rotation.MaxSize, flagValue: fs.Int("logging.info.rotation.max_size", 0, "")},
		"logging.info.rotation.max_age":                 {config: &config.Logging.Info.Rotation.MaxAge, flagValue: fs.Int("logging.info.rotation.max_age", 0, "")},
		"logging.info.rotation.localtime":               {config: &config.Logging.Info.Rotation.LocalTime, flagValue: fs.Bool("logging.info.rotation.localtime", false, "")},
		"logging.info.rotation.rotated_logs_size_limit": {config: &config.Logging.Info.Rotation.RotatedLogsSizeLimit, flagValue: fs.Int("logging.info.rotation.rotated_logs_size_limit", 0, "")},
		"logging.info.rotation.rotation_interval":       {config: &config.Logging.Info.Rotation.RotationInterval, flagValue: fs.String("logging.info.rotation.rotation_interval", "", "")},
		"logging.info.collation_buffer_size":            {config: &config.Logging.Info.CollationBufferSize, flagValue: fs.Int("logging.info.collation_buffer_size", 0, "")},

		"logging.debug.enabled":                          {config: &config.Logging.Debug.Enabled, flagValue: fs.Bool("logging.debug.enabled", false, "")},
		"logging.debug.rotation.max_size":                {config: &config.Logging.Debug.Rotation.MaxSize, flagValue: fs.Int("logging.debug.rotation.max_size", 0, "")},
		"logging.debug.rotation.max_age":                 {config: &config.Logging.Debug.Rotation.MaxAge, flagValue: fs.Int("logging.debug.rotation.max_age", 0, "")},
		"logging.debug.rotation.localtime":               {config: &config.Logging.Debug.Rotation.LocalTime, flagValue: fs.Bool("logging.debug.rotation.localtime", false, "")},
		"logging.debug.rotation.rotated_logs_size_limit": {config: &config.Logging.Debug.Rotation.RotatedLogsSizeLimit, flagValue: fs.Int("logging.debug.rotation.rotated_logs_size_limit", 0, "")},
		"logging.debug.rotation.rotation_interval":       {config: &config.Logging.Debug.Rotation.RotationInterval, flagValue: fs.String("logging.debug.rotation.rotation_interval", "", "")},
		"logging.debug.collation_buffer_size":            {config: &config.Logging.Debug.CollationBufferSize, flagValue: fs.Int("logging.debug.collation_buffer_size", 0, "")},

		"logging.trace.enabled":                          {config: &config.Logging.Trace.Enabled, flagValue: fs.Bool("logging.trace.enabled", false, "")},
		"logging.trace.rotation.max_size":                {config: &config.Logging.Trace.Rotation.MaxSize, flagValue: fs.Int("logging.trace.rotation.max_size", 0, "")},
		"logging.trace.rotation.max_age":                 {config: &config.Logging.Trace.Rotation.MaxAge, flagValue: fs.Int("logging.trace.rotation.max_age", 0, "")},
		"logging.trace.rotation.localtime":               {config: &config.Logging.Trace.Rotation.LocalTime, flagValue: fs.Bool("logging.trace.rotation.localtime", false, "")},
		"logging.trace.rotation.rotated_logs_size_limit": {config: &config.Logging.Trace.Rotation.RotatedLogsSizeLimit, flagValue: fs.Int("logging.trace.rotation.rotated_logs_size_limit", 0, "")},
		"logging.trace.rotation.rotation_interval":       {config: &config.Logging.Trace.Rotation.RotationInterval, flagValue: fs.String("logging.trace.rotation.rotation_interval", "", "")},
		"logging.trace.collation_buffer_size":            {config: &config.Logging.Trace.CollationBufferSize, flagValue: fs.Int("logging.trace.collation_buffer_size", 0, "")},

		"logging.stats.enabled":                          {config: &config.Logging.Stats.Enabled, flagValue: fs.Bool("logging.stats.enabled", false, "")},
		"logging.stats.rotation.max_size":                {config: &config.Logging.Stats.Rotation.MaxSize, flagValue: fs.Int("logging.stats.rotation.max_size", 0, "")},
		"logging.stats.rotation.max_age":                 {config: &config.Logging.Stats.Rotation.MaxAge, flagValue: fs.Int("logging.stats.rotation.max_age", 0, "")},
		"logging.stats.rotation.localtime":               {config: &config.Logging.Stats.Rotation.LocalTime, flagValue: fs.Bool("logging.stats.rotation.localtime", false, "")},
		"logging.stats.rotation.rotated_logs_size_limit": {config: &config.Logging.Stats.Rotation.RotatedLogsSizeLimit, flagValue: fs.Int("logging.stats.rotation.rotated_logs_size_limit", 0, "")},
		"logging.stats.rotation.rotation_interval":       {config: &config.Logging.Stats.Rotation.RotationInterval, flagValue: fs.String("logging.stats.rotation.rotation_interval", "", "")},
		"logging.stats.collation_buffer_size":            {config: &config.Logging.Stats.CollationBufferSize, flagValue: fs.Int("logging.stats.collation_buffer_size", 0, "")},

		"logging.audit.enabled":                          {config: &config.Logging.Audit.Enabled, flagValue: fs.Bool("logging.audit.enabled", false, "")},
		"logging.audit.rotation.max_size":                {config: &config.Logging.Audit.Rotation.MaxSize, flagValue: fs.Int("logging.audit.rotation.max_size", 0, "")},
		"logging.audit.rotation.max_age":                 {config: &config.Logging.Audit.Rotation.MaxAge, flagValue: fs.Int("logging.audit.rotation.max_age", 0, "")},
		"logging.audit.rotation.localtime":               {config: &config.Logging.Audit.Rotation.LocalTime, flagValue: fs.Bool("logging.audit.rotation.localtime", false, "")},
		"logging.audit.rotation.rotated_logs_size_limit": {config: &config.Logging.Audit.Rotation.RotatedLogsSizeLimit, flagValue: fs.Int("logging.audit.rotation.rotated_logs_size_limit", 0, "")},
		"logging.audit.collation_buffer_size":            {config: &config.Logging.Audit.CollationBufferSize, flagValue: fs.Int("logging.audit.collation_buffer_size", 0, "")},
		"logging.audit.rotation.rotation_interval":       {config: &config.Logging.Audit.Rotation.RotationInterval, flagValue: fs.String("logging.audit.rotation.rotation_interval", "", "")},
		"logging.audit.audit_log_file_path":              {config: &config.Logging.Audit.AuditLogFilePath, flagValue: fs.String("logging.audit.audit_log_file_path", "", "")},
		"logging.audit.enabled_events":                   {config: &config.Logging.Audit.EnabledEvents, flagValue: fs.String("logging.audit.enabled_events", "", "")},

		"auth.bcrypt_cost": {config: &config.Auth.BcryptCost, flagValue: fs.Int("auth.bcrypt_cost", 0, "Cost to use for bcrypt password hashes")},

		"replicator.max_heartbeat":                  {config: &config.Replicator.MaxHeartbeat, flagValue: fs.String("replicator.max_heartbeat", "", "Max heartbeat value for _changes request")},
		"replicator.blip_compression":               {config: &config.Replicator.BLIPCompression, flagValue: fs.Int("replicator.blip_compression", 0, "BLIP data compression level (0-9)")},
		"replicator.max_concurrent_replications":    {config: &config.Replicator.MaxConcurrentReplications, flagValue: fs.Int("replicator.max_concurrent_replications", 0, "Maximum number of replication connections to the node")},
		"replicator.max_concurrent_changes_batches": {config: &config.Replicator.MaxConcurrentChangesBatches, flagValue: fs.Int("replicator.max_concurrent_changes_batches", 0, "Maximum number of changes batches to process concurrently per replication")},
		"replicator.max_concurrent_revs":            {config: &config.Replicator.MaxConcurrentRevs, flagValue: fs.Int("replicator.max_concurrent_revs", 0, "Maximum number of revs to process concurrently per replication")},

		"heap_profile_collection_threshold": {config: &config.HeapProfileCollectionThreshold, flagValue: fs.Uint64("heap_profile_collection_threshold", 0, "Threshold in bytes for collecting heap profiles automatically. If set, Sync Gateway will collect a memory profile when it exceeds this value. The default value will be set to 85% of the lesser of cgroup or system memory.")},
		"heap_profile_disable_collection":   {config: &config.HeapProfileDisableCollection, flagValue: fs.Bool("heap_profile_disable_collection", false, "Disables automatic heap profile collection.")},

		"unsupported.diagnostic_interface":                         {config: &config.Unsupported.DiagnosticInterface, flagValue: fs.String("unsupported.diagnostic_interface", "", "Network interface to bind diagnostic API to")},
		"unsupported.stats_log_frequency":                          {config: &config.Unsupported.StatsLogFrequency, flagValue: fs.String("unsupported.stats_log_frequency", "", "How often should stats be written to stats logs")},
		"unsupported.use_stdlib_json":                              {config: &config.Unsupported.UseStdlibJSON, flagValue: fs.Bool("unsupported.use_stdlib_json", false, "Bypass the jsoniter package and use Go's stdlib instead")},
		"unsupported.http2.enabled":                                {config: &config.Unsupported.HTTP2.Enabled, flagValue: fs.Bool("unsupported.http2.enabled", false, "Whether HTTP2 support is enabled")},
		"unsupported.serverless.enabled":                           {config: &config.Unsupported.Serverless.Enabled, flagValue: fs.Bool("unsupported.serverless.enabled", false, "Settings for running Sync Gateway in serverless mode.")},
		"unsupported.serverless.min_config_fetch_interval":         {config: &config.Unsupported.Serverless.MinConfigFetchInterval, flagValue: fs.String("unsupported.serverless.min_config_fetch_interval", "", "How long to cache configs fetched from the buckets for. This cache is used for requested databases that SG does not know about.")},
		"unsupported.use_xattr_config":                             {config: &config.Unsupported.UseXattrConfig, flagValue: fs.Bool("unsupported.use_xattr_config", false, "Store database configurations in system xattrs")},
		"unsupported.allow_dbconfig_env_vars":                      {config: &config.Unsupported.AllowDbConfigEnvVars, flagValue: fs.Bool("unsupported.allow_dbconfig_env_vars", true, "Can be set to false to skip environment variable expansion in database configs")},
		"unsupported.user_queries":                                 {config: &config.Unsupported.UserQueries, flagValue: fs.Bool("unsupported.user_queries", false, "Whether user-query APIs are enabled")},
		"unsupported.audit_info_provider.global_info_env_var_name": {config: &config.Unsupported.AuditInfoProvider.GlobalInfoEnvVarName, flagValue: fs.String("unsupported.audit_info_provider.global_info_env_var_name", "", "Environment variable name to get global audit event info from")},
		"unsupported.audit_info_provider.request_info_header_name": {config: &config.Unsupported.AuditInfoProvider.RequestInfoHeaderName, flagValue: fs.String("unsupported.audit_info_provider.request_info_header_name", "", "Header name to get request audit event info from")},
		"unsupported.effective_user_header_name":                   {config: &config.Unsupported.EffectiveUserHeaderName, flagValue: fs.String("unsupported.effective_user_header_name", "", "HTTP header name to get effective user id from")},

		// Note: These flags are X.509-only. Username/passwords are rejected if specified, as we are not allowing them to be used in the command line flags, only the config file.
		"database_credentials": {config: &config.DatabaseCredentials, flagValue: fs.String("database_credentials", "null", "JSON-encoded per-database credentials (X.509 only), that can be used instead of the bootstrap ones. This will override bucket_credentials that target the bucket that the database is in.")},
		"bucket_credentials":   {config: &config.BucketCredentials, flagValue: fs.String("bucket_credentials", "null", "JSON-encoded per-bucket credentials (X.509 only), that can be used instead of the bootstrap ones.")},

		"max_file_descriptors": {config: &config.MaxFileDescriptors, flagValue: fs.Uint64("max_file_descriptors", 0, "Max # of open file descriptors (RLIMIT_NOFILE)")},

		"couchbase_keepalive_interval": {config: &config.CouchbaseKeepaliveInterval, flagValue: fs.Int("couchbase_keepalive_interval", 0, "TCP keep-alive interval between SG and Couchbase server")},
	}
}

// fillConfigWithFlags fills in the config values from registerConfigFlags if the user
// has explicitly set the flags
func fillConfigWithFlags(fs *flag.FlagSet, flags map[string]configFlag) error {
	var errorMessages *base.MultiError
	fs.Visit(func(f *flag.Flag) {
		if val, exists := flags[f.Name]; exists {
			// force disabled flags to error - we don't want users specifying them at all, even if they are non-functional
			if val.disabled {
				errorMessages = errorMessages.Append(fmt.Errorf("command line flag %q is no longer supported and must be removed %s", f.Name, val.disabledErrorMessage))
				return
			}
			rval := reflect.ValueOf(val.config).Elem()

			pointer := true // Distinguish if to use rval.Set or *val.config
			// Convert to pointer if not already
			if rval.Kind() != reflect.Ptr && rval.CanAddr() {
				rval = rval.Addr()
				pointer = false
			}

			switch rval.Interface().(type) {
			case *string:
				if pointer {
					rval.Set(reflect.ValueOf(val.flagValue))
				} else {
					*val.config.(*string) = *val.flagValue.(*string)
				}
			case *[]string:
				list := strings.Split(*val.flagValue.(*string), ",")
				*val.config.(*[]string) = list
			case *[]uint:
				// split by comma and parse
				strs := strings.Split(*val.flagValue.(*string), ",")
				uints := make([]uint, 0, len(strs))
				for _, s := range strs {
					u, err := strconv.ParseUint(s, 10, 64)
					if err != nil {
						err = fmt.Errorf("flag %s error: %w", f.Name, err)
						errorMessages = errorMessages.Append(err)
						return
					}
					uints = append(uints, uint(u))
				}
				*val.config.(*[]uint) = uints
			case *uint:
				if pointer {
					rval.Set(reflect.ValueOf(val.flagValue))
				} else {
					*val.config.(*uint) = *val.flagValue.(*uint)
				}
			case *uint64:
				if pointer {
					rval.Set(reflect.ValueOf(val.flagValue))
				} else {
					*val.config.(*uint64) = *val.flagValue.(*uint64)
				}
			case *int:
				if pointer {
					rval.Set(reflect.ValueOf(val.flagValue))
				} else {
					*val.config.(*int) = *val.flagValue.(*int)
				}
			case *bool:
				if pointer {
					rval.Set(reflect.ValueOf(val.flagValue))
				} else {
					*val.config.(*bool) = *val.flagValue.(*bool)
				}
			case *int64:
				if pointer {
					rval.Set(reflect.ValueOf(val.flagValue))
				} else {
					*val.config.(*int64) = *val.flagValue.(*int64)
				}
			case *base.ConfigDuration:
				duration, err := time.ParseDuration(*val.flagValue.(*string))
				if err != nil {
					err = fmt.Errorf("flag %s error: %w", f.Name, err)
					errorMessages = errorMessages.Append(err)
					return
				}
				if pointer {
					rval.Set(reflect.ValueOf(&base.ConfigDuration{D: &duration}))
				} else {
					*val.config.(*base.ConfigDuration) = base.ConfigDuration{D: &duration}
				}
			case *base.RedactionLevel:
				var rl base.RedactionLevel
				err := rl.UnmarshalText([]byte(*val.flagValue.(*string)))
				if err != nil {
					err = fmt.Errorf("flag %s error: %w", f.Name, err)
					errorMessages = errorMessages.Append(err)
					return
				}
				*val.config.(*base.RedactionLevel) = rl
			case *base.LogLevel:
				var ll base.LogLevel
				err := ll.UnmarshalText([]byte(*val.flagValue.(*string)))
				if err != nil {
					err = fmt.Errorf("flag %s error: %w", f.Name, err)
					errorMessages = errorMessages.Append(err)
					return
				}
				rval.Set(reflect.ValueOf(&ll))
			case *PerDatabaseCredentialsConfig:
				str := *val.flagValue.(*string)
				// Decode X.509-only JSON and map into generic credentials config
				var dbCredentials PerDatabaseCredentialsConfigX509
				d := base.JSONDecoder(strings.NewReader(str))
				d.DisallowUnknownFields()
				err := d.Decode(&dbCredentials)
				if err != nil {
					err = fmt.Errorf(
						"flag %s for value %q error: only X.509 cert/key paths are supported (fields: x509_cert_path, x509_key_path); username/password are not allowed: %w",
						f.Name, str, err,
					)
					errorMessages = errorMessages.Append(err)
					return
				}
				*val.config.(*PerDatabaseCredentialsConfig) = *dbCredentials.asPerDatabaseCredentialsConfig()
			case *base.PerBucketCredentialsConfig:
				str := *val.flagValue.(*string)
				// Decode X.509-only JSON and map into generic credentials config
				var bucketCredentials PerBucketCredentialsConfigX509
				d := base.JSONDecoder(strings.NewReader(str))
				d.DisallowUnknownFields()
				err := d.Decode(&bucketCredentials)
				if err != nil {
					err = fmt.Errorf(
						"flag %s for value %q error: only X.509 cert/key paths are supported (fields: x509_cert_path, x509_key_path); username/password are not allowed: %w",
						f.Name, str, err,
					)
					errorMessages = errorMessages.Append(err)
					return
				}
				*val.config.(*base.PerBucketCredentialsConfig) = *bucketCredentials.asPerBucketCredentialsConfig()
			default:
				errorMessages = errorMessages.Append(fmt.Errorf("unknown type %v for flag %v", rval.Type(), f.Name))
			}
		}
	})
	return errorMessages.ErrorOrNil()
}

type PerDatabaseCredentialsConfigX509 map[string]*base.CredentialsConfigX509
type PerBucketCredentialsConfigX509 map[string]*base.CredentialsConfigX509

// asPerDatabaseCredentialsConfig converts an X.509 specific PerDatabaseCredentialsConfig struct into a generic credentials config
func (dbCredsX509 *PerDatabaseCredentialsConfigX509) asPerDatabaseCredentialsConfig() *PerDatabaseCredentialsConfig {
	perDatabaseCredentialsConfig := make(PerDatabaseCredentialsConfig)
	for k, v := range *dbCredsX509 {
		perDatabaseCredentialsConfig[k] = &base.CredentialsConfig{
			CredentialsConfigX509: base.CredentialsConfigX509{
				X509CertPath: v.X509CertPath,
				X509KeyPath:  v.X509KeyPath,
			},
		}
	}
	return &perDatabaseCredentialsConfig
}

func (bucketCredsX509 *PerBucketCredentialsConfigX509) asPerBucketCredentialsConfig() *base.PerBucketCredentialsConfig {
	perBucketCredentialsConfig := make(base.PerBucketCredentialsConfig)
	for k, v := range *bucketCredsX509 {
		perBucketCredentialsConfig[k] = &base.CredentialsConfig{
			CredentialsConfigX509: base.CredentialsConfigX509{
				X509CertPath: v.X509CertPath,
				X509KeyPath:  v.X509KeyPath,
			},
		}
	}
	return &perBucketCredentialsConfig
}
