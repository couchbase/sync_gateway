package rest

import (
	"flag"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// configFlag stores the config value, and the corresponding flag value
type configFlag struct {
	config    interface{}
	flagValue interface{}
}

// registerConfigFlags holds a map of the flag values with the configFlag it goes with
// (which stores the corresponding config field pointer and flag value).
func registerConfigFlags(config *StartupConfig, fs *flag.FlagSet) map[string]configFlag {
	return map[string]configFlag{
		"bootstrap.group_id":                {&config.Bootstrap.ConfigGroupID, fs.String("bootstrap.group_id", "", "The config group ID to use when discovering databases. Allows for non-homogenous configuration")},
		"bootstrap.config_update_frequency": {&config.Bootstrap.ConfigUpdateFrequency, fs.String("bootstrap.config_update_frequency", persistentConfigDefaultUpdateFrequency.String(), "How often to poll Couchbase Server for new config changes")},
		"bootstrap.server":                  {&config.Bootstrap.Server, fs.String("bootstrap.server", "", "Couchbase Server connection string/URL")},
		"bootstrap.username":                {&config.Bootstrap.Username, fs.String("bootstrap.username", "", "Username for authenticating to server")},
		"bootstrap.password":                {&config.Bootstrap.Password, fs.String("bootstrap.password", "", "Password for authenticating to server")},
		"bootstrap.ca_cert_path":            {&config.Bootstrap.CACertPath, fs.String("bootstrap.ca_cert_path", "", "Root CA cert path for TLS connection")},
		"bootstrap.server_tls_skip_verify":  {&config.Bootstrap.ServerTLSSkipVerify, fs.Bool("bootstrap.server_tls_skip_verify", false, "Allow empty server CA Cert Path without attempting to use system root pool")},
		"bootstrap.x509_cert_path":          {&config.Bootstrap.X509CertPath, fs.String("bootstrap.x509_cert_path", "", "Cert path (public key) for X.509 bucket auth")},
		"bootstrap.x509_key_path":           {&config.Bootstrap.X509KeyPath, fs.String("bootstrap.x509_key_path", "", "Key path (private key) for X.509 bucket auth")},
		"bootstrap.use_tls_server":          {&config.Bootstrap.UseTLSServer, fs.Bool("bootstrap.use_tls_server", false, "Forces the connection to Couchbase Server to use TLS")},

		"api.public_interface":                              {&config.API.PublicInterface, fs.String("api.public_interface", "", "Network interface to bind public API to")},
		"api.admin_interface":                               {&config.API.AdminInterface, fs.String("api.admin_interface", "", "Network interface to bind admin API to")},
		"api.metrics_interface":                             {&config.API.MetricsInterface, fs.String("api.metrics_interface", "", "Network interface to bind metrics API to")},
		"api.profile_interface":                             {&config.API.ProfileInterface, fs.String("api.profile_interface", "", "Network interface to bind profiling API to")},
		"api.admin_interface_authentication":                {&config.API.AdminInterfaceAuthentication, fs.Bool("api.admin_interface_authentication", false, "Whether the admin API requires authentication")},
		"api.metrics_interface_authentication":              {&config.API.MetricsInterfaceAuthentication, fs.Bool("api.metrics_interface_authentication", false, "Whether the metrics API requires authentication")},
		"api.enable_admin_authentication_permissions_check": {&config.API.EnableAdminAuthenticationPermissionsCheck, fs.Bool("api.enable_admin_authentication_permissions_check", false, "Whether to enable the DP permissions check feature of admin auth")},
		"api.server_read_timeout":                           {&config.API.ServerReadTimeout, fs.String("api.server_read_timeout", "", "Maximum duration.Second before timing out read of the HTTP(S) request")},
		"api.server_write_timeout":                          {&config.API.ServerWriteTimeout, fs.String("api.server_write_timeout", "", "Maximum duration.Second before timing out write of the HTTP(S) response")},
		"api.read_header_timeout":                           {&config.API.ReadHeaderTimeout, fs.String("api.read_header_timeout", "", "The amount of time allowed to read request headers")},
		"api.idle_timeout":                                  {&config.API.IdleTimeout, fs.String("api.idle_timeout", "", "The maximum amount of time to wait for the next request when keep-alives are enabled")},
		"api.pretty":                                        {&config.API.Pretty, fs.Bool("api.pretty", false, "Pretty-print JSON responses")},
		"api.max_connections":                               {&config.API.MaximumConnections, fs.Uint("api.max_connections", 0, "Max # of incoming HTTP connections to accept")},
		"api.compress_responses":                            {&config.API.CompressResponses, fs.Bool("api.compress_responses", false, "If false, disables compression of HTTP responses")},
		"api.hide_product_version":                          {&config.API.CompressResponses, fs.Bool("api.hide_product_version", false, "Whether product versions removed from Server headers and REST API responses")},

		"api.https.tls_minimum_version": {&config.API.HTTPS.TLSMinimumVersion, fs.String("api.https.tls_minimum_version", "", "The minimum allowable TLS version for the REST APIs")},
		"api.https.tls_cert_path":       {&config.API.HTTPS.TLSCertPath, fs.String("api.https.tls_cert_path", "", "The TLS cert file to use for the REST APIs")},
		"api.https.tls_key_path":        {&config.API.HTTPS.TLSKeyPath, fs.String("api.https.tls_key_path", "", "The TLS key file to use for the REST APIs")},

		"api.cors.origin":       {&config.API.CORS.Origin, fs.String("api.cors.origin", "", "List of comma seperated allowed origins. Use '*' to allow access from everywhere")},
		"api.cors.login_origin": {&config.API.CORS.LoginOrigin, fs.String("api.cors.login_origin", "", "List of comma seperated allowed login origins")},
		"api.cors.headers":      {&config.API.CORS.Headers, fs.String("api.cors.headers", "", "List of comma seperated allowed headers")},
		"api.cors.max_age":      {&config.API.CORS.MaxAge, fs.Int("api.cors.max_age", 0, "Maximum age of the CORS Options request")},

		"logging.log_file_path":   {&config.Logging.LogFilePath, fs.String("logging.log_file_path", "", "Absolute or relative path on the filesystem to the log file directory. A relative path is from the directory that contains the Sync Gateway executable file")},
		"logging.redaction_level": {&config.Logging.RedactionLevel, fs.String("logging.redaction_level", "", "Redaction level to apply to log output. Options: none, partial, full, unset")},

		"logging.console.enabled":                          {&config.Logging.Console.Enabled, fs.Bool("logging.console.enabled", false, "")},
		"logging.console.rotation.max_size":                {&config.Logging.Console.Rotation.MaxSize, fs.Int("logging.console.rotation.max_size", 0, "")},
		"logging.console.rotation.max_age":                 {&config.Logging.Console.Rotation.MaxAge, fs.Int("logging.console.rotation.max_age", 0, "")},
		"logging.console.rotation.localtime":               {&config.Logging.Console.Rotation.LocalTime, fs.Bool("logging.console.rotation.localtime", false, "")},
		"logging.console.rotation.rotated_logs_size_limit": {&config.Logging.Console.Rotation.RotatedLogsSizeLimit, fs.Int("logging.console.rotation.rotated_logs_size_limit", 0, "")},
		"logging.console.collation_buffer_size":            {&config.Logging.Console.CollationBufferSize, fs.Int("logging.console.collation_buffer_size", 0, "")},
		"logging.console.log_level":                        {&config.Logging.Console.LogLevel, fs.String("logging.console.log_level", "", "Options: none, error, warn, info, debug, trace")},
		"logging.console.log_keys":                         {&config.Logging.Console.LogKeys, fs.String("logging.console.log_keys", "", "Comma seperated log keys")},
		"logging.console.color_enabled":                    {&config.Logging.Console.ColorEnabled, fs.Bool("logging.console.color_enabled", false, "")},
		"logging.console.file_output":                      {&config.Logging.Console.FileOutput, fs.String("logging.console.file_output", "", "")}, // can be used to override the default stderr output, and write to the file specified instead.

		"logging.error.enabled":                          {&config.Logging.Error.Enabled, fs.Bool("logging.error.enabled", false, "")},
		"logging.error.rotation.max_size":                {&config.Logging.Error.Rotation.MaxSize, fs.Int("logging.error.rotation.max_size", 0, "")},
		"logging.error.rotation.max_age":                 {&config.Logging.Error.Rotation.MaxAge, fs.Int("logging.error.rotation.max_age", 0, "")},
		"logging.error.rotation.localtime":               {&config.Logging.Error.Rotation.LocalTime, fs.Bool("logging.error.rotation.localtime", false, "")},
		"logging.error.rotation.rotated_logs_size_limit": {&config.Logging.Error.Rotation.RotatedLogsSizeLimit, fs.Int("logging.error.rotation.rotated_logs_size_limit", 0, "")},
		"logging.error.collation_buffer_size":            {&config.Logging.Error.CollationBufferSize, fs.Int("logging.error.collation_buffer_size", 0, "")},

		"logging.warn.enabled":                          {&config.Logging.Warn.Enabled, fs.Bool("logging.warn.enabled", false, "")},
		"logging.warn.rotation.max_size":                {&config.Logging.Warn.Rotation.MaxSize, fs.Int("logging.warn.rotation.max_size", 0, "")},
		"logging.warn.rotation.max_age":                 {&config.Logging.Warn.Rotation.MaxAge, fs.Int("logging.warn.rotation.max_age", 0, "")},
		"logging.warn.rotation.localtime":               {&config.Logging.Warn.Rotation.LocalTime, fs.Bool("logging.warn.rotation.localtime", false, "")},
		"logging.warn.rotation.rotated_logs_size_limit": {&config.Logging.Warn.Rotation.RotatedLogsSizeLimit, fs.Int("logging.warn.rotation.rotated_logs_size_limit", 0, "")},
		"logging.warn.collation_buffer_size":            {&config.Logging.Warn.CollationBufferSize, fs.Int("logging.warn.collation_buffer_size", 0, "")},

		"logging.info.enabled":                          {&config.Logging.Info.Enabled, fs.Bool("logging.info.enabled", false, "")},
		"logging.info.rotation.max_size":                {&config.Logging.Info.Rotation.MaxSize, fs.Int("logging.info.rotation.max_size", 0, "")},
		"logging.info.rotation.max_age":                 {&config.Logging.Info.Rotation.MaxAge, fs.Int("logging.info.rotation.max_age", 0, "")},
		"logging.info.rotation.localtime":               {&config.Logging.Info.Rotation.LocalTime, fs.Bool("logging.info.rotation.localtime", false, "")},
		"logging.info.rotation.rotated_logs_size_limit": {&config.Logging.Info.Rotation.RotatedLogsSizeLimit, fs.Int("logging.info.rotation.rotated_logs_size_limit", 0, "")},
		"logging.info.collation_buffer_size":            {&config.Logging.Info.CollationBufferSize, fs.Int("logging.info.collation_buffer_size", 0, "")},

		"logging.debug.enabled":                          {&config.Logging.Debug.Enabled, fs.Bool("logging.debug.enabled", false, "")},
		"logging.debug.rotation.max_size":                {&config.Logging.Debug.Rotation.MaxSize, fs.Int("logging.debug.rotation.max_size", 0, "")},
		"logging.debug.rotation.max_age":                 {&config.Logging.Debug.Rotation.MaxAge, fs.Int("logging.debug.rotation.max_age", 0, "")},
		"logging.debug.rotation.localtime":               {&config.Logging.Debug.Rotation.LocalTime, fs.Bool("logging.debug.rotation.localtime", false, "")},
		"logging.debug.rotation.rotated_logs_size_limit": {&config.Logging.Debug.Rotation.RotatedLogsSizeLimit, fs.Int("logging.debug.rotation.rotated_logs_size_limit", 0, "")},
		"logging.debug.collation_buffer_size":            {&config.Logging.Debug.CollationBufferSize, fs.Int("logging.debug.collation_buffer_size", 0, "")},

		"logging.trace.enabled":                          {&config.Logging.Trace.Enabled, fs.Bool("logging.trace.enabled", false, "")},
		"logging.trace.rotation.max_size":                {&config.Logging.Trace.Rotation.MaxSize, fs.Int("logging.trace.rotation.max_size", 0, "")},
		"logging.trace.rotation.max_age":                 {&config.Logging.Trace.Rotation.MaxAge, fs.Int("logging.trace.rotation.max_age", 0, "")},
		"logging.trace.rotation.localtime":               {&config.Logging.Trace.Rotation.LocalTime, fs.Bool("logging.trace.rotation.localtime", false, "")},
		"logging.trace.rotation.rotated_logs_size_limit": {&config.Logging.Trace.Rotation.RotatedLogsSizeLimit, fs.Int("logging.trace.rotation.rotated_logs_size_limit", 0, "")},
		"logging.trace.collation_buffer_size":            {&config.Logging.Trace.CollationBufferSize, fs.Int("logging.trace.collation_buffer_size", 0, "")},

		"logging.stats.enabled":                          {&config.Logging.Stats.Enabled, fs.Bool("logging.stats.enabled", false, "")},
		"logging.stats.rotation.max_size":                {&config.Logging.Stats.Rotation.MaxSize, fs.Int("logging.stats.rotation.max_size", 0, "")},
		"logging.stats.rotation.max_age":                 {&config.Logging.Stats.Rotation.MaxAge, fs.Int("logging.stats.rotation.max_age", 0, "")},
		"logging.stats.rotation.localtime":               {&config.Logging.Stats.Rotation.LocalTime, fs.Bool("logging.stats.rotation.localtime", false, "")},
		"logging.stats.rotation.rotated_logs_size_limit": {&config.Logging.Stats.Rotation.RotatedLogsSizeLimit, fs.Int("logging.stats.rotation.rotated_logs_size_limit", 0, "")},
		"logging.stats.collation_buffer_size":            {&config.Logging.Stats.CollationBufferSize, fs.Int("logging.stats.collation_buffer_size", 0, "")},

		"auth.bcrypt_cost": {&config.Auth.BcryptCost, fs.Int("auth.bcrypt_cost", 0, "Cost to use for bcrypt password hashes")},

		"replicator.max_heartbeat":    {&config.Replicator.MaxHeartbeat, fs.String("replicator.max_heartbeat", "", "Max heartbeat value for _changes request")},
		"replicator.blip_compression": {&config.Replicator.BLIPCompression, fs.Int("replicator.blip_compression", 0, "BLIP data compression level (0-9)")},

		"unsupported.stats_log_frequency": {&config.Unsupported.StatsLogFrequency, fs.String("unsupported.stats_log_frequency", "", "How often should stats be written to stats logs")},
		"unsupported.use_stdlib_json":     {&config.Unsupported.UseStdlibJSON, fs.Bool("unsupported.use_stdlib_json", false, "Bypass the jsoniter package and use Go's stdlib instead")},

		"unsupported.http2.enabled": {&config.Unsupported.HTTP2.Enabled, fs.Bool("unsupported.http2.enabled", false, "Whether HTTP2 support is enabled")},

		"database_credentials": {&config.DatabaseCredentials, fs.String("database_credentials", "null", "JSON-encoded per-database credentials")},

		"max_file_descriptors": {&config.MaxFileDescriptors, fs.Uint64("max_file_descriptors", 0, "Max # of open file descriptors (RLIMIT_NOFILE)")},

		"couchbase_keepalive_interval": {&config.CouchbaseKeepaliveInterval, fs.Int("couchbase_keepalive_interval", 0, "TCP keep-alive interval between SG and Couchbase server")},
	}
}

// fillConfigWithFlags fills in the config values from registerConfigFlags if the user
// has explicitly set the flags
func fillConfigWithFlags(fs *flag.FlagSet, flags map[string]configFlag) error {
	var errorMessages *base.MultiError
	fs.Visit(func(f *flag.Flag) {
		if val, exists := flags[f.Name]; exists {
			rval := reflect.ValueOf(val.config).Elem()

			pointer := true // Distinguish if to use rval.Set or *val.config
			// Convert to pointer if not already
			if rval.Kind() != reflect.Ptr && rval.CanAddr() {
				rval = rval.Addr()
				pointer = false
			}

			switch rval.Interface().(type) {
			case *string:
				*val.config.(*string) = *val.flagValue.(*string)
			case *[]string:
				list := strings.Split(*val.flagValue.(*string), ",")
				*val.config.(*[]string) = list
			case *uint:
				*val.config.(*uint) = *val.flagValue.(*uint)
			case *uint64:
				*val.config.(*uint64) = *val.flagValue.(*uint64)
			case *int:
				if pointer {
					rval.Set(reflect.ValueOf(val.flagValue))
				} else {
					*val.config.(*int) = *val.flagValue.(*int)
				}
			case *bool:
				rval.Set(reflect.ValueOf(val.flagValue))
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
				var dbCredentials PerDatabaseCredentialsConfig
				d := base.JSONDecoder(strings.NewReader(str))
				d.DisallowUnknownFields()
				err := d.Decode(&dbCredentials)
				if err != nil {
					err = fmt.Errorf("flag %s for value %q error: %w", f.Name, str, err)
					errorMessages = errorMessages.Append(err)
					return
				}
				*val.config.(*PerDatabaseCredentialsConfig) = dbCredentials
			default:
				errorMessages = errorMessages.Append(fmt.Errorf("Unknown type %v for flag %v\n", rval.Type(), f.Name))
			}
		}
	})
	return errorMessages.ErrorOrNil()
}
