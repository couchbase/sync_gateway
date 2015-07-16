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

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// Register profiling handlers (see Go docs)
import _ "net/http/pprof"

// Default value of ServerConfig.Interface
var DefaultInterface = ":4984"

// Default value of ServerConfig.AdminInterface
var DefaultAdminInterface = "127.0.0.1:4985" // Only accessible on localhost!

// Default value of DbConfig.Server
// The default Couchbase database server is Walrus, a simple in-memory database
// for development and testing.
var DefaultServer = "walrus:"

// Default value of DbConfig.Pool
var DefaultPool = "default"

var config *ServerConfig

const (
	// Default value of ServerConfig.MaxCouchbaseConnections
	DefaultMaxCouchbaseConnections = 16

	// Default value of ServerConfig.MaxCouchbaseOverflow. Zero (0) implies no maximum.
	DefaultMaxCouchbaseOverflowConnections = 0

	// Default value of ServerConfig.MaxIncomingConnections. Zero (0) implies no maximum.
	DefaultMaxIncomingConnections = 0

	// Default value of ServerConfig.MaxFileDescriptors
	DefaultMaxFileDescriptors uint64 = 5000
)

// JSON object that defines the server configuration.
type ServerConfig struct {
	Interface                      *string         // HTTP address (IP address for localhost and the port) that the Sync REST API
	                                               // listens on; the default is ":4984"; it is not necessary to specify the host
	                                               // name, which is assumed to be localhost. If you include a host name, it must be
	                                               // "localhost" or the IP address of localhost.
	                                               // The default is defined above in DefaultInterface.
	SSLCert                        *string         // Absolute or relative path on the filesystem to the TLS certificate file,
	                                               // if TLS is used to secure Sync Gateway connections, or "nil" for plaintext
	                                               // connections
	SSLKey                         *string         // Absolute or relative path on the filesystem to the TLS private key file,
	                                               // if TLS is used to secure Sync Gateway connections, or "nil" for plaintext
	                                               // connections
	ServerReadTimeout              *int            // Maximum duration in seconds before timing out the read of an HTTP(S) request
	ServerWriteTimeout             *int            // Maximum duration in seconds before timing out the write of an HTTP(S) response
	AdminInterface                 *string         // HTTP address (IP address for localhost and the port) that the Admin REST API
	                                               // listens on; the default is "127.0.0.1:4985"; it is not necessary to specify
	                                               // the host name, which is assumed to be localhost. If you include a host name,
	                                               // it must be "localhost" or the IP address of localhost.
	                                               // The default is defined above in DefaultAdminInterface.
	AdminUI                        *string         // URL of the Sync Gateway Admin Console HTML page. If omitted, the bundled Admin
	                                               // Console at localhost:4985/_admin/ is used.
	ProfileInterface               *string         // Interface to bind the Go profile API to, in order to obtain go profiling
	                                               // information; there is no default. It is not necessary to specify the host name,
	                                               // which is assumed to be localhost. If you include a host name, it must be
	                                               // "localhost" or the IP address of localhost.
	ConfigServer                   *string         // URL of a Couchbase database-configuration server (for dynamic database discovery).
	                                               // A database-configuration server allows Sync Gateway to load a database configuration
	                                               // dynamically from a remote endpoint. If a database-configuration server is defined, when
	                                               // Sync Gateway gets a request for a database that it doesn't know about, then
	                                               // Sync Gateway will attempt to retrieve the database configuration properties
	                                               // from the URL ConfigServer/DBname, where DBname is the database name.
	Persona                        *PersonaConfig  // Configuration for Mozilla Persona authentication
	Facebook                       *FacebookConfig // Configuration for Facebook Login authentication
	CORS                           *CORSConfig     // Configuration for allowing CORS (cross-origin resource sharing)
	Log                            []string        // Comma-separated list of log keys to enable for logging. If defined, logging is
	                                               // limited to log messages that contain the specified log keys.
	                                               // Log keys include (the most commonly used keys are prefaced by an asterisk):
	                                               //
                                                   //    Access: Log access() calls made by the sync function.
                                                   //    Attach: Log attachment processing.
                                                   //    Auth: Log authentication.
                                                   //    Bucket: Verbosely log Sync Gateway interactions with the bucket.
                                                   //    * Cache/Cache+: Log interactions with Sync Gateway's in-memory channel cache.
                                                   //    * Changes/Changes+: Log processing of _changes requests.
                                                   //    * CRUD/CRUD+: Log updates made by Sync Gateway to documents.
                                                   //    DCP: Verbosely Log DCP-feed processing.
                                                   //    Events/Events+: Log event processing (webhooks).
                                                   //    Feed/Feed+: Log TAP-feed processing.
                                                   //    * HTTP/HTTP+: Log all requests made to the Sync Gateway REST APIs (Sync and Admin).
                                                   //    Note: The log keyword HTTP is always enabled, which means that HTTP requests and
                                                   //    error responses are always logged (in a non-verbose manner). HTTP+ provides more
                                                   //    verbose HTTP logging.
	                                               // 
	LogFilePath                    *string         // Absolute or relative path on the filesystem to the log file. If absent, log
	                                               // to stderr.
	Pretty                         bool            // Whether to pretty-print JSON responses
	DeploymentID                   *string         // Optional customer/deployment ID for statistics reporting
	StatsReportInterval            *float64        // Optional statistics reporting interval in seconds (0 to disable)
	MaxCouchbaseConnections        *int            // Maximum number of sockets to open to a Couchbase Server node. If omitted,
	                                               // the default is used (it is defined above in DefaultMaxCouchbaseConnections).
	MaxCouchbaseOverflow           *int            // Maximum number of overflow sockets to open. If omitted, the default is
	                                               // used (it is defined above in DefaultMaxCouchbaseOverflowConnections).
	CouchbaseKeepaliveInterval     *int            // TCP keep-alive interval in seconds between Sync Gateway and Couchbase Server
	SlowServerCallWarningThreshold *int            // Log warnings if database calls take this many milliseconds or longer. The time
	                                               // is measured from a Sync Gateway request to the Couchbase Server response.
	MaxIncomingConnections         *int            // Maximum number of incoming HTTP connections to accept. If omitted, the
	                                               // default is used (it is defined above in DefaultMaxIncomingConnections).
	MaxFileDescriptors             *uint64         // Maximum number of open file descriptors (RLIMIT_NOFILE). If omitted, the
	                                               // default is used (it is defined above in DefaultMaxFileDescriptors).
	CompressResponses              *bool           // Whether to compress HTTP responses
	Databases                      DbConfigMap     // Pre-configured databases, mapped by name
	MaxHeartbeat                   uint64          // Maximum heartbeat value for _changes feed requests (in seconds)
}

// JSON object that defines a database configuration within the ServerConfig.
type DbConfig struct {
	Name               string                         `json:"name"`                           // Database name for use by the REST APIs (stored as a key in JSON)
	Server             *string                        `json:"server"`                         // Couchbase Server (or Walrus) URL; the default is "walrus:"
	Username           string                         `json:"username,omitempty"`             // Username for authenticating to Couchbase Server
	Password           string                         `json:"password,omitempty"`             // Password for authenticating to Couchbase Server
	Bucket             *string                        `json:"bucket"`                         // Bucket name on Couchbase Server; defaults to the same value as "Name"
	Pool               *string                        `json:"pool"`                           // Name of the Couchbase Server pool in which to find buckets; the default is "default"
	Sync               *string                        `json:"sync"`                           // Sync function, which defines which users can read, update, or delete which documents
	Users              map[string]*db.PrincipalConfig `json:"users,omitempty"`                // Initial user accounts
	Roles              map[string]*db.PrincipalConfig `json:"roles,omitempty"`                // Initial roles
	RevsLimit          *uint32                        `json:"revs_limit,omitempty"`           // Maximum depth to which a document's revision tree can grow
	ImportDocs         interface{}                    `json:"import_docs,omitempty"`          // False, true, or "continuous"
	Shadow             *ShadowConfig                  `json:"shadow,omitempty"`               // External bucket to shadow
	EventHandlers      interface{}                    `json:"event_handlers,omitempty"`       // Event handlers (for webhooks)
	FeedType           string                         `json:"feed_type,omitempty"`            // Feed type: "DCP" or "TAP"; the default depends on the Couchbase server version.
	AllowEmptyPassword bool                           `json:"allow_empty_password,omitempty"` // Whether to allow empty passwords for Couchbase Server authentication. Defaults to false.
	CacheConfig        *CacheConfig                   `json:"cache,omitempty"`                // Cache settings
}

type DbConfigMap map[string]*DbConfig

type PersonaConfig struct {
	Origin   string // URL of Mozilla Persona Identity Provider (IdP) server for Persona authentication
	Register bool   // Whether the Mozilla Persona IdP server will register new user accounts (true or false)
}

type FacebookConfig struct {
	Register bool // Whether the Facebook Login server will register new user accounts (true or false)
}

type CORSConfig struct {
	Origin      []string // Comma-separated list of allowed origins; use an asterisk (*) to allow access from everywhere
	LoginOrigin []string // Comma-separated list of allowed login origins
	Headers     []string // Comma-separated list of allowed HTTP headers
	MaxAge      int      // Value for the Access-Control-Max-Age header. This is the the number of seconds
	                     // that the response to a CORS preflight request can be cached before sending
	                     // another preflight request.
}

type ShadowConfig struct {
	Server       *string `json:"server"`                 // Couchbase server URL
	Pool         *string `json:"pool,omitempty"`         // Couchbase pool name; the default is "default"
	Bucket       string  `json:"bucket"`                 // Bucket name
	Username     string  `json:"username,omitempty"`     // Username for authenticating to the Couchbase server
	Password     string  `json:"password,omitempty"`     // Password for authenticating to the Couchbase server
	Doc_id_regex *string `json:"doc_id_regex,omitempty"` // Optional regex that document IDs must match
	FeedType     string  `json:"feed_type,omitempty"`    // Feed type: "DCP" or "TAP"; defaults to TAP
}

type EventHandlerConfig struct {
	MaxEventProc    uint           `json:"max_processes,omitempty"`    // Maximum concurrent event handling goroutines; the default is 500
	WaitForProcess  string         `json:"wait_for_process,omitempty"` // Maximum wait time when the event queue is full (milliseconds)' the default is 5
	DocumentChanged []*EventConfig `json:"document_changed,omitempty"` // The document_changed event
}

type EventConfig struct {
	HandlerType string  `json:"handler"`           // Handler type ("webhook")
	Url         string  `json:"url,omitempty"`     // URL to which to post documents (for a webhook event handler)
	Filter      string  `json:"filter,omitempty"`  // Filter function to determine which documents to post (for a webhook event handler)
	Timeout     *uint64 `json:"timeout,omitempty"` // Timeout in seconds (for a webhook event handler); the default is 60
}

type CacheConfig struct {
	CachePendingSeqMaxWait *uint32 `json:"max_wait_pending,omitempty"` // Maximum wait time in milliseconds for a pending sequence before skipping the sequence
	CachePendingSeqMaxNum  *int    `json:"max_num_pending,omitempty"`  // Maximum number of pending sequences before skipping sequences
	CacheSkippedSeqMaxWait *uint32 `json:"max_wait_skipped,omitempty"` // Maximum wait time in milliseconds for a skipped sequence before abandoning the sequence
	EnableStarChannel      *bool   `json:"enable_star_channel"`        // Enable the star (*) channel
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

	return err
}

// Implementation of AuthHandler interface for DbConfig
func (dbConfig *DbConfig) GetCredentials() (string, string, string) {
	return dbConfig.Username, dbConfig.Password, *dbConfig.Bucket
}

// Implementation of AuthHandler interface for ShadowConfig
func (shadowConfig *ShadowConfig) GetCredentials() (string, string, string) {
	return shadowConfig.Username, shadowConfig.Password, shadowConfig.Bucket
}

// Reads a ServerConfig from raw data
func ReadServerConfigFromData(data []byte) (*ServerConfig, error) {

	data = base.ConvertBackQuotedStrings(data)
	var config *ServerConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	// Validation:
	for name, dbConfig := range config.Databases {
		dbConfig.setup(name)
	}
	return config, nil
}

// Reads a ServerConfig from a URL.
func ReadServerConfigFromUrl(url string) (*ServerConfig, error) {

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	responseBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return ReadServerConfigFromData(responseBody)

}

// Reads a ServerConfig from either a JSON file or from a URL.
func ReadServerConfig(path string) (*ServerConfig, error) {
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return ReadServerConfigFromUrl(path)
	} else {
		return ReadServerConfigFromFile(path)
	}
}

// Reads a ServerConfig from a JSON file.
func ReadServerConfigFromFile(path string) (*ServerConfig, error) {

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

	// Validation:
	for name, dbConfig := range config.Databases {
		dbConfig.setup(name)
	}
	return config, nil
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
	if self.Persona == nil {
		self.Persona = other.Persona
	}
	if self.Facebook == nil {
		self.Facebook = other.Facebook
	}
	if self.CORS == nil {
		self.CORS = other.CORS
	}
	for _, flag := range other.Log {
		self.Log = append(self.Log, flag)
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
func ParseCommandLine() {

	siteURL := flag.String("personaOrigin", "", "URL that clients use to communicate with a Mozilla Persona IdP server")
	addr := flag.String("interface", DefaultInterface, "Port that the Sync REST API listens on")
	authAddr := flag.String("adminInterface", DefaultAdminInterface, "HTTP address (IP address for localhost and the port), or only the port, that the Admin REST API listens on")
	profAddr := flag.String("profileInterface", "", "Port for a diagnostic interface, from which profiling information can be obtained")
	configServer := flag.String("configServer", "", "URL of server that can return database configs")
	deploymentID := flag.String("deploymentID", "", "Customer/project identifier for stats reporting")
	couchbaseURL := flag.String("url", DefaultServer, "URL of the database server. An HTTP URL implies Couchbase Server. A walrus: URL implies the built-in Walrus database. A combination of a Walrus URL and a file-style URI implies the built-in Walrus database and persisting the database to a file.")
	poolName := flag.String("pool", DefaultPool, "Name of the Couchbase Server pool in which to find buckets")
	bucketName := flag.String("bucket", "sync_gateway", "Name of the Couchbase bucket")
	dbName := flag.String("dbname", "", "Name of the Couchbase Server database to serve through the Sync REST API (defaults to the bucket name)")
	pretty := flag.Bool("pretty", false, "Pretty-print JSON responses to improve readability. This is useful for debugging, but reduces performance.")
	verbose := flag.Bool("verbose", false, "Log more info about requests")
	logKeys := flag.String("log", "", "Comma-separated list of log keywords to enable. The log keyword HTTP is always enabled, which means that HTTP requests and error responses are always logged.")
	logFilePath := flag.String("logFilePath", "", "Path to the log file")
	flag.Parse()

	if flag.NArg() > 0 {
		// Read the configuration file(s), if any:
		for i := 0; i < flag.NArg(); i++ {
			filename := flag.Arg(i)
			c, err := ReadServerConfig(filename)
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
		if config.Log != nil {
			base.ParseLogFlags(config.Log)
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
			config.LogFilePath = logFilePath
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
					Name:   *dbName,
					Server: couchbaseURL,
					Bucket: bucketName,
					Pool:   poolName,
					Users: map[string]*db.PrincipalConfig{
						base.GuestUsername: &db.PrincipalConfig{
							Disabled:         false,
							ExplicitChannels: base.SetFromArray([]string{"*"}),
						},
					},
				},
			},
		}
	}

	if *siteURL != "" {
		if config.Persona == nil {
			config.Persona = new(PersonaConfig)
		}
		config.Persona.Origin = *siteURL
	}

	base.LogKeys["HTTP"] = true
	if *verbose {
		base.LogKeys["HTTP+"] = true
	}
	base.ParseLogFlag(*logKeys)

	//return config
}

func setMaxFileDescriptors(maxP *uint64) {
	maxFDs := DefaultMaxFileDescriptors
	if maxP != nil {
		maxFDs = *maxP
	}
	_ , err := base.SetMaxFileDescriptors(maxFDs)
	if err != nil {
		base.Warn("Error setting MaxFileDescriptors to %d: %v", maxFDs, err)
	}
}

func (config *ServerConfig) serve(addr string, handler http.Handler) {
	maxConns := DefaultMaxIncomingConnections
	if config.MaxIncomingConnections != nil {
		maxConns = *config.MaxIncomingConnections
	}

	err := base.ListenAndServeHTTP(addr, maxConns, config.SSLCert, config.SSLKey, handler, config.ServerReadTimeout, config.ServerWriteTimeout)
	if err != nil {
		base.LogFatal("Failed to start HTTP server on %s: %v", addr, err)
	}
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

	setMaxFileDescriptors(config.MaxFileDescriptors)

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
	go config.serve(*config.AdminInterface, CreateAdminHandler(sc))
	base.Logf("Starting server on %s ...", *config.Interface)
	config.serve(*config.Interface, CreatePublicHandler(sc))
}

// for now  just cycle the logger to allow for log file rotation
func ReloadConf() {
	if config.LogFilePath != nil {
		base.UpdateLogger(*config.LogFilePath)
	}
}

// Main entry point for a simple server; you can have your main() function just call this.
// It parses command-line flags, reads the optional configuration file, then starts the server.
func ServerMain() {
	ParseCommandLine()
	ReloadConf()
	RunServer(config)
}
