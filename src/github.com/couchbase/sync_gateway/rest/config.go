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

// JSON object that defines the server configuration.
type ServerConfig struct {
	Interface                      *string         // Interface to bind REST API to, default ":4984"
	SSLCert                        *string         // Path to SSL cert file, or nil
	SSLKey                         *string         // Path to SSL private key file, or nil
	ServerReadTimeout              *int            // maximum duration.Second before timing out read of the HTTP(S) request
	ServerWriteTimeout             *int            // maximum duration.Second before timing out write of the HTTP(S) response
	AdminInterface                 *string         // Interface to bind admin API to, default ":4985"
	AdminUI                        *string         // Path to Admin HTML page, if omitted uses bundled HTML
	ProfileInterface               *string         // Interface to bind Go profile API to (no default)
	ConfigServer                   *string         // URL of config server (for dynamic db discovery)
	Persona                        *PersonaConfig  // Configuration for Mozilla Persona validation
	Facebook                       *FacebookConfig // Configuration for Facebook validation
	CORS                           *CORSConfig     // Configuration for allowing CORS
	Log                            []string        // Log keywords to enable
	LogFilePath                    *string         // Path to log file, if missing write to stderr
	Pretty                         bool            // Pretty-print JSON responses?
	DeploymentID                   *string         // Optional customer/deployment ID for stats reporting
	StatsReportInterval            *float64        // Optional stats report interval (0 to disable)
	MaxCouchbaseConnections        *int            // Max # of sockets to open to a Couchbase Server node
	MaxCouchbaseOverflow           *int            // Max # of overflow sockets to open
	CouchbaseKeepaliveInterval     *int            // TCP keep-alive interval between SG and Couchbase server
	SlowServerCallWarningThreshold *int            // Log warnings if database calls take this many ms
	MaxIncomingConnections         *int            // Max # of incoming HTTP connections to accept
	MaxFileDescriptors             *uint64         // Max # of open file descriptors (RLIMIT_NOFILE)
	CompressResponses              *bool           // If false, disables compression of HTTP responses
	Databases                      DbConfigMap     // Pre-configured databases, mapped by name
}

// JSON object that defines a database configuration within the ServerConfig.
type DbConfig struct {
	Name               string                         `json:"name"`                           // Database name in REST API (stored as key in JSON)
	Server             *string                        `json:"server"`                         // Couchbase (or Walrus) server URL, default "http://localhost:8091"
	Username           string                         `json:"username,omitempty"`             // Username for authenticating to server
	Password           string                         `json:"password,omitempty"`             // Password for authenticating to server
	Bucket             *string                        `json:"bucket"`                         // Bucket name on server; defaults to same as 'name'
	Pool               *string                        `json:"pool"`                           // Couchbase pool name, default "default"
	Sync               *string                        `json:"sync"`                           // Sync function defines which users can see which data
	Users              map[string]*db.PrincipalConfig `json:"users,omitempty"`                // Initial user accounts
	Roles              map[string]*db.PrincipalConfig `json:"roles,omitempty"`                // Initial roles
	RevsLimit          *uint32                        `json:"revs_limit,omitempty"`           // Max depth a document's revision tree can grow to
	ImportDocs         interface{}                    `json:"import_docs,omitempty"`          // false, true, or "continuous"
	Shadow             *ShadowConfig                  `json:"shadow,omitempty"`               // External bucket to shadow
	EventHandlers      interface{}                    `json:"event_handlers,omitempty"`       // Event handlers (webhook)
	FeedType           string                         `json:"feed_type,omitempty"`            // Feed type - "DCP" or "TAP"; defaults based on Couchbase server version
	AllowEmptyPassword bool                           `json:"allow_empty_password,omitempty"` // Allow empty passwords?  Defaults to false
	CacheConfig        *CacheConfig                   `json:"cache,omitempty"`                // Cache settings
}

type DbConfigMap map[string]*DbConfig

type PersonaConfig struct {
	Origin   string // Canonical server URL for Persona authentication
	Register bool   // If true, server will register new user accounts
}

type FacebookConfig struct {
	Register bool // If true, server will register new user accounts
}

type CORSConfig struct {
	Origin      []string // List of allowed origins, use ["*"] to allow access from everywhere
	LoginOrigin []string // List of allowed login origins
	Headers     []string // List of allowed headers
	MaxAge      int      // Maximum age of the CORS Options request
}

type ShadowConfig struct {
	Server       *string `json:"server"`                 // Couchbase server URL
	Pool         *string `json:"pool,omitempty"`         // Couchbase pool name, default "default"
	Bucket       string  `json:"bucket"`                 // Bucket name
	Username     string  `json:"username,omitempty"`     // Username for authenticating to server
	Password     string  `json:"password,omitempty"`     // Password for authenticating to server
	Doc_id_regex *string `json:"doc_id_regex,omitempty"` // Optional regex that doc IDs must match
	FeedType     string  `json:"feed_type,omitempty"`    // Feed type - "DCP" or "TAP"; defaults to TAP
}

type EventHandlerConfig struct {
	MaxEventProc    uint           `json:"max_processes,omitempty"`    // Max concurrent event handling goroutines
	WaitForProcess  string         `json:"wait_for_process,omitempty"` // Max wait time when event queue is full (ms)
	DocumentChanged []*EventConfig `json:"document_changed,omitempty"` // Document Commit
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

	siteURL := flag.String("personaOrigin", "", "Base URL that clients use to connect to the server")
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
	actualMax, err := base.SetMaxFileDescriptors(maxFDs)
	if err != nil {
		base.Warn("Error setting MaxFileDescriptors to %d: %v", maxFDs, err)
	} else if maxP != nil {
		base.Logf("Configured process to allow %d open file descriptors", actualMax)
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
