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

	"github.com/couchbaselabs/sync_gateway/base"
)

// Register profiling handlers (see Go docs)
import _ "net/http/pprof"

var DefaultInterface = ":4984"
var DefaultAdminInterface = "127.0.0.1:4985" // Only accessible on localhost!
var DefaultServer = "walrus:"
var DefaultPool = "default"

const DefaultMaxCouchbaseConnections = 16
const DefaultMaxCouchbaseOverflowConnections = 0

// Default value of ServerConfig.MaxIncomingConnections
const DefaultMaxIncomingConnections = 0

// Default value of ServerConfig.MaxFileDescriptors
const DefaultMaxFileDescriptors uint64 = 5000

// JSON object that defines the server configuration.
type ServerConfig struct {
	Interface                      *string         // Interface to bind REST API to, default ":4984"
	SSLCert                        *string         // Path to SSL cert file, or nil
	SSLKey                         *string         // Path to SSL private key file, or nil
	AdminInterface                 *string         // Interface to bind admin API to, default ":4985"
	AdminUI                        *string         // Path to Admin HTML page, if omitted uses bundled HTML
	ProfileInterface               *string         // Interface to bind Go profile API to (no default)
	ConfigServer                   *string         // URL of config server (for dynamic db discovery)
	Persona                        *PersonaConfig  // Configuration for Mozilla Persona validation
	Facebook                       *FacebookConfig // Configuration for Facebook validation
	Log                            []string        // Log keywords to enable
	Pretty                         bool            // Pretty-print JSON responses?
	DeploymentID                   *string         // Optional customer/deployment ID for stats reporting
	StatsReportInterval            *float64        // Optional stats report interval (0 to disable)
	MaxCouchbaseConnections        *int            // Max # of sockets to open to a Couchbase Server node
	MaxCouchbaseOverflow           *int            // Max # of overflow sockets to open
	SlowServerCallWarningThreshold *int            // Log warnings if database calls take this many ms
	MaxIncomingConnections         *int            // Max # of incoming HTTP connections to accept
	MaxFileDescriptors             *uint64         // Max # of open file descriptors (RLIMIT_NOFILE)
	CompressResponses              *bool           // If false, disables compression of HTTP responses
	Databases                      DbConfigMap     // Pre-configured databases, mapped by name
}

// JSON object that defines a database configuration within the ServerConfig.
type DbConfig struct {
	name       string                      `json:"name"`                  // Database name in REST API (stored as key in JSON)
	Server     *string                     `json:"server"`                // Couchbase (or Walrus) server URL, default "http://localhost:8091"
	Username   string                      `json:"username,omitempty"`    // Username for authenticating to server
	Password   string                      `json:"password,omitempty"`    // Password for authenticating to server
	Bucket     *string                     `json:"bucket"`                // Bucket name on server; defaults to same as 'name'
	Pool       *string                     `json:"pool"`                  // Couchbase pool name, default "default"
	Sync       *string                     `json:"sync"`                  // Sync function defines which users can see which data
	Users      map[string]*PrincipalConfig `json:"users,omitempty"`       // Initial user accounts
	Roles      map[string]*PrincipalConfig `json:"roles,omitempty"`       // Initial roles
	RevsLimit  *uint32                     `json:"revs_limit,omitempty"`  // Max depth a document's revision tree can grow to
	ImportDocs interface{}                 `json:"import_docs,omitempty"` // false, true, or "continuous"
	Shadow     *ShadowConfig               `json:"shadow,omitempty"`      // External bucket to shadow
}

type DbConfigMap map[string]*DbConfig

// JSON object that defines a User/Role within a DbConfig. (Also used in admin REST API.)
type PrincipalConfig struct {
	Name              *string  `json:"name,omitempty"`
	ExplicitChannels  base.Set `json:"admin_channels,omitempty"`
	Channels          base.Set `json:"all_channels"`
	Email             string   `json:"email,omitempty"`
	Disabled          bool     `json:"disabled,omitempty"`
	Password          *string  `json:"password,omitempty"`
	ExplicitRoleNames []string `json:"admin_roles,omitempty"`
	RoleNames         []string `json:"roles,omitempty"`
}

type PersonaConfig struct {
	Origin   string // Canonical server URL for Persona authentication
	Register bool   // If true, server will register new user accounts
}

type FacebookConfig struct {
	Register bool // If true, server will register new user accounts
}

type ShadowConfig struct {
	Server       string  `json:"server"`                 // Couchbase server URL
	Pool         *string `json:"pool,omitempty"`         // Couchbase pool name, default "default"
	Bucket       string  `json:"bucket"`                 // Bucket name
	Username     string  `json:"username,omitempty"`     // Username for authenticating to server
	Password     string  `json:"password,omitempty"`     // Password for authenticating to server
	Doc_id_regex *string `json:"doc_id_regex,omitempty"` // Optional regex that doc IDs must match
}

func (dbConfig *DbConfig) setup(name string) error {
	dbConfig.name = name
	if dbConfig.Bucket == nil {
		dbConfig.Bucket = &dbConfig.name
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

// Implementation of AuthHandler interface for DbConfig
func (dbConfig *DbConfig) GetCredentials() (string, string) {
	return dbConfig.Username, dbConfig.Password
}

// Implementation of AuthHandler interface for ShadowConfig
func (shadowConfig *ShadowConfig) GetCredentials() (string, string) {
	return shadowConfig.Username, shadowConfig.Password
}

// Reads a ServerConfig from a JSON file.
func ReadServerConfig(path string) (*ServerConfig, error) {
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
func ParseCommandLine() *ServerConfig {
	siteURL := flag.String("personaOrigin", "", "Base URL that clients use to connect to the server")
	addr := flag.String("interface", DefaultInterface, "Address to bind to")
	authAddr := flag.String("adminInterface", DefaultAdminInterface, "Address to bind admin interface to")
	profAddr := flag.String("profileInterface", "", "Address to bind profile interface to")
	configServer := flag.String("configServer", "", "URL of server that can return database configs")
	deploymentID := flag.String("deploymentID", "", "Customer/project identifier for stats reporting")
	couchbaseURL := flag.String("url", DefaultServer, "Address of Couchbase server")
	poolName := flag.String("pool", DefaultPool, "Name of pool")
	bucketName := flag.String("bucket", "sync_gateway", "Name of bucket")
	dbName := flag.String("dbname", "", "Name of CouchDB database (defaults to name of bucket)")
	pretty := flag.Bool("pretty", false, "Pretty-print JSON responses")
	verbose := flag.Bool("verbose", false, "Log more info about requests")
	logKeys := flag.String("log", "", "Log keywords, comma separated")
	flag.Parse()

	var config *ServerConfig

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
		if config.Interface == nil {
			config.Interface = &DefaultInterface
		}
		if config.AdminInterface == nil {
			config.AdminInterface = &DefaultAdminInterface
		}

	} else {
		// If no config file is given, create a default config, filled in from command line flags:
		if *dbName == "" {
			*dbName = *bucketName
		}
		config = &ServerConfig{
			Interface:        addr,
			AdminInterface:   authAddr,
			ProfileInterface: profAddr,
			Pretty:           *pretty,
			Databases: map[string]*DbConfig{
				*dbName: {
					name:   *dbName,
					Server: couchbaseURL,
					Bucket: bucketName,
					Pool:   poolName,
				},
			},
		}
	}

	if *siteURL != "" {
		config.Persona = &PersonaConfig{Origin: *siteURL}
	}

	base.LogKeys["HTTP"] = true
	if *verbose {
		base.LogKeys["HTTP+"] = true
	}
	base.ParseLogFlag(*logKeys)

	return config
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
		base.Log("Configured process to allow %d open file descriptors", actualMax)
	}
}

func (config *ServerConfig) serve(addr string, handler http.Handler) {
	maxConns := DefaultMaxIncomingConnections
	if config.MaxIncomingConnections != nil {
		maxConns = *config.MaxIncomingConnections
	}
	err := base.ListenAndServeHTTP(addr, maxConns, config.SSLCert, config.SSLKey, handler)
	if err != nil {
		base.LogFatal("Failed to start HTTP server on %s: %v", addr, err)
	}
}

// Starts and runs the server given its configuration. (This function never returns.)
func RunServer(config *ServerConfig) {
	PrettyPrint = config.Pretty

	base.Log("==== %s ====", LongVersionString)

	if os.Getenv("GOMAXPROCS") == "" && runtime.GOMAXPROCS(0) == 1 {
		cpus := runtime.NumCPU()
		if cpus > 1 {
			runtime.GOMAXPROCS(cpus)
			base.Log("Configured Go to use all %d CPUs; setenv GOMAXPROCS to override this", cpus)
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
		base.Log("Starting profile server on %s", *config.ProfileInterface)
		go func() {
			http.ListenAndServe(*config.ProfileInterface, nil)
		}()
	}

	base.Log("Starting admin server on %s", *config.AdminInterface)
	go config.serve(*config.AdminInterface, CreateAdminHandler(sc))
	base.Log("Starting server on %s ...", *config.Interface)
	config.serve(*config.Interface, CreatePublicHandler(sc))
}

// Main entry point for a simple server; you can have your main() function just call this.
// It parses command-line flags, reads the optional configuration file, then starts the server.
func ServerMain() {
	RunServer(ParseCommandLine())
}
