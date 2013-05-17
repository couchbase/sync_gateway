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
	"os"
	"regexp"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/couchbaselabs/sync_gateway/db"
)

var DefaultInterface = ":4984"
var DefaultAdminInterface = ":4985"
var DefaultServer = "http://localhost:8091"
var DefaultPool = "default"

// JSON object that defines the server configuration.
type ServerConfig struct {
	Interface      *string // Interface to bind REST API to, default ":4984"
	AdminInterface *string // Interface to bind admin API to, default ":4985"
	Persona        *PersonaConfig
	Log            []string // Log keywords to enable
	Pretty         bool     // Pretty-print JSON responses?
	Databases      map[string]*DbConfig
}

// JSON object that defines a database configuration within the ServerConfig.
type DbConfig struct {
	name   string                     // Database name in REST API (stored as key in JSON)
	Server *string                    // Couchbase (or Walrus) server URL, default "http://localhost:8091"
	Bucket *string                    // Bucket name on server; defaults to same as 'name'
	Pool   *string                    // Couchbase pool name, default "default"
	Sync   *string                    // Sync function defines which users can see which data
	Users  map[string]json.RawMessage // Initial user accounts (values same schema as admin REST API)
	Roles  map[string]json.RawMessage // Initial roles (values same schema as admin REST API)
}

type PersonaConfig struct {
	Origin string // Canonical server URL for Persona authentication
}

// Shared context of HTTP handlers. It's important that this remain immutable, because the
// handlers will access it from multiple goroutines.
type serverContext struct {
	config    *ServerConfig
	databases map[string]*context
}

// Reads a ServerConfig from a JSON file.
func ReadConfig(path string) (*ServerConfig, error) {
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
	if self.Persona == nil {
		self.Persona = other.Persona
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

func newServerContext(config *ServerConfig) *serverContext {
	return &serverContext{
		config:    config,
		databases: map[string]*context{},
	}
}

// Adds a database to the serverContext given its Bucket.
func (sc *serverContext) addDatabase(bucket base.Bucket, dbName string, syncFun *string, nag bool) (*context, error) {
	if dbName == "" {
		dbName = bucket.GetName()
	}

	if match, _ := regexp.MatchString(`^[a-z][-a-z0-9_$()+/]*$`, dbName); !match {
		return nil, fmt.Errorf("Illegal database name: %s", dbName)
	}

	if sc.databases[dbName] != nil {
		return nil, fmt.Errorf("Duplicate database name %q", dbName)
	}

	dbcontext, err := db.NewDatabaseContext(dbName, bucket)
	if err != nil {
		return nil, err
	}
	if syncFun != nil {
		if err := dbcontext.ApplySyncFun(*syncFun); err != nil {
			return nil, err
		}
	}

	if dbcontext.ChannelMapper == nil {
		if nag {
			base.Warn("Sync function undefined; using default")
		}
		// Always have a channel mapper object even if it does nothing:
		dbcontext.ChannelMapper, _ = channels.NewDefaultChannelMapper()
	}

	c := &context{
		dbcontext: dbcontext,
		auth:      dbcontext.Authenticator(),
	}

	sc.databases[dbName] = c
	return c, nil
}

// Adds a database to the serverContext given its configuration.
func (sc *serverContext) addDatabaseFromConfig(config *DbConfig) error {
	server := "http://localhost:8091"
	pool := "default"
	bucketName := config.name

	if config.Server != nil {
		server = *config.Server
	}
	if config.Pool != nil {
		pool = *config.Pool
	}
	if config.Bucket != nil {
		bucketName = *config.Bucket
	}

	// Connect to the bucket and add the database:
	bucket, err := db.ConnectToBucket(server, pool, bucketName)
	if err != nil {
		return err
	}
	context, err := sc.addDatabase(bucket, config.name, config.Sync, true)
	if err != nil {
		return err
	}

	// Create default users & roles:
	if err := sc.installPrincipals(context, config.Roles, "role"); err != nil {
		return nil
	}
	return sc.installPrincipals(context, config.Users, "user")
}

func (sc *serverContext) installPrincipals(context *context, spec map[string]json.RawMessage, what string) error {
	for name, data := range spec {
		isUsers := (what == "user")
		if name == "GUEST" && isUsers {
			name = ""
		}
		newPrincipal, err := context.auth.UnmarshalPrincipal(data, name, 1, isUsers)
		if err != nil {
			return fmt.Errorf("Invalid config for %s %q: %v", what, name, err)
		}
		oldPrincipal, err := context.auth.GetPrincipal(newPrincipal.Name(), isUsers)
		if oldPrincipal == nil || name == "" {
			if err == nil {
				err = context.auth.Save(newPrincipal)
			}
			if err != nil {
				return fmt.Errorf("Couldn't create %s %q: %v", what, name, err)
			} else if name == "" {
				base.Log("Reset guest user to config")
			} else {
				base.Log("Created %s %q", what, name)
			}
		}
	}
	return nil
}

// Reads the command line flags and the optional config file.
func ParseCommandLine() *ServerConfig {
	siteURL := flag.String("personaOrigin", "", "Base URL that clients use to connect to the server")
	addr := flag.String("interface", DefaultInterface, "Address to bind to")
	authAddr := flag.String("adminInterface", DefaultAdminInterface, "Address to bind admin interface to")
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
			c, err := ReadConfig(filename)
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
		if *pretty {
			config.Pretty = *pretty
		}
		if config.Log != nil {
			base.ParseLogFlags(config.Log)
		}

		if len(config.Databases) == 0 {
			base.LogFatal("No databases!")
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
			Interface:      addr,
			AdminInterface: authAddr,
			Pretty:         *pretty,
			Databases: map[string]*DbConfig{
				*dbName: {
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
	base.LogKeys["HTTP+"] = *verbose
	base.ParseLogFlag(*logKeys)

	return config
}

// Starts and runs the server given its configuration. (This function never returns.)
func RunServer(config *ServerConfig) {
	PrettyPrint = config.Pretty

	sc := newServerContext(config)
	for _, dbConfig := range config.Databases {
		if err := sc.addDatabaseFromConfig(dbConfig); err != nil {
			base.LogFatal("Error opening database: %v", err)
		}
	}

	base.Log("Starting auth server on %s", *config.AdminInterface)
	StartAuthListener(*config.AdminInterface, sc)

	base.Log("Starting server on %s ...", *config.Interface)
	if err := http.ListenAndServe(*config.Interface, createHandler(sc)); err != nil {
		base.LogFatal("Server failed: ", err.Error())
	}
}

// Main entry point for a simple server; you can have your main() function just call this.
// It parses command-line flags, reads the optional configuration file, then starts the server.
func ServerMain() {
	RunServer(ParseCommandLine())
}
