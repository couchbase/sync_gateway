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
	"fmt"
	"net/http"
	"sync"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/db"
)

// Shared context of HTTP handlers: primarily a registry of databases by name. It also stores
// the configuration settings so handlers can refer to them.
// This struct is accessed from HTTP handlers running on multiple goroutines, so it needs to
// be thread-safe.
type ServerContext struct {
	config     *ServerConfig
	databases_ map[string]*db.DatabaseContext
	lock       sync.RWMutex
}

func NewServerContext(config *ServerConfig) *ServerContext {
	return &ServerContext{
		config:     config,
		databases_: map[string]*db.DatabaseContext{},
	}
}

func (sc *ServerContext) Close() {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	for _, ctx := range sc.databases_ {
		ctx.Close()
	}
	sc.databases_ = nil
}

func (sc *ServerContext) Database(name string) *db.DatabaseContext {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	return sc.databases_[name]
}

func (sc *ServerContext) AllDatabaseNames() []string {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	names := make([]string, 0, len(sc.databases_))
	for name, _ := range sc.databases_ {
		names = append(names, name)
	}
	return names
}

func (sc *ServerContext) registerDatabase(dbcontext *db.DatabaseContext) error {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	name := dbcontext.Name
	if sc.databases_[name] != nil {
		return &base.HTTPError{http.StatusPreconditionFailed, // what CouchDB returns
			fmt.Sprintf("Duplicate database name %q", name)}
	}
	sc.databases_[name] = dbcontext
	return nil
}

// Adds a database to the ServerContext given its configuration.
func (sc *ServerContext) AddDatabaseFromConfig(config *DbConfig) error {
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
	dbName := config.name
	if dbName == "" {
		dbName = bucketName
	}
	base.Log("Opening db /%s as bucket %q, pool %q, server <%s>",
		dbName, bucketName, pool, server)

	if err := db.ValidateDatabaseName(dbName); err != nil {
		return err
	}

	// Connect to the bucket and add the database:
	bucket, err := db.ConnectToBucket(server, pool, bucketName)
	if err != nil {
		return err
	}
	dbcontext, err := db.NewDatabaseContext(dbName, bucket)
	if err != nil {
		return err
	}
	if config.Sync != nil {
		if err := dbcontext.ApplySyncFun(*config.Sync); err != nil {
			return err
		}
	}

	if dbcontext.ChannelMapper == nil {
		base.Warn("Database %q sync function undefined; using default", dbName)
	}

	// Create default users & roles:
	if err := sc.installPrincipals(dbcontext, config.Roles, "role"); err != nil {
		return err
	} else if err := sc.installPrincipals(dbcontext, config.Users, "user"); err != nil {
		return err
	}

	// Register it so HTTP handlers can find it:
	if err := sc.registerDatabase(dbcontext); err != nil {
		return err
	}
	return nil
}

func (sc *ServerContext) RemoveDatabase(dbName string) bool {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	context := sc.databases_[dbName]
	if context == nil {
		return false
	}
	base.Log("Closing db /%s (bucket %q)", context.Name, context.Bucket.GetName())
	context.Close()
	delete(sc.databases_, dbName)
	return true
}

func (sc *ServerContext) installPrincipals(context *db.DatabaseContext, spec map[string]json.RawMessage, what string) error {
	for name, data := range spec {
		isUsers := (what == "user")
		if name == "GUEST" && isUsers {
			name = ""
		}
		authenticator := context.Authenticator()
		newPrincipal, err := authenticator.UnmarshalPrincipal(data, name, 1, isUsers)
		if err != nil {
			return fmt.Errorf("Invalid config for %s %q: %v", what, name, err)
		}
		oldPrincipal, err := authenticator.GetPrincipal(newPrincipal.Name(), isUsers)
		if oldPrincipal == nil || name == "" {
			if err == nil {
				err = authenticator.Save(newPrincipal)
			}
			if err != nil {
				return fmt.Errorf("Couldn't create %s %q: %v", what, name, err)
			} else if name == "" {
				base.Log("    Reset guest user to config")
			} else {
				base.Log("    Created %s %q", what, name)
			}
		}
	}
	return nil
}
