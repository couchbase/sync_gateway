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
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/couchbaselabs/go-couchbase"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/db"
)

// The URL that stats will be reported to if deployment_id is set in the config
const kStatsReportURL = "http://localhost:9999/stats"
const kStatsReportInterval = time.Hour
const kDefaultSlowServerCallWarningThreshold = 200 // ms

// Shared context of HTTP handlers: primarily a registry of databases by name. It also stores
// the configuration settings so handlers can refer to them.
// This struct is accessed from HTTP handlers running on multiple goroutines, so it needs to
// be thread-safe.
type ServerContext struct {
	config      *ServerConfig
	databases_  map[string]*db.DatabaseContext
	lock        sync.RWMutex
	statsTicker *time.Ticker
	HTTPClient  *http.Client
}

func NewServerContext(config *ServerConfig) *ServerContext {
	sc := &ServerContext{
		config:     config,
		databases_: map[string]*db.DatabaseContext{},
		HTTPClient: http.DefaultClient,
	}
	if config.Databases == nil {
		config.Databases = DbConfigMap{}
	}

	// Initialize the go-couchbase library's global configuration variables:
	couchbase.PoolSize = DefaultMaxCouchbaseConnections
	couchbase.PoolOverflow = DefaultMaxCouchbaseOverflowConnections
	if config.MaxCouchbaseConnections != nil {
		couchbase.PoolSize = *config.MaxCouchbaseConnections
	}
	if config.MaxCouchbaseOverflow != nil {
		couchbase.PoolOverflow = *config.MaxCouchbaseOverflow
	}

	slow := kDefaultSlowServerCallWarningThreshold
	if config.SlowServerCallWarningThreshold != nil {
		slow = *config.SlowServerCallWarningThreshold
	}
	couchbase.SlowServerCallWarningThreshold = time.Duration(slow) * time.Millisecond

	if config.DeploymentID != nil {
		sc.startStatsReporter()
	}
	return sc
}

func (sc *ServerContext) Close() {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	sc.stopStatsReporter()
	for _, ctx := range sc.databases_ {
		ctx.Close()
	}
	sc.databases_ = nil
}

// Returns the DatabaseContext with the given name
func (sc *ServerContext) GetDatabase(name string) (*db.DatabaseContext, error) {
	sc.lock.RLock()
	dbc := sc.databases_[name]
	sc.lock.RUnlock()
	if dbc != nil {
		return dbc, nil
	} else if db.ValidateDatabaseName(name) != nil {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "invalid database name %q", name)
	} else if sc.config.ConfigServer == nil {
		return nil, base.HTTPErrorf(http.StatusNotFound, "no such database %q", name)
	} else {
		// Let's ask the config server if it knows this database:
		base.Log("Asking config server %q about db %q...", *sc.config.ConfigServer, name)
		config, err := sc.getDbConfigFromServer(name)
		if err != nil {
			return nil, err
		}
		if dbc, err = sc.AddDatabaseFromConfig(config); err != nil {
			return nil, err
		}
		return dbc, nil
	}
}

func (sc *ServerContext) GetDatabaseConfig(name string) *DbConfig {
	sc.lock.RLock()
	config := sc.config.Databases[name]
	sc.lock.RUnlock()
	return config
}

func (sc *ServerContext) setDatabaseConfig(name string, config *DbConfig) {
	sc.lock.Lock()
	sc.config.Databases[name] = config
	sc.lock.Unlock()
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
		return base.HTTPErrorf(http.StatusPreconditionFailed, // what CouchDB returns
			"Duplicate database name %q", name)
	}
	sc.databases_[name] = dbcontext
	return nil
}

// Adds a database to the ServerContext given its configuration.
func (sc *ServerContext) AddDatabaseFromConfig(config *DbConfig) (*db.DatabaseContext, error) {
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
		return nil, err
	}

	var importDocs, autoImport bool
	switch config.ImportDocs {
	case nil, false:
	case true:
		importDocs = true
	case "continuous":
		importDocs = true
		autoImport = true
	default:
		return nil, fmt.Errorf("Unrecognized value for ImportDocs: %#v", config.ImportDocs)
	}

	// Connect to the bucket and add the database:
	spec := base.BucketSpec{
		Server:     server,
		PoolName:   pool,
		BucketName: bucketName,
	}
	if config.Username != "" {
		spec.Auth = config
	}
	bucket, err := db.ConnectToBucket(spec)
	if err != nil {
		return nil, err
	}
	dbcontext, err := db.NewDatabaseContext(dbName, bucket, autoImport)
	if err != nil {
		return nil, err
	}

	syncFn := ""
	if config.Sync != nil {
		syncFn = *config.Sync
	}
	if err := dbcontext.ApplySyncFun(syncFn, importDocs); err != nil {
		return nil, err
	}

	if config.RevsLimit != nil && *config.RevsLimit > 0 {
		dbcontext.RevsLimit = *config.RevsLimit
	}

	if dbcontext.ChannelMapper == nil {
		base.Log("Using default sync function 'channel(doc.channels)' for database %q", dbName)
	}

	// Create default users & roles:
	if err := sc.installPrincipals(dbcontext, config.Roles, "role"); err != nil {
		return nil, err
	} else if err := sc.installPrincipals(dbcontext, config.Users, "user"); err != nil {
		return nil, err
	}

	// Install bucket-shadower if any:
	if shadow := config.Shadow; shadow != nil {
		if err := sc.startShadowing(dbcontext, shadow); err != nil {
			base.Warn("Database %q: unable to connect to external bucket for shadowing: %v",
				dbName, err)
		}
	}

	// Register it so HTTP handlers can find it:
	if err := sc.registerDatabase(dbcontext); err != nil {
		dbcontext.Close()
		return nil, err
	}
	sc.setDatabaseConfig(config.name, config)
	return dbcontext, nil
}

func (sc *ServerContext) startShadowing(dbcontext *db.DatabaseContext, shadow *ShadowConfig) error {
	var pattern *regexp.Regexp
	if shadow.Doc_id_regex != nil {
		var err error
		pattern, err = regexp.Compile(*shadow.Doc_id_regex)
		if err != nil {
			base.Warn("Invalid shadow doc_id_regex: %s", *shadow.Doc_id_regex)
			return err
		}
	}

	spec := base.BucketSpec{
		Server:     shadow.Server,
		PoolName:   "default",
		BucketName: shadow.Bucket,
	}
	if shadow.Pool != nil {
		spec.PoolName = *shadow.Pool
	}
	if shadow.Username != "" {
		spec.Auth = shadow
	}

	bucket, err := db.ConnectToBucket(spec)
	if err != nil {
		return err
	}
	shadower, err := db.NewShadower(dbcontext, bucket, pattern)
	if err != nil {
		bucket.Close()
		return err
	}
	dbcontext.Shadower = shadower
	base.Log("Database %q shadowing remote bucket %q, pool %q, server <%s>", dbcontext.Name, spec.BucketName, spec.PoolName, spec.Server)
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

func (sc *ServerContext) installPrincipals(context *db.DatabaseContext, spec map[string]*PrincipalConfig, what string) error {
	for name, princ := range spec {
		princ.Name = &name
		_, err := updatePrincipal(context, *princ, (what == "user"), (name == "GUEST"))
		if err != nil {
			// A conflict error just means updatePrincipal didn't overwrite an existing user.
			if status, _ := base.ErrorAsHTTPStatus(err); status != http.StatusConflict {
				return fmt.Errorf("Couldn't create %s %q: %v", what, name, err)
			}
		} else if name == "GUEST" {
			base.Log("    Reset guest user to config")
		} else {
			base.Log("    Created %s %q", what, name)
		}
	}
	return nil
}

// Fetch a configuration for a database from the ConfigServer
func (sc *ServerContext) getDbConfigFromServer(dbName string) (*DbConfig, error) {
	if sc.config.ConfigServer == nil {
		return nil, base.HTTPErrorf(http.StatusNotFound, "not_found")
	}

	urlStr := *sc.config.ConfigServer
	if !strings.HasSuffix(urlStr, "/") {
		urlStr += "/"
	}
	urlStr += url.QueryEscape(dbName)
	res, err := sc.HTTPClient.Get(urlStr)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway,
			"Error contacting config server: %v", err)
	} else if res.StatusCode >= 300 {
		return nil, base.HTTPErrorf(res.StatusCode, res.Status)
	}

	var config DbConfig
	j := json.NewDecoder(res.Body)
	if err = j.Decode(&config); err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway,
			"Bad response from config server: %v", err)
	}

	if err = config.setup(dbName); err != nil {
		return nil, err
	}
	return &config, nil
}

//////// STATISTICS REPORT:

func (sc *ServerContext) startStatsReporter() {
	interval := kStatsReportInterval
	if sc.config.StatsReportInterval != nil {
		if *sc.config.StatsReportInterval <= 0 {
			return
		}
		interval = time.Duration(*sc.config.StatsReportInterval) * time.Second
	}
	sc.statsTicker = time.NewTicker(interval)
	go func() {
		for _ = range sc.statsTicker.C {
			sc.reportStats()
		}
	}()
	base.Log("Will report server stats for %q every %v",
		*sc.config.DeploymentID, interval)
}

func (sc *ServerContext) stopStatsReporter() {
	if sc.statsTicker != nil {
		sc.statsTicker.Stop()
		sc.reportStats() // Report stuff since the last tick
	}
}

// POST a report of database statistics
func (sc *ServerContext) reportStats() {
	if sc.config.DeploymentID == nil {
		panic("Can't reportStats without DeploymentID")
	}
	stats := sc.Stats()
	if stats == nil {
		return // No activity
	}
	base.Log("Reporting server stats to %s ...", kStatsReportURL)
	body, _ := json.Marshal(stats)
	bodyReader := bytes.NewReader(body)
	_, err := sc.HTTPClient.Post(kStatsReportURL, "application/json", bodyReader)
	if err != nil {
		base.Warn("Error posting stats: %v", err)
	}
}

func (sc *ServerContext) Stats() map[string]interface{} {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	var stats []map[string]interface{}
	any := false
	for _, dbc := range sc.databases_ {
		max := dbc.ChangesClientStats.MaxCount()
		total := dbc.ChangesClientStats.TotalCount()
		dbc.ChangesClientStats.Reset()
		stats = append(stats, map[string]interface{}{
			"max_connections":   max,
			"total_connections": total,
		})
		any = any || total > 0
	}
	if !any {
		return nil
	}
	return map[string]interface{}{
		"deploymentID": *sc.config.DeploymentID,
		"databases":    stats,
	}
}
