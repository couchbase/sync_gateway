// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"crypto/x509"
	"sync"

	"github.com/couchbase/cbgt"
)

// cbgtGlobals holds the state for cbgt's global callbacks, which have no handle to a server or database context.
// Cannot use the serverContext to retrieve this information, as the cbgt manager for a database is initialized
// before the database is added to the server context's set of databases.
var cbgtGlobals = newCbgtGlobalData()

// CbgtDestFactoryFunc creates the cbgt.Dest that receives DCP events for a cbgt manager's pindexes.
type CbgtDestFactoryFunc = func(rollback func()) (cbgt.Dest, error)

// cbgtCreds are bucket specific credentials for connecting to Couchbase Server
type cbgtCreds struct {
	username       string         // Couchbase Server username, if using basic authentication
	password       string         // Couchbase Server password, if using basic authentication
	clientCertPath string         // Couchbase Server client certificate(public key), if using x509 authentication. If specified, username and password will be ignored.
	clientKeyPath  string         // Couchbase Server client certificate(private key), if using x509 authentication. If specified, username and password will be ignored.
	certPool       *x509.CertPool // If using TLS, the root certificates for verifying Couchbase Server. If TLSSkipVerify is set, this value should be nil.
	useTLS         bool           // If using couchbases:// this should be true.
}

// cbgtManagerData is the state registered for a single cbgt manager.
type cbgtManagerData struct {
	creds       cbgtCreds
	bucket      *GocbV2Bucket       // Bucket the cbgt manager streams from.
	dbName      string              // Name of the database that owns the cbgt manager.
	destKey     string              // destKey in the indexParams of the index this manager was started for.
	destFactory CbgtDestFactoryFunc // Creates the cbgt.Dest for pindexes of destKey.
}

// cbgtGlobalData is keyed by manager pointer rather than manager UUID, since the import and resync managers for a
// database share a UUID.
type cbgtGlobalData struct {
	lock     sync.Mutex
	managers map[*cbgt.Manager]cbgtManagerData
}

// newCbgtGlobalData returns an empty cbgtGlobalData.
func newCbgtGlobalData() *cbgtGlobalData {
	return &cbgtGlobalData{
		managers: make(map[*cbgt.Manager]cbgtManagerData),
	}
}

// registerManager registers a cbgt manager for cbgt's global lookup callbacks.
func (c *cbgtGlobalData) registerManager(mgr *cbgt.Manager, data cbgtManagerData) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.managers[mgr] = data
}

// unregisterManager removes mgr, so cbgt's global lookup callbacks no longer find its state.
func (c *cbgtGlobalData) unregisterManager(mgr *cbgt.Manager) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.managers, mgr)
}

// getManagerCredentials returns the credentials of mgr, if mgr is registered.
func (c *cbgtGlobalData) getManagerCredentials(mgr *cbgt.Manager) (cbgtCreds, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	data, found := c.managers[mgr]
	return data.creds, found
}

// getDestFactory returns the dest factory of mgr, if mgr was registered for destKey.
func (c *cbgtGlobalData) getDestFactory(mgr *cbgt.Manager, destKey string) (CbgtDestFactoryFunc, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	data, found := c.managers[mgr]
	if !found || data.destKey != destKey || data.destFactory == nil {
		return nil, false
	}
	return data.destFactory, true
}

// hasDestKey returns true if any registered cbgt manager was registered for destKey.
func (c *cbgtGlobalData) hasDestKey(destKey string) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, data := range c.managers {
		if data.destKey == destKey {
			return true
		}
	}
	return false
}

// getDBCredentials returns the credentials of a registered cbgt manager owned by dbName that streams from bucketName.
func (c *cbgtGlobalData) getDBCredentials(bucketName, dbName string) (cbgtCreds, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, data := range c.managers {
		if data.bucket.GetName() == bucketName && data.dbName == dbName {
			return data.creds, true
		}
	}
	return cbgtCreds{}, false
}

// getBucket returns the bucket of any registered cbgt manager that streams from bucketName.
func (c *cbgtGlobalData) getBucket(bucketName string) (*GocbV2Bucket, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, data := range c.managers {
		if data.bucket.GetName() == bucketName {
			return data.bucket, true
		}
	}
	return nil, false
}

// FetchCbgtDestFactory returns the dest factory for pindexes of destKey on mgr, or ErrNotFound if mgr was not
// registered for destKey. A mismatched destKey usually means a stale index definition that is pending removal.
func FetchCbgtDestFactory(mgr *cbgt.Manager, destKey string) (CbgtDestFactoryFunc, error) {
	destFactory, ok := cbgtGlobals.getDestFactory(mgr, destKey)
	if !ok {
		return nil, ErrNotFound
	}
	return destFactory, nil
}
