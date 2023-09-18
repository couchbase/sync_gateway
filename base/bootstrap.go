// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"sync"
	"time"

	"dario.cat/mergo"
	"github.com/couchbase/gocb/v2"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

// BootstrapConnection is the interface that can be used to bootstrap Sync Gateway against a Couchbase Server cluster.
// Manages retrieval of set of buckets, and generic interaction with bootstrap metadata documents from those buckets.
type BootstrapConnection interface {
	// GetConfigBuckets returns a list of bucket names where a bootstrap metadata documents could reside.
	GetConfigBuckets() ([]string, error)
	// GetMetadataDocument fetches a bootstrap metadata document for a given bucket and key, along with the CAS of the config document.
	GetMetadataDocument(ctx context.Context, bucket, key string, valuePtr interface{}) (cas uint64, err error)
	// InsertMetadataDocument saves a new bootstrap metadata document for a given bucket and key.
	InsertMetadataDocument(ctx context.Context, bucket, key string, value interface{}) (newCAS uint64, err error)
	// DeleteMetadataDocument deletes an existing bootstrap metadata document for a given bucket and key.
	DeleteMetadataDocument(ctx context.Context, bucket, key string, cas uint64) (err error)
	// UpdateMetadataDocument updates an existing bootstrap metadata document for a given bucket and key. updateCallback can return nil to remove the config.  Retries on CAS failure.
	UpdateMetadataDocument(ctx context.Context, bucket, key string, updateCallback func(rawBucketConfig []byte, rawBucketConfigCas uint64) (updatedConfig []byte, err error)) (newCAS uint64, err error)
	// WriteMetadataDocument writes a bootstrap metadata document for a given bucket and key.  Does not retry on CAS failure.
	WriteMetadataDocument(ctx context.Context, bucket, key string, cas uint64, valuePtr interface{}) (casOut uint64, err error)
	// TouchMetadataDocument sets the specified property in a bootstrap metadata document for a given bucket and key.  Used to
	// trigger CAS update on the document, to block any racing updates. Does not retry on CAS failure.
	TouchMetadataDocument(ctx context.Context, bucket, key string, property string, value string, cas uint64) (casOut uint64, err error)
	// KeyExists checks whether the specified key exists
	KeyExists(ctx context.Context, bucket, key string) (exists bool, err error)
	// Returns the bootstrap connection's cluster connection as N1QLStore for the specified bucket/scope/collection.
	// Does NOT establish a bucket connection, the bucketName/scopeName/collectionName is for query scoping only
	GetClusterN1QLStore(bucketName, scopeName, collectionName string) (*ClusterOnlyN1QLStore, error)
	// Close releases any long-lived connections
	Close()
}

// CouchbaseCluster is a GoCBv2 implementation of BootstrapConnection
type CouchbaseCluster struct {
	server                  string
	clusterOptions          gocb.ClusterOptions
	forcePerBucketAuth      bool // Forces perBucketAuth authenticators to be used to connect to the bucket
	perBucketAuth           map[string]*gocb.Authenticator
	bucketConnectionMode    BucketConnectionMode    // Whether to cache cluster connections
	cachedClusterConnection *gocb.Cluster           // Cached cluster connection, should only be used by GetConfigBuckets
	cachedBucketConnections cachedBucketConnections // Per-bucket cached connections
	cachedConnectionLock    sync.Mutex              // mutex for access to cachedBucketConnections
	configPersistence       ConfigPersistence       // ConfigPersistence mode
}

type BucketConnectionMode int

const (
	// CachedClusterConnections mode reuses a cached cluster connection.  Should be used for recurring operations
	CachedClusterConnections BucketConnectionMode = iota
	// PerUseClusterConnections mode establishes a new cluster connection per cluster operation.  Should be used for adhoc operations
	PerUseClusterConnections
)

type cachedBucket struct {
	bucket        *gocb.Bucket // underlying bucket
	bucketCloseFn func()       // teardown function which will close the gocb connection
	refcount      int          // count of how many functions are using this cachedBucket
	shouldClose   bool         // mark this cachedBucket as needing to be closed with ref
}

// cahedBucketConnections is a lockable map cached buckets containing refcounts
type cachedBucketConnections struct {
	buckets map[string]*cachedBucket
	lock    sync.Mutex
}

// removeOutdatedBuckets marks any active buckets for closure and removes the cached connections.
func (c *cachedBucketConnections) removeOutdatedBuckets(activeBuckets Set) {
	c.lock.Lock()
	defer c.lock.Unlock()
	for bucketName, bucket := range c.buckets {
		_, exists := activeBuckets[bucketName]
		if exists {
			continue
		}
		bucket.shouldClose = true
		c._teardown(bucketName)
	}
}

// closeAll removes all cached bucekts
func (c *cachedBucketConnections) closeAll() {
	c.lock.Lock()
	defer c.lock.Unlock()
	for _, bucket := range c.buckets {
		bucket.shouldClose = true
		bucket.bucketCloseFn()
	}
}

// teardown closes the cached bucket connection while locked, suitable for CouchbaseCluster.getBucket() teardowns
func (c *cachedBucketConnections) teardown(bucketName string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.buckets[bucketName].refcount--
	c._teardown(bucketName)
}

// _teardown closes expects the lock to be acquired before calling this function and the reference count to be up to date.
func (c *cachedBucketConnections) _teardown(bucketName string) {
	if !c.buckets[bucketName].shouldClose || c.buckets[bucketName].refcount > 0 {
		return
	}
	c.buckets[bucketName].bucketCloseFn()
	delete(c.buckets, bucketName)
}

// get returns a cachedBucket for a given bucketName, or nil if it doesn't exist
func (c *cachedBucketConnections) _get(bucketName string) *cachedBucket {
	bucket, ok := c.buckets[bucketName]
	if !ok {
		return nil
	}
	c.buckets[bucketName].refcount++
	return bucket
}

// set adds a cachedBucket for a given bucketName, or nil if it doesn't exist
func (c *cachedBucketConnections) _set(bucketName string, bucket *cachedBucket) {
	c.buckets[bucketName] = bucket
}

var _ BootstrapConnection = &CouchbaseCluster{}

// NewCouchbaseCluster creates and opens a Couchbase Server cluster connection.
func NewCouchbaseCluster(ctx context.Context, server, username, password,
	x509CertPath, x509KeyPath, caCertPath string,
	forcePerBucketAuth bool, perBucketCreds PerBucketCredentialsConfig,
	tlsSkipVerify *bool, useXattrConfig *bool, bucketMode BucketConnectionMode) (*CouchbaseCluster, error) {

	securityConfig, err := GoCBv2SecurityConfig(ctx, tlsSkipVerify, caCertPath)
	if err != nil {
		return nil, err
	}

	clusterAuthConfig, err := GoCBv2Authenticator(
		username, password,
		x509CertPath, x509KeyPath,
	)
	if err != nil {
		return nil, err
	}

	// Populate individual bucket credentials
	perBucketAuth := make(map[string]*gocb.Authenticator, len(perBucketCreds))
	for bucket, credentials := range perBucketCreds {
		authenticator, err := GoCBv2Authenticator(
			credentials.Username, credentials.Password,
			credentials.X509CertPath, credentials.X509KeyPath,
		)
		if err != nil {
			return nil, err
		}
		perBucketAuth[bucket] = &authenticator
	}

	clusterOptions := gocb.ClusterOptions{
		Authenticator:  clusterAuthConfig,
		SecurityConfig: securityConfig,
		TimeoutsConfig: GoCBv2TimeoutsConfig(nil, nil),
		RetryStrategy:  gocb.NewBestEffortRetryStrategy(nil),
	}

	cbCluster := &CouchbaseCluster{
		server:               server,
		forcePerBucketAuth:   forcePerBucketAuth,
		perBucketAuth:        perBucketAuth,
		clusterOptions:       clusterOptions,
		bucketConnectionMode: bucketMode,
	}

	if bucketMode == CachedClusterConnections {
		cbCluster.cachedBucketConnections = cachedBucketConnections{buckets: make(map[string]*cachedBucket)}
	}

	cbCluster.configPersistence = &DocumentBootstrapPersistence{}
	if useXattrConfig != nil && *useXattrConfig == true {
		cbCluster.configPersistence = &XattrBootstrapPersistence{}
	}

	return cbCluster, nil
}

func (cc *CouchbaseCluster) SetConnectionStringServerless() error {
	connSpec, err := gocbconnstr.Parse(cc.server)
	if err != nil {
		return err
	}
	if connSpec.Options == nil {
		connSpec.Options = map[string][]string{}
	}

	asValues := url.Values(connSpec.Options)

	fromConnStr := asValues.Get("kv_pool_size")
	if fromConnStr == "" {
		asValues.Set("kv_pool_size", strconv.Itoa(DefaultGocbKvPoolSizeServerless))
	}
	fromConnStr = asValues.Get("kv_buffer_size")
	if fromConnStr == "" {
		asValues.Set("kv_buffer_size", strconv.Itoa(DefaultKvBufferSizeServerless))
	}
	fromConnStr = asValues.Get("dcp_buffer_size")
	if fromConnStr == "" {
		asValues.Set("dcp_buffer_size", strconv.Itoa(DefaultDCPBufferServerless))
	}
	connSpec.Options = asValues
	cc.server = connSpec.String()
	return nil
}

// connect attempts to open a gocb.Cluster connection. Callers will be responsible for closing the connection.
// Pass an authenticator to use that to connect instead of using the cluster credentials.
func (cc *CouchbaseCluster) connect(auth *gocb.Authenticator) (*gocb.Cluster, error) {
	clusterOptions := cc.clusterOptions
	if auth != nil {
		clusterOptions.Authenticator = *auth
		clusterOptions.Username = ""
		clusterOptions.Password = ""
	}

	cluster, err := gocb.Connect(cc.server, clusterOptions)
	if err != nil {
		return nil, err
	}

	err = cluster.WaitUntilReady(time.Second*10, &gocb.WaitUntilReadyOptions{
		DesiredState:  gocb.ClusterStateOnline,
		ServiceTypes:  []gocb.ServiceType{gocb.ServiceTypeManagement},
		RetryStrategy: &goCBv2FailFastRetryStrategy{},
	})
	if err != nil {
		_ = cluster.Close(nil)
		return nil, err
	}

	return cluster, nil
}

func (cc *CouchbaseCluster) getClusterConnection() (*gocb.Cluster, error) {

	if cc.bucketConnectionMode == PerUseClusterConnections {
		return cc.connect(nil)
	}

	cc.cachedConnectionLock.Lock()
	defer cc.cachedConnectionLock.Unlock()
	if cc.cachedClusterConnection != nil {
		return cc.cachedClusterConnection, nil
	}

	clusterConnection, err := cc.connect(nil)
	if err != nil {
		return nil, err
	}
	cc.cachedClusterConnection = clusterConnection
	return cc.cachedClusterConnection, nil

}

func (cc *CouchbaseCluster) GetConfigBuckets() ([]string, error) {
	if cc == nil {
		return nil, errors.New("nil CouchbaseCluster")
	}

	connection, err := cc.getClusterConnection()
	if err != nil {
		return nil, err
	}

	defer func() {
		if cc.bucketConnectionMode == PerUseClusterConnections {
			_ = connection.Close(nil)
		}
	}()

	buckets, err := connection.Buckets().GetAllBuckets(nil)
	if err != nil {
		cc.cachedClusterConnection = nil
		return nil, err
	}

	bucketList := make([]string, 0, len(buckets))
	for bucketName := range buckets {
		bucketList = append(bucketList, bucketName)
	}

	cc.cachedBucketConnections.removeOutdatedBuckets(SetOf(bucketList...))

	return bucketList, nil
}

func (cc *CouchbaseCluster) GetMetadataDocument(ctx context.Context, location, docID string, valuePtr interface{}) (cas uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)

	if err != nil {
		return 0, err
	}

	defer teardown()

	return cc.configPersistence.loadConfig(ctx, b.DefaultCollection(), docID, valuePtr)
}

func (cc *CouchbaseCluster) InsertMetadataDocument(ctx context.Context, location, key string, value interface{}) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	return cc.configPersistence.insertConfig(b.DefaultCollection(), key, value)
}

// WriteMetadataDocument writes a metadata document, and fails on CAS mismatch
func (cc *CouchbaseCluster) WriteMetadataDocument(ctx context.Context, location, docID string, cas uint64, value interface{}) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	rawDocument, err := JSONMarshal(value)
	if err != nil {
		return 0, err
	}

	casOut, err := cc.configPersistence.replaceRawConfig(b.DefaultCollection(), docID, rawDocument, gocb.Cas(cas))
	return uint64(casOut), err
}

func (cc *CouchbaseCluster) TouchMetadataDocument(ctx context.Context, location, docID string, property, value string, cas uint64) (newCAS uint64, err error) {

	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	casOut, err := cc.configPersistence.touchConfigRollback(b.DefaultCollection(), docID, property, value, gocb.Cas(cas))
	return uint64(casOut), err

}

func (cc *CouchbaseCluster) DeleteMetadataDocument(ctx context.Context, location, key string, cas uint64) (err error) {
	if cc == nil {
		return errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)
	if err != nil {
		return err
	}
	defer teardown()

	_, removeErr := cc.configPersistence.removeRawConfig(b.DefaultCollection(), key, gocb.Cas(cas))
	return removeErr
}

// UpdateMetadataDocument retries on CAS mismatch
func (cc *CouchbaseCluster) UpdateMetadataDocument(ctx context.Context, location, docID string, updateCallback func(bucketConfig []byte, rawBucketConfigCas uint64) (newConfig []byte, err error)) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	collection := b.DefaultCollection()

	for {
		bucketValue, cas, err := cc.configPersistence.loadRawConfig(ctx, collection, docID)
		if err != nil {
			return 0, err
		}
		newConfig, err := updateCallback(bucketValue, uint64(cas))
		if err != nil {
			return 0, err
		}

		// handle delete when updateCallback returns nil
		if newConfig == nil {
			removeCasOut, err := cc.configPersistence.removeRawConfig(collection, docID, cas)
			if err != nil {
				// retry on cas failure
				if errors.Is(err, gocb.ErrCasMismatch) {
					continue
				}
				return 0, err
			}
			return uint64(removeCasOut), nil
		}

		replaceCfgCasOut, err := cc.configPersistence.replaceRawConfig(collection, docID, newConfig, cas)
		if err != nil {
			if errors.Is(err, gocb.ErrCasMismatch) {
				// retry on cas failure
				continue
			}
			return 0, err
		}

		return uint64(replaceCfgCasOut), nil
	}

}

func (cc *CouchbaseCluster) KeyExists(ctx context.Context, location, docID string) (exists bool, err error) {
	if cc == nil {
		return false, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(ctx, location)

	if err != nil {
		return false, err
	}

	defer teardown()

	return cc.configPersistence.keyExists(b.DefaultCollection(), docID)
}

// Close calls teardown for any cached buckets and removes from cachedBucketConnections
func (cc *CouchbaseCluster) Close() {

	cc.cachedBucketConnections.closeAll()

	cc.cachedConnectionLock.Lock()
	defer cc.cachedConnectionLock.Unlock()

	if cc.cachedClusterConnection != nil {
		_ = cc.cachedClusterConnection.Close(nil)
		cc.cachedClusterConnection = nil
	}
}

func (cc *CouchbaseCluster) GetClusterN1QLStore(bucketName, scopeName, collectionName string) (*ClusterOnlyN1QLStore, error) {
	gocbCluster, err := cc.getClusterConnection()
	if err != nil {
		return nil, err
	}

	return NewClusterOnlyN1QLStore(gocbCluster, bucketName, scopeName, collectionName)
}

func (cc *CouchbaseCluster) getBucket(ctx context.Context, bucketName string) (b *gocb.Bucket, teardownFn func(), err error) {

	if cc.bucketConnectionMode != CachedClusterConnections {
		return cc.connectToBucket(ctx, bucketName)
	}

	teardownFn = func() {
		cc.cachedBucketConnections.teardown(bucketName)
	}
	cc.cachedBucketConnections.lock.Lock()
	defer cc.cachedBucketConnections.lock.Unlock()
	bucket := cc.cachedBucketConnections._get(bucketName)
	if bucket != nil {
		return bucket.bucket, teardownFn, nil
	}

	// cached bucket not found, connect and add
	newBucket, bucketCloseFn, err := cc.connectToBucket(ctx, bucketName)
	if err != nil {
		return nil, nil, err
	}
	cc.cachedBucketConnections._set(bucketName, &cachedBucket{
		bucket:        newBucket,
		bucketCloseFn: bucketCloseFn,
		refcount:      1,
	})

	return newBucket, teardownFn, nil
}

// connectToBucket establishes a new connection to a bucket, and returns the bucket after waiting for it to be ready.
func (cc *CouchbaseCluster) connectToBucket(ctx context.Context, bucketName string) (b *gocb.Bucket, teardownFn func(), err error) {
	var connection *gocb.Cluster
	if bucketAuth, set := cc.perBucketAuth[bucketName]; set {
		connection, err = cc.connect(bucketAuth)
	} else if cc.forcePerBucketAuth {
		return nil, nil, fmt.Errorf("unable to get bucket %q since credentials are not defined in bucket_credentials", MD(bucketName).Redact())
	} else {
		connection, err = cc.connect(nil)
	}

	if err != nil {
		return nil, nil, err
	}

	b = connection.Bucket(bucketName)
	err = b.WaitUntilReady(time.Second*10, &gocb.WaitUntilReadyOptions{
		DesiredState:  gocb.ClusterStateOnline,
		RetryStrategy: &goCBv2FailFastRetryStrategy{},
		ServiceTypes:  []gocb.ServiceType{gocb.ServiceTypeKeyValue},
	})
	if err != nil {
		_ = connection.Close(&gocb.ClusterCloseOptions{})

		if errors.Is(err, gocb.ErrAuthenticationFailure) {
			return nil, nil, ErrAuthError
		}

		return nil, nil, err
	}

	teardownFn = func() {
		err := connection.Close(&gocb.ClusterCloseOptions{})
		if err != nil {
			WarnfCtx(ctx, "Failed to close cluster connection: %v", err)
		}
	}

	return b, teardownFn, nil
}

type PerBucketCredentialsConfig map[string]*CredentialsConfig

type CredentialsConfig struct {
	Username     string `json:"username,omitempty"       help:"Username for authenticating to the bucket"`
	Password     string `json:"password,omitempty"       help:"Password for authenticating to the bucket"`
	X509CertPath string `json:"x509_cert_path,omitempty" help:"Cert path (public key) for X.509 bucket auth"`
	X509KeyPath  string `json:"x509_key_path,omitempty"  help:"Key path (private key) for X.509 bucket auth"`
}

// ConfigMerge applies non-empty fields from b onto non-empty fields on a
func ConfigMerge(a, b interface{}) error {
	return mergo.Merge(a, b, mergo.WithTransformers(&mergoNilTransformer{}), mergo.WithOverride)
}

// mergoNilTransformer is a mergo.Transformers implementation that treats non-nil zero values as non-empty when merging.
type mergoNilTransformer struct{}

var _ mergo.Transformers = &mergoNilTransformer{}

func (t *mergoNilTransformer) Transformer(typ reflect.Type) func(dst, src reflect.Value) error {
	if typ.Kind() == reflect.Ptr {
		if typ.Elem().Kind() == reflect.Struct {
			// skip nilTransformer for structs, to allow recursion
			return nil
		}
		return func(dst, src reflect.Value) error {
			if dst.CanSet() && !src.IsNil() {
				dst.Set(src)
			}
			return nil
		}
	}
	return nil
}
