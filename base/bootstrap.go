package base

import (
	"errors"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/imdario/mergo"
)

// BootstrapConnection is the interface that can be used to bootstrap Sync Gateway against a Couchbase Server cluster.
type BootstrapConnection interface {
	// GetConfigBuckets returns a list of bucket names where a database config could belong. In the future we'll need to fetch collections (and possibly scopes).
	GetConfigBuckets() ([]string, error)
	// GetConfig fetches a database config for a given bucket and config group ID, along with the CAS of the config document.
	GetConfig(bucket, groupID string, valuePtr interface{}) (cas uint64, err error)
	// HasPost30Config returns true if the bucket has a database registry from 3.1 or greater.
	HasPost30Config(bucket string) (bool, error)
	// InsertConfig saves a new database config for a given bucket and config group ID.

	InsertConfig(bucket, groupID string, value interface{}) (newCAS uint64, err error)
	// UpdateConfig updates an existing database config for a given bucket and config group ID. updateCallback can return nil to remove the config.
	UpdateConfig(bucket, groupID string, updateCallback func(rawBucketConfig []byte) (updatedConfig []byte, err error)) (newCAS uint64, err error)
	// Close releases any long-lived connections
	Close()
}

// CouchbaseCluster is a GoCBv2 implementation of BootstrapConnection
type CouchbaseCluster struct {
	server                  string
	configPersistence       ConfigPersistence
	clusterOptions          gocb.ClusterOptions
	bucketConnectionMode    BucketConnectionMode     // Whether to cache cluster connections
	cachedClusterConnection *gocb.Cluster            // Cached cluster connection, should only be used by GetConfigBuckets
	cachedBucketConnections map[string]*cachedBucket // Per-bucket cached connections
	cachedConnectionLock    sync.Mutex               // mutex for access to cachedBucketConnections
}

type BucketConnectionMode int

const (
	// CachedClusterConnections mode reuses a cached cluster connection.  Should be used for recurring operations
	CachedClusterConnections BucketConnectionMode = iota
	// PerUseClusterConnections mode establishes a new cluster connection per cluster operation.  Should be used for adhoc operations
	PerUseClusterConnections
)

type cachedBucket struct {
	bucket     *gocb.Bucket
	teardownFn func()
}

// noopTeardown is returned by getBucket when using a cached bucket - these buckets are torn down
// when CouchbaseCluster.Close is called.
func noopTeardown() {}

var _ BootstrapConnection = &CouchbaseCluster{}

// NewCouchbaseCluster creates and opens a Couchbase Server cluster connection.
func NewCouchbaseCluster(server, username, password,
	x509CertPath, x509KeyPath,
	caCertPath string, tlsSkipVerify *bool,
	useXattrConfig *bool, bucketMode BucketConnectionMode) (*CouchbaseCluster, error) {

	securityConfig, err := GoCBv2SecurityConfig(tlsSkipVerify, caCertPath)
	if err != nil {
		return nil, err
	}

	authenticatorConfig, err := GoCBv2Authenticator(
		username, password,
		x509CertPath, x509KeyPath,
	)
	if err != nil {
		return nil, err
	}

	clusterOptions := gocb.ClusterOptions{
		Authenticator:  authenticatorConfig,
		SecurityConfig: securityConfig,
		TimeoutsConfig: GoCBv2TimeoutsConfig(nil, nil),
		RetryStrategy:  gocb.NewBestEffortRetryStrategy(nil),
	}

	var configPersistence ConfigPersistence

	configPersistence = &DocumentBootstrapPersistence{}
	if useXattrConfig != nil && *useXattrConfig == true {
		configPersistence = &XattrBootstrapPersistence{}
	}

	cbCluster := &CouchbaseCluster{
		server:               server,
		clusterOptions:       clusterOptions,
		configPersistence:    configPersistence,
		bucketConnectionMode: bucketMode,
	}

	if bucketMode == CachedClusterConnections {
		cbCluster.cachedBucketConnections = make(map[string]*cachedBucket)
	}

	return cbCluster, nil
}

// connect attempts to open a gocb.Cluster connection. Callers will be responsible for closing the connection.
func (cc *CouchbaseCluster) connect() (*gocb.Cluster, error) {
	cluster, err := gocb.Connect(cc.server, cc.clusterOptions)
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
		return cc.connect()
	}

	cc.cachedConnectionLock.Lock()
	defer cc.cachedConnectionLock.Unlock()
	if cc.cachedClusterConnection != nil {
		return cc.cachedClusterConnection, nil
	}

	clusterConnection, err := cc.connect()
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

	sort.Strings(bucketList)
	return bucketList, nil
}

func (cc *CouchbaseCluster) GetConfig(location, groupID string, valuePtr interface{}) (cas uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(location)

	if err != nil {
		return 0, err
	}

	defer teardown()

	return cc.configPersistence.loadConfig(b.DefaultCollection(), PersistentConfigPrefix+groupID, valuePtr)
}

func (cc *CouchbaseCluster) HasPost30Config(bucket string) (bool, error) {
	if cc == nil {
		return false, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(bucket)

	if err != nil {
		return false, err
	}

	defer teardown()

	var valuePtr interface{}
	_, err = cc.configPersistence.loadConfig(b.DefaultCollection(), "_sync:registry", valuePtr)
	return !IsDocNotFoundError(err), nil
}

func (cc *CouchbaseCluster) InsertConfig(location, groupID string, value interface{}) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	docID := PersistentConfigPrefix + groupID

	return cc.configPersistence.insertConfig(b.DefaultCollection(), docID, value)
}

func (cc *CouchbaseCluster) UpdateConfig(location, groupID string, updateCallback func(bucketConfig []byte) (newConfig []byte, err error)) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, teardown, err := cc.getBucket(location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	collection := b.DefaultCollection()

	docID := PersistentConfigPrefix + groupID
	for {

		bucketValue, cas, err := cc.configPersistence.loadRawConfig(collection, docID)
		if err != nil {
			return 0, err
		}

		newConfig, err := updateCallback(bucketValue)
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

		_, replaceCfgCasOut, err := cc.configPersistence.replaceRawConfig(collection, docID, newConfig, cas)
		if err != nil {
			if errors.Is(err, gocb.ErrCasMismatch) {
				// retry on cas failure
				continue
			}
			return 0, err
		}

		return replaceCfgCasOut, nil
	}

}

// Close calls teardown for any cached buckets and removes from cachedBucketConnections
func (cc *CouchbaseCluster) Close() {

	cc.cachedConnectionLock.Lock()
	defer cc.cachedConnectionLock.Unlock()

	for bucketName, cachedBucket := range cc.cachedBucketConnections {
		cachedBucket.teardownFn()
		delete(cc.cachedBucketConnections, bucketName)
	}
	if cc.cachedClusterConnection != nil {
		_ = cc.cachedClusterConnection.Close(nil)
		cc.cachedClusterConnection = nil
	}
}

func (cc *CouchbaseCluster) getBucket(bucketName string) (b *gocb.Bucket, teardownFn func(), err error) {

	if cc.bucketConnectionMode != CachedClusterConnections {
		return cc.connectToBucket(bucketName)
	}

	cc.cachedConnectionLock.Lock()
	defer cc.cachedConnectionLock.Unlock()

	cacheBucket, ok := cc.cachedBucketConnections[bucketName]
	if ok {
		return cacheBucket.bucket, noopTeardown, nil
	}

	// cached bucket not found, connect and add
	newBucket, newTeardownFn, err := cc.connectToBucket(bucketName)
	if err != nil {
		return nil, nil, err
	}
	cc.cachedBucketConnections[bucketName] = &cachedBucket{
		bucket:     newBucket,
		teardownFn: newTeardownFn,
	}
	return newBucket, noopTeardown, nil
}

// For unrecoverable errors when using cached buckets, remove the bucket from the cache to trigger a new connection on next usage
func (cc *CouchbaseCluster) onCachedBucketError(bucketName string) {

	cc.cachedConnectionLock.Lock()
	defer cc.cachedConnectionLock.Unlock()
	cacheBucket, ok := cc.cachedBucketConnections[bucketName]
	if ok {
		cacheBucket.teardownFn()
		delete(cc.cachedBucketConnections, bucketName)
	}
}

// connectToBucket establishes a new connection to a bucket, and returns the bucket after waiting for it to be ready.
func (cc *CouchbaseCluster) connectToBucket(bucketName string) (b *gocb.Bucket, teardownFn func(), err error) {

	var connection *gocb.Cluster
	connection, err = cc.connect()

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
			Warnf("Failed to close cluster connection: %v", err)
		}
	}

	return b, teardownFn, nil
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
