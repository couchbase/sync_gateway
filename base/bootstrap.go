package base

import (
	"errors"
	"reflect"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10/memd"
	"github.com/imdario/mergo"
)

// BootstrapConnection is the interface that can be used to bootstrap Sync Gateway against a Couchbase Server cluster.
type BootstrapConnection interface {
	// GetConfigBuckets returns a list of bucket names where a database config could belong. In the future we'll need to fetch collections (and possibly scopes).
	GetConfigBuckets() ([]string, error)
	// GetConfig fetches a database config for a given bucket and config group ID, along with the CAS of the config document.
	GetConfig(bucket, groupID string, valuePtr interface{}) (cas uint64, err error)
	// InsertConfig saves a new database config for a given bucket and config group ID.
	InsertConfig(bucket, groupID string, value interface{}) (newCAS uint64, err error)
	// UpdateConfig updates an existing database config for a given bucket and config group ID. updateCallback can return nil to remove the config.
	UpdateConfig(bucket, groupID string, updateCallback func(rawBucketConfig []byte) (updatedConfig []byte, err error)) (newCAS uint64, err error)
	// Close closes the connection
	Close() error
}

// CouchbaseCluster is a GoCBv2 implementation of BootstrapConnection
type CouchbaseCluster struct {
	c *gocb.Cluster
}

var _ BootstrapConnection = &CouchbaseCluster{}

// NewCouchbaseCluster creates and opens a Couchbase Server cluster connection.
func NewCouchbaseCluster(server, username, password,
	x509CertPath, x509KeyPath,
	caCertPath string, tlsSkipVerify *bool) (*CouchbaseCluster, error) {

	securityConfig, err := GoCBv2SecurityConfig(tlsSkipVerify, caCertPath)
	if err != nil {
		return nil, err
	}

	authenticatorConfig, _, err := GoCBv2AuthenticatorConfig(
		username, password,
		x509CertPath, x509KeyPath,
	)
	if err != nil {
		return nil, err
	}

	clusterOptions := gocb.ClusterOptions{
		Authenticator:  authenticatorConfig,
		SecurityConfig: securityConfig,
		RetryStrategy:  &goCBv2FailFastRetryStrategy{},
	}

	cluster, err := gocb.Connect(server, clusterOptions)
	if err != nil {
		return nil, err
	}

	err = cluster.WaitUntilReady(time.Second*5, &gocb.WaitUntilReadyOptions{
		DesiredState:  gocb.ClusterStateOnline,
		ServiceTypes:  []gocb.ServiceType{gocb.ServiceTypeManagement},
		RetryStrategy: &goCBv2FailFastRetryStrategy{},
	})
	if err != nil {
		_ = cluster.Close(nil)
		return nil, err
	}

	return &CouchbaseCluster{c: cluster}, nil
}

func (cc *CouchbaseCluster) GetConfigBuckets() ([]string, error) {
	if cc == nil {
		return nil, errors.New("nil CouchbaseCluster")
	}

	buckets, err := cc.c.Buckets().GetAllBuckets(nil)
	if err != nil {
		return nil, err
	}

	bucketList := make([]string, 0, len(buckets))
	for bucketName := range buckets {
		bucketList = append(bucketList, bucketName)
	}

	return bucketList, nil
}

func (cc *CouchbaseCluster) GetConfig(location, groupID string, valuePtr interface{}) (cas uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, err := cc.getBucket(location)
	if err != nil {
		return 0, err
	}

	res, err := b.DefaultCollection().Get(PersistentConfigPrefix+groupID, &gocb.GetOptions{
		Timeout:       time.Second * 10,
		RetryStrategy: gocb.NewBestEffortRetryStrategy(nil),
	})
	if err != nil {
		if errors.Is(err, gocb.ErrDocumentNotFound) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	err = res.Content(valuePtr)
	if err != nil {
		return 0, err
	}

	return uint64(res.Cas()), nil
}

func (cc *CouchbaseCluster) InsertConfig(location, groupID string, value interface{}) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, err := cc.getBucket(location)
	if err != nil {
		return 0, err
	}

	docID := PersistentConfigPrefix + groupID
	res, err := b.DefaultCollection().Insert(docID, value, nil)
	if err != nil {
		if isKVError(err, memd.StatusKeyExists) {
			return 0, ErrAlreadyExists
		}
		return 0, err
	}

	return uint64(res.Cas()), nil
}

func (cc *CouchbaseCluster) UpdateConfig(location, groupID string, updateCallback func(bucketConfig []byte) (newConfig []byte, err error)) (newCAS uint64, err error) {
	if cc == nil {
		return 0, errors.New("nil CouchbaseCluster")
	}

	b, err := cc.getBucket(location)
	if err != nil {
		return 0, err
	}

	collection := b.DefaultCollection()

	docID := PersistentConfigPrefix + groupID
	for {
		res, err := collection.Get(docID, &gocb.GetOptions{
			Transcoder: gocb.NewRawJSONTranscoder(),
		})
		if err != nil {
			return 0, err
		}

		var bucketValue []byte
		err = res.Content(&bucketValue)
		if err != nil {
			return 0, err
		}

		newConfig, err := updateCallback(bucketValue)
		if err != nil {
			return 0, err
		}

		// handle delete when updateCallback returns nil
		if newConfig == nil {
			deleteRes, err := collection.Remove(docID, &gocb.RemoveOptions{Cas: res.Cas()})
			if err != nil {
				// retry on cas failure
				if errors.Is(err, gocb.ErrCasMismatch) {
					continue
				}
				return 0, err
			}
			return uint64(deleteRes.Cas()), nil
		}

		replaceRes, err := collection.Replace(docID, newConfig, &gocb.ReplaceOptions{Transcoder: gocb.NewRawJSONTranscoder(), Cas: res.Cas()})
		if err != nil {
			if errors.Is(err, gocb.ErrCasMismatch) {
				// retry on cas failure
				continue
			}
			return 0, err
		}

		return uint64(replaceRes.Cas()), nil
	}

}

func (cc *CouchbaseCluster) Close() error {
	if cc.c == nil {
		return nil
	}
	return cc.c.Close(&gocb.ClusterCloseOptions{})
}

// getBucket returns the bucket after waiting for it to be ready.
func (cc *CouchbaseCluster) getBucket(bucketName string) (*gocb.Bucket, error) {
	b := cc.c.Bucket(bucketName)
	err := b.WaitUntilReady(time.Second*10, &gocb.WaitUntilReadyOptions{
		DesiredState:  gocb.ClusterStateOnline,
		RetryStrategy: gocb.NewBestEffortRetryStrategy(nil),
		ServiceTypes:  []gocb.ServiceType{gocb.ServiceTypeKeyValue},
	})
	if err != nil {
		return nil, err
	}
	return b, nil
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
