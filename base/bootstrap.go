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
}

// CouchbaseCluster is a GoCBv2 implementation of BootstrapConnection
type CouchbaseCluster struct {
	server             string
	clusterOptions     gocb.ClusterOptions
	forcePerBucketAuth bool // Forces perBucketAuth authenticators to be used to connect to the bucket
	perBucketAuth      map[string]*gocb.Authenticator
}

var _ BootstrapConnection = &CouchbaseCluster{}

// NewCouchbaseCluster creates and opens a Couchbase Server cluster connection.
func NewCouchbaseCluster(server, username, password,
	x509CertPath, x509KeyPath, caCertPath string,
	forcePerBucketAuth bool, perBucketCreds PerBucketCredentialsConfig,
	tlsSkipVerify *bool) (*CouchbaseCluster, error) {

	securityConfig, err := GoCBv2SecurityConfig(tlsSkipVerify, caCertPath)
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
		RetryStrategy:  &goCBv2FailFastRetryStrategy{},
	}

	cbCluster := &CouchbaseCluster{
		server:             server,
		forcePerBucketAuth: forcePerBucketAuth,
		perBucketAuth:      perBucketAuth,
		clusterOptions:     clusterOptions,
	}
	return cbCluster, nil
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

func (cc *CouchbaseCluster) GetConfigBuckets() ([]string, error) {
	if cc == nil {
		return nil, errors.New("nil CouchbaseCluster")
	}

	connection, err := cc.connect(nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = connection.Close(&gocb.ClusterCloseOptions{})
	}()

	buckets, err := connection.Buckets().GetAllBuckets(nil)
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

	b, teardown, err := cc.getBucket(location)
	if err != nil {
		return 0, err
	}

	defer teardown()

	docID, err := PersistentConfigKey(groupID)
	if err != nil {
		return 0, err
	}
	res, err := b.DefaultCollection().Get(docID, &gocb.GetOptions{
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

	b, teardown, err := cc.getBucket(location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	docID, err := PersistentConfigKey(groupID)
	if err != nil {
		return 0, err
	}
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

	b, teardown, err := cc.getBucket(location)
	if err != nil {
		return 0, err
	}
	defer teardown()

	collection := b.DefaultCollection()

	docID, err := PersistentConfigKey(groupID)
	if err != nil {
		return 0, err
	}
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

// getBucket returns the bucket after waiting for it to be ready.
func (cc *CouchbaseCluster) getBucket(bucketName string) (b *gocb.Bucket, teardownFn func(), err error) {
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
			WarnfCtx(context.Background(), "Failed to close cluster connection: %v", err)
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
