package base

import (
	"errors"
	"time"

	"github.com/couchbase/gocb"
)

// BootstrapConnection is the interface that can be used to bootstrap Sync Gateway against a Couchbase Server cluster.
type BootstrapConnection interface {
	// GetConfigLocations returns a list of location names where a database config could belong (e.g. Bucket names, Collection names)
	GetConfigLocations() ([]string, error)
	// GetConfig fetches a database config for a given location and config group ID, along with the CAS of the document.
	GetConfig(location, groupID string, valuePtr interface{}) (cas uint64, err error)
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
	caCertPath string) (*CouchbaseCluster, error) {

	securityConfig, err := GoCBv2SecurityConfig(caCertPath)
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
		return nil, err
	}

	return &CouchbaseCluster{c: cluster}, nil
}

func (cc *CouchbaseCluster) GetConfigLocations() ([]string, error) {
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

	res, err := cc.c.Bucket(location).DefaultCollection().Get(PersistentConfigPrefix+groupID, nil)
	if err != nil {
		return 0, err
	}
	err = res.Content(valuePtr)
	if err != nil {
		return 0, err
	}

	return uint64(res.Cas()), nil
}

func (cc *CouchbaseCluster) Close() error {
	if cc.c == nil {
		return nil
	}
	return cc.c.Close(&gocb.ClusterCloseOptions{})
}
