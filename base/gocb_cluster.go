package base

import (
	"time"

	"github.com/couchbase/gocb"
)

// BootstrapConnection is the interface that can be used to bootstrap Sync Gateway against a Couchbase Server cluster.
type BootstrapConnection interface {
	Close() error
}

// CouchbaseCluster is a GoCBv2 implementation of BootstrapConnection
type CouchbaseCluster struct {
	c *gocb.Cluster
}

var _ BootstrapConnection = &CouchbaseCluster{}

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
		RetryStrategy:  &GoCBv2FailFastRetryStrategy{},
	}

	cluster, err := gocb.Connect(server, clusterOptions)
	if err != nil {
		return nil, err
	}

	err = cluster.WaitUntilReady(time.Second, &gocb.WaitUntilReadyOptions{
		DesiredState: gocb.ClusterStateOnline,
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeManagement},
	})
	if err != nil {
		return nil, err
	}

	return &CouchbaseCluster{c: cluster}, nil
}

func (cc *CouchbaseCluster) Close() error {
	if cc.c == nil {
		return nil
	}
	return cc.c.Close(&gocb.ClusterCloseOptions{})
}
