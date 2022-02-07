package db

import (
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/gocaves/client"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func SetupCaves(t *testing.T) (cluster *gocb.Cluster, collection *gocb.Collection) {
	caves, err := client.NewClient(client.NewClientOptions{
		Path: "../../../couchbaselabs/gocaves/main.go",
	})
	require.NoError(t, err)

	clusterID := uuid.New().String()
	connDetails, err := caves.CreateCluster(clusterID)
	require.NoError(t, err)

	cluster, err = gocb.Connect(connDetails.ConnStr, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
	})
	require.NoError(t, err)

	bucket := cluster.Bucket("default")
	collection = bucket.DefaultCollection()

	err = bucket.WaitUntilReady(10*time.Second, nil)
	require.NoError(t, err)

	return cluster, collection
}
