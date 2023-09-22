package base

import (
	"sort"
	"strings"
	"testing"

	"github.com/imdario/mergo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeStructPointer(t *testing.T) {
	type structPtr struct {
		I *int
		S string
	}
	type wrap struct {
		Ptr *structPtr
	}
	override := wrap{Ptr: &structPtr{nil, "changed"}}

	source := wrap{Ptr: &structPtr{IntPtr(5), "test"}}
	err := mergo.Merge(&source, &override, mergo.WithTransformers(&mergoNilTransformer{}), mergo.WithOverride)

	require.Nil(t, err)
	assert.Equal(t, "changed", source.Ptr.S)
	assert.Equal(t, IntPtr(5), source.Ptr.I)
}

func TestBootstrapSorting(t *testing.T) {
	if UnitTestUrlIsWalrus() {
		t.Skip("Test requires making a connection to CBS")
	}
	// Integration tests are configured to run in these parameters, they are used in main_test_bucket_pool.go
	// Future enhancement would be to allow all integration tests to run with TLS
	x509CertPath := ""
	x509KeyPath := ""
	caCertPath := ""
	tlsSkipVerify := BoolPtr(false)
	cluster, err := NewCouchbaseCluster(UnitTestUrl(), TestClusterUsername(), TestClusterPassword(), x509CertPath, x509KeyPath, caCertPath, tlsSkipVerify, BoolPtr(TestUseXattrs()), CachedClusterConnections)
	require.NoError(t, err)
	defer cluster.Close()
	require.NotNil(t, cluster)

	clusterConnection, err := cluster.getClusterConnection()
	require.NoError(t, err)
	require.NotNil(t, clusterConnection)

	buckets, err := cluster.GetConfigBuckets()
	require.NoError(t, err)
	// ensure these are sorted for determinstic bootstraping
	sortedBuckets := make([]string, len(buckets))
	copy(sortedBuckets, buckets)
	sort.Strings(sortedBuckets)
	require.Equal(t, sortedBuckets, buckets)

	var testBuckets []string
	for _, bucket := range buckets {
		if strings.HasPrefix(bucket, tbpBucketNamePrefix) {
			testBuckets = append(testBuckets, bucket)
		}

	}
	require.Len(t, testBuckets, tbpNumBuckets())

}
