package base

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBucketSpec(t *testing.T) {
	queryTimeout := uint32(30)
	bucketSpec := BucketSpec{
		Server:               "http://localhost:8091",
		Certpath:             "/myCertPath",
		Keypath:              "/my/key/path",
		CACertPath:           "./myCACertPath",
		ViewQueryTimeoutSecs: &queryTimeout,
	}

	connStr, err := bucketSpec.GetGoCBConnString()
	assert.NoError(t, err, "Error creating connection string for bucket spec")
	assert.Equal(t, "http://localhost:8091?cacertpath=.%2FmyCACertPath&certpath=%2FmyCertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&keypath=%2Fmy%2Fkey%2Fpath&n1ql_timeout=30000", connStr)

	// CACertPath not required
	bucketSpec.CACertPath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assert.NoError(t, err, "Error creating connection string for bucket spec")
	assert.Equal(t, "http://localhost:8091?certpath=%2FmyCertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&keypath=%2Fmy%2Fkey%2Fpath&n1ql_timeout=30000", connStr)

	// Certpath and keypath must both be defined - if either are missing, they shouldn't be included in connection string
	bucketSpec.CACertPath = "./myCACertPath"
	bucketSpec.Certpath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assert.NoError(t, err, "Error creating connection string for bucket spec")
	assert.Equal(t, "http://localhost:8091?cacertpath=.%2FmyCACertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&n1ql_timeout=30000", connStr)

	// Standard no-cert
	bucketSpec.CACertPath = ""
	bucketSpec.Certpath = ""
	bucketSpec.Keypath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assert.NoError(t, err, "Error creating connection string for bucket spec")
}

func TestGetStatsVbSeqno(t *testing.T) {

	// We'll artificially lower this here to make for easier test data
	const maxVbno = 4

	tests := []struct {
		name               string
		stats              map[string]map[string]string
		expectedUUIDs      map[uint16]uint64
		expectedHighSeqnos map[uint16]uint64
	}{
		{
			name: "1 node",
			stats: map[string]map[string]string{
				"host1:11210": map[string]string{
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
				},
			},
			expectedUUIDs: map[uint16]uint64{
				0: 90,
				1: 91,
				2: 92,
				3: 93,
			},
			expectedHighSeqnos: map[uint16]uint64{
				0: 990,
				1: 991,
				2: 992,
				3: 993,
			},
		},
		{
			name: "2 nodes",
			stats: map[string]map[string]string{
				"host1:11210": map[string]string{
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
				},
				"host2:11210": map[string]string{
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
				},
			},
			expectedUUIDs: map[uint16]uint64{
				0: 90,
				1: 91,
				2: 92,
				3: 93,
			},
			expectedHighSeqnos: map[uint16]uint64{
				0: 990,
				1: 991,
				2: 992,
				3: 993,
			},
		},
		{
			name: "4 nodes",
			stats: map[string]map[string]string{
				"host1:11210": map[string]string{
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
				},
				"host2:11210": map[string]string{
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
				},
				"host3:11210": map[string]string{
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
				},
				"host4:11210": map[string]string{
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
				},
			},
			expectedUUIDs: map[uint16]uint64{
				0: 90,
				1: 91,
				2: 92,
				3: 93,
			},
			expectedHighSeqnos: map[uint16]uint64{
				0: 990,
				1: 991,
				2: 992,
				3: 993,
			},
		},
		{
			name: "2 nodes with replica vbuckets",
			stats: map[string]map[string]string{
				"host1:11210": map[string]string{
					// active vbuckets
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
					// replica vbuckets
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
				},
				"host2:11210": map[string]string{
					// active vbuckets
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
					// replica vbuckets
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
				},
			},
			expectedUUIDs: map[uint16]uint64{
				0: 90,
				1: 91,
				2: 92,
				3: 93,
			},
			expectedHighSeqnos: map[uint16]uint64{
				0: 990,
				1: 991,
				2: 992,
				3: 993,
			},
		},
		{
			name: "2 nodes with lagging replica vbuckets",
			stats: map[string]map[string]string{
				"host1:11210": map[string]string{
					// active vbuckets
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "990",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "991",
					// slightly behind replicas
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "982",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "983",
				},
				"host2:11210": map[string]string{
					// active vbuckets
					"vb_2:uuid":       "92",
					"vb_2:high_seqno": "992",
					"vb_3:uuid":       "93",
					"vb_3:high_seqno": "993",
					// slightly behind replicas
					"vb_0:uuid":       "90",
					"vb_0:high_seqno": "980",
					"vb_1:uuid":       "91",
					"vb_1:high_seqno": "981",
				},
			},
			expectedUUIDs: map[uint16]uint64{
				0: 90,
				1: 91,
				2: 92,
				3: 93,
			},
			expectedHighSeqnos: map[uint16]uint64{
				0: 990,
				1: 991,
				2: 992,
				3: 993,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(ts *testing.T) {
			actualUUIDs, actualHighSeqnos, err := GetStatsVbSeqno(test.stats, maxVbno, false)
			assert.NoError(ts, err)
			assert.Equal(t, test.expectedUUIDs, actualUUIDs)
			assert.Equal(t, test.expectedHighSeqnos, actualHighSeqnos)
		})
	}
}

func TestChooseCouchbaseDriver(t *testing.T) {
	assert.Equal(t, GoCBCustomSGTranscoder, ChooseCouchbaseDriver(DataBucket))
	assert.Equal(t, GoCB, ChooseCouchbaseDriver(IndexBucket))
	unknownCouchbaseBucketType := CouchbaseBucketType(math.MaxInt8)
	assert.Equal(t, GoCB, ChooseCouchbaseDriver(unknownCouchbaseBucketType))
}

func TestCouchbaseDriverToString(t *testing.T) {
	assert.Equal(t, "GoCB", GoCB.String())
	assert.Equal(t, "GoCBCustomSGTranscoder", GoCBCustomSGTranscoder.String())
	unknownCouchbaseDriver := CouchbaseDriver(math.MaxInt8)
	assert.Equal(t, "UnknownCouchbaseDriver", unknownCouchbaseDriver.String())
}

func TestIsTLS(t *testing.T) {
	fakeBucketSpec := &BucketSpec{}
	fakeBucketSpec.Server = "http://localhost:8091"
	assert.False(t, fakeBucketSpec.IsTLS())
	fakeBucketSpec.Server = "https://localhost:443"
	assert.True(t, fakeBucketSpec.IsTLS())
	fakeBucketSpec.Server = "couchbases"
	assert.True(t, fakeBucketSpec.IsTLS())
}

func TestUseClientCert(t *testing.T) {
	fakeBucketSpec := &BucketSpec{}
	assert.False(t, fakeBucketSpec.UseClientCert())
	fakeBucketSpec.Certpath = "/var/lib/couchbase/inbox/ca.pem"
	assert.False(t, fakeBucketSpec.UseClientCert())
	fakeBucketSpec.Keypath = "/var/lib/couchbase/inbox/pkey.key"
	assert.True(t, fakeBucketSpec.UseClientCert())
}

func TestGetPoolName(t *testing.T) {
	fakeBucketSpec := &BucketSpec{}
	assert.Equal(t, "default", fakeBucketSpec.GetPoolName())
	fakeBucketSpec.PoolName = "Liverpool"
	assert.Equal(t, "Liverpool", fakeBucketSpec.GetPoolName())
}
