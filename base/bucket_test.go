package base

import (
	"testing"

	goassert "github.com/couchbaselabs/go.assert"
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
	goassert.Equals(t, connStr, "http://localhost:8091?cacertpath=.%2FmyCACertPath&certpath=%2FmyCertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&keypath=%2Fmy%2Fkey%2Fpath&n1ql_timeout=30000")

	// CACertPath not required
	bucketSpec.CACertPath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assert.NoError(t, err, "Error creating connection string for bucket spec")
	goassert.Equals(t, connStr, "http://localhost:8091?certpath=%2FmyCertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&keypath=%2Fmy%2Fkey%2Fpath&n1ql_timeout=30000")

	// Certpath and keypath must both be defined - if either are missing, they shouldn't be included in connection string
	bucketSpec.CACertPath = "./myCACertPath"
	bucketSpec.Certpath = ""
	connStr, err = bucketSpec.GetGoCBConnString()
	assert.NoError(t, err, "Error creating connection string for bucket spec")
	goassert.Equals(t, connStr, "http://localhost:8091?cacertpath=.%2FmyCACertPath&http_idle_conn_timeout=90000&http_max_idle_conns=64000&http_max_idle_conns_per_host=256&n1ql_timeout=30000")

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
			goassert.Equals(ts, err, nil)
			goassert.DeepEquals(ts, actualUUIDs, test.expectedUUIDs)
			goassert.DeepEquals(ts, actualHighSeqnos, test.expectedHighSeqnos)
		})
	}
}
