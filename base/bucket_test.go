package base

import (
	"testing"

	"github.com/couchbaselabs/go.assert"
)

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
			assert.Equals(ts, err, nil)
			assert.DeepEquals(ts, actualUUIDs, test.expectedUUIDs)
			assert.DeepEquals(ts, actualHighSeqnos, test.expectedHighSeqnos)
		})
	}
}
