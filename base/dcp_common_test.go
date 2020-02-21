package base

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetExternalAlternateAddress(t *testing.T) {
	tests := []struct {
		name         string
		dest         string
		altAddrMap   map[string]string
		expectedDest string
	}{
		{
			name:         "no alts",
			dest:         "node1.cbs.example.org:1234",
			expectedDest: "node1.cbs.example.org:1234",
		},
		{
			name: "non-matching alt",
			dest: "node1.cbs.example.org:1234",
			altAddrMap: map[string]string{
				"node9.cbs.example.org": "10.10.10.9:5678",
			},
			expectedDest: "node1.cbs.example.org:1234",
		},
		{
			name: "matching alt, alt connect URL",
			dest: "node1.cbs.example.org:1234",
			altAddrMap: map[string]string{
				"node1.cbs.example.org": "10.10.10.1:5678",
			},
			expectedDest: "10.10.10.1:5678",
		},
		{
			name: "matching alt multiple",
			dest: "node2.cbs.example.org:1234",
			altAddrMap: map[string]string{
				"node1.cbs.example.org": "10.10.10.1:5678",
				"node2.cbs.example.org": "10.10.10.2:5678",
				"node3.cbs.example.org": "10.10.10.3:5678",
			},
			expectedDest: "10.10.10.2:5678",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			newDest, err := getExternalAlternateAddress(nil, test.altAddrMap, test.dest)
			require.NoError(t, err)
			assert.Equal(t, test.expectedDest, newDest)
		})
	}
}
