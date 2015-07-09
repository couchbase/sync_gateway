package db

import (
	"log"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func TestChannelPartitionMap(t *testing.T) {
	cpMap := make(ChannelPartitionMap, 0)
	channels := []string{"A", "B", "C"}
	partitions := []uint16{0, 1, 2, 3}
	for _, ch := range channels {
		for _, par := range partitions {
			entry := &LogEntry{Sequence: 1, VbNo: 1}
			chanPar := ChannelPartition{channelName: ch, partition: par}
			cpMap.add(chanPar, entry, false)
		}
	}
	log.Printf("map: %v", cpMap)
	assert.True(t, len(cpMap) == 12)

}
