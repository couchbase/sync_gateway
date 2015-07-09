package db

import (
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

func testPartitionMap() IndexPartitionMap {
	// Simplified partition map of 64 sequential partitions, 16 vbuckets each
	partitions := make(IndexPartitionMap, 64)

	numPartitions := uint16(64)
	vbPerPartition := 1024 / numPartitions
	for partition := uint16(0); partition < numPartitions; partition++ {
		for index := uint16(0); index < vbPerPartition; index++ {
			vb := partition*vbPerPartition + index
			partitions[vb] = partition
		}
	}
	return partitions
}

func testStableSequence() (uint64, error) {
	return 0, nil
}

func testOnChange(keys base.Set) {
	for key, _ := range keys {
		log.Println("on change:", key)
	}
}

func TestIndexBlocks(t *testing.T) {

	context, _ := NewDatabaseContext("db", testBucket(), false, CacheOptions{}, nil)
	channelIndex := NewKvChannelIndex("ABC", context, testPartitionMap(), testStableSequence, testOnChange)

	blockKey := GenerateBlockKey("ABC", uint64(1), uint16(1))
	block := channelIndex.getIndexBlock(blockKey)
	if block == nil {
		channelIndex.putIndexBlockToCache(blockKey, NewIndexBlock("ABC", 1, 1))
	}
	assert.True(t, len(channelIndex.indexBlockCache) == 1)

}
