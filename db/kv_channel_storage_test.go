package db

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

type ChannelStorageType uint8

const (
	ChannelStorageType_BitFlag ChannelStorageType = iota
	ChannelStorageType_Inline
)

func testPartitionMapWithShards(numShards int) *base.IndexPartitions {

	partitions := make(base.PartitionStorageSet, numShards)

	numPartitions := uint16(numShards)
	vbPerPartition := 1024 / numPartitions
	for partition := uint16(0); partition < numPartitions; partition++ {
		pStorage := base.PartitionStorage{
			Index: partition,
			Uuid:  fmt.Sprintf("partition_%d", partition),
			VbNos: make([]uint16, vbPerPartition),
		}
		for index := uint16(0); index < vbPerPartition; index++ {
			vb := partition*vbPerPartition + index
			pStorage.VbNos[index] = vb
		}
		partitions[partition] = pStorage
	}

	indexPartitions := base.NewIndexPartitions(partitions)
	return indexPartitions
}

func testChannelStorage(storageType ChannelStorageType, indexBucket base.Bucket, channelName string, numShards int) ChannelStorage {

	partitionMap := testPartitionMapWithShards(numShards)

	if storageType == ChannelStorageType_BitFlag {
		return NewBitFlagStorage(indexBucket, channelName, partitionMap)
	} else {
		return nil
	}
}

type LogEntryGenerator struct {
	SequenceGap    uint64         // LogEntries will have gaps in sequences averaging SequenceGap (for each vbucket)
	Output         chan *LogEntry // Channel to return generated LogEntries
	terminator     chan bool      // Used to stop Generator
	latestSequence []uint64       // Tracks high sequence per vbucket for the generator
	rnd            *rand.Rand     // Random generator
}

func (l *LogEntryGenerator) Close() {
	close(l.terminator)
}

func (l *LogEntryGenerator) Start() {
	l.Output = make(chan *LogEntry, 20)
	l.terminator = make(chan bool)
	l.latestSequence = make([]uint64, 1024)
	l.rnd = rand.New(rand.NewSource(time.Now().UnixNano()))

	go l.WriteEntries()
}

func (l *LogEntryGenerator) WriteEntries() {
	for {
		select {
		case l.Output <- l.MakeNextEntry():
		case <-l.terminator:
			return
		}
	}

}

func (l *LogEntryGenerator) MakeNextEntry() *LogEntry {

	vbNo := rand.Intn(1024)
	// TODO: random distribution around SequenceGap, instead of fixed step
	l.latestSequence[vbNo] = l.latestSequence[vbNo] + l.SequenceGap

	entry := LogEntry{
		VbNo:     uint16(vbNo),
		Sequence: l.latestSequence[vbNo],
		DocID:    fmt.Sprintf("generated_document_%d_%d", vbNo, l.latestSequence[vbNo]),
		RevID:    "1-480a0a76c43f80e572405c164ffc7e3d",
	}

	return &entry
}

func TestChannelStorage(t *testing.T) {

	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	channelStorage := testChannelStorage(ChannelStorageType_BitFlag, indexBucket, "ABC", 64)

	generator := LogEntryGenerator{
		SequenceGap: 79,
	}
	defer generator.Close()
	generator.Start()

	entryCount := 10340
	validationSet, stableClock, err := WriteEntries(channelStorage, generator, entryCount)

	retrievedEntries, err := channelStorage.GetChanges(base.NewSequenceClockImpl(), stableClock)
	assertNoError(t, err, "Error retrieving entries")

	assert.Equals(t, len(retrievedEntries), entryCount)

	// Validate results against validation set
	for _, retrievedEntry := range retrievedEntries {
		matchedEntry, ok := validationSet[retrievedEntry.DocID]
		if !ok {
			assertFailed(t, fmt.Sprintf("Returned entry with doc ID %s not in validation set", retrievedEntry.DocID))
		}
		assert.Equals(t, retrievedEntry.RevID, matchedEntry.RevID)
		assert.Equals(t, retrievedEntry.Sequence, matchedEntry.Sequence)
		delete(validationSet, retrievedEntry.DocID)
	}

	// validationSet should be empty if we recieved everything we sent
	assert.Equals(t, len(validationSet), 0)

}

func WriteEntries(storage ChannelStorage, generator LogEntryGenerator, count int) (validationSet map[string]*LogEntry, stableClock base.SequenceClock, err error) {
	stableClock = base.NewSequenceClockImpl()
	validationSet = make(map[string]*LogEntry)
	// Write  entries to storage, in batches of 100
	batchSize := 100
	remaining := count
	for remaining > 0 {
		if remaining < batchSize {
			batchSize = remaining
		}
		entrySet := make([]*LogEntry, batchSize)
		for j := 0; j < batchSize; j++ {
			entry := <-generator.Output
			entrySet[j] = entry
			validationSet[entry.DocID] = entry
			if storage.StoresLogEntries() {
				storage.WriteLogEntry(entry)
			}
		}
		clockUpdates, err := storage.AddEntrySet(entrySet)
		if err != nil {
			return nil, nil, err
		}
		stableClock.UpdateWithClock(clockUpdates)
		remaining = remaining - batchSize
	}

	return validationSet, stableClock, nil
}

// Documents with (relatively) contiguous sequence values
func BenchmarkChannelStorage_Write_BitFlag_1chan(b *testing.B) {
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	channelStorage := testChannelStorage(ChannelStorageType_BitFlag, indexBucket, "ABC", 64)
	generator := LogEntryGenerator{
		SequenceGap: 1,
	}
	defer generator.Close()
	generator.Start()

	entryCount := 1000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WriteEntries(channelStorage, generator, entryCount)
	}
}

// Documents with non-contiguous sequence values
func BenchmarkChannelStorage_Write_BitFlag_1000chan(b *testing.B) {
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	channelStorage := testChannelStorage(ChannelStorageType_BitFlag, indexBucket, "ABC", 64)
	generator := LogEntryGenerator{
		SequenceGap: 1000,
	}
	defer generator.Close()
	generator.Start()

	entryCount := 1000
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WriteEntries(channelStorage, generator, entryCount)
	}
}

// Documents with non-contiguous sequence values
func BenchmarkChannelStorage_Read_BitFlag_1chan(b *testing.B) {
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	channelStorage := testChannelStorage(ChannelStorageType_BitFlag, indexBucket, "ABC", 64)
	generator := LogEntryGenerator{
		SequenceGap: 1,
	}
	defer generator.Close()
	generator.Start()

	entryCount := 1000
	var startClock base.SequenceClock
	startClock = base.NewSequenceClockImpl()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, stableClock, _ := WriteEntries(channelStorage, generator, entryCount)
		b.StartTimer()
		channelStorage.GetChanges(startClock, stableClock)
		startClock = stableClock
	}
}

func BenchmarkChannelStorage_Read_BitFlag_1000chan(b *testing.B) {
	indexBucket := testIndexBucket()
	defer indexBucket.Close()

	channelStorage := testChannelStorage(ChannelStorageType_BitFlag, indexBucket, "ABC", 64)
	generator := LogEntryGenerator{
		SequenceGap: 1000,
	}
	defer generator.Close()
	generator.Start()

	entryCount := 1000
	var startClock base.SequenceClock
	startClock = base.NewSequenceClockImpl()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		_, stableClock, _ := WriteEntries(channelStorage, generator, entryCount)
		b.StartTimer()
		channelStorage.GetChanges(startClock, stableClock)
		startClock = stableClock
	}
}
