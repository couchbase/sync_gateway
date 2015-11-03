package base

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

var numShards = uint16(64)
var maxVbNo = uint16(1024)

func GenerateTestIndexPartitions(maxVbNo uint16, numPartitions uint16) *IndexPartitions {

	partitionDefs := make([]PartitionStorage, numPartitions)
	vbPerPartition := maxVbNo / numPartitions
	for partition := uint16(0); partition < numPartitions; partition++ {
		storage := PartitionStorage{
			Index: partition,
			VbNos: make([]uint16, vbPerPartition),
		}
		for index := uint16(0); index < vbPerPartition; index++ {
			vb := partition*vbPerPartition + index
			storage.VbNos[index] = vb
		}
		partitionDefs[partition] = storage
	}

	return NewIndexPartitions(partitionDefs)
}

func TestShardedClockSizes(t *testing.T) {

	scp := InitShardedClockPartition()
	scpBytes, _ := scp.Marshal()
	clock := NewSequenceClockImpl()
	scp.AddToClock(clock)
	log.Printf("SCP bytes:%d", len(scpBytes))

	p := InitShardedClockPartition()
	pclock := NewSequenceClockImpl()
	p.AddToClock(pclock)
	pBytes, _ := p.Marshal()
	log.Printf("Packed SCP bytes:%d", len(pBytes))

}

func InitGobShardedClockPartition() *GobShardedClockPartition {
	scp := NewGobShardedClockPartition("testKey", 0)
	r := rand.New(rand.NewSource(42))
	numVbs := 1024 / numShards
	for i := uint16(0); i < numVbs; i++ {
		seq := r.Int63()
		scp.SetSequence(uint16(i), uint64(seq))
	}
	return scp
}

func InitShardedClockPartition() *ShardedClockPartition {
	return InitShardedClockPartitionWithVbNos(GenerateTestIndexPartitions(maxVbNo, numShards).PartitionDefs[0].VbNos)
}

func InitShardedClockPartitionWithVbNos(vbnos []uint16) *ShardedClockPartition {

	p := NewShardedClockPartition("testKey", 0, vbnos)
	r := rand.New(rand.NewSource(42))
	numVbs := 1024 / numShards
	for i := uint16(0); i < numVbs; i++ {
		seq := r.Int63()
		p.SetSequence(uint16(i), uint64(seq))
	}
	return p
}

func TestShardedClockPartition(t *testing.T) {

	vbNos := GenerateTestIndexPartitions(maxVbNo, numShards).PartitionDefs[5].VbNos
	p := NewShardedClockPartition("testKey", 5, vbNos)
	assert.Equals(t, p.GetIndex(), uint16(5))

	p.SetSequence(vbNos[0], 50)

	assert.Equals(t, p.GetSequence(vbNos[0]), uint64(50))
}

func BenchmarkShardedClockPartitionInit(b *testing.B) {
	for i := 0; i < b.N; i++ {
		InitShardedClockPartition()
	}
}

func BenchmarkShardedClockPartitionMarshal(b *testing.B) {
	scp := InitShardedClockPartition()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = scp.Marshal()
	}
}

func BenchmarkShardedClockPartitionUnmarshal(b *testing.B) {
	scp := InitShardedClockPartition()
	bytes, _ := scp.Marshal()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = scp.Unmarshal(bytes)
	}
}

func BenchmarkShardedClockPartitionSetSequence(b *testing.B) {
	scp := InitShardedClockPartition()
	vbNo := uint16(12)
	seq := uint64(453678593)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scp.SetSequence(vbNo, seq)
	}
}

func BenchmarkShardedClockPartitionAddToClock(b *testing.B) {
	scp := InitShardedClockPartition()
	clock := NewSequenceClockImpl()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = scp.AddToClock(clock)
	}
}

func BenchmarkGobShardedClockPartitionInit(b *testing.B) {
	vbNos := GenerateTestIndexPartitions(maxVbNo, numShards).PartitionDefs[0].VbNos
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		InitShardedClockPartitionWithVbNos(vbNos)
	}
}

func BenchmarkGobShardedClockPartitionMarshal(b *testing.B) {
	p := InitGobShardedClockPartition()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = p.Marshal()
	}
}

func BenchmarkGobShardedClockPartitionUnmarshal(b *testing.B) {
	p := InitGobShardedClockPartition()
	bytes, _ := p.Marshal()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Unmarshal(bytes)
	}
}

func BenchmarkGobShardedClockPartitionSetSequence(b *testing.B) {
	p := InitGobShardedClockPartition()
	vbNo := uint16(12)
	seq := uint64(453678593)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.SetSequence(vbNo, seq)
	}
}

func BenchmarkGobShardedClockPartitionAddToClock(b *testing.B) {
	p := InitGobShardedClockPartition()
	clock := NewSequenceClockImpl()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.AddToClock(clock)
	}
}

// ShardedClockPartition manages storage for one clock partition, where a clock partition is a set of
// {vb, seq} values for a subset of vbuckets.
type GobShardedClockPartition struct {
	Index  uint16            // Partition Index
	Key    string            // Clock partition document key
	values map[uint16]uint64 // Clock partition values, indexed by vb
	cas    uint64            // cas value of partition doc
	dirty  bool              // Whether values have been updated since last save/load
}

func NewGobShardedClockPartition(baseKey string, index uint16) *GobShardedClockPartition {
	return &GobShardedClockPartition{
		Index:  index,
		Key:    fmt.Sprintf(kClockPartitionKeyFormat, baseKey, index),
		values: make(map[uint16]uint64),
	}
}

// TODO: replace with something more intelligent than gob encode, to take advantage of known
//       clock structure?
func (scp *GobShardedClockPartition) Marshal() ([]byte, error) {
	var output bytes.Buffer
	enc := gob.NewEncoder(&output)
	err := enc.Encode(scp)
	if err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func (scp *GobShardedClockPartition) Unmarshal(value []byte) error {
	input := bytes.NewBuffer(value)
	dec := gob.NewDecoder(input)
	err := dec.Decode(&scp)
	if err != nil {
		return err
	}
	return nil
}

func (scp *GobShardedClockPartition) SetSequence(vb uint16, seq uint64) {
	scp.values[vb] = seq
	scp.dirty = true
}

func (scp *GobShardedClockPartition) AddToClock(clock SequenceClock) error {
	for vb, seq := range scp.values {
		clock.SetSequence(vb, seq)
	}
	return nil
}
