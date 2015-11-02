package base

import (
	"bytes"
	"encoding/gob"
	"expvar"
	"fmt"
	"sync"

	"github.com/couchbase/sg-bucket"
)

var shardedClockExpvars *expvar.Map

func init() {
	shardedClockExpvars = expvar.NewMap("syncGateway_index_clocks")
}

const (
	KIndexPartitionKey       = "_idxPartitionMap"
	kIndexPrefix             = "_idx"
	kCountKeyFormat          = "_idx_c:%s:count"    // key
	kClockPartitionKeyFormat = "_idx_c:%s:clock-%d" // key, partition index
)

type PartitionStorage struct {
	Uuid  string   `json:"uuid"`
	Index uint16   `json:"index"`
	VbNos []uint16 `json:"vbNos"`
}

type IndexPartitionMap map[uint16]uint16 // Maps vbuckets to index partition value

type IndexPartitions struct {
	VbMap         IndexPartitionMap
	PartitionDefs []PartitionStorage
}

func NewIndexPartitions(partitions []PartitionStorage) *IndexPartitions {

	indexPartitions := &IndexPartitions{
		PartitionDefs: make([]PartitionStorage, len(partitions)),
	}
	copy(indexPartitions.PartitionDefs, partitions)

	indexPartitions.VbMap = make(map[uint16]uint16)
	for _, partition := range partitions {
		for _, vbNo := range partition.VbNos {
			indexPartitions.VbMap[vbNo] = partition.Index
		}
	}
	return indexPartitions
}

// ShardedClock maintains the collection of clock shards (ShardedClockPartitions), and also manages
// the counter for the clock.
type ShardedClock struct {
	baseKey       string                            // key prefix used to build keys for clock component docs
	counter       uint64                            // count value for clock
	countKey      string                            // key used to incr count value
	partitionMap  *IndexPartitions                  // Index partition map
	partitions    map[uint16]*ShardedClockPartition // Clock partitions - one doc written per partition
	bucket        Bucket                            // Bucket used to store clocks
	partitionKeys []string                          // Keys for all partitions.  Convenience to avoid rebuilding set from partitions
}

// ShardedClockPartition manages storage for one clock partition, where a clock partition is a set of
// {vb, seq} values for a subset of vbuckets.
type ShardedClockPartition struct {
	Index  uint16            // Partition Index
	Key    string            // Clock partition document key
	Values map[uint16]uint64 // Clock partition values, indexed by vb
	cas    uint64            // cas value of partition doc
	dirty  bool              // Whether values have been updated since last save/load
}

func NewShardedClockPartition(baseKey string, index uint16) *ShardedClockPartition {
	return &ShardedClockPartition{
		Index:  index,
		Key:    fmt.Sprintf(kClockPartitionKeyFormat, baseKey, index),
		Values: make(map[uint16]uint64),
	}
}

// TODO: replace with something more intelligent than gob encode, to take advantage of known
//       clock structure?
func (scp *ShardedClockPartition) Marshal() ([]byte, error) {
	var output bytes.Buffer
	enc := gob.NewEncoder(&output)
	err := enc.Encode(scp)
	if err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func (scp *ShardedClockPartition) Unmarshal(value []byte) error {
	input := bytes.NewBuffer(value)
	dec := gob.NewDecoder(input)
	err := dec.Decode(&scp)
	if err != nil {
		return err
	}
	return nil
}

func (scp *ShardedClockPartition) SetSequence(vb uint16, seq uint64) {
	scp.Values[vb] = seq
	scp.dirty = true
}

func NewShardedClock(baseKey string, partitions *IndexPartitions, bucket Bucket) *ShardedClock {
	clock := &ShardedClock{
		baseKey:      baseKey,
		partitionMap: partitions,
		countKey:     fmt.Sprintf(kCountKeyFormat, baseKey),
		bucket:       bucket,
	}

	// Initialize partitions
	numPartitions := len(partitions.PartitionDefs)

	clock.partitions = make(map[uint16]*ShardedClockPartition, numPartitions)
	clock.partitionKeys = make([]string, numPartitions)

	return clock

}

func NewShardedClockWithPartitions(baseKey string, partitions *IndexPartitions, bucket Bucket) *ShardedClock {
	clock := &ShardedClock{
		baseKey:      baseKey,
		partitionMap: partitions,
		countKey:     fmt.Sprintf(kCountKeyFormat, baseKey),
		bucket:       bucket,
	}

	// Initialize partitions
	numPartitions := len(partitions.PartitionDefs)

	clock.partitions = make(map[uint16]*ShardedClockPartition, numPartitions)
	clock.partitionKeys = make([]string, numPartitions)

	// Initialize empty clock partitions
	for _, partitionDef := range partitions.PartitionDefs {
		partition := NewShardedClockPartition(baseKey, partitionDef.Index)
		clock.partitions[partitionDef.Index] = partition
		clock.partitionKeys[partitionDef.Index] = partition.Key
	}

	return clock

}

func (s *ShardedClock) Write() (err error) {
	// write all modified partitions to bucket
	shardedClockExpvars.Add("count_write", 1)
	var wg sync.WaitGroup
	for _, partition := range s.partitions {
		if partition.dirty {
			wg.Add(1)
			// TODO: replace with SetBulk when available
			go func(p *ShardedClockPartition) {
				defer wg.Done()
				value, err := p.Marshal()
				casOut, err := s.bucket.WriteCas(p.Key, 0, 0, p.cas, value, sgbucket.Raw)

				if err != nil {
					Warn("Error writing sharded clock partition [%d]:%v", p.Key, err)
					shardedClockExpvars.Add("partition_cas_failures", 1)
					return
				}

				p.cas = casOut
				p.dirty = false
			}(partition)
		}
	}
	wg.Wait()

	// Increment the clock count
	s.counter, err = s.bucket.Incr(s.countKey, 1, 1, 0)
	return err
}

// Loads clock from bucket.  If counter isn't changed, returns false and leaves as-is.
// For newly initialized ShardedClocks (counter=0), this will only happen if there are no
// entries in the bucket for the clock.
func (s *ShardedClock) Load() (isChanged bool, err error) {

	newCounter, err := s.bucket.Incr(s.countKey, 0, 0, 0)
	if err != nil {
		LogTo("DIndex+", "Error getting count:%v", err)
	}
	if newCounter == s.counter {
		return false, nil
	}
	s.counter = newCounter

	resultsMap, err := s.bucket.GetBulkRaw(s.partitionKeys)
	if err != nil {
		Warn("Error retrieving partition keys")
		return false, err
	}
	for _, partitionBytes := range resultsMap {
		if len(partitionBytes) > 0 {
			clockPartition := &ShardedClockPartition{}
			err := clockPartition.Unmarshal(partitionBytes)
			if err != nil {
				Warn("Error unmarshalling partition bytes")
				return false, err
			}
			s.partitions[clockPartition.Index] = clockPartition
		}
	}

	return true, nil
}

func (s *ShardedClock) UpdateAndWrite(updateClock SequenceClock) error {
	for vb, sequence := range updateClock.Value() {
		if sequence > 0 {
			partitionNo := s.partitionMap.VbMap[uint16(vb)]
			if s.partitions[partitionNo] == nil {
				shardedClockExpvars.Add("count_update_partition_not_found", 1)
				err := s.initPartition(partitionNo)
				if err != nil {
					return err
				}
			}
			s.partitions[partitionNo].SetSequence(uint16(vb), sequence)
		}
	}
	return s.Write()
}

func (s *ShardedClock) initPartition(partitionNo uint16) error {

	// initialize the partition
	s.partitions[partitionNo] = NewShardedClockPartition(s.baseKey, partitionNo)
	partitionKey := fmt.Sprintf(kClockPartitionKeyFormat, s.baseKey, partitionNo)

	partitionBytes, casOut, err := s.bucket.GetRaw(partitionKey)
	if err == nil {
		unmarshalErr := s.partitions[partitionNo].Unmarshal(partitionBytes)
		s.partitions[partitionNo].cas = casOut
		if unmarshalErr != nil {
			return unmarshalErr
		}
	}
	s.partitionKeys[partitionNo] = partitionKey
	return nil
}

func (s *ShardedClock) AsClock() *SequenceClockImpl {
	clock := NewSequenceClockImpl()
	for _, partition := range s.partitions {
		for vb, seq := range partition.Values {
			clock.SetSequence(vb, seq)
		}
	}
	return clock
}

// Count retrieval - utility for use outside of the context of a sharded clock.
func LoadClockCounter(baseKey string, bucket Bucket) (uint64, error) {
	countKey := fmt.Sprintf(kCountKeyFormat, baseKey)
	return bucket.Incr(countKey, 0, 0, 0)
}
