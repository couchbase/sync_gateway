package base

import (
	"encoding/json"
	"expvar"
	"fmt"
	"math"
	"sort"
	"sync"
)

var shardedClockExpvars *expvar.Map

func init() {
	shardedClockExpvars = expvar.NewMap("syncGateway_index_clocks")
}

const (
	KIndexPartitionKey       = "_idxPartitionMap"
	KIndexPrefix             = "_idx"
	kCountKeyFormat          = "_idx_c:%s:count"    // key
	kClockPartitionKeyFormat = "_idx_c:%s:clock-%d" // key, partition index
	KPrincipalCountKeyFormat = "_idx_p_count:%s"    // key for principal count
	KPrincipalCountKeyPrefix = "_idx_p_count:"      // key prefix for principal count
	KTotalPrincipalCountKey  = "_idx_p_count_all"   // key for overall principal count
)

const (
	_ = iota
	seq_size_uint16
	seq_size_uint32
	seq_size_uint48
	seq_size_uint64
)

type PartitionStorage struct {
	Uuid  string   `json:"uuid"`
	Index uint16   `json:"index"`
	VbNos []uint16 `json:"vbNos"`
}

type PartitionStorageSet []PartitionStorage

// Sorts the PartitionStorageSet by Uuid
func (c PartitionStorageSet) Sort() {
	sort.Sort(c)
}

// Implementation of sort.Interface
func (c PartitionStorageSet) Len() int           { return len(c) }
func (c PartitionStorageSet) Less(i, j int) bool { return c[i].Uuid < c[j].Uuid }
func (c PartitionStorageSet) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func (c PartitionStorageSet) String() string {
	result := ""
	c.Sort()
	for _, partition := range c {
		result = fmt.Sprintf("%s[%d - %v]", result, partition.Index, partition.VbNos)
	}
	return result
}

type IndexPartitionMap []uint16      // Maps vbuckets to index partition value
type VbPositionMap map[uint16]uint64 // Map from vbucket to position within partition.  Stored as uint64 to avoid cast during arithmetic

type IndexPartitions struct {
	PartitionDefs  PartitionStorageSet      // Partition definitions, as stored in bucket _idxPartitionMap
	VbMap          IndexPartitionMap        // Map from vbucket to partition
	VbPositionMaps map[uint16]VbPositionMap // VBPositionMaps, keyed by partition
}

func (i IndexPartitions) PartitionCount() int {
	return len(i.PartitionDefs)
}

// Returns the partition the vb is assigned to
func (i *IndexPartitions) PartitionForVb(vbNo uint16) uint16 {
	return i.VbMap[vbNo]
}

func NewIndexPartitions(partitions PartitionStorageSet) *IndexPartitions {

	indexPartitions := &IndexPartitions{
		PartitionDefs: make(PartitionStorageSet, len(partitions)),
	}
	copy(indexPartitions.PartitionDefs, partitions)

	indexPartitions.VbMap = make([]uint16, 1024)
	indexPartitions.VbPositionMaps = make(map[uint16]VbPositionMap)

	for _, partition := range partitions {
		vbPosMap := make(VbPositionMap)
		for vbIndex, vbNo := range partition.VbNos {
			indexPartitions.VbMap[vbNo] = partition.Index
			vbPosMap[vbNo] = uint64(vbIndex)
		}
		indexPartitions.VbPositionMaps[partition.Index] = vbPosMap
	}
	return indexPartitions
}

// Priority of a journal message
type CompareResult int

const (
	CompareLessThan CompareResult = iota - 1
	CompareEquals
	CompareGreaterThan
)

// VbSeq stores a vbucket number and vbucket sequence pair
type VbSeq struct {
	Vb  uint16
	Seq uint64
}

// Updates to the other sequence value if empty (seq=0), or the other value compares less than v
func (v *VbSeq) UpdateIfEarlier(other VbSeq) bool {
	if v.Seq == 0 || CompareVbSequence(other, *v) == CompareLessThan {
		v.Vb = other.Vb
		v.Seq = other.Seq
		return true
	}
	return false
}

func (v VbSeq) LessThanOrEqualsClock(clock SequenceClock) bool {
	if v.Seq == 0 {
		return false
	}

	if v.Seq <= clock.GetSequence(v.Vb) {
		return true
	}
	return false

}

func (v VbSeq) CompareTo(vb uint16, seq uint64) CompareResult {
	return CompareVbAndSequence(v.Vb, v.Seq, vb, seq)
}

// Compares based on vbno, then sequence.  Returns 0 if identical, 1 if s1 > s2, -1 if s1 < s2
func CompareVbSequence(s1, s2 VbSeq) CompareResult {
	return CompareVbAndSequence(s1.Vb, s1.Seq, s2.Vb, s2.Seq)
}

// Compares based on vbno, then sequence.  Returns 0 if identical, 1 if s1 > s2, -1 if s1 < s2
func CompareVbAndSequence(vb1 uint16, s1 uint64, vb2 uint16, s2 uint64) CompareResult {
	if vb1 < vb2 {
		return -1
	}
	if vb1 > vb2 {
		return 1
	}
	// Vbno equal, compare sequences
	if s1 < s2 {
		return -1
	}
	if s1 > s2 {
		return 1
	}
	return 0
}

// ShardedClock is a full clock for the bucket.  ShardedClock manages the collection of clock shards (ShardedClockPartitions),
// and also manages the counter for the clock.
type ShardedClock struct {
	baseKey       string                   // key prefix used to build keys for clock component docs
	counter       uint64                   // count value for clock to minimize clock reads
	countKey      string                   // key used to incr count value
	partitionMap  *IndexPartitions         // Index partition map
	partitions    []*ShardedClockPartition // Clock partitions - one doc written per partition
	bucket        Bucket                   // Bucket used to store clocks
	partitionKeys []string                 // Keys for all partitions.  Convenience to avoid rebuilding set from partitions
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

	clock.partitions = make([]*ShardedClockPartition, numPartitions)
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

	clock.partitions = make([]*ShardedClockPartition, numPartitions)
	clock.partitionKeys = make([]string, numPartitions)

	// Initialize empty clock partitions
	for _, partitionDef := range partitions.PartitionDefs {
		partition := NewShardedClockPartition(baseKey, partitionDef.Index, partitions.PartitionDefs[partitionDef.Index].VbNos)
		clock.partitions[partitionDef.Index] = partition
		clock.partitionKeys[partitionDef.Index] = partition.Key
	}

	return clock

}

/*
func (s *ShardedClock) write() (err error) {
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
					Warn("Error writing sharded clock partition key: %v.  Error: %v, p.cas: %v, casOut: %v", p.Key, err, p.cas, casOut)
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
*/

// Loads clock from bucket.  If counter isn't changed, returns false and leaves as-is.
// For newly initialized ShardedClocks (counter=0), this will only happen if there are no
// entries in the bucket for the clock.
func (s *ShardedClock) Load() (isChanged bool, err error) {

	newCounter, err := s.bucket.Incr(s.countKey, 0, 0, 0)
	if err != nil {
		Warn("Error getting count for %s:%v", s.countKey, err)
		LogTo("DIndex+", "Error getting count:%v", err)
		return false, err
	}
	if newCounter == s.counter {
		return false, nil
	}
	s.counter = newCounter

	resultsMap, err := s.bucket.GetBulkRaw(s.partitionKeys)
	if err != nil {
		Warn("Error retrieving partition keys:%v", err)
		return false, err
	}
	for key, partitionBytes := range resultsMap {
		if len(partitionBytes) > 0 {
			clockPartition := NewShardedClockPartitionForBytes(key, partitionBytes, s.partitionMap)
			s.partitions[clockPartition.GetIndex()] = clockPartition
		}
	}

	return true, nil
}

func (s *ShardedClock) GetSequence(vbNo uint16) (vbSequence uint64) {
	partitionNo := s.partitionMap.VbMap[vbNo]
	clockPartition := s.partitions[partitionNo]
	if clockPartition != nil {
		return clockPartition.GetSequence(vbNo)
	} else {
		return 0
	}
}

// Update and write a sharded clock with the specified values.
func (s *ShardedClock) UpdateAndWrite(updates map[uint16]uint64) (err error) {

	// Build set of sequence updates by partition
	// Future optimization: have method accept sequences already grouped by partition - potentially
	// in a clock implementation
	partitionSequences := make(map[uint16][]VbSeq)

	for vb, sequence := range updates {
		partitionNo := s.partitionMap.VbMap[uint16(vb)]
		_, ok := partitionSequences[partitionNo]
		if !ok {
			partitionSequences[partitionNo] = make([]VbSeq, 0)
		}
		partitionSequences[partitionNo] = append(partitionSequences[partitionNo], VbSeq{Vb: uint16(vb), Seq: sequence})

	}

	if len(partitionSequences) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	// Update and cas write each changed partition
	for partitionNo, sequences := range partitionSequences {
		wg.Add(1)
		// Initialize partition if needed
		if s.partitions[partitionNo] == nil {
			shardedClockExpvars.Add("count_update_partition_not_found", 1)
			err = s.initPartition(partitionNo)
			if err != nil {
				return err
			}
		}

		// Update partitions in parallel goroutines.
		// Future optimization: implement a bulk set based version of WriteCasRaw
		go func(p *ShardedClockPartition, seqs []VbSeq) {
			defer wg.Done()
			// Apply sequences to clock partition
			for _, vbSeq := range seqs {
				p.SetSequence(vbSeq.Vb, vbSeq.Seq)
			}
			value, err := p.Marshal()

			// Cas Write - reapplies sequences updates on cas failure/retry
			casOut, err := WriteCasRaw(s.bucket, p.Key, value, p.cas, 0, func(value []byte) (updatedValue []byte, err error) {
				// Note: The following is invoked upon cas failure - may be called multiple times
				err = p.Unmarshal(value)
				if err != nil {
					Warn("Error unmarshalling clock during update", err)
					return nil, err
				}
				// Reapply sequences to partition
				for _, vbSeq := range seqs {
					p.SetSequence(vbSeq.Vb, vbSeq.Seq)
				}
				return p.Marshal()
			})
			if err != nil {
				Warn("Error writing sharded clock partition [%d]:%v", p.Key, err)
				shardedClockExpvars.Add("partition_cas_failures", 1)
				return
			}
			p.cas = casOut
		}(s.partitions[partitionNo], sequences)

	}
	wg.Wait()

	// Increment the clock count
	s.counter, err = s.bucket.Incr(s.countKey, 1, 1, 0)

	return err
}

func (s *ShardedClock) initPartition(partitionNo uint16) error {

	// initialize the partition
	s.partitions[partitionNo] = NewShardedClockPartition(s.baseKey, partitionNo, s.partitionMap.PartitionDefs[partitionNo].VbNos)
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
		partition.AddToClock(clock)
	}
	return clock
}

const kShardedClockMetaSize = 3

var kClockMaxSequences = []uint64{uint64(0), uint64(math.MaxUint16), uint64(math.MaxUint32), uint64(math.MaxUint32 << 16), math.MaxUint64}

// ShardedClockPartition manages storage for one clock partition, where a clock partition is a set of
// {vb, seq} values for a subset of vbuckets.
// Modifying clock values and metadata is done directly to the []byte storage, to avoid marshal/unmarshal overhead.
// SeqSize defines how many bytes are used to store each clock value.  It is initialized at 2 bytes/value (uint16 capacity), but gets increased
// via the resize() operation when a call to SetSequence would exceed the current capacity.
// Structure of []byte:
//    index : 2 bytes.  Partition Index, as uint16.
//    seqSize: 1 byte.  Sequence Size.  Supports values 1-4, where max sequence for that size is defined in kClockMaxSequences[size]
//    vbucket sequences: 2-8 bytes per sequence (depending on seqSize)
type ShardedClockPartition struct {
	Key            string            // Clock partition document key
	value          []byte            // Clock partition values
	cas            uint64            // cas value of partition doc
	vbucketOffsets map[uint16]uint16 // Offset for vbuckets
	dirty          bool              // Whether values have been updated since last save/load
}

func NewShardedClockPartition(baseKey string, index uint16, vbuckets []uint16) *ShardedClockPartition {

	initialSeqSize := seq_size_uint16

	p := &ShardedClockPartition{
		Key:   fmt.Sprintf(kClockPartitionKeyFormat, baseKey, index),
		value: make([]byte, kShardedClockMetaSize+len(vbuckets)*initialSeqSize*2),
	}
	p.SetSeqSize(uint8(initialSeqSize))
	p.SetIndex(index)
	p.Init(vbuckets)
	return p
}

func NewShardedClockPartitionForBytes(key string, bytes []byte, partitions *IndexPartitions) *ShardedClockPartition {
	p := &ShardedClockPartition{
		Key:   key,
		value: bytes,
	}
	// load the index from the incoming bytes, and use it to get the vb set for this partition
	partitionIndex := p.GetIndex()
	vbuckets := partitions.PartitionDefs[partitionIndex].VbNos
	p.Init(vbuckets)
	return p
}

// Initializes vbucketOffsets
func (p *ShardedClockPartition) Init(vbuckets []uint16) {

	p.vbucketOffsets = make(map[uint16]uint16, len(vbuckets)-1)
	for index, vbNo := range vbuckets {
		p.vbucketOffsets[vbNo] = uint16(kShardedClockMetaSize) + 2*uint16(p.GetSeqSize())*uint16(index) // metadata + (2*size) bytes per vbucket
	}
}

func (p *ShardedClockPartition) Marshal() ([]byte, error) {
	return p.value, nil
}

func (p *ShardedClockPartition) Unmarshal(value []byte) error {
	p.value = make([]byte, len(value))
	copy(p.value, value)
	return nil
}

// Sets sequence.  Uses big endian byte ordering.
func (p *ShardedClockPartition) SetSequence(vb uint16, seq uint64) {

	if seq > kClockMaxSequences[p.GetSeqSize()] {
		p.resize(seq)
	}

	p.setSequenceForOffset(p.vbucketOffsets[vb], p.GetSeqSize(), seq)
}

func (p *ShardedClockPartition) setSequenceForOffset(offset uint16, size uint8, seq uint64) {

	switch size {
	case seq_size_uint16:
		p.setSequenceUint16(offset, seq)
	case seq_size_uint32:
		p.setSequenceUint32(offset, seq)
	case seq_size_uint48:
		p.setSequenceUint48(offset, seq)
	case seq_size_uint64:
		p.setSequenceUint64(offset, seq)
	}

	p.dirty = true

}

func (p *ShardedClockPartition) setSequenceUint16(offset uint16, seq uint64) {

	p.value[offset] = byte(seq >> 8)
	p.value[offset+1] = byte(seq)
}

func (p *ShardedClockPartition) setSequenceUint32(offset uint16, seq uint64) {

	p.value[offset] = byte(seq >> 24)
	p.value[offset+1] = byte(seq >> 16)
	p.value[offset+2] = byte(seq >> 8)
	p.value[offset+3] = byte(seq)
}

func (p *ShardedClockPartition) setSequenceUint48(offset uint16, seq uint64) {

	p.value[offset] = byte(seq >> 40)
	p.value[offset+1] = byte(seq >> 32)
	p.value[offset+2] = byte(seq >> 24)
	p.value[offset+3] = byte(seq >> 16)
	p.value[offset+4] = byte(seq >> 8)
	p.value[offset+5] = byte(seq)
}

func (p *ShardedClockPartition) setSequenceUint64(offset uint16, seq uint64) {

	p.value[offset] = byte(seq >> 56)
	p.value[offset+1] = byte(seq >> 48)
	p.value[offset+2] = byte(seq >> 40)
	p.value[offset+3] = byte(seq >> 32)
	p.value[offset+4] = byte(seq >> 24)
	p.value[offset+5] = byte(seq >> 16)
	p.value[offset+6] = byte(seq >> 8)
	p.value[offset+7] = byte(seq)
}

func (p *ShardedClockPartition) GetSequence(vb uint16) (seq uint64) {
	return p.getSequenceForOffset(p.vbucketOffsets[vb], p.GetSeqSize())
}

func (p *ShardedClockPartition) getSequenceForOffset(offset uint16, size uint8) (seq uint64) {

	switch size {
	case seq_size_uint16:
		return p.getSequenceUint16(offset)
	case seq_size_uint32:
		return p.getSequenceUint32(offset)
	case seq_size_uint48:
		return p.getSequenceUint48(offset)
	case seq_size_uint64:
		return p.getSequenceUint64(offset)
	default:
		return p.getSequenceUint64(offset)
	}

}

func (p *ShardedClockPartition) getSequenceUint16(offset uint16) (seq uint64) {

	return uint64(p.value[offset+1]) |
		uint64(p.value[offset])<<8
}

func (p *ShardedClockPartition) getSequenceUint32(offset uint16) (seq uint64) {

	return uint64(p.value[offset+3]) |
		uint64(p.value[offset+2])<<8 |
		uint64(p.value[offset+1])<<16 |
		uint64(p.value[offset])<<24
}

func (p *ShardedClockPartition) getSequenceUint48(offset uint16) (seq uint64) {

	seq = uint64(p.value[offset+5]) |
		uint64(p.value[offset+4])<<8 |
		uint64(p.value[offset+3])<<16 |
		uint64(p.value[offset+2])<<24 |
		uint64(p.value[offset+1])<<32 |
		uint64(p.value[offset])<<40
	return seq
}

func (p *ShardedClockPartition) getSequenceUint64(offset uint16) (seq uint64) {

	return uint64(p.value[offset+7]) |
		uint64(p.value[offset+6])<<8 |
		uint64(p.value[offset+5])<<16 |
		uint64(p.value[offset+4])<<24 |
		uint64(p.value[offset+3])<<32 |
		uint64(p.value[offset+2])<<40 |
		uint64(p.value[offset+1])<<48 |
		uint64(p.value[offset])<<56
}

func (p *ShardedClockPartition) AddToClock(clock SequenceClock) error {

	for vb := range p.vbucketOffsets {
		clock.SetSequence(vb, p.GetSequence(vb))
	}
	return nil
}

func (p *ShardedClockPartition) GetIndex() uint16 {
	return uint16(p.value[1]) | uint16(p.value[0])<<8
}

func (p *ShardedClockPartition) SetIndex(index uint16) {
	p.value[0] = byte(index >> 8)
	p.value[1] = byte(index)
}

// Sequence Size - used as variable-length encoding, but for all sequences in the partition.
func (p *ShardedClockPartition) GetSeqSize() uint8 {
	return uint8(p.value[2])
}

func (p *ShardedClockPartition) SetSeqSize(size uint8) {
	p.value[2] = byte(size)
}

// resize increases the number of bytes used to store each sequence in the clock partition.  Allows us to minimize document
// size when sequence numbers are smaller
func (p *ShardedClockPartition) resize(seq uint64) {

	// Figure out what the new size should be
	newSize := uint8(seq_size_uint64)
	for i := seq_size_uint32; i <= seq_size_uint64; i++ {
		if seq < kClockMaxSequences[i] {
			newSize = uint8(i)
			break
		}
	}

	// Increase the size of []byte to handle the new sizes
	p.value = append(p.value, make([]byte, uint16(len(p.vbucketOffsets))*2*uint16((newSize-p.GetSeqSize())))...)

	oldSize := p.GetSeqSize()

	// Shift and resize existing entries
	for index := len(p.vbucketOffsets) - 1; index >= 0; index-- {
		oldOffset := uint16(kShardedClockMetaSize) + 2*uint16(oldSize)*uint16(index)
		newOffset := uint16(kShardedClockMetaSize) + 2*uint16(newSize)*uint16(index)
		seq := p.getSequenceForOffset(oldOffset, oldSize)
		p.setSequenceForOffset(oldOffset, oldSize, uint64(0))
		p.setSequenceForOffset(newOffset, newSize, seq)
	}
	// Update vbucket offset map
	for vb, offset := range p.vbucketOffsets {
		p.vbucketOffsets[vb] = (offset-kShardedClockMetaSize)*uint16(newSize)/uint16(oldSize) + kShardedClockMetaSize
	}

	p.SetSeqSize(newSize)

}

// Count retrieval - utility for use outside of the context of a sharded clock.
func LoadClockCounter(baseKey string, bucket Bucket) (uint64, error) {
	countKey := fmt.Sprintf(kCountKeyFormat, baseKey)
	return bucket.Incr(countKey, 0, 0, 0)
}

// Index partitions for unit tests
func SeedTestPartitionMap(bucket Bucket, numPartitions uint16) (PartitionStorageSet, error) {
	maxVbNo := uint16(1024)
	partitionDefs := make(PartitionStorageSet, numPartitions)
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

	// Persist to bucket
	value, err := json.Marshal(partitionDefs)
	if err != nil {
		return nil, err
	}
	bucket.SetRaw(KIndexPartitionKey, 0, value)
	return partitionDefs, nil
}
