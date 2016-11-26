//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"encoding/binary"

	"github.com/couchbase/sync_gateway/base"
)

// Implementation of ChannelStorage that stores entries as an append-based list of
// full log entries
type DenseStorage struct {
	bucket           base.Bucket                       // Index bucket
	channelName      string                            // Channel name
	partitions       *base.IndexPartitions             // Partition assignment map
	partitionStorage *map[uint16]DensePartitionStorage // PartitionStorage for this channel
}

func NewDenseStorage(bucket base.Bucket, channelName string, partitions *base.IndexPartitions) *DenseStorage {

	storage := &DenseStorage{
		bucket:      bucket,
		channelName: channelName,
		partitions:  partitions,
	}

	// Maximum theoretical block cache capacity is 1024 - if this index writer were indexing every vbucket,
	// and each vbucket sequence was in a different block.  More common case would be this index writer
	// having at most 512 vbuckets, and most of those vbuckets working the same block index per partition (16 vbs per
	// partition) == 32 blocks.  Setting default to 50 to handle any temporary spikes.
	/* var err error
	storage.indexBlockCache, err = base.NewLRUCache(50)
	if err != nil {
		base.LogFatal("Error creating LRU cache for index blocks: %v", err)
	}
	*/
	return storage
}

// Adds the provided set of entries to storage.  Batches by partition, then invokes each partition update
func (ds *DenseStorage) AddEntrySet(entries []*LogEntry) (clockUpdates base.SequenceClock, err error) {

	/*
		partitionSets := make(map[uint16][]*LogEntry)
		for _, entry := range entries {

		}
	*/
	// group entries into partitions
	// call partitionstorage[partition].AddEntrySet for each set, get partition clock back and add to clockUpdates
	return nil, nil
}

// TODO: For channel backfill, need to guarantee that []*LogEntry is ordered identically for repeated requests

func (ds *DenseStorage) GetChanges(fromSeq base.SequenceClock, channelClock base.SequenceClock) ([]*LogEntry, error) {
	// Identify changed partitions based on clock
	// call partitionStorage.GetChanges(from partition clock, to partition clock) for each partition to get entries for that partition
	//
	return nil, nil
}

func (ds *DenseStorage) StoresLogEntries() bool {
	return false
}

func (ds *DenseStorage) WriteLogEntry(entry *LogEntry) error {
	// no-op
	return nil
}

type DensePartitionStorage struct {
	partitionIndex      PartitionIndex // Mapping from clock to index block key
	partitionBlockCache *base.LRUCache // Cache of recently used index blocks
}

type PartitionIndex []PartitionIndexEntry
type PartitionClock map[uint16]uint64

type PartitionIndexEntry struct {
	partitionStart PartitionClock
	blockId        string
}

// DenseBlock has the following binary-encoded format
//  | Name               | Size                  | Description                                     |
//  |--------------------|-----------------------|-------------------------------------------------|
//  | blockIndexCount    | 2 bytes               | Number of entries in block                      |
//  | []BlockIndexEntry  | 12 bytes/entry        | List of vb, seq and length for entries in block |
//  | []BlockEntry       | variable length/entry | Key, rev id and flags for each entry            |
//  ------------------------------------------------------------------------------------------------

// BlockIndexEntry format
//  | Name      | Size     | Description                      |
//  |-----------|----------|----------------------------------|
//  | vbno      | 2 bytes  | Vbucket number                   |
//  | sequence  | 8 bytes  | Vbucket seq                      |
//  | entryLen  | 2 bytes  | Length of associated block entry |
//  -----------------------------------------------------------

// BlockEntry format
//  | Name      | Size     | Description                      |
//  |-----------|----------|----------------------------------|
//  | flags     | 1 byte   | Flags (deleted, removed, etc)    |
//  | keylen    | 2 bytes  | Length of key                    |
//  | key       | n bytes  | Key                              |
//  | revid     | n bytes  | Revision id                      |
//  -----------------------------------------------------------

const ENTRY_STATIC_LEN = 15

type DenseBlock struct {
	key   string // Key of block document in the index bucket
	value []byte // Raw document value
	cas   uint64 // Document cas
}

type DenseBlockData struct {
	entryCount     uint16
	indexEntryData []byte
	entryData      []byte
	indexEntries   []DenseBlockIndexEntry
	entries        []DenseBlockEntry
}

type DenseBlockIndexEntry struct {
	vbno     uint16
	sequence uint64
	entryLen uint16
}

type DenseBlockEntry struct {
	flags  uint8
	keylen uint16
	key    []byte
	revId  []byte
}

func (d *DenseBlock) AddEntry(entry *LogEntry) error {
	// increment entryCount
	// Create DenseBlockIndexEntry, append to indexEntries
	// Create DenseBlockEntry, append to entries
	return nil
}

func (d *DenseBlock) Key() string {
	return d.key
}

func (d *DenseBlockData) Marshal() ([]byte, error) {
	data := make([]byte, d.size())
	binary.BigEndian.PutUint16(data[0:2], d.entryCount)
	copy(data[2:2+len(d.indexEntryData)], d.indexEntryData)
	copy(data[2+len(d.indexEntryData):], d.entryData)
	return data, nil
}

func (d *DenseBlockData) Unmarshal(value []byte) error {
	d.entryCount = binary.BigEndian.Uint16(value[0:])
	d.indexEntryData = value[2 : 12*d.entryCount]
	d.entryData = value[12*d.entryCount:]
	return nil
}

func (d *DenseBlock) Cas() uint64 {
	return d.cas
}

func (d *DenseBlock) SetCas(cas uint64) {
	d.cas = cas
}

func (d *DenseBlock) GetEntries(vbNo uint16, fromSeq uint64, toSeq uint64, includeKeys bool) (entries []*LogEntry, keySet []string) {
	return nil, nil
}

func (d *DenseBlock) GetAllEntries() []*LogEntry {
	return nil
}

func (d *DenseBlockData) size() int {
	// 2 bytes for entryCount, indexEntries, entries
	return 2 + len(d.indexEntryData) + len(d.entryData)
}

/*
func (e *DenseBlockEntry) encode() []byte {

	data := make([]byte, e.Size())
	pos := 0
	// Write fixed length data
	binary.BigEndian.PutUint16(data[0:2], e.vbno)
	binary.BigEndian.PutUint64(data[2:10], e.sequence)
	data[11] = byte(e.flags)
	binary.BigEndian.PutUint16(data[11:13], len(e.docId))
	binary.BigEndian.PutUint16(data[13:15], len(e.revId))

	// Write variable length data
	docIdEnd := 15 + len(e.docId)
	copy(data[15:docIdEnd], e.docId)
	copy(data[docIdEnd:docIdEnd+len(e.revId)], e.revId)
	return data
}

func UnmarshalDenseBlockEntry(value []byte) (*DenseBlockEntry, error) {

}
*/
