//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"errors"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

var byteCacheBlockCapacity = uint64(10000)

type IndexBlock interface {
	Key() string
	Marshal() []byte
	Unmarshal(value []byte, cas uint64) error
	AddEntry(entry kvIndexEntry) error
	AddEntrySet(entries []kvIndexEntry) error
	Cas() uint64
}

func NewIndexBlock(channelName string, sequence uint64, partition uint16) IndexBlock {
	return NewBitFlagBlock(channelName, sequence, partition)
}

func GenerateBlockKey(channelName string, sequence uint64, partition uint16) string {
	return GenerateBitFlagBlockKey(channelName, sequence, partition)
}

// Determine the cache block key for a sequence
func GenerateBitFlagBlockKey(channelName string, sequence uint64, partition uint16) string {
	base.LogTo("DCache", "block index for sequence %d is %d", sequence, uint16(sequence/byteCacheBlockCapacity))
	index := uint16(sequence / byteCacheBlockCapacity)
	return getIndexBlockKey(channelName, index, partition)

}

type BitFlagBlock struct {
	key         string            // DocID for the cache block doc
	value       []byte            // Raw document value
	values      map[uint16][]byte // Contents of the cache block doc
	minSequence uint64            // Starting sequence
	cas         uint64            // cas value of block in database
	expiry      time.Time         // Expiry - used for compact
}

func NewBitFlagBlock(channelName string, sequence uint64, partition uint16) IndexBlock {

	key := GenerateBitFlagBlockKey(channelName, sequence, partition)

	// Initialize the byte[] bucket
	cacheValue := make([]byte, byteCacheBlockCapacity+kSequenceOffsetLength)
	sequenceOffset := uint64(sequence/byteCacheBlockCapacity) * byteCacheBlockCapacity
	// Write the sequence Offset first (8 bytes for uint64)
	/* disabling until we actually need the offset
	offSet := intToBytes(sequenceOffset)
	copy(cacheValue[0:8], offset)
	*/
	// Grow the buffer by kvCacheCapacity

	cacheBlock := &BitFlagBlock{
		key:         key,
		value:       cacheValue,
		minSequence: sequenceOffset,
	}
	return cacheBlock
}

func (b *BitFlagBlock) Key() string {
	return b.key
}

func (b *BitFlagBlock) Cas() uint64 {
	return b.cas
}

func (b *BitFlagBlock) Marshal() []byte {
	return make([]byte, 0)
}

func (b *BitFlagBlock) Unmarshal(value []byte, cas uint64) error {
	return nil
}

func (b *BitFlagBlock) AddEntry(entry kvIndexEntry) error {
	// Byte for sequence:
	//   0 : sequence is not in channel
	//   1 : sequence is in channel
	//   2 : sequence triggers removal from channel
	index := b.getIndexForSequence(entry.sequence)
	if index < 0 || index >= uint64(len(b.value)) {
		base.LogTo("DCache", "CACHE ERROR CACHE ERROR KLAXON KLAXON")
		return errors.New("Sequence out of range of block")
	}
	if entry.removal {
		b.value[index] = 2
	} else {
		b.value[index] = 1
	}
	return nil
}

func (b *BitFlagBlock) AddEntrySet(entries []kvIndexEntry) error {
	return nil
}

func (b *BitFlagBlock) getIndexForSequence(sequence uint64) uint64 {
	return sequence - b.minSequence + kSequenceOffsetLength
}

// Determine the cache block index for a sequence
func (k *BitFlagBlock) getBlockIndex(sequence uint64) uint16 {
	return uint16(sequence / byteCacheBlockCapacity)
}
