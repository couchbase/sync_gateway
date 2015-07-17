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
	"bytes"
	"encoding/gob"
	"errors"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

var byteCacheBlockCapacity = uint64(10000)

type IndexBlock interface {
	Key() string
	Marshal() ([]byte, error)
	Unmarshal(value []byte, cas uint64) error
	AddEntry(entry kvIndexEntry) error
	AddEntrySet(entries []kvIndexEntry) error
	Cas() uint64
	SetCas(cas uint64)
	GetAllEntries() []kvIndexEntry
}

func NewIndexBlock(channelName string, sequence uint64, partition uint16) IndexBlock {
	return NewBitFlagBlock(channelName, sequence, partition)
}

func GenerateBlockKey(channelName string, sequence uint64, partition uint16) string {
	return GenerateBitFlagBlockKey(channelName, sequence, partition)
}

// Determine the cache block key for a sequence
func GenerateBitFlagBlockKey(channelName string, minSequence uint64, partition uint16) string {
	base.LogTo("DCache", "block index for minSequence %d is %d", minSequence, uint16(minSequence/byteCacheBlockCapacity))
	index := uint16(minSequence / byteCacheBlockCapacity)
	return getIndexBlockKey(channelName, index, partition)

}

type BitFlagBlock struct {
	key    string           // DocID for the cache block doc
	value  BitFlagBlockData // Raw document value
	cas    uint64           // cas value of block in database
	expiry time.Time        // Expiry - used for compact
}

type BitFlagBlockData struct {
	MinSequence uint64            // Starting sequence
	Entries     map[uint16][]byte // Contents of the cache block doc
}

/*
Note: Considered trying to store 4 sequences (presence/removed flags) per byte to optimize space, but don't feel it's worth
      the effort at this stage.  Could revisit as an optimization
type BitFlags byte

func (bf *BitFlags) Set(n uint8) error {
	if n > 7 {
		return errors.New("out of range")
	}
	*bf = (*bf) | (1 << n)
	return nil
}

func (bf *BitFlags) Unset(n uint8) error {
	if n > 7 {
		return errors.New("out of range")
	}
	*bf = (*bf) &^ (1 << n)
	return nil
}

func (d *BitFlagBlockData) addEntry(vbNo uint16, sequence uint64, removal bool) {
	localIndex := sequence - d.MinSequence
	byteIndex := uint64(localIndex / 4)
	bytePosition := localIndex%4 - 1
	bitFlags := d.Entries[vbNo][byteIndex]
	// presence is at flag 2 * bytePosition + 1
	presencePos := uint8(2*bytePosition + 1)
	// removal is at flag position 2 * bytePosition
	removalPos := uint8(2 * bytePosition)
	bitFlags.Set(presencePos)
	if removal {
		bitFlags.Set(removalPos)
	} else {
		bitFlags.Unset(removalPos)
	}
	return
*/

func (d *BitFlagBlockData) hasEntry(vbNo uint16, sequence uint64) (found, isRemoval bool) {
	return false, false
}

func NewBitFlagBlock(channelName string, minSequence uint64, partition uint16) IndexBlock {

	key := GenerateBitFlagBlockKey(channelName, minSequence, partition)

	cacheBlock := &BitFlagBlock{
		key: key,
	}
	// Initialize the entry map
	cacheBlock.value = BitFlagBlockData{
		MinSequence: minSequence,
		Entries:     make(map[uint16][]byte),
	}

	return cacheBlock
}

func (b *BitFlagBlock) Key() string {
	return b.key
}

func (b *BitFlagBlock) Cas() uint64 {
	return b.cas
}

func (b *BitFlagBlock) SetCas(cas uint64) {
	b.cas = cas
}

func (b *BitFlagBlock) Marshal() ([]byte, error) {
	var output bytes.Buffer
	enc := gob.NewEncoder(&output)
	err := enc.Encode(b.value)
	if err != nil {
		return nil, err
	}

	return output.Bytes(), nil
}

func (b *BitFlagBlock) Unmarshal(value []byte, cas uint64) error {

	input := bytes.NewBuffer(value)
	dec := gob.NewDecoder(input)
	err := dec.Decode(&b.value)
	if err != nil {
		return err
	}

	b.SetCas(cas)
	return nil
}

func (b *BitFlagBlock) AddEntry(entry kvIndexEntry) error {
	if _, ok := b.value.Entries[entry.vbNo]; !ok {
		b.value.Entries[entry.vbNo] = make([]byte, byteCacheBlockCapacity+kSequenceOffsetLength)
	}

	index := b.getIndexForSequence(entry.sequence)
	if index < 0 || index >= uint64(len(b.value.Entries[entry.vbNo])) {
		return errors.New("Sequence out of range of block")
	}
	if entry.removal {
		b.value.Entries[entry.vbNo][index] = byte(2)
	} else {
		b.value.Entries[entry.vbNo][index] = byte(1)
	}
	return nil
}

func (b *BitFlagBlock) AddEntrySet(entries []kvIndexEntry) error {
	return nil
}

func (b *BitFlagBlock) GetAllEntries() []kvIndexEntry {
	results := make([]kvIndexEntry, 0)
	// Iterate over all vbuckets, returning entries for each.
	for vbNo, sequences := range b.value.Entries {
		for index, entry := range sequences {
			if entry != byte(0) {
				removed := entry == byte(2)
				newEntry := kvIndexEntry{
					vbNo:     vbNo,
					sequence: b.value.MinSequence + uint64(index),
					removal:  removed,
				}
				results = append(results, newEntry)
			}

		}
	}
	return results
}

func (b *BitFlagBlock) getIndexForSequence(sequence uint64) uint64 {
	return sequence - b.value.MinSequence + kSequenceOffsetLength
}

// Determine the cache block index for a sequence
func (k *BitFlagBlock) getBlockIndex(sequence uint64) uint16 {
	return uint16(sequence / byteCacheBlockCapacity)
}
