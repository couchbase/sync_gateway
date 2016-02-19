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
	"encoding/binary"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
)

const (
	kIndexPrefix          = "_idx"
	maxCacheUpdate        = 2000
	minCacheUpdate        = 1
	kPollFrequency        = 500
	maxUnmarshalProcesses = 16
)

// kvChangeIndex is the index-based implementation of the change cache API.  Functionality is split
// into reader and writer functionality.  A single index partition definition is shared between reader and
// writer, as this partition definition is immutable and defined at index creation time
type kvChangeIndex struct {
	context             *DatabaseContext      // Database context
	indexPartitions     *base.IndexPartitions // Partitioning of vbuckets in the index
	indexPartitionsLock sync.RWMutex          // Manages access to indexPartitions
	reader              *kvChangeIndexReader  // Index reader
	writer              *kvChangeIndexWriter  // Index writer
}

type IndexPartitionsFunc func() (*base.IndexPartitions, error)

var indexExpvars *expvar.Map

var latestWriteBatch expvar.Int

func init() {
	indexExpvars = expvar.NewMap("syncGateway_index")
	indexExpvars.Set("latest_write_batch", &latestWriteBatch)
}

func (k *kvChangeIndex) Init(context *DatabaseContext, lastSequence SequenceID, onChange func(base.Set), options *CacheOptions, indexOptions *ChangeIndexOptions) (err error) {

	k.context = context
	k.reader = &kvChangeIndexReader{}
	err = k.reader.Init(options, indexOptions, onChange, k.getIndexPartitions)
	if err != nil {
		return err
	}

	if indexOptions.Writer {
		k.writer = &kvChangeIndexWriter{}
		err = k.writer.Init(context, options, indexOptions, k.getIndexPartitions)
		if err != nil {
			return err
		}
	}
	return nil
}

func (k *kvChangeIndex) Prune() {
	// TODO: currently no pruning of channel indexes
}

func (k *kvChangeIndex) Clear() {
	k.reader.Clear()
	if k.writer != nil {
		k.writer.Clear()
	}
}

func (k *kvChangeIndex) Stop() {

	k.reader.Stop()
	if k.writer != nil {
		k.writer.Stop()
	}
}

// Returns the stable sequence for a document's vbucket from the stable clock.
// Used during document write for handling deduplicated sequences on the DCP feed,
// to determine when recent sequences can be pruned.
// Uses reader's stable sequence to avoid db lookups, since document write doesn't
// need the absolute latest version for pruning.
func (k *kvChangeIndex) GetStableSequence(docID string) (seq SequenceID) {

	return k.reader.GetStableSequence(docID)
}

// Used for external retrieval of the stable clock by cbgt. If partition map is missing(potential race with cbgt),
// check for existence of stable counter.  If present, but no partition map, there's a problem and return error.
// If both partition map and counter are not present, can safely assume it's a
// new index.  Return a zero clock and let cbgt initialize DCP feed from zero.
// If stale=true, returns the latest polled reader clock.
// If stale=false, forces load from bucket
func (k *kvChangeIndex) GetStableClock(stale bool) (clock base.SequenceClock, err error) {
	if stale {
		result := k.reader.getReaderStableSequence()
		if result == nil {
			return k.reader.GetStableClock()
		} else {
			return result, nil
		}
	} else {
		return k.reader.GetStableClock()
	}
}

// Retrieves the index partition information for the channel index.  Returns error if the partition map
// is unavailable - this will happen when running as an index reader on a fresh channel index bucket, until
// the first writer comes online and initializes the partition map.  Callers are expected to cancel their
// index operation when an error is returned.
func (k *kvChangeIndex) getIndexPartitions() (*base.IndexPartitions, error) {

	var result *base.IndexPartitions
	func() {
		k.indexPartitionsLock.RLock()
		defer k.indexPartitionsLock.RUnlock()
		result = k.indexPartitions
	}()

	if result != nil {
		return result, nil
	} else {
		return k.initIndexPartitions()
	}
}

// initIndexPartitions first attempts to initialize the index partitions using the partition map document in
// the index bucket.  If that hasn't been created yet, tries to generate the partition information from CBGT.
// If unsuccessful on both of these, returns error and leaves k.indexPartitions as nil.
func (k *kvChangeIndex) initIndexPartitions() (*base.IndexPartitions, error) {

	k.indexPartitionsLock.Lock()
	defer k.indexPartitionsLock.Unlock()

	// Check if it's been initialized while we waited for the lock
	if k.indexPartitions != nil {
		return k.indexPartitions, nil
	}

	// First attempt to load from the bucket
	partitionDef, err := k.loadIndexPartitionsFromBucket()
	if err != nil {
		return nil, err
	}

	indexExpvars.Add("get_indexPartitionMap", 1)
	// If unable to load from index bucket - attempt to initialize based on cbgt partitions
	if partitionDef == nil {
		partitionDef, err = k.retrieveCBGTPartitions()
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Unable to determine partition map for index - not found in index, and not available from cbgt: %v", err))
		}
		// Add to the bucket
		value, err := json.Marshal(partitionDef)
		if err != nil {
			return nil, err
		}
		added, err := k.reader.indexReadBucket.AddRaw(base.KIndexPartitionKey, 0, value)
		if err != nil {
			return nil, err
		}
		// If add fails, it may have been written by another node since the last read attempt.  Make
		// another attempt to use the version from the bucket, to ensure consistency.
		if added == false {
			partitionDef, err = k.loadIndexPartitionsFromBucket()
			if err != nil {
				return nil, err
			}
		}
	}

	// Create k.indexPartitions based on partitionDef
	k.indexPartitions = base.NewIndexPartitions(partitionDef)
	return k.indexPartitions, nil
}

func (k *kvChangeIndex) loadIndexPartitionsFromBucket() (base.PartitionStorageSet, error) {
	var partitionDef base.PartitionStorageSet
	value, _, err := k.reader.indexReadBucket.GetRaw(base.KIndexPartitionKey)
	if err == nil {
		if err = json.Unmarshal(value, &partitionDef); err != nil {
			return nil, err
		}
	}
	return partitionDef, nil
}

func (k *kvChangeIndex) retrieveCBGTPartitions() (partitionDef base.PartitionStorageSet, err error) {

	var manager *cbgt.Manager
	if k.context != nil {
		manager = k.context.BucketSpec.CbgtContext.Manager
	} else {
		return nil, errors.New("Unable to retrieve CBGT partitions - no database context")
	}

	if manager == nil {
		return nil, errors.New("Unable to retrieve CBGT partitions - no CBGT manager")
	}

	_, planPIndexesByName, _ := manager.GetPlanPIndexes(true)
	indexName := k.context.GetCBGTIndexNameForBucket(k.context.Bucket)
	pindexes := planPIndexesByName[indexName]

	for _, pIndex := range pindexes {
		vbStrings := strings.Split(pIndex.SourcePartitions, ",")
		// convert string vbNos to uint16
		vbNos := make([]uint16, len(vbStrings))
		for i := 0; i < len(vbStrings); i++ {
			vbNumber, err := strconv.ParseUint(vbStrings[i], 10, 16)
			if err != nil {
				base.LogFatal("Error creating index partition definition - unable to parse vbucket number %s as integer:%v", vbStrings[i], err)
			}
			vbNos[i] = uint16(vbNumber)
		}
		entry := base.PartitionStorage{
			Index: uint16(0), // see below for index assignment
			Uuid:  pIndex.UUID,
			VbNos: vbNos,
		}
		partitionDef = append(partitionDef, entry)
	}

	// NOTE: the ordering of pindexes returned by manager.GetPlanPIndexes isn't fixed (it's doing a map iteration somewhere).
	//    The mapping from UUID to VbNos will always be consistent.  Sorting by UUID to maintain a consistent index ordering,
	// then assigning index values.
	partitionDef.Sort()
	for i := 0; i < len(partitionDef); i++ {
		partitionDef[i].Index = uint16(i)
	}
	return partitionDef, nil
}
func (k *kvChangeIndex) getIndexPartitionMap() (base.IndexPartitionMap, error) {

	partitions, err := k.getIndexPartitions()
	if err != nil {
		return nil, err
	}
	return partitions.VbMap, nil
}

// Primarily for use by unit tests to avoid CBGT dependency for partition map
func (k *kvChangeIndex) setIndexPartitionMap(partitionMap base.IndexPartitionMap) {
	k.indexPartitions = &base.IndexPartitions{
		VbMap: partitionMap,
	}
}

func (k *kvChangeIndex) DocChanged(docID string, docJSON []byte, seq uint64, vbNo uint16) {
	k.writer.DocChanged(docID, docJSON, seq, vbNo)
}

// No-ops - pending refactoring of change_cache.go to remove usage (or deprecation of
// change_cache altogether)
func (k *kvChangeIndex) getOldestSkippedSequence() uint64 {
	return uint64(0)
}
func (k *kvChangeIndex) getChannelCache(channelName string) *channelCache {
	return nil
}

// TODO: refactor waitForSequence to accept either vbNo or clock
func (k *kvChangeIndex) waitForSequenceID(sequence SequenceID) {
	k.waitForSequence(sequence.Seq)
}
func (k *kvChangeIndex) waitForSequence(sequence uint64) {
	return
}
func (k *kvChangeIndex) waitForSequenceWithMissing(sequence uint64) {
	k.waitForSequence(sequence)
}

// If set to false, DocChanged() becomes a no-op.
func (k *kvChangeIndex) EnableChannelIndexing(enable bool) {
	if k.writer != nil {
		k.writer.EnableChannelIndexing(enable)
	}
}

// Sets the callback function for channel changes
func (k *kvChangeIndex) SetNotifier(onChange func(base.Set)) {
	k.reader.SetNotifier(onChange)
}

func (k *kvChangeIndex) GetChanges(channelName string, options ChangesOptions) (entries []*LogEntry, err error) {
	return k.reader.GetChanges(channelName, options)
}

// ValidFrom not used for channel index, so just returns GetChanges
func (k *kvChangeIndex) GetCachedChanges(channelName string, options ChangesOptions) (uint64, []*LogEntry) {

	changes, _ := k.reader.GetChanges(channelName, options)
	return uint64(0), changes
}

func (k *kvChangeIndex) InitLateSequenceClient(channelName string) uint64 {
	// no-op when not buffering sequences
	return 0
}

func (k *kvChangeIndex) GetLateSequencesSince(channelName string, sinceSequence uint64) (entries []*LogEntry, lastSequence uint64, err error) {
	// no-op when not buffering sequences
	return entries, lastSequence, nil
}

func (k *kvChangeIndex) ReleaseLateSequenceClient(channelName string, sequence uint64) error {
	// no-op when not buffering sequences
	return nil
}

// Add late sequence information to channel cache
func (k *kvChangeIndex) addLateSequence(channelName string, change *LogEntry) error {
	// TODO: no-op for now
	return nil
}

// utils for int/byte

func byteToUint64(input []byte) uint64 {
	readBuffer := bytes.NewReader(input)
	var result uint64
	err := binary.Read(readBuffer, binary.LittleEndian, &result)
	if err != nil {
		base.Warn("byteToUint64 error:%v", err)
	}
	return result
}

func uint64ToByte(input uint64) []byte {
	result := new(bytes.Buffer)
	binary.Write(result, binary.LittleEndian, input)
	return result.Bytes()
}

// debug API

type AllChannelStats struct {
	Channels []ChannelStats `json:"channels"`
}

type IndexStats struct {
	PartitionStats PartitionStats `json:"partitions"`
}

type PartitionStats struct {
	PartitionMap    PartitionMapStats `json:"index_partitions"`
	CBGTMap         PartitionMapStats `json:"cbgt_partitions"`
	PartitionsMatch bool              `json:"matches"`
}

type PartitionMapStats struct {
	Storage base.PartitionStorageSet `json:"partitions"`
}

type ChannelStats struct {
	Name         string              `json:"channel_name"`
	IndexStats   ChannelIndexStats   `json:"index,omitempty"`
	PollingStats ChannelPollingStats `json:"poll,omitempty"`
}

type ChannelIndexStats struct {
	Clock     string `json:"index_clock,omitempty"`
	ClockHash uint64 `json:"index_clock_hash,omitempty"`
}
type ChannelPollingStats struct {
	Clock     string `json:"poll_clock,omitempty"`
	ClockHash uint64 `json:"poll_clock_hash,omitempty"`
}

func (db *DatabaseContext) IndexStats() (indexStats *IndexStats, err error) {

	kvIndex, ok := db.changeCache.(*kvChangeIndex)
	if !ok {
		return nil, errors.New("No channel index in use")
	}

	indexStats = &IndexStats{}

	indexStats.PartitionStats, err = kvIndex.generatePartitionStats()
	return indexStats, err
}

func (db *DatabaseContext) IndexChannelStats(channelName string) (*ChannelStats, error) {

	kvIndex, ok := db.changeCache.(*kvChangeIndex)
	if !ok {
		return nil, errors.New("No channel index in use")
	}

	return db.singleChannelStats(kvIndex, channelName)

}

func (db *DatabaseContext) IndexAllChannelStats() ([]*ChannelStats, error) {

	kvIndex, ok := db.changeCache.(*kvChangeIndex)
	if !ok {
		return nil, errors.New("No channel index in use")
	}

	/*	results := &AllChannelStats{
		Channels: make([]*ChannelStats, len(kvIndex.channelIndexReaders)),
	}*/
	results := make([]*ChannelStats, 0)

	for channelName := range kvIndex.reader.channelIndexReaders {
		channelStats, err := db.singleChannelStats(kvIndex, channelName)
		if err == nil {
			results = append(results, channelStats)
		}
	}
	return results, nil

}

func (db *DatabaseContext) singleChannelStats(kvIndex *kvChangeIndex, channelName string) (*ChannelStats, error) {

	channelStats := &ChannelStats{
		Name: channelName,
	}

	// Create a clean channel reader to retrieve bucket index stats
	indexPartitions, err := kvIndex.getIndexPartitions()
	if err != nil {
		return nil, err
	}

	// Retrieve index stats from bucket
	channelIndex := NewKvChannelIndex(channelName, kvIndex.reader.indexReadBucket, indexPartitions, nil)
	indexClock, err := channelIndex.loadChannelClock()
	if err == nil {
		channelStats.IndexStats = ChannelIndexStats{}
		channelStats.IndexStats.Clock = base.PrintClock(indexClock)
		channelStats.IndexStats.ClockHash = db.SequenceHasher.calculateHash(indexClock)
	}

	// Retrieve polling stats from kvIndex
	pollingChannelIndex := kvIndex.reader.getChannelReader(channelName)
	if pollingChannelIndex != nil {
		lastPolledClock := pollingChannelIndex.lastPolledChannelClock
		if lastPolledClock != nil {
			channelStats.PollingStats = ChannelPollingStats{}
			channelStats.PollingStats.Clock = base.PrintClock(lastPolledClock)
			channelStats.PollingStats.ClockHash = db.SequenceHasher.calculateHash(lastPolledClock)
		}
	}
	return channelStats, nil
}

func (k *kvChangeIndex) generatePartitionStats() (PartitionStats, error) {

	partitionStats := PartitionStats{}
	partitions, err := k.getIndexPartitions()
	if err != nil {
		return partitionStats, err
	}

	if partitions != nil {
		partitions.PartitionDefs.Sort()
		partitionStats.PartitionMap = PartitionMapStats{
			Storage: partitions.PartitionDefs,
		}
	}

	cbgtPartitions, err := k.retrieveCBGTPartitions()
	if err != nil {
		return partitionStats, err
	}
	if cbgtPartitions != nil {
		cbgtPartitions.Sort()
		partitionStats.CBGTMap = PartitionMapStats{
			Storage: cbgtPartitions,
		}
	}

	partitionStats.PartitionsMatch = reflect.DeepEqual(partitionStats.PartitionMap, partitionStats.CBGTMap)
	return partitionStats, nil
}

func IsNotFoundError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "not found")
}
