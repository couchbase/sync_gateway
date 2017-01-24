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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

var vbSuffixMap = []string{"12ci", "12g8", "136k", "13ji", "13n8", "13wq", "141i", "1458", "14mk", "14ps", "158i", "15dk", "15ys", "180u", "18lw", "18qo", "199u", "19ew", "19xo", "1a0q", "1als", "1aqk", "1f7s", "1fkq", "1fr8", "1fvi", "1gbq", "1j6o", "1jjm", "1jwu", "1kcm", "1l8m", "1ldo", "1lyw", "1m1m", "1mmo", "1mpw", "1p2z", "1pnx", "1pw1", "1qgx", "1vy3", "1w5x", "1wiz", "1wp3", "1wtb", "1z95", "1zaf", "1ze7", "20ah", "20e9", "214j", "21hh", "21l9", "21up", "263h", "2679", "26oj", "26rr", "27fj", "2bgr", "2bzj", "2c2p", "2cnr", "2csj", "2d5r", "2dip", "2dp9", "2dth", "2ey9", "2h4n", "2hhl", "2hut", "2ial", "2nfn", "2o3l", "2oon", "2orv", "2rly", "2rqa", "2ru0", "2sey", "2sxa", "2u7y", "2ur2", "2uvc", "2xcg", "2xg6", "2y24", "2y6e", "2yjg", "2yn6", "32gr", "32zj", "332p", "33nr", "33sj", "345r", "34ip", "34p9", "34th", "35y9", "384n", "38hl", "38ut", "39al", "3a4j", "3ahh", "3al9", "3aup", "3f3h", "3f79", "3foj", "3frr", "3gfj", "3j2t", "3jnv", "3jsn", "3kgv", "3kzn", "3m5v", "3mit", "3mtl", "3p20", "3p6a", "3pjc", "3pn2", "3qcc", "3qg2", "3v8c", "3vda", "3vyy", "3w1c", "3w52", "3wi0", "3wma", "3wpy", "3zxe", "427j", "42kh", "42o9", "42vp", "43bh", "43f9", "449h", "44ej", "44xr", "450h", "4549", "45lj", "45qr", "488t", "48dv", "48yn", "491t", "49mv", "49pn", "4a8p", "4adr", "4ayj", "4fcp", "4fz9", "4g6r", "4gjp", "4gs9", "4gwh", "4jbl", "4k7n", "4kkl", "4kvt", "4l0l", "4lln", "4lqv", "4m9l", "4men", "4mxv", "4pfy", "4qoy", "4qra", "4qv0", "4v4y", "4vq2", "4vuc", "4wx2", "4z14", "4z5e", "4zig", "4zm6", "501p", "50mr", "50pj", "518p", "51dr", "51yj", "56cp", "56z9", "576r", "57jp", "57s9", "57wh", "5b7j", "5bkh", "5bo9", "5bvp", "5cbh", "5cf9", "5d9h", "5dej", "5dxr", "5e0h", "5e49", "5elj", "5eqr", "5h8t", "5hdv", "5hyn", "5i1t", "5imv", "5ipn", "5n6v", "5njt", "5nwl", "5oct", "5r80", "5rd2", "5s10", "5s5a", "5sic", "5sm2", "5t2c", "5t62", "5tj0", "5tna", "5tsy", "5uc0", "5uga", "5uzy", "5xre", "5xv4", "623q", "62os", "62rk", "63fs", "64aq", "64x8", "654s", "65hq", "65q8", "65ui", "695o", "69im", "69tu", "6ad8", "6fgk", "6fzs", "6g2i", "6g68", "6gnk", "6gss", "6jfw", "6k3u", "6kow", "6kro", "6l4w", "6lhu", "6lum", "6mau", "6pbb", "6pf3", "6q31", "6qkb", "6qo3", "6qvz", "6v0b", "6v43", "6vh1", "6vqx", "6w9b", "6wa1", "6wxx", "6zpd", "6zt5", "705k", "70ii", "70m8", "70tq", "71d8", "76gk", "76zs", "772i", "7768", "77nk", "77ss", "7b3q", "7bos", "7brk", "7cfs", "7daq", "7dx8", "7e4s", "7ehq", "7eq8", "7eui", "7i5o", "7iim", "7itu", "7n2m", "7nno", "7nsw", "7ogo", "7ozw", "7r8z", "7rdx", "7s1z", "7smx", "7st1", "7t6x", "7tjz", "7ts3", "7twb", "7ucz", "7uz3", "7x35", "7x7d", "7xkf", "7xo7", "7ybf", "7yf7", "82cv", "836t", "83jv", "83wn", "841v", "84mt", "84pl", "858v", "85dt", "85yl", "880j", "88h9", "88lh", "88qp", "899j", "89a9", "89eh", "89xp", "8a0n", "8all", "8aqt", "8f7l", "8fkn", "8fvv", "8gbn", "8j6p", "8jjr", "8jwj", "8kcr", "8l8r", "8ldp", "8lyh", "8m1r", "8mmp", "8mph", "8mt9", "8p2e", "8p64", "8pj6", "8png", "8qc6", "8qgg", "8v86", "8vd4", "8w16", "8w5g", "8wie", "8wm4", "8zay", "8zx0", "909n", "90el", "90xt", "910n", "91ll", "91qt", "967l", "96kn", "96vv", "97bn", "9bcv", "9c6t", "9cjv", "9cwn", "9d1v", "9dmt", "9dpl", "9e8v", "9edt", "9eyl", "9h0j", "9hh9", "9hlh", "9hqp", "9i9j", "9ia9", "9ieh", "9ixp", "9nbj", "9o39", "9o7h", "9okj", "9ovr", "9rq4", "9rue", "9sx4", "9urg", "9uv6", "9xc2", "9xgc", "9y2a", "9y60", "9yj2", "9ync", "a23e", "a274", "a2k6", "a2og", "a3b6", "a3fg", "a496", "a4ae", "a4e4", "a506", "a54g", "a5he", "a5l4", "a8y0", "a9iy", "a9p0", "a9ta", "aay4", "afzg", "agsg", "agw6", "ajb2", "ajfc", "ak3a", "ak70", "akk2", "akoc", "al02", "al4c", "alha", "all0", "aluy", "am92", "amaa", "ame0", "apbv", "aq7t", "aqkv", "aqvn", "av0v", "avlt", "avql", "aw9v", "awet", "awxl", "az1j", "azi9", "azmh", "azpp", "b01d", "b055", "b0i7", "b0mf", "b18d", "b1df", "b6cd", "b6g5", "b727", "b76f", "b7jd", "b7n5", "bbr5", "bbvd", "bdxf", "beqf", "beu7", "bhdb", "bhyz", "bi51", "bii3", "bimb", "bipz", "bn23", "bn6b", "bnn1", "bnwx", "bog1", "bs5u", "bsiw", "bsto", "bt2w", "btnu", "btsm", "bugu", "buzm", "bx3k", "bxk8", "bxoi", "bxrq", "byb8", "byfi", "c2r5", "c2vd", "c4xf", "c5qf", "c5u7", "c8db", "c8yz", "c951", "c9i3", "c9mb", "c9pz", "ca8d", "cadf", "cfcd", "cfg5", "cg27", "cg6f", "cgjd", "cgn5", "cjbx", "ck7z", "ckkx", "ckr1", "cl0x", "cllz", "clqb", "clu3", "cm9x", "cmez", "cmxb", "cpfm", "cq3o", "cqom", "cqru", "cv4m", "cvho", "cvuw", "cwao", "cz5q", "czis", "cztk", "d2c7", "d2gf", "d32d", "d365", "d3j7", "d3nf", "d417", "d45f", "d4id", "d4m5", "d587", "d5d5", "d84z", "d8hx", "d8q1", "d9ax", "d9x1", "daq5", "daud", "dfrf", "dfv7", "dj61", "djj3", "djnb", "djsz", "dkc3", "dkgb", "dkzz", "dl83", "dld1", "dm13", "dm5b", "dmm1", "dmtx", "dp6u", "dpjw", "dpwo", "dqcw", "dv8w", "dvdu", "dvym", "dw1w", "dwmu", "dwpm", "dz9k", "dza8", "dzei", "dzxq", "e0x5", "e1q5", "e1ud", "e6rf", "e6v7", "ebc7", "ebgf", "ec2d", "ec65", "ecj7", "ecnf", "ed17", "ed5f", "edid", "edm5", "ee87", "eed5", "eh4z", "ehhx", "ehq1", "eiax", "eix1", "enfz", "eo3x", "eooz", "eorb", "eov3", "er0o", "erlm", "erqu", "es9o", "esem", "esxu", "etbo", "eu7m", "euko", "euvw", "excs", "ey6q", "eyjs", "eywk", "f2z4", "f3s4", "f3we", "f4pg", "f4t6", "f5yg", "f80a", "f840", "f8h2", "f8lc", "f99a", "f9a2", "f9ec", "fa0e", "fa44", "fah6", "falg", "ff36", "ff7g", "ffke", "ffo4", "fgbe", "fgf4", "fjjy", "fjs0", "fjwa", "fkcy", "fkz0", "fl8y", "flyc", "fm1y", "fmpc", "fmt2", "fp2n", "fpnl", "fpst", "fqgl", "fqzt", "fw5l", "fwin", "fwtv", "fzar", "g09e", "g0a6", "g0eg", "g10e", "g144", "g1h6", "g1lg", "g636", "g67g", "g6ke", "g6o4", "g7be", "g7f4", "gbz4", "gcs4", "gcwe", "gdpg", "gdt6", "geyg", "gh0a", "gh40", "ghh2", "ghlc", "gi9a", "gia2", "giec", "gnba", "gnf0", "go32", "go7c", "goka", "goo0", "govy", "gr4t", "grhv", "grun", "gsav", "gtft", "gu3v", "guot", "gurl", "gxc9", "gxgh", "gxzp", "gy2j", "gyj9", "gynh", "gysp", "h23z", "h2ox", "h2v1", "h3fx", "h4az", "h4x3", "h54x", "h5hz", "h5q3", "h5ub", "h885", "h8d7", "h915", "h95d", "h9if", "h9m7", "ha81", "had3", "hfc1", "hfzx", "hg2b", "hg63", "hgj1", "hgsx", "hkrd", "hkv5", "hlq7", "hluf", "hmx7", "hpbi", "hpf8", "hq7k", "hqki", "hqo8", "hqvq", "hv0i", "hv48", "hvlk", "hvqs", "hw9i", "hwek", "hwxs", "hz1u", "hzmw", "hzpo", "i011", "i0ib", "i0m3", "i0tz", "i181", "i1d3", "i6c1", "i6zx", "i72b", "i763", "i7j1", "i7sx", "ib3z", "ibox", "ibv1", "icfx", "idaz", "idx3", "ie4x", "iehz", "ieq3", "ieub", "ih85", "ihd7", "ii15", "ii5d", "iiif", "iim7", "in2f", "in67", "inj5", "innd", "ioc5", "iogd", "ir8q", "irds", "iryk", "is1q", "isms", "ispk", "it6s", "itjq", "its8", "itwi", "iucq", "iuz8", "ix7o", "ixkm", "ixvu", "iybm", "j230", "j27a", "j2kc", "j2o2", "j3bc", "j3f2", "j49c", "j4a0", "j4ea", "j4xy", "j50c", "j542", "j5h0", "j5la", "j5qy", "j8ye", "j9pe", "j9t4", "jady", "jaya", "jfz2", "jg6y", "jgs2", "jgwc", "jjbg", "jjf6", "jk34", "jk7e", "jkkg", "jko6", "jl0g", "jl46", "jlh4", "jlle", "jm9g", "jma4", "jmee", "jpfr", "jq3p", "jqor", "jqrj", "jv4r", "jvhp", "jvq9", "jvuh", "jwap", "jwx9", "jz5n", "jzil", "jztt", "k0my", "k0pa", "k0t0", "k1dy", "k1ya", "k6z2", "k76y", "k7s2", "k7wc", "kb30", "kb7a", "kbkc", "kbo2", "kcbc", "kcf2", "kd9c", "kda0", "kdea", "kdxy", "ke0c", "ke42", "keh0", "kela", "keqy", "khye", "kipe", "kit4", "kns6", "knwg", "koz6", "krd9", "ks5j", "ksih", "ksm9", "kstp", "kt2h", "kt69", "ktnj", "ktsr", "kugj", "kuzr", "kx3t", "kxov", "kxrn", "kyfv", "l090", "l0ac", "l0e2", "l100", "l14a", "l1hc", "l1l2", "l63c", "l672", "l6k0", "l6oa", "l6ry", "l7b0", "l7fa", "lbgy", "lbza", "lcny", "lcsa", "lcw0", "ld5y", "ldp2", "ldtc", "ley2", "lh04", "lh4e", "lhhg", "lhl6", "li94", "liag", "lie6", "lnb4", "lnfe", "lo3g", "lo76", "lok4", "looe", "lr0p", "lrlr", "lrqj", "ls9p", "lser", "lsxj", "ltbp", "lu7r", "lukp", "lur9", "luvh", "lxcl", "ly6n", "lyjl", "lywt", "m2gy", "m2za", "m3ny", "m3sa", "m3w0", "m45y", "m4p2", "m4tc", "m5y2", "m804", "m84e", "m8hg", "m8l6", "m994", "m9ag", "m9e6", "ma00", "ma4a", "mahc", "mal2", "mf3c", "mf72", "mfk0", "mfoa", "mfry", "mgb0", "mgfa", "mjse", "mjw4", "mkze", "mly6", "mmp6", "mmtg", "mp6j", "mpjh", "mpn9", "mpwp", "mqch", "mqg9", "mv8h", "mvdj", "mvyr", "mw1h", "mw59", "mwmj", "mwpr", "mz9t", "mzev", "mzxn", "n09z", "n0ex"}

const byteIndexBlockCapacity = uint64(256)

const (
	kSequenceOffsetLength = 0 // disabled until we actually need it
)

// ChannelStorage implemented as two interfaces, to support swapping to different underlying storage model
// without significant refactoring.
type ChannelStorageReader interface {
	// GetAllEntries returns all entries for the channel in the specified range, for all vbuckets
	GetChanges(fromSeq base.SequenceClock, channelClock base.SequenceClock, limit int) ([]*LogEntry, error)
	UpdateCache(fromSeq base.SequenceClock, channelClock base.SequenceClock, changedPartitions []*base.PartitionRange) error
}

type ChannelStorageWriter interface {

	// AddEntrySet adds a set of entries to the channel index
	AddEntrySet(entries []*LogEntry) (clockUpdates []*base.PartitionClock, err error)
	RollbackTo(rollbackVbNo uint16, rollbackSeq uint64) error

	// If channel storage implementation uses separate storage for log entries and channel presence,
	// WriteLogEntry and ReadLogEntry can be used to read/write.  Useful when changeIndex wants to
	// manage these document outside the scope of a channel.  StoresLogEntries() allows callers to
	// check whether this is available.
	StoresLogEntries() bool
	WriteLogEntry(entry *LogEntry) error
}

type ChannelStorage interface {
	ChannelStorageReader
	ChannelStorageWriter
}

// Bit flag values
const (
	Seq_NotInChannel = iota
	Seq_InChannel
	Seq_Removed
)

type BitFlagStorage struct {
	bucket          base.Bucket // Index bucket
	channelName     string      // Channel name
	partitions      *base.IndexPartitions
	indexBlockCache *base.LRUCache // Cache of recently used index blocks
}

func NewBitFlagStorage(bucket base.Bucket, channelName string, partitions *base.IndexPartitions) *BitFlagStorage {

	storage := &BitFlagStorage{
		bucket:      bucket,
		channelName: channelName,
		partitions:  partitions,
	}

	// Maximum theoretical block cache capacity is 1024 - if this index writer were indexing every vbucket,
	// and each vbucket sequence was in a different block.  More common case would be this index writer
	// having at most 512 vbuckets, and most of those vbuckets working the same block index per partition (16 vbs per
	// partition) == 32 blocks.  Setting default to 50 to handle any temporary spikes.
	var err error
	storage.indexBlockCache, err = base.NewLRUCache(50)
	if err != nil {
		base.LogFatal("Error creating LRU cache for index blocks: %v", err)
	}
	return storage
}

func (b *BitFlagStorage) StoresLogEntries() bool {
	return true
}

func (b *BitFlagStorage) WriteLogEntry(entry *LogEntry) error {
	key := getEntryKey(entry.VbNo, entry.Sequence)
	value, _ := json.Marshal(entry)
	return b.bucket.SetRaw(key, 0, value)
}

// Reads a single entry from the index
func (b *BitFlagStorage) ReadLogEntry(vbNo uint16, sequence uint64) (*LogEntry, error) {
	entry := &LogEntry{}
	err := b.readIndexEntryInto(vbNo, sequence, entry)
	return entry, err
}

// Reads a single entry from the index
func (b *BitFlagStorage) readIndexEntryInto(vbNo uint16, sequence uint64, entry *LogEntry) error {
	key := getEntryKey(vbNo, sequence)
	value, _, err := b.bucket.GetRaw(key)
	if err != nil {
		base.Warn("Error retrieving entry from bucket for sequence %d, key %s", sequence, key)
		return err
	}
	err = json.Unmarshal(value, entry)
	if err != nil {
		base.Warn("Error unmarshalling entry for sequence %d, key %s", sequence, key)
		return err
	}
	return nil
}

// Adds a set
func (b *BitFlagStorage) AddEntrySet(entries []*LogEntry) (partitionUpdates []*base.PartitionClock, err error) {

	// Update the sequences in the appropriate cache block
	if len(entries) == 0 {
		return partitionUpdates, nil
	}

	// The set of updates may be distributed over multiple partitions and blocks.
	// To support this, iterate over the set, and define groups of sequences by block
	// TODO: this approach feels like it's generating a lot of GC work.  Considered an iterative
	//       approach where a set update returned a list of entries that weren't targeted at the
	//       same block as the first entry in the list, but this would force sequential
	//       processing of the blocks.  Might be worth revisiting if we see high GC overhead.
	blockSets := make(BlockSet)
	clockUpdates := base.NewSequenceClockImpl()
	for _, entry := range entries {
		// Update the sequence in the appropriate cache block
		base.LogTo("DIndex+", "Add to channel index [%s], vbNo=%d, isRemoval:%v", b.channelName, entry.VbNo, entry.IsRemoved())
		blockKey := GenerateBlockKey(b.channelName, entry.Sequence, b.partitions.VbMap[entry.VbNo])
		if _, ok := blockSets[blockKey]; !ok {
			blockSets[blockKey] = make([]*LogEntry, 0)
		}
		blockSets[blockKey] = append(blockSets[blockKey], entry)
		clockUpdates.SetMaxSequence(entry.VbNo, entry.Sequence)
	}

	err = b.writeBlockSetsWithCas(blockSets)
	if err != nil {
		base.Warn("Error writing blockSets with cas for block %s: %+v", blockSets, err)
		return partitionUpdates, err
	}

	partitionUpdates = base.ConvertClockToPartitionClocks(clockUpdates, *b.partitions)
	return partitionUpdates, nil
}

func (b *BitFlagStorage) RollbackTo(rollbackVbNo uint16, rollbackSeq uint64) error {
	return errors.New("Not implemented: BitFlagStorage.RollbackTo")
}

func (b *BitFlagStorage) writeBlockSetsWithCas(blockSets BlockSet) error {

	bulkSets := []*sgbucket.BulkSetEntry{}
	blockKeyToBlock := make(map[string]IndexBlock, len(blockSets))

	for blockKey, logEntries := range blockSets {
		if len(logEntries) == 0 {
			continue
		}
		value, block, err := b.marshalBlock(logEntries)
		if err != nil {
			return err
		}

		// save the IndexBlock for later in case we need it
		blockKeyToBlock[blockKey] = block

		bulkSets = append(bulkSets, &sgbucket.BulkSetEntry{
			Key:   blockKey,
			Value: value,
			Cas:   block.Cas(),
		})
	}
	if err := b.bucket.SetBulk(bulkSets); err != nil {
		return err
	}

	// Cas retry for failed ops

	// create an error channel where goroutines can write errors.
	// make it a buffered channel with enough capacity to handle an error from
	// each goroutine (worst case)
	errorChan := make(chan error, len(bulkSets))

	wg := sync.WaitGroup{}
	for _, bulkSet := range bulkSets {
		if bulkSet.Error != nil {
			wg.Add(1)
			go func(bulkSetEntryParam *sgbucket.BulkSetEntry) {
				defer wg.Done()
				if err := b.writeBlockWithCas(
					blockKeyToBlock[bulkSetEntryParam.Key],
					blockSets[bulkSetEntryParam.Key],
				); err != nil {
					errorChan <- err
				}

			}(bulkSet)

		}
	}

	// wait for goroutines to finish
	wg.Wait()

	// if any errors, return the first error
	if len(errorChan) > 0 {
		return <-errorChan
	}

	return nil

}

func (b *BitFlagStorage) writeBlockWithCas(block IndexBlock, entries []*LogEntry) error {

	// use an empty initial value to force writeCasRaw() to call GET first
	value := []byte{}

	casOut, err := base.WriteCasRaw(b.bucket, block.Key(), value, block.Cas(), 0, func(value []byte) (updatedValue []byte, err error) {

		// Note: The following is invoked upon cas failure - may be called multiple times
		changeCacheExpvars.Add("writeSingleBlock-casRetryCount", 1)
		err = block.Unmarshal(value)
		for _, entry := range entries {
			err := block.AddEntry(entry)
			if err != nil { // Wrong block for this entry
				return nil, err
			}
		}
		return block.Marshal()
	})

	if err != nil {
		return err
	}
	block.SetCas(casOut)
	return nil

}

func (b *BitFlagStorage) marshalBlock(entries []*LogEntry) ([]byte, IndexBlock, error) {
	if len(entries) == 0 {
		return nil, nil, nil
	}
	// Get block based on first entry
	block := b.getIndexBlockForEntry(entries[0])
	// Apply updates to the in-memory block
	for _, entry := range entries {
		err := block.AddEntry(entry)
		if err != nil { // Wrong block for this entry
			return nil, nil, err
		}
	}
	localValue, err := block.Marshal()
	if err != nil {
		base.Warn("Unable to marshal channel block - cancelling block update")
		return nil, block, errors.New("Error marshalling channel block")
	}
	return localValue, block, nil

}

func (b *BitFlagStorage) loadBlock(block IndexBlock) error {

	data, cas, err := b.bucket.GetRaw(block.Key())
	IndexExpvars.Add("get_loadBlock", 1)
	if err != nil {
		return err
	}

	err = block.Unmarshal(data)
	block.SetCas(cas)

	if err != nil {
		return err
	}
	// If found, add to the cache
	b.putIndexBlockToCache(block.Key(), block)
	return nil
}

func (b *BitFlagStorage) GetChanges(fromSeq base.SequenceClock, toSeq base.SequenceClock, limit int) ([]*LogEntry, error) {

	// Determine which blocks have changed, and load those blocks
	blocksByKey, blocksByVb, err := b.calculateChangedBlocks(fromSeq, toSeq)
	if err != nil {
		return nil, err
	}
	b.bulkLoadBlocks(blocksByKey)

	// For each vbucket, create the entries from the blocks.  Create in reverse sequence order, for
	// deduplication once we've retrieved the full entry
	entries := make([]*LogEntry, 0)
	entryKeys := make([]string, 0)
	for blockSetIndex := len(blocksByVb) - 1; blockSetIndex >= 0; blockSetIndex-- {
		blockSet := blocksByVb[blockSetIndex]
		vbNo := blockSet.vbNo
		blocks := blockSet.blocks
		fromVbSeq := fromSeq.GetSequence(vbNo) + 1
		toVbSeq := toSeq.GetSequence(vbNo)

		for blockIndex := len(blocks) - 1; blockIndex >= 0; blockIndex-- {
			blockEntries, keys := blocks[blockIndex].GetEntries(vbNo, fromVbSeq, toVbSeq, true)
			entries = append(entries, blockEntries...)
			entryKeys = append(entryKeys, keys...)
		}
	}

	// Bulk retrieval of individual entries.  Performs deduplication, and reordering into ascending vb and sequence order
	results := b.bulkLoadEntries(entryKeys, entries)

	return results, nil

}

func (b *BitFlagStorage) UpdateCache(sinceClock base.SequenceClock, toClock base.SequenceClock, changedPartitions []*base.PartitionRange) error {
	// no-op, not a caching reader
	return nil
}

type vbBlockSet struct {
	vbNo   uint16
	blocks []IndexBlock
}

// Calculate the set of index blocks that need to be loaded.
//   blocksByVb stores which blocks need to be processed for each vbucket, in ascending vbucket order.  Multiple vb map
//   values can point to the same IndexBlock (i.e. when those vbs share a partition).
//   blocksByKey stores all IndexBlocks to be retrieved, indexed by block key - no duplicates
func (b *BitFlagStorage) calculateChangedBlocks(fromSeq base.SequenceClock, channelClock base.SequenceClock) (blocksByKey map[string]IndexBlock, blocksByVb []vbBlockSet, err error) {

	blocksByKey = make(map[string]IndexBlock, 1)
	blocksByVb = make([]vbBlockSet, 0)

	for vbNo, clockVbSeq := range channelClock.Value() {
		fromVbSeq := fromSeq.GetSequence(uint16(vbNo))
		// Verify that the requested from value is less than the channel clock sequence (there are
		// new entries for this vbucket in the channel)
		if fromVbSeq >= clockVbSeq {
			continue
		}

		blockSet := vbBlockSet{vbNo: uint16(vbNo)}
		partition := b.partitions.VbMap[uint16(vbNo)]
		for _, blockIndex := range generateBitFlagBlockIndexes(b.channelName, fromVbSeq, clockVbSeq, partition) {
			blockKey := GetIndexBlockKey(b.channelName, blockIndex, partition)
			block, found := blocksByKey[blockKey]
			if !found {
				block = newBitFlagBufferBlockForKey(blockKey, b.channelName, blockIndex, partition, b.partitions.VbPositionMaps[partition])
				blocksByKey[blockKey] = block
			}
			blockSet.blocks = append(blockSet.blocks, block)
		}
		blocksByVb = append(blocksByVb, blockSet)

	}
	return blocksByKey, blocksByVb, nil
}

// Bulk get the blocks from the index bucket, and unmarshal into the provided map.
func (b *BitFlagStorage) bulkLoadBlocks(loadedBlocks map[string]IndexBlock) {
	// Do bulk retrieval of blocks, and unmarshal into loadedBlocks
	var keySet []string
	for key := range loadedBlocks {
		keySet = append(keySet, key)
	}
	blocks, err := b.bucket.GetBulkRaw(keySet)
	if err != nil {
		// TODO FIX: if there's an error on a single block retrieval, differentiate between that
		//  and an empty/non-existent block.  Requires identification of expected blocks by the cache.
		base.Warn("Error doing bulk get:%v", err)
	}

	IndexExpvars.Add("bulkGet_bulkLoadBlocks", 1)
	IndexExpvars.Add("bulkGet_bulkLoadBlocksCount", int64(len(keySet)))

	// Unmarshal concurrently
	var wg sync.WaitGroup
	for key, blockBytes := range blocks {
		if len(blockBytes) > 0 {
			wg.Add(1)
			go func(key string, blockBytes []byte) {
				defer wg.Done()
				if err := loadedBlocks[key].Unmarshal(blockBytes); err != nil {
					base.Warn("Error unmarshalling block into map")
				}
			}(key, blockBytes)
		}
	}
	wg.Wait()
}

// Bulk get the blocks from the index bucket, and unmarshal into the provided map.  Expects incoming keySet descending by vbucket and sequence.
// Returns entries ascending by vbucket and sequence
func (b *BitFlagStorage) bulkLoadEntries(keySet []string, blockEntries []*LogEntry) (results []*LogEntry) {
	// Do bulk retrieval of entries
	// TODO: do in batches if keySet is very large?

	entries, err := b.bucket.GetBulkRaw(keySet)
	if err != nil {
		base.Warn("Error doing bulk get:%v", err)
	}
	IndexExpvars.Add("bulkGet_bulkLoadEntries", 1)
	IndexExpvars.Add("bulkGet_bulkLoadEntriesCount", int64(len(keySet)))

	results = make([]*LogEntry, 0, len(blockEntries))
	// Unmarshal, deduplicate, and reorder to ascending by sequence within vbucket

	// TODO: unmarshalls and deduplicates sequentially on a single thread - consider unmarshalling in parallel, with a separate
	// iteration for deduplication - based on performance

	currentVb := uint16(0)                       // tracks current vb for deduplication, as we only need to deduplicate within vb
	currentVbDocIDs := make(map[string]struct{}) // set of doc IDs found in the current vb
	for _, entry := range blockEntries {
		// if vb has changed, reset the docID deduplication map
		if entry.VbNo != currentVb {
			currentVb = entry.VbNo
			currentVbDocIDs = make(map[string]struct{})
		}

		entryKey := getEntryKey(entry.VbNo, entry.Sequence)
		entryBytes, ok := entries[entryKey]
		if !ok || entryBytes == nil {
			base.Warn("Expected entry for %s in get bulk response - not found", entryKey)
			continue
		}
		removed := entry.IsRemoved()
		if err := json.Unmarshal(entryBytes, entry); err != nil {
			base.Warn("Error unmarshalling entry for key %s: %v", entryKey, err)
		}
		if _, exists := currentVbDocIDs[entry.DocID]; !exists {
			currentVbDocIDs[entry.DocID] = struct{}{}
			if removed {
				entry.SetRemoved()
			}
			// TODO: optimize performance for prepend?
			results = append([]*LogEntry{entry}, results...)
		}
	}
	return results
}

// Retrieves the appropriate index block for an entry. If not found in the channel's block cache,
// will initialize a new block and add to the map.
func (b *BitFlagStorage) getIndexBlockForEntry(entry *LogEntry) IndexBlock {

	partition := b.partitions.VbMap[entry.VbNo]
	key := GenerateBlockKey(b.channelName, entry.Sequence, partition)

	// First attempt to retrieve from the cache of recently accessed blocks
	block := b.getIndexBlockFromCache(key)
	if block != nil {
		return block
	}

	// If not found in cache, attempt to retrieve from the index bucket
	block = NewIndexBlock(b.channelName, entry.Sequence, partition, b.partitions)
	err := b.loadBlock(block)
	if err == nil {
		return block
	}

	// If still not found, initialize a new empty index block and add to cache
	block = NewIndexBlock(b.channelName, entry.Sequence, partition, b.partitions)
	b.putIndexBlockToCache(key, block)

	return block
}

// Safely retrieves index block from the channel's block cache
func (b *BitFlagStorage) getIndexBlockFromCache(key string) IndexBlock {
	cacheValue, ok := b.indexBlockCache.Get(key)
	if !ok {
		return nil
	}

	block, ok := cacheValue.(*BitFlagBlock)
	if ok {
		return block
	} else {
		return nil
	}
}

// Safely inserts index block from the channel's block cache
func (b *BitFlagStorage) putIndexBlockToCache(key string, block IndexBlock) {
	b.indexBlockCache.Put(key, block)
}

// IndexBlock interface - defines interactions with a block
type IndexBlock interface {
	AddEntry(entry *LogEntry) error
	Key() string
	Marshal() ([]byte, error)
	Unmarshal(value []byte) error
	Cas() uint64
	SetCas(cas uint64)
	GetEntries(vbNo uint16, fromSeq uint64, toSeq uint64, includeKeys bool) (entries []*LogEntry, keySet []string)
	GetAllEntries() []*LogEntry
}

func NewIndexBlock(channelName string, sequence uint64, partition uint16, partitions *base.IndexPartitions) IndexBlock {
	return newBitFlagBufferBlock(channelName, sequence, partition, partitions.VbPositionMaps[partition])
}

func GenerateBlockKey(channelName string, sequence uint64, partition uint16) string {
	return generateBitFlagBlockKey(channelName, sequence, partition)
}

// Returns the set of all block keys required to return sequences from minSeq to maxSeq for the channel, partition
func GenerateBlockKeys(channelName string, minSeq uint64, maxSeq uint64, partition uint16) []string {
	return generateBitFlagBlockKeys(channelName, minSeq, maxSeq, partition)
}

// Determine the cache block key for a sequence
func generateBitFlagBlockKey(channelName string, minSequence uint64, partition uint16) string {
	index := uint16(minSequence / byteIndexBlockCapacity)
	return GetIndexBlockKey(channelName, index, partition)
}

// Returns an ordered list of blocks needed to return the range minSequence to maxSequence
func generateBitFlagBlockKeys(channelName string, minSequence uint64, maxSequence uint64, partition uint16) []string {
	var keys []string
	firstIndex := GenerateBitFlagIndex(minSequence)
	lastIndex := GenerateBitFlagIndex(maxSequence)
	for index := firstIndex; index <= lastIndex; index++ {
		keys = append(keys, GetIndexBlockKey(channelName, index, partition))
	}
	return keys
}

// Returns an ordered list of blocks needed to return the range minSequence to maxSequence
func generateBitFlagBlockIndexes(channelName string, minSequence uint64, maxSequence uint64, partition uint16) []uint16 {
	var indexes []uint16
	firstIndex := GenerateBitFlagIndex(minSequence)
	lastIndex := GenerateBitFlagIndex(maxSequence)
	for index := firstIndex; index <= lastIndex; index++ {
		indexes = append(indexes, index)
	}
	return indexes
}

func GenerateBitFlagIndex(sequence uint64) uint16 {
	return uint16(sequence / byteIndexBlockCapacity)
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

func (b *BitFlagBlockData) MaxSequence() uint64 {
	return b.MinSequence + byteIndexBlockCapacity - 1
}

func newBitFlagBlock(channelName string, forSequence uint64, partition uint16) *BitFlagBlock {

	minSequence := uint64(forSequence/byteIndexBlockCapacity) * byteIndexBlockCapacity

	key := generateBitFlagBlockKey(channelName, minSequence, partition)

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

func newBitFlagBlockForKey(key string, channelName string, index uint16, partition uint16) *BitFlagBlock {

	minSequence := uint64(index) * byteIndexBlockCapacity

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

func (b *BitFlagBlock) Unmarshal(value []byte) error {
	input := bytes.NewBuffer(value)
	dec := gob.NewDecoder(input)
	err := dec.Decode(&b.value)
	if err != nil {
		return err
	}
	return nil
}

func (b *BitFlagBlock) AddEntry(entry *LogEntry) error {
	if _, ok := b.value.Entries[entry.VbNo]; !ok {
		b.value.Entries[entry.VbNo] = make([]byte, byteIndexBlockCapacity+kSequenceOffsetLength)
	}

	index := b.getIndexForSequence(entry.Sequence)
	if index < 0 || index >= uint64(len(b.value.Entries[entry.VbNo])) {
		return errors.New("Sequence out of range of block")
	}
	if entry.IsRemoved() {
		b.value.Entries[entry.VbNo][index] = byte(Seq_Removed)
	} else {
		b.value.Entries[entry.VbNo][index] = byte(Seq_InChannel)
	}
	return nil
}

func (b *BitFlagBlock) AddEntrySet(entries []*LogEntry) error {
	return nil
}

func (b *BitFlagBlock) GetAllEntries() []*LogEntry {
	results := make([]*LogEntry, 0)
	// Iterate over all vbuckets, returning entries for each.
	for vbNo, sequences := range b.value.Entries {
		for index, entry := range sequences {
			if entry != byte(Seq_NotInChannel) {
				removed := entry == byte(Seq_Removed)
				newEntry := &LogEntry{VbNo: vbNo,
					Sequence: b.value.MinSequence + uint64(index),
				}
				if removed {
					newEntry.SetRemoved()
				}
				results = append(results, newEntry)
			}

		}
	}
	return results
}

// Block entry retrieval - used by GetEntries and GetEntriesAndKeys.
func (b *BitFlagBlock) GetEntries(vbNo uint16, fromSeq uint64, toSeq uint64, includeKeys bool) (entries []*LogEntry, keySet []string) {

	entries = make([]*LogEntry, 0)
	keySet = make([]string, 0)
	vbEntries, ok := b.value.Entries[vbNo]
	if !ok { // nothing for this vbucket
		return entries, keySet
	}

	// Validate range against block bounds
	if fromSeq > b.value.MaxSequence() || toSeq < b.value.MinSequence {
		base.LogTo("DIndex+", "Invalid Range for block [%s] (from, to): (%d, %d).  MinSeq:%d", b.Key(), fromSeq, toSeq, b.value.MinSequence)
		return entries, keySet
	}

	// Determine the range to iterate within this block, based on fromSeq and toSeq
	var startIndex, endIndex uint64
	if fromSeq < b.value.MinSequence {
		startIndex = 0
	} else {
		startIndex = fromSeq - b.value.MinSequence
	}
	if toSeq > b.value.MaxSequence() {
		endIndex = b.value.MaxSequence() - b.value.MinSequence // max index value within block
	} else {
		endIndex = toSeq - b.value.MinSequence
	}

	for index := int(endIndex); index >= int(startIndex); index-- {
		entry := vbEntries[index]
		if entry != byte(0) {
			removed := entry == byte(2)
			newEntry := &LogEntry{VbNo: vbNo,
				Sequence: b.value.MinSequence + uint64(index),
			}
			if removed {
				newEntry.SetRemoved()
			}
			entries = append(entries, newEntry)
			if includeKeys {
				keySet = append(keySet, getEntryKey(vbNo, newEntry.Sequence))
			}
		}
	}

	return entries, keySet
}

func (b *BitFlagBlock) getIndexForSequence(sequence uint64) uint64 {
	return sequence - b.value.MinSequence + kSequenceOffsetLength
}

// Determine the cache block index for a sequence
func (b *BitFlagBlock) getBlockIndex(sequence uint64) uint16 {
	return uint16(sequence / byteIndexBlockCapacity)
}

type BitFlagBufferBlock struct {
	key         string             // DocID for the cache block doc
	value       []byte             // Raw document value
	cas         uint64             // cas value of block in database
	expiry      time.Time          // Expiry - used for compact
	minSequence uint64             // Minimum sequence
	maxSequence uint64             // Maximum sequence
	vbPositions base.VbPositionMap // Position of vbuckets in partition

}

func newBitFlagBufferBlock(channelName string, forSequence uint64, partition uint16, vbPositions base.VbPositionMap) *BitFlagBufferBlock {

	index := uint16(forSequence / byteIndexBlockCapacity)
	key := generateBitFlagBlockKey(channelName, uint64(index)*byteIndexBlockCapacity, partition)

	return newBitFlagBufferBlockForKey(key, channelName, index, partition, vbPositions)
}

func newBitFlagBufferBlockForKey(key string, channelName string, index uint16, partition uint16, vbPositions base.VbPositionMap) *BitFlagBufferBlock {

	cacheBlock := &BitFlagBufferBlock{
		key:         key,
		vbPositions: vbPositions,
		minSequence: uint64(index) * byteIndexBlockCapacity,
	}

	// Initialize the entry map
	blockSize := kSequenceOffsetLength + uint64(len(vbPositions))*byteIndexBlockCapacity
	cacheBlock.value = make([]byte, blockSize)

	cacheBlock.maxSequence = cacheBlock.minSequence + byteIndexBlockCapacity - 1

	return cacheBlock

}

func (b *BitFlagBufferBlock) Key() string {
	return b.key
}

func (b *BitFlagBufferBlock) Cas() uint64 {
	return b.cas
}

func (b *BitFlagBufferBlock) SetCas(cas uint64) {
	b.cas = cas
}

func (b *BitFlagBufferBlock) Marshal() ([]byte, error) {
	return b.value, nil
}

func (b *BitFlagBufferBlock) Unmarshal(value []byte) error {
	b.value = value
	return nil
}

func (b *BitFlagBufferBlock) AddEntry(entry *LogEntry) error {

	index, err := b.getIndexForSequence(entry.VbNo, entry.Sequence)
	if err != nil {
		return err
	}
	if entry.IsRemoved() {
		b.value[index] = byte(2)
	} else {
		b.value[index] = byte(1)
	}
	return nil
}

func (b *BitFlagBufferBlock) GetAllEntries() []*LogEntry {
	results := make([]*LogEntry, 0)
	// Iterate over all vbuckets, returning entries for each.
	for vbNo := range b.vbPositions {
		sequences := b.getBytesForVb(vbNo)
		for index, entry := range sequences {
			if entry != byte(0) {
				removed := entry == byte(2)
				newEntry := &LogEntry{VbNo: vbNo,
					Sequence: b.minSequence + uint64(index),
				}
				if removed {
					newEntry.SetRemoved()
				}
				results = append(results, newEntry)
			}

		}
	}
	return results
}

// Block entry retrieval - used by GetEntries and GetEntriesAndKeys.
func (b *BitFlagBufferBlock) GetEntries(vbNo uint16, fromSeq uint64, toSeq uint64, includeKeys bool) (entries []*LogEntry, keySet []string) {

	entries = make([]*LogEntry, 0)
	keySet = make([]string, 0)

	// Validate range against block bounds
	if fromSeq > b.maxSequence || toSeq < b.minSequence {
		base.LogTo("DIndex+", "Invalid Range for block (from, to): (%d, %d).  MinSeq:%d", fromSeq, toSeq, b.minSequence)
		return entries, keySet
	}

	vbEntries := b.getBytesForVb(vbNo)

	// Determine the range to iterate within this block, based on fromSeq and toSeq
	var startIndex, endIndex uint64
	if fromSeq < b.minSequence {
		startIndex = 0
	} else {
		startIndex = fromSeq - b.minSequence
	}
	if toSeq > b.maxSequence {
		endIndex = b.maxSequence - b.minSequence // max index value within block
	} else {
		endIndex = toSeq - b.minSequence
	}

	for index := int(endIndex); index >= int(startIndex); index-- {
		entry := vbEntries[index]
		if entry != byte(0) {
			removed := entry == byte(2)
			newEntry := &LogEntry{VbNo: vbNo,
				Sequence: b.minSequence + uint64(index),
			}
			if removed {
				newEntry.SetRemoved()
			}
			entries = append(entries, newEntry)
			if includeKeys {
				keySet = append(keySet, getEntryKey(vbNo, newEntry.Sequence))
			}
		}
	}
	return entries, keySet
}

func (b *BitFlagBufferBlock) getIndexForSequence(vbNo uint16, sequence uint64) (uint64, error) {

	// Position for vb, seq in []byte storage is
	// [global offset] + [vb position * vb length] + [sequence - minSequence]
	// -global offset-------find block for vb-------find position in vb block--

	if sequence-b.minSequence < 0 || sequence-b.minSequence > byteIndexBlockCapacity {
		return 0, fmt.Errorf("Block %s is wrong block for vbNo: %d, sequence: %d", b.Key(), vbNo, sequence)
	}
	return kSequenceOffsetLength + (uint64(b.vbPositions[vbNo]) * byteIndexBlockCapacity) + (sequence - b.minSequence), nil
}

func (b *BitFlagBufferBlock) getBytesForVb(vbNo uint16) []byte {

	// Position for vb, seq in []byte storage is
	// [global offset] + [vb position * vb length] + [sequence - minSequence]
	// -global offset-------find block for vb-------find position in vb block--
	startIndex := kSequenceOffsetLength + (b.vbPositions[vbNo] * byteIndexBlockCapacity)
	return b.value[startIndex : startIndex+byteIndexBlockCapacity]
}

// BlockSet - used to organize collections of vbuckets by partition for block-based removal

type BlockSet map[string][]*LogEntry // Collection of index entries, indexed by block id.

func (b BlockSet) keySet() []string {
	keys := make([]string, len(b))
	i := 0
	for k := range b {
		keys[i] = k
		i += 1
	}
	return keys
}

// IndexEntry handling
// The BitFlagBlock implementation only stores presence and removal information in the block - the
// rest of the LogEntry details (DocID, RevID) are stored in a separate "entry" document.  The functions
// below manage interaction with the entry documents

// Bulk retrieval of index entries
/*
func readIndexEntriesInto(bucket base.Bucket, keys []string, entries map[string]*LogEntry) error {
	results, err := bucket.GetBulkRaw(keys)
	if err != nil {
		base.Warn("error doing bulk get:", err)
		return err
	}
	// TODO: unmarshal in concurrent goroutines
	for key, entryBytes := range results {
		if len(entryBytes) > 0 {
			entry := entries[key]
			removed := entry.Removed
			if err := entries[key].Unmarshal(entry); err != nil {
				base.Warn("Error unmarshalling entry")
			}
			if removed {
				entry.SetRemoved()
			}
		}
	}
	return nil
}
*/

// Generate the key for a single sequence/entry
func getEntryKey(vbNo uint16, sequence uint64) string {
	return fmt.Sprintf("%s_entry:%d:%d", base.KIndexPrefix, vbNo, sequence)
}

func getSuffixForVbNo(vbNo uint16) string {
	return vbSuffixMap[vbNo]
}
