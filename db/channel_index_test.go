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
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/sync_gateway/base"
)

type channelIndexTest struct {
	numVbuckets   int         // Number of vbuckets
	indexBucket   base.Bucket // Index Bucket
	sequenceGap   int         // Max sequence gap within vbucket - random between 1 and sequenceGap
	lastSequences []uint64    // Last sequence per vbucket
	r             *rand.Rand  // seeded random number generator
	channelName   string      // Channel name
}

func NewChannelIndex(vbNum int, sequenceGap int, name string) *channelIndexTest {
	lastSeqs := make([]uint64, vbNum)

	index := &channelIndexTest{
		numVbuckets:   vbNum,
		indexBucket:   testIndexBucket(),
		sequenceGap:   sequenceGap,
		lastSequences: lastSeqs,
		r:             rand.New(rand.NewSource(42)),
		channelName:   name,
	}
	couchbase.PoolSize = 64
	return index
}

func NewChannelIndexForBucket(vbNum int, sequenceGap int, name string, bucket base.Bucket) *channelIndexTest {
	lastSeqs := make([]uint64, vbNum)

	index := &channelIndexTest{
		numVbuckets:   vbNum,
		indexBucket:   bucket,
		sequenceGap:   sequenceGap,
		lastSequences: lastSeqs,
		r:             rand.New(rand.NewSource(42)),
		channelName:   name,
	}
	couchbase.PoolSize = 64
	return index
}

func (c *channelIndexTest) seedData(format string) error {

	// Check if the data has already been loaded
	loadFlagDoc := fmt.Sprintf("seedComplete::%s", format)
	_, _, err := c.indexBucket.GetRaw(loadFlagDoc)
	if err == nil {
		return nil
	}

	vbucketBytes := make(map[int][]byte)

	// Populate index
	for i := 0; i < 100000; i++ {
		// Choose a vbucket at random
		vbNo := c.r.Intn(c.numVbuckets)
		_, ok := vbucketBytes[vbNo]
		if !ok {
			vbucketBytes[vbNo] = []byte("")
		}
		nextSequence := c.getNextSequenceBytes(vbNo)
		c.lastSequences[vbNo] = nextSequence
		vbucketBytes[vbNo] = append(vbucketBytes[vbNo], getSequenceAsBytes(nextSequence)...)
	}

	for vbNum, value := range vbucketBytes {
		_, err = c.indexBucket.AddRaw(c.getIndexDocName(vbNum), 0, value)
	}

	// Write flag doc
	_, err = c.indexBucket.AddRaw(loadFlagDoc, 0, []byte("complete"))
	if err != nil {
		log.Printf("Load error %v", err)
		return err
	}
	return nil
}

func (c *channelIndexTest) getNextSequenceBytes(vb int) uint64 {
	lastSequence := c.lastSequences[vb]
	gap := 1
	if c.sequenceGap > 0 {
		gap = 1 + c.r.Intn(c.sequenceGap)
	}
	nextSequence := lastSequence + uint64(gap)
	return nextSequence
}

func (c *channelIndexTest) addToCache(vb int) error {

	lastSequence := c.lastSequences[vb]
	gap := 1
	if c.sequenceGap > 0 {
		gap = 1 + c.r.Intn(c.sequenceGap)
	}
	nextSequence := lastSequence + uint64(gap)
	c.writeToCache(vb, nextSequence)
	c.lastSequences[vb] = nextSequence
	return nil
}

func (c *channelIndexTest) writeToCache(vb int, sequence uint64) error {
	docName := c.getIndexDocName(vb)
	sequenceBytes := getSequenceAsBytes(sequence)
	err := c.indexBucket.Append(docName, sequenceBytes)
	if err != nil {
		added, err := c.indexBucket.AddRaw(docName, 0, sequenceBytes)
		if err != nil || added == false {
			log.Printf("AddRaw also failed?! %s:%v", docName, err)
		}
	}
	return nil
}

func appendWrite(bucket base.Bucket, key string, vb int, sequence uint64) error {
	sequenceBytes := getSequenceAsBytes(sequence)
	err := bucket.Append(key, sequenceBytes)
	// TODO: assumes err means it hasn't been created yet
	if err != nil {
		added, err := bucket.AddRaw(key, 0, sequenceBytes)
		if err != nil || added == false {
			log.Printf("AddRaw also failed?! %s:%v", key, err)
		}
	}
	return nil
}

func getDocName(channelName string, vb int, blockNumber int) string {
	return fmt.Sprintf("_index::%s::%d::%s", channelName, blockNumber, vbSuffixMap[vb])
}

func (c *channelIndexTest) getIndexDocName(vb int) string {
	blockNumber := 0
	return getDocName(c.channelName, vb, blockNumber)
}

func (c *channelIndexTest) readIndexSingle() error {

	for i := 0; i < c.numVbuckets; i++ {
		key := c.getIndexDocName(i)
		body, _, err := c.indexBucket.GetRaw(key)
		if err != nil {
			log.Printf("Error retrieving for key %s: %s", key, err)
		} else {
			size := len(body)
			if size == 0 {
				return errors.New("Empty body")
			}
		}
	}
	return nil
}

func (c *channelIndexTest) readIndexSingleParallel() error {

	var wg sync.WaitGroup
	for i := 0; i < c.numVbuckets; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := c.getIndexDocName(i)
			body, _, err := c.indexBucket.GetRaw(key)
			if err != nil {
				log.Printf("Error retrieving for key %s: %s", key, err)
			} else {
				size := len(body)
				if size == 0 {
					log.Printf("zero size body found")
				}
			}
		}(i)
	}
	wg.Wait()
	return nil
}

func (c *channelIndexTest) readIndexBulk() error {
	keys := make([]string, c.numVbuckets)
	for i := 0; i < c.numVbuckets; i++ {
		keys[i] = c.getIndexDocName(i)
	}
	couchbaseBucket, ok := c.indexBucket.(base.CouchbaseBucket)
	if !ok {
		log.Printf("Unable to convert to couchbase bucket")
		return errors.New("Unable to convert to couchbase bucket")
	}
	responses, _, err := couchbaseBucket.GetBulk(keys)
	if err != nil {
		return err
	}

	for _, response := range responses {
		body := response.Body
		// read last from body
		size := len(body)
		if size <= 0 {
			return errors.New(fmt.Sprintf("Empty body for response %v", response))
		}
	}
	return nil

}

func getSequenceAsBytes(sequence uint64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, sequence)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	return buf.Bytes()
}

// set up bucket connection

// define channel index (num vbuckets)

// write docs with random(?) vbucket distribution.
// include random gaps in sequence numbering within bucket

// benchmark
// init with vbucket size
//
// write n docs to channel index
// -
// get since 0
// write n docs to channel index
// get since previous

// bonus points:
// - check vbucket hash for docs

func BenchmarkChannelIndexSimpleGet(b *testing.B) {

	// num vbuckets
	vbCount := 1024
	index := NewChannelIndex(vbCount, 0, "basicChannel")

	index.seedData("default")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := index.readIndexSingle()
		if err != nil {
			log.Printf("Error reading index single: %v", err)
		}
	}
}

func BenchmarkChannelIndexSimpleParallelGet(b *testing.B) {

	// num vbuckets
	vbCount := 1024
	index := NewChannelIndex(vbCount, 0, "basicChannel")

	index.seedData("default")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := index.readIndexSingleParallel()
		if err != nil {
			log.Printf("Error reading index single: %v", err)
		}
	}
}

func BenchmarkChannelIndexBulkGet(b *testing.B) {

	// num vbuckets
	vbCount := 1024
	index := NewChannelIndex(vbCount, 0, "basicChannel")

	index.seedData("default")

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := index.readIndexBulk()
		if err != nil {
			log.Printf("Error reading index bulk: %v", err)
		}
	}
}

func BenchmarkChannelIndexPartitionReadSimple(b *testing.B) {

	// num vbuckets
	vbCount := 16
	index := NewChannelIndex(vbCount, 0, "basicChannel")

	// Populate index with 100K sequences

	index.seedData("default")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := index.readIndexSingle()
		if err != nil {
			log.Printf("Error reading index single: %v", err)
		}
	}
}

func BenchmarkChannelIndexPartitionReadBulk(b *testing.B) {

	// num vbuckets
	vbCount := 16
	index := NewChannelIndex(vbCount, 0, "basicChannel")

	index.seedData("default")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := index.readIndexBulk()
		if err != nil {
			log.Printf("Error reading index bulk: %v", err)
		}
	}
}

func BenchmarkMultiChannelIndexSimpleGet_10(b *testing.B) {
	MultiChannelIndexSimpleGet(b, 10)
}

func BenchmarkMultiChannelIndexSimpleGet_100(b *testing.B) {
	MultiChannelIndexSimpleGet(b, 100)
}

func BenchmarkMultiChannelIndexSimpleGet_1000(b *testing.B) {
	MultiChannelIndexSimpleGet(b, 1000)
}

func MultiChannelIndexSimpleGet(b *testing.B, numChannels int) {

	// num vbuckets
	vbCount := 1024

	bucket := testIndexBucket()
	indices := seedMultiChannelData(vbCount, bucket, numChannels)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for index := 0; index < numChannels; index++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				log.Printf("init %d", index)
				err := indices[index].readIndexSingle()
				log.Printf("done %d", index)
				if err != nil {
					log.Printf("Error reading index single: %v", err)
				}
			}(index)
		}
		wg.Wait()
	}
}

func seedMultiChannelData(vbCount int, bucket base.Bucket, numChannels int) []*channelIndexTest {

	indices := make([]*channelIndexTest, numChannels)
	// seed data
	for chanIndex := 0; chanIndex < numChannels; chanIndex++ {
		channelName := fmt.Sprintf("channel_%d", chanIndex)
		indices[chanIndex] = NewChannelIndexForBucket(vbCount, 0, channelName, bucket)
		indices[chanIndex].seedData(channelName)
	}

	log.Printf("Load complete")
	return indices
}
func BenchmarkMultiChannelIndexBulkGet_3(b *testing.B) {
	MultiChannelIndexBulkGet(b, 3)
}

func BenchmarkMultiChannelIndexBulkGet_10(b *testing.B) {
	MultiChannelIndexBulkGet(b, 10)
}

func BenchmarkMultiChannelIndexBulkGet_100(b *testing.B) {
	MultiChannelIndexBulkGet(b, 100)
}

func BenchmarkMultiChannelIndexBulkGet_1000(b *testing.B) {
	MultiChannelIndexBulkGet(b, 1000)
}

func MultiChannelIndexBulkGet(b *testing.B, numChannels int) {

	// num vbuckets
	vbCount := 1024

	bucket := testIndexBucket()
	indices := seedMultiChannelData(vbCount, bucket, numChannels)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for index := 0; index < numChannels; index++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				log.Printf("Calling bulk read for %d", index)
				err := indices[index].readIndexBulk()
				if err != nil {
					log.Printf("Error reading index single: %v", err)
				}
			}(index)
		}
		wg.Wait()
	}
}

func TestChannelIndexBulkGet10(t *testing.T) {

	// num vbuckets
	vbCount := 1024
	numChannels := 10
	bucket := testIndexBucket()
	indices := seedMultiChannelData(vbCount, bucket, numChannels)

	startTime := time.Now()

	var wg sync.WaitGroup
	for index := 0; index < numChannels; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			err := indices[index].readIndexBulk()
			if err != nil {
				log.Printf("Error reading index single: %v", err)
			}
		}(index)
	}
	wg.Wait()

	log.Printf("test took %v", time.Since(startTime))
}

func TestChannelIndexSimpleReadSingle(t *testing.T) {

	log.Printf("Test single...")
	// num vbuckets
	vbCount := 1024
	index := NewChannelIndex(vbCount, 10, "basicChannel")

	// Populate index
	for i := 0; i < 5000; i++ {
		// Choose a vbucket
		vbNo := index.r.Intn(vbCount)
		index.addToCache(vbNo)
	}

	index.readIndexSingle()
}

func TestChannelIndexSimpleReadBulk(t *testing.T) {

	log.Printf("Test bulk...")
	// num vbuckets
	vbCount := 1024
	index := NewChannelIndex(vbCount, 0, "basicChannel")

	index.readIndexBulk()
}

func TestChannelIndexPartitionReadSingle(t *testing.T) {

	log.Printf("Test single...")
	// num vbuckets
	vbCount := 16
	index := NewChannelIndex(vbCount, 0, "basicChannel")

	// Populate index
	for i := 0; i < 5000; i++ {
		// Choose a vbucket
		vbNo := index.r.Intn(vbCount)
		index.addToCache(vbNo)
	}

	index.readIndexSingle()
}

func TestChannelIndexPartitionReadBulk(t *testing.T) {

	log.Printf("Test single...")
	// num vbuckets
	vbCount := 16
	index := NewChannelIndex(vbCount, 0, "basicChannel")

	// Populate index
	for i := 0; i < 5000; i++ {
		// Choose a vbucket
		vbNo := index.r.Intn(vbCount)
		index.addToCache(vbNo)
	}

	index.readIndexSingle()
}

func TestVbucket(t *testing.T) {

	index := NewChannelIndex(1024, 0, "basicChannel")
	counts := make(map[uint32]int)

	results := ""
	for i := uint64(0); i < 10000000; i++ {
		suffix := strconv.FormatUint(i, 36)
		if len(suffix) == 4 {
			docId := fmt.Sprintf("basicChannel::index::indexDoc_%s", suffix)
			vbNo := index.indexBucket.VBHash(docId)
			if vbNo == 490 {
				results = fmt.Sprintf("%s, %q", results, suffix)
			}
			counts[vbNo] = counts[vbNo] + 1
			if counts[vbNo] > 1024 {
				log.Printf("your winner %d, at %d:", vbNo, i)
				break
			}
		}
	}
	log.Printf(results)
	log.Printf("done")

}

func verifyVBMapping(bucket base.Bucket, channelName string) error {

	channelVbNo := uint32(0)

	for i := 0; i < 1024; i++ {
		docId := fmt.Sprintf("_index::%s::%d::%s", channelName, 1, vbSuffixMap[i])
		vbNo := bucket.VBHash(docId)
		if channelVbNo == 0 {
			channelVbNo = vbNo
		}
		if vbNo != channelVbNo {
			return errors.New("vb numbers don't match")
		}
	}
	return nil
}

func TestChannelVbucketMappings(t *testing.T) {

	index := NewChannelIndex(1024, 0, "basicChannel")

	err := verifyVBMapping(index.indexBucket, "foo")
	assertTrue(t, err == nil, "inconsistent hash")

	err = verifyVBMapping(index.indexBucket, "SomeVeryLongChannelNameInCaseLengthIsSomehowAFactor")
	assertTrue(t, err == nil, "inconsistent hash")
	err = verifyVBMapping(index.indexBucket, "Punc-tu-@-tio-n")
	assertTrue(t, err == nil, "inconsistent hash")
	err = verifyVBMapping(index.indexBucket, "more::punc::tu::a::tion")
	assertTrue(t, err == nil, "inconsistent hash")

	err = verifyVBMapping(index.indexBucket, "1")
	assertTrue(t, err == nil, "inconsistent hash")

	log.Printf("checks out")
}
