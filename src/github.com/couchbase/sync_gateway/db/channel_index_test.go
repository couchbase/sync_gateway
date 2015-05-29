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
	"sync"
	"testing"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/sync_gateway/base"
)

var suffixMap = []uint16{95, 105, 129, 131, 240, 258, 274, 496, 506, 532, 643, 677, 952, 966, 1056, 1062,
	1283, 1313, 1327, 1455, 1461, 1479, 1680, 1698, 1708, 1710, 1724, 1801, 1819, 1835, 1989, 1991,
	2096, 2106, 2132, 2243, 2277, 2495, 2505, 2529, 2531, 2640, 2658, 2674, 2949, 2951, 2965, 3055,
	3061, 3079, 3280, 3298, 3308, 3310, 3324, 3456, 3462, 3683, 3713, 3727, 3802, 3836, 3992, 4147,
	4173, 4202, 4236, 4392, 4544, 4568, 4570, 4601, 4619, 4635, 4789, 4791, 4880, 4898, 4908, 4910,
	4924, 5014, 5020, 5038, 5184, 5349, 5351, 5365, 5417, 5423, 5587, 5752, 5766, 5843, 5877, 6144,
	6168, 6170, 6201, 6219, 6235, 6389, 6391, 6547, 6573, 6602, 6636, 6792, 6883, 6913, 6927, 7017,
	7023, 7187, 7352, 7366, 7414, 7420, 7438, 7584, 7749, 7751, 7765, 7840, 7858, 7874, 8009, 8011,
	8025, 8181, 8199, 8354, 8360, 8378, 8412, 8426, 8582, 8757, 8763, 8846, 8872, 9142, 9176, 9207,
	9233, 9397, 9541, 9559, 9575, 9604, 9628, 9630, 9794, 9885, 9915, 9921, 9939, 10047, 10073, 10292,
	10302, 10336, 10444, 10468, 10470, 10689, 10691, 10701, 10719, 10735, 10808, 10810, 10824, 10980, 10998, 11084,
	11114, 11120, 11138, 11249, 11251, 11265, 11487, 11517, 11523, 11652, 11666, 11943, 11977, 12044, 12068, 12070,
	12289, 12291, 12301, 12319, 12335, 12447, 12473, 12692, 12702, 12736, 12813, 12827, 12983, 13087, 13117, 13123,
	13252, 13266, 13484, 13514, 13520, 13538, 13649, 13651, 13665, 13940, 13958, 13974, 14005, 14029, 14031, 14195,
	14340, 14358, 14374, 14406, 14432, 14596, 14743, 14777, 14852, 14866, 15156, 15162, 15213, 15227, 15383, 15555,
	15561, 15579, 15608, 15610, 15624, 15780, 15798, 15889, 15891, 15901, 15919, 15935, 16006, 16032, 16196, 16343,
	16377, 16405, 16429, 16431, 16595, 16740, 16758, 16774, 16849, 16851, 16865, 17155, 17161, 17179, 17208, 17210,
	17224, 17380, 17398, 17556, 17562, 17613, 17627, 17783, 17892, 17902, 17936, 18153, 18167, 18216, 18222, 18386,
	18548, 18550, 18564, 18615, 18621, 18639, 18785, 18894, 18904, 18928, 18930, 19000, 19018, 19034, 19188, 19190,
	19345, 19369, 19371, 19403, 19437, 19593, 19746, 19772, 19857, 19863, 20095, 20105, 20129, 20131, 20240, 20258,
	20274, 20496, 20506, 20532, 20643, 20677, 20952, 20966, 21056, 21062, 21283, 21313, 21327, 21455, 21461, 21479,
	21680, 21698, 21708, 21710, 21724, 21801, 21819, 21835, 21989, 21991, 22096, 22106, 22132, 22243, 22277, 22495,
	22505, 22529, 22531, 22640, 22658, 22674, 22949, 22951, 22965, 23055, 23061, 23079, 23280, 23298, 23308, 23310,
	23324, 23456, 23462, 23683, 23713, 23727, 23802, 23836, 23992, 24147, 24173, 24202, 24236, 24392, 24544, 24568,
	24570, 24601, 24619, 24635, 24789, 24791, 24880, 24898, 24908, 24910, 24924, 25014, 25020, 25038, 25184, 25349,
	25351, 25365, 25417, 25423, 25587, 25752, 25766, 25843, 25877, 26144, 26168, 26170, 26201, 26219, 26235, 26389,
	26391, 26547, 26573, 26602, 26636, 26792, 26883, 26913, 26927, 27017, 27023, 27187, 27352, 27366, 27414, 27420,
	27438, 27584, 27749, 27751, 27765, 27840, 27858, 27874, 28009, 28011, 28025, 28181, 28199, 28354, 28360, 28378,
	28412, 28426, 28582, 28757, 28763, 28846, 28872, 29142, 29176, 29207, 29233, 29397, 29541, 29559, 29575, 29604,
	29628, 29630, 29794, 29885, 29915, 29921, 29939, 30047, 30073, 30292, 30302, 30336, 30444, 30468, 30470, 30689,
	30691, 30701, 30719, 30735, 30808, 30810, 30824, 30980, 30998, 31084, 31114, 31120, 31138, 31249, 31251, 31265,
	31487, 31517, 31523, 31652, 31666, 31943, 31977, 32044, 32068, 32070, 32289, 32291, 32301, 32319, 32335, 32447,
	32473, 32692, 32702, 32736, 32813, 32827, 32983, 33087, 33117, 33123, 33252, 33266, 33484, 33514, 33520, 33538,
	33649, 33651, 33665, 33940, 33958, 33974, 34005, 34029, 34031, 34195, 34340, 34358, 34374, 34406, 34432, 34596,
	34743, 34777, 34852, 34866, 35156, 35162, 35213, 35227, 35383, 35555, 35561, 35579, 35608, 35610, 35624, 35780,
	35798, 35889, 35891, 35901, 35919, 35935, 36006, 36032, 36196, 36343, 36377, 36405, 36429, 36431, 36595, 36740,
	36758, 36774, 36849, 36851, 36865, 37155, 37161, 37179, 37208, 37210, 37224, 37380, 37398, 37556, 37562, 37613,
	37627, 37783, 37892, 37902, 37936, 38153, 38167, 38216, 38222, 38386, 38548, 38550, 38564, 38615, 38621, 38639,
	38785, 38894, 38904, 38928, 38930, 39000, 39018, 39034, 39188, 39190, 39345, 39369, 39371, 39403, 39437, 39593,
	39746, 39772, 39857, 39863, 40095, 40105, 40129, 40131, 40240, 40258, 40274, 40496, 40506, 40532, 40643, 40677,
	40952, 40966, 41056, 41062, 41283, 41313, 41327, 41455, 41461, 41479, 41680, 41698, 41708, 41710, 41724, 41801,
	41819, 41835, 41989, 41991, 42096, 42106, 42132, 42243, 42277, 42495, 42505, 42529, 42531, 42640, 42658, 42674,
	42949, 42951, 42965, 43055, 43061, 43079, 43280, 43298, 43308, 43310, 43324, 43456, 43462, 43683, 43713, 43727,
	43802, 43836, 43992, 44147, 44173, 44202, 44236, 44392, 44544, 44568, 44570, 44601, 44619, 44635, 44789, 44791,
	44880, 44898, 44908, 44910, 44924, 45014, 45020, 45038, 45184, 45349, 45351, 45365, 45417, 45423, 45587, 45752,
	45766, 45843, 45877, 46144, 46168, 46170, 46201, 46219, 46235, 46389, 46391, 46547, 46573, 46602, 46636, 46792,
	46883, 46913, 46927, 47017, 47023, 47187, 47352, 47366, 47414, 47420, 47438, 47584, 47749, 47751, 47765, 47840,
	47858, 47874, 48009, 48011, 48025, 48181, 48199, 48354, 48360, 48378, 48412, 48426, 48582, 48757, 48763, 48846,
	48872, 49142, 49176, 49207, 49233, 49397, 49541, 49559, 49575, 49604, 49628, 49630, 49794, 49885, 49915, 49921,
	49939, 50047, 50073, 50292, 50302, 50336, 50444, 50468, 50470, 50689, 50691, 50701, 50719, 50735, 50808, 50810,
	50824, 50980, 50998, 51084, 51114, 51120, 51138, 51249, 51251, 51265, 51487, 51517, 51523, 51652, 51666, 51943,
	51977, 52044, 52068, 52070, 52289, 52291, 52301, 52319, 52335, 52447, 52473, 52692, 52702, 52736, 52813, 52827,
	52983, 53087, 53117, 53123, 53252, 53266, 53484, 53514, 53520, 53538, 53649, 53651, 53665, 53940, 53958, 53974,
	54005, 54029, 54031, 54195, 54340, 54358, 54374, 54406, 54432, 54596, 54743, 54777, 54852, 54866, 55156, 55162,
	55213, 55227, 55383, 55555, 55561, 55579, 55608, 55610, 55624, 55780, 55798, 55889, 55891, 55901, 55919, 55935,
	56006, 56032, 56196, 56343, 56377, 56405, 56429, 56431, 56595, 56740, 56758, 56774, 56849, 56851, 56865, 57155,
	57161, 57179, 57208, 57210, 57224, 57380, 57398, 57556, 57562, 57613, 57627, 57783, 57892, 57902, 57936, 58153,
	58167, 58216, 58222, 58386, 58548, 58550, 58564, 58615, 58621, 58639, 58785, 58894, 58904, 58928, 58930, 59000,
	59018, 59034, 59188, 59190, 59345, 59369, 59371, 59403, 59437, 59593, 59746, 59772, 59857, 59863, 60095, 60105,
	60129, 60131, 60240, 60258, 60274, 60496, 60506, 60532, 60643, 60677, 60952, 60966, 61056, 61062, 61283, 61313,
	61327, 61455, 61461, 61479, 61680, 61698, 61708, 61710, 61724, 61801, 61819, 61835, 61989, 61991, 62096, 62106,
	62132, 62243, 62277, 62495, 62505, 62529, 62531, 62640, 62658, 62674, 62949, 62951, 62965, 63055, 63061, 63079,
	63280, 63298, 63308, 63310, 63324, 63456, 63462, 63683, 63713, 63727, 63802, 63836, 63992, 64147, 64173, 64202,
	64236, 64392, 64544, 64568, 64570, 64601, 64619, 64635, 64789, 64791, 64880, 64898, 64908, 64910, 64924, 65014,
}

func testIndexBucket() base.Bucket {
	bucket, err := ConnectToBucket(base.BucketSpec{
		Server:     "http://localhost:8091",
		BucketName: "channel_index"})
	/*
		bucket, err := ConnectToBucket(base.BucketSpec{
			Server:     "http://172.23.96.62:8091",
			BucketName: "channel_index"})
	*/
	/*
		bucket, err := ConnectToBucket(base.BucketSpec{
			Server:     "http://10.0.1.24:8091",
			BucketName: "channel_index"})
	*/
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
	return bucket
}

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
	_, err := c.indexBucket.GetRaw(loadFlagDoc)
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
	return fmt.Sprintf("_index::%s::%d::%05d", channelName, blockNumber, suffixMap[vb])
}

func (c *channelIndexTest) getIndexDocName(vb int) string {
	blockNumber := 0
	return getDocName(c.channelName, vb, blockNumber)
}

func (c *channelIndexTest) readIndexSingle() error {

	for i := 0; i < c.numVbuckets; i++ {
		key := c.getIndexDocName(i)
		body, err := c.indexBucket.GetRaw(key)
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
			body, err := c.indexBucket.GetRaw(key)
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
	responses, err := couchbaseBucket.GetBulk(keys)
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
	for i := 0; i < 100000; i++ {
		docId := fmt.Sprintf("basicChannel::index::indexDoc_%05d", suffixMap[i])
		vbNo := index.indexBucket.VBHash(docId)

		if vbNo == 4 {
			log.Printf("%d", i)
		}

		counts[vbNo] = counts[vbNo] + 1
		if counts[vbNo] > 1024 {
			log.Printf("your winner %d, at %d:", vbNo, i)
			break
		}
	}
	log.Printf("done")

}

func verifyVBMapping(bucket base.Bucket, channelName string) error {

	channelVbNo := uint32(0)

	for i := 0; i < 1024; i++ {
		docId := fmt.Sprintf("_index::%s::%d::%05d", channelName, 1, suffixMap[i])
		vbNo := bucket.VBHash(docId)
		if channelVbNo == 0 {
			channelVbNo = vbNo
			log.Printf("channel %s gets vb %d", channelName, channelVbNo)
		}
		if vbNo != channelVbNo {
			log.Println("Sad trombone - no match")
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
