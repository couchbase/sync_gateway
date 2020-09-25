package db

import (
	"bytes"
	"encoding/binary"
	"log"
	"sort"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
)

// TODO: Could consider checking this in as a file and include it into the compiled test binary using something like https://github.com/jteeuwen/go-bindata
// Review if we need larger test docs in future.
var doc_1k = `{
    "index": 0,
    "guid": "bc22f4d5-e13f-4b64-9397-2afd5a843c4d",
    "isActive": false,
    "balance": "$1,168.62",
    "picture": "http://placehold.it/32x32",
    "age": 20,
    "eyeColor": "green",
    "name": "Miranda Kline",
    "company": "COMTREK",
    "email": "mirandakline@comtrek.com",
    "phone": "+1 (831) 408-2162",
    "address": "701 Devon Avenue, Ballico, Alabama, 9673",
    "about": "Minim ea esse dolor ex laborum do velit cupidatat tempor do qui. Aliqua consequat consectetur esse officia ullamco velit labore irure ea non proident. Tempor elit nostrud deserunt in ullamco pariatur enim pariatur et. Veniam fugiat ad mollit ut mollit aute adipisicing aliquip veniam consectetur incididunt. Id cupidatat duis cupidatat quis amet elit sit sit esse velit pariatur do. Excepteur tempor labore esse adipisicing laborum sit enim incididunt quis sint fugiat commodo Lorem. Dolore laboris quis ex do.\r\n",
    "registered": "2016-09-16T12:08:17 +07:00",
    "latitude": -14.616751,
    "longitude": 175.689016,
    "channels": [
      "channel_1",
      "channel_2",
      "channel_3",
      "channel_4",
      "channel_5",
      "channel_6",
      "channel_7"
    ],
    "friends": [
      {
        "id": 0,
        "name": "Wise Hewitt"
      },
      {
        "id": 1,
        "name": "Winnie Schultz"
      },
      {
        "id": 2,
        "name": "Browning Carlson"
      }
    ],
    "greeting": "Hello, Miranda Kline! You have 4 unread messages.",
    "favoriteFruit": "strawberry"
  }`

var doc_meta = `{
    "rev": "3-89758294abc63157354c2b08547c2d21",
    "sequence": 7,
    "recent_sequences": [
      5,
      6,
      7
    ],
    "history": {
      "revs": [
        "1-fc591a068c153d6c3d26023d0d93dcc1",
        "2-0eab03571bc55510c8fc4bfac9fe4412",
        "3-89758294abc63157354c2b08547c2d21"
      ],
      "parents": [
        -1,
        0,
        1
      ],
      "channels": [
        [
          "ABC",
          "DEF"
        ],
        [
          "ABC",
          "DEF",
          "GHI"
        ],
        [
          "ABC",
          "GHI"
        ]
      ]
    },
    "channels": {
      "ABC": null,
      "DEF": {
        "seq": 7,
        "rev": "3-89758294abc63157354c2b08547c2d21"
      },
      "GHI": null
    },
    "cas": "",
    "time_saved": "2017-10-25T12:45:29.622450174-07:00"
  }`

func BenchmarkDocUnmarshal(b *testing.B) {

	doc1k_body := []byte(doc_1k)
	doc1k_meta := []byte(doc_meta)

	unmarshalBenchmarks := []struct {
		name           string
		unmarshalLevel DocumentUnmarshalLevel
	}{
		{"All", DocUnmarshalAll},
		{"Sync", DocUnmarshalSync},
		{"NoHistory", DocUnmarshalNoHistory},
		{"Rev", DocUnmarshalRev},
		{"CAS", DocUnmarshalCAS},
		{"None", DocUnmarshalNone},
	}

	for _, bm := range unmarshalBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_, _ = unmarshalDocumentWithXattr("doc_1k", doc1k_body, doc1k_meta, 1, bm.unmarshalLevel)
			}
		})
	}
}

func BenchmarkUnmarshalBody(b *testing.B) {
	doc1k_body := []byte(doc_1k)
	unmarshalBenchmarks := []struct {
		name           string
		useDecode      bool
		fixJSONNumbers bool
	}{
		{"Decode", true, false},
		{"DecodeUseNumber", true, true},
		{"UnmarshalAndFixNumbers", false, true},
		{"Unmarshal", false, false},
	}

	for _, bm := range unmarshalBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				doc := NewDocument("testDocID")
				docReader := bytes.NewReader(doc1k_body)
				b.StartTimer()
				var err error
				if bm.useDecode {
					//decoder := base.JSONDecoder(bytes.NewReader(doc1k_body))
					decoder := base.JSONDecoder(docReader)
					if bm.fixJSONNumbers {
						decoder.UseNumber()
					}
					err = decoder.Decode(&doc._body)
				} else {
					err = base.JSONUnmarshal(doc1k_body, &doc._body)
					if bm.fixJSONNumbers {
						doc.Body().FixJSONNumbers()
					}
				}
				b.StopTimer()
				if err != nil {
					log.Printf("Unmarshal error: %s", err)
				}

				if len(doc.Body()) == 0 {
					log.Printf("Empty body")
				}

			}
		})
	}
}

func TestParseXattr(t *testing.T) {
	zeroByte := byte(0)
	// Build payload for single xattr pair and body
	xattrValue := `{"seq":1}`
	xattrPairLength := 4 + len(base.SyncXattrName) + len(xattrValue) + 2
	xattrTotalLength := xattrPairLength
	body := `{"value":"ABC"}`

	// Build up the dcp Body
	dcpBody := make([]byte, 8)
	binary.BigEndian.PutUint32(dcpBody[0:4], uint32(xattrTotalLength))
	binary.BigEndian.PutUint32(dcpBody[4:8], uint32(xattrPairLength))
	dcpBody = append(dcpBody, base.SyncXattrName...)
	dcpBody = append(dcpBody, zeroByte)
	dcpBody = append(dcpBody, xattrValue...)
	dcpBody = append(dcpBody, zeroByte)
	dcpBody = append(dcpBody, body...)

	resultBody, resultXattr, err := parseXattrStreamData(base.SyncXattrName, dcpBody)
	assert.NoError(t, err, "Unexpected error parsing dcp body")
	goassert.Equals(t, string(resultBody), string(body))
	goassert.Equals(t, string(resultXattr), string(xattrValue))

	// Attempt to retrieve non-existent xattr
	resultBody, resultXattr, err = parseXattrStreamData("nonexistent", dcpBody)
	assert.NoError(t, err, "Unexpected error parsing dcp body")
	goassert.Equals(t, string(resultBody), string(body))
	goassert.Equals(t, string(resultXattr), "")

	// Attempt to retrieve xattr from empty dcp body
	emptyBody, emptyXattr, emptyErr := parseXattrStreamData(base.SyncXattrName, []byte{})
	goassert.Equals(t, emptyErr, base.ErrEmptyMetadata)
	assert.True(t, emptyBody == nil, "Nil body expected")
	assert.True(t, emptyXattr == nil, "Nil xattr expected")
}

func TestParseDocumentCas(t *testing.T) {
	syncData := &SyncData{}
	syncData.Cas = "0x00002ade734fb714"

	casInt := syncData.GetSyncCas()

	goassert.Equals(t, casInt, uint64(1492749160563736576))
}

func TestPruneRevisionsWithDisconnected(t *testing.T) {
	revTree := RevTree{
		"100-abc": {ID: "100-abc"},
		"101-def": {ID: "101-def", Parent: "100-abc", Deleted: true},
		"101-abc": {ID: "101-abc", Parent: "100-abc"},
		"102-abc": {ID: "102-abc", Parent: "101-abc"},
		"103-def": {ID: "103-def", Parent: "102-abc", Deleted: true},
		"103-abc": {ID: "103-abc", Parent: "102-abc"},
		"104-abc": {ID: "104-abc", Parent: "103-abc"},
		"105-abc": {ID: "105-abc", Parent: "104-abc"},
		"106-def": {ID: "106-def", Parent: "105-abc", Deleted: true},
		"106-abc": {ID: "106-abc", Parent: "105-abc"},
		"107-abc": {ID: "107-abc", Parent: "106-abc"},

		"1-abc": {ID: "1-abc"},
		"2-abc": {ID: "2-abc", Parent: "1-abc"},
		"3-abc": {ID: "3-abc", Parent: "2-abc"},
		"4-abc": {ID: "4-abc", Parent: "3-abc", Deleted: true},

		"70-abc": {ID: "70-abc"},
		"71-abc": {ID: "71-abc", Parent: "70-abc"},
		"72-abc": {ID: "72-abc", Parent: "71-abc"},
		"73-abc": {ID: "73-abc", Parent: "72-abc", Deleted: true},
	}

	prunedCount, _ := revTree.pruneRevisions(4, "")
	assert.Equal(t, 10, prunedCount)

	remainingKeys := make([]string, 0, len(revTree))
	for key := range revTree {
		remainingKeys = append(remainingKeys, key)
	}
	sort.Strings(remainingKeys)

	assert.Equal(t, []string{"101-abc", "102-abc", "103-abc", "103-def", "104-abc", "105-abc", "106-abc", "106-def", "107-abc"}, remainingKeys)
}
