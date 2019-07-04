package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
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
				doc, err := unmarshalDocumentWithXattr("doc_1k", doc1k_body, doc1k_meta, 1, bm.unmarshalLevel)
				b.StopTimer()
				if err != nil {
					log.Printf("Unexpected error during unmarshal: %s", err)
				} else if doc == nil {
					log.Printf("Post-unmarshal, document is nil.")
				}
				b.StartTimer()
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
				doc := newDocument("testDocID")
				docReader := bytes.NewReader(doc1k_body)
				b.StartTimer()
				var err error
				if bm.useDecode {
					//decoder := json.NewDecoder(bytes.NewReader(doc1k_body))
					decoder := json.NewDecoder(docReader)
					if bm.fixJSONNumbers {
						decoder.UseNumber()
					}
					err = decoder.Decode(&doc._body)
				} else {
					err = json.Unmarshal(doc1k_body, &doc._body)
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
	xattrKey := "_sync"
	xattrValue := `{"seq":1}`
	xattrPairLength := 4 + len(xattrKey) + len(xattrValue) + 2
	xattrTotalLength := xattrPairLength
	body := `{"value":"ABC"}`

	// Build up the dcp Body
	dcpBody := make([]byte, 8)
	binary.BigEndian.PutUint32(dcpBody[0:4], uint32(xattrTotalLength))
	binary.BigEndian.PutUint32(dcpBody[4:8], uint32(xattrPairLength))
	dcpBody = append(dcpBody, xattrKey...)
	dcpBody = append(dcpBody, zeroByte)
	dcpBody = append(dcpBody, xattrValue...)
	dcpBody = append(dcpBody, zeroByte)
	dcpBody = append(dcpBody, body...)

	resultBody, resultXattr, err := parseXattrStreamData("_sync", dcpBody)
	assert.NoError(t, err, "Unexpected error parsing dcp body")
	goassert.Equals(t, string(resultBody), string(body))
	goassert.Equals(t, string(resultXattr), string(xattrValue))

	// Attempt to retrieve non-existent xattr
	resultBody, resultXattr, err = parseXattrStreamData("nonexistent", dcpBody)
	assert.NoError(t, err, "Unexpected error parsing dcp body")
	goassert.Equals(t, string(resultBody), string(body))
	goassert.Equals(t, string(resultXattr), "")

	// Attempt to retrieve xattr from empty dcp body
	emptyBody, emptyXattr, emptyErr := parseXattrStreamData("_sync", []byte{})
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
