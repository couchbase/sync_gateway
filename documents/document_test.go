/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package documents

import (
	"bytes"
	"encoding/binary"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
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

func TestDocMarshalChannels(t *testing.T) {
	doc1k_body := []byte(doc_1k)
	doc1k_meta := []byte(doc_meta)
	doc, err := UnmarshalDocumentWithXattr("doc_1k", doc1k_body, doc1k_meta, nil, 1, DocUnmarshalAll)
	assert.NoError(t, err)
	assert.NotNil(t, doc)

	assert.NotNil(t, doc.GetChannels())

	data, err := doc.MarshalBodyAndSync()
	assert.NoError(t, err)

	doc2, err := UnmarshalDocument("doc_1k", data)
	assert.NoError(t, err)
	assert.NotNil(t, doc)

	assert.Equal(t, doc.GetChannels(), doc2.GetChannels())
}

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
				_, _ = UnmarshalDocumentWithXattr("doc_1k", doc1k_body, doc1k_meta, nil, 1, bm.unmarshalLevel)
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
				var body Body
				var err error
				if bm.useDecode {
					// decoder := base.JSONDecoder(bytes.NewReader(doc1k_body))
					decoder := base.JSONDecoder(docReader)
					if bm.fixJSONNumbers {
						decoder.UseNumber()
					}
					err = decoder.Decode(&body)
				} else {
					err = base.JSONUnmarshal(doc1k_body, &body)
					if bm.fixJSONNumbers {
						doc.UnmarshalBody().fixJSONNumbers()
					}
				}
				b.StopTimer()
				if err != nil {
					log.Printf("Unmarshal error: %s", err)
				}

				if body == nil {
					log.Printf("Empty body")
				}

			}
		})
	}
}

// Version of fixJSONNumbers (see base/util.go) that operates on a Body
func (body Body) fixJSONNumbers() {
	for k, v := range body {
		body[k] = base.FixJSONNumbers(v)
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

	resultBody, resultXattr, _, err := ParseXattrStreamData(base.SyncXattrName, "", dcpBody)
	assert.NoError(t, err, "Unexpected error parsing dcp body")
	assert.Equal(t, string(body), string(resultBody))
	assert.Equal(t, string(xattrValue), string(resultXattr))

	// Attempt to retrieve non-existent xattr
	resultBody, resultXattr, _, err = ParseXattrStreamData("nonexistent", "", dcpBody)
	assert.NoError(t, err, "Unexpected error parsing dcp body")
	assert.Equal(t, string(body), string(resultBody))
	assert.Equal(t, "", string(resultXattr))

	// Attempt to retrieve xattr from empty dcp body
	emptyBody, emptyXattr, _, emptyErr := ParseXattrStreamData(base.SyncXattrName, "", []byte{})
	assert.Equal(t, base.ErrEmptyMetadata, emptyErr)
	assert.True(t, emptyBody == nil, "Nil body expected")
	assert.True(t, emptyXattr == nil, "Nil xattr expected")
}

func TestParseDocumentCas(t *testing.T) {
	syncData := &SyncData{}
	syncData.Cas = "0x00002ade734fb714"

	casInt := syncData.GetSyncCas()

	assert.Equal(t, uint64(1492749160563736576), casInt)
}

func TestGetDeepMutableBody(t *testing.T) {
	testCases := []struct {
		name        string
		inputDoc    *Document
		expectError bool
		expected    Body
	}{
		{
			name:        "Empty doc",
			inputDoc:    &Document{},
			expectError: false,
			expected:    nil,
		},
		{
			name:        "Raw body",
			inputDoc:    &Document{_rawBody: []byte(`{"test": true}`)},
			expectError: false,
			expected:    Body{"test": true},
		},
		{
			name:        "Malformed raw body",
			inputDoc:    &Document{_rawBody: []byte(`{test: true}`)},
			expectError: true,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			body, err := test.inputDoc.GetDeepMutableBody()
			if test.expectError {
				assert.Error(t, err)
				assert.Nil(t, body)
			} else {
				if assert.Nil(t, err) {
					assert.Equal(t, test.expected, body)
				}
			}
		})
	}
}

func TestParseDocumentRevision(t *testing.T) {
	expectedExpiry := base.CbsExpiryToTime(12345678)

	cases := []struct {
		name, input string
		expectedRev *DocumentRevision
		error       string
	}{
		{
			name:  "Empty",
			input: "{ }",
			expectedRev: &DocumentRevision{
				_bodyBytes: []byte(`{}`),
			},
		},
		{
			name:  "One regular",
			input: `{"foo":1234}`,
			expectedRev: &DocumentRevision{
				_bodyBytes: []byte(`{"foo":1234}`),
			},
		},
		{
			name:  "Two regular",
			input: `{"foo":1234,"bar":{"a": 1, "z": 26}}`,
			expectedRev: &DocumentRevision{
				_bodyBytes: []byte(`{"foo":1234,"bar":{"a": 1, "z": 26}}`),
			},
		},
		{
			name:  "One underscored",
			input: `{"_id":"Hey"}`,
			expectedRev: &DocumentRevision{
				DocID:      "Hey",
				_bodyBytes: []byte(`{}`),
			},
		},
		{
			name:  "Two underscored",
			input: `{"_id":"Hey","_attachments":{"a": {"length":1}}}`,
			expectedRev: &DocumentRevision{
				DocID:       "Hey",
				Attachments: AttachmentsMeta{"a": &DocAttachment{Length: 1}},
				_bodyBytes:  []byte(`{}`),
			},
		},
		{
			name:  "Regular and underscored",
			input: `{"_id":"Hey","foo":1234,"bar":{"a": 1, "z": 26}, "_rev": "1-abcd"}`,
			expectedRev: &DocumentRevision{
				DocID:      "Hey",
				RevID:      "1-abcd",
				_bodyBytes: []byte(`{"foo":1234,"bar":{"a": 1, "z": 26}}`),
			},
		},

		{
			name:  "Expiry",
			input: `{"_id": "Hey", "_exp": 12345678}`,
			expectedRev: &DocumentRevision{
				DocID:      "Hey",
				Expiry:     &expectedExpiry,
				_bodyBytes: []byte(`{}`),
			},
		},

		// Errors:
		{
			name:  "Not an object",
			input: "[]",
			error: "expected an object",
		},
		{
			name:  "Duplicate key",
			input: `{"_id": "doc1","_id":"fake"}`,
			error: "duplicate key",
		},
		{
			name:  "Illegal key",
			input: `{"_id": "foo", "_purged":"hi"}`,
			error: `'_purged' is a reserved internal property`,
		},
		{
			name:  "Illegal key prefix",
			input: `{"_id": "foo", "_sync_whoa":"hi"}`,
			error: `'_sync_whoa' is a reserved internal property`,
		},
		{
			name:  "Wrong type",
			input: `{"_id": "foo", "_rev":false}`,
			error: `invalid value type for special key "_rev"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rev, err := ParseDocumentRevision([]byte(tc.input), BodyId, BodyRev, BodyExpiry, BodyAttachments)

			if tc.error != "" {
				assert.ErrorContains(t, err, tc.error)
				log.Printf("--> %s", err)
				return
			} else if !assert.NoError(t, err) {
				return
			}
			log.Printf("--> %#v", rev)
			assert.Equal(t, *tc.expectedRev, rev)
		})
	}
}

func TestDocumentRevisionBodyBytes(t *testing.T) {
	expiry := base.CbsExpiryToTime(0x7FFFFFFF)
	rev := DocumentRevision{
		DocID:      "wassup",
		RevID:      "5-6789",
		Expiry:     &expiry,
		Deleted:    true,
		_bodyBytes: []byte(`{"foo":false}`),
	}

	assert.Equal(t, `{"foo":false}`, string(rev.BodyBytes()))

	j, err := rev.BodyBytesWith(BodyId, BodyDeleted, BodyRev, BodyExpiry, BodyRemoved)
	if assert.NoError(t, err) {
		assert.Equal(t, `{"foo":false,"_id":"wassup","_deleted":true,"_rev":"5-6789","_exp":"2038-01-18T19:14:07-08:00"}`, string(j))
	}

	rev.Expiry = nil
	j, err = rev.BodyBytesWith(BodyExpiry)
	if assert.NoError(t, err) {
		assert.Equal(t, `{"foo":false}`, string(j))
	}
}

func TestDocumentHasNonEmptyBody(t *testing.T) {
	nilDoc := Document{_rawBody: nil}
	assert.False(t, nilDoc.HasBody())
	assert.False(t, nilDoc.HasNonEmptyBody())

	emptyDoc := Document{_rawBody: []byte{}}
	assert.True(t, emptyDoc.HasBody())
	assert.False(t, emptyDoc.HasNonEmptyBody())

	emptyObjDoc := Document{_rawBody: []byte(`{}`)}
	assert.True(t, emptyObjDoc.HasBody())
	assert.False(t, emptyObjDoc.HasNonEmptyBody())

	emptyObjDoc = Document{_rawBody: []byte(`{ }`)}
	assert.True(t, emptyObjDoc.HasBody())
	assert.False(t, emptyObjDoc.HasNonEmptyBody())

	emptyObjDoc = Document{_rawBody: []byte("\t\t{\n } ")}
	assert.True(t, emptyObjDoc.HasBody())
	assert.False(t, emptyObjDoc.HasNonEmptyBody())

	nonEmptyDoc := Document{_rawBody: []byte(`{"x":0}`)}
	assert.True(t, nonEmptyDoc.HasBody())
	assert.True(t, nonEmptyDoc.HasNonEmptyBody())
}
