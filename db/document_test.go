/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"bytes"
	"encoding/binary"
	"log"
	"reflect"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			ctx := base.TestCtx(b)
			for i := 0; i < b.N; i++ {
				_, _ = unmarshalDocumentWithXattrs(ctx, "doc_1k", doc1k_body, doc1k_meta, nil, nil, nil, nil, nil, 1, bm.unmarshalLevel)
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
			ctx := base.TestCtx(b)
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				doc := NewDocument("testDocID")
				docReader := bytes.NewReader(doc1k_body)
				b.StartTimer()
				var err error
				if bm.useDecode {
					// decoder := base.JSONDecoder(bytes.NewReader(doc1k_body))
					decoder := base.JSONDecoder(docReader)
					if bm.fixJSONNumbers {
						decoder.UseNumber()
					}
					err = decoder.Decode(&doc._body)
				} else {
					err = base.JSONUnmarshal(doc1k_body, &doc._body)
					if bm.fixJSONNumbers {
						doc.Body(ctx).FixJSONNumbers()
					}
				}
				b.StopTimer()
				if err != nil {
					log.Printf("Unmarshal error: %s", err)
				}

				if len(doc.Body(ctx)) == 0 {
					log.Printf("Empty body")
				}

			}
		})
	}
}

const doc_meta_no_vv = `{
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

const doc_meta_vv = `{"cvCas":"0x40e2010000000000","src":"cb06dc003846116d9b66d2ab23887a96","ver":"0x40e2010000000000",
	"mv":["c0ff05d7ac059a16@s_LhRPsa7CpjEvP5zeXTXEBA","1c008cd6@s_NqiIe0LekFPLeX4JvTO6Iw"],
	"pv":["f0ff44d6ac059a16@s_YZvBpEaztom9z5V/hDoeIw"]
}`

func TestParseVersionVectorSyncData(t *testing.T) {
	mv := make(HLVVersions)
	pv := make(HLVVersions)
	mv["s_LhRPsa7CpjEvP5zeXTXEBA"] = 1628620455147864000
	mv["s_NqiIe0LekFPLeX4JvTO6Iw"] = 1628620458747363292
	pv["s_YZvBpEaztom9z5V/hDoeIw"] = 1628620455135215600

	ctx := base.TestCtx(t)

	sync_meta := []byte(doc_meta_no_vv)
	vv_meta := []byte(doc_meta_vv)
	doc, err := unmarshalDocumentWithXattrs(ctx, "doc_1k", nil, sync_meta, vv_meta, nil, nil, nil, nil, 1, DocUnmarshalNoHistory)
	require.NoError(t, err)

	vrsCAS := uint64(123456)
	// assert on doc version vector values
	assert.Equal(t, vrsCAS, doc.SyncData.HLV.CurrentVersionCAS)
	assert.Equal(t, vrsCAS, doc.SyncData.HLV.Version)
	assert.Equal(t, "cb06dc003846116d9b66d2ab23887a96", doc.SyncData.HLV.SourceID)
	assert.True(t, reflect.DeepEqual(mv, doc.SyncData.HLV.MergeVersions))
	assert.True(t, reflect.DeepEqual(pv, doc.SyncData.HLV.PreviousVersions))

	doc, err = unmarshalDocumentWithXattrs(ctx, "doc1", nil, sync_meta, vv_meta, nil, nil, nil, nil, 1, DocUnmarshalAll)
	require.NoError(t, err)

	// assert on doc version vector values
	assert.Equal(t, vrsCAS, doc.SyncData.HLV.CurrentVersionCAS)
	assert.Equal(t, vrsCAS, doc.SyncData.HLV.Version)
	assert.Equal(t, "cb06dc003846116d9b66d2ab23887a96", doc.SyncData.HLV.SourceID)
	assert.True(t, reflect.DeepEqual(mv, doc.SyncData.HLV.MergeVersions))
	assert.True(t, reflect.DeepEqual(pv, doc.SyncData.HLV.PreviousVersions))

	doc, err = unmarshalDocumentWithXattrs(ctx, "doc1", nil, sync_meta, vv_meta, nil, nil, nil, nil, 1, DocUnmarshalNoHistory)
	require.NoError(t, err)

	// assert on doc version vector values
	assert.Equal(t, vrsCAS, doc.SyncData.HLV.CurrentVersionCAS)
	assert.Equal(t, vrsCAS, doc.SyncData.HLV.Version)
	assert.Equal(t, "cb06dc003846116d9b66d2ab23887a96", doc.SyncData.HLV.SourceID)
	assert.True(t, reflect.DeepEqual(mv, doc.SyncData.HLV.MergeVersions))
	assert.True(t, reflect.DeepEqual(pv, doc.SyncData.HLV.PreviousVersions))
}

const doc_meta_vv_corrupt = `{"cvCas":"0x40e2010000000000","src":"cb06dc003846116d9b66d2ab23887a96","ver":"0x40e2010000000000",
	"mv":["c0ff05d7ac059a16@s_LhRPsa7CpjEvP5zeXTXEBA","1c008cd61c008cd61c008cd6@s_NqiIe0LekFPLeX4JvTO6Iw"],
	"pv":["f0ff44d6ac059a16@s_YZvBpEaztom9z5V/hDoeIw"]
}`

func TestParseVersionVectorCorruptDelta(t *testing.T) {

	ctx := base.TestCtx(t)

	sync_meta := []byte(doc_meta_no_vv)
	vv_meta := []byte(doc_meta_vv_corrupt)
	_, err := unmarshalDocumentWithXattrs(ctx, "doc1", nil, sync_meta, vv_meta, nil, nil, nil, nil, 1, DocUnmarshalAll)
	require.Error(t, err)

}

// TestRevAndVersion tests marshalling and unmarshalling rev and current version
func TestRevAndVersion(t *testing.T) {

	ctx := base.TestCtx(t)
	testCases := []struct {
		testName  string
		revTreeID string
		source    string
		version   string
	}{
		{
			testName:  "rev_and_version",
			revTreeID: "1-abc",
			source:    "source1",
			version:   "0x0100000000000000",
		},
		{
			testName:  "both_empty",
			revTreeID: "",
			source:    "",
			version:   "0",
		},
		{
			testName:  "revTreeID_only",
			revTreeID: "1-abc",
			source:    "",
			version:   "0",
		},
		{
			testName:  "currentVersion_only",
			revTreeID: "",
			source:    "source1",
			version:   "0x0100000000000000",
		},
	}

	var expectedSequence = uint64(100)
	for _, test := range testCases {
		t.Run(test.testName, func(t *testing.T) {
			syncData := &SyncData{
				CurrentRev: test.revTreeID,
				Sequence:   expectedSequence,
			}
			if test.source != "" {
				syncData.HLV = &HybridLogicalVector{
					SourceID: test.source,
					Version:  base.HexCasToUint64(test.version),
				}
			}
			// SyncData test
			marshalledSyncData, err := base.JSONMarshal(syncData)
			require.NoError(t, err)
			log.Printf("marshalled:%s", marshalledSyncData)

			// Document test
			document := NewDocument("docID")
			document.SyncData.CurrentRev = test.revTreeID
			document.SyncData.Sequence = expectedSequence
			document.SyncData.HLV = &HybridLogicalVector{
				SourceID: test.source,
				Version:  base.HexCasToUint64(test.version),
			}

			marshalledDoc, marshalledXattr, marshalledVvXattr, _, _, err := document.MarshalWithXattrs()
			require.NoError(t, err)

			newDocument := NewDocument("docID")
			err = newDocument.UnmarshalWithXattrs(ctx, marshalledDoc, marshalledXattr, marshalledVvXattr, nil, nil, DocUnmarshalAll)
			require.NoError(t, err)
			require.Equal(t, test.revTreeID, newDocument.CurrentRev)
			require.Equal(t, expectedSequence, newDocument.Sequence)
			if test.source != "" {
				require.NotNil(t, newDocument.HLV)
				require.Equal(t, test.source, newDocument.HLV.SourceID)
				require.Equal(t, base.HexCasToUint64(test.version), newDocument.HLV.Version)
			}
			//require.Equal(t, test.expectedCombinedVersion, newDocument.RevAndVersion)
		})
	}
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
		expected    *Body
	}{
		{
			name:        "Empty doc",
			inputDoc:    &Document{},
			expectError: true,
			expected:    nil,
		},
		{
			name:        "Raw body",
			inputDoc:    &Document{_rawBody: []byte(`{"test": true}`)},
			expectError: false,
			expected:    &Body{"test": true},
		},
		{
			name:        "Malformed raw body",
			inputDoc:    &Document{_rawBody: []byte(`{test: true}`)},
			expectError: true,
			expected:    nil,
		},
		{
			name:        "Body, no raw body",
			inputDoc:    &Document{_body: Body{"test": true}},
			expectError: false,
			expected:    &Body{"test": true},
		},
		{
			name:        "Inline function in body, no raw body",
			inputDoc:    &Document{_body: Body{"test": func() bool { return true }}},
			expectError: true,
			expected:    nil,
		},
		{
			name:        "Body and raw body",
			inputDoc:    &Document{_rawBody: []byte(`{"test": true}`), _body: Body{"notTest": false}},
			expectError: false,
			expected:    &Body{"test": true},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			body, err := test.inputDoc.GetDeepMutableBody()
			if test.expectError {
				assert.Error(t, err)
				assert.Nil(t, body)
				return
			}
			assert.Nil(t, err)
			assert.Equal(t, *test.expected, body)
		})
	}
}

func TestDCPDecodeValue(t *testing.T) {
	testCases := []struct {
		name              string
		body              []byte
		expectedErr       error
		expectedBody      []byte
		expectedSyncXattr []byte
	}{
		{
			name:        "bad value",
			body:        []byte("abcde"),
			expectedErr: sgbucket.ErrXattrInvalidLen,
		},
		{
			name:        "xattr length 4, overflow",
			body:        []byte{0x00, 0x00, 0x00, 0x04, 0x01},
			expectedErr: sgbucket.ErrXattrInvalidLen,
		},
		{
			name:        "empty",
			body:        nil,
			expectedErr: sgbucket.ErrEmptyMetadata,
		},
		{
			name:              "single xattr pair and body",
			body:              getSingleXattrDCPBytes(),
			expectedBody:      []byte(`{"value":"ABC"}`),
			expectedSyncXattr: []byte(`{"seq":1}`),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// DecodeValueWithXattrs is the underlying function
			body, xattrs, err := sgbucket.DecodeValueWithXattrs([]string{"_sync"}, test.body)
			require.ErrorIs(t, err, test.expectedErr)
			require.Equal(t, test.expectedBody, body)
			if test.expectedSyncXattr != nil {
				require.Len(t, xattrs, 1)
				require.Equal(t, test.expectedSyncXattr, xattrs["_sync"])
			} else {
				require.Nil(t, xattrs)
			}
			// UnmarshalDocumentSyncData wraps DecodeValueWithXattrs
			result, rawBody, rawXattrs, err := UnmarshalDocumentSyncDataFromFeed(test.body, base.MemcachedDataTypeXattr, "", false)
			require.ErrorIs(t, err, test.expectedErr)
			if test.expectedSyncXattr != nil {
				require.NotNil(t, result)
				require.Equal(t, test.expectedSyncXattr, rawXattrs[base.SyncXattrName])
			} else {
				require.Nil(t, result)
			}
			require.Equal(t, test.expectedBody, rawBody)
		})
	}
}

// TestInvalidXattrStreamEmptyBody tests is a bit different than cases in TestDCPDecodeValue since DecodeValueWithXattrs will pass but UnmarshalDocumentSyncDataFromFeed will fail due to invalid json.
func TestInvalidXattrStreamEmptyBody(t *testing.T) {
	inputStream := []byte{0x00, 0x00, 0x00, 0x01, 0x01}
	emptyBody := []byte{}

	// DecodeValueWithXattrs is the underlying function
	body, xattrs, err := sgbucket.DecodeValueWithXattrs([]string{base.SyncXattrName}, inputStream)
	require.NoError(t, err)
	require.Equal(t, emptyBody, body)
	require.Empty(t, xattrs)

	// UnmarshalDocumentSyncData wraps DecodeValueWithXattrs
	result, rawBody, rawXattrs, err := UnmarshalDocumentSyncDataFromFeed(inputStream, base.MemcachedDataTypeXattr, "", false)
	require.NoError(t, err) // body will be nil, no xattrs are found
	require.Nil(t, result)
	require.Equal(t, emptyBody, rawBody)
	require.Empty(t, rawXattrs)

}

// getSingleXattrDCPBytes returns a DCP body with a single xattr pair and body
func getSingleXattrDCPBytes() []byte {
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
	return dcpBody
}

const syncDataWithAttachment = `{
      "attachments": {
        "bye.txt": {
          "digest": "sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=",
          "length": 19,
          "revpos": 1,
          "stub": true,
          "ver": 2
        },
        "hello.txt": {
          "digest": "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
          "length": 11,
          "revpos": 1,
          "stub": true,
          "ver": 2
        }
      },
      "cas": "0x0000d2ba4104f217",
      "channel_set": [
        {
          "name": "sg_test_0",
          "start": 1
        }
      ],
      "channel_set_history": null,
      "channels": {
        "sg_test_0": null
      },
      "cluster_uuid": "6eca6cdd1ffcd7b2b7ea07039e68a774",
      "history": {
        "channels": [
          [
            "sg_test_0"
          ]
        ],
        "parents": [
          -1
        ],
        "revs": [
          "1-ca9ad22802b66f662ff171f226211d5c"
        ]
      },
      "recent_sequences": [
        1
      ],
      "rev": {
        "rev": "1-ca9ad22802b66f662ff171f226211d5c",
        "src": "RS1pdSMRlrNr0Ns0oOfc8A",
        "ver": "0x0000d2ba4104f217"
      },
      "sequence": 1,
      "time_saved": "2024-09-04T11:38:05.093225+01:00",
      "value_crc32c": "0x297bd0aa"
    }`

const globalXattr = `{
      "attachments_meta": {
        "bye.txt": {
          "digest": "sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=",
          "length": 19,
          "revpos": 1,
          "stub": true,
          "ver": 2
        },
        "hello.txt": {
          "digest": "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=",
          "length": 11,
          "revpos": 1,
          "stub": true,
          "ver": 2
        }
      }
    }`

// TestAttachmentReadStoredInXattr tests reads legacy format for attachments being stored in sync data xattr as well as
// testing the new location for attachments in global xattr
func TestAttachmentReadStoredInXattr(t *testing.T) {
	ctx := base.TestCtx(t)

	// unmarshal attachments on sync data
	testSync := []byte(syncDataWithAttachment)
	doc, err := unmarshalDocumentWithXattrs(ctx, "doc1", nil, testSync, nil, nil, nil, nil, nil, 1, DocUnmarshalSync)
	require.NoError(t, err)

	// assert on attachments
	atts := doc.Attachments()
	assert.Len(t, atts, 2)
	hello := atts["hello.txt"].(map[string]interface{})
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(1), hello["revpos"])
	assert.Equal(t, float64(2), hello["ver"])
	assert.True(t, hello["stub"].(bool))

	bye := atts["bye.txt"].(map[string]interface{})
	assert.Equal(t, "sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=", bye["digest"])
	assert.Equal(t, float64(19), bye["length"])
	assert.Equal(t, float64(1), bye["revpos"])
	assert.Equal(t, float64(2), bye["ver"])
	assert.True(t, bye["stub"].(bool))

	// unmarshal attachments on global data
	testGlobal := []byte(globalXattr)
	sync_meta_no_attachments := []byte(doc_meta_no_vv)
	doc, err = unmarshalDocumentWithXattrs(ctx, "doc1", nil, sync_meta_no_attachments, nil, nil, nil, nil, testGlobal, 1, DocUnmarshalSync)
	require.NoError(t, err)

	// assert on attachments
	atts = doc.Attachments()
	assert.Len(t, atts, 2)
	hello = atts["hello.txt"].(map[string]interface{})
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(1), hello["revpos"])
	assert.Equal(t, float64(2), hello["ver"])
	assert.True(t, hello["stub"].(bool))

	bye = atts["bye.txt"].(map[string]interface{})
	assert.Equal(t, "sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=", bye["digest"])
	assert.Equal(t, float64(19), bye["length"])
	assert.Equal(t, float64(1), bye["revpos"])
	assert.Equal(t, float64(2), bye["ver"])
	assert.True(t, bye["stub"].(bool))
}
