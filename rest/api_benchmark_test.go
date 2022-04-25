/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

var doc_1k_format = `{%s
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

func BenchmarkReadOps_Get(b *testing.B) {

	base.DisableTestLogging(b)

	rt := NewRestTester(b, nil)
	defer rt.Close()
	defer PurgeDoc(rt, "doc1k")

	doc1k_putDoc := fmt.Sprintf(doc_1k_format, "")
	response := rt.SendAdminRequest("PUT", "/db/doc1k", doc1k_putDoc)
	var body db.Body
	require.NoError(b, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revid := body["rev"].(string)

	// Create user
	username := "user1"
	rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_user/%s", username), fmt.Sprintf(`{"name":"%s", "password":"letmein", "admin_channels":["channel_1"]}`, username))

	getBenchmarks := []struct {
		name   string
		URI    string
		asUser string
	}{
		{"Admin_Simple", "/db/doc1k", ""},
		{"Admin_WithRev", fmt.Sprintf("/db/doc1k?rev=%s", revid), ""},
		{"Admin_OpenRevsAll", fmt.Sprintf("/db/doc1k?open_revs=all&rev=%s", revid), ""},
		{"User_Simple", "/db/doc1k", username},
		{"User_WithRev", fmt.Sprintf("/db/doc1k?rev=%s", revid), username},
		{"User_OpenRevsAll", fmt.Sprintf("/db/doc1k?open_revs=all&rev=%s", revid), username},
	}

	for _, bm := range getBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			var getResponse *TestResponse
			for i := 0; i < b.N; i++ {
				if bm.asUser == "" {
					getResponse = rt.SendAdminRequest("GET", bm.URI, "")
				} else {
					getResponse = rt.Send(requestByUser("GET", bm.URI, "", bm.asUser))
				}
				b.StopTimer()
				if getResponse.Code != 200 {
					log.Printf("Unexpected response status code: %d", getResponse.Code)
				}
				b.StartTimer()
			}
		})
	}
}

// Benchmark 100% rev cache miss scenario
func BenchmarkReadOps_GetRevCacheMisses(b *testing.B) {

	base.DisableTestLogging(b)

	rt := NewRestTester(b, nil)
	defer rt.Close()
	defer PurgeDoc(rt, "doc1k")

	// Get database handle
	rtDatabase := rt.GetDatabase()
	revCacheSize := rtDatabase.Options.RevisionCacheOptions.Size

	doc1k_putDoc := fmt.Sprintf(doc_1k_format, "")
	numDocs := int(revCacheSize + 1)
	var revid string
	for i := 0; i < numDocs; i++ {
		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/doc1k_%d", i), doc1k_putDoc)
		// revid will be the same for all docs
		if i == 0 {
			var body db.Body
			require.NoError(b, base.JSONUnmarshal(response.Body.Bytes(), &body))
			revid = body["rev"].(string)
		}
	}

	// Create user
	username := "user1"
	rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_user/%s", username), fmt.Sprintf(`{"name":"%s", "password":"letmein", "admin_channels":["channel_1"]}`, username))

	getBenchmarks := []struct {
		name   string
		URI    string
		asUser string
	}{
		{"Admin_Simple", "/db/doc1k", ""},
		{"Admin_WithRev", fmt.Sprintf("/db/doc1k?rev=%s", revid), ""},
		{"Admin_OpenRevsAll", fmt.Sprintf("/db/doc1k?open_revs=all&rev=%s", revid), ""},
		{"User_Simple", "/db/doc1k", username},
		{"User_WithRev", fmt.Sprintf("/db/doc1k?rev=%s", revid), username},
		{"User_OpenRevsAll", fmt.Sprintf("/db/doc1k?open_revs=all&rev=%s", revid), username},
	}

	for _, bm := range getBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			var getResponse *TestResponse
			rtDatabase.FlushRevisionCacheForTest()
			for i := 0; i < b.N; i++ {
				// update key in URI
				docNum := i % numDocs
				newDocID := fmt.Sprintf("doc1k_%d", docNum)
				docURI := strings.Replace(bm.URI, "doc1k", newDocID, 1)

				if bm.asUser == "" {
					getResponse = rt.SendAdminRequest("GET", docURI, "")
				} else {
					getResponse = rt.Send(requestByUser("GET", docURI, "", bm.asUser))
				}
				b.StopTimer()
				if getResponse.Code != 200 {
					log.Printf("Unexpected response status code: %d", getResponse.Code)
				}
				b.StartTimer()
			}
		})
	}
}

func BenchmarkReadOps_Changes(b *testing.B) {

	base.DisableTestLogging(b)

	rt := NewRestTester(b, nil)
	defer rt.Close()
	defer PurgeDoc(rt, "doc1k")

	// Create user
	username := "user1"
	rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_user/%s", username), fmt.Sprintf(`{"name":"%s", "password":"letmein", "admin_channels":["channel_1"]}`, username))

	doc1k_putDoc := fmt.Sprintf(doc_1k_format, "")

	// Create branched doc
	response := rt.SendAdminRequest("PUT", "/db/doc1k", doc1k_putDoc)
	if response.Code != 201 {
		log.Printf("Unexpected create response: %d  %s", response.Code, response.Body.Bytes())
	}

	var body db.Body
	require.NoError(b, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revid := body["rev"].(string)
	_, rev1_digest := db.ParseRevID(revid)
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/doc1k?rev=%s", revid), doc1k_putDoc)
	if response.Code != 201 {
		log.Printf("Unexpected add rev response: %d  %s", response.Code, response.Body.Bytes())
	}

	doc1k_rev2_meta := fmt.Sprintf(`"_revisions":{"start":2, "ids":["two", "%s"]},`, rev1_digest)
	doc1k_rev2 := fmt.Sprintf(doc_1k_format, doc1k_rev2_meta)
	response = rt.SendAdminRequest("PUT", "/db/doc1k?new_edits=false", doc1k_rev2)
	if response.Code != 201 {
		log.Printf("Unexpected add conflicting rev response: %d  %s", response.Code, response.Body.Bytes())
	}

	// Changes benchmarks use since=1 to exclude the user doc from the response
	changesBenchmarks := []struct {
		name   string
		URI    string
		asUser string
	}{
		{"Admin_Simple", "/db/_changes?since=1", ""},
		{"Admin_Longpoll", "/db/_changes?since=1&feed=longpoll", ""},
		{"Admin_StyleAllDocs", "/db/_changes?since=1&style=all_docs", ""},
		{"Admin_StyleAllDocsChannelFilter", "/db/_changes?since=1&style=all_docs&filter=sync_gateway/bychannel&channels=channel_1", ""},
		{"Admin_IncludeDocs", "/db/_changes?since=1&include_docs=true", ""},
		{"User_Simple", "/db/_changes?since=1", username},
		{"User_Longpoll", "/db/_changes?since=1&feed=longpoll", username},
		{"User_StyleAllDocs", "/db/_changes?since=1&style=all_docs", username},
		{"User_StyleAllDocsChannelFilter", "/db/_changes?since=1&style=all_docs&filter=sync_gateway/bychannel&channels=channel_1", username},
		{"User_IncludeDocs", "/db/_changes?since=1&include_docs=true", username},
	}

	for _, bm := range changesBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			var changesResponse *TestResponse
			for i := 0; i < b.N; i++ {
				if bm.asUser == "" {
					changesResponse = rt.SendAdminRequest("GET", bm.URI, "")
				} else {
					changesResponse = rt.Send(requestByUser("GET", bm.URI, "", bm.asUser))
				}
				b.StopTimer()
				if changesResponse.Code != 200 {
					log.Printf("Unexpected response status code: %d", changesResponse.Code)
				}
				b.StartTimer()
			}
		})
	}
}

func BenchmarkReadOps_RevsDiff(b *testing.B) {

	base.DisableTestLogging(b)

	rt := NewRestTester(b, nil)
	defer rt.Close()
	defer PurgeDoc(rt, "doc1k")

	// Create target doc for revs_diff:
	doc1k_bulkDocs_meta := `"_id":"doc1k", 
	"_rev":"12-abc", 
	"_revisions":{
		"start": 12, 
		"ids": ["abc", "eleven", "ten", "nine"]
		},`

	doc1k_bulkDocs_entry := fmt.Sprintf(doc_1k_format, doc1k_bulkDocs_meta)
	bulkDocs_body := fmt.Sprintf(`{"new_edits":false, "docs": [%s]}`, doc1k_bulkDocs_entry)
	response := rt.SendAdminRequest("POST", "/db/_bulk_docs", bulkDocs_body)
	if response.Code != 201 {
		log.Printf("Unexpected response: %d", response.Code)
		log.Printf("Response:%s", response.Body.Bytes())
	}

	// Create user
	username := "user1"
	rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_user/%s", username), fmt.Sprintf(`{"name":"%s", "password":"letmein", "admin_channels":["channel_1"]}`, username))

	revsDiffBody := `{"doc1k": ["10-ten", "9-nine"]}`
	revsDiffBenchmarks := []struct {
		name   string
		URI    string
		asUser string
	}{
		{"Admin", "/db/_revs_diff", ""},
		{"User", "/db/_revs_diff", username},
	}

	for _, bm := range revsDiffBenchmarks {
		b.Run(bm.name, func(b *testing.B) {
			var getResponse *TestResponse
			for i := 0; i < b.N; i++ {
				if bm.asUser == "" {
					getResponse = rt.SendAdminRequest("POST", bm.URI, revsDiffBody)
				} else {
					getResponse = rt.Send(requestByUser("POST", bm.URI, revsDiffBody, bm.asUser))
				}
				b.StopTimer()
				if getResponse.Code != 200 {
					b.Logf("Unexpected response: %v", getResponse)
				}
				b.StartTimer()
			}
		})
	}

}

func PurgeDoc(rt *RestTester, docid string) {
	response := rt.SendAdminRequest("POST", "/db/_purge", fmt.Sprintf(`{"%s":["*"]}`, docid))
	if response.Code != 200 {
		log.Printf("Unexpected purge response: %d  %s", response.Code, response.Body.Bytes())
	}
}
