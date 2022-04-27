/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocumentUnmarshal(t *testing.T) {

	jsonContent := `
{
   "_id":"docid",
   "_rev":"1-rev",
   "foo":"bar",
   "_attachments":{
      "myattachment":{
         "content_type":"text",
         "digest":"987u98u",
         "length":10,
         "revpos":1,
         "stub":true
      }
   }
}
`

	doc := RestDocument{}
	err := base.JSONUnmarshal([]byte(jsonContent), &doc)
	if err != nil {
		log.Printf("Error: %v", err)
	}
	assert.True(t, err == nil)
	log.Printf("doc: %+v", doc)

	assert.True(t, doc.ID() == "docid")

	docFooField, hasFoo := doc["foo"]
	assert.True(t, hasFoo)
	log.Printf("docFooField: %v", docFooField)

	attachments, err := doc.GetAttachments()
	assert.True(t, err == nil)

	assert.Equal(t, 1, len(attachments))
	myattachment := attachments["myattachment"]
	assert.Equal(t, "text", myattachment.ContentType)

}

func TestAttachmentRoundTrip(t *testing.T) {

	doc := RestDocument{}
	attachmentMap := db.AttachmentMap{
		"foo": &db.DocAttachment{
			ContentType: "application/octet-stream",
			Digest:      "WHATEVER",
		},
		"bar": &db.DocAttachment{
			ContentType: "text/plain",
			Digest:      "something",
		},
		"baz": &db.DocAttachment{
			Data: []byte(""),
		},
	}

	doc.SetAttachments(attachmentMap)

	attachments, err := doc.GetAttachments()
	require.NoError(t, err)
	require.Equal(t, 3, len(attachments))

	require.NotNil(t, attachments["foo"])
	assert.Equal(t, "application/octet-stream", attachments["foo"].ContentType)
	assert.Equal(t, "WHATEVER", attachments["foo"].Digest)

	require.NotNil(t, attachments["bar"])
	assert.Equal(t, "text/plain", attachments["bar"].ContentType)
	assert.Equal(t, "something", attachments["bar"].Digest)

	require.NotNil(t, attachments["baz"])
	assert.Equal(t, "", attachments["baz"].ContentType)
	assert.Equal(t, "", attachments["baz"].Digest)
	assert.Equal(t, []byte{}, attachments["baz"].Data) // data field is explicitly ignored

}
