package rest

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/db"
	goassert "github.com/couchbaselabs/go.assert"
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
	err := json.Unmarshal([]byte(jsonContent), &doc)
	if err != nil {
		log.Printf("Error: %v", err)
	}
	goassert.True(t, err == nil)
	log.Printf("doc: %+v", doc)

	goassert.True(t, doc.ID() == "docid")

	docFooField, hasFoo := doc["foo"]
	goassert.True(t, hasFoo)
	log.Printf("docFooField: %v", docFooField)

	attachments, err := doc.GetAttachments()
	goassert.True(t, err == nil)

	goassert.Equals(t, len(attachments), 1)
	myattachment := attachments["myattachment"]
	goassert.Equals(t, myattachment.ContentType, "text")

}

func TestAttachmentRoundTrip(t *testing.T) {

	doc := RestDocument{}
	attachmentMap := db.AttachmentMap{
		"foo": &db.DocAttachment{
			ContentType: "text",
			Digest:      "whatever",
		},
		"bar": &db.DocAttachment{
			ContentType: "text",
			Digest:      "whatever",
		},
	}

	doc.SetAttachments(attachmentMap)

	attachments, err := doc.GetAttachments()
	goassert.True(t, err == nil)

	goassert.Equals(t, len(attachments), 2)

	for attachName, attachment := range attachments {
		goassert.Equals(t, attachment.ContentType, "text")
		goassert.True(t, attachName == "foo" || attachName == "bar")
	}

}
