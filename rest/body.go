package rest

import (
	"bytes"
	"errors"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/document"
)

// A decoded JSON document/object.
type Body map[string]interface{}

func (b *Body) Unmarshal(data []byte) error {

	if len(data) == 0 {
		return errors.New("Unexpected empty JSON input to body.Unmarshal")
	}

	// Use decoder for unmarshalling to preserve large numbers
	decoder := base.JSONDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(b); err != nil {
		return err
	}
	return nil
}

func (body Body) ShallowCopy() Body {
	if body == nil {
		return body
	}
	copied := make(Body, len(body))
	for key, value := range body {
		copied[key] = value
	}
	return copied
}

func (body Body) ExtractRev() string {
	revid, _ := body[document.BodyRev].(string)
	delete(body, document.BodyRev)
	return revid
}

// Parses a CouchDB _rev or _revisions property into a list of revision IDs
func ParseRevisions(body Body) []string {
	// http://wiki.apache.org/couchdb/HTTP_Document_API#GET

	revisionsProperty, ok := body[document.BodyRevisions]
	if !ok {
		revid, ok := body[document.BodyRev].(string)
		if !ok {
			return nil
		}
		if document.GenOfRevID(revid) < 1 {
			return nil
		}
		oneRev := make([]string, 0, 1)
		oneRev = append(oneRev, revid)
		return oneRev
	}

	// Revisions may be stored in a Body as Revisions or map[string]interface{}, depending on the source of the Body
	var revisions document.Revisions
	switch revs := revisionsProperty.(type) {
	case document.Revisions:
		revisions = revs
	case map[string]interface{}:
		revisions = document.Revisions(revs)
	default:
		return nil
	}

	return revisions.ParseRevisions()
}
