package document

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// The body of a revision: special properties like _id, plus an arbitrary JSON object.
// DocumentRevision is stored and returned by the rev cache
type DocumentRevision struct {
	DocID       string
	RevID       string
	History     Revisions
	Channels    base.Set
	Expiry      *time.Time
	Attachments AttachmentsMeta
	Delta       *RevisionDelta
	Deleted     bool
	Removed     bool // True if the revision is a removal.
	Invalid     bool // Used by RevisionCache

	BodyBytes        []byte // BodyBytes contains the raw document, with no special properties.
	_shallowCopyBody Body   // an unmarshalled body that can produce shallow copies
}

// Parses and validates a JSON document, creating a DocumentRevision.
// The arguments after the JSON are special property keys like `BodyId`, `BodyRev`.
// These are removed from the JSON and set as struct fields.
// Only properties corresponding to fields of `DocumentRevision` are allowed.
// Underscored properties in the JSON that were _not_ given as arguments trigger errors.
func ParseDocumentRevision(json []byte, specialProperties ...string) (*DocumentRevision, error) {
	rev := &DocumentRevision{}
	var expiry any
	var err error
	rev.BodyBytes, err = base.JSONExtract(json, func(key string) (valp any, err error) {
		// JSONExtract callback: process one key:
		if IsReservedKey(key) {
			for _, specialKey := range specialProperties {
				if key == specialKey {
					if key == BodyExpiry {
						return &expiry, nil // store "_exp" value in temporary var
					} else {
						return rev.propertyPtr(key, true)
					}
				}
			}
			return nil, base.HTTPErrorf(http.StatusBadRequest, "top-level property '"+key+"' is a reserved internal property")
		} else {
			return nil, nil
		}
	})
	if err != nil {
		return nil, err
	}

	if expiry != nil {
		// Translate "_exp" property to Time value:
		if expNum, err := base.ReflectExpiry(expiry); err != nil {
			return nil, err
		} else if expNum != nil || *expNum != 0 {
			expTime := base.CbsExpiryToTime(*expNum)
			rev.Expiry = &expTime
		}
	}
	return rev, nil
}

// The JSON data of the body. By default this is just the application properties.
// If any special property names (`BodyId`, `BodyRev`...) are given as arguments, those properties
// are added to the JSON if they have non-default/empty values in the struct.
func (rev *DocumentRevision) JSONData(specialProperties ...string) ([]byte, error) {
	if len(specialProperties) == 0 {
		return rev.BodyBytes, nil
	}
	specialKV := make([]base.KVPair, 0, 8)
	for _, key := range specialProperties {
		if value, err := rev.propertyPtr(key, false); err != nil {
			return nil, err
		} else if value != nil {
			if key == BodyExpiry {
				value = rev.Expiry.Format(time.RFC3339)
			}
			specialKV = append(specialKV, base.KVPair{Key: key, Val: value})
		}
	}

	return base.InjectJSONProperties(rev.BodyBytes, specialKV...)
}

// Subroutine used by ParseDocumentRevision() and JSONData().
// Given a JSON special property key, returns the address of the corrresponding struct member.
// If `always` is false, returns nil if the property has no value.
func (rev *DocumentRevision) propertyPtr(key string, always bool) (value any, err error) {
	switch key {
	case BodyId:
		if always || rev.DocID != "" {
			value = &rev.DocID
		}
	case BodyRev:
		if always || rev.RevID != "" {
			value = &rev.RevID
		}
	case BodyAttachments:
		if always || rev.Attachments != nil {
			value = &rev.Attachments
		}
	case BodyExpiry:
		if always || rev.Expiry != nil {
			value = &rev.Expiry
		}
	case BodyDeleted:
		if always || rev.Deleted {
			value = &rev.Deleted
		}
	case BodyRemoved:
		if always || rev.Removed {
			value = &rev.Removed
		}
	default:
		// Invalid key passed to ParseDocumentRevision() or JSONData().
		err = fmt.Errorf("internal error: DocumentRevision doesn't recognize property %q", key)
	}
	return
}

//------- OLD STUFF TO BE REFACTORED AWAY

// MutableBody returns a deep copy of the given document revision as a plain body (without any special properties)
// Callers are free to modify any of this body without affecting the document revision.
func (rev *DocumentRevision) MutableBody() (b Body, err error) {
	if err := b.Unmarshal(rev.BodyBytes); err != nil {
		return nil, err
	}

	return b, nil
}

func (rev *DocumentRevision) SetShallowCopyBody(body Body) {
	rev._shallowCopyBody = body
}

// Body returns an unmarshalled body that is kept in the document revision to produce shallow copies.
// If an unmarshalled copy is not available in the document revision, it makes a copy from the raw body
// bytes and stores it in document revision itself before returning the body.
func (rev *DocumentRevision) Body() (b Body, err error) {
	// if we already have an unmarshalled body, take a copy and return it
	if rev._shallowCopyBody != nil {
		return rev._shallowCopyBody, nil
	}

	if err := b.Unmarshal(rev.BodyBytes); err != nil {
		return nil, err
	}

	// store a copy of the unmarshalled body for next time we need it
	rev._shallowCopyBody = b

	return b, nil
}

//------- SHOULD BE MOVED BACK TO db OR EVEN rest

// Abstract interface for a database
type DocumentProvider interface {
	LoadAttachmentsData(meta AttachmentsMeta, minRevPos int, docID string) (AttachmentsMeta, error)
}

// Mutable1xBody returns a copy of the given document revision as a 1.x style body (with special properties)
// Callers are free to modify this body without affecting the document revision.
func (rev *DocumentRevision) Mutable1xBody(db DocumentProvider, requestedHistory Revisions, attachmentsSince []string, showExp bool) (b Body, err error) {
	b, err = rev.Body()
	if err != nil {
		return nil, err
	}

	b[BodyId] = rev.DocID
	b[BodyRev] = rev.RevID

	// Add revision metadata:
	if requestedHistory != nil {
		b[BodyRevisions] = requestedHistory
	}

	if showExp && rev.Expiry != nil && !rev.Expiry.IsZero() {
		b[BodyExpiry] = rev.Expiry.Format(time.RFC3339)
	}

	if rev.Deleted {
		b[BodyDeleted] = true
	}

	// Add attachment data if requested:
	if attachmentsSince != nil {
		if len(rev.Attachments) > 0 {
			minRevpos := 1
			if len(attachmentsSince) > 0 {
				ancestor := rev.History.FindAncestor(attachmentsSince)
				if ancestor != "" {
					minRevpos, _ = ParseRevID(ancestor)
					minRevpos++
				}
			}
			bodyAtts, err := db.LoadAttachmentsData(rev.Attachments, minRevpos, rev.DocID)
			if err != nil {
				return nil, err
			}
			DeleteAttachmentVersion(bodyAtts)
			b[BodyAttachments] = bodyAtts
		}
	} else if rev.Attachments != nil {
		// Stamp attachment metadata back into the body
		DeleteAttachmentVersion(rev.Attachments)
		b[BodyAttachments] = rev.Attachments
	}

	return b, nil
}

// As1xBytes returns a byte slice representing the 1.x style body, containing special properties (i.e. _id, _rev, _attachments, etc.)
func (rev *DocumentRevision) As1xBytes(db DocumentProvider, requestedHistory Revisions, attachmentsSince []string, showExp bool) (b []byte, err error) {
	// unmarshal
	body1x, err := rev.Mutable1xBody(db, requestedHistory, attachmentsSince, showExp)
	if err != nil {
		return nil, err
	}

	// TODO: We could avoid the unmarshal -> marshal work here by injecting properties into the original body bytes directly.
	return json.Marshal(body1x)
}
