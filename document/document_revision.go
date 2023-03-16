package document

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"golang.org/x/exp/slices"
)

// The body of a revision: special properties like _id, plus an arbitrary JSON object.
// DocumentRevision is stored and returned by the rev cache.
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
	Invalid     bool // Used by RevisionCache.

	_bodyBytes []byte // the raw document JSON, with no special properties.
}

// Parses and validates a JSON document, creating a DocumentRevision.
// The arguments after the JSON are reserved property keys like `BodyId`, `BodyRev`.
// These are removed from the JSON and set as struct fields.
//
// The allowed properties are:
//
//	BodyId, BodyRev, BodyAttachments, BodyExpiry, BodyRevisions, BodyDeleted, BodyRemoved.
//
// Reserved properties in the JSON that were _not_ given as arguments trigger errors.
func ParseDocumentRevision(json []byte, specialProperties ...string) (DocumentRevision, error) {
	var rev DocumentRevision
	var expiry any
	var err error
	rev._bodyBytes, err = base.JSONExtract(json, func(key string) (valp any, err error) {
		// JSONExtract callback: process one key:
		if IsReservedKey(key) {
			if slices.Contains(specialProperties, key) {
				if key == BodyExpiry {
					return &expiry, nil // store "_exp" value in temporary var
				} else {
					return rev.propertyPtr(key, true)
				}
			} else {
				return nil, base.HTTPErrorf(http.StatusBadRequest, "top-level property '"+key+"' is a reserved internal property")
			}
		} else {
			return nil, nil
		}
	})

	if err == nil && expiry != nil {
		// Translate "_exp" property to Time value:
		err = rev.SetExpiry(expiry)
	}
	return rev, err
}

// Creates a DocumentRevision from already-parsed JSON (a `Body` map.)
func DocumentRevisionFromBody(body Body, specialProperties ...string) (rev DocumentRevision, err error) {
	appProperties := Body{}
	for key, val := range body {
		if IsReservedKey(key) {
			if slices.Contains(specialProperties, key) {
				err = rev.setProperty(key, val)
			} else {
				err = base.HTTPErrorf(http.StatusBadRequest, "top-level property '"+key+"' is a reserved internal property")
			}
			if err != nil {
				return
			}
		} else {
			appProperties[key] = val
		}
	}
	rev._bodyBytes, err = json.Marshal(appProperties)
	return
}

// Creates a Document struct populated from a DocumentRevision.
func (rev *DocumentRevision) AsDocument() *Document {
	return &Document{
		ID:             rev.DocID,
		RevID:          rev.RevID,
		DocAttachments: rev.Attachments,
		DocExpiry:      base.TimeToCbsExpiry(rev.Expiry),
		Deleted:        rev.Deleted,
		_rawBody:       rev._bodyBytes,
	}
}

// Sets the JSON body.
func (rev *DocumentRevision) SetBodyBytes(body []byte) {
	rev._bodyBytes = body
}

// Sets the JSON body to `{}`.
func (rev *DocumentRevision) SetEmptyBody() {
	rev._bodyBytes = []byte(base.EmptyDocument)
}

// The JSON data of the body; just application properties, no specials.
// May be nil if this is an older revision whose body has expired.
func (rev *DocumentRevision) BodyBytes() []byte {
	return rev._bodyBytes
}

// The JSON data of the body, with the given special properties added
// if they have non-default/empty values in the struct.
func (rev *DocumentRevision) BodyBytesWith(specialProperties ...string) ([]byte, error) {
	bodyBytes := rev._bodyBytes
	if bodyBytes == nil {
		return nil, base.HTTPErrorf(404, "Revision body is missing") //??? Is this the right error?
	}
	if len(specialProperties) == 0 {
		return bodyBytes, nil
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

	return base.InjectJSONProperties(bodyBytes, specialKV...)
}

// Unmarshals a DocumentRevision's body. No special properties are included.
// The resulting map is not shared and can be mutated freely.
//
// This function is expensive and should be used RARELY, primarily for tests.
func (rev *DocumentRevision) UnmarshalBody() (map[string]any, error) {
	var body Body
	err := body.Unmarshal(rev._bodyBytes)
	return body, err
}

// Sets the expiry from any supported JSON type -- int64, float64, json.Number, string.
func (rev *DocumentRevision) SetExpiry(expiry any) error {
	if expNum, err := base.ReflectExpiry(expiry); err != nil {
		return err
	} else if expNum != nil || *expNum != 0 {
		expTime := base.CbsExpiryToTime(*expNum)
		rev.Expiry = &expTime
	} else {
		rev.Expiry = nil
	}
	return nil
}

// Trims rev.History to at most `maxHistory` revisions starting from the given ancestors.
func (rev *DocumentRevision) TrimHistory(maxHistory int, historyFrom []string) {
	if maxHistory == 0 {
		rev.History = nil
	} else if rev.History != nil {
		_, rev.History = TrimEncodedRevisionsToAncestor(rev.History, historyFrom, maxHistory)
	}
}

//-------- INTERNALS

// Subroutine used by ParseDocumentRevision() and JSONData().
// Given a JSON special property key, returns the address of the corrresponding struct member.
// If `always` is false, returns nil if the property is not set.
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
		if always || len(rev.Attachments) > 0 {
			DeleteAttachmentVersion(rev.Attachments)
			value = &rev.Attachments
		}
	case BodyExpiry:
		if always || rev.Expiry != nil && !rev.Expiry.IsZero() {
			value = &rev.Expiry
		}
	case BodyRevisions:
		if always || len(rev.History) > 0 {
			value = &rev.History
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

// Subroutine used by DocumentRevisionFromBody. Sets a struct field given a JSON property key.
func (rev *DocumentRevision) setProperty(key string, val any) error {
	switch key {
	case BodyAttachments:
		if atts, found := val.(AttachmentsMeta); found {
			rev.Attachments = atts
		} else if atts, found := val.(map[string]any); found {
			rev.Attachments = AttachmentsMeta(atts)
		} else {
			return fmt.Errorf("invalid value for '_attachments' property")
		}
	case BodyExpiry:
		return rev.SetExpiry(val)
	default:
		// TODO: Add other properties as needed
		return fmt.Errorf("internal error: DocumentRevision.setProperty doesn't recognize property %q", key)
	}
	return nil
}
