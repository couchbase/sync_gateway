//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package document

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

// The body of a CouchDB document/revision as decoded from JSON.
type Body map[string]interface{}

const (
	BodyDeleted        = "_deleted"
	BodyRev            = "_rev"
	BodyId             = "_id"
	BodyRevisions      = "_revisions"
	BodyAttachments    = "_attachments"
	BodyPurged         = "_purged"
	BodyExpiry         = "_exp"
	BodyRemoved        = "_removed"
	BodyInternalPrefix = "_sync_" // New internal properties prefix (CBG-1995)
)

// Read-only set of reserved document body keys
var bodyReservedKeys = base.SetOf(BodyDeleted, BodyRev, BodyId, BodyRevisions, BodyAttachments, BodyPurged, BodyExpiry, BodyRemoved, base.SyncPropertyName) // per spec

var bodyInternalKeys = base.SetOf(BodyRev, BodyId, BodyRevisions, BodyPurged, BodyExpiry, BodyRemoved, base.SyncPropertyName)

// True if this is a reserved document body key
func IsReservedKey(key string) bool {
	return bodyReservedKeys.Contains(key) || strings.HasPrefix(key, BodyInternalPrefix)
}

// True if this is a reserved document body key, except for _deleted and _attachments
func isInternalReservedKey(key string) bool {
	return bodyInternalKeys.Contains(key) || strings.HasPrefix(key, BodyInternalPrefix)
}

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

func CopyMap(sourceMap map[string]interface{}) map[string]interface{} {
	copy := make(map[string]interface{}, len(sourceMap))
	for k, v := range sourceMap {
		if valueMap, ok := v.(map[string]interface{}); ok {
			copiedValue := CopyMap(valueMap)
			copy[k] = copiedValue
		} else {
			copy[k] = v
		}
	}
	return copy
}

// Returns _attachments property from body, when found.  Checks for either map[string]interface{} (unmarshalled with body),
// or AttachmentsMeta (written by body by SG)
func (body Body) GetAttachments() (AttachmentsMeta, error) {
	return AttachmentsMetaFromAny(body[BodyAttachments])
}

// Returns _attachments property as a raw map-of-maps with no structs.
func (body Body) GetRawAttachments() AttachmentsMetaJSON {
	switch atts := body[BodyAttachments].(type) {
	case AttachmentsMeta:
		return atts.AsMap()
	case map[string]any /*, AttachmentsMetaJSON*/ :
		return atts
	default:
	}
	return nil
}

// Returns the expiry as uint32 (using getExpiry), and removes the _exp property from the body
func (body Body) ExtractExpiry() (uint32, error) {

	exp, present, err := body.getExpiry()
	if !present || err != nil {
		return exp, err
	}
	delete(body, "_exp")

	return exp, nil
}

func (body Body) ExtractDeleted() bool {
	deleted, _ := body[BodyDeleted].(bool)
	delete(body, BodyDeleted)
	return deleted
}

func (body Body) ExtractRev() string {
	revid, _ := body[BodyRev].(string)
	delete(body, BodyRev)
	return revid
}

// Looks up the _exp property in the document, and turns it into a Couchbase Server expiry value, as:
func (body Body) getExpiry() (uint32, bool, error) {
	rawExpiry, ok := body["_exp"]
	if !ok {
		return 0, false, nil // _exp not present
	}
	expiry, err := base.ReflectExpiry(rawExpiry)
	if err != nil || expiry == nil {
		return 0, false, err
	}
	return *expiry, true, err
}

// NonJSONPrefix is used to ensure old revision bodies aren't hidden from N1QL/Views.
const NonJSONPrefix = byte(1)

//-------- REVISIONS

// A revisions property found within a Body.  Expected to be of the form:
//
//	Revisions["start"]: int64, starting generation number
//	Revisions["ids"]: []string, list of digests
//
// Used as map[string]interface{} instead of Revisions struct because it's unmarshalled
// along with Body, and we don't need the overhead of allocating a new object
type Revisions map[string]interface{}

const (
	RevisionsStart = "start"
	RevisionsIds   = "ids"
)

func (revisions Revisions) ShallowCopy() Revisions {
	copied := make(Revisions, len(revisions))
	for key, value := range revisions {
		copied[key] = value
	}
	return copied
}

// Version of doc.History.findAncestorFromSet that works against formatted Revisions.
// Returns the most recent ancestor found in revisions
func (revisions Revisions) FindAncestor(ancestors []string) (revId string) {

	start, ids := splitRevisionList(revisions)
	for _, id := range ids {
		revid := fmt.Sprintf("%d-%s", start, id)
		for _, a := range ancestors {
			if a == revid {
				return a
			}
		}
		start--
	}
	return ""
}

// ParseRevisions returns revisions as a slice of revids.
func (revisions Revisions) ParseRevisions() []string {
	start, ids := splitRevisionList(revisions)
	if ids == nil {
		return nil
	}
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		result = append(result, fmt.Sprintf("%d-%s", start, id))
		start--
	}
	return result
}

// Returns revisions as a slice of ancestor revids, from the parent to the target ancestor.
func (revisions Revisions) ParseAncestorRevisions(toAncestorRevID string) []string {
	start, ids := splitRevisionList(revisions)
	if ids == nil || len(ids) < 2 {
		return nil
	}
	result := make([]string, 0)

	// Start at the parent, end at toAncestorRevID
	start = start - 1
	for i := 1; i < len(ids); i++ {
		revID := fmt.Sprintf("%d-%s", start, ids[i])
		result = append(result, revID)
		if revID == toAncestorRevID {
			break
		}
		start--
	}
	return result
}

//-------- REVISION IDS:

func CreateRevID(generation int, parentRevID string, body Body) (string, error) {
	// This should produce the same results as Couchbase Lite.
	strippedBody, _ := StripInternalProperties(body)
	encoding, err := base.JSONMarshalCanonical(strippedBody)
	if err != nil {
		return "", err
	}
	return CreateRevIDWithBytes(generation, parentRevID, encoding), nil
}

func CreateRevIDWithBytes(generation int, parentRevID string, bodyBytes []byte) string {
	digester := md5.New()
	digester.Write([]byte{byte(len(parentRevID))})
	digester.Write([]byte(parentRevID))
	digester.Write(bodyBytes)
	return fmt.Sprintf("%d-%x", generation, digester.Sum(nil))
}

// Returns the generation number (numeric prefix) of a revision ID.
func GenOfRevID(revid string) int {
	if revid == "" {
		return 0
	}
	var generation int
	n, _ := fmt.Sscanf(revid, "%d-", &generation)
	if n < 1 || generation < 1 {
		base.WarnfCtx(context.Background(), "GenOfRevID unsuccessful for %q", revid)
		return -1
	}
	return generation
}

// Splits a revision ID into generation number and hex digest.
func ParseRevID(revid string) (int, string) {
	if revid == "" {
		return 0, ""
	}

	idx := strings.Index(revid, "-")
	if idx == -1 {
		base.WarnfCtx(context.Background(), "parseRevID found no separator in rev %q", revid)
		return -1, ""
	}

	gen, err := strconv.Atoi(revid[:idx])
	if err != nil {
		base.WarnfCtx(context.Background(), "parseRevID unexpected generation in rev %q: %s", revid, err)
		return -1, ""
	} else if gen < 1 {
		base.WarnfCtx(context.Background(), "parseRevID unexpected generation in rev %q", revid)
		return -1, ""
	}

	return gen, revid[idx+1:]
}

// CompareRevIDs compares the two rev IDs and returns:
// 1  if id1 is 'greater' than id2
// -1 if id1 is 'less' than id2
// 0  if the two are equal.
func CompareRevIDs(id1, id2 string) int {
	gen1, sha1 := ParseRevID(id1)
	gen2, sha2 := ParseRevID(id2)
	switch {
	case gen1 > gen2:
		return 1
	case gen1 < gen2:
		return -1
	case sha1 > sha2:
		return 1
	case sha1 < sha2:
		return -1
	}
	return 0
}

//-------- UTILITY FUNCTIONS:

// StripInternalProperties returns a copy of the given body with all internal underscore-prefixed keys removed, except _attachments and _deleted.
func StripInternalProperties(b Body) (Body, bool) {
	return stripSpecialProperties(b, true)
}

// stripAllInternalProperties returns a copy of the given body with all underscore-prefixed keys removed.
func StripAllSpecialProperties(b Body) (Body, bool) {
	return stripSpecialProperties(b, false)
}

// stripSpecialPropertiesExcept returns a copy of the given body with underscore-prefixed keys removed.
// Set internalOnly to only strip internal properties except _deleted and _attachments
func stripSpecialProperties(b Body, internalOnly bool) (sb Body, foundSpecialProps bool) {
	// Assume that most of the time no properties will be stripped
	var stripped Body
	for k := range b {
		// Property is not stripped if:
		// - It is blank
		// - Does not start with an underscore ('_')
		// - Is not an internal special property (this check is excluded when internalOnly = false)
		if k != "" && k[0] == '_' && (!internalOnly || isInternalReservedKey(k)) {
			if stripped == nil {
				stripped = b.ShallowCopy()
			}
			delete(stripped, k)
		}
	}

	if stripped != nil {
		return stripped, true
	} else {
		// Return original body if nothing was removed
		return b, false
	}
}

func GetStringArrayProperty(body map[string]interface{}, property string) ([]string, error) {
	if raw, exists := body[property]; !exists {
		return nil, nil
	} else if strings, ok := raw.([]string); ok {
		return strings, nil
	} else if items, ok := raw.([]interface{}); ok {
		strings := make([]string, len(items))
		for i := 0; i < len(items); i++ {
			strings[i], ok = items[i].(string)
			if !ok {
				return nil, base.HTTPErrorf(http.StatusBadRequest, property+" must be a string array")
			}
		}
		return strings, nil
	} else {
		return nil, base.HTTPErrorf(http.StatusBadRequest, property+" must be a string array")
	}
}
