//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"bytes"
	"crypto/md5"
	"errors"
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
)

// The body of a CouchDB document/revision as decoded from JSON.
type Body map[string]interface{}

const (
	BodyDeleted     = "_deleted"
	BodyRev         = "_rev"
	BodyId          = "_id"
	BodyRevisions   = "_revisions"
	BodyAttachments = "_attachments"
	BodyPurged      = "_purged"
	BodyExpiry      = "_exp"
	BodyRemoved     = "_removed"
)

// A revisions property found within a Body.  Expected to be of the form:
//   Revisions["start"]: int64, starting generation number
//   Revisions["ids"]: []string, list of digests
// Used as map[string]interface{} instead of Revisions struct because it's unmarshalled
// along with Body, and we don't need the overhead of allocating a new object
type Revisions map[string]interface{}

const (
	RevisionsStart = "start"
	RevisionsIds   = "ids"
)

type BodyCopyType int

const (
	BodyDeepCopy    BodyCopyType = iota // Performs a deep copy (json marshal/unmarshal)
	BodyShallowCopy                     // Performs a shallow copy (copies top level properties, doesn't iterate into nested properties)
	BodyNoCopy                          // Doesn't copy - callers must not mutate the response
)

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

func (body Body) Copy(copyType BodyCopyType) Body {
	switch copyType {
	case BodyShallowCopy:
		return body.ShallowCopy()
	case BodyDeepCopy:
		return body.DeepCopy()
	case BodyNoCopy:
		return body
	default:
		base.Infof(base.KeyCRUD, "Unexpected copy type specified in body.Copy - defaulting to shallow copy.  copyType: %d", copyType)
		return body.ShallowCopy()
	}
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

func (body Body) DeepCopy() Body {
	var copiedBody Body
	base.DeepCopyInefficient(&copiedBody, body)
	return copiedBody
}

func (revisions Revisions) ShallowCopy() Revisions {
	copied := make(Revisions, len(revisions))
	for key, value := range revisions {
		copied[key] = value
	}
	return copied
}

// Version of doc.History.findAncestorFromSet that works against formatted Revisions.
// Returns the most recent ancestor found in revisions
func (revisions Revisions) findAncestor(ancestors []string) (revId string) {

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
func (revisions Revisions) parseAncestorRevisions(toAncestorRevID string) []string {
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

func (attachments AttachmentsMeta) ShallowCopy() AttachmentsMeta {
	if attachments == nil {
		return attachments
	}
	return copyMap(attachments)
}

func copyMap(sourceMap map[string]interface{}) map[string]interface{} {
	copy := make(map[string]interface{}, len(sourceMap))
	for k, v := range sourceMap {
		if valueMap, ok := v.(map[string]interface{}); ok {
			copiedValue := copyMap(valueMap)
			copy[k] = copiedValue
		} else {
			copy[k] = v
		}
	}
	return copy
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
		return 0, false, nil //_exp not present
	}
	expiry, err := base.ReflectExpiry(rawExpiry)
	if err != nil || expiry == nil {
		return 0, false, err
	}
	return *expiry, true, err
}

// nonJSONPrefix is used to ensure old revision bodies aren't hidden from N1QL/Views.
const nonJSONPrefix = byte(1)

// Looks up the raw JSON data of a revision that's been archived to a separate doc.
// If the revision isn't found (e.g. has been deleted by compaction) returns 404 error.
func (db *DatabaseContext) getOldRevisionJSON(docid string, revid string) ([]byte, error) {
	data, _, err := db.Bucket.GetRaw(oldRevisionKey(docid, revid))
	if base.IsDocNotFoundError(err) {
		base.Debugf(base.KeyCRUD, "No old revision %q / %q", base.UD(docid), revid)
		err = base.HTTPErrorf(404, "missing")
	}
	if data != nil {
		// Strip out the non-JSON prefix
		if len(data) > 0 && data[0] == nonJSONPrefix {
			data = data[1:]
		}
		base.Debugf(base.KeyCRUD, "Got old revision %q / %q --> %d bytes", base.UD(docid), revid, len(data))
	}
	return data, err
}

// Makes a backup of revision body for use by delta sync, and in-flight replications requesting an old revision.
// Backup policy depends on whether delta sync and/or shared_bucket_access is enabled
//   delta=false || delta_rev_max_age_seconds=0
//      - old revision stored, with expiry OldRevExpirySeconds
//   delta=true && shared_bucket_access=true
//      - new revision stored (as duplicate), with expiry rev_max_age_seconds
//   delta=true && shared_bucket_access=false
//      - old revision stored, with expiry rev_max_age_seconds
func (db *Database) backupRevisionJSON(docId, newRevId, oldRevId string, newBody []byte, oldBody []byte, newAtts AttachmentsMeta) {

	// Without delta sync, store the old rev for in-flight replication purposes
	if !db.DeltaSyncEnabled() || db.Options.DeltaSyncOptions.RevMaxAgeSeconds == 0 {
		_ = db.setOldRevisionJSON(docId, oldRevId, oldBody, db.Options.OldRevExpirySeconds)
		return
	}

	// Otherwise, store the revs for delta generation purposes, with a longer expiry

	// Special handling for Xattrs so that SG still has revisions that were updated by an SDK write
	if db.UseXattrs() {
		// Inject _attachments metadata into the body we're about to backup
		var newBodyBytes []byte
		if len(newAtts) > 0 {
			var err error
			newBodyBytes, err = base.InjectJSONProperties(newBody, base.KVPair{Key: BodyAttachments, Val: newAtts})
			if err != nil {
				base.Warnf(base.KeyAll, "Unable to marshal new revision body during backupRevisionJSON: doc=%q rev=%q err=%v ", base.UD(docId), newRevId, err)
				return
			}
		} else {
			newBodyBytes = newBody
		}
		// Backup the current revision
		_ = db.setOldRevisionJSON(docId, newRevId, newBodyBytes, db.Options.DeltaSyncOptions.RevMaxAgeSeconds)

		// Refresh the expiry on the previous revision backup
		_ = db.refreshPreviousRevisionBackup(docId, oldRevId, oldBody, db.Options.DeltaSyncOptions.RevMaxAgeSeconds)
		return
	}

	// Non-xattr only need to store the previous revision, as all writes come through SG
	_ = db.setOldRevisionJSON(docId, oldRevId, oldBody, db.Options.DeltaSyncOptions.RevMaxAgeSeconds)
}

func (db *Database) setOldRevisionJSON(docid string, revid string, body []byte, expiry uint32) error {

	// Setting the binary flag isn't sufficient to make N1QL ignore the doc - the binary flag is only used by the SDKs.
	// To ensure it's not available via N1QL, need to prefix the raw bytes with non-JSON data.
	// Prepending using append/shift/set to reduce garbage.
	body = append(body, byte(0))
	copy(body[1:], body[0:])
	body[0] = nonJSONPrefix
	err := db.Bucket.SetRaw(oldRevisionKey(docid, revid), expiry, base.BinaryDocument(body))
	if err == nil {
		base.Debugf(base.KeyCRUD, "Backed up revision body %q/%q (%d bytes, ttl:%d)", base.UD(docid), revid, len(body), expiry)
	} else {
		base.Warnf(base.KeyAll, "setOldRevisionJSON failed: doc=%q rev=%q err=%v", base.UD(docid), revid, err)
	}
	return err
}

// Extends the expiry on a revision backup.  If this fails w/ key not found, will attempt to
// recreate the revision backup when body is non-empty.
func (db *Database) refreshPreviousRevisionBackup(docid string, revid string, body []byte, expiry uint32) error {

	_, err := db.Bucket.Touch(oldRevisionKey(docid, revid), expiry)
	if base.IsKeyNotFoundError(db.Bucket, err) && len(body) > 0 {
		return db.setOldRevisionJSON(docid, revid, body, expiry)
	}
	return err
}

// Currently only used by unit tests - deletes an archived old revision from the database
func (db *Database) purgeOldRevisionJSON(docid string, revid string) error {
	base.Debugf(base.KeyCRUD, "Purging old revision backup %q / %q ", base.UD(docid), revid)
	return db.Bucket.Delete(oldRevisionKey(docid, revid))
}

//////// UTILITY FUNCTIONS:

func oldRevisionKey(docid string, revid string) string {
	return fmt.Sprintf("%s%s:%d:%s", base.RevPrefix, docid, len(revid), revid)
}

// Version of FixJSONNumbers (see base/util.go) that operates on a Body
func (body Body) FixJSONNumbers() {
	for k, v := range body {
		body[k] = base.FixJSONNumbers(v)
	}
}

func createRevID(generation int, parentRevID string, body Body) (string, error) {
	// This should produce the same results as TouchDB.
	encoding, err := base.JSONMarshalCanonical(stripSpecialProperties(body))
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
func genOfRevID(revid string) int {
	if revid == "" {
		return 0
	}
	var generation int
	n, _ := fmt.Sscanf(revid, "%d-", &generation)
	if n < 1 || generation < 1 {
		base.Warnf(base.KeyAll, "genOfRevID unsuccessful for %q", revid)
		return -1
	}
	return generation
}

// Splits a revision ID into generation number and hex digest.
func ParseRevID(revid string) (int, string) {
	if revid == "" {
		return 0, ""
	}
	var generation int
	var id string
	n, _ := fmt.Sscanf(revid, "%d-%s", &generation, &id)
	if n < 1 || generation < 1 {
		base.Warnf(base.KeyAll, "parseRevID unsuccessful for %q", revid)
		return -1, ""
	}
	return generation, id
}

// compareRevIDs compares the two rev IDs and returns:
// 1  if id1 is 'greater' than id2
// -1 if id1 is 'less' than id2
// 0  if the two are equal.
func compareRevIDs(id1, id2 string) int {
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

// stripSpecialProperties returns a copy of the given body with all underscore-prefixed keys removed, except _attachments and _deleted.
func stripSpecialProperties(b Body) Body {
	return stripSpecialPropertiesExcept(b, BodyAttachments, BodyDeleted)
}

// stripAllSpecialProperties returns a copy of the given body with all underscore-prefixed keys removed.
func stripAllSpecialProperties(b Body) Body {
	return stripSpecialPropertiesExcept(b)
}

// stripSpecialPropertiesExcept returns a copy of the given body with all underscore-prefixed keys removed, except those given.
func stripSpecialPropertiesExcept(b Body, exceptions ...string) Body {
	// Assume no properties removed for the initial capacity to reduce allocs on large docs.
	stripped := make(Body, len(b))
	for k, v := range b {
		if k == "" || k[0] != '_' ||
			base.StringSliceContains(exceptions, k) {
			// property is allowed
			stripped[k] = v
		}
	}
	return stripped
}

// containsUserSpecialProperties returns true if the given body contains a non-SG special property (underscore prefixed)
func containsUserSpecialProperties(b Body) bool {
	for k := range b {
		if k != "" && k[0] == '_' &&
			!base.ContainsString([]string{
				BodyId,
				BodyRev,
				BodyDeleted,
				BodyAttachments,
				BodyRevisions,
			}, k) {
			// body contains special property that isn't one of the above... must be user's
			return true
		}
	}
	return false
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
