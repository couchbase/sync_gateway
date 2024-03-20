//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

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

func (body Body) Copy(ctx context.Context, copyType BodyCopyType) Body {
	switch copyType {
	case BodyShallowCopy:
		return body.ShallowCopy()
	case BodyDeepCopy:
		return body.DeepCopy(ctx)
	case BodyNoCopy:
		return body
	default:
		base.InfofCtx(ctx, base.KeyCRUD, "Unexpected copy type specified in body.Copy - defaulting to shallow copy.  copyType: %d", copyType)
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

func (body Body) DeepCopy(ctx context.Context) Body {
	var copiedBody Body
	err := base.DeepCopyInefficient(&copiedBody, body)
	if err != nil {
		base.InfofCtx(ctx, base.KeyCRUD, "Error copying body: %v", err)
	}
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
		return 0, false, nil // _exp not present
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
func (c *DatabaseCollection) getOldRevisionJSON(ctx context.Context, docid string, rev string) ([]byte, error) {
	data, _, err := c.dataStore.GetRaw(oldRevisionKey(docid, rev))
	if base.IsDocNotFoundError(err) {
		base.DebugfCtx(ctx, base.KeyCRUD, "No old revision %q / %q", base.UD(docid), rev)
		err = ErrMissing
	}
	if data != nil {
		// Strip out the non-JSON prefix
		if len(data) > 0 && data[0] == nonJSONPrefix {
			data = data[1:]
		}
		base.DebugfCtx(ctx, base.KeyCRUD, "Got old revision %q / %q --> %d bytes", base.UD(docid), rev, len(data))
	}
	return data, err
}

// Makes a backup of revision body for use by delta sync, and in-flight replications requesting an old revision.
// Backup policy depends on whether delta sync and/or shared_bucket_access is enabled
//
//	delta=false || delta_rev_max_age_seconds=0
//	   - old revision stored, with expiry OldRevExpirySeconds
//	delta=true && shared_bucket_access=true
//	   - new revision stored (as duplicate), with expiry rev_max_age_seconds
//	delta=true && shared_bucket_access=false
//	   - old revision stored, with expiry rev_max_age_seconds
func (db *DatabaseCollectionWithUser) backupRevisionJSON(ctx context.Context, docId, newRev, oldRev string, newBody, oldBody []byte, newAtts AttachmentsMeta) (backupRev bool) {

	// Without delta sync, store the old rev for in-flight replication purposes
	if !db.deltaSyncEnabled() || db.deltaSyncRevMaxAgeSeconds() == 0 {
		if len(oldBody) > 0 {
			oldRevHash := base.Crc32cHashString([]byte(oldRev))
			_ = db.setOldRevisionJSON(ctx, docId, oldRevHash, oldBody, db.oldRevExpirySeconds())
		}
		return backupRev
	}

	// Otherwise, store the revs for delta generation purposes, with a longer expiry

	// Special handling for Xattrs so that SG still has revisions that were updated by an SDK write
	if db.UseXattrs() {
		backupRev = true

		// Refresh the expiry on the previous revision backup
		oldRevHash := base.Crc32cHashString([]byte(oldRev))
		_ = db.refreshPreviousRevisionBackup(ctx, docId, oldRevHash, oldBody, db.deltaSyncRevMaxAgeSeconds())
		return backupRev
	}

	// Non-xattr only need to store the previous revision, as all writes come through SG
	if len(oldBody) > 0 {
		oldRevHash := base.Crc32cHashString([]byte(oldRev))
		_ = db.setOldRevisionJSON(ctx, docId, oldRevHash, oldBody, db.deltaSyncRevMaxAgeSeconds())
	}
	return backupRev
}

func (db *DatabaseCollectionWithUser) setOldRevisionJSON(ctx context.Context, docid string, rev string, body []byte, expiry uint32) error {

	// Setting the binary flag isn't sufficient to make N1QL ignore the doc - the binary flag is only used by the SDKs.
	// To ensure it's not available via N1QL, need to prefix the raw bytes with non-JSON data.
	// Copying byte slice to make sure we don't modify the version stored in the revcache.
	nonJSONBytes := make([]byte, 1, len(body)+1)
	nonJSONBytes[0] = nonJSONPrefix
	nonJSONBytes = append(nonJSONBytes, body...)
	err := db.dataStore.SetRaw(oldRevisionKey(docid, rev), expiry, nil, nonJSONBytes)
	if err == nil {
		base.DebugfCtx(ctx, base.KeyCRUD, "Backed up revision body %q/%q (%d bytes, ttl:%d)", base.UD(docid), rev, len(body), expiry)
	} else {
		base.WarnfCtx(ctx, "setOldRevisionJSON failed: doc=%q rev=%q err=%v", base.UD(docid), rev, err)
	}
	return err
}

// Extends the expiry on a revision backup.  If this fails w/ key not found, will attempt to
// recreate the revision backup when body is non-empty.
func (db *DatabaseCollectionWithUser) refreshPreviousRevisionBackup(ctx context.Context, docid string, revid string, body []byte, expiry uint32) error {

	_, err := db.dataStore.Touch(oldRevisionKey(docid, revid), expiry)
	if base.IsKeyNotFoundError(db.dataStore, err) && len(body) > 0 {
		return db.setOldRevisionJSON(ctx, docid, revid, body, expiry)
	}
	return err
}

// Currently only used by unit tests - deletes an archived old revision from the database
func (c *DatabaseCollection) PurgeOldRevisionJSON(ctx context.Context, docid string, revid string) error {
	base.DebugfCtx(ctx, base.KeyCRUD, "Purging old revision backup %q / %q ", base.UD(docid), revid)
	return c.dataStore.Delete(oldRevisionKey(docid, revid))
}

// ////// UTILITY FUNCTIONS:

func oldRevisionKey(docid string, rev string) string {
	return fmt.Sprintf("%s%s:%d:%s", base.RevPrefix, docid, len(rev), rev)
}

// Version of FixJSONNumbers (see base/util.go) that operates on a Body
func (body Body) FixJSONNumbers() {
	for k, v := range body {
		body[k] = base.FixJSONNumbers(v)
	}
}

func CreateRevID(generation int, parentRevID string, body Body) (string, error) {
	// This should produce the same results as TouchDB.
	strippedBody, _ := stripInternalProperties(body)
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
func genOfRevID(ctx context.Context, revid string) int {
	if revid == "" {
		return 0
	}
	var generation int
	n, _ := fmt.Sscanf(revid, "%d-", &generation)
	if n < 1 || generation < 1 {
		base.WarnfCtx(ctx, "genOfRevID unsuccessful for %q", revid)
		return -1
	}
	return generation
}

// Splits a revision ID into generation number and hex digest.
func ParseRevID(ctx context.Context, revid string) (int, string) {
	if revid == "" {
		return 0, ""
	}

	idx := strings.Index(revid, "-")
	if idx == -1 {
		base.WarnfCtx(ctx, "parseRevID found no separator in rev %q", revid)
		return -1, ""
	}

	gen, err := strconv.Atoi(revid[:idx])
	if err != nil {
		base.WarnfCtx(ctx, "parseRevID unexpected generation in rev %q: %s", revid, err)
		return -1, ""
	} else if gen < 1 {
		base.WarnfCtx(ctx, "parseRevID unexpected generation in rev %q", revid)
		return -1, ""
	}

	return gen, revid[idx+1:]
}

// compareRevIDs compares the two rev IDs and returns:
// 1  if id1 is 'greater' than id2
// -1 if id1 is 'less' than id2
// 0  if the two are equal.
func compareRevIDs(ctx context.Context, id1, id2 string) int {
	gen1, sha1 := ParseRevID(ctx, id1)
	gen2, sha2 := ParseRevID(ctx, id2)
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

// stripInternalProperties returns a copy of the given body with all internal underscore-prefixed keys removed, except _attachments and _deleted.
func stripInternalProperties(b Body) (Body, bool) {
	return stripSpecialProperties(b, true)
}

// stripAllInternalProperties returns a copy of the given body with all underscore-prefixed keys removed.
func stripAllSpecialProperties(b Body) (Body, bool) {
	return stripSpecialProperties(b, false)
}

// stripSpecialPropertiesExcept returns a copy of the given body with underscore-prefixed keys removed.
// Set internalOnly to only strip internal properties except _deleted and _attachments
func stripSpecialProperties(b Body, internalOnly bool) (sb Body, foundSpecialProps bool) {
	// Assume no properties removed for the initial capacity to reduce allocs on large docs.
	stripped := make(Body, len(b))
	for k, v := range b {
		// Property is not stripped if:
		// - It is blank
		// - Does not start with an underscore ('_')
		// - Is not an internal special property (this check is excluded when internalOnly = false)
		if k == "" || k[0] != '_' || (internalOnly && (!strings.HasPrefix(k, BodyInternalPrefix) &&
			!base.StringSliceContains([]string{
				base.SyncPropertyName,
				BodyId,
				BodyRev,
				BodyRevisions,
				BodyExpiry,
				BodyPurged,
				BodyRemoved,
			}, k))) {
			// property is allowed
			stripped[k] = v
		} else {
			foundSpecialProps = true
		}
	}

	if foundSpecialProps {
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
