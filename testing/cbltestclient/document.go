// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"fmt"
	"slices"
	"strings"

	"github.com/couchbase/sync_gateway/db"
)

// keypathMetacharacters are the characters the test server's keypath syntax gives a meaning to.  A
// property name containing one would address something other than that top-level property, so
// ReplaceBodyUpdate refuses it rather than writing to the wrong place.
const keypathMetacharacters = `.[]\`

// ReplaceBodyUpdate builds the single update item that makes a document's properties exactly equal
// to newBody, given the properties it has now.  Pass a nil current for a document that does not
// exist yet.
//
// UpdateDatabase merges by keypath rather than replacing the body, so a plain update would leave
// behind any property newBody drops.  The removals are computed as a set difference rather than
// relying on the server applying removedProperties before updatedProperties, so the result is
// correct either way.  It is one item, and so one revision, because two items would give the
// document two current versions and make the caller's assertions meaningless.
func ReplaceBodyUpdate(collection, docID string, current, newBody map[string]any) (DatabaseUpdateItem, error) {
	updated := make(map[string]any, len(newBody))
	for key, value := range newBody {
		if err := validateTopLevelKey(key); err != nil {
			return DatabaseUpdateItem{}, err
		}
		updated[key] = value
	}

	var removed []string
	for key := range current {
		if key == DocumentIDProperty || key == DocumentRevsProperty {
			continue
		}
		if _, retained := newBody[key]; retained {
			continue
		}
		if err := validateTopLevelKey(key); err != nil {
			return DatabaseUpdateItem{}, err
		}
		removed = append(removed, key)
	}
	// sorted so that the same pair of bodies always produces the same request, which keeps test
	// failures and captured server logs comparable between runs
	slices.Sort(removed)

	item := DatabaseUpdateItem{
		Type:              UpdateTypeUpdate,
		Collection:        collection,
		DocumentID:        docID,
		RemovedProperties: removed,
	}
	if len(updated) > 0 {
		item.UpdatedProperties = []map[string]any{updated}
	}
	return item, nil
}

// DeleteUpdate builds the update item that deletes a document, leaving a tombstone that replicates.
func DeleteUpdate(collection, docID string) DatabaseUpdateItem {
	return DatabaseUpdateItem{Type: UpdateTypeDelete, Collection: collection, DocumentID: docID}
}

// PurgeUpdate builds the update item that removes a document locally without leaving a tombstone.
func PurgeUpdate(collection, docID string) DatabaseUpdateItem {
	return DatabaseUpdateItem{Type: UpdateTypePurge, Collection: collection, DocumentID: docID}
}

// validateTopLevelKey rejects a property name the test server would read as a compound keypath.
func validateTopLevelKey(key string) error {
	if key == "" {
		return fmt.Errorf("cbltestclient: empty property name is not a valid keypath")
	}
	if strings.ContainsAny(key, keypathMetacharacters) {
		return fmt.Errorf("cbltestclient: property name %q contains a keypath metacharacter (one of %q) and cannot be addressed as a top-level property", key, keypathMetacharacters)
	}
	return nil
}

// ParseRevisionHistory parses a document's _revs metadata into a hybrid logical vector.
//
// A 4.x client reports a version vector in the same format Sync Gateway uses on the blip wire -
// "cv[,mv,mv][;pv,...]" - so this is the same parser, which means a disagreement between what
// Couchbase Lite writes and what Sync Gateway reads shows up here as an error rather than as a
// silently wrong vector.
//
// Any legacy revtree IDs found in the history are returned separately; a 4.x client replicating
// with version vectors should produce none, so a caller seeing them is talking to a 3.x client.
func ParseRevisionHistory(revs string) (*db.HybridLogicalVector, []string, error) {
	hlv, legacyRevs, err := db.ParseHLVFromBlipString(revs)
	if err != nil {
		return nil, nil, fmt.Errorf("cbltestclient: parsing revision history %q: %w", revs, err)
	}
	return hlv, legacyRevs, nil
}

// HLV parses the document's _revs metadata into a hybrid logical vector.
func (d Document) HLV() (*db.HybridLogicalVector, []string, error) {
	return ParseRevisionHistory(d.Revs())
}
