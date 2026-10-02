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

	"github.com/couchbase/sync_gateway/db"
)

// Document metadata keys the test server adds to the properties it returns from GetDocument.
const (
	// DocumentIDProperty holds the document ID.
	DocumentIDProperty = "_id"
	// DocumentRevsProperty holds the revision history, current revision first.
	DocumentRevsProperty = "_revs"
)

// ServerInfo is the response from GET /, the only endpoint that needs no session.
type ServerInfo struct {
	Version    string `json:"version"`
	APIVersion int    `json:"apiVersion"`
	CBL        string `json:"cbl"`
}

// DatabaseSpec describes how Reset should create one database.
type DatabaseSpec struct {
	// Collections is a list of fully qualified "<scope>.<collection>" names.
	Collections []string `json:"collections,omitempty"`
}

// DocumentRef identifies a document within a database.
type DocumentRef struct {
	// Collection is the fully qualified "<scope>.<collection>" name.
	Collection string `json:"collection"`
	ID         string `json:"id"`
}

// Document is a document's properties as returned by GetDocument, including the _id and _revs
// metadata the test server adds.
type Document map[string]any

// ID returns the document ID.
func (d Document) ID() string {
	id, _ := d[DocumentIDProperty].(string)
	return id
}

// Revs returns the raw revision history metadata.
func (d Document) Revs() string {
	revs, _ := d[DocumentRevsProperty].(string)
	return revs
}

// Properties returns a copy of the document body with the test server's metadata removed.
func (d Document) Properties() map[string]any {
	properties := make(map[string]any, len(d))
	for key, value := range d {
		if key == DocumentIDProperty || key == DocumentRevsProperty {
			continue
		}
		properties[key] = value
	}
	return properties
}

// HLV parses the document's _revs metadata into a hybrid logical vector.  A 4.x client reports it
// in the same format Sync Gateway uses on the blip wire, so this uses Sync Gateway's own parser: a
// format disagreement between the two shows up here as an error rather than as a wrong vector.
//
// Any legacy revtree IDs found in the history are returned separately.
func (d Document) HLV() (*db.HybridLogicalVector, []string, error) {
	hlv, legacyRevs, err := db.ExtractHLVFromBlipString(d.Revs())
	if err != nil {
		return nil, nil, fmt.Errorf("cbltestclient: parsing revision history %q: %w", d.Revs(), err)
	}
	return hlv, legacyRevs, nil
}

// DocumentEntry is one element of a GetAllDocuments response.
type DocumentEntry struct {
	ID  string `json:"id"`
	Rev string `json:"rev"`
}

// UpdateType is the kind of change one DatabaseUpdateItem makes.
type UpdateType string

const (
	// UpdateTypeUpdate creates the document if it is absent, then applies the property changes.
	UpdateTypeUpdate UpdateType = "UPDATE"
	// UpdateTypeDelete deletes the document, leaving a tombstone that replicates.
	UpdateTypeDelete UpdateType = "DELETE"
)

// DatabaseUpdateItem is a single change for UpdateDatabase.  Properties are merged in by keypath,
// not replaced as a whole body.
type DatabaseUpdateItem struct {
	Type UpdateType `json:"type"`
	// Collection is the fully qualified "<scope>.<collection>" name.
	Collection string `json:"collection"`
	DocumentID string `json:"documentID"`
	// UpdatedProperties maps keypaths to their new values, applied in order.
	UpdatedProperties []map[string]any `json:"updatedProperties,omitempty"`
}

// ReplicatorType is the direction of a replication.
type ReplicatorType string

const ReplicatorTypePushAndPull ReplicatorType = "pushAndPull"

// AuthenticatorType is the kind of credential a replicator presents.
type AuthenticatorType string

const AuthenticatorTypeBasic AuthenticatorType = "BASIC"

// Authenticator is the credential a replicator presents to the remote.
type Authenticator struct {
	Type     AuthenticatorType `json:"type"`
	Username string            `json:"username,omitempty"`
	Password string            `json:"password,omitempty"`
}

// ReplicationCollection is the set of collections that share one replication configuration.
type ReplicationCollection struct {
	// Names are fully qualified "<scope>.<collection>" names.
	Names []string `json:"names"`
}

// ReplicatorConfig configures a replicator for StartReplicator.
type ReplicatorConfig struct {
	Database    string                  `json:"database"`
	Collections []ReplicationCollection `json:"collections"`
	// Endpoint is the remote's websocket URL, e.g. "ws://127.0.0.1:4984/db".
	Endpoint       string         `json:"endpoint"`
	ReplicatorType ReplicatorType `json:"replicatorType,omitempty"`
	Continuous     bool           `json:"continuous,omitempty"`
	Authenticator  *Authenticator `json:"authenticator,omitempty"`
	// EnableAutoPurge purges documents the user loses access to.  The test server defaults this to
	// true, so it is always sent rather than omitted when false.
	EnableAutoPurge bool `json:"enableAutoPurge"`
}

// ReplicatorActivity is the replicator's current activity level.
type ReplicatorActivity string

const ReplicatorActivityStopped ReplicatorActivity = "STOPPED"

// ReplicatorStatus is the response from ReplicatorStatus.
type ReplicatorStatus struct {
	Activity ReplicatorActivity `json:"activity"`
	Error    *APIError          `json:"error,omitempty"`
}
