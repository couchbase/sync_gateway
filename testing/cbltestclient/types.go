// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"strconv"
	"strings"
)

// Document metadata keys the test server adds to the properties it returns from GetDocument.
const (
	// DocumentIDProperty holds the document ID.
	DocumentIDProperty = "_id"
	// DocumentRevsProperty holds the revision history, current revision first.  For a 4.x client
	// that is a version vector in the format db.ParseHLVFromBlipString accepts.
	DocumentRevsProperty = "_revs"
)

// ServerInfo is the response from GET /, the only endpoint that needs no session.
type ServerInfo struct {
	Version        string `json:"version"`
	APIVersion     int    `json:"apiVersion"`
	CBL            string `json:"cbl"`
	Device         Device `json:"device"`
	AdditionalInfo string `json:"additionalInfo,omitempty"`
}

// Device describes the host the test server is running on.
type Device struct {
	Model            string `json:"model,omitempty"`
	SystemName       string `json:"systemName,omitempty"`
	SystemVersion    string `json:"systemVersion,omitempty"`
	SystemAPIVersion string `json:"systemApiVersion,omitempty"`
}

// IsEnterprise reports whether the server is linked against an Enterprise Edition Couchbase Lite.
// The edition is only reported in the free-form AdditionalInfo field ("Edition: Enterprise, Build: 2"),
// so this parses it rather than reading a dedicated field.
func (s ServerInfo) IsEnterprise() bool {
	for _, field := range strings.Split(s.AdditionalInfo, ",") {
		name, value, found := strings.Cut(field, ":")
		if found && strings.EqualFold(strings.TrimSpace(name), "edition") {
			return strings.EqualFold(strings.TrimSpace(value), "enterprise")
		}
	}
	return false
}

// MajorVersion returns the major component of the Couchbase Lite version, or 0 if it can't be
// parsed.  Version is "<major>.<minor>.<patch>" for a release and "<major>.<minor>.<patch>-<build>"
// for a CI build.
func (s ServerInfo) MajorVersion() int {
	major, _, _ := strings.Cut(s.Version, ".")
	parsed, err := strconv.Atoi(major)
	if err != nil {
		return 0
	}
	return parsed
}

// DatabaseSpec describes how Reset should create one database.  Collections and Dataset are
// mutually exclusive, and a zero DatabaseSpec creates an empty database.
type DatabaseSpec struct {
	// Collections is a list of fully qualified "<scope>.<collection>" names.
	Collections []string `json:"collections,omitempty"`
	// Dataset is the URL of a prebuilt database to unpack instead of creating an empty one.
	Dataset string `json:"dataset,omitempty"`
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

// Revs returns the raw revision history metadata.  Use ParseRevisionHistory to turn it into an HLV.
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

// DocumentEntry is one element of a GetAllDocuments response.  Rev is a revtree ID or an HLV
// current version, depending on what the client is using.
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
	// UpdateTypePurge removes the document locally without leaving a tombstone.
	UpdateTypePurge UpdateType = "PURGE"
)

// DatabaseUpdateItem is a single change for UpdateDatabase.  Properties are addressed by keypath,
// not by whole body - see ReplaceBodyUpdate for the whole-body-replace case.
//
// The test server applies RemovedProperties before UpdatedProperties.
type DatabaseUpdateItem struct {
	Type UpdateType `json:"type"`
	// Collection is the fully qualified "<scope>.<collection>" name.
	Collection string `json:"collection"`
	DocumentID string `json:"documentID"`
	// UpdatedProperties maps keypaths to their new values, applied in order.
	UpdatedProperties []map[string]any `json:"updatedProperties,omitempty"`
	// RemovedProperties are keypaths to delete.
	RemovedProperties []string `json:"removedProperties,omitempty"`
	// UpdatedBlobs maps keypaths to the URL of the blob to store there.
	UpdatedBlobs map[string]string `json:"updatedBlobs,omitempty"`
}

// ReplicatorType is the direction of a replication.
type ReplicatorType string

const (
	ReplicatorTypePush        ReplicatorType = "push"
	ReplicatorTypePull        ReplicatorType = "pull"
	ReplicatorTypePushAndPull ReplicatorType = "pushAndPull"
)

// AuthenticatorType is the kind of credential a replicator presents.
type AuthenticatorType string

const (
	AuthenticatorTypeBasic   AuthenticatorType = "BASIC"
	AuthenticatorTypeSession AuthenticatorType = "SESSION"
)

// Authenticator is the credential a replicator presents to the remote.  Username and Password are
// used for AuthenticatorTypeBasic, SessionID and Cookie for AuthenticatorTypeSession.
type Authenticator struct {
	Type      AuthenticatorType `json:"type"`
	Username  string            `json:"username,omitempty"`
	Password  string            `json:"password,omitempty"`
	SessionID string            `json:"sessionID,omitempty"`
	Cookie    string            `json:"cookieName,omitempty"`
}

// ReplicationCollection is the set of collections that share one replication configuration.
type ReplicationCollection struct {
	// Names are fully qualified "<scope>.<collection>" names.
	Names            []string `json:"names"`
	Channels         []string `json:"channels,omitempty"`
	DocumentIDs      []string `json:"documentIDs,omitempty"`
	PushFilter       *Filter  `json:"pushFilter,omitempty"`
	PullFilter       *Filter  `json:"pullFilter,omitempty"`
	ConflictResolver *Filter  `json:"conflictResolver,omitempty"`
}

// Filter names one of the test server's built-in replication filters or conflict resolvers, with
// its parameters.  See spec/replication-filters.md and spec/conflict-resolvers.md.
type Filter struct {
	Name   string         `json:"name"`
	Params map[string]any `json:"params,omitempty"`
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
	// EnableDocumentListener makes ReplicatorStatus report the documents replicated since the
	// previous call.
	EnableDocumentListener bool `json:"enableDocumentListener,omitempty"`
	// EnableAutoPurge purges documents the user loses access to.  The test server defaults this to
	// true, so it is serialised unconditionally rather than omitted when false.
	EnableAutoPurge  bool              `json:"enableAutoPurge"`
	Headers          map[string]string `json:"headers,omitempty"`
	PinnedServerCert string            `json:"pinnedServerCert,omitempty"`
}

// ReplicatorActivity is the replicator's current activity level.
type ReplicatorActivity string

const (
	ReplicatorActivityStopped    ReplicatorActivity = "STOPPED"
	ReplicatorActivityOffline    ReplicatorActivity = "OFFLINE"
	ReplicatorActivityConnecting ReplicatorActivity = "CONNECTING"
	ReplicatorActivityIdle       ReplicatorActivity = "IDLE"
	ReplicatorActivityBusy       ReplicatorActivity = "BUSY"
)

// ReplicatorStatus is the response from GetReplicatorStatus.
type ReplicatorStatus struct {
	Activity ReplicatorActivity `json:"activity"`
	Progress struct {
		Completed bool `json:"completed"`
	} `json:"progress"`
	// Documents is populated only when the config set EnableDocumentListener, and reports the
	// documents replicated since the previous call - the test server clears them once returned.
	Documents []ReplicatedDocument `json:"documents,omitempty"`
	Error     *APIError            `json:"error,omitempty"`
}

// ReplicatedDocument is one document reported by a replicator's document listener.
type ReplicatedDocument struct {
	Collection string    `json:"collection"`
	DocumentID string    `json:"documentID"`
	IsPush     bool      `json:"isPush"`
	Flags      []string  `json:"flags"`
	Error      *APIError `json:"error,omitempty"`
}

// VerifyResult is the response from VerifyDocuments.
type VerifyResult struct {
	Result bool `json:"result"`
	// Description explains the first difference found, when Result is false.
	Description string `json:"description,omitempty"`
	Expected    any    `json:"expected,omitempty"`
	Actual      any    `json:"actual,omitempty"`
	Document    any    `json:"document,omitempty"`
}

// MaintenanceType is the kind of work PerformMaintenance does.
type MaintenanceType string

const (
	MaintenanceTypeCompact   MaintenanceType = "compact"
	MaintenanceTypeIntegrity MaintenanceType = "integrityCheck"
	MaintenanceTypeOptimize  MaintenanceType = "optimize"
	MaintenanceTypeFullSync  MaintenanceType = "fullOptimize"
)
