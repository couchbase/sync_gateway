// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"time"

	"github.com/google/uuid"
)

// MetadataMigrationStatusDocID is the document key for the bucket-level metadata-migration status
// doc. The doc is stored directly in _system._mobile from creation and is never routed through the
// dual-collection bootstrap wrapper — its location is what tells callers whether migration is
// running or done in the first place.
const MetadataMigrationStatusDocID = "_sync:metadata_migration_status"

// MigrationState is the per-database (or bootstrap-scope) state in the metadata-migration lifecycle.
type MigrationState string

const (
	MigrationStateNotStarted MigrationState = "not_started"
	MigrationStateInProgress MigrationState = "in_progress"
	MigrationStateComplete   MigrationState = "complete"
)

// MetadataMigrationStatus tracks the per-bucket metadata-migration lifecycle. One doc per bucket,
// keyed by MetadataMigrationStatusDocID, born in _system._mobile.
//
// Databases is keyed by RegistryDatabase.MetadataID so renames don't break tracking. Bootstrap is
// the bucket-global state covering _sync:registry / _sync:dbconfig:* / cbgt cfg docs; it only
// transitions to complete once every entry in Databases is complete.
type MetadataMigrationStatus struct {
	BucketMigrationID string                              `json:"bucket_migration_id"`
	Databases         map[string]*DatabaseMigrationStatus `json:"databases"`
	Bootstrap         BootstrapMigrationStatus            `json:"bootstrap"`
}

// DatabaseMigrationStatus is the per-database entry in the status doc.
type DatabaseMigrationStatus struct {
	State       MigrationState `json:"state"`
	StartedAt   *time.Time     `json:"started_at,omitempty"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
}

// BootstrapMigrationStatus is the bucket-global state for bootstrap docs.
type BootstrapMigrationStatus struct {
	State       MigrationState `json:"state"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
}

// NewMetadataMigrationStatus returns a freshly-stamped status doc with an empty databases map and
// bootstrap.state = not_started. The caller is expected to InsertMetadataMigrationStatus this into
// the bucket; concurrent stamp attempts from other SG nodes will receive ErrAlreadyExists.
func NewMetadataMigrationStatus() *MetadataMigrationStatus {
	return &MetadataMigrationStatus{
		BucketMigrationID: uuid.NewString(),
		Databases:         map[string]*DatabaseMigrationStatus{},
		Bootstrap:         BootstrapMigrationStatus{State: MigrationStateNotStarted},
	}
}

// AllDatabasesComplete reports whether every metadataID in expected has a Databases entry in
// state complete. An entry that is missing, in_progress, or not_started counts as not complete.
// Callers must pass the live registry-derived set so a DB added mid-migration isn't missed.
func (s *MetadataMigrationStatus) AllDatabasesComplete(expected []string) bool {
	if len(expected) == 0 {
		return false
	}
	for _, id := range expected {
		entry, ok := s.Databases[id]
		if !ok || entry == nil || entry.State != MigrationStateComplete {
			return false
		}
	}
	return true
}
