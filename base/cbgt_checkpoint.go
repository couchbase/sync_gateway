/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"encoding/json"
	"maps"
)

// cbgtCheckpointWriter builds the raw JSON persisted for a DCP checkpoint.  cbgt's own checkpoint value (from
// OpaqueSet) is captured as opaque per-field raw JSON, so no field is lost on persistence, with SG's own LastSeq
// tracking field - the last sequence SG actually processed for the vbucket - added in.
type cbgtCheckpointWriter struct {
	LastSeq uint64
	fields  map[string]json.RawMessage
}

// newCbgtCheckpointWriter parses a cbgt-supplied checkpoint value for persistence, along with the last sequence SG
// actually processed for the vbucket.  A nil/empty raw value (e.g. a vbucket with no prior cbgt checkpoint) is
// treated as an empty set of cbgt fields, rather than a JSON parse error.
func newCbgtCheckpointWriter(raw []byte, lastSeq uint64) (cbgtCheckpointWriter, error) {
	var fields map[string]json.RawMessage
	if len(raw) > 0 {
		if err := JSONUnmarshal(raw, &fields); err != nil {
			return cbgtCheckpointWriter{}, err
		}
	}
	return cbgtCheckpointWriter{LastSeq: lastSeq, fields: fields}, nil
}

// MarshalJSON flattens LastSeq into fields, so the persisted checkpoint is a single JSON object matching cbgt's own
// checkpoint shape plus SG's lastSeq, rather than a nested struct.
func (w cbgtCheckpointWriter) MarshalJSON() ([]byte, error) {
	out := make(map[string]json.RawMessage, len(w.fields)+1)
	maps.Copy(out, w.fields)
	lastSeqRaw, err := JSONMarshal(w.LastSeq)
	if err != nil {
		return nil, err
	}
	out["lastSeq"] = lastSeqRaw
	return JSONMarshal(out)
}

// cbgtCheckpointReader parses a persisted DCP checkpoint for loading.  It exposes cbgt's own checkpoint metadata
// (mirrored by CbgtOpaqueMetadata) with typed field access for SG's own logic, alongside the raw per-field
// JSON (fields), so reconstructing the checkpoint to hand back to cbgt doesn't drop fields SG doesn't model.
type cbgtCheckpointReader struct {
	LastSeq uint64 `json:"lastSeq"`
	*CbgtOpaqueMetadata
	fields map[string]json.RawMessage
}

// newCbgtCheckpointReader parses a raw persisted checkpoint value.  The raw bytes are tokenized into per-field raw
// JSON, for reconstructing cbgt's checkpoint value without dropping fields SG doesn't model, alongside a single
// typed decode of the fields SG does model, via the json tags on cbgtCheckpointReader/CbgtOpaqueMetadata.
func newCbgtCheckpointReader(raw []byte) (cbgtCheckpointReader, error) {
	var fields map[string]json.RawMessage
	if err := JSONUnmarshal(raw, &fields); err != nil {
		return cbgtCheckpointReader{}, err
	}
	reader := cbgtCheckpointReader{CbgtOpaqueMetadata: &CbgtOpaqueMetadata{}, fields: fields}
	if err := JSONUnmarshal(raw, &reader); err != nil {
		return cbgtCheckpointReader{}, err
	}
	return reader, nil
}

// extractLastSeq returns the last sequence SG actually processed for the vbucket, along with the raw JSON for
// cbgt's own checkpoint fields (i.e. without SG's lastSeq), reconstructed from fields so any field SG doesn't
// model survives.  Checkpoints persisted before lastSeq was tracked have no lastSeq field at all (as opposed to a
// legitimately-persisted value of zero), so this falls back to the checkpoint's snapStart only in that case.
func (r cbgtCheckpointReader) extractLastSeq() (lastSeq uint64, rawMetadata []byte, err error) {
	if _, ok := r.fields["lastSeq"]; ok {
		lastSeq = r.LastSeq
	} else {
		lastSeq = r.SnapStart
	}
	delete(r.fields, "lastSeq")
	// A checkpoint containing only SG's lastSeq (no cbgt metadata) must round-trip back to nil, not "{}", so
	// OpaqueGet's len(metadata) == 0 check still treats it as "no cbgt checkpoint".
	if len(r.fields) == 0 {
		return lastSeq, nil, nil
	}
	rawMetadata, err = JSONMarshal(r.fields)
	return lastSeq, rawMetadata, err
}

// readCbgtCheckpoint parses a persisted DCP checkpoint value, returning the last sequence SG actually processed
// for the vbucket alongside cbgt's own checkpoint metadata (i.e. without SG's lastSeq field), for use by DCPCommon.
func readCbgtCheckpoint(rawValue []byte) (lastSeq uint64, metadata []byte, err error) {
	reader, err := newCbgtCheckpointReader(rawValue)
	if err != nil {
		return 0, nil, err
	}
	return reader.extractLastSeq()
}

// createCbgtCheckpoint builds the raw JSON to persist for a DCP checkpoint from cbgt's own opaque checkpoint value
// and the last sequence SG actually processed for the vbucket, for use by DCPCommon.
func createCbgtCheckpoint(opaqueValue []byte, lastSeq uint64) ([]byte, error) {
	writer, err := newCbgtCheckpointWriter(opaqueValue, lastSeq)
	if err != nil {
		return nil, err
	}
	return JSONMarshal(writer)
}
