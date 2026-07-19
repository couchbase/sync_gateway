/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

// cbgtOpaqueCheckpoint mirrors cbgt's own private metaData struct (feed_dcp_gocbcore.go) - the opaque checkpoint
// value cbgt itself round-trips through OpaqueSet/OpaqueGet. Its fields (name, type, and json tag) must stay
// identical to cbgt's - TestCbgtOpaqueMetadataMatchesCbgtMetaData (cbgt_checkpoint_test.go) parses cbgt's actual
// source at test time to verify this, rather than relying on a hand-maintained mirror staying in sync.
type cbgtOpaqueCheckpoint struct {
	FailOverLog [][]uint64 `json:"failOverLog"`
	SeqStart    uint64     `json:"seqStart"`
	SeqEnd      uint64     `json:"seqEnd"`
	SnapStart   uint64     `json:"snapStart"`
	SnapEnd     uint64     `json:"snapEnd"`
}

// isEmpty reports whether none of cbgt's own checkpoint fields are populated, i.e. there's no cbgt checkpoint to
// speak of - in practice cbgt doesn't ever persist a legitimately all-zero checkpoint (a real one always carries
// at least a non-empty failOverLog).
func (c cbgtOpaqueCheckpoint) isEmpty() bool {
	return len(c.FailOverLog) == 0 && c.SeqStart == 0 && c.SeqEnd == 0 && c.SnapStart == 0 && c.SnapEnd == 0
}

// CbgtCheckpoint is the full shape of a persisted DCP checkpoint: cbgt's own opaque checkpoint fields, embedded so
// they marshal/unmarshal at the top level (matching cbgt's own flat JSON shape), plus SG's own LastSeq tracking
// field - the last sequence SG actually processed for the vbucket, which isn't part of cbgt's own checkpoint shape.
type CbgtCheckpoint struct {
	cbgtOpaqueCheckpoint
	LastSeq uint64 `json:"lastSeq"`
}

// readCbgtCheckpoint parses a persisted DCP checkpoint value, returning the last sequence SG actually processed
// for the vbucket alongside cbgt's own opaque checkpoint fields (i.e. without SG's lastSeq), for use by DCPCommon.
// Checkpoints persisted before lastSeq was tracked have no lastSeq field at all, which unmarshals the same as a
// legitimately-persisted value of zero, so this falls back to the checkpoint's snapStart in either case.
func readCbgtCheckpoint(rawValue []byte) (lastSeq uint64, metadata []byte, err error) {
	var checkpoint CbgtCheckpoint
	if err := JSONUnmarshal(rawValue, &checkpoint); err != nil {
		return 0, nil, err
	}

	lastSeq = checkpoint.LastSeq
	if lastSeq == 0 {
		lastSeq = checkpoint.SnapStart
	}

	// A checkpoint with no cbgt-owned fields at all must round-trip back to nil, not "{}", so OpaqueGet's
	// len(metadata) == 0 check still treats it as "no cbgt checkpoint".
	if checkpoint.cbgtOpaqueCheckpoint.isEmpty() {
		return lastSeq, nil, nil
	}
	metadata, err = JSONMarshal(checkpoint.cbgtOpaqueCheckpoint)
	return lastSeq, metadata, err
}

// createCbgtCheckpoint builds the raw JSON to persist for a DCP checkpoint from cbgt's own opaque checkpoint value
// and the last sequence SG actually processed for the vbucket, for use by DCPCommon. A nil/empty opaqueValue (e.g.
// a vbucket with no prior cbgt checkpoint) is treated as an empty cbgt checkpoint, rather than a JSON parse error.
func createCbgtCheckpoint(opaqueValue []byte, lastSeq uint64) ([]byte, error) {
	checkpoint := CbgtCheckpoint{LastSeq: lastSeq}
	if len(opaqueValue) > 0 {
		if err := JSONUnmarshal(opaqueValue, &checkpoint.cbgtOpaqueCheckpoint); err != nil {
			return nil, err
		}
	}
	return JSONMarshal(checkpoint)
}
