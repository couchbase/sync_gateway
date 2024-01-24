/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

// HLVAgent performs HLV updates directly (not via SG) for simulating/testing interaction with non-SG HLV agents
type HLVAgent struct {
	t         *testing.T
	datastore base.DataStore
	source    string // All writes by the HLVHelper are done as this source
	xattrName string // xattr name to store the HLV
}

var defaultHelperBody = map[string]interface{}{"version": 1}

func NewHLVAgent(t *testing.T, datastore base.DataStore, source string, xattrName string) *HLVAgent {
	return &HLVAgent{
		t:         t,
		datastore: datastore,
		source:    source, // all writes by the HLVHelper are done as this source
		xattrName: xattrName,
	}
}

// InsertWithHLV inserts a new document into the bucket with a populated HLV (matching a write from
// a different HLV-aware peer)
func (h *HLVAgent) InsertWithHLV(ctx context.Context, key string) (casOut uint64) {
	hlv := &HybridLogicalVector{}
	err := hlv.AddVersion(CreateVersion(h.source, hlvExpandMacroCASValue))
	require.NoError(h.t, err)
	hlv.CurrentVersionCAS = hlvExpandMacroCASValue

	syncData := &SyncData{HLV: hlv}
	syncDataBytes, err := base.JSONMarshal(syncData)
	require.NoError(h.t, err)

	mutateInOpts := &sgbucket.MutateInOptions{
		MacroExpansion: hlv.computeMacroExpansions(),
	}

	docBody := base.MustJSONMarshal(h.t, defaultHelperBody)
	xattrData := map[string][]byte{
		h.xattrName: syncDataBytes,
	}

	cas, err := h.datastore.WriteWithXattrs(ctx, key, 0, 0, docBody, xattrData, mutateInOpts)
	require.NoError(h.t, err)
	return cas
}
