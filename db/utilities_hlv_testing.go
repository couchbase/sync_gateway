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
	"fmt"
	"strings"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

// HLVAgent performs HLV updates directly (not via SG) for simulating/testing interaction with non-SG HLV agents
type HLVAgent struct {
	t         *testing.T
	datastore base.DataStore
	Source    string // All writes by the HLVHelper are done as this source
	xattrName string // xattr name to store the HLV
}

var defaultHelperBody = map[string]interface{}{"version": 1}

func NewHLVAgent(t *testing.T, datastore base.DataStore, source string, xattrName string) *HLVAgent {
	return &HLVAgent{
		t:         t,
		datastore: datastore,
		Source:    EncodeSource(source), // all writes by the HLVHelper are done as this source
		xattrName: xattrName,
	}
}

// InsertWithHLV inserts a new document into the bucket with a populated HLV (matching a write from
// a different HLV-aware peer)
func (h *HLVAgent) InsertWithHLV(ctx context.Context, key string) (casOut uint64) {
	hlv := &HybridLogicalVector{}
	err := hlv.AddVersion(CreateVersion(h.Source, hlvExpandMacroCASValue))
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

// EncodeTestVersion converts a simplified string version of the form 1@abc to a hex-encoded version and base64 encoded
// source, like 0x0100000000000000@YWJj.  Allows use of simplified versions in tests for readability, ease of use.
func EncodeTestVersion(versionString string) (encodedString string) {
	timestampString, source, found := strings.Cut(versionString, "@")
	if !found {
		return versionString
	}
	hexTimestamp, err := EncodeValueStr(timestampString)
	if err != nil {
		panic(fmt.Sprintf("unable to encode timestampString %v", timestampString))
	}
	base64Source := EncodeSource(source)
	return hexTimestamp + "@" + base64Source
}

// encodeTestHistory converts a simplified version history of the form "1@abc,2@def;3@ghi" to use hex-encoded versions and
// base64 encoded sources
func EncodeTestHistory(historyString string) (encodedString string) {
	// possible versionSets are pv;mv
	// possible versionSets are pv;mv
	versionSets := strings.Split(historyString, ";")
	if len(versionSets) == 0 {
		return ""
	}
	for index, versionSet := range versionSets {
		// versionSet delimiter
		if index > 0 {
			encodedString += ";"
		}
		versions := strings.Split(versionSet, ",")
		for index, version := range versions {
			// version delimiter
			if index > 0 {
				encodedString += ","
			}
			encodedString += EncodeTestVersion(version)
		}
	}
	return encodedString
}

// ParseTestHistory takes a string test history in the form 1@abc,2@def;3@ghi,4@jkl and formats this
// as pv and mv maps keyed by encoded source, with encoded values
func ParseTestHistory(t *testing.T, historyString string) (pv map[string]string, mv map[string]string) {
	versionSets := strings.Split(historyString, ";")

	pv = make(map[string]string)
	mv = make(map[string]string)

	var pvString, mvString string
	switch len(versionSets) {
	case 1:
		pvString = versionSets[0]
	case 2:
		mvString = versionSets[0]
		pvString = versionSets[1]
	default:
		return pv, mv
	}

	// pv
	for _, versionStr := range strings.Split(pvString, ",") {
		version, err := ParseVersion(versionStr)
		require.NoError(t, err)
		encodedValue, err := EncodeValueStr(version.Value)
		require.NoError(t, err)
		pv[EncodeSource(version.SourceID)] = encodedValue
	}

	// mv
	if mvString != "" {
		for _, versionStr := range strings.Split(mvString, ",") {
			version, err := ParseVersion(versionStr)
			require.NoError(t, err)
			encodedValue, err := EncodeValueStr(version.Value)
			require.NoError(t, err)
			mv[EncodeSource(version.SourceID)] = encodedValue
		}
	}
	return pv, mv
}

// Requires that the CV for the provided HLV matches the expected CV (sent in simplified test format)
func RequireCVEqual(t *testing.T, hlv *HybridLogicalVector, expectedCV string) {
	testVersion, err := ParseVersion(expectedCV)
	require.NoError(t, err)
	require.Equal(t, EncodeSource(testVersion.SourceID), hlv.SourceID)
	encodedValue, err := EncodeValueStr(testVersion.Value)
	require.NoError(t, err)
	require.Equal(t, encodedValue, hlv.Version)
}
