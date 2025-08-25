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
	"strconv"
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
// a different, non-SGW HLV-aware peer)
func (h *HLVAgent) InsertWithHLV(ctx context.Context, key string) (casOut uint64) {
	hlv := &HybridLogicalVector{}
	err := hlv.AddVersion(CreateVersion(h.Source, expandMacroCASValueUint64))
	require.NoError(h.t, err)
	hlv.CurrentVersionCAS = expandMacroCASValueUint64

	vvDataBytes := base.MustJSONMarshal(h.t, hlv)
	mutateInOpts := &sgbucket.MutateInOptions{
		MacroExpansion: hlv.computeMacroExpansions(),
	}

	docBody := base.MustJSONMarshal(h.t, defaultHelperBody)
	xattrData := map[string][]byte{
		h.xattrName: vvDataBytes,
	}

	cas, err := h.datastore.WriteWithXattrs(ctx, key, 0, 0, docBody, xattrData, nil, mutateInOpts)
	require.NoError(h.t, err)
	return cas
}

// UpdateWithHLV will update and existing doc in bucket mocking write from another hlv aware peer
func (h *HLVAgent) UpdateWithHLV(ctx context.Context, key string, inputCas uint64, hlv *HybridLogicalVector) (casOut uint64) {
	err := hlv.AddVersion(CreateVersion(h.Source, expandMacroCASValueUint64))
	require.NoError(h.t, err)
	hlv.CurrentVersionCAS = expandMacroCASValueUint64

	vvXattr, err := hlv.MarshalJSON()
	require.NoError(h.t, err)
	mutateInOpts := &sgbucket.MutateInOptions{
		MacroExpansion: hlv.computeMacroExpansions(),
	}

	docBody := base.MustJSONMarshal(h.t, defaultHelperBody)
	xattrData := map[string][]byte{
		h.xattrName: vvXattr,
	}
	cas, err := h.datastore.WriteWithXattrs(ctx, key, 0, inputCas, docBody, xattrData, nil, mutateInOpts)
	require.NoError(h.t, err)
	return cas
}

// EncodeTestVersion converts a simplified string version of the form 1@abc to a hex-encoded version and base64 encoded
// source, like 169a05acd705ffc0@YWJj.  Allows use of simplified versions in tests for readability, ease of use.
func EncodeTestVersion(versionString string) (encodedString string) {
	timestampString, source, found := strings.Cut(versionString, "@")
	if !found {
		return versionString
	}
	if len(timestampString) > 0 && timestampString[0] == ' ' {
		timestampString = timestampString[1:]
	}
	timestampUint, err := strconv.ParseUint(timestampString, 10, 64)
	if err != nil {
		return ""
	}
	hexTimestamp := strconv.FormatUint(timestampUint, 16)
	base64Source := EncodeSource(source)
	return hexTimestamp + "@" + base64Source
}

// GetHelperBody returns the body contents of a document written by HLVAgent.
func (h *HLVAgent) GetHelperBody() string {
	return string(base.MustJSONMarshal(h.t, defaultHelperBody))
}

// SourceID returns the encoded source ID for the HLVAgent
func (h *HLVAgent) SourceID() string {
	return h.Source
}

// EncodeTestHistory converts a simplified version history of the form "1@abc,2@def;3@ghi" to use hex-encoded versions and
// base64 encoded sources
func EncodeTestHistory(historyString string) (encodedString string) {
	// possible versionSets:
	// 	mv,mv;pv,pv
	// 	mv,mv;
	// 	pv,pv
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
func ParseTestHistory(t *testing.T, historyString string) (pv HLVVersions, mv HLVVersions) {
	versionSets := strings.Split(historyString, ";")

	pv = make(HLVVersions)
	mv = make(HLVVersions)

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
		pv[EncodeSource(version.SourceID)] = version.Value
	}

	// mv
	if mvString != "" {
		for _, versionStr := range strings.Split(mvString, ",") {
			version, err := ParseVersion(versionStr)
			require.NoError(t, err)
			mv[EncodeSource(version.SourceID)] = version.Value
		}
	}
	return pv, mv
}

// RequireCVEqual fails tests if provided HLV does not have expected CV (sent in blip wire format)
func RequireCVEqual(t *testing.T, hlv *HybridLogicalVector, expectedCV string) {
	testVersion, err := ParseVersion(expectedCV)
	require.NoError(t, err)
	require.Equal(t, EncodeSource(testVersion.SourceID), hlv.SourceID)
	require.Equal(t, testVersion.Value, hlv.Version)
}

// hlvAsBlipString returns the full HLV as a string in the format seen over Blip
func hlvAsBlipString(_ testing.TB, hlv *HybridLogicalVector) string {
	s := &strings.Builder{}
	s.WriteString(hlv.GetCurrentVersionString())
	history := hlv.toHistoryForHLV(HLVVersions.sorted)
	if history != "" {
		if len(hlv.MergeVersions) > 0 {
			fmt.Fprintf(s, ",")
		} else if len(hlv.PreviousVersions) > 0 {
			fmt.Fprintf(s, ";")
		}
		s.WriteString(history)
	}
	return s.String()
}
