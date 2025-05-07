/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsIndexerError(t *testing.T) {
	var err error
	assert.False(t, isIndexerError(err))
	err = errors.New("MCResponse status=KEY_ENOENT, opcode=0x89, opaque=0")
	assert.False(t, isIndexerError(err))
	err = errors.New("err:[5000]  MCResponse status=KEY_ENOENT, opcode=0x89, opaque=0")
	assert.True(t, isIndexerError(err))
}

func TestShouldUseLegacySyncDocsIndex(t *testing.T) {
	testCases := []struct {
		name                         string
		indexes                      []SGIndexType
		shouldUseLegacySyncDocsIndex bool
	}{
		{
			name:                         "no indexes",
			indexes:                      []SGIndexType{},
			shouldUseLegacySyncDocsIndex: false,
		},
		{
			name:                         "syncDocs only",
			indexes:                      []SGIndexType{IndexSyncDocs},
			shouldUseLegacySyncDocsIndex: true,
		},
		{
			name:                         "user only",
			indexes:                      []SGIndexType{IndexUser},
			shouldUseLegacySyncDocsIndex: false,
		},
		{
			name:                         "role only",
			indexes:                      []SGIndexType{IndexRole},
			shouldUseLegacySyncDocsIndex: false,
		},
		{
			name:                         "user and role",
			indexes:                      []SGIndexType{IndexUser, IndexRole},
			shouldUseLegacySyncDocsIndex: false,
		},
		{
			name:                         "syncDocs and role",
			indexes:                      []SGIndexType{IndexSyncDocs, IndexRole},
			shouldUseLegacySyncDocsIndex: true,
		},
		{
			name:                         "syncDocs and user",
			indexes:                      []SGIndexType{IndexSyncDocs, IndexUser},
			shouldUseLegacySyncDocsIndex: true,
		},
		{
			name:                         "syncDocs, user, role",
			indexes:                      []SGIndexType{IndexSyncDocs, IndexUser, IndexRole},
			shouldUseLegacySyncDocsIndex: false,
		},
		{
			name:                         "syncDocs, user, role, extraenous", // this would be a programming bug to include an extra index
			indexes:                      []SGIndexType{IndexSyncDocs, IndexUser, IndexRole, IndexAccess},
			shouldUseLegacySyncDocsIndex: false,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			shouldUse := shouldUseLegacySyncDocsIndex(testCase.indexes)
			assert.Equal(t, testCase.shouldUseLegacySyncDocsIndex, shouldUse)
		})
	}
}
