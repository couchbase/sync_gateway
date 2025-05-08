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

func TestIsSGIndex(t *testing.T) {
	validSGIndexes := []string{
		"sg_access_foo", // this will get marked as a sync gateway index
		"sg_access_x1",
		"sg_access_x2",
		"sg_channels_1",
		"sg_roleAccess_x1",
		"sg_syncDocs_x1",
		"sg_syncDocs_x1_p1",
		"sg_syncDocs_x2",
		"sg_tombstones_x1",
	}
	for _, indexName := range validSGIndexes {
		t.Run(indexName, func(t *testing.T) {
			assert.True(t, isSGIndex(indexName), "expected %s to be a sync gateway index", indexName)
		})
	}
	invalidSGIndexes := []string{
		"sg_sync_docs_1",
		"sg_sync_docs",
		"some_index",
		"_sg_access_x1",
	}
	for _, indexName := range invalidSGIndexes {
		t.Run(indexName, func(t *testing.T) {
			assert.False(t, isSGIndex(indexName), "expected %s to not be a sync gateway index", indexName)
		})
	}
}
