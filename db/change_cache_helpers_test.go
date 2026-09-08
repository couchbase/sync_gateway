//  Copyright 2015-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

// The helpers below delegate to the exported implementations in util_testing.go. They exist so
// the tests remaining in package db keep their original call sites; they were relocated here
// from change_cache_test.go when the change cache tests moved to db/changecachetest. Prefer the
// exported names in new code.

func testLogEntry(seq uint64, docid string, revid string) *LogEntry {
	return MakeTestLogEntry(seq, docid, revid)
}

func testLogEntryForChannels(seq int, channelNames []string) *LogEntry {
	return MakeTestLogEntryForChannels(seq, channelNames)
}

func logEntry(seq uint64, docid string, revid string, channelNames []string, collectionID uint32) *LogEntry {
	return MakeLogEntry(seq, docid, revid, channelNames, collectionID)
}

func testLogEntryWithCV(seq uint64, docid string, revid string, channelNames []string, collectionID uint32, sourceID string, version uint64) *LogEntry {
	return MakeTestLogEntryWithCV(seq, docid, revid, channelNames, collectionID, sourceID, version)
}

// et returns a tombstoned entry.
func et(seq uint64, docid string, revid string) *LogEntry {
	return MakeDeletedTestLogEntry(seq, docid, revid)
}

func shortWaitCache() CacheOptions {
	return ShortWaitCache()
}

func getChanges(t *testing.T, collection *DatabaseCollectionWithUser, channels base.Set, options ChangesOptions) []*ChangeEntry {
	return GetChangesForTest(t, collection, channels, options)
}
