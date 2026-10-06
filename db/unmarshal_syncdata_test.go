// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"reflect"
	"strconv"
	"testing"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// feedSyncShape describes the _sync xattr of a synthetic DCP event. The parse cost on the caching feed scales with
// the parts of _sync the cache never reads (history, access grants, channel set history), so the shapes sweep those
// independently of the channel count the cache does need.
type feedSyncShape struct {
	name            string
	channels        int
	history         int
	removedChannels int // channels the doc has been removed from, which stay in the channels map with a removal seq
	accessGrants    int // users (and roles) granted every channel, populating access and role_access
}

var feedSyncShapes = []feedSyncShape{
	{name: "ch=1/hist=1", channels: 1, history: 1},
	{name: "ch=10/hist=1", channels: 10, history: 1},
	{name: "ch=10/hist=10", channels: 10, history: 10},
	{name: "ch=10/hist=50", channels: 10, history: 50},
	{name: "ch=10/hist=100", channels: 10, history: 100},
	{name: "ch=10/hist=20/removed=3/access=5", channels: 10, history: 20, removedChannels: 3, accessGrants: 5},
}

const testFeedVV = `{"ver":"0x0000aeed831bd415","src":"s_LhRPsa7CpjEvP5zeXTXEBA"}`

// buildFeedValue returns a DCP value carrying a body plus _sync and _vv xattrs, as DocChanged receives it.
func buildFeedValue(tb testing.TB, shape feedSyncShape) []byte {
	syncJSON, err := base.JSONMarshal(buildFeedSyncData(shape))
	require.NoError(tb, err)
	return sgbucket.EncodeValueWithXattrs([]byte(`{"some":"body"}`),
		sgbucket.Xattr{Name: base.SyncXattrName, Value: syncJSON},
		sgbucket.Xattr{Name: base.VvXattrName, Value: []byte(testFeedVV)},
	)
}

func buildFeedSyncData(shape feedSyncShape) SyncData {
	const currentSeq = 12345
	chanMap := make(channels.ChannelMap, shape.channels+shape.removedChannels)
	chanSet := make([]ChannelSetEntry, 0, shape.channels+shape.removedChannels)
	activeChannels := base.Set{}
	for i := range shape.channels {
		name := "channel_" + strconv.Itoa(i)
		chanMap[name] = nil
		chanSet = append(chanSet, ChannelSetEntry{Name: name, Start: 1})
		activeChannels[name] = struct{}{}
	}
	for i := range shape.removedChannels {
		name := "removed_" + strconv.Itoa(i)
		removalSeq := uint64(currentSeq - shape.removedChannels + i)
		chanMap[name] = &channels.ChannelRemoval{Seq: removalSeq, Rev: channels.RevAndVersion{RevTreeID: "2-0123456789abcdef0123456789abcdef"}}
		chanSet = append(chanSet, ChannelSetEntry{Name: name, Start: 1, End: removalSeq})
	}

	revTree := make(RevTree, shape.history)
	parent := ""
	var lastRev string
	for i := 1; i <= shape.history; i++ {
		rev := strconv.Itoa(i) + "-0123456789abcdef0123456789abcdef"
		revTree[rev] = &RevInfo{ID: rev, Parent: parent}
		parent = rev
		lastRev = rev
	}
	revTree[lastRev].Channels = activeChannels

	var access, roleAccess UserAccessMap
	if shape.accessGrants > 0 {
		access = make(UserAccessMap, shape.accessGrants)
		roleAccess = make(UserAccessMap, shape.accessGrants)
		for i := range shape.accessGrants {
			access["user_"+strconv.Itoa(i)] = channels.AtSequence(activeChannels, currentSeq)
			roleAccess["user_"+strconv.Itoa(i)] = channels.AtSequence(base.SetOf("role_"+strconv.Itoa(i)), currentSeq)
		}
	}

	recentSequences := []uint64{currentSeq}
	for i := range shape.removedChannels {
		recentSequences = append(recentSequences, chanMap["removed_"+strconv.Itoa(i)].Seq)
	}

	return SyncData{
		Sequence:          currentSeq,
		RevAndVersion:     channels.RevAndVersion{RevTreeID: lastRev, CurrentSource: "s_LhRPsa7CpjEvP5zeXTXEBA", CurrentVersion: "0x0000aeed831bd415"},
		History:           revTree,
		Channels:          chanMap,
		ChannelSet:        chanSet,
		ChannelSetHistory: chanSet,
		Access:            access,
		RoleAccess:        roleAccess,
		TimeSaved:         time.Now(),
		Cas:               "0x0000aeed831bd415",
		Crc32c:            "0x1fe2c8b3",
		RecentSequences:   recentSequences,
	}
}

// BenchmarkUnmarshalSyncDataFromFeed measures the per-event cost of extracting sync metadata from a DCP value, for
// the full parse used by import and attachment migration and for the partial parse used by the caching feed.
func BenchmarkUnmarshalSyncDataFromFeed(b *testing.B) {
	base.DisableTestLogging(b)
	parsers := []struct {
		name  string
		parse func(value []byte) (sequence uint64, err error)
	}{
		{name: "full", parse: func(value []byte) (uint64, error) {
			_, syncData, err := UnmarshalDocumentSyncDataFromFeed(value, base.MemcachedDataTypeXattr, "")
			if err != nil {
				return 0, err
			}
			return syncData.Sequence, nil
		}},
		{name: "cache", parse: func(value []byte) (uint64, error) {
			feedData, err := unmarshalCachingFeedData(value, "")
			if err != nil {
				return 0, err
			}
			return feedData.syncData.Sequence, nil
		}},
	}
	for _, parser := range parsers {
		for _, shape := range feedSyncShapes {
			b.Run(parser.name+"/"+shape.name, func(b *testing.B) {
				value := buildFeedValue(b, shape)
				sequence, err := parser.parse(value)
				require.NoError(b, err)
				require.Equal(b, uint64(12345), sequence)
				b.SetBytes(int64(len(value)))
				b.ReportAllocs()
				for b.Loop() {
					if _, err := parser.parse(value); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// TestCacheFeedSyncDataFieldsMatchSyncData guards cacheFeedSyncData against drifting from SyncData: a renamed tag or
// changed type in SyncData would otherwise leave the caching feed silently reading a zero value.
func TestCacheFeedSyncDataFieldsMatchSyncData(t *testing.T) {
	cacheType := reflect.TypeFor[cacheFeedSyncData]()
	syncDataType := reflect.TypeFor[SyncData]()
	for i := range cacheType.NumField() {
		cacheField := cacheType.Field(i)
		syncDataField, ok := syncDataType.FieldByName(cacheField.Name)
		require.Truef(t, ok, "SyncData has no field %s", cacheField.Name)
		assert.Equalf(t, syncDataField.Type, cacheField.Type, "type of field %s", cacheField.Name)
		assert.Equalf(t, syncDataField.Tag.Get("json"), cacheField.Tag.Get("json"), "json tag of field %s", cacheField.Name)
	}
}

// TestUnmarshalCachingFeedDataMatchesFullParse checks that every field the caching feed reads comes out the
// same as from the full parse, and that the returned xattrs match.
func TestUnmarshalCachingFeedDataMatchesFullParse(t *testing.T) {
	const userXattrKey = "myXattr"
	expiry := time.Now().Add(time.Hour).Truncate(time.Second)

	// Populates every field the cache reads, alongside the fields it skips, so a field cacheFeedSyncData drops is caught.
	rich := buildFeedSyncData(feedSyncShape{channels: 10, history: 20, removedChannels: 3, accessGrants: 5})
	rich.Flags = channels.Deleted | channels.UnchangedCV
	rich.UnusedSequences = []uint64{12343, 12344}
	rich.Crc32cUserXattr = "0x5b7c3f1a"
	rich.NewestRev = "21-0123456789abcdef0123456789abcdef"
	rich.Expiry = &expiry
	rich.TombstonedAt = 1700000000
	rich.ClusterUUID = "6d1c2a7f0e3b4c5d"
	rich.AttachmentsPre4dot0 = AttachmentsMeta{"att1": map[string]any{"digest": "sha1-abc", "length": 3, "revpos": 1, "stub": true}}

	testCases := []struct {
		name     string
		syncData SyncData
	}{{name: "rich", syncData: rich}}
	for _, shape := range feedSyncShapes {
		testCases = append(testCases, struct {
			name     string
			syncData SyncData
		}{name: shape.name, syncData: buildFeedSyncData(shape)})
	}

	cacheType := reflect.TypeFor[cacheFeedSyncData]()
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			syncJSON, err := base.JSONMarshal(tc.syncData)
			require.NoError(t, err)
			value := sgbucket.EncodeValueWithXattrs([]byte(`{"some":"body"}`),
				sgbucket.Xattr{Name: base.SyncXattrName, Value: syncJSON},
				sgbucket.Xattr{Name: base.VvXattrName, Value: []byte(testFeedVV)},
				sgbucket.Xattr{Name: userXattrKey, Value: []byte(`{"some":"user xattr"}`)},
				sgbucket.Xattr{Name: base.MouXattrName, Value: []byte(`{"cas":"0x0000aeed831bd415"}`)},
			)
			dataType := uint8(base.MemcachedDataTypeXattr | base.MemcachedDataTypeJSON)

			fullDoc, fullSyncData, err := UnmarshalDocumentSyncDataFromFeed(value, dataType, userXattrKey)
			require.NoError(t, err)
			require.NotNil(t, fullSyncData)
			feedData, err := unmarshalCachingFeedData(value, userXattrKey)
			require.NoError(t, err)
			require.NotNil(t, feedData)

			fullValue := reflect.ValueOf(fullSyncData).Elem()
			cacheValue := reflect.ValueOf(feedData.syncData)
			for i := range cacheType.NumField() {
				name := cacheType.Field(i).Name
				if tc.name == "rich" {
					require.Falsef(t, fullValue.FieldByName(name).IsZero(), "rich test case must populate %s", name)
				}
				assert.Equalf(t, fullValue.FieldByName(name).Interface(), cacheValue.FieldByName(name).Interface(), "field %s", name)
			}

			assert.Equal(t, fullDoc.Xattrs[base.VvXattrName], []byte(feedData.rawVV))
			assert.Equal(t, fullDoc.Xattrs[userXattrKey], feedData.rawUserXattr)
		})
	}
}

// TestUnmarshalCachingFeedDataNoSyncXattr checks that a document with xattrs but no _sync xattr is not cached.
func TestUnmarshalCachingFeedDataNoSyncXattr(t *testing.T) {
	const userXattrKey = "myXattr"
	value := sgbucket.EncodeValueWithXattrs([]byte(`{"some":"body"}`),
		sgbucket.Xattr{Name: base.VvXattrName, Value: []byte(testFeedVV)},
		sgbucket.Xattr{Name: userXattrKey, Value: []byte(`{"a":"b"}`)},
	)
	feedData, err := unmarshalCachingFeedData(value, userXattrKey)
	require.NoError(t, err)
	require.Nil(t, feedData)
}

// TestUnmarshalDocumentSyncDataFromFeedInlineSync checks that the full parse finds inline _sync, which import relies on
// to migrate pre-xattr documents.
func TestUnmarshalDocumentSyncDataFromFeedInlineSync(t *testing.T) {
	const userXattrKey = "myXattr"
	inlineSyncBody := []byte(`{"_sync":{"rev":"1-abc","sequence":100,"channels":{"ABC":null}},"some":"body"}`)
	value := sgbucket.EncodeValueWithXattrs(inlineSyncBody, sgbucket.Xattr{Name: userXattrKey, Value: []byte(`{"a":"b"}`)})
	_, syncData, err := UnmarshalDocumentSyncDataFromFeed(value, base.MemcachedDataTypeXattr|base.MemcachedDataTypeJSON, userXattrKey)
	require.NoError(t, err)
	require.NotNil(t, syncData)
	require.Equal(t, uint64(100), syncData.Sequence)
}

func TestUnmarshalCachingFeedDataErrors(t *testing.T) {
	t.Run("truncated xattrs", func(t *testing.T) {
		_, err := unmarshalCachingFeedData([]byte{0, 0}, "")
		require.ErrorIs(t, err, sgbucket.ErrEmptyMetadata)
	})
	t.Run("malformed _sync xattr", func(t *testing.T) {
		value := sgbucket.EncodeValueWithXattrs([]byte(`{}`), sgbucket.Xattr{Name: base.SyncXattrName, Value: []byte(`{"sequence":"not a number"}`)})
		feedData, err := unmarshalCachingFeedData(value, "")
		require.Error(t, err)
		require.Nil(t, feedData)
	})
}
