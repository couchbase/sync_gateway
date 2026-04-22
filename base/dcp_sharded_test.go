// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/cbgt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexName(t *testing.T) {
	tests := []struct {
		dbName    string
		feedType  ShardedDCPFeedType
		indexName string
		wantErr   bool
	}{
		{
			dbName:    "",
			feedType:  ShardedDCPFeedTypeImport,
			indexName: "db0x0_index",
		},
		{
			dbName:    "foo",
			feedType:  ShardedDCPFeedTypeImport,
			indexName: "db0xcfc4ae1d_index",
		},
		{
			dbName:    "",
			feedType:  ShardedDCPFeedTypeResync,
			indexName: "db0x0_resync_index",
		},
		{
			dbName:    "foo",
			feedType:  ShardedDCPFeedTypeResync,
			indexName: "db0xcfc4ae1d_resync_index",
		},
		{
			dbName:   "foo",
			feedType: "unknown-feed-type",
			wantErr:  true,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("dbName %q feedType %s", test.dbName, test.feedType), func(t *testing.T) {
			indexName, err := GenerateCBGTIndexName(test.dbName, test.feedType)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			require.Equal(t, test.indexName, indexName)
		})
	}
}

func TestDestKey(t *testing.T) {
	tests := []struct {
		name        string
		dbName      string
		scopeName   string
		collections []string
		key         string
		feedType    ShardedDCPFeedType
	}{
		{
			name:     "import no scope or collections",
			dbName:   "foo",
			key:      "foo_import",
			feedType: ShardedDCPFeedTypeImport,
		},
		{
			name:     "resync no scope or collections",
			dbName:   "foo",
			key:      "foo_resync_e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			feedType: ShardedDCPFeedTypeResync,
		},
		{
			name:      "import default scope without collections",
			dbName:    "foo",
			scopeName: DefaultScope,
			key:       "foo_import",
			feedType:  ShardedDCPFeedTypeImport,
		},
		{
			name:      "resync default scope without collections",
			dbName:    "foo",
			scopeName: DefaultScope,
			key:       "foo_resync_e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
			feedType:  ShardedDCPFeedTypeResync,
		},
		{
			name:        "import custom collection in default scope",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{"bar"},
			key:         "foo_import_02e3c10f452b5d9d5051ae25270ae5714471774097ca7e00424b52bf63de1f6d",
			feedType:    ShardedDCPFeedTypeImport,
		},
		{
			name:        "resync custom collection in default scope",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{"bar"},
			key:         "foo_resync_02e3c10f452b5d9d5051ae25270ae5714471774097ca7e00424b52bf63de1f6d",
			feedType:    ShardedDCPFeedTypeResync,
		},
		{
			name:        "import custom collection in custom scope",
			dbName:      "foo",
			scopeName:   "baz",
			collections: []string{"bar"},
			key:         "foo_import_3a4b66f3c8aa40608000c82c417f201de305a1994f3048b7734a33205be5e410",
			feedType:    ShardedDCPFeedTypeImport,
		},
		{
			name:        "resync custom collection in custom scope",
			dbName:      "foo",
			scopeName:   "baz",
			collections: []string{"bar"},
			key:         "foo_resync_3a4b66f3c8aa40608000c82c417f201de305a1994f3048b7734a33205be5e410",
			feedType:    ShardedDCPFeedTypeResync,
		},
		{
			name:        "import multiple collections in custom scope",
			dbName:      "foo",
			scopeName:   "bar",
			collections: []string{"baz", "bat"},
			key:         "foo_import_cc2777dc506c83ef70c0630be2f21cbe9380d83d2d50c8aeb428e67691503cfb",
			feedType:    ShardedDCPFeedTypeImport,
		},
		{
			name:        "resync multiple collections in custom scope",
			dbName:      "foo",
			scopeName:   "bar",
			collections: []string{"baz", "bat"},
			key:         "foo_resync_cc2777dc506c83ef70c0630be2f21cbe9380d83d2d50c8aeb428e67691503cfb",
			feedType:    ShardedDCPFeedTypeResync,
		},
		{
			name:        "import multiple collections across scope",
			dbName:      "foo",
			scopeName:   "bar",
			collections: []string{"baz", "bat"},
			key:         "foo_import_cc2777dc506c83ef70c0630be2f21cbe9380d83d2d50c8aeb428e67691503cfb",
			feedType:    ShardedDCPFeedTypeImport,
		},
		{
			name:        "resync multiple collections across scope",
			dbName:      "foo",
			scopeName:   "bar",
			collections: []string{"baz", "bat"},
			key:         "foo_resync_cc2777dc506c83ef70c0630be2f21cbe9380d83d2d50c8aeb428e67691503cfb",
			feedType:    ShardedDCPFeedTypeResync,
		},
		{
			name:        "import default scope with multiple collections",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{"baz", "bat"},
			key:         "foo_import_98ea225323328e1d6ae54575908419f85dcad91b2ee3acb56b3a6491145d87cf",
			feedType:    ShardedDCPFeedTypeImport,
		},
		{
			name:        "resync default scope with multiple collections",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{"baz", "bat"},
			key:         "foo_resync_98ea225323328e1d6ae54575908419f85dcad91b2ee3acb56b3a6491145d87cf",
			feedType:    ShardedDCPFeedTypeResync,
		},
		{
			name:        "import default scope with default collection",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{DefaultCollection},
			key:         "foo_import",
			feedType:    ShardedDCPFeedTypeImport,
		},
		{
			name:        "resync default scope with default collection",
			dbName:      "foo",
			scopeName:   DefaultScope,
			collections: []string{DefaultCollection},
			key:         "foo_resync_03d1187922d96d534d985a0a386ecdf062d369673981c217d07610a7b8ca4a52",
			feedType:    ShardedDCPFeedTypeResync,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.key, DestKey(test.dbName, test.scopeName, test.collections, test.feedType))
		})
	}
}

func TestCfgNodePoller_Register(t *testing.T) {
	bucket := GetTestBucket(t)
	defer bucket.Close(t.Context())
	datastore := bucket.GetMetadataStore()
	ctx := t.Context()

	tests := []struct {
		name            string
		keysToCreate    []string        // keys to create in datastore before test
		keysToRegister  []string        // keys to register with poller
		expectedCasZero map[string]bool // keys expected to have cas=0 after registration
	}{
		{
			name:            "register_non_existent_key",
			keysToCreate:    []string{},
			keysToRegister:  []string{"non_existent_key"},
			expectedCasZero: map[string]bool{"non_existent_key": true},
		},
		{
			name:            "register_existing_key",
			keysToCreate:    []string{"existing_key"},
			keysToRegister:  []string{"existing_key"},
			expectedCasZero: map[string]bool{},
		},
		{
			name:            "register_same_key_twice",
			keysToCreate:    []string{"duplicate_key"},
			keysToRegister:  []string{"duplicate_key", "duplicate_key"},
			expectedCasZero: map[string]bool{},
		},
		{
			name:            "register_multiple_keys_mixed",
			keysToCreate:    []string{"exists_1", "exists_2"},
			keysToRegister:  []string{"exists_1", "not_exists", "exists_2"},
			expectedCasZero: map[string]bool{"not_exists": true},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Create a new poller for each test (without starting polling)
			poller := &cfgNodePoller{
				ctx:        ctx,
				keyWatcher: make(map[string]uint64),
				datastore:  datastore,
				fireEvent:  func(docID string, cas uint64, err error) {},
			}

			// Common setup: create documents
			createdCas := make(map[string]uint64)
			for _, key := range test.keysToCreate {
				added, err := datastore.Add(key, 0, []byte(`{"test": true}`))
				require.NoError(t, err)
				require.True(t, added)
				cas, err := datastore.Get(key, nil)
				require.NoError(t, err)
				createdCas[key] = cas
			}

			// Cleanup created docs after test
			t.Cleanup(func() {
				for _, key := range test.keysToCreate {
					_ = datastore.Delete(key)
				}
			})

			// Register all keys
			for _, key := range test.keysToRegister {
				err := poller.Register(key)
				require.NoError(t, err)
			}

			// Build unique set of registered keys for length check
			uniqueKeys := make(map[string]bool)
			for _, key := range test.keysToRegister {
				uniqueKeys[key] = true
			}

			// Verify all registered keys
			poller.lock.Lock()
			defer poller.lock.Unlock()

			require.Len(t, poller.keyWatcher, len(uniqueKeys), "keyWatcher should have all unique registered keys")

			for key := range uniqueKeys {
				cas, exists := poller.keyWatcher[key]
				require.True(t, exists, "key %s should be registered", key)

				if test.expectedCasZero[key] {
					require.Equal(t, uint64(0), cas, "key %s should have cas=0", key)
				} else {
					require.NotEqual(t, uint64(0), cas, "key %s should have non-zero cas", key)
					require.Equal(t, createdCas[key], cas, "key %s CAS should match datastore", key)
				}
			}
		})
	}

	// Verify re-registration picks up new CAS after document update
	t.Run("register_key_after_document_update", func(t *testing.T) {
		poller := &cfgNodePoller{
			ctx:        ctx,
			keyWatcher: make(map[string]uint64),
			datastore:  datastore,
			fireEvent:  func(docID string, cas uint64, err error) {},
		}

		key := "update_then_register"

		// Create initial document
		added, err := datastore.Add(key, 0, []byte(`{"test": true}`))
		require.NoError(t, err)
		require.True(t, added)

		// Cleanup created doc after test
		t.Cleanup(func() {
			_ = datastore.Delete(key)
		})

		// Register first time
		err = poller.Register(key)
		require.NoError(t, err)

		poller.lock.Lock()
		initialCas := poller.keyWatcher[key]
		poller.lock.Unlock()
		require.NotEqual(t, uint64(0), initialCas)

		// Update document externally
		err = datastore.Set(key, 0, nil, []byte(`{"updated": true}`))
		require.NoError(t, err)

		// Get new CAS from datastore
		newExpectedCas, err := datastore.Get(key, nil)
		require.NoError(t, err)
		require.NotEqual(t, initialCas, newExpectedCas, "document should have new CAS after update")

		// Register again - should pick up new CAS
		err = poller.Register(key)
		require.NoError(t, err)

		poller.lock.Lock()
		updatedCas := poller.keyWatcher[key]
		poller.lock.Unlock()

		require.Equal(t, newExpectedCas, updatedCas, "keyWatcher should have updated CAS after re-registration")
	})
}

func TestCfgNodePoller_Poll(t *testing.T) {
	bucket := GetTestBucket(t)
	defer bucket.Close(t.Context())
	datastore := bucket.GetMetadataStore()
	ctx := t.Context()

	tests := []struct {
		name              string
		keysToCreate      []string        // keys to create in datastore before registration
		keysToRegister    []string        // keys to register with poller
		keysToUpdate      []string        // keys to update after registration (before poll)
		keysToDelete      []string        // keys to delete after registration (before poll)
		keysToPurge       []string        // keys to purge after registration (before poll)
		expectedEventKeys map[string]bool // keys expected to trigger events during poll
		expectedCasZero   map[string]bool // keys expected to have cas=0 after poll
	}{
		{
			name:              "poll_detects_document_creation",
			keysToCreate:      []string{},
			keysToRegister:    []string{"created_doc"},
			keysToUpdate:      []string{"created_doc"}, // creating a doc that didn't exist
			keysToDelete:      []string{},
			keysToPurge:       []string{},
			expectedEventKeys: map[string]bool{"created_doc": true},
			expectedCasZero:   map[string]bool{},
		},
		{
			name:              "poll_detects_document_update",
			keysToCreate:      []string{"updated_doc"},
			keysToRegister:    []string{"updated_doc"},
			keysToUpdate:      []string{"updated_doc"},
			keysToDelete:      []string{},
			keysToPurge:       []string{},
			expectedEventKeys: map[string]bool{"updated_doc": true},
			expectedCasZero:   map[string]bool{},
		},
		{
			name:              "poll_no_event_when_cas_unchanged",
			keysToCreate:      []string{"unchanged_doc"},
			keysToRegister:    []string{"unchanged_doc"},
			keysToUpdate:      []string{},
			keysToDelete:      []string{},
			keysToPurge:       []string{},
			expectedEventKeys: map[string]bool{},
			expectedCasZero:   map[string]bool{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Track fired events
			firedEvents := make(map[string]uint64)
			var eventLock sync.Mutex

			// Create a new poller for each test (without starting polling)
			poller := &cfgNodePoller{
				ctx:        ctx,
				keyWatcher: make(map[string]uint64),
				datastore:  datastore,
				fireEvent: func(docID string, cas uint64, err error) {
					eventLock.Lock()
					firedEvents[docID] = cas
					eventLock.Unlock()
				},
			}

			// Setup: create initial documents
			for _, key := range test.keysToCreate {
				added, err := datastore.Add(key, 0, []byte(`{"test": true}`))
				require.NoError(t, err)
				require.True(t, added)
			}

			// Cleanup all potentially created docs after test
			t.Cleanup(func() {
				for _, key := range test.keysToCreate {
					_ = datastore.Delete(key)
				}
				for _, key := range test.keysToUpdate {
					_ = datastore.Delete(key)
				}
			})

			// Register all keys
			for _, key := range test.keysToRegister {
				err := poller.Register(key)
				require.NoError(t, err)
			}

			// Update documents (creates if doesn't exist)
			for _, key := range test.keysToUpdate {
				err := datastore.Set(key, 0, nil, []byte(`{"updated": true}`))
				require.NoError(t, err)
			}

			// Delete documents (creates tombstone)
			for _, key := range test.keysToDelete {
				err := datastore.Delete(key)
				require.NoError(t, err)
			}

			// Purge documents (complete removal - no tombstone)
			for _, key := range test.keysToPurge {
				cas, err := datastore.Get(key, nil)
				require.NoError(t, err)
				_, err = datastore.Remove(key, cas)
				require.NoError(t, err)
			}

			// Call poll
			poller.poll(t.Context())

			// Verify fired events
			eventLock.Lock()
			require.Len(t, firedEvents, len(test.expectedEventKeys), "unexpected number of events fired")
			for key := range test.expectedEventKeys {
				_, fired := firedEvents[key]
				require.True(t, fired, "expected event for key %s was not fired", key)
			}
			eventLock.Unlock()

			// Verify keyWatcher CAS values were updated
			poller.lock.Lock()
			defer poller.lock.Unlock()

			for key := range test.expectedEventKeys {
				if test.expectedCasZero[key] {
					require.Equal(t, uint64(0), poller.keyWatcher[key], "key %s should have cas=0 after poll", key)
				} else {
					require.NotEqual(t, uint64(0), poller.keyWatcher[key], "key %s should have non-zero cas after poll", key)
					// Verify CAS matches current datastore value
					currentCas, err := datastore.Get(key, nil)
					require.NoError(t, err)
					require.Equal(t, currentCas, poller.keyWatcher[key], "key %s CAS should match datastore", key)
				}
			}
		})
	}

	// Verify fireEvent callback receives correct docID, CAS, and nil error
	t.Run("poll_fireevent_receives_correct_parameters", func(t *testing.T) {
		var receivedDocID string
		var receivedCas uint64
		var receivedErr error
		eventCalled := false

		poller := &cfgNodePoller{
			ctx:        ctx,
			keyWatcher: make(map[string]uint64),
			datastore:  datastore,
			fireEvent: func(docID string, cas uint64, err error) {
				receivedDocID = docID
				receivedCas = cas
				receivedErr = err
				eventCalled = true
			},
		}

		key := "event_params_test"

		// Register key (doesn't exist yet, cas=0)
		err := poller.Register(key)
		require.NoError(t, err)

		// Create document
		added, err := datastore.Add(key, 0, []byte(`{"test": true}`))
		require.NoError(t, err)
		require.True(t, added)

		t.Cleanup(func() {
			_ = datastore.Delete(key)
		})

		// Get expected CAS
		expectedCas, err := datastore.Get(key, nil)
		require.NoError(t, err)

		// Call Poll
		poller.poll(t.Context())

		// Verify event parameters
		require.True(t, eventCalled, "fireEvent should have been called")
		require.Equal(t, key, receivedDocID, "docID should match")
		require.Equal(t, expectedCas, receivedCas, "cas should match datastore CAS")
		require.Nil(t, receivedErr, "err should be nil")
	})

	// Verify keyWatcher update prevents duplicate events on subsequent polls
	t.Run("poll_updates_keywatcher_prevents_duplicate_events", func(t *testing.T) {
		eventCount := 0

		poller := &cfgNodePoller{
			ctx:        ctx,
			keyWatcher: make(map[string]uint64),
			datastore:  datastore,
			fireEvent: func(docID string, cas uint64, err error) {
				eventCount++
			},
		}

		key := "duplicate_event_test"

		// Create document
		added, err := datastore.Add(key, 0, []byte(`{"test": true}`))
		require.NoError(t, err)
		require.True(t, added)

		t.Cleanup(func() {
			_ = datastore.Delete(key)
		})

		// Register key
		err = poller.Register(key)
		require.NoError(t, err)

		// Update document
		err = datastore.Set(key, 0, nil, []byte(`{"updated": true}`))
		require.NoError(t, err)

		// First poll - should fire event
		poller.poll(t.Context())
		require.Equal(t, 1, eventCount, "first poll should fire one event")

		// Second poll without changes - should NOT fire event
		poller.poll(t.Context())
		require.Equal(t, 1, eventCount, "second poll should not fire event since CAS unchanged")

		// Third poll without changes - should still NOT fire event
		poller.poll(t.Context())
		require.Equal(t, 1, eventCount, "third poll should not fire event since CAS unchanged")
	})

	// Verify deletion (tombstone) triggers event with cas=0 - Couchbase Server only
	t.Run("poll_detects_document_deletion", func(t *testing.T) {
		if UnitTestUrlIsWalrus() {
			t.Skip("This test requires Couchbase Server - deletion behavior differs in Rosmar")
		}

		eventFired := false
		var firedCas uint64

		poller := &cfgNodePoller{
			ctx:        ctx,
			keyWatcher: make(map[string]uint64),
			datastore:  datastore,
			fireEvent: func(docID string, cas uint64, err error) {
				eventFired = true
				firedCas = cas
			},
		}

		key := "deleted_doc_test"

		// Create document
		added, err := datastore.Add(key, 0, []byte(`{"test": true}`))
		require.NoError(t, err)
		require.True(t, added)

		t.Cleanup(func() {
			_ = datastore.Delete(key)
		})

		// Register key
		err = poller.Register(key)
		require.NoError(t, err)

		// Delete document (creates tombstone)
		err = datastore.Delete(key)
		require.NoError(t, err)

		// Call Poll
		poller.poll(t.Context())

		// Verify event was fired
		require.True(t, eventFired, "event should be fired for deleted document")
		require.Equal(t, uint64(0), firedCas, "cas should be 0 for deleted document")

		// Verify keyWatcher was updated
		poller.lock.Lock()
		require.Equal(t, uint64(0), poller.keyWatcher[key], "keyWatcher should have cas=0 after deletion")
		poller.lock.Unlock()
	})

	// Verify purge (complete removal) triggers event with cas=0 - Couchbase Server only
	t.Run("poll_detects_document_purge", func(t *testing.T) {
		if UnitTestUrlIsWalrus() {
			t.Skip("This test requires Couchbase Server - purge behavior differs in Rosmar")
		}

		eventFired := false
		var firedCas uint64

		poller := &cfgNodePoller{
			ctx:        ctx,
			keyWatcher: make(map[string]uint64),
			datastore:  datastore,
			fireEvent: func(docID string, cas uint64, err error) {
				eventFired = true
				firedCas = cas
			},
		}

		key := "purged_doc_test"

		// Create document
		added, err := datastore.Add(key, 0, []byte(`{"test": true}`))
		require.NoError(t, err)
		require.True(t, added)

		t.Cleanup(func() {
			_ = datastore.Delete(key)
		})

		err = poller.Register(key)
		require.NoError(t, err)

		// Get current CAS before purge
		cas, err := datastore.Get(key, nil)
		require.NoError(t, err)

		// Purge document (complete removal - no tombstone)
		_, err = datastore.Remove(key, cas)
		require.NoError(t, err)

		poller.poll(t.Context())

		// Verify event was fired
		require.True(t, eventFired, "event should be fired for purged document")
		require.Equal(t, uint64(0), firedCas, "cas should be 0 for purged document")

		// Verify keyWatcher was updated
		poller.lock.Lock()
		require.Equal(t, uint64(0), poller.keyWatcher[key], "keyWatcher should have cas=0 after purge")
		poller.lock.Unlock()
	})

	// Verify poll handles multiple document changes in a single poll cycle (race condition test)
	t.Run("poll_detects_multiple_document_changes", func(t *testing.T) {
		firedEvents := make(map[string]uint64)
		var eventLock sync.Mutex

		poller := &cfgNodePoller{
			ctx:        ctx,
			keyWatcher: make(map[string]uint64),
			datastore:  datastore,
			fireEvent: func(docID string, cas uint64, err error) {
				eventLock.Lock()
				firedEvents[docID] = cas
				eventLock.Unlock()
			},
		}

		key1 := "race_condition_doc1"
		key2 := "race_condition_doc2"

		// Create both documents
		added, err := datastore.Add(key1, 0, []byte(`{"test": true}`))
		require.NoError(t, err)
		require.True(t, added)

		added, err = datastore.Add(key2, 0, []byte(`{"test": true}`))
		require.NoError(t, err)
		require.True(t, added)

		t.Cleanup(func() {
			_ = datastore.Delete(key1)
			_ = datastore.Delete(key2)
		})

		// Register both keys
		err = poller.Register(key1)
		require.NoError(t, err)

		err = poller.Register(key2)
		require.NoError(t, err)

		// Change first document
		err = datastore.Set(key1, 0, nil, []byte(`{"updated": true}`))
		require.NoError(t, err)

		// Change second document
		err = datastore.Set(key2, 0, nil, []byte(`{"updated": true}`))
		require.NoError(t, err)

		// Run poll - should detect both changes
		poller.poll(t.Context())

		// Verify both events were fired
		eventLock.Lock()
		require.Len(t, firedEvents, 2, "both documents should trigger events")
		_, fired1 := firedEvents[key1]
		require.True(t, fired1, "event should be fired for first document")
		_, fired2 := firedEvents[key2]
		require.True(t, fired2, "event should be fired for second document")
		eventLock.Unlock()

		// Verify both keyWatcher CAS values were updated
		poller.lock.Lock()
		defer poller.lock.Unlock()

		require.NotEqual(t, uint64(0), poller.keyWatcher[key1], "first document should have non-zero cas after poll")
		require.NotEqual(t, uint64(0), poller.keyWatcher[key2], "second document should have non-zero cas after poll")

		// Verify CAS matches current datastore values
		currentCas1, err := datastore.Get(key1, nil)
		require.NoError(t, err)
		require.Equal(t, currentCas1, poller.keyWatcher[key1], "first document CAS should match datastore")

		currentCas2, err := datastore.Get(key2, nil)
		require.NoError(t, err)
		require.Equal(t, currentCas2, poller.keyWatcher[key2], "second document CAS should match datastore")
	})

}

func TestCfgNodePoller_StartPolling(t *testing.T) {
	bucket := GetTestBucket(t)
	defer bucket.Close(t.Context())
	datastore := bucket.GetMetadataStore()

	// Polling should stop when context is cancelled
	t.Run("start_polling_stops_on_context_cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())

		var eventCount atomic.Int32
		poller := &cfgNodePoller{
			ctx:          ctx,
			keyWatcher:   make(map[string]uint64),
			datastore:    datastore,
			fireEvent:    func(docID string, cas uint64, err error) { eventCount.Add(1) },
			pollInterval: 5 * time.Millisecond,
		}

		key := "stop_on_cancel_test"

		added, err := datastore.Add(key, 0, []byte(`{"test": true}`))
		require.NoError(t, err)
		require.True(t, added)

		t.Cleanup(func() {
			_ = datastore.Delete(key)
		})

		err = poller.Register(key)
		require.NoError(t, err)

		poller.startPolling(ctx)

		// Wait for at least one poll cycle
		time.Sleep(20 * time.Millisecond)

		// Cancel context to stop polling
		cancel(errors.New("test: stopping polling"))

		// Wait for goroutine to stop
		time.Sleep(20 * time.Millisecond)

		// Update document after cancellation
		err = datastore.Set(key, 0, nil, []byte(`{"updated": true}`))
		require.NoError(t, err)

		// Record event count before waiting
		countBeforeWait := eventCount.Load()

		// Wait to ensure no more polls happen
		time.Sleep(30 * time.Millisecond)

		// Event count should not have increased after cancellation
		countAfterWait := eventCount.Load()
		require.Equal(t, countBeforeWait, countAfterWait, "no events should fire after context cancellation")
	})

	// Changes should be detected within the poll interval
	t.Run("start_polling_polls_at_interval", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)

		var eventCount atomic.Int32
		poller := &cfgNodePoller{
			ctx:          ctx,
			keyWatcher:   make(map[string]uint64),
			datastore:    datastore,
			fireEvent:    func(docID string, cas uint64, err error) { eventCount.Add(1) },
			pollInterval: 10 * time.Millisecond,
		}

		key := "poll_interval_test"

		err := poller.Register(key)
		require.NoError(t, err)

		poller.startPolling(ctx)

		// Create document - should be detected on next poll
		added, err := datastore.Add(key, 0, []byte(`{"test": true}`))
		require.NoError(t, err)
		require.True(t, added)

		t.Cleanup(func() {
			_ = datastore.Delete(key)
		})

		// Wait for poll to detect the change
		require.Eventually(t, func() bool {
			return eventCount.Load() >= 1
		}, 100*time.Millisecond, 5*time.Millisecond, "event should be fired within poll interval")
	})

	// Multiple sequential changes should all be detected over time
	t.Run("start_polling_detects_changes_over_time", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)

		var eventCount atomic.Int32
		poller := &cfgNodePoller{
			ctx:          ctx,
			keyWatcher:   make(map[string]uint64),
			datastore:    datastore,
			fireEvent:    func(docID string, cas uint64, err error) { eventCount.Add(1) },
			pollInterval: 10 * time.Millisecond,
		}

		key := "changes_over_time_test"

		added, err := datastore.Add(key, 0, []byte(`{"version": 1}`))
		require.NoError(t, err)
		require.True(t, added)

		t.Cleanup(func() {
			_ = datastore.Delete(key)
		})

		err = poller.Register(key)
		require.NoError(t, err)

		poller.startPolling(ctx)

		// First update
		err = datastore.Set(key, 0, nil, []byte(`{"version": 2}`))
		require.NoError(t, err)

		// Wait for first event
		require.Eventually(t, func() bool {
			return eventCount.Load() >= 1
		}, 100*time.Millisecond, 5*time.Millisecond, "first update should be detected")

		// Second update
		err = datastore.Set(key, 0, nil, []byte(`{"version": 3}`))
		require.NoError(t, err)

		// Wait for second event
		require.Eventually(t, func() bool {
			return eventCount.Load() >= 2
		}, 100*time.Millisecond, 5*time.Millisecond, "second update should be detected")

		// Third update
		err = datastore.Set(key, 0, nil, []byte(`{"version": 4}`))
		require.NoError(t, err)

		// Wait for third event
		require.Eventually(t, func() bool {
			return eventCount.Load() >= 3
		}, 100*time.Millisecond, 5*time.Millisecond, "third update should be detected")
	})

	// No polling should occur after context is cancelled
	t.Run("start_polling_no_poll_after_context_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())

		var eventCount atomic.Int32
		poller := &cfgNodePoller{
			ctx:          ctx,
			keyWatcher:   make(map[string]uint64),
			datastore:    datastore,
			fireEvent:    func(docID string, cas uint64, err error) { eventCount.Add(1) },
			pollInterval: 10 * time.Millisecond,
		}

		key := "no_poll_after_cancel_test"

		err := poller.Register(key)
		require.NoError(t, err)

		poller.startPolling(ctx)
		cancel(errors.New("test: stopping polling"))

		// Wait for goroutine to stop
		time.Sleep(20 * time.Millisecond)

		// Now create the document
		added, err := datastore.Add(key, 0, []byte(`{"test": true}`))
		require.NoError(t, err)
		require.True(t, added)

		t.Cleanup(func() {
			_ = datastore.Delete(key)
		})

		// Wait and verify no events were fired
		time.Sleep(50 * time.Millisecond)
		require.Equal(t, int32(0), eventCount.Load(), "no events should fire after context cancellation")
	})
}

func TestCfgNodePollerDistributed(t *testing.T) {

	ctx := t.Context()
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)
	dataStore := bucket.GetSingleDataStore()
	testPollInterval := 10 * time.Millisecond

	// Helper to create a CfgSG with poller. ctx should be the subtest's t.Context() so
	// the poller goroutine is cancelled when the subtest ends.
	createNode := func(ctx context.Context, prefix string) (*CfgSG, chan cbgt.CfgEvent) {
		cfg, err := newCfgSG(ctx, dataStore, prefix, true, testPollInterval)
		require.NoError(t, err)

		eventChan := make(chan cbgt.CfgEvent, 10)
		return cfg, eventChan
	}

	t.Run("cross_node_change_detection", func(t *testing.T) {
		nodeA, eventsA := createNode(t.Context(), t.Name())
		nodeB, eventsB := createNode(t.Context(), t.Name())

		key := t.Name() + "key1"

		// Both nodes subscribe to the key
		err := nodeA.Subscribe(key, eventsA)
		require.NoError(t, err)
		err = nodeB.Subscribe(key, eventsB)
		require.NoError(t, err)

		// Node A writes document
		cas, err := nodeA.Set(key, []byte(`{"source": "nodeA", "ver": 1}`), 0)
		require.NoError(t, err)

		eventA := RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, cas, eventA.CAS)
		assert.NoError(t, eventA.Error)

		eventB := RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key, eventB.Key)
		assert.Equal(t, cas, eventB.CAS)
		assert.NoError(t, eventB.Error)
	})

	t.Run("bidirectional_change_detection", func(t *testing.T) {
		nodeA, eventsA := createNode(t.Context(), t.Name())
		nodeB, eventsB := createNode(t.Context(), t.Name())

		key1 := t.Name() + "key1"
		key2 := t.Name() + "key2"

		// Both nodes subscribe to the keys
		err := nodeA.Subscribe(key1, eventsA)
		require.NoError(t, err)
		err = nodeA.Subscribe(key2, eventsA)
		require.NoError(t, err)
		err = nodeB.Subscribe(key1, eventsB)
		require.NoError(t, err)
		err = nodeB.Subscribe(key2, eventsB)
		require.NoError(t, err)

		// NodeA writes document
		casA, err := nodeA.Set(key1, []byte(`{"source": "nodeA", "ver": 1}`), 0)
		require.NoError(t, err)
		// NodeB writes document
		casB, err := nodeB.Set(key2, []byte(`{"source": "nodeB", "ver": 2}`), 0)
		require.NoError(t, err)

		//Check if event is received in node A
		receivedEventA := make(map[string]uint64)
		require.EventuallyWithT(t, func(t *assert.CollectT) {
			select {
			case event := <-eventsA:
				receivedEventA[event.Key] = event.CAS
				assert.NoError(t, event.Error)
			default:
				assert.Fail(t, "no event received")
			}
			assert.Contains(t, receivedEventA, key1)
			assert.Equal(t, receivedEventA[key1], casA)
			assert.Contains(t, receivedEventA, key2)
			assert.Equal(t, receivedEventA[key2], casB)
		}, 100*time.Millisecond, 10*time.Millisecond)

		//Check if event is received in node B
		receivedEventB := make(map[string]uint64)
		require.EventuallyWithT(t, func(t *assert.CollectT) {
			select {
			case event := <-eventsB:
				receivedEventB[event.Key] = event.CAS
				assert.NoError(t, event.Error)
			default:
				assert.Fail(t, "no event received")
			}
			assert.Contains(t, receivedEventB, key1)
			assert.Equal(t, receivedEventB[key1], casA)
			assert.Contains(t, receivedEventB, key2)
			assert.Equal(t, receivedEventB[key2], casB)
		}, 100*time.Millisecond, 10*time.Millisecond)
	})

	t.Run("concurrent_cas_conflict", func(t *testing.T) {
		nodeA, eventsA := createNode(t.Context(), t.Name())
		nodeB, eventsB := createNode(t.Context(), t.Name())

		key := t.Name() + "conflict_key"

		// Both nodes subscribe to the key
		err := nodeA.Subscribe(key, eventsA)
		require.NoError(t, err)
		err = nodeB.Subscribe(key, eventsB)
		require.NoError(t, err)

		// Node A writes document
		cas, err := nodeA.Set(key, []byte(`{"source": "nodeA", "ver": 1}`), 0)
		require.NoError(t, err)

		eventA := RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, cas, eventA.CAS)
		assert.NoError(t, eventA.Error)

		eventB := RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key, eventB.Key)
		assert.Equal(t, cas, eventB.CAS)
		assert.NoError(t, eventB.Error)

		// Both nodes try to update the document with the same CAS
		var wg sync.WaitGroup
		var errA, errB error
		var casANew, casBNew uint64

		wg.Go(func() {
			casANew, errA = nodeA.Set(key, []byte(`{"source": "nodeA", "ver": 2}`), cas)
		})
		wg.Go(func() {
			casBNew, errB = nodeB.Set(key, []byte(`{"source": "nodeB", "ver": 2}`), cas)
		})
		wg.Wait()

		var successCas uint64
		var failedNodeEvents chan cbgt.CfgEvent
		// Exactly one update should succeed and the other should fail with CAS Error
		require.True(t, (errA == nil && errB != nil) || (errA != nil && errB == nil))
		if errA != nil {
			require.ErrorIs(t, errA, ErrCfgCasError)
			successCas = casBNew
			failedNodeEvents = eventsA
		} else if errB != nil {
			require.ErrorIs(t, errB, ErrCfgCasError)
			successCas = casANew
			failedNodeEvents = eventsB
		}

		failedEvent := RequireChanRecvWithTimeout(t, failedNodeEvents, 100*time.Millisecond)
		assert.Equal(t, key, failedEvent.Key)
		assert.Equal(t, successCas, failedEvent.CAS, "should detect the winner's CAS")
		assert.NoError(t, failedEvent.Error)
	})

	t.Run("delete_detection_across_nodes", func(t *testing.T) {

		nodeA, eventsA := createNode(t.Context(), t.Name())
		nodeB, eventsB := createNode(t.Context(), t.Name())

		key := "delete_test_key"

		// Both nodes subscribe
		err := nodeA.Subscribe(key, eventsA)
		require.NoError(t, err)
		err = nodeB.Subscribe(key, eventsB)
		require.NoError(t, err)

		// Node A creates document
		initialCas, err := nodeA.Set(key, []byte(`{"test": "data"}`), 0)
		require.NoError(t, err)

		// Wait for Node A to detect its own write
		eventA := RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, initialCas, eventA.CAS)
		assert.NoError(t, eventA.Error)

		// Wait for Node B to detect the creation
		eventB := RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key, eventB.Key)
		assert.Equal(t, initialCas, eventB.CAS)
		assert.NoError(t, eventB.Error)

		// Node A deletes the document
		delCas, err := dataStore.Remove(nodeA.sgCfgBucketKey(key), initialCas)
		require.NoError(t, err)

		// Node A should detect its own deletion
		eventA = RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, delCas, eventA.CAS, "Node A: CAS should be 0 for deletion")
		assert.NoError(t, eventA.Error)

		// Node B should detect deletion via polling
		eventB = RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key, eventB.Key)
		assert.Equal(t, delCas, eventB.CAS, "Node B: CAS should be 0 for deletion")
		assert.NoError(t, eventB.Error)
	})

	t.Run("multiple_keys_mixed_operations", func(t *testing.T) {

		nodeA, eventsA := createNode(t.Context(), t.Name())
		nodeB, eventsB := createNode(t.Context(), t.Name())

		key1 := "key1_create_by_a"
		key2 := "key2_update_by_both"
		key3 := "key3_delete_by_a"
		key4 := "key4_create_by_b"

		// Both nodes subscribe to all keys
		for _, key := range []string{key1, key2, key3, key4} {
			err := nodeA.Subscribe(key, eventsA)
			require.NoError(t, err)
			err = nodeB.Subscribe(key, eventsB)
			require.NoError(t, err)
		}

		// === PHASE 1: Initial creates ===

		// Node A creates key1
		casA1, err := nodeA.Set(key1, []byte(`{"created_by": "nodeA"}`), 0)
		require.NoError(t, err)

		// Node A creates key2 (will be updated by both)
		casA2Initial, err := nodeA.Set(key2, []byte(`{"version": 1, "by": "nodeA"}`), 0)
		require.NoError(t, err)

		// Node A creates key3 (will be deleted)
		casA3, err := nodeA.Set(key3, []byte(`{"to_be_deleted": true}`), 0)
		require.NoError(t, err)

		// Node B creates key4
		casB4, err := nodeB.Set(key4, []byte(`{"created_by": "nodeB"}`), 0)
		require.NoError(t, err)

		// Wait for Node A to detect all 4 keys (3 self + 1 from Node B)
		receivedEventsA := make(map[string]uint64)
		receivedErrorsA := make(map[string]error)
		require.EventuallyWithT(t, func(collectT *assert.CollectT) {
			select {
			case event := <-eventsA:
				receivedEventsA[event.Key] = event.CAS
				receivedErrorsA[event.Key] = event.Error
			default:
			}
			// Node A should see all 4 keys created with correct CAS and no errors
			assert.Contains(collectT, receivedEventsA, key1)
			assert.Equal(collectT, casA1, receivedEventsA[key1])
			assert.NoError(collectT, receivedErrorsA[key1])

			assert.Contains(collectT, receivedEventsA, key2)
			assert.Equal(collectT, casA2Initial, receivedEventsA[key2])
			assert.NoError(collectT, receivedErrorsA[key2])

			assert.Contains(collectT, receivedEventsA, key3)
			assert.Equal(collectT, casA3, receivedEventsA[key3])
			assert.NoError(collectT, receivedErrorsA[key3])

			assert.Contains(collectT, receivedEventsA, key4)
			assert.Equal(collectT, casB4, receivedEventsA[key4])
			assert.NoError(collectT, receivedErrorsA[key4])
		}, 200*time.Millisecond, 10*time.Millisecond)

		// Wait for Node B to detect all 4 keys (1 self + 3 from Node A)
		receivedEventsB := make(map[string]uint64)
		receivedErrorsB := make(map[string]error)
		require.EventuallyWithT(t, func(collectT *assert.CollectT) {
			select {
			case event := <-eventsB:
				receivedEventsB[event.Key] = event.CAS
				receivedErrorsB[event.Key] = event.Error
			default:
			}
			// Node B should see all 4 keys created with correct CAS and no errors
			assert.Contains(collectT, receivedEventsB, key1)
			assert.Equal(collectT, casA1, receivedEventsB[key1])
			assert.NoError(collectT, receivedErrorsB[key1])

			assert.Contains(collectT, receivedEventsB, key2)
			assert.Equal(collectT, casA2Initial, receivedEventsB[key2])
			assert.NoError(collectT, receivedErrorsB[key2])

			assert.Contains(collectT, receivedEventsB, key3)
			assert.Equal(collectT, casA3, receivedEventsB[key3])
			assert.NoError(collectT, receivedErrorsB[key3])

			assert.Contains(collectT, receivedEventsB, key4)
			assert.Equal(collectT, casB4, receivedEventsB[key4])
			assert.NoError(collectT, receivedErrorsB[key4])
		}, 200*time.Millisecond, 10*time.Millisecond)

		// === PHASE 2: Mixed operations ===

		// Node A updates key2
		casA2Updated, err := nodeA.Set(key2, []byte(`{"version": 2, "by": "nodeA"}`), casA2Initial)
		require.NoError(t, err)

		// Node B updates key1
		casB1Updated, err := nodeB.Set(key1, []byte(`{"updated_by": "nodeB"}`), casA1)
		require.NoError(t, err)

		// Node A deletes key3
		delCas, err := dataStore.Remove(nodeA.sgCfgBucketKey(key3), casA3)
		require.NoError(t, err)

		// Node A should detect: key2 update (self), key1 update (from B), key3 delete (self)
		receivedEventsA = make(map[string]uint64)
		receivedErrorsA = make(map[string]error)
		require.EventuallyWithT(t, func(collectT *assert.CollectT) {
			select {
			case event := <-eventsA:
				receivedEventsA[event.Key] = event.CAS
				receivedErrorsA[event.Key] = event.Error
			default:
			}
			assert.Contains(collectT, receivedEventsA, key1, "Node A should detect key1 update from Node B")
			assert.Equal(collectT, casB1Updated, receivedEventsA[key1], "Node A should have updated CAS for key1")
			assert.NoError(collectT, receivedErrorsA[key1], "Node A should have no error for key1")

			assert.Contains(collectT, receivedEventsA, key2, "Node A should detect key2 update")
			assert.Equal(collectT, casA2Updated, receivedEventsA[key2], "Node A should have updated CAS for key2")
			assert.NoError(collectT, receivedErrorsA[key2], "Node A should have no error for key2")

			assert.Contains(collectT, receivedEventsA, key3, "Node A should detect key3 deletion")
			assert.Equal(collectT, delCas, receivedEventsA[key3], "Node A should have CAS=0 for deleted key3")
			assert.NoError(collectT, receivedErrorsA[key3], "Node A should have no error for key3 deletion")
		}, 200*time.Millisecond, 10*time.Millisecond)

		// Node B should detect: key2 update (from A), key1 update (self), key3 delete (from A)
		receivedEventsB = make(map[string]uint64)
		receivedErrorsB = make(map[string]error)
		require.EventuallyWithT(t, func(collectT *assert.CollectT) {
			select {
			case event := <-eventsB:
				receivedEventsB[event.Key] = event.CAS
				receivedErrorsB[event.Key] = event.Error
			default:
			}
			assert.Contains(collectT, receivedEventsB, key1, "Node B should detect key1 update")
			assert.Equal(collectT, casB1Updated, receivedEventsB[key1], "Node B should have updated CAS for key1")
			assert.NoError(collectT, receivedErrorsB[key1], "Node B should have no error for key1")

			assert.Contains(collectT, receivedEventsB, key2, "Node B should detect key2 update from Node A")
			assert.Equal(collectT, casA2Updated, receivedEventsB[key2], "Node B should have updated CAS for key2")
			assert.NoError(collectT, receivedErrorsB[key2], "Node B should have no error for key2")

			assert.Contains(collectT, receivedEventsB, key3, "Node B should detect key3 deletion from Node A")
			assert.Equal(collectT, delCas, receivedEventsB[key3], "Node B should have CAS=0 for deleted key3")
			assert.NoError(collectT, receivedErrorsB[key3], "Node B should have no error for key3 deletion")
		}, 200*time.Millisecond, 10*time.Millisecond)
	})

	t.Run("rapid_sequential_updates", func(t *testing.T) {
		nodeA, eventsA := createNode(t.Context(), t.Name())
		nodeB, eventsB := createNode(t.Context(), t.Name())

		key := "rapid_update_key"

		// Both nodes subscribe
		err := nodeA.Subscribe(key, eventsA)
		require.NoError(t, err)
		err = nodeB.Subscribe(key, eventsB)
		require.NoError(t, err)

		// Node A creates initial document
		cas, err := nodeA.Set(key, []byte(`{"version": 0}`), 0)
		require.NoError(t, err)

		// Wait for both nodes to sync initial state
		eventA := RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, cas, eventA.CAS)
		assert.NoError(t, eventA.Error)

		eventB := RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key, eventB.Key)
		assert.Equal(t, cas, eventB.CAS)
		assert.NoError(t, eventB.Error)

		// Node A performs 10 rapid sequential updates
		numUpdates := 10
		var finalCas uint64
		for i := 1; i <= numUpdates; i++ {
			cas, err = nodeA.Set(key, []byte(fmt.Sprintf(`{"version": %d}`, i)), cas)
			require.NoError(t, err)
			if i == numUpdates {
				finalCas = cas
			}
		}

		// Node A should eventually converge to final state
		// May detect all or some of its own updates
		receivedEventsA := make(map[string]uint64)
		require.EventuallyWithT(t, func(collectT *assert.CollectT) {
			select {
			case event := <-eventsA:
				receivedEventsA[event.Key] = event.CAS
				assert.NoError(collectT, event.Error)
			default:
			}
			// Eventually should converge to final CAS
			assert.Contains(collectT, receivedEventsA, key)
			assert.Equal(collectT, finalCas, receivedEventsA[key], "Node A should converge to final CAS")
		}, 300*time.Millisecond, 10*time.Millisecond)

		// Node B should eventually converge to the final state via polling
		// May miss intermediate updates, but final state must match
		receivedEventsB := make(map[string]uint64)
		require.EventuallyWithT(t, func(collectT *assert.CollectT) {
			select {
			case event := <-eventsB:
				receivedEventsB[event.Key] = event.CAS
				assert.NoError(collectT, event.Error)
			default:
			}
			// Eventually should converge to final CAS
			assert.Contains(collectT, receivedEventsB, key)
			assert.Equal(collectT, finalCas, receivedEventsB[key], "Node B should converge to final CAS")
		}, 300*time.Millisecond, 10*time.Millisecond)

		// Verify final state matches on both nodes
		assert.Equal(t, finalCas, receivedEventsA[key], "Node A final state should match")
		assert.Equal(t, finalCas, receivedEventsB[key], "Node B final state should match")
	})

	t.Run("late_registration", func(t *testing.T) {
		nodeA, eventsA := createNode(t.Context(), t.Name())
		nodeB, eventsB := createNode(t.Context(), t.Name())

		key := "late_registration_key"

		// Only Node A subscribes initially
		err := nodeA.Subscribe(key, eventsA)
		require.NoError(t, err)

		// Node A creates document
		initialCas, err := nodeA.Set(key, []byte(`{"version": 1}`), 0)
		require.NoError(t, err)

		// Node A should detect its own write
		eventA := RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, initialCas, eventA.CAS)
		assert.NoError(t, eventA.Error)

		// Node A updates the document
		updatedCas, err := nodeA.Set(key, []byte(`{"version": 2}`), initialCas)
		require.NoError(t, err)

		// Node A should detect its own update
		eventA = RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, updatedCas, eventA.CAS)
		assert.NoError(t, eventA.Error)

		// NOW Node B subscribes (late registration - after document already exists and was updated)
		err = nodeB.Subscribe(key, eventsB)
		require.NoError(t, err)

		// Verify Node B has registered the key with current CAS from datastore
		require.EventuallyWithT(t, func(collectT *assert.CollectT) {
			nodeB.nodePoller.lock.Lock()
			cas, registered := nodeB.nodePoller.keyWatcher[nodeB.sgCfgBucketKey(key)]
			nodeB.nodePoller.lock.Unlock()

			assert.True(collectT, registered, "Node B should have registered the key")
			assert.Equal(collectT, updatedCas, cas, "Node B should pick up current CAS on late registration")
		}, 100*time.Millisecond, 10*time.Millisecond)

		// Node A makes another update (after Node B has registered)
		finalCas, err := nodeA.Set(key, []byte(`{"version": 3}`), updatedCas)
		require.NoError(t, err)

		// Both nodes should detect this new update
		eventA = RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, finalCas, eventA.CAS)
		assert.NoError(t, eventA.Error)

		eventB := RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key, eventB.Key)
		assert.Equal(t, finalCas, eventB.CAS)
		assert.NoError(t, eventB.Error)
	})

	t.Run("subscription_isolation", func(t *testing.T) {
		nodeA, eventsA := createNode(t.Context(), t.Name())
		nodeB, eventsB := createNode(t.Context(), t.Name())

		key1 := "key1_only_a"
		key2 := "key2_only_b"

		// Node A subscribes only to key1
		err := nodeA.Subscribe(key1, eventsA)
		require.NoError(t, err)

		// Node B subscribes only to key2
		err = nodeB.Subscribe(key2, eventsB)
		require.NoError(t, err)

		// Node A creates key1
		casA1, err := nodeA.Set(key1, []byte(`{"from": "nodeA"}`), 0)
		require.NoError(t, err)

		// Node B creates key2
		casB2, err := nodeB.Set(key2, []byte(`{"from": "nodeB"}`), 0)
		require.NoError(t, err)

		// Node A should only detect key1 (its own subscription)
		eventA := RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key1, eventA.Key, "Node A should only receive events for key1")
		assert.Equal(t, casA1, eventA.CAS)
		assert.NoError(t, eventA.Error)

		// Node B should only detect key2 (its own subscription)
		eventB := RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key2, eventB.Key, "Node B should only receive events for key2")
		assert.Equal(t, casB2, eventB.CAS)
		assert.NoError(t, eventB.Error)

		// Verify Node A's channel has no more events (shouldn't have received key2)
		select {
		case unexpectedEvent := <-eventsA:
			t.Fatalf("Node A should not receive events for key2, got event for key: %s", unexpectedEvent.Key)
		case <-time.After(50 * time.Millisecond):
			// Good - no unexpected events
		}

		// Verify Node B's channel has no more events (shouldn't have received key1)
		select {
		case unexpectedEvent := <-eventsB:
			t.Fatalf("Node B should not receive events for key1, got event for key: %s", unexpectedEvent.Key)
		case <-time.After(50 * time.Millisecond):
			// Good - no unexpected events
		}

		// Now Node A updates key2 (which Node B is subscribed to)
		casA2, err := nodeA.Set(key2, []byte(`{"from": "nodeA", "updated": true}`), casB2)
		require.NoError(t, err)

		// Node B should detect the update to key2 (even though Node A made the change)
		eventB = RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key2, eventB.Key, "Node B should receive update for key2")
		assert.Equal(t, casA2, eventB.CAS)
		assert.NoError(t, eventB.Error)

		// Node A still should not receive events for key2 (not subscribed)
		select {
		case unexpectedEvent := <-eventsA:
			t.Fatalf("Node A should not receive events for key2, got event for key: %s", unexpectedEvent.Key)
		case <-time.After(50 * time.Millisecond):
			// Good - no unexpected events
		}
	})

	t.Run("document_resurrection", func(t *testing.T) {

		nodeA, eventsA := createNode(t.Context(), t.Name())
		nodeB, eventsB := createNode(t.Context(), t.Name())

		key := "resurrection_key"

		// Both nodes subscribe
		err := nodeA.Subscribe(key, eventsA)
		require.NoError(t, err)
		err = nodeB.Subscribe(key, eventsB)
		require.NoError(t, err)

		// Node A creates document
		initialCas, err := nodeA.Set(key, []byte(`{"version": 1}`), 0)
		require.NoError(t, err)

		// Both nodes detect creation
		eventA := RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, initialCas, eventA.CAS)
		assert.NoError(t, eventA.Error)

		eventB := RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key, eventB.Key)
		assert.Equal(t, initialCas, eventB.CAS)
		assert.NoError(t, eventB.Error)

		// Node A deletes the document
		//err = nodeA.Del(key, initialCas)
		delCas, err := dataStore.Remove(nodeA.sgCfgBucketKey(key), initialCas)
		require.NoError(t, err)

		// Both nodes detect deletion
		eventA = RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, delCas, eventA.CAS, "Node A: CAS should be 0 for deletion")
		assert.NoError(t, eventA.Error)

		eventB = RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key, eventB.Key)
		assert.Equal(t, delCas, eventB.CAS, "Node B: CAS should be 0 for deletion")
		assert.NoError(t, eventB.Error)

		// === RESURRECTION: Node A recreates the document ===
		resurrectCas, err := nodeA.Set(key, []byte(`{"version": 2, "resurrected": true}`), 0)
		require.NoError(t, err)

		// Both nodes should detect resurrection with new non-zero CAS
		eventA = RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, resurrectCas, eventA.CAS, "Node A should detect resurrection with new CAS")
		assert.Greater(t, eventA.CAS, uint64(0), "Resurrected document should have non-zero CAS")
		assert.NoError(t, eventA.Error)

		eventB = RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key, eventB.Key)
		assert.Equal(t, resurrectCas, eventB.CAS, "Node B should detect resurrection with new CAS")
		assert.Greater(t, eventB.CAS, uint64(0), "Resurrected document should have non-zero CAS")
		assert.NoError(t, eventB.Error)

		// Verify we can continue updating the resurrected document
		finalCas, err := nodeA.Set(key, []byte(`{"version": 3}`), resurrectCas)
		require.NoError(t, err)

		// Both nodes detect the update after resurrection
		eventA = RequireChanRecvWithTimeout(t, eventsA, 100*time.Millisecond)
		assert.Equal(t, key, eventA.Key)
		assert.Equal(t, finalCas, eventA.CAS)
		assert.NoError(t, eventA.Error)

		eventB = RequireChanRecvWithTimeout(t, eventsB, 100*time.Millisecond)
		assert.Equal(t, key, eventB.Key)
		assert.Equal(t, finalCas, eventB.CAS)
		assert.NoError(t, eventB.Error)
	})

}
