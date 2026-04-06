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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

			// Call Poll
			poller.Poll()

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
		poller.Poll()

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
		poller.Poll()
		require.Equal(t, 1, eventCount, "first poll should fire one event")

		// Second poll without changes - should NOT fire event
		poller.Poll()
		require.Equal(t, 1, eventCount, "second poll should not fire event since CAS unchanged")

		// Third poll without changes - should still NOT fire event
		poller.Poll()
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
		poller.Poll()

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

		poller.Poll()

		// Verify event was fired
		require.True(t, eventFired, "event should be fired for purged document")
		require.Equal(t, uint64(0), firedCas, "cas should be 0 for purged document")

		// Verify keyWatcher was updated
		poller.lock.Lock()
		require.Equal(t, uint64(0), poller.keyWatcher[key], "keyWatcher should have cas=0 after purge")
		poller.lock.Unlock()
	})
}

func TestCfgNodePoller_StartPolling(t *testing.T) {
	bucket := GetTestBucket(t)
	defer bucket.Close(t.Context())
	datastore := bucket.GetMetadataStore()

	// Polling should stop when context is cancelled
	t.Run("start_polling_stops_on_context_cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

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

		poller.StartPolling(ctx)

		// Wait for at least one poll cycle
		time.Sleep(20 * time.Millisecond)

		// Cancel context to stop polling
		cancel()

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
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

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

		poller.StartPolling(ctx)

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
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

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

		poller.StartPolling(ctx)

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
		ctx, cancel := context.WithCancel(t.Context())

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

		poller.StartPolling(ctx)
		cancel()

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
