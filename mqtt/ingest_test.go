package mqtt

import (
	"context"
	"fmt"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestTimeSeries(t *testing.T) {
	const topicName = "temperature"
	cfg := IngestConfig{
		DocID:    "tempDoc",
		Encoding: base.StringPtr("JSON"),
		TimeSeries: &TimeSeriesConfig{
			TimeProperty:   "${message.payload.when}",
			TimeFormat:     "unix_epoch",
			ValuesTemplate: []any{"${message.payload.temperature}"},
			Rotation:       "1d",
		},
	}
	require.NoError(t, cfg.Validate(topicName))

	// Set up a mock DataStore that allows only Update to "tempDoc" and puts the body in `docBody`:
	var docBody Body
	dataStore := mockDataStore{
		update: func(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
			require.Equal(t, cfg.DocID, k)
			require.Equal(t, uint32(0), exp)
			var rawDoc []byte
			rawDoc, _, _, err = callback(base.MustJSONMarshal(t, docBody))
			if err == nil {
				require.NoError(t, base.JSONUnmarshal(rawDoc, &docBody))
			}
			return 1, err
		},
	}

	topic := TopicMatch{Name: topicName}

	var expectedDocBody Body

	// Add first message:
	t.Run("first message", func(t *testing.T) {
		payload := []byte(`{"temperature": 98.5, "when": 1712864539}`)
		err := ingestMessageToDataStore(context.TODO(), topic, payload, &cfg, &dataStore, 0)
		require.NoError(t, err)

		expectedDocBody := map[string]any{
			"ts_data": []any{
				[]any{1712864539.0, 98.5},
			},
			"ts_start": 1712864539.0,
			"ts_end":   1712864539.0,
		}
		assert.EqualValues(t, expectedDocBody, docBody)
	})

	// Add another message:
	t.Run("second message", func(t *testing.T) {
		payload := []byte(`{"temperature": 99.3, "when": 1712889720}`)
		err := ingestMessageToDataStore(context.TODO(), topic, payload, &cfg, &dataStore, 0)
		require.NoError(t, err)

		expectedDocBody = map[string]any{
			"ts_data": []any{
				[]any{1712864539.0, 98.5},
				[]any{1712889720.0, 99.3},
			},
			"ts_start": 1712864539.0,
			"ts_end":   1712889720.0,
		}
		assert.EqualValues(t, expectedDocBody, docBody)
	})

	// A later message triggering rotation:
	t.Run("third message with rotation", func(t *testing.T) {
		// Enable the DataStore.Add method, and save the key and value:
		addCalls := 0
		var addedKey string
		var addedValue any
		dataStore.add = func(k string, exp uint32, v interface{}) (added bool, err error) {
			require.Equal(t, addCalls, 0)
			require.Equal(t, uint32(0), exp)
			addCalls += 1
			addedKey = k
			addedValue = v
			return true, nil
		}

		// Add an entry whose timestamp is more than a day after the first, triggering rotation:
		payload := []byte(`{"temperature": 99.9, "when": 1713466800}`)
		err := ingestMessageToDataStore(context.TODO(), topic, payload, &cfg, &dataStore, 0)
		require.NoError(t, err)

		// Assert that the old doc body was backed up, with the first timestamp in the docID:
		require.Equal(t, addCalls, 1)
		assert.Equal(t, addedKey, "tempDoc @ 2024-04-11T12:42:19-07:00")
		assert.EqualValues(t, expectedDocBody, addedValue)

		// Assert that the doc now contains just the new entry:
		expectedDocBody = map[string]any{
			"ts_data": []any{
				[]any{1713466800.0, 99.9},
			},
			"ts_start": 1713466800.0,
			"ts_end":   1713466800.0,
		}
		assert.EqualValues(t, expectedDocBody, docBody)
	})
}

//======== MOCK DATA STORE

// A classic mock implementing sgbucket.KVStore as well as sgbucket.DataStore's GetName method.
// All methods but GetName are stubbed out to return an "unimplemented" error.
// The Add and Update methods can be plugged in as funcs.
type mockDataStore struct {
	add    func(k string, exp uint32, v interface{}) (added bool, err error)
	update func(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error)
}

func (store *mockDataStore) GetName() string { return "mock" }
func (store *mockDataStore) Get(k string, rv interface{}) (cas uint64, err error) {
	err = errUnmocked
	return
}
func (store *mockDataStore) GetRaw(k string) (rv []byte, cas uint64, err error) {
	err = errUnmocked
	return
}
func (store *mockDataStore) GetAndTouchRaw(k string, exp uint32) (rv []byte, cas uint64, err error) {
	err = errUnmocked
	return
}
func (store *mockDataStore) Touch(k string, exp uint32) (cas uint64, err error) {
	err = errUnmocked
	return
}
func (store *mockDataStore) Add(k string, exp uint32, v interface{}) (added bool, err error) {
	if store.add != nil {
		return store.add(k, exp, v)
	} else {
		return false, errUnmocked
	}
}
func (store *mockDataStore) AddRaw(k string, exp uint32, v []byte) (added bool, err error) {
	err = errUnmocked
	return
}
func (store *mockDataStore) Set(k string, exp uint32, opts *sgbucket.UpsertOptions, v interface{}) error {
	return errUnmocked
}
func (store *mockDataStore) SetRaw(k string, exp uint32, opts *sgbucket.UpsertOptions, v []byte) error {
	return errUnmocked
}
func (store *mockDataStore) WriteCas(k string, exp uint32, cas uint64, v interface{}, opt sgbucket.WriteOptions) (casOut uint64, err error) {
	err = errUnmocked
	return
}
func (store *mockDataStore) Delete(k string) error {
	return errUnmocked
}
func (store *mockDataStore) Remove(k string, cas uint64) (casOut uint64, err error) {
	err = errUnmocked
	return
}
func (store *mockDataStore) Update(k string, exp uint32, callback sgbucket.UpdateFunc) (casOut uint64, err error) {
	if store.update != nil {
		return store.update(k, exp, callback)
	} else {
		return 0, errUnmocked
	}
}
func (store *mockDataStore) Incr(k string, amt, def uint64, exp uint32) (casOut uint64, err error) {
	err = errUnmocked
	return
}
func (store *mockDataStore) GetExpiry(ctx context.Context, k string) (expiry uint32, err error) {
	err = errUnmocked
	return
}
func (store *mockDataStore) Exists(k string) (exists bool, err error) { err = errUnmocked; return }

var errUnmocked = fmt.Errorf("unimplemented method in mockDataStore")

var (
	_ iDataStore = &mockDataStore{} // test interface compliance
)
