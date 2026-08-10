/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"runtime"
	"testing"
	"time"

	"github.com/couchbase/cbgt"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// cfgSGImplementations returns a constructor per CfgSG backend (bucket-backed and
// cfgMemoryStorage-backed), so shared behavior can be verified against both. Call each
// constructor at most once per top-level test - they share a bucket/keyspace keyed off t.Name().
func cfgSGImplementations(t *testing.T) map[string]func() *CfgSG {
	ctx := t.Context()
	bucket := GetTestBucket(t)
	t.Cleanup(func() { bucket.Close(ctx) })
	dataStore := bucket.GetSingleDataStore()

	return map[string]func() *CfgSG{
		"bucket": func() *CfgSG {
			cfg, err := NewCfgSG(ctx, dataStore, t.Name()+":", false)
			require.NoError(t, err)
			return cfg
		},
		"memory": func() *CfgSG {
			cfg, err := NewCbgtCfgMem(ctx)
			require.NoError(t, err)
			return cfg
		},
	}
}

// TestCfgSGGetSetDel runs the same sequence of Get/Set/Del steps against both CfgSG backends.
// Steps are ordered - each depends on cas values from earlier ones.
func TestCfgSGGetSetDel(t *testing.T) {
	for name, newCfg := range cfgSGImplementations(t) {
		t.Run(name, func(t *testing.T) {
			cfg := newCfg()
			var cas1, cas2 uint64

			steps := []struct {
				name string
				run  func(t *testing.T)
			}{
				{
					name: "missing key returns no error, no value, cas 0",
					run: func(t *testing.T) {
						val, cas, err := cfg.Get("key1", 0)
						require.NoError(t, err)
						assert.Nil(t, val)
						assert.Equal(t, uint64(0), cas)
					},
				},
				{
					name: "insert",
					run: func(t *testing.T) {
						var err error
						cas1, err = cfg.Set("key1", []byte("val1"), 0)
						require.NoError(t, err)
						assert.NotEqual(t, uint64(0), cas1)
					},
				},
				{
					name: "inserting again with cas=0 fails, the entry already exists",
					run: func(t *testing.T) {
						_, err := cfg.Set("key1", []byte("val1-again"), 0)
						require.ErrorIs(t, err, ErrCfgCasError)
					},
				},
				{
					name: "Get returns the inserted value and cas",
					run: func(t *testing.T) {
						val, cas, err := cfg.Get("key1", 0)
						require.NoError(t, err)
						assert.Equal(t, []byte("val1"), val)
						assert.Equal(t, cas1, cas)
					},
				},
				{
					name: "Get with the matching cas succeeds",
					run: func(t *testing.T) {
						_, _, err := cfg.Get("key1", cas1)
						require.NoError(t, err)
					},
				},
				{
					name: "Get with a mismatched cas fails",
					run: func(t *testing.T) {
						_, _, err := cfg.Get("key1", cas1+1000)
						require.ErrorIs(t, err, ErrCfgCasError)
					},
				},
				{
					name: "Set with the wrong cas fails",
					run: func(t *testing.T) {
						_, err := cfg.Set("key1", []byte("val2"), cas1+1000)
						require.ErrorIs(t, err, ErrCfgCasError)
					},
				},
				{
					name: "Set with the correct cas succeeds",
					run: func(t *testing.T) {
						var err error
						cas2, err = cfg.Set("key1", []byte("val2"), cas1)
						require.NoError(t, err)
						assert.NotEqual(t, cas1, cas2)
					},
				},
				{
					name: "a key can't start with a colon",
					run: func(t *testing.T) {
						_, err := cfg.Set(":bad", []byte("x"), 0)
						require.Error(t, err)
					},
				},
				{
					name: "Del with the wrong cas fails",
					run: func(t *testing.T) {
						err := cfg.Del("key1", cas1)
						require.ErrorIs(t, err, ErrCfgCasError)
					},
				},
				{
					name: "Del with the correct cas succeeds",
					run: func(t *testing.T) {
						require.NoError(t, cfg.Del("key1", cas2))
					},
				},
				{
					// a real bucket leaves a tombstone with a nonzero cas, so only the value is
					// reliably comparable across backends here
					name: "Get after Del returns no error, no value",
					run: func(t *testing.T) {
						val, _, err := cfg.Get("key1", 0)
						require.NoError(t, err)
						assert.Nil(t, val)
					},
				},
				{
					// a fresh key, not the now-deleted "key1": Rosmar's Remove(cas=0) treats a
					// tombstone's real cas as a mismatch rather than not-found
					name: "Del on a never-existed key returns a not-found error, unlike Get",
					run: func(t *testing.T) {
						err := cfg.Del("never-existed", 0)
						require.Error(t, err)
						assert.True(t, IsDocNotFoundError(err))
					},
				},
			}

			for _, step := range steps {
				t.Run(step.name, step.run)
			}
		})
	}
}

// TestCfgSGEventDelivery runs {trigger, expected per-backend delivery} cases against both CfgSG
// backends, including a real divergence: only cfgMemoryStorage fires events directly from Set -
// the bucket-backed CfgSG needs a caching feed to call FireEvent for it.
func TestCfgSGEventDelivery(t *testing.T) {
	tests := []struct {
		name      string
		trigger   func(t *testing.T, cfg *CfgSG)
		wantEvent map[string]bool // keyed by cfgSGImplementations name
	}{
		{
			name:      "FireEvent delivers directly",
			trigger:   func(t *testing.T, cfg *CfgSG) { cfg.FireEvent(cfg.sgCfgBucketKey("key1"), 42, nil) },
			wantEvent: map[string]bool{"bucket": true, "memory": true},
		},
		{
			name:      "Refresh delivers directly",
			trigger:   func(t *testing.T, cfg *CfgSG) { require.NoError(t, cfg.Refresh()) },
			wantEvent: map[string]bool{"bucket": true, "memory": true},
		},
		{
			name: "Set only fires an event by itself on the memory backend",
			trigger: func(t *testing.T, cfg *CfgSG) {
				_, err := cfg.Set("key1", []byte("val1"), 0)
				require.NoError(t, err)
			},
			wantEvent: map[string]bool{"bucket": false, "memory": true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for name, newCfg := range cfgSGImplementations(t) {
				t.Run(name, func(t *testing.T) {
					cfg := newCfg()

					ch := make(chan cbgt.CfgEvent, 1)
					require.NoError(t, cfg.Subscribe("key1", ch))

					tc.trigger(t, cfg)

					select {
					case <-ch:
						assert.True(t, tc.wantEvent[name], "unexpected event delivery for %s", name)
					case <-time.After(200 * time.Millisecond):
						assert.False(t, tc.wantEvent[name], "expected event delivery for %s", name)
					}
				})
			}
		})
	}
}

// TestCfgSGStopUnblocksPendingDeliveries is a regression test for the leak this fixed: cbgt.Cfg
// has no Unsubscribe, so a dead subscriber used to block its delivery goroutine forever.
func TestCfgSGStopUnblocksPendingDeliveries(t *testing.T) {
	for name, newCfg := range cfgSGImplementations(t) {
		t.Run(name, func(t *testing.T) {
			cfg := newCfg()

			ch := make(chan cbgt.CfgEvent) // unbuffered and never read - nobody is listening
			require.NoError(t, cfg.Subscribe("key1", ch))

			before := runtime.NumGoroutine()
			cfg.FireEvent(cfg.sgCfgBucketKey("key1"), 1, nil)

			// compare against this snapshot, not `before` - ambient goroutine churn elsewhere in
			// the binary would make an absolute comparison flaky
			var blocked int
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				blocked = runtime.NumGoroutine()
				assert.GreaterOrEqual(c, blocked, before+1)
			}, 5*time.Second, 10*time.Millisecond, "delivery goroutine should be blocked on send")

			cfg.Stop()

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Less(c, runtime.NumGoroutine(), blocked)
			}, 5*time.Second, 10*time.Millisecond, "Stop() should unblock the delivery goroutine")
		})
	}
}

// TestCfgMemoryStorageGet exercises rv handling only reachable by calling cfgMemoryStorage
// directly - CfgSG.Get always passes a *[]byte.
func TestCfgMemoryStorageGet(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, m *cfgMemoryStorage)
	}{
		{
			name: "nil rv is ok",
			run: func(t *testing.T, m *cfgMemoryStorage) {
				cas, err := m.Get(t.Context(), "key1", nil)
				require.NoError(t, err)
				assert.NotEqual(t, uint64(0), cas)
			},
		},
		{
			name: "wrong rv type errors",
			run: func(t *testing.T, m *cfgMemoryStorage) {
				var val string
				_, err := m.Get(t.Context(), "key1", &val)
				require.Error(t, err)
			},
		},
		{
			name: "returned value is a defensive copy",
			run: func(t *testing.T, m *cfgMemoryStorage) {
				var val []byte
				_, err := m.Get(t.Context(), "key1", &val)
				require.NoError(t, err)
				val[0] = 'X'

				var val2 []byte
				_, err = m.Get(t.Context(), "key1", &val2)
				require.NoError(t, err)
				assert.Equal(t, []byte("val1"), val2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := newCfgMemoryStorage(nil)
			_, err := m.WriteCas(t.Context(), "key1", 0, 0, []byte("val1"), 0)
			require.NoError(t, err)
			tc.run(t, m)
		})
	}
}

// TestCfgMemoryStorageCasSemantics covers cas edge cases that only cfgMemoryStorage needs to
// handle itself (WriteCas/Remove are otherwise exercised indirectly via CfgSG.Set/Del).
func TestCfgMemoryStorageCasSemantics(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, m *cfgMemoryStorage)
	}{
		{
			// mirrors cbgt.CfgMem; the bucket-backed CfgSG.Set never special-cased this sentinel,
			// so it would fail as an ordinary CAS mismatch there instead
			name: "CFG_CAS_FORCE bypasses the cas check",
			run: func(t *testing.T, m *cfgMemoryStorage) {
				cas1, err := m.WriteCas(t.Context(), "key1", 0, 0, []byte("val1"), 0)
				require.NoError(t, err)

				cas2, err := m.WriteCas(t.Context(), "key1", 0, cbgt.CFG_CAS_FORCE, []byte("val2"), 0)
				require.NoError(t, err)
				assert.NotEqual(t, cas1, cas2)
			},
		},
		{
			// per the cbgt.Cfg.Del contract, not Rosmar's behavior (which treats cas=0 as literal
			// and fails)
			name: "cas=0 on Remove deletes regardless of current cas",
			run: func(t *testing.T, m *cfgMemoryStorage) {
				_, err := m.WriteCas(t.Context(), "key1", 0, 0, []byte("val1"), 0)
				require.NoError(t, err)

				_, err = m.Remove(t.Context(), "key1", 0)
				require.NoError(t, err)

				_, err = m.Get(t.Context(), "key1", nil)
				require.Error(t, err)
				assert.True(t, IsDocNotFoundError(err))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.run(t, newCfgMemoryStorage(nil))
		})
	}
}
