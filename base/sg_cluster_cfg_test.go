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
	"testing"

	"github.com/couchbase/cbgt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKVOpStatCfg checks that NewKVOpStatCfg adds the KV operations a bucket-backed CfgSG makes to the stat, and none for
// an in-memory Cfg.
func TestKVOpStatCfg(t *testing.T) {
	ctx := t.Context()
	bucket := GetTestBucket(t)
	t.Cleanup(func() { bucket.Close(ctx) })

	bucketCfg, err := NewCfgSG(ctx, bucket.GetSingleDataStore(), t.Name()+":", false)
	require.NoError(t, err)
	memoryCfg, err := NewCbgtCfgMem()
	require.NoError(t, err)

	for name, tc := range map[string]struct {
		cfg           cbgt.Cfg
		expectedKVOps int64
	}{
		"bucket": {cfg: bucketCfg, expectedKVOps: 3},
		"memory": {cfg: memoryCfg, expectedKVOps: 0},
	} {
		t.Run(name, func(t *testing.T) {
			stat := &SgwIntStat{}
			cfg := NewKVOpStatCfg(tc.cfg, stat)
			cas, err := cfg.Set("key1", []byte("val1"), 0)
			require.NoError(t, err)
			_, _, err = cfg.Get("key1", 0)
			require.NoError(t, err)
			// Answered without a KV read.
			_, _, err = cfg.Get(cbgt.VERSION_KEY, 0)
			require.NoError(t, err)
			require.NoError(t, cfg.Del("key1", cas))

			assert.Equal(t, tc.expectedKVOps, stat.Value(), "a set, a get and a delete")
		})
	}
}
