/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package channels

import (
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func TestActiveChannelsConcurrency(t *testing.T) {
	activeChannelStat := &base.SgwIntStat{}
	ac := NewActiveChannels(activeChannelStat)
	var wg sync.WaitGroup

	// Concurrent Incr, Decr
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ac.IncrChannels(TimedSetFromString("ABC:1,DEF:2,GHI:3,JKL:1,MNO:1"))
			ac.DecrChannels(TimedSetFromString("ABC:1,DEF:2"))
		}()
	}
	wg.Wait()
	assert.False(t, ac.IsActive("ABC"))
	assert.False(t, ac.IsActive("DEF"))
	assert.True(t, ac.IsActive("GHI"))
	assert.True(t, ac.IsActive("JKL"))
	assert.True(t, ac.IsActive("MNO"))
	assert.Equal(t, int64(3), activeChannelStat.Value())

	// Concurrent UpdateChanged
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			changedKeys := ChangedKeys{"ABC": true, "DEF": true, "GHI": false, "MNO": true}
			ac.UpdateChanged(changedKeys)
			changedKeys = ChangedKeys{"DEF": false}
			ac.UpdateChanged(changedKeys)
		}()
	}
	wg.Wait()
	assert.True(t, ac.IsActive("ABC"))
	assert.False(t, ac.IsActive("DEF"))
	assert.False(t, ac.IsActive("GHI"))
	assert.True(t, ac.IsActive("JKL"))
	assert.True(t, ac.IsActive("MNO"))
	assert.Equal(t, int64(3), activeChannelStat.Value())

}
