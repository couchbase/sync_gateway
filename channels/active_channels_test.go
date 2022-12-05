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
	"github.com/stretchr/testify/require"
)

func TestActiveChannelsConcurrency(t *testing.T) {
	activeChannelStat := &base.SgwIntStat{}
	ac := NewActiveChannels(activeChannelStat)
	var wg sync.WaitGroup

	ABCChan := NewID("ABC", base.DefaultCollectionID)
	DEFChan := NewID("DEF", base.DefaultCollectionID)
	GHIChan := NewID("GHI", base.DefaultCollectionID)
	JKLChan := NewID("JKL", base.DefaultCollectionID)
	MNOChan := NewID("MNO", base.DefaultCollectionID)
	// Concurrent Incr, Decr
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			const seqNo uint64 = 0
			activeChans, err := SetOf(ABCChan, DEFChan, GHIChan, JKLChan, MNOChan)
			require.NoError(t, err)
			activeChansTimedSet := AtSequenceByCollection(activeChans, seqNo)
			ac.IncrChannels(activeChansTimedSet)
			inactiveChans, err := SetOf(ABCChan, DEFChan)
			require.NoError(t, err)
			inactiveChansTimedSet := AtSequenceByCollection(inactiveChans, seqNo)

			ac.DecrChannels(inactiveChansTimedSet)
		}()
	}
	wg.Wait()
	assert.False(t, ac.IsActive(ABCChan))
	assert.False(t, ac.IsActive(DEFChan))
	assert.True(t, ac.IsActive(GHIChan))
	assert.True(t, ac.IsActive(JKLChan))
	assert.True(t, ac.IsActive(MNOChan))
	assert.Equal(t, int64(3), activeChannelStat.Value())

	// Concurrent UpdateChanged
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			changedKeys := ChangedChannels{ABCChan: true, DEFChan: true, GHIChan: false, MNOChan: true}
			ac.UpdateChanged(changedKeys)
			changedKeys = ChangedChannels{DEFChan: false}
			ac.UpdateChanged(changedKeys)
		}()
	}
	wg.Wait()
	assert.True(t, ac.IsActive(ABCChan))
	assert.False(t, ac.IsActive(DEFChan))
	assert.False(t, ac.IsActive(GHIChan))
	assert.True(t, ac.IsActive(JKLChan))
	assert.True(t, ac.IsActive(MNOChan))
	assert.Equal(t, int64(3), activeChannelStat.Value())

}
