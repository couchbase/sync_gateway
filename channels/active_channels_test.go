package channels

import (
	"expvar"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestActiveChannelsConcurrency(t *testing.T) {

	activeChannelStat := &expvar.Int{}
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
	assert.False(t, ac.isActive("ABC"))
	assert.False(t, ac.isActive("DEF"))
	assert.True(t, ac.isActive("GHI"))
	assert.True(t, ac.isActive("JKL"))
	assert.True(t, ac.isActive("MNO"))
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
	assert.True(t, ac.isActive("ABC"))
	assert.False(t, ac.isActive("DEF"))
	assert.False(t, ac.isActive("GHI"))
	assert.True(t, ac.isActive("JKL"))
	assert.True(t, ac.isActive("MNO"))
	assert.Equal(t, int64(3), activeChannelStat.Value())

}
