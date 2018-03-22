package base

import (
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"
)

func TestLogKeyConcurrency(t *testing.T) {
	logKey := LogKey{flag: KEY_ACCESS | KEY_HTTP | KEY_REPLICATE}

	stop := make(chan struct{})

	go func() {
		for {
			select {
			default:
				logKey.Enable(KEY_DCP)
			case <-stop:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			default:
				logKey.Disable(KEY_ACCESS)
			case <-stop:
				return
			}
		}
	}()

	time.Sleep(time.Millisecond * 100)
	stop <- struct{}{}
}

func TestLogKey(t *testing.T) {
	logKey := LogKey{flag: KEY_HTTP}
	assert.True(t, logKey.Enabled(KEY_HTTP))

	// Enable more log keys.
	logKey.Enable(KEY_ACCESS | KEY_REPLICATE)
	assert.True(t, logKey.Enabled(KEY_ACCESS))
	assert.True(t, logKey.Enabled(KEY_REPLICATE))
	assert.Equals(t, logKey.flag, KEY_ACCESS|KEY_HTTP|KEY_REPLICATE)

	// Enable wildcard and check unset key is enabled.
	logKey.Enable(KEY_ALL)
	assert.True(t, logKey.Enabled(KEY_CACHE))
	assert.Equals(t, logKey.flag, KEY_ALL|KEY_ACCESS|KEY_HTTP|KEY_REPLICATE)

	// Disable wildcard and check that existing keys are still set.
	logKey.Disable(KEY_ALL)
	assert.False(t, logKey.Enabled(KEY_CACHE))
	assert.Equals(t, logKey.flag, KEY_ACCESS|KEY_HTTP|KEY_REPLICATE)
}

func TestLogKeyNames(t *testing.T) {
	keys := []string{}
	logKeys := ToLogKey(keys)
	assert.Equals(t, logKeys.flag, KEY_NONE)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{})

	keys = append(keys, "DCP")
	logKeys = ToLogKey(keys)
	assert.Equals(t, logKeys.flag, KEY_DCP)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{"DCP"})

	keys = append(keys, "Access")
	logKeys = ToLogKey(keys)
	assert.Equals(t, logKeys.flag, KEY_ACCESS|KEY_DCP)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{"Access", "DCP"})

	keys = []string{"*", "DCP"}
	logKeys = ToLogKey(keys)
	assert.Equals(t, logKeys.flag, KEY_ALL|KEY_DCP)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{"*", "DCP"})
}

func BenchmarkEnabledLogKeys(b *testing.B) {
	logKeys := LogKey{flag: KEY_CRUD | KEY_DCP | KEY_REPLICATE}
	for i := 0; i < b.N; i++ {
		_ = logKeys.EnabledLogKeys()
	}
}

func BenchmarkToLogKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = ToLogKey([]string{"CRUD", "DCP", "Replicate"})
	}
}

func BenchmarkLogKeyEnabled(b *testing.B) {
	logKeys := LogKey{flag: KEY_CRUD | KEY_DCP | KEY_REPLICATE}
	benchmarkLogKeyEnabled(b, "Wildcard", KEY_CACHE, LogKey{flag: KEY_ALL})
	benchmarkLogKeyEnabled(b, "Hit", KEY_DCP, logKeys)
	benchmarkLogKeyEnabled(b, "Miss", KEY_CACHE, logKeys)
}

func benchmarkLogKeyEnabled(b *testing.B, name string, logKey uint32, logKeys LogKey) {
	b.Run(name, func(bn *testing.B) {
		for i := 0; i < bn.N; i++ {
			logKeys.Enabled(logKey)
		}
	})
}
