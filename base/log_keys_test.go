package base

import (
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"
)

func TestLogKey(t *testing.T) {
	logKeys := KEY_HTTP
	assert.True(t, logKeys.Enabled(KEY_HTTP))

	// Enable more log keys.
	logKeys.Enable(KEY_ACCESS | KEY_REPLICATE)
	assert.True(t, logKeys.Enabled(KEY_ACCESS))
	assert.True(t, logKeys.Enabled(KEY_REPLICATE))
	assert.Equals(t, logKeys, KEY_ACCESS|KEY_HTTP|KEY_REPLICATE)

	// Enable wildcard and check unset key is enabled.
	logKeys.Enable(KEY_ALL)
	assert.True(t, logKeys.Enabled(KEY_CACHE))
	assert.Equals(t, logKeys, KEY_ALL|KEY_ACCESS|KEY_HTTP|KEY_REPLICATE)

	// Disable wildcard and check that existing keys are still set.
	logKeys.Disable(KEY_ALL)
	assert.False(t, logKeys.Enabled(KEY_CACHE))
	assert.Equals(t, logKeys, KEY_ACCESS|KEY_HTTP|KEY_REPLICATE)
}

func TestLogKeyNames(t *testing.T) {
	name := LogKeyName(KEY_DCP)
	assert.Equals(t, name, "DCP")

	// Can't retrieve name of combined log keys.
	name = LogKeyName(KEY_DCP | KEY_REPLICATE)
	assert.Equals(t, name, "")

	keys := []string{}
	logKeys := ToLogKey(keys)
	assert.Equals(t, logKeys, KEY_NONE)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{})

	keys = append(keys, "DCP")
	logKeys = ToLogKey(keys)
	assert.Equals(t, logKeys, KEY_DCP)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{LogKeyName(KEY_DCP)})

	keys = append(keys, "Access")
	logKeys = ToLogKey(keys)
	assert.Equals(t, logKeys, KEY_ACCESS|KEY_DCP)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{LogKeyName(KEY_ACCESS), LogKeyName(KEY_DCP)})

	keys = []string{"*", "DCP"}
	logKeys = ToLogKey(keys)
	assert.Equals(t, logKeys, KEY_ALL|KEY_DCP)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{LogKeyName(KEY_ALL), LogKeyName(KEY_DCP)})
}

func TestLogKeyConcurrency(t *testing.T) {
	var logKey LogKey
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
				logKey.Disable(KEY_DCP)
			case <-stop:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			default:
				logKey.Enabled(KEY_DCP)
			case <-stop:
				return
			}
		}
	}()

	time.Sleep(time.Millisecond * 100)
	stop <- struct{}{}
}

func BenchmarkLogKeyEnabled(b *testing.B) {
	logKeys := KEY_CRUD | KEY_DCP | KEY_REPLICATE
	benchmarkLogKeyEnabled(b, "Wildcard", KEY_CACHE, KEY_ALL)
	benchmarkLogKeyEnabled(b, "Hit", KEY_DCP, logKeys)
	benchmarkLogKeyEnabled(b, "Miss", KEY_CACHE, logKeys)
}

func BenchmarkToggleLogKeys(b *testing.B) {
	b.Run("Enable", func(bn *testing.B) {
		logKeys := KEY_CRUD | KEY_DCP | KEY_REPLICATE
		for i := 0; i < bn.N; i++ {
			logKeys.Enable(KEY_HTTP)
		}
	})
	b.Run("Disable", func(bn *testing.B) {
		logKeys := KEY_CRUD | KEY_DCP | KEY_REPLICATE
		for i := 0; i < bn.N; i++ {
			logKeys.Disable(KEY_DCP)
		}
	})
}

func BenchmarkLogKeyName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = LogKeyName(KEY_DCP)
	}
}

func BenchmarkToLogKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = ToLogKey([]string{"CRUD", "DCP", "Replicate"})
	}
}

func BenchmarkEnabledLogKeys(b *testing.B) {
	logKeys := KEY_CRUD | KEY_DCP | KEY_REPLICATE
	for i := 0; i < b.N; i++ {
		_ = logKeys.EnabledLogKeys()
	}
}

func benchmarkLogKeyEnabled(b *testing.B, name string, logKey LogKey, logKeys LogKey) {
	b.Run(name, func(bn *testing.B) {
		for i := 0; i < bn.N; i++ {
			logKeys.Enabled(logKey)
		}
	})
}
