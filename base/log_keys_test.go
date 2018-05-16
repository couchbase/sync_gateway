package base

import (
	"fmt"
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"
)

func TestLogKey(t *testing.T) {
	var logKeysPtr *LogKey
	assert.False(t, logKeysPtr.Enabled(KeyHTTP))

	logKeys := KeyHTTP
	assert.True(t, logKeys.Enabled(KeyHTTP))

	// Enable more log keys.
	logKeys.Enable(KeyAccess | KeyReplicate)
	assert.True(t, logKeys.Enabled(KeyAccess))
	assert.True(t, logKeys.Enabled(KeyReplicate))
	assert.Equals(t, logKeys, KeyAccess|KeyHTTP|KeyReplicate)

	// Enable wildcard and check unset key is enabled.
	logKeys.Enable(KeyAll)
	assert.True(t, logKeys.Enabled(KeyCache))
	assert.Equals(t, logKeys, KeyAll|KeyAccess|KeyHTTP|KeyReplicate)

	// Disable wildcard and check that existing keys are still set.
	logKeys.Disable(KeyAll)
	assert.True(t, logKeys.Enabled(KeyAccess))
	assert.False(t, logKeys.Enabled(KeyCache))
	assert.Equals(t, logKeys, KeyAccess|KeyHTTP|KeyReplicate)

	// Set KeyNone and check keys are disabled.
	logKeys = KeyNone
	assert.False(t, logKeys.Enabled(KeyAll))
	assert.False(t, logKeys.Enabled(KeyCache))
	assert.Equals(t, logKeys, KeyNone)
}

func TestLogKeyNames(t *testing.T) {
	name := KeyDCP.String()
	assert.Equals(t, name, "DCP")

	// Combined log keys, or key masks print the binary representation.
	name = LogKey(KeyDCP | KeyReplicate).String()
	assert.Equals(t, name, fmt.Sprintf("LogKey(%b)", KeyDCP|KeyReplicate))

	keys := []string{}
	logKeys, _ := ToLogKey(keys)
	assert.Equals(t, logKeys, LogKey(0))
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{})

	keys = append(keys, "DCP")
	logKeys, _ = ToLogKey(keys)
	assert.Equals(t, logKeys, KeyDCP)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyDCP.String()})

	keys = append(keys, "Access")
	logKeys, _ = ToLogKey(keys)
	assert.Equals(t, logKeys, KeyAccess|KeyDCP)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyAccess.String(), KeyDCP.String()})

	keys = []string{"*", "DCP"}
	logKeys, _ = ToLogKey(keys)
	assert.Equals(t, logKeys, KeyAll|KeyDCP)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyAll.String(), KeyDCP.String()})

	// Special handling of log keys
	keys = []string{"HTTP+"}
	logKeys, _ = ToLogKey(keys)
	assert.Equals(t, logKeys, KeyHTTP|KeyHTTPResp)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyHTTP.String(), KeyHTTPResp.String()})

	// Test that invalid log keys are ignored, and "+" suffixes are stripped.
	keys = []string{"DCP", "WS+", "InvalidLogKey"}
	logKeys, _ = ToLogKey(keys)
	assert.Equals(t, logKeys, KeyDCP|KeyWebSocket)
	assert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyDCP.String(), KeyWebSocket.String()})
}

func TestConvertSpecialLogKey(t *testing.T) {
	tests := []struct {
		input  string
		output *LogKey
		ok     bool
	}{
		{
			input:  "HTTP",
			output: nil,
			ok:     false,
		},
		{
			input:  "HTTP+",
			output: logKeyPtr(KeyHTTP | KeyHTTPResp),
			ok:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(ts *testing.T) {
			output, ok := convertSpecialLogKey(test.input)
			assert.Equals(ts, ok, test.ok)
			if ok {
				assert.Equals(ts, *output, *test.output)
			}
		})
	}
}

// This test has no assertions, but will flag any data races when run under `-race`.
func TestLogKeyConcurrency(t *testing.T) {
	var logKey LogKey
	stop := make(chan struct{})

	go func() {
		for {
			select {
			default:
				logKey.Enable(KeyDCP)
			case <-stop:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			default:
				logKey.Disable(KeyDCP)
			case <-stop:
				return
			}
		}
	}()

	go func() {
		for {
			select {
			default:
				logKey.Enabled(KeyDCP)
			case <-stop:
				return
			}
		}
	}()

	time.Sleep(time.Millisecond * 100)
	stop <- struct{}{}
}

func BenchmarkLogKeyEnabled(b *testing.B) {
	logKeys := KeyCRUD | KeyDCP | KeyReplicate
	benchmarkLogKeyEnabled(b, "Wildcard", KeyCache, KeyAll)
	benchmarkLogKeyEnabled(b, "Hit", KeyDCP, logKeys)
	benchmarkLogKeyEnabled(b, "Miss", KeyCache, logKeys)
}

func BenchmarkToggleLogKeys(b *testing.B) {
	b.Run("Enable", func(bn *testing.B) {
		logKeys := KeyCRUD | KeyDCP | KeyReplicate
		for i := 0; i < bn.N; i++ {
			logKeys.Enable(KeyHTTP)
		}
	})
	b.Run("Disable", func(bn *testing.B) {
		logKeys := KeyCRUD | KeyDCP | KeyReplicate
		for i := 0; i < bn.N; i++ {
			logKeys.Disable(KeyDCP)
		}
	})
}

func BenchmarkLogKeyName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = KeyDCP.String()
	}
}

func BenchmarkToLogKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = ToLogKey([]string{"CRUD", "DCP", "Replicate"})
	}
}

func BenchmarkEnabledLogKeys(b *testing.B) {
	logKeys := KeyCRUD | KeyDCP | KeyReplicate
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
