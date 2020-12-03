package base

import (
	"testing"
	"time"

	goassert "github.com/couchbaselabs/go.assert"
)

func TestLogKey(t *testing.T) {
	var logKeysPtr *LogKeyMask
	goassert.False(t, logKeysPtr.Enabled(KeyHTTP))

	logKeys := logKeyMask(KeyHTTP)
	goassert.True(t, logKeys.Enabled(KeyHTTP))

	// Enable more log keys.
	logKeys.Enable(KeyAccess)
	logKeys.Enable(KeyReplicate)
	goassert.True(t, logKeys.Enabled(KeyAccess))
	goassert.True(t, logKeys.Enabled(KeyReplicate))
	goassert.Equals(t, *logKeys, *logKeyMask(KeyAccess, KeyHTTP, KeyReplicate))

	// Enable wildcard and check unset key is enabled.
	logKeys.Enable(KeyAll)
	goassert.True(t, logKeys.Enabled(KeyCache))
	goassert.Equals(t, *logKeys, *logKeyMask(KeyAll, KeyAccess, KeyHTTP, KeyReplicate))

	// Disable wildcard and check that existing keys are still set.
	logKeys.Disable(KeyAll)
	goassert.True(t, logKeys.Enabled(KeyAccess))
	goassert.False(t, logKeys.Enabled(KeyCache))
	goassert.Equals(t, *logKeys, *logKeyMask(KeyAccess, KeyHTTP, KeyReplicate))
	// Set KeyNone and check keys are disabled.
	logKeys = logKeyMask(KeyNone)
	goassert.False(t, logKeys.Enabled(KeyAll))
	goassert.False(t, logKeys.Enabled(KeyCache))
	goassert.Equals(t, *logKeys, *logKeyMask(KeyNone))
}

func TestLogKeyNames(t *testing.T) {
	name := KeyDCP.String()
	goassert.Equals(t, name, "DCP")

	// Combined log keys, will pretty-print a set of log keys
	name = logKeyMask(KeyDCP, KeyReplicate).String()
	goassert.StringContains(t, name, "DCP")
	goassert.StringContains(t, name, "Replicate")

	keys := []string{}
	logKeys := ToLogKey(keys)
	goassert.Equals(t, logKeys, LogKeyMask(0))
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{})

	keys = append(keys, "DCP")
	logKeys = ToLogKey(keys)
	goassert.Equals(t, logKeys, *logKeyMask(KeyDCP))
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyDCP.String()})

	keys = append(keys, "Access")
	logKeys = ToLogKey(keys)
	goassert.Equals(t, logKeys, *logKeyMask(KeyAccess, KeyDCP))
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyAccess.String(), KeyDCP.String()})

	keys = []string{"*", "DCP"}
	logKeys = ToLogKey(keys)
	goassert.Equals(t, logKeys, *logKeyMask(KeyAll, KeyDCP))
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyAll.String(), KeyDCP.String()})

	// Special handling of log keys
	keys = []string{"HTTP+"}
	logKeys = ToLogKey(keys)
	goassert.Equals(t, logKeys, *logKeyMask(KeyHTTP, KeyHTTPResp))
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyHTTP.String(), KeyHTTPResp.String()})

	// Test that invalid log keys are ignored, and "+" suffixes are stripped.
	keys = []string{"DCP", "WS+", "InvalidLogKey"}
	logKeys = ToLogKey(keys)
	goassert.Equals(t, logKeys, *logKeyMask(KeyDCP, KeyWebSocket))
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyDCP.String(), KeyWebSocket.String()})
}

func TestConvertSpecialLogKey(t *testing.T) {
	tests := []struct {
		input  string
		output []LogKey
		ok     bool
	}{
		{
			input:  "HTTP",
			output: nil,
			ok:     false,
		},
		{
			input:  "HTTP+",
			output: []LogKey{KeyHTTP, KeyHTTPResp},
			ok:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.input, func(ts *testing.T) {
			output, ok := convertSpecialLogKey(test.input)
			goassert.Equals(ts, ok, test.ok)
			if ok {
				goassert.DeepEquals(ts, output, test.output)
			}
		})
	}
}

// This test has no assertions, but will flag any data races when run under `-race`.
func TestLogKeyConcurrency(t *testing.T) {
	var logKey LogKeyMask
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
	close(stop)
}

func BenchmarkLogKeyEnabled(b *testing.B) {
	logKeys := logKeyMask(KeyCRUD, KeyDCP, KeyReplicate)
	benchmarkLogKeyEnabled(b, "Wildcard", KeyCache, logKeyMask(KeyAll))
	benchmarkLogKeyEnabled(b, "Hit", KeyDCP, logKeys)
	benchmarkLogKeyEnabled(b, "Miss", KeyCache, logKeys)
}

func BenchmarkToggleLogKeys(b *testing.B) {
	b.Run("Enable", func(bn *testing.B) {
		logKeyMask := logKeyMask(KeyCRUD, KeyDCP, KeyReplicate)
		for i := 0; i < bn.N; i++ {
			logKeyMask.Enable(KeyHTTP)
		}
	})
	b.Run("Disable", func(bn *testing.B) {
		logKeyMask := logKeyMask(KeyCRUD, KeyDCP, KeyReplicate)
		for i := 0; i < bn.N; i++ {
			logKeyMask.Disable(KeyDCP)
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
		_ = ToLogKey([]string{"CRUD", "DCP", "Replicate"})
	}
}

func BenchmarkEnabledLogKeys(b *testing.B) {
	logKeys := logKeyMask(KeyCRUD, KeyDCP, KeyReplicate)
	for i := 0; i < b.N; i++ {
		_ = logKeys.EnabledLogKeys()
	}
}

func benchmarkLogKeyEnabled(b *testing.B, name string, logKey LogKey, logKeys *LogKeyMask) {
	b.Run(name, func(bn *testing.B) {
		for i := 0; i < bn.N; i++ {
			logKeys.Enabled(logKey)
		}
	})
}
