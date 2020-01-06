package base

import (
	"testing"
	"time"

	goassert "github.com/couchbaselabs/go.assert"
)

func TestLogKey(t *testing.T) {
	var logKeysPtr *LogKey
	goassert.False(t, logKeysPtr.Enabled(KeyHTTP))

	logKeys := KeyHTTP
	goassert.True(t, logKeys.Enabled(KeyHTTP))

	// Enable more log keys.
	logKeys.Enable(KeyAccess | KeyReplicate)
	goassert.True(t, logKeys.Enabled(KeyAccess))
	goassert.True(t, logKeys.Enabled(KeyReplicate))
	goassert.Equals(t, logKeys, KeyAccess|KeyHTTP|KeyReplicate)

	// Enable wildcard and check unset key is enabled.
	logKeys.Enable(KeyAll)
	goassert.True(t, logKeys.Enabled(KeyCache))
	goassert.Equals(t, logKeys, KeyAll|KeyAccess|KeyHTTP|KeyReplicate)

	// Disable wildcard and check that existing keys are still set.
	logKeys.Disable(KeyAll)
	goassert.True(t, logKeys.Enabled(KeyAccess))
	goassert.False(t, logKeys.Enabled(KeyCache))
	goassert.Equals(t, logKeys, KeyAccess|KeyHTTP|KeyReplicate)

	// Set KeyNone and check keys are disabled.
	logKeys = KeyNone
	goassert.False(t, logKeys.Enabled(KeyAll))
	goassert.False(t, logKeys.Enabled(KeyCache))
	goassert.Equals(t, logKeys, KeyNone)
}

func TestLogKeyNames(t *testing.T) {
	name := KeyDCP.String()
	goassert.Equals(t, name, "DCP")

	// Combined log keys, will pretty-print a set of log keys
	name = LogKey(KeyDCP | KeyReplicate).String()
	goassert.StringContains(t, name, "DCP")
	goassert.StringContains(t, name, "Replicate")

	keys := []string{}
	logKeys, warnings := ToLogKey(keys)
	goassert.Equals(t, len(warnings), 0)
	goassert.Equals(t, logKeys, LogKey(0))
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{})

	keys = append(keys, "DCP")
	logKeys, warnings = ToLogKey(keys)
	goassert.Equals(t, len(warnings), 0)
	goassert.Equals(t, logKeys, KeyDCP)
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyDCP.String()})

	keys = append(keys, "Access")
	logKeys, warnings = ToLogKey(keys)
	goassert.Equals(t, len(warnings), 0)
	goassert.Equals(t, logKeys, KeyAccess|KeyDCP)
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyAccess.String(), KeyDCP.String()})

	keys = []string{"*", "DCP"}
	logKeys, warnings = ToLogKey(keys)
	goassert.Equals(t, len(warnings), 0)
	goassert.Equals(t, logKeys, KeyAll|KeyDCP)
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyAll.String(), KeyDCP.String()})

	// Special handling of log keys
	keys = []string{"HTTP+"}
	logKeys, warnings = ToLogKey(keys)
	goassert.Equals(t, len(warnings), 0)
	goassert.Equals(t, logKeys, KeyHTTP|KeyHTTPResp)
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyHTTP.String(), KeyHTTPResp.String()})

	// Test that invalid log keys are ignored, and "+" suffixes are stripped.
	keys = []string{"DCP", "WS+", "InvalidLogKey"}
	logKeys, warnings = ToLogKey(keys)
	goassert.Equals(t, len(warnings), 2)
	goassert.Equals(t, logKeys, KeyDCP|KeyWebSocket)
	goassert.DeepEquals(t, logKeys.EnabledLogKeys(), []string{KeyDCP.String(), KeyWebSocket.String()})
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
			goassert.Equals(ts, ok, test.ok)
			if ok {
				goassert.Equals(ts, *output, *test.output)
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
	close(stop)
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
