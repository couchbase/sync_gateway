/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLogKey(t *testing.T) {
	var logKeysPtr *LogKeyMask
	assert.False(t, logKeysPtr.Enabled(KeyHTTP))

	logKeys := logKeyMask(KeyHTTP)
	assert.True(t, logKeys.Enabled(KeyHTTP))

	// Enable more log keys.
	logKeys.Enable(KeyAccess)
	logKeys.Enable(KeyReplicate)
	assert.True(t, logKeys.Enabled(KeyAccess))
	assert.True(t, logKeys.Enabled(KeyReplicate))
	assert.Equal(t, *logKeyMask(KeyAccess, KeyHTTP, KeyReplicate), *logKeys)

	// Enable wildcard and check unset key is enabled.
	logKeys.Enable(KeyAll)
	assert.True(t, logKeys.Enabled(KeyCache))
	assert.Equal(t, *logKeyMask(KeyAll, KeyAccess, KeyHTTP, KeyReplicate), *logKeys)

	// Disable wildcard and check that existing keys are still set.
	logKeys.Disable(KeyAll)
	assert.True(t, logKeys.Enabled(KeyAccess))
	assert.False(t, logKeys.Enabled(KeyCache))
	assert.Equal(t, *logKeyMask(KeyAccess, KeyHTTP, KeyReplicate), *logKeys)
	// Set KeyNone and check keys are disabled.
	logKeys = logKeyMask(KeyNone)
	assert.False(t, logKeys.Enabled(KeyAll))
	assert.False(t, logKeys.Enabled(KeyCache))
	assert.Equal(t, *logKeyMask(KeyNone), *logKeys)
}

func TestLogKeyNames(t *testing.T) {
	name := KeyDCP.String()
	assert.Equal(t, "DCP", name)

	// Combined log keys, will pretty-print a set of log keys
	name = logKeyMask(KeyDCP, KeyReplicate).String()
	assert.Contains(t, name, "DCP")
	assert.Contains(t, name, "Replicate")

	keys := []string{}
	logKeys := ToLogKey(keys)
	assert.Equal(t, LogKeyMask(0), logKeys)
	assert.Equal(t, []string{}, logKeys.EnabledLogKeys())

	keys = append(keys, "DCP")
	logKeys = ToLogKey(keys)
	assert.Equal(t, *logKeyMask(KeyDCP), logKeys)
	assert.Equal(t, []string{KeyDCP.String()}, logKeys.EnabledLogKeys())

	keys = append(keys, "Access")
	logKeys = ToLogKey(keys)
	assert.Equal(t, *logKeyMask(KeyAccess, KeyDCP), logKeys)
	assert.Equal(t, []string{KeyAccess.String(), KeyDCP.String()}, logKeys.EnabledLogKeys())

	keys = []string{"*", "DCP"}
	logKeys = ToLogKey(keys)
	assert.Equal(t, *logKeyMask(KeyAll, KeyDCP), logKeys)
	assert.Equal(t, []string{KeyAll.String(), KeyDCP.String()}, logKeys.EnabledLogKeys())

	// Special handling of log keys
	keys = []string{"HTTP+"}
	logKeys = ToLogKey(keys)
	assert.Equal(t, *logKeyMask(KeyHTTP, KeyHTTPResp), logKeys)
	assert.Equal(t, []string{KeyHTTP.String(), KeyHTTPResp.String()}, logKeys.EnabledLogKeys())

	// Test that invalid log keys are ignored, and "+" suffixes are stripped.
	keys = []string{"DCP", "WS+", "InvalidLogKey"}
	logKeys = ToLogKey(keys)
	assert.Equal(t, *logKeyMask(KeyDCP, KeyWebSocket), logKeys)
	assert.Equal(t, []string{KeyDCP.String(), KeyWebSocket.String()}, logKeys.EnabledLogKeys())
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
			assert.Equal(ts, test.ok, ok)
			if ok {
				assert.Equal(ts, test.output, output)
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
