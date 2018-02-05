package rest

import (
	"testing"

	assert "github.com/couchbaselabs/go.assert"
)

func TestLocalhostAddr(t *testing.T) {
	tests := []struct {
		input, expected string
	}{
		{":4984", "localhost:4984"},
		{"localhost:4984", "localhost:4984"},
		{"sg.couchbase.com:4984", "sg.couchbase.com:4984"},

		{"0.0.0.0:4984", "0.0.0.0:4984"},
		{"127.0.0.1:4984", "127.0.0.1:4984"},
		{"203.0.113.5:4984", "203.0.113.5:4984"},

		{"[::]:4984", "[::]:4984"},
		{"[::1]:4984", "[::1]:4984"},
		{"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4984", "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4984"},
	}

	for _, test := range tests {
		assert.Equals(t, localhostAddr(test.input), test.expected)
	}
}
