package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSubprotocolString roundtrips the parse/format Subprotocol methods on the subprotocol constants.
func TestSubprotocolString(t *testing.T) {
	for i := minCBMobileSubprotocolVersion; i <= maxCBMobileSubprotocolVersion; i++ {
		str := i.SubprotocolString()
		parsed, err := ParseSubprotocolString(str)
		require.NoError(t, err)
		require.Equal(t, i, parsed)
	}
}
