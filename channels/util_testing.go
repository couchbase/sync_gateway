package channels

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

// Creates a set from zero or more inline string arguments.
// Channel names must be valid, else the function will panic, so this should only be called
// with hardcoded known-valid strings.
func SetOf(tb testing.TB, names ...string) base.Set {
	set, err := SetFromArray(names, KeepStar)
	if err != nil {
		tb.Fatalf("channels.SetOf failed: %v", err)
	}
	return set
}
