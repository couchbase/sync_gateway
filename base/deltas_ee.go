// +build cb_sg_enterprise

package base

import (
	"time"

	"github.com/couchbaselabs/go-fleecedelta"
)

// The reason for these wrappers is to keep go-fleecedelta out of the compiled CE builds by providing no-op versions in deltas_ce.go

func init() {
	fleecedelta.StringDiffEnabled = true                  // Global toggle for string diffs
	fleecedelta.StringDiffMinLength = 60                  // 60 B min length to match CBL
	fleecedelta.StringDiffMaxLength = 1024 * 1024         // 1 MB max length for string diffs
	fleecedelta.StringDiffTimeout = time.Millisecond * 10 // Aggressive string diff timeout
}

// Diff will return the fleece delta between old and new.
func Diff(old, new map[string]interface{}) (delta []byte, err error) {
	return fleecedelta.DiffJSON(old, new)
}

// Patch will patch old with the given delta.
func Patch(old *map[string]interface{}, delta []byte) (err error) {
	return fleecedelta.PatchJSON(old, delta)
}
