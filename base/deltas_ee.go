// +build cb_sg_enterprise

package base

import fleecedelta "github.com/couchbaselabs/go-fleecedelta"

// The reason for these wrappers is to keep go-fleecedelta out of the compiled CE builds by providing no-op versions in deltas_ce.go

// Diff will return the fleece delta between old and new.
func Diff(old, new map[string]interface{}) (delta []byte, err error) {
	return fleecedelta.DiffJSON(old, new)
}

// Patch will patch old with the given delta.
func Patch(old *map[string]interface{}, delta []byte) (err error) {
	return fleecedelta.PatchJSON(old, delta)
}
