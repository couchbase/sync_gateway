// +build !cb_sg_enterprise

package base

import (
	"fmt"
)

// ErrDeltasNotSupported is returned when these functions are called in CE
var ErrDeltasNotSupported = fmt.Errorf("Deltas not supported in CE")

// Diff is only implemented in EE, the CE stub always returns an error.
func Diff(old, new map[string]interface{}) (delta []byte, err error) {
	return nil, ErrDeltasNotSupported
}

// Patch is only implemented in EE, the CE stub always returns an error.
func Patch(old *map[string]interface{}, delta map[string]interface{}) (err error) {
	return ErrDeltasNotSupported
}
