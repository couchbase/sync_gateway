//go:build !windows

package base

import "time"

func hlcWallClock() uint64 {
	return uint64(time.Now().UnixNano())
}
