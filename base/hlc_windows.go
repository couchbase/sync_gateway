//go:build windows

package base

import (
	"sync/atomic"
	"time"

	"golang.org/x/sys/windows"
)

var (
	gpcFreq          int64
	gpcAnchorTickets int64
	gpcAnchorNanos   int64
)

func init() {
	windows.QueryPerformanceFrequency(&gpcFreq)

	var ticks int64
	windows.QueryPerformanceCounter(&ticks)
	atomic.StoreInt64(&gpcAnchorTickets, ticks)
	atomic.StoreInt64(&gpcAnchorNanos, uint64(time.Now().UnixNano()))
}

func hlvWallClock() uint64 {
	var ticks int64
	windows.QueryPerformanceCounter(&ticks)

	elapsed := ticks - atomic.LoadInt64(&gpcAnchorTickets)
	// convert elapsed ticks to nanoseconds
	elapsedNanos := uint64(elapsed) * 1e9 / uint64(gpcFreq)
	return atomic.LoadUint64(&gpcAnchorNanos) + elapsedNanos
}
