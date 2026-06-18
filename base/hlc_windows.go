//go:build windows

//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"sync/atomic"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	procQPF = windows.NewLazySystemDLL("kernel32.dll").NewProc("QueryPerformanceFrequency")
	procQPC = windows.NewLazySystemDLL("kernel32.dll").NewProc("QueryPerformanceCounter")

	qpcFreq        int64
	qpcAnchorTicks int64
	qpcAnchorNanos int64
)

func init() {
	var freq int64
	procQPF.Call(uintptr(unsafe.Pointer(&freq)))
	atomic.StoreInt64(&qpcFreq, freq)

	var ticks int64
	procQPC.Call(uintptr(unsafe.Pointer(&ticks)))
	atomic.StoreInt64(&qpcAnchorTicks, ticks)
	atomic.StoreInt64(&qpcAnchorNanos, time.Now().UnixNano())
}

func hlcWallClock() uint64 {
	var ticks int64
	procQPC.Call(uintptr(unsafe.Pointer(&ticks)))

	elapsed := ticks - atomic.LoadInt64(&qpcAnchorTicks)
	elapsedNanos := uint64(elapsed) * 1_000_000_000 / uint64(atomic.LoadInt64(&qpcFreq))
	return uint64(atomic.LoadInt64(&qpcAnchorNanos)) + elapsedNanos
}
