//go:build cb_sg_deadlock_detector
// +build cb_sg_deadlock_detector

/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Package base provides a deadlock-detecting RWMutex and Mutex when built with
// the cb_sg_deadlock_detector build tag. These are drop-in replacements for
// sync.RWMutex and sync.Mutex via github.com/sasha-s/go-deadlock.
package base

import "github.com/sasha-s/go-deadlock"

func init() {
	panic("here")
}

// Mutex is an alias for deadlock.Mutex, a drop-in replacement for sync.Mutex
// that detects potential deadlocks at runtime.
type Mutex = deadlock.Mutex

// RWMutex is an alias for deadlock.RWMutex, a drop-in replacement for sync.RWMutex
// that detects potential deadlocks at runtime.
type RWMutex = deadlock.RWMutex
