//go:build !cb_sg_deadlock_detector
// +build !cb_sg_deadlock_detector

/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import "sync"

// Mutex is an alias for sync.Mutex. When built with -tags cb_sg_deadlock_detector,
// it is replaced with a deadlock-detecting implementation from go-deadlock.
type Mutex = sync.Mutex

// RWMutex is an alias for sync.RWMutex. When built with -tags cb_sg_deadlock_detector,
// it is replaced with a deadlock-detecting implementation from go-deadlock.
type RWMutex = sync.RWMutex
