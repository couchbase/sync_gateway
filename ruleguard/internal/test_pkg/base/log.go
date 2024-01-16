// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"fmt"
	"log"
)

//nolint:goprintffuncname
func PanicfCtx(_ context.Context, format string, args ...interface{}) {
	log.Printf("[ERR] "+format, args...)
}

//nolint:goprintffuncname
func InfofCtx(_ context.Context, logkey, format string, args ...interface{}) {
	format = fmt.Sprintf("[INF] %s: %s", logkey, format)
	log.Printf(format, args...)
}
